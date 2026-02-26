use crc32fast::Hasher;
use regex::Regex;
use sea_orm::sqlx;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, DbErr, Statement, TransactionTrait, Value,
};
use sea_orm_migration::SchemaManager;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use thiserror::Error;

const DEFAULT_ADVISORY_LOCK_ID: i64 = 7_333_654_209_921_337;

#[derive(Debug, Clone)]
pub struct SchemalaneConfig {
    pub schema: String,
    pub history_table: String,
    pub migrations_dir: PathBuf,
    pub installed_by: Option<String>,
    pub advisory_lock_id: i64,
}

impl Default for SchemalaneConfig {
    fn default() -> Self {
        Self {
            schema: "public".to_owned(),
            history_table: "flyway_schema_history".to_owned(),
            migrations_dir: PathBuf::from("./migrations"),
            installed_by: None,
            advisory_lock_id: DEFAULT_ADVISORY_LOCK_ID,
        }
    }
}

#[derive(Debug, Error)]
pub enum SchemalaneError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Db(#[from] DbErr),

    #[error("Locking error: {0}")]
    Lock(#[from] sqlx::Error),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Drift detected: {0}")]
    Drift(String),

    #[error("Failed migration found in history: {0}")]
    FailedHistory(String),

    #[error("Migration execution failed for {script}: {source}")]
    MigrationExecution {
        script: String,
        #[source]
        source: DbErr,
    },

    #[error("`fresh` requires --yes confirmation")]
    FreshRequiresYes,

    #[error("Pending migrations found ({0})")]
    PendingMigrations(usize),

    #[error("Only PostgreSQL is supported in Schemalane v1")]
    UnsupportedBackend,
}

impl SchemalaneError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Validation(_) => 2,
            Self::Drift(_) => 3,
            Self::FailedHistory(_) => 4,
            Self::PendingMigrations(_) => 5,
            Self::FreshRequiresYes => 6,
            _ => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum MigrationState {
    Success,
    Pending,
    Failed,
    Missing,
    ChecksumMismatch,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusEntry {
    pub version: Option<String>,
    pub description: String,
    #[serde(rename = "type")]
    pub migration_type: String,
    pub script: String,
    pub checksum: Option<i32>,
    pub installed_rank: Option<i32>,
    pub installed_on: Option<String>,
    pub execution_time_ms: Option<i32>,
    pub state: MigrationState,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct StatusSummary {
    pub success: usize,
    pub pending: usize,
    pub failed: usize,
    pub missing: usize,
    pub checksum_mismatch: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusReport {
    pub schema: String,
    pub history_table: String,
    pub migrations: Vec<StatusEntry>,
    pub summary: StatusSummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct AppliedMigration {
    pub version: String,
    pub description: String,
    #[serde(rename = "type")]
    pub migration_type: String,
    pub script: String,
    pub execution_time_ms: i32,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RunReport {
    pub applied: Vec<AppliedMigration>,
    pub skipped: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RustTransactionMode {
    NoTransaction,
    Transaction,
}

pub type RustMigrationFuture<'a> = Pin<Box<dyn Future<Output = Result<(), DbErr>> + Send + 'a>>;

type DynRustMigrationFn =
    dyn for<'a> Fn(&'a SchemaManager<'a>) -> RustMigrationFuture<'a> + Send + Sync;

#[derive(Clone)]
pub struct RustMigrationExecutor {
    transaction_mode: RustTransactionMode,
    run: Arc<DynRustMigrationFn>,
}

impl RustMigrationExecutor {
    pub fn new<F>(run: F) -> Self
    where
        F: for<'a> Fn(&'a SchemaManager<'a>) -> RustMigrationFuture<'a> + Send + Sync + 'static,
    {
        Self::with_mode(RustTransactionMode::NoTransaction, run)
    }

    pub fn transactional<F>(run: F) -> Self
    where
        F: for<'a> Fn(&'a SchemaManager<'a>) -> RustMigrationFuture<'a> + Send + Sync + 'static,
    {
        Self::with_mode(RustTransactionMode::Transaction, run)
    }

    pub fn with_mode<F>(transaction_mode: RustTransactionMode, run: F) -> Self
    where
        F: for<'a> Fn(&'a SchemaManager<'a>) -> RustMigrationFuture<'a> + Send + Sync + 'static,
    {
        Self {
            transaction_mode,
            run: Arc::new(run),
        }
    }

    fn transaction_mode(&self) -> RustTransactionMode {
        self.transaction_mode
    }

    async fn up(&self, manager: &SchemaManager<'_>) -> Result<(), DbErr> {
        (self.run)(manager).await
    }
}

pub struct SchemalaneMigrator {
    config: SchemalaneConfig,
    rust_migrations: HashMap<String, RustMigrationExecutor>,
}

impl SchemalaneMigrator {
    pub fn new(config: SchemalaneConfig) -> Self {
        Self {
            config,
            rust_migrations: HashMap::new(),
        }
    }

    pub fn config(&self) -> &SchemalaneConfig {
        &self.config
    }

    pub fn register_rust_migration<S>(&mut self, script: S, migration: RustMigrationExecutor)
    where
        S: Into<String>,
    {
        self.rust_migrations
            .insert(normalize_script_key(script.into()), migration);
    }

    pub fn with_rust_migration<S>(mut self, script: S, migration: RustMigrationExecutor) -> Self
    where
        S: Into<String>,
    {
        self.register_rust_migration(script, migration);
        self
    }

    pub fn with_rust_migrations<I, S>(mut self, migrations: I) -> Self
    where
        I: IntoIterator<Item = (S, RustMigrationExecutor)>,
        S: Into<String>,
    {
        for (script, migration) in migrations {
            self.register_rust_migration(script, migration);
        }
        self
    }

    pub async fn up(&self, db: &DatabaseConnection) -> Result<RunReport, SchemalaneError> {
        self.ensure_postgres(db)?;
        let migrations = self.discover_migrations()?;
        self.ensure_rust_executors_registered(&migrations)?;
        self.with_advisory_lock(db, async {
            self.ensure_history_table(db).await?;
            let installed_by = self.resolve_installed_by(db).await?;
            let mut history = self.load_history(db).await?;
            self.ensure_no_blocking_history(&migrations, &history)?;

            let mut report = RunReport::default();
            for migration in &migrations {
                if is_applied_success(migration, &history) {
                    report.skipped += 1;
                    continue;
                }

                let started = Instant::now();
                let run_result = self.apply_migration(db, migration).await;
                let execution_time_ms = millis_i32(started.elapsed().as_millis());

                match run_result {
                    Ok(()) => {
                        let installed_rank = self
                            .insert_history_row(
                                db,
                                migration,
                                &installed_by,
                                execution_time_ms,
                                true,
                            )
                            .await?;
                        history.push(HistoryRow::from_migration(
                            migration,
                            execution_time_ms,
                            true,
                            installed_rank,
                        ));
                        report.applied.push(AppliedMigration {
                            version: migration.version_text.clone(),
                            description: migration.description_display.clone(),
                            migration_type: migration.migration_type.as_history_type().to_owned(),
                            script: migration.script.clone(),
                            execution_time_ms,
                        });
                    }
                    Err(source) => {
                        self.insert_history_row(
                            db,
                            migration,
                            &installed_by,
                            execution_time_ms,
                            false,
                        )
                        .await?;
                        return Err(SchemalaneError::MigrationExecution {
                            script: migration.script.clone(),
                            source,
                        });
                    }
                }
            }

            Ok(report)
        })
        .await
    }

    pub async fn status(&self, db: &DatabaseConnection) -> Result<StatusReport, SchemalaneError> {
        self.ensure_postgres(db)?;
        let migrations = self.discover_migrations()?;

        let history = if self.history_table_exists(db).await? {
            self.load_history(db).await?
        } else {
            Vec::new()
        };

        Ok(build_status_report(
            &self.config.schema,
            &self.config.history_table,
            &migrations,
            &history,
        ))
    }

    pub async fn fresh(
        &self,
        db: &DatabaseConnection,
        confirmed: bool,
    ) -> Result<RunReport, SchemalaneError> {
        if !confirmed {
            return Err(SchemalaneError::FreshRequiresYes);
        }

        self.ensure_postgres(db)?;
        let migrations = self.discover_migrations()?;
        self.ensure_rust_executors_registered(&migrations)?;

        self.with_advisory_lock(db, async {
            self.drop_all_tables(db).await?;
            self.ensure_history_table(db).await?;

            let installed_by = self.resolve_installed_by(db).await?;
            let mut report = RunReport::default();

            for migration in &migrations {
                let started = Instant::now();
                let run_result = self.apply_migration(db, migration).await;
                let execution_time_ms = millis_i32(started.elapsed().as_millis());

                match run_result {
                    Ok(()) => {
                        self.insert_history_row(
                            db,
                            migration,
                            &installed_by,
                            execution_time_ms,
                            true,
                        )
                        .await?;
                        report.applied.push(AppliedMigration {
                            version: migration.version_text.clone(),
                            description: migration.description_display.clone(),
                            migration_type: migration.migration_type.as_history_type().to_owned(),
                            script: migration.script.clone(),
                            execution_time_ms,
                        });
                    }
                    Err(source) => {
                        self.insert_history_row(
                            db,
                            migration,
                            &installed_by,
                            execution_time_ms,
                            false,
                        )
                        .await?;
                        return Err(SchemalaneError::MigrationExecution {
                            script: migration.script.clone(),
                            source,
                        });
                    }
                }
            }

            Ok(report)
        })
        .await
    }

    async fn with_advisory_lock<T, F>(
        &self,
        db: &DatabaseConnection,
        fut: F,
    ) -> Result<T, SchemalaneError>
    where
        F: Future<Output = Result<T, SchemalaneError>>,
    {
        let pool = db.get_postgres_connection_pool();
        let mut lock_conn = pool.acquire().await?;

        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(self.config.advisory_lock_id)
            .execute(&mut *lock_conn)
            .await?;

        let operation_result = fut.await;

        let unlock_result = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(self.config.advisory_lock_id)
            .execute(&mut *lock_conn)
            .await;

        match (operation_result, unlock_result) {
            (Ok(value), Ok(_)) => Ok(value),
            (Err(err), Ok(_)) => Err(err),
            (Ok(_), Err(err)) => Err(SchemalaneError::Lock(err)),
            (Err(err), Err(_unlock_err)) => Err(err),
        }
    }

    fn ensure_postgres(&self, db: &DatabaseConnection) -> Result<(), SchemalaneError> {
        if db.get_database_backend() != DbBackend::Postgres {
            return Err(SchemalaneError::UnsupportedBackend);
        }
        Ok(())
    }

    fn discover_migrations(&self) -> Result<Vec<DiscoveredMigration>, SchemalaneError> {
        let mut migrations = self.discover_sql_migrations()?;
        migrations.extend(self.discover_rust_migrations()?);

        let mut versions = BTreeSet::new();
        let mut scripts = BTreeSet::new();

        for migration in &migrations {
            if !versions.insert(migration.version_text.clone()) {
                return Err(SchemalaneError::Validation(format!(
                    "duplicate migration version '{}'",
                    migration.version_text
                )));
            }
            if !scripts.insert(migration.script.clone()) {
                return Err(SchemalaneError::Validation(format!(
                    "duplicate migration script '{}'",
                    migration.script
                )));
            }
        }

        migrations.sort_by(|a, b| {
            a.version
                .cmp(&b.version)
                .then_with(|| a.script.cmp(&b.script))
        });
        Ok(migrations)
    }

    fn discover_sql_migrations(&self) -> Result<Vec<DiscoveredMigration>, SchemalaneError> {
        if !self.config.migrations_dir.exists() {
            return Err(SchemalaneError::Validation(format!(
                "migrations directory not found: {}",
                self.config.migrations_dir.display()
            )));
        }

        let mut migrations = Vec::new();
        for entry in std::fs::read_dir(&self.config.migrations_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            if path.extension().and_then(|ext| ext.to_str()) != Some("sql") {
                continue;
            }

            let file_name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
                SchemalaneError::Validation("non-utf8 migration filename".to_owned())
            })?;

            let (version_text, parsed_version, description) = parse_sql_filename(file_name)?;
            let content = std::fs::read(&path)?;
            let checksum = Some(calculate_checksum(&content));
            let description_display = description.replace('_', " ");

            migrations.push(DiscoveredMigration {
                version: parsed_version,
                version_text,
                description_display,
                script: file_name.to_owned(),
                checksum,
                migration_type: MigrationType::Sql,
                source: MigrationSource::SqlFile(path),
            });
        }

        Ok(migrations)
    }

    fn discover_rust_migrations(&self) -> Result<Vec<DiscoveredMigration>, SchemalaneError> {
        let mut migrations = Vec::new();

        for entry in std::fs::read_dir(&self.config.migrations_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }

            let file_name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
                SchemalaneError::Validation("non-utf8 migration filename".to_owned())
            })?;

            let (version_text, parsed_version, description) = parse_rust_filename(file_name)?;
            let content = std::fs::read(&path)?;
            let checksum = Some(calculate_checksum(&content));
            let description_display = description.replace('_', " ");

            migrations.push(DiscoveredMigration {
                version: parsed_version,
                version_text,
                description_display,
                script: file_name.to_owned(),
                checksum,
                migration_type: MigrationType::Rust,
                source: MigrationSource::RustFile(path),
            });
        }

        Ok(migrations)
    }

    fn ensure_rust_executors_registered(
        &self,
        migrations: &[DiscoveredMigration],
    ) -> Result<(), SchemalaneError> {
        let mut missing_scripts = Vec::new();

        for migration in migrations {
            if migration.migration_type == MigrationType::Rust
                && !self.rust_migrations.contains_key(migration.script.as_str())
            {
                missing_scripts.push(migration.script.clone());
            }
        }

        if missing_scripts.is_empty() {
            Ok(())
        } else {
            missing_scripts.sort();
            Err(SchemalaneError::Validation(format!(
                "missing Rust migration executor(s) for script(s): {}",
                missing_scripts.join(", ")
            )))
        }
    }

    fn ensure_no_blocking_history(
        &self,
        migrations: &[DiscoveredMigration],
        history: &[HistoryRow],
    ) -> Result<(), SchemalaneError> {
        let latest = latest_history_by_script(history);
        let local_by_script: HashMap<&str, &DiscoveredMigration> =
            migrations.iter().map(|m| (m.script.as_str(), m)).collect();

        let mut failed = Vec::new();
        let mut missing = Vec::new();
        let mut checksum_mismatch = Vec::new();

        for row in latest.values() {
            if !row.success {
                failed.push(row.script.clone());
            }
            if row.success && !local_by_script.contains_key(row.script.as_str()) {
                missing.push(row.script.clone());
            }
        }

        for migration in migrations {
            if let Some(row) = latest.get(migration.script.as_str()) {
                if row.success && row.checksum != migration.checksum {
                    checksum_mismatch.push(migration.script.clone());
                }
            }
        }

        if !failed.is_empty() {
            failed.sort();
            return Err(SchemalaneError::FailedHistory(failed.join(", ")));
        }

        let mut drift_items = Vec::new();
        if !missing.is_empty() {
            missing.sort();
            drift_items.push(format!("missing: {}", missing.join(", ")));
        }
        if !checksum_mismatch.is_empty() {
            checksum_mismatch.sort();
            drift_items.push(format!(
                "checksum mismatch: {}",
                checksum_mismatch.join(", ")
            ));
        }

        if !drift_items.is_empty() {
            return Err(SchemalaneError::Drift(drift_items.join("; ")));
        }

        Ok(())
    }

    async fn apply_migration(
        &self,
        db: &DatabaseConnection,
        migration: &DiscoveredMigration,
    ) -> Result<(), DbErr> {
        match &migration.source {
            MigrationSource::SqlFile(path) => {
                let sql = std::fs::read_to_string(path).map_err(|err| {
                    DbErr::Custom(format!(
                        "failed to read SQL migration {}: {err}",
                        path.display()
                    ))
                })?;
                let manager = SchemaManager::new(db);
                execute_sql_migration(&manager, &sql).await
            }
            MigrationSource::RustFile(path) => {
                let executor = self
                    .rust_migrations
                    .get(migration.script.as_str())
                    .ok_or_else(|| {
                        DbErr::Custom(format!(
                            "missing Rust migration executor for script {} ({})",
                            migration.script,
                            path.display()
                        ))
                    })?;
                let manager = SchemaManager::new(db);
                execute_rust_migration(&manager, executor).await
            }
        }
    }

    async fn ensure_history_table(&self, db: &DatabaseConnection) -> Result<(), DbErr> {
        let table = qualified_table(&self.config.schema, &self.config.history_table);
        let success_idx = quote_ident(&format!("{}_s_idx", self.config.history_table));
        let version_idx = quote_ident(&format!("{}_v_idx", self.config.history_table));

        let ddl = format!(
            "\
CREATE TABLE IF NOT EXISTS {table} (\
\"installed_rank\" INTEGER NOT NULL,\
\"version\" VARCHAR(50),\
\"description\" VARCHAR(200) NOT NULL,\
\"type\" VARCHAR(20) NOT NULL,\
\"script\" VARCHAR(1000) NOT NULL,\
\"checksum\" INTEGER,\
\"installed_by\" VARCHAR(100) NOT NULL,\
\"installed_on\" TIMESTAMPTZ NOT NULL DEFAULT now(),\
\"execution_time\" INTEGER NOT NULL,\
\"success\" BOOLEAN NOT NULL,\
CONSTRAINT {pk} PRIMARY KEY (\"installed_rank\")\
);\
CREATE INDEX IF NOT EXISTS {success_idx} ON {table} (\"success\");\
CREATE INDEX IF NOT EXISTS {version_idx} ON {table} (\"version\");",
            pk = quote_ident(&format!("{}_pk", self.config.history_table)),
        );

        db.execute_unprepared(&ddl).await?;
        Ok(())
    }

    async fn history_table_exists(&self, db: &DatabaseConnection) -> Result<bool, DbErr> {
        let regclass = format!("{}.{}", self.config.schema, self.config.history_table);
        let stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT to_regclass($1) IS NOT NULL AS exists",
            [regclass.into()],
        );

        let row = db.query_one_raw(stmt).await?.ok_or_else(|| {
            DbErr::Custom("failed to evaluate history table existence".to_owned())
        })?;

        row.try_get("", "exists")
    }

    async fn load_history(&self, db: &DatabaseConnection) -> Result<Vec<HistoryRow>, DbErr> {
        let table = qualified_table(&self.config.schema, &self.config.history_table);
        let query = format!(
            "SELECT \"installed_rank\", \"version\", \"description\", \"type\", \"script\", \"checksum\", \"installed_by\", \"installed_on\"::text AS \"installed_on\", \"execution_time\", \"success\" FROM {table} ORDER BY \"installed_rank\" ASC"
        );

        let stmt = Statement::from_string(DbBackend::Postgres, query);
        let rows = db.query_all_raw(stmt).await?;

        rows.into_iter()
            .map(|row| {
                Ok(HistoryRow {
                    installed_rank: row.try_get("", "installed_rank")?,
                    version: row.try_get("", "version")?,
                    description: row.try_get("", "description")?,
                    migration_type: row.try_get("", "type")?,
                    script: row.try_get("", "script")?,
                    checksum: row.try_get("", "checksum")?,
                    installed_on: row.try_get("", "installed_on")?,
                    execution_time: row.try_get("", "execution_time")?,
                    success: row.try_get("", "success")?,
                })
            })
            .collect()
    }

    async fn resolve_installed_by(&self, db: &DatabaseConnection) -> Result<String, DbErr> {
        if let Some(installed_by) = &self.config.installed_by {
            return Ok(installed_by.clone());
        }

        let stmt = Statement::from_string(
            DbBackend::Postgres,
            "SELECT current_user AS current_user".to_owned(),
        );
        let row = db
            .query_one_raw(stmt)
            .await?
            .ok_or_else(|| DbErr::Custom("could not resolve current_user".to_owned()))?;

        row.try_get("", "current_user")
    }

    async fn next_installed_rank(&self, db: &DatabaseConnection) -> Result<i32, DbErr> {
        let table = qualified_table(&self.config.schema, &self.config.history_table);
        let stmt = Statement::from_string(
            DbBackend::Postgres,
            format!("SELECT COALESCE(MAX(\"installed_rank\"), 0) + 1 AS next_rank FROM {table}"),
        );

        let row = db
            .query_one_raw(stmt)
            .await?
            .ok_or_else(|| DbErr::Custom("failed to compute next installed_rank".to_owned()))?;

        row.try_get("", "next_rank")
    }

    async fn insert_history_row(
        &self,
        db: &DatabaseConnection,
        migration: &DiscoveredMigration,
        installed_by: &str,
        execution_time: i32,
        success: bool,
    ) -> Result<i32, DbErr> {
        let installed_rank = self.next_installed_rank(db).await?;
        let table = qualified_table(&self.config.schema, &self.config.history_table);

        let sql = format!(
            "INSERT INTO {table} (\"installed_rank\", \"version\", \"description\", \"type\", \"script\", \"checksum\", \"installed_by\", \"execution_time\", \"success\") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)"
        );

        let values = vec![
            Value::from(installed_rank),
            Value::from(Some(migration.version_text.clone())),
            Value::from(migration.description_display.clone()),
            Value::from(migration.migration_type.as_history_type().to_owned()),
            Value::from(migration.script.clone()),
            Value::from(migration.checksum),
            Value::from(installed_by.to_owned()),
            Value::from(execution_time),
            Value::from(success),
        ];

        let stmt = Statement::from_sql_and_values(DbBackend::Postgres, sql, values);
        db.execute_raw(stmt).await?;
        Ok(installed_rank)
    }

    async fn drop_all_tables(&self, db: &DatabaseConnection) -> Result<(), DbErr> {
        let stmt = Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $1 ORDER BY tablename",
            [self.config.schema.clone().into()],
        );

        let rows = db.query_all_raw(stmt).await?;
        for row in rows {
            let table_name: String = row.try_get("", "tablename")?;
            let sql = format!(
                "DROP TABLE IF EXISTS {}.{} CASCADE",
                quote_ident(&self.config.schema),
                quote_ident(&table_name)
            );
            db.execute_unprepared(&sql).await?;
        }

        Ok(())
    }
}

fn build_status_report(
    schema: &str,
    history_table: &str,
    migrations: &[DiscoveredMigration],
    history: &[HistoryRow],
) -> StatusReport {
    let latest = latest_history_by_script(history);
    let local_by_script: HashMap<&str, &DiscoveredMigration> =
        migrations.iter().map(|m| (m.script.as_str(), m)).collect();

    let mut entries = Vec::new();

    for migration in migrations {
        let entry = match latest.get(migration.script.as_str()) {
            Some(row) if !row.success => StatusEntry {
                version: row.version.clone(),
                description: row.description.clone(),
                migration_type: row.migration_type.clone(),
                script: row.script.clone(),
                checksum: row.checksum,
                installed_rank: Some(row.installed_rank),
                installed_on: Some(row.installed_on.clone()),
                execution_time_ms: Some(row.execution_time),
                state: MigrationState::Failed,
            },
            Some(row) if row.checksum != migration.checksum => StatusEntry {
                version: Some(migration.version_text.clone()),
                description: migration.description_display.clone(),
                migration_type: migration.migration_type.as_history_type().to_owned(),
                script: migration.script.clone(),
                checksum: migration.checksum,
                installed_rank: Some(row.installed_rank),
                installed_on: Some(row.installed_on.clone()),
                execution_time_ms: Some(row.execution_time),
                state: MigrationState::ChecksumMismatch,
            },
            Some(row) => StatusEntry {
                version: Some(migration.version_text.clone()),
                description: migration.description_display.clone(),
                migration_type: migration.migration_type.as_history_type().to_owned(),
                script: migration.script.clone(),
                checksum: migration.checksum,
                installed_rank: Some(row.installed_rank),
                installed_on: Some(row.installed_on.clone()),
                execution_time_ms: Some(row.execution_time),
                state: MigrationState::Success,
            },
            None => StatusEntry {
                version: Some(migration.version_text.clone()),
                description: migration.description_display.clone(),
                migration_type: migration.migration_type.as_history_type().to_owned(),
                script: migration.script.clone(),
                checksum: migration.checksum,
                installed_rank: None,
                installed_on: None,
                execution_time_ms: None,
                state: MigrationState::Pending,
            },
        };

        entries.push(entry);
    }

    for row in latest.values() {
        if row.success && !local_by_script.contains_key(row.script.as_str()) {
            entries.push(StatusEntry {
                version: row.version.clone(),
                description: row.description.clone(),
                migration_type: row.migration_type.clone(),
                script: row.script.clone(),
                checksum: row.checksum,
                installed_rank: Some(row.installed_rank),
                installed_on: Some(row.installed_on.clone()),
                execution_time_ms: Some(row.execution_time),
                state: MigrationState::Missing,
            });
        }
    }

    entries.sort_by(|a, b| {
        let a_version = a
            .version
            .as_ref()
            .and_then(|v| ParsedVersion::parse(v).ok());
        let b_version = b
            .version
            .as_ref()
            .and_then(|v| ParsedVersion::parse(v).ok());

        a_version
            .cmp(&b_version)
            .then_with(|| a.script.cmp(&b.script))
            .then_with(|| a.installed_rank.cmp(&b.installed_rank))
    });

    let mut summary = StatusSummary::default();
    for entry in &entries {
        match entry.state {
            MigrationState::Success => summary.success += 1,
            MigrationState::Pending => summary.pending += 1,
            MigrationState::Failed => summary.failed += 1,
            MigrationState::Missing => summary.missing += 1,
            MigrationState::ChecksumMismatch => summary.checksum_mismatch += 1,
        }
    }

    StatusReport {
        schema: schema.to_owned(),
        history_table: history_table.to_owned(),
        migrations: entries,
        summary,
    }
}

async fn execute_sql_migration(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
    let db = manager.get_connection();
    let txn = db.begin().await?;

    match txn.execute_unprepared(sql).await {
        Ok(_) => txn.commit().await,
        Err(err) => {
            let _ = txn.rollback().await;
            Err(err)
        }
    }
}

async fn execute_rust_migration(
    manager: &SchemaManager<'_>,
    migration: &RustMigrationExecutor,
) -> Result<(), DbErr> {
    match migration.transaction_mode() {
        RustTransactionMode::NoTransaction => migration.up(manager).await,
        RustTransactionMode::Transaction => {
            let db = manager.get_connection();
            let txn = db.begin().await?;
            let txn_manager = SchemaManager::new(&txn);

            match migration.up(&txn_manager).await {
                Ok(_) => txn.commit().await,
                Err(err) => {
                    let _ = txn.rollback().await;
                    Err(err)
                }
            }
        }
    }
}

fn is_applied_success(migration: &DiscoveredMigration, history: &[HistoryRow]) -> bool {
    latest_history_by_script(history)
        .get(migration.script.as_str())
        .is_some_and(|row| row.success && row.checksum == migration.checksum)
}

fn latest_history_by_script<'a>(history: &'a [HistoryRow]) -> HashMap<&'a str, &'a HistoryRow> {
    let mut latest = HashMap::new();
    for row in history {
        latest.insert(row.script.as_str(), row);
    }
    latest
}

fn parse_sql_filename(file_name: &str) -> Result<(String, ParsedVersion, String), SchemalaneError> {
    let captures = sql_migration_regex().captures(file_name).ok_or_else(|| {
        SchemalaneError::Validation(format!(
            "invalid SQL migration filename '{}': expected V<version>__<description>.sql",
            file_name
        ))
    })?;

    let version_text = captures
        .name("version")
        .ok_or_else(|| SchemalaneError::Validation("missing version capture".to_owned()))?
        .as_str()
        .to_owned();

    let description = captures
        .name("description")
        .ok_or_else(|| SchemalaneError::Validation("missing description capture".to_owned()))?
        .as_str()
        .to_owned();

    let parsed = ParsedVersion::parse(&version_text)?;
    Ok((version_text, parsed, description))
}

fn parse_rust_filename(
    file_name: &str,
) -> Result<(String, ParsedVersion, String), SchemalaneError> {
    let captures = rust_migration_regex().captures(file_name).ok_or_else(|| {
        SchemalaneError::Validation(format!(
            "invalid Rust migration filename '{}': expected V<version>__<description>.rs",
            file_name
        ))
    })?;

    let version_text = captures
        .name("version")
        .ok_or_else(|| SchemalaneError::Validation("missing version capture".to_owned()))?
        .as_str()
        .to_owned();

    let description = captures
        .name("description")
        .ok_or_else(|| SchemalaneError::Validation("missing description capture".to_owned()))?
        .as_str()
        .to_owned();

    let parsed = ParsedVersion::parse(&version_text)?;
    Ok((version_text, parsed, description))
}

fn validate_version(version: &str) -> Result<(), SchemalaneError> {
    if version_regex().is_match(version) {
        Ok(())
    } else {
        Err(SchemalaneError::Validation(format!(
            "invalid version '{}': expected ^[0-9]+([._][0-9]+)*$",
            version
        )))
    }
}

fn sql_migration_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"^V(?P<version>[0-9]+(?:[._][0-9]+)*)__(?P<description>[a-z0-9_]+)\.sql$")
            .expect("valid SQL migration regex")
    })
}

fn rust_migration_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"^V(?P<version>[0-9]+(?:[._][0-9]+)*)__(?P<description>[a-z0-9_]+)\.rs$")
            .expect("valid Rust migration regex")
    })
}

fn version_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"^[0-9]+([._][0-9]+)*$").expect("valid version regex"))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn qualified_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

fn millis_i32(millis: u128) -> i32 {
    if millis > i32::MAX as u128 {
        i32::MAX
    } else {
        millis as i32
    }
}

fn calculate_checksum(bytes: &[u8]) -> i32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    i32::from_be_bytes(hasher.finalize().to_be_bytes())
}

fn normalize_script_key(script: String) -> String {
    Path::new(&script)
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_owned())
        .unwrap_or(script)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationType {
    Sql,
    Rust,
}

impl MigrationType {
    fn as_history_type(self) -> &'static str {
        match self {
            Self::Sql => "SQL",
            Self::Rust => "RUST",
        }
    }
}

#[derive(Clone)]
struct DiscoveredMigration {
    version: ParsedVersion,
    version_text: String,
    description_display: String,
    script: String,
    checksum: Option<i32>,
    migration_type: MigrationType,
    source: MigrationSource,
}

#[derive(Clone)]
enum MigrationSource {
    SqlFile(PathBuf),
    RustFile(PathBuf),
}

#[derive(Debug, Clone)]
struct HistoryRow {
    installed_rank: i32,
    version: Option<String>,
    description: String,
    migration_type: String,
    script: String,
    checksum: Option<i32>,
    installed_on: String,
    execution_time: i32,
    success: bool,
}

impl HistoryRow {
    fn from_migration(
        migration: &DiscoveredMigration,
        execution_time: i32,
        success: bool,
        installed_rank: i32,
    ) -> Self {
        Self {
            installed_rank,
            version: Some(migration.version_text.clone()),
            description: migration.description_display.clone(),
            migration_type: migration.migration_type.as_history_type().to_owned(),
            script: migration.script.clone(),
            checksum: migration.checksum,
            installed_on: String::new(),
            execution_time,
            success,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ParsedVersion(Vec<u64>);

impl ParsedVersion {
    fn parse(value: &str) -> Result<Self, SchemalaneError> {
        validate_version(value)?;

        let mut segments = Vec::new();
        for part in value.split(['.', '_']) {
            let number = part.parse::<u64>().map_err(|_| {
                SchemalaneError::Validation(format!("invalid version segment '{}'", part))
            })?;
            segments.push(number);
        }

        Ok(Self(segments))
    }
}

pub fn format_status_table(report: &StatusReport) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "schema={}, history_table={}",
        report.schema, report.history_table
    ));
    lines.push(
        "version | description | type | script | state | rank | execution_time_ms".to_owned(),
    );
    lines.push(
        "--------|-------------|------|--------|-------|------|------------------".to_owned(),
    );

    for migration in &report.migrations {
        lines.push(format!(
            "{} | {} | {} | {} | {:?} | {} | {}",
            migration.version.as_deref().unwrap_or("-"),
            migration.description,
            migration.migration_type,
            migration.script,
            migration.state,
            migration
                .installed_rank
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            migration
                .execution_time_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_owned()),
        ));
    }

    lines.push(String::new());
    lines.push(format!(
        "summary: success={}, pending={}, failed={}, missing={}, checksum_mismatch={}",
        report.summary.success,
        report.summary.pending,
        report.summary.failed,
        report.summary.missing,
        report.summary.checksum_mismatch
    ));

    lines.join("\n")
}

pub fn should_fail_on_pending(report: &StatusReport) -> Result<(), SchemalaneError> {
    if report.summary.pending > 0 {
        Err(SchemalaneError::PendingMigrations(report.summary.pending))
    } else {
        Ok(())
    }
}

pub fn migrations_dir_exists(path: &Path) -> bool {
    path.exists()
}

#[cfg(test)]
mod tests {
    use super::{ParsedVersion, parse_rust_filename, parse_sql_filename};

    #[test]
    fn parses_sql_filename() {
        let (version, parsed, description) =
            parse_sql_filename("V2026.02.24.1__price_histories.sql").expect("valid filename");
        assert_eq!(version, "2026.02.24.1");
        assert_eq!(description, "price_histories");
        assert_eq!(
            parsed,
            ParsedVersion(vec![2026, 2, 24, 1]),
            "version segments should parse numerically"
        );
    }

    #[test]
    fn rejects_invalid_sql_filename() {
        let err = parse_sql_filename("2026_02_24_price_histories.sql")
            .expect_err("invalid filename should fail");
        assert!(
            err.to_string().contains("invalid SQL migration filename"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parses_rust_filename() {
        let (version, parsed, description) =
            parse_rust_filename("V2026.02.24.2__seed_reference_data.rs").expect("valid filename");
        assert_eq!(version, "2026.02.24.2");
        assert_eq!(description, "seed_reference_data");
        assert_eq!(
            parsed,
            ParsedVersion(vec![2026, 2, 24, 2]),
            "version segments should parse numerically"
        );
    }

    #[test]
    fn rejects_invalid_rust_filename() {
        let err = parse_rust_filename("seed_reference_data.rs")
            .expect_err("invalid filename should fail");
        assert!(
            err.to_string().contains("invalid Rust migration filename"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn compares_versions_numerically() {
        let v1 = ParsedVersion::parse("2.10").expect("parse");
        let v2 = ParsedVersion::parse("2.2").expect("parse");
        assert!(v1 > v2);
    }
}
