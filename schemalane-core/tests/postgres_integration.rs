use schemalane_core::{
    MigrationState, RustMigrationExecutor, RustTransactionMode, SchemalaneConfig, SchemalaneError,
    SchemalaneMigrator,
};
use sea_orm::{ConnectionTrait, Database, DbBackend, DbErr, Statement};
use sea_orm_migration::SchemaManager;
use std::error::Error;
use std::fs;
use std::path::Path;
use tempfile::TempDir;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::SyncRunner};

#[test]
#[ignore = "requires Docker daemon"]
fn up_and_status_with_sql_migrations() -> Result<(), Box<dyn Error + 'static>> {
    let node = Postgres::default().start()?;
    let db_url = connection_string(&node)?;

    let temp = TempDir::new()?;
    let migrations_dir = temp.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;

    write_migration(
        &migrations_dir,
        "V1__create_cake.sql",
        r#"
CREATE TABLE cake (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO cake(name) VALUES ('chocolate');
"#,
    )?;

    write_migration(
        &migrations_dir,
        "V2__create_price_histories.sql",
        r#"
CREATE TABLE price_histories (
    id SERIAL PRIMARY KEY,
    asset TEXT NOT NULL,
    price NUMERIC NOT NULL
);
"#,
    )?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let db = Database::connect(&db_url).await?;

        let migrator = SchemalaneMigrator::new(SchemalaneConfig {
            migrations_dir,
            ..Default::default()
        });

        let up_report = migrator.up(&db).await?;
        assert_eq!(up_report.applied.len(), 2);
        assert_eq!(up_report.skipped, 0);

        let second_up_report = migrator.up(&db).await?;
        assert_eq!(second_up_report.applied.len(), 0);
        assert_eq!(second_up_report.skipped, 2);

        let status = migrator.status(&db).await?;
        assert_eq!(status.summary.success, 2);
        assert_eq!(status.summary.pending, 0);
        assert_eq!(status.summary.failed, 0);
        assert_eq!(status.summary.missing, 0);
        assert_eq!(status.summary.checksum_mismatch, 0);

        let history_count = scalar_i64(
            &db,
            "SELECT COUNT(*) AS count FROM public.flyway_schema_history",
        )
        .await?;
        assert_eq!(history_count, 2);

        let cake_count = scalar_i64(&db, "SELECT COUNT(*) AS count FROM public.cake").await?;
        assert_eq!(cake_count, 1);

        Ok::<(), Box<dyn Error + 'static>>(())
    })?;

    Ok(())
}

#[test]
#[ignore = "requires Docker daemon"]
fn fresh_recreates_schema() -> Result<(), Box<dyn Error + 'static>> {
    let node = Postgres::default().start()?;
    let db_url = connection_string(&node)?;

    let temp = TempDir::new()?;
    let migrations_dir = temp.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;

    write_migration(
        &migrations_dir,
        "V1__create_cake.sql",
        r#"
CREATE TABLE cake (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
"#,
    )?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let db = Database::connect(&db_url).await?;

        let migrator = SchemalaneMigrator::new(SchemalaneConfig {
            migrations_dir,
            ..Default::default()
        });

        migrator.up(&db).await?;
        db.execute_unprepared("INSERT INTO public.cake(name) VALUES ('temp-row')")
            .await?;

        let before = scalar_i64(&db, "SELECT COUNT(*) AS count FROM public.cake").await?;
        assert_eq!(before, 1);

        let fresh_report = migrator.fresh(&db, true).await?;
        assert_eq!(fresh_report.applied.len(), 1);

        let after = scalar_i64(&db, "SELECT COUNT(*) AS count FROM public.cake").await?;
        assert_eq!(after, 0);

        let history_count = scalar_i64(
            &db,
            "SELECT COUNT(*) AS count FROM public.flyway_schema_history",
        )
        .await?;
        assert_eq!(history_count, 1);

        Ok::<(), Box<dyn Error + 'static>>(())
    })?;

    Ok(())
}

#[test]
#[ignore = "requires Docker daemon"]
fn status_detects_checksum_mismatch() -> Result<(), Box<dyn Error + 'static>> {
    let node = Postgres::default().start()?;
    let db_url = connection_string(&node)?;

    let temp = TempDir::new()?;
    let migrations_dir = temp.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;

    let migration_path = migrations_dir.join("V1__create_cake.sql");
    fs::write(
        &migration_path,
        r#"
CREATE TABLE cake (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
"#,
    )?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let db = Database::connect(&db_url).await?;

        let migrator = SchemalaneMigrator::new(SchemalaneConfig {
            migrations_dir,
            ..Default::default()
        });

        migrator.up(&db).await?;

        fs::write(
            &migration_path,
            r#"
CREATE TABLE cake (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    note TEXT
);
"#,
        )?;

        let status = migrator.status(&db).await?;
        assert_eq!(status.summary.checksum_mismatch, 1);
        assert_eq!(status.summary.success, 0);

        let mismatch_entry = status
            .migrations
            .iter()
            .find(|entry| entry.script == "V1__create_cake.sql")
            .ok_or_else(|| "expected migration entry".to_string())?;

        assert_eq!(mismatch_entry.state, MigrationState::ChecksumMismatch);

        Ok::<(), Box<dyn Error + 'static>>(())
    })?;

    Ok(())
}

#[test]
#[ignore = "requires Docker daemon"]
fn rust_migration_success_and_history_type() -> Result<(), Box<dyn Error + 'static>> {
    let node = Postgres::default().start()?;
    let db_url = connection_string(&node)?;

    let temp = TempDir::new()?;
    let migrations_dir = temp.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;
    write_rust_migration(&migrations_dir, "V1__create_rust_records.rs")?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let db = Database::connect(&db_url).await?;

        let mut migrator = SchemalaneMigrator::new(SchemalaneConfig {
            migrations_dir,
            ..Default::default()
        });
        migrator.register_rust_migration(
            "V1__create_rust_records.rs",
            RustMigrationExecutor::new(|manager| Box::pin(create_rust_records(manager))),
        );

        let report = migrator.up(&db).await?;
        assert_eq!(report.applied.len(), 1);
        assert_eq!(report.applied[0].migration_type, "RUST");
        assert_eq!(report.applied[0].script, "V1__create_rust_records.rs");

        let row_count = scalar_i64(
            &db,
            "SELECT COUNT(*) AS count FROM public.rust_records WHERE name = 'from-rust'",
        )
        .await?;
        assert_eq!(row_count, 1);

        let history_row = db
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT \"type\", \"script\", \"success\" FROM public.flyway_schema_history ORDER BY \"installed_rank\" LIMIT 1".to_owned(),
            ))
            .await?
            .ok_or_else(|| DbErr::Custom("expected history row".to_owned()))?;

        let migration_type: String = history_row.try_get("", "type")?;
        let script: String = history_row.try_get("", "script")?;
        let success: bool = history_row.try_get("", "success")?;
        assert_eq!(migration_type, "RUST");
        assert_eq!(script, "V1__create_rust_records.rs");
        assert!(success);

        let status = migrator.status(&db).await?;
        assert_eq!(status.summary.success, 1);
        assert_eq!(status.summary.failed, 0);

        Ok::<(), Box<dyn Error + 'static>>(())
    })?;

    Ok(())
}

#[test]
#[ignore = "requires Docker daemon"]
fn rust_migration_transaction_mode_rolls_back_on_failure() -> Result<(), Box<dyn Error + 'static>> {
    let node = Postgres::default().start()?;
    let db_url = connection_string(&node)?;

    let temp = TempDir::new()?;
    let migrations_dir = temp.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;
    write_rust_migration(&migrations_dir, "V2__rust_tx_failure.rs")?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let db = Database::connect(&db_url).await?;

        let mut migrator = SchemalaneMigrator::new(SchemalaneConfig {
            migrations_dir,
            ..Default::default()
        });
        migrator.register_rust_migration(
            "V2__rust_tx_failure.rs",
            RustMigrationExecutor::transactional(|manager| {
                Box::pin(fail_after_insert(manager, "rust_tx_failure_items"))
            }),
        );

        let err = migrator.up(&db).await.expect_err("migration should fail");
        assert!(
            matches!(err, SchemalaneError::MigrationExecution { .. }),
            "expected MigrationExecution, got: {err}"
        );

        let exists = table_exists(&db, "rust_tx_failure_items").await?;
        assert!(
            !exists,
            "transactional failure should roll back table creation"
        );

        let status = migrator.status(&db).await?;
        assert_eq!(status.summary.failed, 1);

        let failed_entry = status
            .migrations
            .iter()
            .find(|entry| entry.script == "V2__rust_tx_failure.rs")
            .ok_or_else(|| "expected failed rust migration entry".to_string())?;
        assert_eq!(failed_entry.state, MigrationState::Failed);

        Ok::<(), Box<dyn Error + 'static>>(())
    })?;

    Ok(())
}

#[test]
#[ignore = "requires Docker daemon"]
fn rust_migration_no_transaction_mode_persists_partial_work_on_failure()
-> Result<(), Box<dyn Error + 'static>> {
    let node = Postgres::default().start()?;
    let db_url = connection_string(&node)?;

    let temp = TempDir::new()?;
    let migrations_dir = temp.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;
    write_rust_migration(&migrations_dir, "V3__rust_no_tx_failure.rs")?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let db = Database::connect(&db_url).await?;

        let mut migrator = SchemalaneMigrator::new(SchemalaneConfig {
            migrations_dir,
            ..Default::default()
        });
        migrator.register_rust_migration(
            "V3__rust_no_tx_failure.rs",
            RustMigrationExecutor::with_mode(RustTransactionMode::NoTransaction, |manager| {
                Box::pin(fail_after_insert(manager, "rust_no_tx_failure_items"))
            }),
        );

        let err = migrator.up(&db).await.expect_err("migration should fail");
        assert!(
            matches!(err, SchemalaneError::MigrationExecution { .. }),
            "expected MigrationExecution, got: {err}"
        );

        let exists = table_exists(&db, "rust_no_tx_failure_items").await?;
        assert!(
            exists,
            "non-transactional failure should keep created table"
        );

        let row_count = scalar_i64(
            &db,
            "SELECT COUNT(*) AS count FROM public.rust_no_tx_failure_items",
        )
        .await?;
        assert_eq!(row_count, 1);

        let status = migrator.status(&db).await?;
        assert_eq!(status.summary.failed, 1);

        let failed_entry = status
            .migrations
            .iter()
            .find(|entry| entry.script == "V3__rust_no_tx_failure.rs")
            .ok_or_else(|| "expected failed rust migration entry".to_string())?;
        assert_eq!(failed_entry.state, MigrationState::Failed);

        Ok::<(), Box<dyn Error + 'static>>(())
    })?;

    Ok(())
}

#[test]
#[ignore = "requires Docker daemon"]
fn rust_migration_requires_registered_executor() -> Result<(), Box<dyn Error + 'static>> {
    let node = Postgres::default().start()?;
    let db_url = connection_string(&node)?;

    let temp = TempDir::new()?;
    let migrations_dir = temp.path().join("migrations");
    fs::create_dir_all(&migrations_dir)?;
    write_rust_migration(&migrations_dir, "V9__missing_executor.rs")?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let db = Database::connect(&db_url).await?;

        let migrator = SchemalaneMigrator::new(SchemalaneConfig {
            migrations_dir,
            ..Default::default()
        });

        let err = migrator.up(&db).await.expect_err("expected validation error");
        assert!(
            matches!(err, SchemalaneError::Validation(ref message) if message.contains("missing Rust migration executor")),
            "expected Validation for missing Rust migration executor, got: {err}"
        );

        Ok::<(), Box<dyn Error + 'static>>(())
    })?;

    Ok(())
}

fn connection_string(
    node: &testcontainers_modules::testcontainers::core::Container<Postgres>,
) -> Result<String, Box<dyn Error + 'static>> {
    let host = node.get_host()?;
    let port = node.get_host_port_ipv4(5432)?;
    Ok(format!(
        "postgres://postgres:postgres@{host}:{port}/postgres"
    ))
}

fn write_migration(
    migrations_dir: &Path,
    file_name: &str,
    sql: &str,
) -> Result<(), std::io::Error> {
    let path = migrations_dir.join(file_name);
    fs::write(path, sql)
}

fn write_rust_migration(migrations_dir: &Path, file_name: &str) -> Result<(), std::io::Error> {
    let path = migrations_dir.join(file_name);
    fs::write(
        path,
        r#"
use sea_orm::DbErr;
use sea_orm_migration::SchemaManager;

pub async fn migration(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let _ = manager;
    Ok(())
}
"#,
    )
}

async fn scalar_i64(db: &sea_orm::DatabaseConnection, sql: &str) -> Result<i64, sea_orm::DbErr> {
    let stmt = Statement::from_string(DbBackend::Postgres, sql.to_owned());
    let row = db
        .query_one_raw(stmt)
        .await?
        .ok_or_else(|| sea_orm::DbErr::Custom("expected scalar row".to_owned()))?;
    row.try_get("", "count")
}

async fn table_exists(
    db: &sea_orm::DatabaseConnection,
    table: &str,
) -> Result<bool, sea_orm::DbErr> {
    let stmt = Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT to_regclass($1) IS NOT NULL AS exists",
        [format!("public.{table}").into()],
    );
    let row = db
        .query_one_raw(stmt)
        .await?
        .ok_or_else(|| sea_orm::DbErr::Custom("expected table_exists row".to_owned()))?;
    row.try_get("", "exists")
}

async fn create_rust_records(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let db = manager.get_connection();
    db.execute_unprepared(
        r#"
CREATE TABLE rust_records (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO rust_records(name) VALUES ('from-rust');
"#,
    )
    .await?;

    Ok(())
}

async fn fail_after_insert(manager: &SchemaManager<'_>, table_name: &str) -> Result<(), DbErr> {
    let db = manager.get_connection();
    db.execute_unprepared(&format!(
        "CREATE TABLE {} (id SERIAL PRIMARY KEY, note TEXT NOT NULL);",
        table_name
    ))
    .await?;
    db.execute_unprepared(&format!(
        "INSERT INTO {}(note) VALUES ('partial-write');",
        table_name
    ))
    .await?;

    Err(DbErr::Custom(
        "intentional rust migration failure".to_owned(),
    ))
}
