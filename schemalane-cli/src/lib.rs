use clap::{Args, Parser, Subcommand, ValueEnum};
use schemalane_core::{
    SchemalaneConfig, SchemalaneError, SchemalaneMigrator, format_status_table,
    init_migration_project, should_fail_on_pending,
};
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

const DEFAULT_MIGRATION_DIR: &str = "./migration";
const DEFAULT_SQL_DIR: &str = "./migrations";

pub struct EmbeddedRunner {
    migrations_dir: &'static str,
    build_migrator: fn(SchemalaneConfig) -> SchemalaneMigrator,
}

impl EmbeddedRunner {
    pub fn new(
        migrations_dir: &'static str,
        build_migrator: fn(SchemalaneConfig) -> SchemalaneMigrator,
    ) -> Self {
        Self {
            migrations_dir,
            build_migrator,
        }
    }

    pub async fn run(self) {
        if let Err(err) = self.run_with(std::env::args_os()).await {
            eprintln!("{err}");
            std::process::exit(err.exit_code());
        }
    }

    pub async fn run_with<I, T>(self, args: I) -> Result<(), SchemalaneError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let cli = EmbeddedCli::parse_from(args);

        let db = connect(&cli.database_url).await?;
        let migrations_dir = cli
            .dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(self.migrations_dir));

        let config = SchemalaneConfig {
            schema: cli.schema,
            history_table: cli.history_table,
            migrations_dir,
            installed_by: cli.installed_by,
            ..Default::default()
        };

        let migrator = (self.build_migrator)(config);
        run_db_command(&migrator, &db, cli.command.into()).await
    }
}

pub async fn run_cli() {
    if let Err(err) = run_cli_with(std::env::args_os()).await {
        eprintln!("{err}");
        std::process::exit(err.exit_code());
    }
}

pub async fn run_cli_with<I, T>(args: I) -> Result<(), SchemalaneError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::parse_from(args);
    run_root_cli(cli).await
}

#[derive(Debug, Parser)]
#[command(name = "schemalane")]
#[command(about = "Schemalane migration toolkit")]
struct Cli {
    #[command(subcommand)]
    command: RootCommand,
}

#[derive(Debug, Subcommand)]
enum RootCommand {
    Migrate(MigrateArgs),
}

#[derive(Debug, Args)]
struct MigrateArgs {
    /// Migration script directory.
    ///
    /// If your migrations are in their own crate,
    /// provide the root of that crate.
    /// If your migrations are in a submodule of your app,
    /// provide the directory of that submodule.
    #[arg(
        short = 'd',
        long = "migration-dir",
        env = "MIGRATION_DIR",
        default_value = DEFAULT_MIGRATION_DIR
    )]
    migration_dir: PathBuf,

    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    #[arg(long, default_value = "public")]
    schema: String,

    #[arg(long, default_value = "flyway_schema_history")]
    history_table: String,

    #[arg(long)]
    installed_by: Option<String>,

    #[command(subcommand)]
    command: Option<MigrateCommand>,
}

#[derive(Debug, Subcommand)]
enum MigrateCommand {
    Init {
        #[arg(long, default_value = "./migration")]
        path: PathBuf,

        #[arg(long)]
        force: bool,
    },
    Up,
    Status {
        #[arg(long, value_enum, default_value_t = StatusFormat::Table)]
        format: StatusFormat,

        #[arg(long)]
        fail_on_pending: bool,
    },
    Fresh {
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Parser)]
struct EmbeddedCli {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[arg(long, default_value = "public")]
    schema: String,

    #[arg(long, default_value = "flyway_schema_history")]
    history_table: String,

    #[arg(long)]
    installed_by: Option<String>,

    #[arg(long)]
    dir: Option<PathBuf>,

    #[command(subcommand)]
    command: EmbeddedCommand,
}

#[derive(Debug, Subcommand)]
enum EmbeddedCommand {
    Up,
    Status {
        #[arg(long, value_enum, default_value_t = StatusFormat::Table)]
        format: StatusFormat,

        #[arg(long)]
        fail_on_pending: bool,
    },
    Fresh {
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StatusFormat {
    Table,
    Json,
}

enum DbCommand {
    Up,
    Status {
        format: StatusFormat,
        fail_on_pending: bool,
    },
    Fresh {
        yes: bool,
    },
}

impl From<EmbeddedCommand> for DbCommand {
    fn from(command: EmbeddedCommand) -> Self {
        match command {
            EmbeddedCommand::Up => Self::Up,
            EmbeddedCommand::Status {
                format,
                fail_on_pending,
            } => Self::Status {
                format,
                fail_on_pending,
            },
            EmbeddedCommand::Fresh { yes } => Self::Fresh { yes },
        }
    }
}

async fn run_root_cli(cli: Cli) -> Result<(), SchemalaneError> {
    let RootCommand::Migrate(args) = cli.command;
    let MigrateArgs {
        migration_dir,
        database_url,
        schema,
        history_table,
        installed_by,
        command,
    } = args;
    let command = command.unwrap_or(MigrateCommand::Up);

    match command {
        MigrateCommand::Init { path, force } => {
            let report = init_migration_project(&path, force)?;
            println!("Initialized migration crate at {}", report.root.display());
            println!(
                "Created {} file(s), overwritten {} file(s).",
                report.created.len(),
                report.overwritten.len()
            );
            println!("Run migrations via:");
            println!(
                "cargo run --manifest-path {}/Cargo.toml -- --database-url \"$DATABASE_URL\" up",
                report.root.display()
            );
            Ok(())
        }
        command => {
            let manifest_path = migration_dir.join("Cargo.toml");
            if manifest_path.is_file() {
                return run_via_migration_crate(
                    &manifest_path,
                    database_url.as_deref(),
                    &schema,
                    &history_table,
                    installed_by.as_deref(),
                    command,
                );
            }
            if migration_dir != PathBuf::from(DEFAULT_MIGRATION_DIR) {
                return Err(SchemalaneError::Validation(format!(
                    "migration crate manifest not found: {}",
                    manifest_path.display()
                )));
            }

            let database_url = database_url.ok_or_else(|| {
                SchemalaneError::Validation(
                    "--database-url (or DATABASE_URL env var) is required for this command"
                        .to_owned(),
                )
            })?;

            let db = connect(&database_url).await?;

            let config = SchemalaneConfig {
                schema,
                history_table,
                migrations_dir: PathBuf::from(DEFAULT_SQL_DIR),
                installed_by,
                ..Default::default()
            };

            let migrator = SchemalaneMigrator::new(config);

            let db_command = match command {
                MigrateCommand::Init { .. } => unreachable!("init is handled in outer match"),
                MigrateCommand::Up => DbCommand::Up,
                MigrateCommand::Status {
                    format,
                    fail_on_pending,
                } => DbCommand::Status {
                    format,
                    fail_on_pending,
                },
                MigrateCommand::Fresh { yes } => DbCommand::Fresh { yes },
            };

            run_db_command(&migrator, &db, db_command).await
        }
    }
}

fn run_via_migration_crate(
    manifest_path: &Path,
    database_url: Option<&str>,
    schema: &str,
    history_table: &str,
    installed_by: Option<&str>,
    command: MigrateCommand,
) -> Result<(), SchemalaneError> {
    let mut cargo = Command::new("cargo");
    cargo
        .arg("run")
        .arg("--manifest-path")
        .arg(manifest_path)
        .arg("--");

    if let Some(database_url) = database_url {
        cargo.arg("--database-url").arg(database_url);
    }

    cargo
        .arg("--schema")
        .arg(schema)
        .arg("--history-table")
        .arg(history_table);

    if let Some(installed_by) = installed_by {
        cargo.arg("--installed-by").arg(installed_by);
    }

    match command {
        MigrateCommand::Init { .. } => unreachable!("init is handled in outer match"),
        MigrateCommand::Up => {
            cargo.arg("up");
        }
        MigrateCommand::Status {
            format,
            fail_on_pending,
        } => {
            cargo.arg("status");
            cargo.arg("--format").arg(match format {
                StatusFormat::Table => "table",
                StatusFormat::Json => "json",
            });
            if fail_on_pending {
                cargo.arg("--fail-on-pending");
            }
        }
        MigrateCommand::Fresh { yes } => {
            cargo.arg("fresh");
            if yes {
                cargo.arg("--yes");
            }
        }
    }

    let status = cargo.status().map_err(|err| {
        SchemalaneError::Validation(format!(
            "failed to run cargo for migration crate {}: {err}",
            manifest_path.display()
        ))
    })?;

    if status.success() {
        Ok(())
    } else {
        Err(SchemalaneError::Validation(format!(
            "migration crate command failed for {} with status {status}",
            manifest_path.display()
        )))
    }
}

async fn connect(database_url: &str) -> Result<DatabaseConnection, SchemalaneError> {
    let mut connect_opts = ConnectOptions::new(database_url.to_owned());
    connect_opts.max_connections(5);
    connect_opts.min_connections(1);
    Database::connect(connect_opts)
        .await
        .map_err(SchemalaneError::from)
}

async fn run_db_command(
    migrator: &SchemalaneMigrator,
    db: &DatabaseConnection,
    command: DbCommand,
) -> Result<(), SchemalaneError> {
    match command {
        DbCommand::Up => {
            let report = migrator.up(db).await?;
            println!(
                "Applied {} migration(s), skipped {}.",
                report.applied.len(),
                report.skipped
            );
            for applied in report.applied {
                println!(
                    "- V{} {} ({}) [{} ms]",
                    applied.version, applied.description, applied.script, applied.execution_time_ms
                );
            }
        }
        DbCommand::Status {
            format,
            fail_on_pending,
        } => {
            let report = migrator.status(db).await?;
            match format {
                StatusFormat::Table => println!("{}", format_status_table(&report)),
                StatusFormat::Json => println!(
                    "{}",
                    serde_json::to_string_pretty(&report).map_err(|err| {
                        SchemalaneError::Validation(format!("failed to encode JSON: {err}"))
                    })?
                ),
            }
            if fail_on_pending {
                should_fail_on_pending(&report)?;
            }
        }
        DbCommand::Fresh { yes } => {
            let report = migrator.fresh(db, yes).await?;
            println!(
                "Fresh completed. Applied {} migration(s).",
                report.applied.len()
            );
            for applied in report.applied {
                println!(
                    "- V{} {} ({}) [{} ms]",
                    applied.version, applied.description, applied.script, applied.execution_time_ms
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Cli, DEFAULT_MIGRATION_DIR, MigrateCommand, RootCommand};
    use clap::Parser;
    use std::path::PathBuf;

    #[test]
    fn parse_short_migration_dir_flag() {
        let cli = Cli::try_parse_from(["schemalane", "migrate", "-d", "test2/migration", "up"])
            .expect("CLI args should parse");
        let RootCommand::Migrate(args) = cli.command;
        assert_eq!(args.migration_dir, PathBuf::from("test2/migration"));
        assert!(matches!(args.command, Some(MigrateCommand::Up)));
    }

    #[test]
    fn parse_default_migration_dir() {
        let cli = Cli::try_parse_from(["schemalane", "migrate", "status"])
            .expect("CLI args should parse");
        let RootCommand::Migrate(args) = cli.command;
        assert_eq!(args.migration_dir, PathBuf::from(DEFAULT_MIGRATION_DIR));
        assert!(matches!(args.command, Some(MigrateCommand::Status { .. })));
    }

    #[test]
    fn parse_migrate_without_subcommand() {
        let cli = Cli::try_parse_from(["schemalane", "migrate"]).expect("CLI args should parse");
        let RootCommand::Migrate(args) = cli.command;
        assert_eq!(args.migration_dir, PathBuf::from(DEFAULT_MIGRATION_DIR));
        assert!(args.command.is_none(), "no subcommand means implicit up");
    }
}
