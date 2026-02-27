use clap::{Args, Parser, Subcommand, ValueEnum};
use schemalane::{
    SchemalaneConfig, SchemalaneError, SchemalaneMigrator, format_status_table,
    init_migration_project, should_fail_on_pending,
};
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use std::ffi::OsString;
use std::path::PathBuf;

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
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    #[arg(long, default_value = "public")]
    schema: String,

    #[arg(long, default_value = "./migrations")]
    dir: PathBuf,

    #[arg(long, default_value = "flyway_schema_history")]
    history_table: String,

    #[arg(long)]
    installed_by: Option<String>,

    #[command(subcommand)]
    command: MigrateCommand,
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
        database_url,
        schema,
        dir,
        history_table,
        installed_by,
        command,
    } = args;

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
                migrations_dir: dir,
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
