use clap::{Args, Parser, Subcommand, ValueEnum};
use schemalane::{
    SchemalaneConfig, SchemalaneError, SchemalaneMigrator, format_status_table,
    should_fail_on_pending,
};
use sea_orm::{ConnectOptions, Database};
use std::path::PathBuf;

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
    database_url: String,

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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Err(err) = run(cli).await {
        eprintln!("{err}");
        std::process::exit(err.exit_code());
    }
}

async fn run(cli: Cli) -> Result<(), SchemalaneError> {
    let RootCommand::Migrate(args) = cli.command;

    let mut connect_opts = ConnectOptions::new(args.database_url.clone());
    connect_opts.max_connections(5);
    connect_opts.min_connections(1);

    let db = Database::connect(connect_opts).await?;

    let config = SchemalaneConfig {
        schema: args.schema,
        history_table: args.history_table,
        migrations_dir: args.dir,
        installed_by: args.installed_by,
        ..Default::default()
    };

    let migrator = SchemalaneMigrator::new(config);

    match args.command {
        MigrateCommand::Up => {
            let report = migrator.up(&db).await?;
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
        MigrateCommand::Status {
            format,
            fail_on_pending,
        } => {
            let report = migrator.status(&db).await?;
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
        MigrateCommand::Fresh { yes } => {
            let report = migrator.fresh(&db, yes).await?;
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
