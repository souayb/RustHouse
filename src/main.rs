use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rusthouse::auth::StaticFileAuthenticator;
use rusthouse::catalog::Catalog;
use rusthouse::config::Config;
use rusthouse::governance::audit::{self, AuditSink, ExportFilter, FileAuditSink};
use rusthouse::http_api::{create_router, AppState};
use rusthouse::session::Session;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::signal;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "rusthouse")]
#[command(about = "RustHouse: a hybrid Lakehouse with bank-grade governance", long_about = None)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "config.json")]
    config: String,

    /// HTTP server bind address
    #[arg(short, long, default_value = "0.0.0.0:8123")]
    bind: String,

    /// Data directory
    #[arg(short, long, default_value = "./data")]
    data_dir: String,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Static authenticator JSON file (API-SQL-002). Without it every request is 401.
    #[arg(long)]
    auth_file: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the HTTP server (default)
    Serve,
    /// Audit log tooling (GOV-AUD-006, GOV-AUD-010)
    Audit {
        #[command(subcommand)]
        cmd: AuditCmd,
    },
}

#[derive(Subcommand, Debug)]
enum AuditCmd {
    /// Walk the hash chain and report the first broken record
    Verify {
        #[arg(long)]
        from: Option<u64>,
    },
    /// Export records as JSONL plus a manifest for independent verification
    Export {
        /// RFC 3339 lower bound (inclusive)
        #[arg(long)]
        from: Option<String>,
        /// RFC 3339 upper bound (inclusive)
        #[arg(long)]
        to: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        table: Option<String>,
        /// Output file (stdout if omitted); the manifest goes to `<out>.manifest.json`
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level)),
        )
        .with_target(false)
        .with_thread_ids(true)
        .init();

    let mut config = Config::new(args.data_dir.clone(), args.bind.clone());
    config.auth_file = args.auth_file.clone();

    match args.command.unwrap_or(Command::Serve) {
        Command::Serve => serve(Arc::new(config)).await,
        Command::Audit { cmd } => run_audit(&config, cmd),
    }
}

async fn serve(config: Arc<Config>) -> Result<()> {
    info!("Starting RustHouse");
    info!("Data directory: {}", config.data_dir);
    info!("Bind address: {}", config.bind_address);

    tokio::fs::create_dir_all(&config.data_dir).await?;
    let catalog = Arc::new(
        Catalog::open(&config.data_dir)
            .await
            .context("opening catalog")?,
    );
    let audit: Arc<dyn AuditSink> = Arc::new(
        FileAuditSink::open(Path::new(&config.audit_path())).context("opening audit log")?,
    );
    let session = Arc::new(Session::new(catalog, audit));
    let authenticator = match &config.auth_file {
        Some(p) => {
            Arc::new(StaticFileAuthenticator::load(Path::new(p)).context("loading auth file")?)
        }
        None => {
            warn!("no --auth-file given: every request will be rejected with 401");
            Arc::new(StaticFileAuthenticator::deny_all())
        }
    };
    let state = AppState {
        session,
        authenticator,
        config: config.clone(),
    };

    let app = create_router(state);
    let addr: std::net::SocketAddr = config.bind_address.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("HTTP server listening on {}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            if let Err(e) = signal::ctrl_c().await {
                warn!("Unable to listen for shutdown signal: {}", e);
            }
            info!("Received shutdown signal, gracefully shutting down...");
        })
        .await?;
    info!("RustHouse shutdown complete");
    Ok(())
}

fn run_audit(config: &Config, cmd: AuditCmd) -> Result<()> {
    let root = PathBuf::from(config.audit_path());
    match cmd {
        AuditCmd::Verify { from } => {
            let report = audit::verify(&root, from)?;
            if report.ok {
                println!("audit chain OK: {} records", report.records);
                Ok(())
            } else {
                eprintln!(
                    "audit chain BROKEN at seq {}: {}",
                    report
                        .first_bad_seq
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "?".into()),
                    report.reason.unwrap_or_default()
                );
                std::process::exit(1)
            }
        }
        AuditCmd::Export {
            from,
            to,
            principal,
            table,
            out,
        } => {
            let parse = |s: Option<String>| -> Result<Option<chrono::DateTime<chrono::Utc>>> {
                Ok(match s {
                    Some(s) => {
                        Some(chrono::DateTime::parse_from_rfc3339(&s)?.with_timezone(&chrono::Utc))
                    }
                    None => None,
                })
            };
            let filter = ExportFilter {
                from_ts: parse(from)?,
                to_ts: parse(to)?,
                principal,
                table,
            };
            let manifest = match &out {
                Some(path) => {
                    let mut f = std::fs::File::create(path)?;
                    let m = audit::export(&root, filter, &mut f)?;
                    let mp = path.with_extension("manifest.json");
                    std::fs::write(&mp, serde_json::to_vec_pretty(&m)?)?;
                    eprintln!("wrote {} and {}", path.display(), mp.display());
                    m
                }
                None => {
                    let stdout = std::io::stdout();
                    let mut lock = stdout.lock();
                    audit::export(&root, filter, &mut lock)?
                }
            };
            eprintln!("{}", serde_json::to_string(&manifest)?);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parsing() {
        let args = Args::parse_from(["rusthouse", "--bind", "127.0.0.1:9000"]);
        assert_eq!(args.bind, "127.0.0.1:9000");
        assert_eq!(args.data_dir, "./data");
        assert!(args.command.is_none());
        let args = Args::parse_from(["rusthouse", "audit", "verify", "--from", "10"]);
        assert!(matches!(
            args.command,
            Some(Command::Audit {
                cmd: AuditCmd::Verify { from: Some(10) }
            })
        ));
    }

    #[tokio::test]
    async fn test_config_initialization() {
        let config = Config::new("./test_data".to_string(), "127.0.0.1:8123".to_string());
        assert_eq!(config.data_dir, "./test_data");
        assert_eq!(config.bind_address, "127.0.0.1:8123");
    }
}
