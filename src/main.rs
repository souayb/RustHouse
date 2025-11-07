use anyhow::Result;
use clap::Parser;
use rusthouse::config::Config;
use std::sync::Arc;
use tokio::signal;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "rusthouse")]
#[command(about = "A ClickHouse-like OLAP database in Rust", long_about = None)]
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level)),
        )
        .with_target(false)
        .with_thread_ids(true)
        .init();

    info!("Starting RustHouse OLAP Database");
    info!("Data directory: {}", args.data_dir);
    info!("Bind address: {}", args.bind);

    // Load configuration
    let config = Config::new(args.data_dir.clone(), args.bind.clone());
    let config = Arc::new(config);

    info!("Configuration loaded successfully");

    // Create data directory if it doesn't exist
    tokio::fs::create_dir_all(&args.data_dir).await?;

    // Start HTTP server
    let server_handle = tokio::spawn({
        let config = Arc::clone(&config);
        async move {
            if let Err(e) = start_server(config).await {
                warn!("Server error: {}", e);
            }
        }
    });

    // Start background workers
    let compaction_handle = tokio::spawn({
        let config = Arc::clone(&config);
        async move {
            if let Err(e) = start_background_workers(config).await {
                warn!("Background worker error: {}", e);
            }
        }
    });

    // Wait for shutdown signal
    match signal::ctrl_c().await {
        Ok(()) => {
            info!("Received shutdown signal, gracefully shutting down...");
        }
        Err(err) => {
            warn!("Unable to listen for shutdown signal: {}", err);
        }
    }

    // Wait for tasks to complete
    let _ = tokio::join!(server_handle, compaction_handle);

    info!("RustHouse shutdown complete");
    Ok(())
}

async fn start_server(config: Arc<Config>) -> Result<()> {
    use axum::{routing::get, Router};
    use std::net::SocketAddr;

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/", get(root_handler))
        .with_state(config.clone());

    let addr: SocketAddr = config.bind_address.parse()?;
    info!("HTTP server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn start_background_workers(_config: Arc<Config>) -> Result<()> {
    info!("Background workers started");
    // Workers will be implemented in later steps
    tokio::time::sleep(tokio::time::Duration::from_secs(u64::MAX)).await;
    Ok(())
}

async fn health_check() -> &'static str {
    "OK"
}

async fn root_handler() -> &'static str {
    "RustHouse OLAP Database v0.1.0"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parsing() {
        let args = Args::parse_from(&["rusthouse", "--bind", "127.0.0.1:9000"]);
        assert_eq!(args.bind, "127.0.0.1:9000");
        assert_eq!(args.data_dir, "./data");
    }

    #[tokio::test]
    async fn test_config_initialization() {
        let config = Config::new("./test_data".to_string(), "127.0.0.1:8123".to_string());
        assert_eq!(config.data_dir, "./test_data");
        assert_eq!(config.bind_address, "127.0.0.1:8123");
    }
}
