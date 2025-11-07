use rusthouse::config::Config;

#[tokio::test]
async fn test_basic_initialization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = Config::new(
        temp_dir.path().to_str().unwrap().to_string(),
        "127.0.0.1:8124".to_string(),
    );

    // Create directories
    tokio::fs::create_dir_all(&config.data_dir).await.unwrap();
    tokio::fs::create_dir_all(config.metadata_path())
        .await
        .unwrap();
    tokio::fs::create_dir_all(config.segments_path())
        .await
        .unwrap();

    // Verify directories exist
    assert!(tokio::fs::metadata(&config.data_dir).await.is_ok());
    assert!(tokio::fs::metadata(config.metadata_path()).await.is_ok());
    assert!(tokio::fs::metadata(config.segments_path()).await.is_ok());
}

#[tokio::test]
async fn test_health_endpoint() {
    use axum::{routing::get, Router};

    async fn health() -> &'static str {
        "OK"
    }

    let app = Router::new().route("/health", get(health));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Test health endpoint
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/health", addr))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "OK");
}