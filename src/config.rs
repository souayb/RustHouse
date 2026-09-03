use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub data_dir: String,
    pub bind_address: String,
    pub max_segment_size: usize,
    pub compaction_interval_secs: u64,
    pub compression_level: i32,
    /// HTTP request body limit (API-SQL-009).
    pub max_request_bytes: usize,
    /// Static authenticator file (API-SQL-002); `None` means every request is 401.
    pub auth_file: Option<String>,
}

impl Config {
    pub fn new(data_dir: String, bind_address: String) -> Self {
        Self {
            data_dir,
            bind_address,
            max_segment_size: 1024 * 1024 * 100, // 100MB default
            compaction_interval_secs: 300,       // 5 minutes
            compression_level: 3,                // LZ4 default level
            max_request_bytes: 16 * 1024 * 1024, // 16 MiB
            auth_file: None,
        }
    }

    pub fn metadata_path(&self) -> String {
        format!("{}/metadata", self.data_dir)
    }

    pub fn segments_path(&self) -> String {
        format!("{}/segments", self.data_dir)
    }

    pub fn audit_path(&self) -> String {
        format!("{}/audit", self.data_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = Config::new("./data".to_string(), "0.0.0.0:8123".to_string());
        assert_eq!(config.data_dir, "./data");
        assert_eq!(config.bind_address, "0.0.0.0:8123");
        assert_eq!(config.max_segment_size, 1024 * 1024 * 100);
    }

    #[test]
    fn test_config_paths() {
        let config = Config::new("./data".to_string(), "0.0.0.0:8123".to_string());
        assert_eq!(config.metadata_path(), "./data/metadata");
        assert_eq!(config.segments_path(), "./data/segments");
    }
}
