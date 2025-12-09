use std::{fs, path::PathBuf};

use anyhow::Context;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

/// Configuration for IMAP connection and authentication.
#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    /// Username for IMAP authentication.
    pub username: String,
    /// App password for IMAP authentication.
    pub password: String,
    /// IMAP server hostname.
    pub server: String,
    /// IMAP server port.
    pub port: u16,
}

impl Config {
    /// Get the path to the config file.
    pub fn config_path() -> anyhow::Result<PathBuf> {
        let proj_dirs =
            ProjectDirs::from("", "", "omnitool").context("Failed to get project directories")?;
        let config_dir = proj_dirs.config_dir();
        fs::create_dir_all(config_dir).context("Failed to create config directory")?;
        Ok(config_dir.join("config.json"))
    }

    /// Load configuration from file.
    pub fn load() -> anyhow::Result<Config> {
        let path = Self::config_path()?;
        if !path.exists() {
            anyhow::bail!("No configuration found. Please run 'login' first.");
        }

        let contents = fs::read_to_string(&path).context("Failed to read config file")?;
        let config = serde_json::from_str(&contents).context("Failed to parse config file")?;
        Ok(config)
    }

    /// Save configuration to file.
    pub fn save(&self) -> anyhow::Result<()> {
        let path = Self::config_path()?;
        let contents = serde_json::to_string_pretty(self).context("Failed to serialize config")?;
        fs::write(&path, contents).context("Failed to write config file")?;
        Ok(())
    }
}
