use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use dirs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub username: String,
    pub password: String,
    pub server: String,
    pub port: u16,
}

impl Config {
    /// Get the path to the config file.
    pub fn config_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir().context("Failed to get config directory")?;
        let app_dir = config_dir.join("omnitool");
        fs::create_dir_all(&app_dir).context("Failed to create config directory")?;
        Ok(app_dir.join("config.json"))
    }

    /// Load configuration from file.
    pub fn load() -> Result<Config> {
        let path = Self::config_path()?;
        if !path.exists() {
            anyhow::bail!("No configuration found. Please run 'login' first.");
        }

        let contents = fs::read_to_string(&path).context("Failed to read config file")?;
        let config = serde_json::from_str(&contents).context("Failed to parse config file")?;
        Ok(config)
    }

    /// Save configuration to file.
    pub fn save(&self) -> Result<()> {
        let path = Self::config_path()?;
        let contents = serde_json::to_string_pretty(self).context("Failed to serialize config")?;
        fs::write(&path, contents).context("Failed to write config file")?;
        Ok(())
    }
}
