pub mod imap;

use std::path::PathBuf;

use anyhow::Context;
use directories::ProjectDirs;
use rustyline::DefaultEditor;

use crate::config::Config;

/// Get the path to a shell-specific history file.
fn history_path(shell_name: &str) -> anyhow::Result<PathBuf> {
    let project_dirs =
        ProjectDirs::from("", "", "omnitool").context("Failed to get project directories")?;
    let data_dir = project_dirs.data_dir();
    std::fs::create_dir_all(data_dir)?;
    Ok(data_dir.join(format!("{}_history.txt", shell_name)))
}

/// A prompt manager that handles configuration, history, and user input for shells.
pub struct Prompt {
    /// The shell configuration.
    pub config: std::sync::Arc<Config>,
    /// The readline editor.
    editor: DefaultEditor,
    /// The path to the history file.
    history_file: PathBuf,
}

impl Prompt {
    /// Initialize a new prompt with configuration and history for a specific shell.
    pub fn new(shell_name: &str) -> anyhow::Result<Self> {
        let config = Config::load()?;

        let mut editor = DefaultEditor::new()
            .map_err(|e| anyhow::anyhow!("Failed to create readline editor: {}", e))?;

        // Get history file path
        let history_file = history_path(shell_name)?;

        // Load existing history if available
        if history_file.exists() {
            if let Err(e) = editor.load_history(&history_file) {
                eprintln!("Warning: Failed to load history: {}", e);
            }
        }

        Ok(Self {
            config: std::sync::Arc::new(config),
            editor,
            history_file,
        })
    }

    /// Read a line from the user.
    ///
    /// Returns a trimmed line or `None` on EOF.
    pub fn read_line(&mut self, prompt_str: &str) -> anyhow::Result<Option<String>> {
        loop {
            let readline = self.editor.readline(prompt_str);
            return match readline {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() {
                        // Empty line - continue prompting
                        continue;
                    }

                    // Add line to history
                    self.editor
                        .add_history_entry(line)
                        .map_err(|e| anyhow::anyhow!("Failed to add history entry: {}", e))?;

                    // Save history after each command
                    if let Err(e) = self.editor.save_history(&self.history_file) {
                        eprintln!("Warning: Failed to save history: {}", e);
                    }

                    Ok(Some(line.to_string()))
                }
                Err(rustyline::error::ReadlineError::Interrupted)
                | Err(rustyline::error::ReadlineError::Eof) => {
                    // User wants to exit - return None
                    Ok(None)
                }
                Err(e) => {
                    // Actual error
                    Err(anyhow::anyhow!("Error reading line: {}", e))
                }
            };
        }
    }
}
