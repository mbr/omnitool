pub mod imap;

use std::path::PathBuf;

use anyhow::Context;
use directories::ProjectDirs;
use rustyline::DefaultEditor;
use structopt::StructOpt;

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
    /// The shell name for display purposes.
    shell_name: String,
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
            shell_name: shell_name.to_string(),
        })
    }

    /// Read and parse a command from the user.
    ///
    /// Returns:
    /// - `Ok(Some(command))` - Successfully parsed command
    /// - `Ok(None)` - User wants to exit (Ctrl+C/Ctrl+D) or empty input that should retry
    /// - `Err(...)` - Actual error occurred
    pub async fn read_command<T>(&mut self, prompt_str: &str) -> anyhow::Result<Option<T>>
    where
        T: StructOpt,
    {
        loop {
            let readline = self.editor.readline(prompt_str);
            match readline {
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

                    let args = match shlex::split(line) {
                        Some(args) => args,
                        None => {
                            println!("Error: Invalid shell syntax");
                            // Continue prompting on parse error
                            continue;
                        }
                    };

                    // Add a dummy arg0 for structopt parsing
                    let args_with_arg0 =
                        std::iter::once(format!("{}_cmd", self.shell_name)).chain(args.into_iter());

                    match T::from_iter_safe(args_with_arg0) {
                        Ok(command) => return Ok(Some(command)),
                        Err(e) => {
                            println!("Error parsing command: {}", e);
                            // Continue prompting on parse error
                            continue;
                        }
                    }
                }
                Err(rustyline::error::ReadlineError::Interrupted)
                | Err(rustyline::error::ReadlineError::Eof) => {
                    // User wants to exit - return None
                    return Ok(None);
                }
                Err(e) => {
                    // Actual error
                    return Err(anyhow::anyhow!("Error reading line: {}", e));
                }
            }
        }
    }
}
