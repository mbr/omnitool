/// Configuration management for IMAP credentials and settings.
mod config;
/// IMAP client functionality for email operations.
mod imap;
/// DataFusion custom data source implementation.
mod imap_datasource;
/// Shell functionality for interactive command interfaces.
mod shells;

use std::sync::Arc;

use anyhow::Context;
use datafusion::prelude::SessionContext;
use structopt::StructOpt;

use crate::{config::Config, imap_datasource::AccountTableProvider};

/// Command-line interface for the omnitool IMAP email search application.
#[derive(StructOpt)]
#[structopt(name = "omnitool", about = "IMAP email search tool")]
enum Command {
    /// Login to IMAP server and save credentials
    Login {
        /// IMAP server hostname (default: imap.gmail.com)
        #[structopt(short, long, default_value = "imap.gmail.com")]
        server: String,
        /// IMAP server port (default: 993)
        #[structopt(long, default_value = "993")]
        port: u16,
    },
    /// Search emails by keyword
    Search {
        /// Search query
        query: String,
    },
    /// Start interactive IMAP shell for raw commands
    ImapShell,
    /// Start interactive IMAP shell for raw commands
    SqlShell,
    /// Execute an SQL query on mails
    ImapSql {
        /// SQL query to execute.
        command: String,
    },
}

async fn create_context(config: Arc<Config>) -> anyhow::Result<SessionContext> {
    let pool = Arc::new(imap::create_pool(config).await?);

    let ctx = SessionContext::new();
    ctx.register_table("mailboxes", Arc::new(AccountTableProvider::new(pool)))
        .context("failed to register mailboxes table")?;
    Ok(ctx)
}

/// Main entry point for the omnitool IMAP email search application.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let cmd = Command::from_args();

    match cmd {
        Command::Login { server, port } => {
            print!("Enter username: ");
            std::io::Write::flush(&mut std::io::stdout())?;
            let mut username = String::new();
            std::io::stdin().read_line(&mut username)?;
            let username = username.trim().to_string();

            print!("Enter password: ");
            std::io::Write::flush(&mut std::io::stdout())?;
            let password = rpassword::read_password()?;
            let config = config::Config {
                username,
                password,
                server,
                port,
            };

            imap::test_login(&config).await?;
            config.save()?;
            println!("Credentials saved successfully");
        }
        Command::Search { query } => {
            let results = imap::search_emails(&query).await?;
            for result in results {
                println!("{}", result);
            }
        }
        Command::ImapShell => {
            shells::imap::start().await?;
        }
        Command::SqlShell => {
            let mut prompt = shells::Prompt::new("imap")?;
            let ctx = create_context(prompt.config.clone()).await?;

            println!(
                "SQL Shell - Enter SQL query. The `mailboxes` table has a list of all mailboxes"
            );

            while let Some(sql) = prompt.read_line("SQL> ")? {
                let df = match ctx.sql(&sql).await {
                    Ok(df) => df,
                    Err(err) => {
                        eprintln!("err: {}", err);
                        continue;
                    }
                };
                df.show().await?;
            }
        }
        Command::ImapSql { command: sql } => {
            let config = Arc::new(Config::load()?);
            let ctx = create_context(config).await?;
            let df = ctx.sql(&sql).await?;
            df.show().await?;
        }
    }

    Ok(())
}
