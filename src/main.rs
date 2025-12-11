/// Configuration management for IMAP credentials and settings.
mod config;
/// IMAP client functionality for email operations.
mod imap;
/// DataFusion custom data source implementation.
mod my_data;
/// Interactive IMAP shell functionality.
mod shell;

use std::sync::Arc;

use anyhow::Context;
use datafusion::prelude::SessionContext;
use structopt::StructOpt;

use crate::my_data::MyDataSource;

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
    Shell,
    /// Datafusion test
    DfTest,
}

/// Main entry point for the omnitool IMAP email search application.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
        Command::Shell => {
            shell::start().await?;
        }
        Command::DfTest => {
            let ctx = SessionContext::new();
            ctx.register_table("mailboxes", Arc::new(MyDataSource {}))
                .context("failed to register mailboxes table")?;
            let df = ctx
                .sql("SELECT * FROM mailboxes;")
                .await
                .expect("query failed");
            df.show().await?;
        }
    }

    Ok(())
}
