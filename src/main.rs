mod config;
mod imap;

use structopt::StructOpt;
use tracing::info;

/// Command-line interface for the omnitool IMAP email search application.
#[derive(StructOpt)]
#[structopt(name = "omnitool", about = "IMAP email search tool")]
enum Command {
    /// Login to IMAP server and save credentials
    Login {
        /// Username for IMAP authentication
        #[structopt(short, long)]
        username: String,
        /// App password for IMAP authentication
        #[structopt(short, long)]
        password: String,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cmd = Command::from_args();

    match cmd {
        Command::Login {
            username,
            password,
            server,
            port,
        } => {
            let config = config::Config {
                username,
                password,
                server,
                port,
            };

            info!("Testing IMAP connection before saving credentials");
            imap::test_login(&config).await?;

            config.save()?;
            info!("Credentials saved successfully");
        }
        Command::Search { query } => {
            let results = imap::search_emails(&query).await?;
            for result in results {
                println!("{}", result);
            }
        }
    }

    Ok(())
}
