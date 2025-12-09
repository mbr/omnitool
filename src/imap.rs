use anyhow::Context;
use async_imap::Client;
use futures::TryStreamExt;
use rustyline::{DefaultEditor, Result as RustylineResult};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::config::Config;

/// Type alias for the TLS stream used in IMAP connections.
type ImapStream = tokio_util::compat::Compat<TlsStream<TcpStream>>;
/// Type alias for an active IMAP session over a TLS connection.
type ImapSession = async_imap::Session<ImapStream>;

/// Test IMAP login with the provided configuration.
pub async fn test_login(config: &Config) -> anyhow::Result<()> {
    let mut session = connect_and_login(config).await?;
    session.logout().await?;
    Ok(())
}

/// Search emails using the provided query string and return formatted results.
pub async fn search_emails(query: &str) -> anyhow::Result<Vec<String>> {
    let config = Config::load()?;
    let mut session = connect_and_login(&config).await?;

    session.select("INBOX").await?;

    let search_criteria = format!("TEXT \"{}\"", query);
    let message_ids = session.search(&search_criteria).await?;

    if message_ids.is_empty() {
        session.logout().await?;
        return Ok(vec!["No messages found".to_string()]);
    }

    let messages: Vec<_> = session
        .fetch(
            &format!("1:{}", message_ids.len().min(10)),
            "(ENVELOPE BODY[HEADER.FIELDS (SUBJECT FROM)])",
        )
        .await?
        .try_collect()
        .await?;

    let mut results = Vec::new();
    for msg in messages.iter() {
        if let Some(envelope) = &msg.envelope() {
            let subject = envelope
                .subject
                .as_ref()
                .and_then(|s| std::str::from_utf8(s).ok())
                .unwrap_or("(no subject)");
            let from = envelope
                .from
                .as_ref()
                .and_then(|addrs| addrs.first())
                .and_then(|addr| addr.mailbox.as_ref())
                .and_then(|mailbox| std::str::from_utf8(mailbox).ok())
                .unwrap_or("(unknown sender)");

            results.push(format!("From: {} | Subject: {}", from, subject));
        }
    }

    session.logout().await?;
    Ok(results)
}

/// Start an interactive IMAP shell for sending raw commands.
pub async fn start_shell() -> anyhow::Result<()> {
    let config = Config::load()?;
    let mut session = connect_and_login(&config).await?;

    let mut rl = DefaultEditor::new()
        .map_err(|e| anyhow::anyhow!("Failed to create readline editor: {}", e))?;

    println!("IMAP Shell - Enter raw IMAP commands. Type 'quit' to exit.");

    loop {
        let readline: RustylineResult<String> = rl.readline("IMAP> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if line.eq_ignore_ascii_case("quit") || line.eq_ignore_ascii_case("exit") {
                    break;
                }

                // Handle common IMAP commands that are supported by async-imap
                let command_lower = line.to_lowercase();

                if command_lower.starts_with("select ") {
                    let mailbox = line[7..].trim();
                    match session.select(mailbox).await {
                        Ok(mailbox_info) => {
                            println!("* {} EXISTS", mailbox_info.exists);
                            println!("* {} RECENT", mailbox_info.recent);
                            if let Some(uid_validity) = mailbox_info.uid_validity {
                                println!("* OK [UIDVALIDITY {}]", uid_validity);
                            }
                            println!("* OK [UIDNEXT {}]", mailbox_info.uid_next.unwrap_or(1));
                            println!("A001 OK [READ-WRITE] SELECT completed");
                        }
                        Err(e) => println!("A001 NO {}", e),
                    }
                } else if command_lower.starts_with("list ") {
                    let parts: Vec<&str> = line[5..].split_whitespace().collect();
                    if parts.len() >= 2 {
                        let reference = parts[0].trim_matches('"');
                        let mailbox = parts[1].trim_matches('"');
                        match session.list(Some(reference), Some(mailbox)).await {
                            Ok(mailboxes) => {
                                let mailboxes: Vec<_> =
                                    mailboxes.try_collect().await.unwrap_or_default();
                                for mailbox in mailboxes.iter() {
                                    let attrs: Vec<String> = mailbox
                                        .attributes()
                                        .iter()
                                        .map(|a| format!("{:?}", a))
                                        .collect();
                                    println!(
                                        "* LIST ({}) \"{}\" \"{}\"",
                                        attrs.join(" "),
                                        mailbox.delimiter().unwrap_or("/"),
                                        mailbox.name()
                                    );
                                }
                                println!("A001 OK LIST completed");
                            }
                            Err(e) => println!("A001 NO {}", e),
                        }
                    } else {
                        println!("A001 BAD Invalid LIST command syntax");
                    }
                } else if command_lower == "capability" {
                    match session.capabilities().await {
                        Ok(caps) => {
                            let cap_strings: Vec<String> =
                                caps.iter().map(|c| format!("{:?}", c)).collect();
                            println!("* CAPABILITY {}", cap_strings.join(" "));
                            println!("A001 OK CAPABILITY completed");
                        }
                        Err(e) => println!("A001 NO {}", e),
                    }
                } else {
                    println!("A001 BAD Command not supported in shell mode");
                    println!("Supported commands: SELECT, LIST, CAPABILITY");
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("Interrupted");
                break;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!("EOF");
                break;
            }
            Err(e) => {
                println!("Error reading line: {}", e);
                break;
            }
        }
    }

    session.logout().await?;
    println!("Goodbye!");
    Ok(())
}

/// Establish a TLS connection to the IMAP server and authenticate.
async fn connect_and_login(config: &Config) -> anyhow::Result<ImapSession> {
    let tcp_stream = TcpStream::connect((config.server.as_str(), config.port))
        .await
        .context("Failed to connect to IMAP server")?;

    let mut root_cert_store = rustls::RootCertStore::empty();
    root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    let connector = TlsConnector::from(std::sync::Arc::new(tls_config));
    let domain = rustls_pki_types::ServerName::try_from(config.server.clone())
        .context("Invalid server name")?;

    let tls_stream = connector
        .connect(domain, tcp_stream)
        .await
        .context("Failed to establish TLS connection")?;

    let compat_stream = tls_stream.compat();
    let client = Client::new(compat_stream);

    let session = client
        .login(&config.username, &config.password)
        .await
        .map_err(|e| anyhow::anyhow!("Login failed: {}", e.0))?;

    Ok(session)
}
