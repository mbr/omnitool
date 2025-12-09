use anyhow::{Context, Result};
use async_imap::Client;
use futures::TryStreamExt;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{debug, info};

use crate::config::Config;

type ImapStream = tokio_util::compat::Compat<TlsStream<TcpStream>>;
type ImapSession = async_imap::Session<ImapStream>;

pub async fn test_login(config: &Config) -> Result<()> {
    info!("Testing IMAP connection");
    let mut session = connect_and_login(config).await?;
    session.logout().await?;
    debug!("IMAP login test successful");
    Ok(())
}

pub async fn search_emails(query: &str) -> Result<Vec<String>> {
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

async fn connect_and_login(config: &Config) -> Result<ImapSession> {
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
    debug!("Connected to IMAP server with TLS");

    let session = client
        .login(&config.username, &config.password)
        .await
        .map_err(|e| anyhow::anyhow!("Login failed: {}", e.0))?;

    Ok(session)
}
