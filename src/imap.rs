use std::{io, sync::Arc};

use async_imap::Client;
use bb8::{ManageConnection, Pool};
use futures::TryStreamExt;
use rustls_pki_types::InvalidDnsNameError;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::config::Config;

/// Type alias for the TLS stream used in IMAP connections.
type ImapStream = tokio_util::compat::Compat<TlsStream<TcpStream>>;
/// Type alias for an active IMAP session over a TLS connection.
pub type ImapSession = async_imap::Session<ImapStream>;
/// A logged-in IMAP connection pool.
pub type ImapPool = Pool<ImapConMan>;

/// Test IMAP login with the provided configuration.
pub async fn test_login(config: &Config) -> anyhow::Result<()> {
    let mut session = connect_and_login(config).await?;
    session.logout().await?;
    Ok(())
}

/// Search emails using the provided query string and return formatted results.
pub async fn search_emails(query: &str) -> anyhow::Result<Vec<String>> {
    let config = Config::load()?;
    let pool = create_pool(Arc::new(config)).await?;
    let mut session = pool.get().await.map_err(|e| anyhow::Error::from(e))?;

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

/// An IMAP connection manager.
pub struct ImapConMan {
    /// The configuration used to connect.
    config: Arc<Config>,
}

impl ManageConnection for ImapConMan {
    type Connection = ImapSession;
    type Error = ImapError;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send {
        async { Ok(connect_and_login(&self.config).await?) }
    }

    fn is_valid(
        &self,
        conn: &mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move { Ok(conn.noop().await?) }
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

/// Creates a new connection pool from a given configuration.
pub async fn create_pool(config: Arc<Config>) -> Result<ImapPool, ImapError> {
    Pool::builder().build(ImapConMan { config }).await
}

/// Top-level IMAP error.
#[derive(Error, Debug)]
pub enum ImapError {
    /// Connection failed.
    #[error("connection failed")]
    Connect(#[from] ImapConnectErr),
    /// IMAP session error.
    #[error("imap error")]
    Imap(#[from] async_imap::error::Error),
}

/// Imap connection error.
#[derive(Debug, Error)]
pub enum ImapConnectErr {
    /// TCP connection could not be established.
    #[error("TCP connection failed")]
    TcpConnectionFailure(#[source] io::Error),
    /// The given server name is not a valid DNS name.
    #[error("Invalid server name")]
    InvalidServerName(#[source] InvalidDnsNameError),
    /// TLS transport could not be established.
    #[error("TLS negotiation failed")]
    TlsConnectionFailure(#[source] io::Error),
    /// IMAP login was unsuccessful.
    #[error("Login failed")]
    LoginFailed(#[source] async_imap::error::Error),
}

/// Establish a TLS connection to the IMAP server and authenticate.
async fn connect_and_login(config: &Config) -> Result<ImapSession, ImapConnectErr> {
    let tcp_stream = TcpStream::connect((config.server.as_str(), config.port))
        .await
        .map_err(ImapConnectErr::TcpConnectionFailure)?;

    let mut root_cert_store = rustls::RootCertStore::empty();
    root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    let connector = TlsConnector::from(std::sync::Arc::new(tls_config));
    let domain = rustls_pki_types::ServerName::try_from(config.server.clone())
        .map_err(ImapConnectErr::InvalidServerName)?;

    let tls_stream = connector
        .connect(domain, tcp_stream)
        .await
        .map_err(ImapConnectErr::TlsConnectionFailure)?;

    let compat_stream = tls_stream.compat();
    let client = Client::new(compat_stream);

    let session = client
        .login(&config.username, &config.password)
        .await
        .map_err(|(e, _)| ImapConnectErr::LoginFailed(e))?;

    Ok(session)
}
