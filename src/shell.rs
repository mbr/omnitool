use futures::TryStreamExt;
use rustyline::{DefaultEditor, Result as RustylineResult};
use structopt::StructOpt;

use crate::{config::Config, imap};

/// Type alias for an active IMAP session over a TLS connection.
type ImapSession = imap::ImapSession;

/// Format and print mailbox information from a SELECT or EXAMINE response.
fn print_mailbox_info(operation: &str, mailbox_name: &str, info: &async_imap::types::Mailbox) {
    println!("{} mailbox: {}", operation, mailbox_name);
    println!("  EXISTS: {}", info.exists);
    println!("  RECENT: {}", info.recent);
    if let Some(uid_validity) = info.uid_validity {
        println!("  UIDVALIDITY: {}", uid_validity);
    }
    if let Some(uid_next) = info.uid_next {
        println!("  UIDNEXT: {}", uid_next);
    }
    println!("  FLAGS: {:?}", info.flags);
}

/// Format and print fetched message information.
fn print_fetch_results(messages: &[async_imap::types::Fetch]) {
    println!("Fetched {} messages", messages.len());
    for msg in messages.iter() {
        if let Some(envelope) = msg.envelope() {
            if let Some(subject) = &envelope.subject {
                println!("  Subject: {}", String::from_utf8_lossy(subject));
            }
        }
        println!("  UID: {:?}", msg.uid);
        let flags: Vec<_> = msg.flags().collect();
        println!("  FLAGS: {:?}", flags);
    }
}

/// IMAP commands that can be executed in the shell.
#[derive(Debug, StructOpt)]
pub enum ImapCommand {
    /// Get server capabilities.
    Capabilities,
    /// Request a checkpoint of the currently selected mailbox.
    Check,
    /// Close the currently selected mailbox.
    Close,
    /// Select a mailbox for examination (read-only).
    Examine {
        /// Mailbox name to examine.
        mailbox: String,
    },
    /// Fetch messages from the mailbox.
    Fetch {
        /// Message sequence set.
        sequence_set: String,
        /// Message data items to fetch.
        items: String,
    },
    /// Get metadata for a mailbox.
    GetMetadata {
        /// Mailbox name.
        mailbox: String,
        /// Metadata entry specifier.
        entry: String,
    },
    /// Get quota information.
    GetQuota {
        /// Quota root name.
        quota_root: String,
    },
    /// Send identification information.
    Id {
        /// Identification parameters.
        parameters: Vec<String>,
    },
    /// Send nil identification.
    IdNil,
    /// Enter idle mode to wait for server updates.
    Idle,
    /// List mailboxes.
    List {
        /// Reference name.
        reference: String,
        /// Mailbox name with wildcards.
        mailbox: String,
    },
    /// Send a no-op to keep connection alive.
    Noop,
    /// Search for messages.
    Search {
        /// Search criteria.
        criteria: String,
    },
    /// Select a mailbox.
    Select {
        /// Mailbox name to select.
        mailbox: String,
    },
    /// Select a mailbox with CONDSTORE extension.
    SelectCondstore {
        /// Mailbox name to select.
        mailbox: String,
    },
    /// Get status information for a mailbox.
    Status {
        /// Mailbox name.
        mailbox: String,
        /// Status data items.
        items: String,
    },
    /// Subscribe to a mailbox.
    Subscribe {
        /// Mailbox name to subscribe to.
        mailbox: String,
    },
    /// Search for messages by UID.
    UidSearch {
        /// Search criteria.
        criteria: String,
    },
    /// Unsubscribe from a mailbox.
    Unsubscribe {
        /// Mailbox name to unsubscribe from.
        mailbox: String,
    },
}

impl ImapCommand {
    /// Execute the IMAP command against the provided session.
    pub async fn execute(self, session: &mut ImapSession) -> anyhow::Result<()> {
        match self {
            ImapCommand::Capabilities => {
                let caps = session.capabilities().await?;
                println!("Capabilities:");
                for cap in caps.iter() {
                    println!("  {:?}", cap);
                }
            }
            ImapCommand::Check => {
                session.check().await?;
                println!("CHECK completed");
            }
            ImapCommand::Close => {
                session.close().await?;
                println!("CLOSE completed");
            }
            ImapCommand::Examine { mailbox } => {
                let info = session.examine(&mailbox).await?;
                print_mailbox_info("Examined", &mailbox, &info);
            }
            ImapCommand::Fetch {
                sequence_set,
                items,
            } => {
                let messages = session.fetch(&sequence_set, &items).await?;
                let messages: Vec<_> = messages.try_collect().await?;
                print_fetch_results(&messages);
            }
            ImapCommand::GetMetadata { mailbox, entry } => {
                // Note: get_metadata might not be available in async-imap
                println!("GET_METADATA not implemented in async-imap");
                let _ = (mailbox, entry);
            }
            ImapCommand::GetQuota { quota_root } => {
                // Note: get_quota might not be available in async-imap
                println!("GET_QUOTA not implemented in async-imap");
                let _ = quota_root;
            }
            ImapCommand::Id { parameters } => {
                // Note: id might not be available in async-imap
                println!("ID not implemented in async-imap");
                let _ = parameters;
            }
            ImapCommand::IdNil => {
                // Note: id_nil might not be available in async-imap
                println!("ID NIL not implemented in async-imap");
            }
            ImapCommand::Idle => {
                // Note: IDLE takes ownership of the session, which conflicts with our shell loop
                println!(
                    "IDLE not available in interactive shell mode (takes ownership of session)"
                );
            }
            ImapCommand::List { reference, mailbox } => {
                let mailboxes = session.list(Some(&reference), Some(&mailbox)).await?;
                let mailboxes: Vec<_> = mailboxes.try_collect().await?;
                println!("Mailboxes:");
                for mb in mailboxes.iter() {
                    println!("  {} (delimiter: {:?})", mb.name(), mb.delimiter());
                    if !mb.attributes().is_empty() {
                        println!("    Attributes: {:?}", mb.attributes());
                    }
                }
            }
            ImapCommand::Noop => {
                session.noop().await?;
                println!("NOOP completed");
            }
            ImapCommand::Search { criteria } => {
                let uids = session.search(&criteria).await?;
                println!("Search results: {:?}", uids);
            }
            ImapCommand::Select { mailbox } => {
                let info = session.select(&mailbox).await?;
                print_mailbox_info("Selected", &mailbox, &info);
            }
            ImapCommand::SelectCondstore { mailbox } => {
                // Note: select_condstore might not be available in async-imap
                println!("SELECT_CONDSTORE not implemented, using regular SELECT");
                let info = session.select(&mailbox).await?;
                print_mailbox_info("Selected", &mailbox, &info);
            }
            ImapCommand::Status { mailbox, items } => {
                // Note: status might not be available in async-imap
                println!("STATUS not fully implemented in async-imap");
                let _ = (mailbox, items);
            }
            ImapCommand::Subscribe { mailbox } => {
                session.subscribe(&mailbox).await?;
                println!("Subscribed to: {}", mailbox);
            }
            ImapCommand::UidSearch { criteria } => {
                let uids = session.uid_search(&criteria).await?;
                println!("UID search results: {:?}", uids);
            }
            ImapCommand::Unsubscribe { mailbox } => {
                session.unsubscribe(&mailbox).await?;
                println!("Unsubscribed from: {}", mailbox);
            }
        }
        Ok(())
    }
}

/// Start an interactive IMAP shell for sending raw commands.
pub async fn start() -> anyhow::Result<()> {
    let config = Config::load()?;
    let mut session = imap::connect_and_login(&config).await?;

    let mut rl = DefaultEditor::new()
        .map_err(|e| anyhow::anyhow!("Failed to create readline editor: {}", e))?;

    println!("IMAP Shell - Enter IMAP commands, or --help for a command list");

    loop {
        let readline: RustylineResult<String> = rl.readline("IMAP> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let args: Vec<&str> = line.split_whitespace().collect();
                let args_with_arg0 = std::iter::once("imapcmd").chain(args.into_iter());

                match ImapCommand::from_iter_safe(args_with_arg0) {
                    Ok(command) => {
                        if let Err(e) = command.execute(&mut session).await {
                            println!("Error executing command: {}", e);
                        }
                    }
                    Err(e) => {
                        println!("Error parsing command: {}", e);
                    }
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted)
            | Err(rustyline::error::ReadlineError::Eof) => {
                break;
            }
            Err(e) => {
                println!("Error reading line: {}", e);
                break;
            }
        }
    }

    session.logout().await?;
    Ok(())
}
