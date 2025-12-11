use futures::TryStreamExt;
use structopt::StructOpt;

use crate::imap;

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

/// Format and print a single fetched message.
fn print_fetch_result(msg: &async_imap::types::Fetch) {
    if let Some(envelope) = msg.envelope() {
        if let Some(subject) = &envelope.subject {
            println!("  Subject: {}", String::from_utf8_lossy(subject));
        }
    }
    println!("  UID: {:?}", msg.uid);
    let flags: Vec<_> = msg.flags().collect();
    println!("  FLAGS: {:?}", flags);
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
                println!("Fetched {} messages", messages.len());
                for msg in messages.iter() {
                    print_fetch_result(msg);
                }
            }
            ImapCommand::GetMetadata { mailbox, entry } => {
                let metadata = session.get_metadata(&mailbox, "", &entry).await?;
                println!("Metadata for {}: {:?}", mailbox, metadata);
            }
            ImapCommand::GetQuota { quota_root } => {
                let quota = session.get_quota(&quota_root).await?;
                println!("Quota for {}: {:?}", quota_root, quota);
            }
            ImapCommand::Id { parameters } => {
                if parameters.len() % 2 != 0 {
                    return Err(anyhow::anyhow!("ID parameters must be key-value pairs"));
                }
                let pairs: Vec<(&str, Option<&str>)> = parameters
                    .chunks(2)
                    .map(|chunk| (chunk[0].as_str(), Some(chunk[1].as_str())))
                    .collect();
                let response = session.id(pairs).await?;
                println!("ID response: {:?}", response);
            }
            ImapCommand::IdNil => {
                let response = session.id_nil().await?;
                println!("ID NIL response: {:?}", response);
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
                let messages = session.search(&criteria).await?;
                println!("Search results: {:?}", messages);
            }
            ImapCommand::Select { mailbox } => {
                let info = session.select(&mailbox).await?;
                print_mailbox_info("Selected", &mailbox, &info);
            }
            ImapCommand::SelectCondstore { mailbox } => {
                let info = session.select_condstore(&mailbox).await?;
                print_mailbox_info("Selected (CONDSTORE)", &mailbox, &info);
            }
            ImapCommand::Status { mailbox, items } => {
                let response = session.status(&mailbox, &items).await?;
                println!("Status for {}: {:?}", mailbox, response);
            }
            ImapCommand::Subscribe { mailbox } => {
                session.subscribe(&mailbox).await?;
                println!("Subscribed to {}", mailbox);
            }
            ImapCommand::UidSearch { criteria } => {
                let messages = session.uid_search(&criteria).await?;
                println!("UID search results: {:?}", messages);
            }
            ImapCommand::Unsubscribe { mailbox } => {
                session.unsubscribe(&mailbox).await?;
                println!("Unsubscribed from {}", mailbox);
            }
        }
        Ok(())
    }
}

/// Start an interactive IMAP shell for sending raw commands.
pub async fn start() -> anyhow::Result<()> {
    let mut prompt = crate::shells::Prompt::new("imap")?;
    let pool = imap::create_pool(prompt.config.clone()).await?;
    let mut session = pool.get().await?;

    println!("IMAP Shell - Enter IMAP commands, or --help for a command list");

    loop {
        match prompt.read_line("IMAP> ").await {
            Ok(Some(line)) => {
                // Parse the line into shell arguments
                let args = match shlex::split(&line) {
                    Some(args) => args,
                    None => {
                        println!("Error: Invalid shell syntax");
                        continue;
                    }
                };

                // Add a dummy arg0 for structopt parsing
                let args_with_arg0 =
                    std::iter::once("imap_cmd".to_string()).chain(args.into_iter());

                // Parse into command
                let command = match ImapCommand::from_iter_safe(args_with_arg0) {
                    Ok(command) => command,
                    Err(e) => {
                        println!("Error parsing command: {}", e);
                        continue;
                    }
                };

                // Execute the command
                if let Err(e) = command.execute(&mut session).await {
                    println!("Error executing command: {}", e);
                }
            }
            Ok(None) => {
                // User exited
                break;
            }
            Err(e) => {
                // Actual error
                println!("Error: {}", e);
                break;
            }
        }
    }

    session.logout().await?;
    Ok(())
}
