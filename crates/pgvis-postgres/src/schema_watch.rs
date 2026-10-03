//! Schema-change notifications: `LISTEN pgrst` on a dedicated connection.
//!
//! Follows PostgREST's convention: `NOTIFY pgrst, 'reload schema'` (or an
//! empty payload) asks every pgvis instance to re-introspect. Typically sent
//! by a migration, or by a DDL event trigger:
//!
//! ```sql
//! CREATE OR REPLACE FUNCTION pgrst_watch() RETURNS event_trigger
//!   LANGUAGE plpgsql AS $$ BEGIN NOTIFY pgrst, 'reload schema'; END; $$;
//! CREATE EVENT TRIGGER pgrst_watch ON ddl_command_end EXECUTE PROCEDURE pgrst_watch();
//! ```
//!
//! The connection is separate from the request pool (LISTEN needs a
//! long-lived session). After a reconnect one change is reported, since a
//! notification may have been missed while disconnected.

use std::time::Duration;

use futures::StreamExt;
use pgvis_core::backend::SchemaChangeStream;
use tokio::sync::mpsc;
use tokio_postgres::AsyncMessage;

use crate::tls_connector;

const CHANNEL: &str = "pgrst";
const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Start listening; the returned stream yields once per schema change.
/// The background task ends once the stream is dropped.
pub(crate) fn watch(dsn: String) -> SchemaChangeStream {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(listen(dsn, tx));
    futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|()| ((), rx)) })
        .boxed()
}

/// Whether a `pgrst` payload asks for a schema reload.
fn is_reload(payload: &str) -> bool {
    matches!(payload.trim(), "" | "reload schema")
}

async fn listen(dsn: String, changes: mpsc::UnboundedSender<()>) {
    let mut backoff = BACKOFF_MIN;
    let mut reconnecting = false;
    while !changes.is_closed() {
        match tokio_postgres::connect(&dsn, tls_connector()).await {
            Ok((client, mut connection)) => {
                // Drive the connection; forward its notifications.
                let (payloads_tx, mut payloads) = mpsc::unbounded_channel();
                let driver = tokio::spawn(async move {
                    loop {
                        match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                            Some(Ok(AsyncMessage::Notification(n))) => {
                                let _ = payloads_tx.send(n.payload().to_string());
                            }
                            Some(Ok(_)) => {}
                            Some(Err(_)) | None => break,
                        }
                    }
                });
                match client.batch_execute(&format!("LISTEN {CHANNEL}")).await {
                    Ok(()) => {
                        backoff = BACKOFF_MIN;
                        tracing::info!(channel = CHANNEL, "listening for schema changes");
                        if reconnecting && changes.send(()).is_err() {
                            driver.abort();
                            return;
                        }
                        while let Some(payload) = payloads.recv().await {
                            if is_reload(&payload) && changes.send(()).is_err() {
                                driver.abort();
                                return;
                            }
                        }
                        tracing::warn!("schema-change listener disconnected; reconnecting");
                    }
                    Err(e) => tracing::warn!(error = %e, "LISTEN for schema changes failed"),
                }
                driver.abort();
            }
            Err(e) => tracing::warn!(error = %e, "schema-change listener could not connect"),
        }
        reconnecting = true;
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reload_payloads() {
        assert!(is_reload(""));
        assert!(is_reload("reload schema"));
        assert!(!is_reload("reload config"));
        assert!(!is_reload("something else"));
    }
}
