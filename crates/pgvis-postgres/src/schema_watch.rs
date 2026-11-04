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
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Start listening; the returned stream yields once per schema change.
/// The background task ends once the stream is dropped.
pub(crate) fn watch(dsn: String) -> SchemaChangeStream {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(listen(dsn, tx));
    futures::stream::unfold(
        rx,
        |mut rx| async move { rx.recv().await.map(|()| ((), rx)) },
    )
    .boxed()
}

/// Whether a `pgrst` payload asks for a schema reload.
fn is_reload(payload: &str) -> bool {
    matches!(payload.trim(), "" | "reload schema")
}

async fn listen(dsn: String, changes: mpsc::Sender<()>) {
    let mut backoff = BACKOFF_MIN;
    let mut reconnecting = false;
    loop {
        let connected = tokio::select! {
            () = changes.closed() => return,
            result = tokio::time::timeout(CONNECT_TIMEOUT, tokio_postgres::connect(&dsn, tls_connector())) => result,
        };
        match connected {
            Ok(Ok((client, mut connection))) => {
                // Coalesce reload requests directly into one bounded signal.
                // Never stop driving the connection while LISTEN awaits a reply.
                let notifications = changes.clone();
                let mut driver = tokio::spawn(async move {
                    while let Some(Ok(message)) =
                        std::future::poll_fn(|cx| connection.poll_message(cx)).await
                    {
                        if let AsyncMessage::Notification(n) = message
                            && is_reload(n.payload())
                        {
                            let _ = notifications.try_send(());
                        }
                    }
                });
                let listen_sql = format!("LISTEN {CHANNEL}");
                let listening = tokio::select! {
                    () = changes.closed() => { driver.abort(); return; }
                    result = tokio::time::timeout(CONNECT_TIMEOUT, client.batch_execute(&listen_sql)) => result,
                };
                match listening {
                    Ok(Ok(())) => {
                        backoff = BACKOFF_MIN;
                        tracing::info!(channel = CHANNEL, "listening for schema changes");
                        if reconnecting {
                            let _ = changes.try_send(());
                        }
                        tokio::select! {
                            () = changes.closed() => { driver.abort(); return; }
                            _ = &mut driver => {}
                        }
                        tracing::warn!("schema-change listener disconnected; reconnecting");
                    }
                    Ok(Err(e)) => tracing::warn!(error = %e, "LISTEN for schema changes failed"),
                    Err(_) => tracing::warn!("LISTEN for schema changes timed out"),
                }
                driver.abort();
            }
            Ok(Err(e)) => tracing::warn!(error = %e, "schema-change listener could not connect"),
            Err(_) => tracing::warn!("schema-change listener connection timed out"),
        }
        reconnecting = true;
        tokio::select! {
            () = changes.closed() => return,
            () = tokio::time::sleep(backoff) => {}
        }
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
