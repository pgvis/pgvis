//! Keeping the schema cache current after DDL.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use futures::{FutureExt, StreamExt};
use pgvis_core::backend::{Backend, IntrospectConfig};
use pgvis_core::cache::SchemaCache;
use pgvis_core::error::Error;
use tokio::sync::Notify;

/// How long to wait for a burst of changes (a migration's many DDL
/// statements) to settle before re-introspecting once.
const DEBOUNCE: Duration = Duration::from_millis(200);

/// Re-introspects the database and swaps in the new schema cache when the
/// backend reports a schema change (Postgres: `NOTIFY pgrst, 'reload
/// schema'`), or when asked to.
///
/// The swap is atomic: in-flight requests finish on the cache they started
/// with. A failed introspection keeps the current cache. Usable on its own
/// by embedders that build their own cache:
///
/// ```rust,ignore
/// let reloader = SchemaReloader::spawn(backend.clone(), cache.clone(), introspect_cfg);
/// run_migrations().await?;
/// reloader.reload_now().await?; // new tables are routable from here on
/// ```
#[derive(Clone)]
pub struct SchemaReloader {
    inner: Arc<Inner>,
    _task: Arc<ReloadTask>,
}

struct ReloadTask(tokio::task::JoinHandle<()>);

impl Drop for ReloadTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Inner {
    backend: Arc<dyn Backend>,
    cache: Arc<ArcSwap<SchemaCache>>,
    config: IntrospectConfig,
    requested: Notify,
}

impl SchemaReloader {
    /// Start the reload task (it subscribes to the backend's schema-change
    /// notifications, if it has any) and return a handle to it.
    /// Dropping the last handle stops the task and its schema listener.
    pub fn spawn(
        backend: Arc<dyn Backend>,
        cache: Arc<ArcSwap<SchemaCache>>,
        config: IntrospectConfig,
    ) -> Self {
        let inner = Arc::new(Inner {
            backend,
            cache,
            config,
            requested: Notify::new(),
        });
        let task = tokio::spawn(run(inner.clone()));
        Self {
            inner,
            _task: Arc::new(ReloadTask(task)),
        }
    }

    /// Ask for a reload; returns at once. Requests that arrive together
    /// are coalesced into one introspection.
    pub fn reload(&self) {
        self.inner.requested.notify_one();
    }

    /// Reload now and wait for it. On error the current cache is kept.
    pub async fn reload_now(&self) -> Result<(), Error> {
        reload(&self.inner).await
    }
}

async fn reload(inner: &Inner) -> Result<(), Error> {
    let fresh = inner.backend.introspect(&inner.config).await?;
    inner.cache.store(Arc::new(fresh));
    Ok(())
}

async fn run(inner: Arc<Inner>) {
    let mut changes = inner.backend.watch_schema().await;
    loop {
        // Wait for a notification or an explicit request.
        match &mut changes {
            Some(stream) => tokio::select! {
                change = stream.next() => {
                    if change.is_none() {
                        changes = None; // the backend stopped watching
                        continue;
                    }
                }
                () = inner.requested.notified() => {}
            },
            None => inner.requested.notified().await,
        }
        // Let a burst of changes settle, then reload once.
        tokio::time::sleep(DEBOUNCE).await;
        if let Some(stream) = &mut changes {
            // Bound draining even if an embedding backend produces notifications
            // continuously; introspection and task cancellation must make progress.
            for _ in 0..256 {
                if !matches!(stream.next().now_or_never(), Some(Some(()))) {
                    break;
                }
            }
        }
        match reload(&inner).await {
            Ok(()) => tracing::info!("schema cache reloaded"),
            Err(e) => {
                tracing::error!(error = %e, "schema cache reload failed; keeping the current cache");
            }
        }
    }
}
