//! Bounded backend admission, deadlines and optional AIMD resizing.
//! The raw deadpool ceiling is never raised above the configured size.

use crate::Checkout;
use deadpool_postgres::Pool;
use pgvis_core::backend::{ExecContext, PoolMetrics, PoolStatus, QueryResult};
use pgvis_core::config::{AdaptivePoolConfig, PoolConfig};
use pgvis_core::error::Error;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
struct Counters {
    checkout_attempts: AtomicU64,
    checkout_errors: AtomicU64,
    checkout_rejected: AtomicU64,
    checkout_duration_us: AtomicU64,
    queries: AtomicU64,
    query_errors: AtomicU64,
    query_cancelled: AtomicU64,
    query_duration_us: AtomicU64,
    adaptive_increases: AtomicU64,
    adaptive_decreases: AtomicU64,
}

struct Adaptive {
    config: AdaptivePoolConfig,
    limit: usize,
    last: Instant,
    congestion: bool,
    demand: bool,
    success: bool,
}

impl Adaptive {
    fn observe(
        &mut self,
        now: Instant,
        hard_max: usize,
        slow: bool,
        demand: bool,
        success: bool,
    ) -> Option<usize> {
        self.congestion |= slow;
        self.demand |= demand;
        self.success |= success;
        if now.duration_since(self.last) < Duration::from_millis(self.config.adjustment_interval_ms)
        {
            return None;
        }
        let next = if self.congestion {
            (self.limit / 2).max(self.config.min_size)
        } else if self.demand && self.success {
            self.limit.saturating_add(1).min(hard_max)
        } else {
            self.limit
        };
        self.last = now;
        self.congestion = false;
        self.demand = false;
        self.success = false;
        let changed = next != self.limit;
        self.limit = next;
        changed.then_some(next)
    }
}

struct Inner {
    config: PoolConfig,
    waiting: AtomicUsize,
    counters: Counters,
    adaptive: Mutex<Option<Adaptive>>,
}

#[derive(Clone)]
pub(crate) struct ManagedPool {
    pub(crate) raw: Pool,
    inner: Arc<Inner>,
}

impl ManagedPool {
    pub(crate) fn new(dsn: &str, config: &PoolConfig) -> Result<Self, Error> {
        if config.size == 0 || config.max_waiters == 0 {
            return Err(Error::Introspection(
                "pool size and max_waiters must be positive".into(),
            ));
        }
        if let Some(adaptive) = &config.adaptive
            && (adaptive.min_size == 0
                || adaptive.min_size > config.size as usize
                || adaptive.target_query_time_ms == 0
                || adaptive.adjustment_interval_ms == 0
                || config.timeout_ms == 0
                || config.query_timeout_ms == 0)
        {
            return Err(Error::Introspection("adaptive pool requires positive deadlines/interval/target and min_size within size".into()));
        }
        let raw = crate::create_pool(dsn, config)?;
        let adaptive = config.adaptive.clone().map(|config| {
            raw.resize(config.min_size);
            Adaptive {
                limit: config.min_size,
                config,
                last: Instant::now(),
                congestion: false,
                demand: false,
                success: false,
            }
        });
        Ok(Self {
            raw,
            inner: Arc::new(Inner {
                config: config.clone(),
                waiting: AtomicUsize::new(0),
                counters: Counters::default(),
                adaptive: Mutex::new(adaptive),
            }),
        })
    }

    pub(crate) async fn checkout(&self) -> Result<Checkout, Error> {
        let counters = &self.inner.counters;
        counters.checkout_attempts.fetch_add(1, Relaxed);
        if self
            .inner
            .waiting
            .fetch_update(Relaxed, Relaxed, |n| {
                (n < self.inner.config.max_waiters).then_some(n + 1)
            })
            .is_err()
        {
            counters.checkout_rejected.fetch_add(1, Relaxed);
            return Err(Error::Introspection(
                "database checkout queue is full".into(),
            ));
        }
        let waiting = Waiting(&self.inner, Instant::now());
        let status = self.raw.status();
        let demand = status.available == 0 && status.size >= status.max_size;
        let get = Checkout::get(&self.raw);
        // A single overall checkout deadline, not one full timeout per phase.
        let result = if self.inner.config.timeout_ms == 0 {
            get.await
        } else {
            tokio::time::timeout(Duration::from_millis(self.inner.config.timeout_ms), get)
                .await
                .unwrap_or_else(|_| {
                    Err(Error::Introspection(
                        "database checkout deadline exceeded".into(),
                    ))
                })
        };
        drop(waiting);
        if result.is_err() {
            counters.checkout_errors.fetch_add(1, Relaxed);
        }
        self.feedback(result.is_err(), demand, false);
        result
    }

    pub(crate) async fn introspect(
        &self,
        cfg: &pgvis_core::backend::IntrospectConfig,
    ) -> Result<pgvis_core::SchemaCache, Error> {
        let checkout = self.checkout().await?;
        let query = checkout.introspect(cfg);
        if self.inner.config.query_timeout_ms == 0 {
            query.await
        } else {
            tokio::time::timeout(
                Duration::from_millis(self.inner.config.query_timeout_ms),
                query,
            )
            .await
            .unwrap_or_else(|_| {
                Err(Error::Introspection(
                    "schema introspection deadline exceeded".into(),
                ))
            })
        }
    }

    pub(crate) async fn execute(
        &self,
        checkout: Checkout,
        ctx: &ExecContext,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, Error> {
        let mut running = Running {
            pool: self,
            started: Instant::now(),
            completed: false,
            success: false,
        };
        let query = checkout.execute(ctx, sql, params);
        let deadline_ms = self.inner.config.query_timeout_ms;
        let result = if deadline_ms == 0 {
            query.await
        } else {
            let ms = deadline_ms.max(
                ctx.statement_timeout
                    .map_or(0, |ms| ms.saturating_add(1000)),
            );
            tokio::time::timeout(Duration::from_millis(ms), query)
                .await
                .unwrap_or_else(|_| {
                    Err(Error::Execution {
                        message:
                            "database request deadline exceeded; execution outcome is uncertain"
                                .into(),
                        db_code: Some("57014".into()),
                        detail: None,
                        hint: None,
                    })
                })
        };
        running.completed = true;
        running.success = result.is_ok();
        if result.is_err() {
            self.inner.counters.query_errors.fetch_add(1, Relaxed);
        }
        if matches!(&result, Err(Error::Execution { db_code: Some(code), .. }) if code.starts_with("08") || code.starts_with("53") || code.starts_with("57"))
        {
            self.feedback(true, false, false);
        }
        // Never retry here: a lost COMMIT acknowledgement can hide a write.
        result
    }

    fn feedback(&self, congested: bool, demand: bool, success: bool) {
        let mut adaptive = self.inner.adaptive.lock().expect("adaptive pool lock");
        if let Some(state) = adaptive.as_mut() {
            let old = state.limit;
            if let Some(next) = state.observe(
                Instant::now(),
                self.inner.config.size as usize,
                congested,
                demand,
                success,
            ) {
                self.raw.resize(next);
                if next > old {
                    self.inner.counters.adaptive_increases.fetch_add(1, Relaxed);
                } else {
                    self.inner.counters.adaptive_decreases.fetch_add(1, Relaxed);
                }
            }
        }
    }

    pub(crate) fn status(&self) -> PoolStatus {
        let s = self.raw.status();
        let c = &self.inner.counters;
        PoolStatus {
            max_size: s.max_size,
            hard_max_size: self.inner.config.size as usize,
            size: s.size,
            available: s.available,
            waiting: self.inner.waiting.load(Relaxed),
            metrics: PoolMetrics {
                checkout_attempts: c.checkout_attempts.load(Relaxed),
                checkout_errors: c.checkout_errors.load(Relaxed),
                checkout_rejected: c.checkout_rejected.load(Relaxed),
                checkout_duration_us: c.checkout_duration_us.load(Relaxed),
                queries: c.queries.load(Relaxed),
                query_errors: c.query_errors.load(Relaxed),
                query_cancelled: c.query_cancelled.load(Relaxed),
                query_duration_us: c.query_duration_us.load(Relaxed),
                adaptive_increases: c.adaptive_increases.load(Relaxed),
                adaptive_decreases: c.adaptive_decreases.load(Relaxed),
            },
        }
    }
}

struct Waiting<'a>(&'a Inner, Instant);
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.waiting.fetch_sub(1, Relaxed);
        self.0
            .counters
            .checkout_duration_us
            .fetch_add(micros(self.1.elapsed()), Relaxed);
    }
}

struct Running<'a> {
    pool: &'a ManagedPool,
    started: Instant,
    completed: bool,
    success: bool,
}
impl Drop for Running<'_> {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        let counters = &self.pool.inner.counters;
        counters.queries.fetch_add(1, Relaxed);
        counters
            .query_duration_us
            .fetch_add(micros(elapsed), Relaxed);
        if !self.completed {
            counters.query_cancelled.fetch_add(1, Relaxed);
        }
        let slow = self
            .pool
            .inner
            .config
            .adaptive
            .as_ref()
            .is_some_and(|c| elapsed > Duration::from_millis(c.target_query_time_ms));
        self.pool.feedback(
            slow,
            self.pool.inner.waiting.load(Relaxed) > 0,
            self.success,
        );
    }
}

fn micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn aimd_grows_under_demand_backs_off_and_never_exceeds_its_bounds() {
        let now = Instant::now();
        let mut a = Adaptive {
            config: AdaptivePoolConfig {
                min_size: 1,
                adjustment_interval_ms: 100,
                ..Default::default()
            },
            limit: 1,
            last: now,
            congestion: false,
            demand: false,
            success: false,
        };
        assert_eq!(a.observe(now, 3, false, true, true), None);
        assert_eq!(
            a.observe(now + Duration::from_millis(100), 3, false, true, true),
            Some(2)
        );
        assert_eq!(
            a.observe(now + Duration::from_millis(200), 3, false, true, true),
            Some(3)
        );
        assert_eq!(
            a.observe(now + Duration::from_millis(300), 3, false, true, true),
            None
        );
        assert_eq!(
            a.observe(now + Duration::from_millis(400), 3, true, true, true),
            Some(1)
        );
        assert_eq!(
            a.observe(now + Duration::from_millis(500), 3, true, true, false),
            None
        );
        assert_eq!(
            a.observe(now + Duration::from_millis(600), 3, false, false, true),
            None
        );
    }
}
