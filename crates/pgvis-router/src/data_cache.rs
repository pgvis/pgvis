//! # In-memory data cache for read query responses.
//!
//! Provides response caching for table reads using the [`svcache`] crate.
//!
//! ## Keys
//!
//! A cache key hashes the **security context** (JWT claims, which RLS policies
//! read via `request.jwt.claims`) together with the **rendered query** (SQL +
//! parameters, which already encodes select, filters, order, and pagination)
//! and a **per-table generation counter**.
//! Each `(role, claims, query shape, table_generation)` therefore maps to a
//! distinct entry — one user's rows are never served to another, and two reads
//! of the same PK with different `select`/filters don't collide.
//! PK lookups are cached by default; list queries only when `cache_lists`
//! is enabled.
//!
//! ## Invalidation
//!
//! Table-scoped: a mutation on table T bumps its generation counter, causing all
//! existing cache entries for that table to become stale misses (the generation
//! in the key no longer matches). Volatile RPCs bump a global generation that
//! affects all tables. Old stale entries are cleaned up by TTL/LRU eviction.
//!
//! ## Thread Safety
//!
//! `DataCache` is `Send + Sync` and designed for concurrent access from multiple
//! axum handler tasks. The underlying `svcache` uses `DashMap` on native targets.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Duration;

use pgvis_core::cache::QualifiedIdentifier;
use pgvis_core::config::CacheConfig;
use pgvis_core::plan::types::ReadPlan;
use pgvis_core::query_params::types::Operator;
use serde_json::Value;
use svcache::{CacheKey, SvCache};

// ---------------------------------------------------------------------------
// CachedResponse — the value stored in the cache
// ---------------------------------------------------------------------------

/// A cached query response entry.
///
/// Implements [`CacheKey`] for use with `svcache`. The cache key string is the
/// primary ID; no slug is used.
#[derive(Debug, Clone)]
pub struct CachedEntry {
    /// The computed cache key (e.g., `"role:pk:public.users:9f3a7c1b40e2d5a8"`).
    pub key: String,
    /// The JSON body (array or object).
    pub body: Value,
    /// Total count (if count was requested).
    pub total_count: Option<i64>,
    /// Page total.
    pub page_total: Option<i64>,
}

impl CacheKey for CachedEntry {
    type Id = String;

    fn id(&self) -> Self::Id {
        self.key.clone()
    }

    fn slug(&self) -> Option<&str> {
        None
    }
}

// ---------------------------------------------------------------------------
// CacheStats — observable cache metrics
// ---------------------------------------------------------------------------

/// Snapshot of cache performance statistics.
///
/// Returned by [`DataCache::stats()`] and serialized at the `GET /pgvis/cache` endpoint.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheStats {
    /// Number of cache lookups that returned a stored entry.
    pub hits: u64,
    /// Number of cache lookups that found no entry (triggered DB query).
    pub misses: u64,
    /// Number of times a table was invalidated (store cleared).
    pub invalidations: u64,
    /// Number of entries currently stored (approximate — TTL expiry is lazy).
    pub entries: u64,
    /// Hit rate as a percentage (0.0–100.0). Returns 0.0 if no lookups yet.
    pub hit_rate: f64,
}

// ---------------------------------------------------------------------------
// DataCache — the public cache interface
// ---------------------------------------------------------------------------

/// In-memory data cache for read query responses.
///
/// Wraps [`SvCache`] with table-level invalidation tracking. Created by
/// `build_app()` when `config.cache.enabled == true`.
///
/// # Usage
///
/// ```rust,ignore
/// let cache = DataCache::new(&config.cache);
///
/// // Compute key from a ReadPlan
/// if let Some(key) = cache.compute_key(&read_plan, &sql, &params, role.as_deref(), claims.as_ref()) {
///     // Try cache hit
///     if let Some(entry) = cache.get(&key) {
///         return use_cached(entry);
///     }
///     // After DB execution, store result
///     cache.store(&key, body, total_count, page_total);
/// }
///
/// // On mutation, invalidate
/// cache.invalidate_table(&mutate_plan.target);
/// ```
pub struct DataCache {
    /// The underlying svcache instance.
    store: SvCache<CachedEntry>,
    /// Whether list/collection queries are cacheable (not just PK lookups).
    cache_lists: bool,
    /// Per-table generation counters for scoped invalidation.
    /// A mutation on table T bumps its generation, making all existing entries
    /// for that table stale (their key no longer matches the current generation).
    table_generations: RwLock<HashMap<String, u64>>,
    /// Global generation counter — bumped by volatile RPCs that can affect anything.
    global_generation: AtomicU64,
    /// Atomic stats counters.
    stat_hits: AtomicU64,
    stat_misses: AtomicU64,
    stat_invalidations: AtomicU64,
}

impl DataCache {
    /// Create a new `DataCache` from configuration.
    pub fn new(config: &CacheConfig) -> Self {
        let store = SvCache::with_ttl_and_limit(
            Duration::from_secs(config.ttl_seconds),
            config.max_entries as usize,
        );

        Self {
            store,
            cache_lists: config.cache_lists,
            table_generations: RwLock::new(HashMap::new()),
            global_generation: AtomicU64::new(0),
            stat_hits: AtomicU64::new(0),
            stat_misses: AtomicU64::new(0),
            stat_invalidations: AtomicU64::new(0),
        }
    }

    /// Return a snapshot of current cache performance statistics.
    pub fn stats(&self) -> CacheStats {
        let hits = self.stat_hits.load(Ordering::Relaxed);
        let misses = self.stat_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            (hits as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        CacheStats {
            hits,
            misses,
            invalidations: self.stat_invalidations.load(Ordering::Relaxed),
            entries: self.store.len() as u64,
            hit_rate,
        }
    }

    /// Get the current generation for a table (combines table-specific + global).
    fn table_generation(&self, table_id: &str) -> u64 {
        let global = self.global_generation.load(Ordering::Relaxed);
        let table_gen = self
            .table_generations
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(table_id)
            .copied()
            .unwrap_or(0);
        global.wrapping_add(table_gen)
    }

    /// Compute a cache key for a read plan, or `None` if the query is not cacheable.
    ///
    /// # Cacheability rules
    ///
    /// A query is cacheable when:
    /// - It's a read (already guaranteed by the caller)
    /// - It has no embeds (cross-table invalidation is complex)
    /// - Either:
    ///   - It's a PK lookup (all PK cols filtered with `eq`, non-negated), OR
    ///   - `cache_lists` is enabled
    ///
    /// # Key identity
    ///
    /// The returned key incorporates the table's generation counter, role, a hash
    /// of claims + SQL + params. When a table's generation is bumped (on mutation),
    /// existing keys for that table no longer match, causing misses.
    pub fn compute_key(
        &self,
        plan: &ReadPlan,
        sql: &str,
        params: &[Value],
        role: Option<&str>,
        claims: Option<&Value>,
    ) -> Option<String> {
        // Don't cache queries with embeds
        if !plan.embeds.is_empty() {
            return None;
        }

        // Decide cacheability. PK lookups are cached by default; list queries
        // only when explicitly enabled.
        let is_pk = Self::is_pk_lookup(plan);
        if !is_pk && !self.cache_lists {
            return None;
        }

        let role_prefix = role.unwrap_or("anon");
        let table_id = format!("{}.{}", plan.target.schema, plan.target.name);
        let label = if is_pk { "pk" } else { "list" };

        // Include the table generation in the key so mutations auto-invalidate.
        let generation = self.table_generation(&table_id);

        // Hash the security context (claims) + the executed query (sql + params).
        // Uses a streaming hash of the Value tree to avoid allocating a String.
        let mut hasher = FnvHasher::new();
        if let Some(claims) = claims {
            hash_json_value(claims, &mut hasher);
        }
        sql.hash(&mut hasher);
        for p in params {
            hash_json_value(p, &mut hasher);
        }
        let hash = hasher.finish();

        Some(format!("{role_prefix}:{label}:{table_id}:g{generation}:{hash:016x}"))
    }

    /// Attempt to retrieve a cached entry by key. Tracks hit/miss stats.
    pub fn get(&self, key: &str) -> Option<CachedEntry> {
        let result = self.store.get_by_id(key.to_string());
        if result.is_some() {
            self.stat_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stat_misses.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Store a response in the cache under the given key.
    pub fn store(&self, key: &str, body: Value, total_count: Option<i64>, page_total: Option<i64>) {
        let entry = CachedEntry {
            key: key.to_string(),
            body,
            total_count,
            page_total,
        };

        self.store.insert(entry);
    }

    /// Invalidate all cached entries (for volatile RPCs that can affect anything).
    ///
    /// Bumps the global generation counter so all existing keys become stale.
    /// Stale entries are cleaned up by TTL/LRU eviction naturally.
    pub fn invalidate_all(&self) {
        self.global_generation.fetch_add(1, Ordering::Relaxed);
        self.stat_invalidations.fetch_add(1, Ordering::Relaxed);
    }

    /// Invalidate cached entries for a specific table.
    ///
    /// Bumps the table's generation counter, causing all cached entries for that
    /// table to become stale misses. Entries for other tables remain valid.
    /// This is safe even when cascading mutations exist: the TTL bounds staleness,
    /// and dependent tables can be invalidated explicitly if needed.
    pub fn invalidate_table(&self, table: &QualifiedIdentifier) {
        let table_id = format!("{}.{}", table.schema, table.name);
        let mut gens = self
            .table_generations
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let generation = gens.entry(table_id).or_insert(0);
        *generation = generation.wrapping_add(1);
        self.stat_invalidations.fetch_add(1, Ordering::Relaxed);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Whether `plan` is a primary-key lookup: every PK column is constrained by
    /// a non-negated `eq` against a single value.
    ///
    /// Used only to decide cacheability — the cache key itself hashes the
    /// rendered SQL (see [`compute_key`](Self::compute_key)), so it already
    /// accounts for any additional filters, `select`, ordering, and pagination.
    fn is_pk_lookup(plan: &ReadPlan) -> bool {
        let pk_cols = &plan.table_info.primary_key_columns;
        if pk_cols.is_empty() {
            return false;
        }

        pk_cols.iter().all(|pk_col| {
            plan.filters.iter().any(|f| {
                f.column == *pk_col
                    && matches!(f.operator, Operator::Eq)
                    && !f.negated
                    && matches!(
                        f.value,
                        pgvis_core::query_params::types::FilterValue::Single(_)
                    )
            })
        })
    }
}

// ---------------------------------------------------------------------------
// FnvHasher — fast non-cryptographic hash for internal cache keys
// ---------------------------------------------------------------------------

/// A simple FNV-1a hasher — faster than SipHash for non-adversarial data.
///
/// Used only for internal cache key computation where DoS resistance is not
/// needed (the inputs are server-generated SQL + trusted JWT claims).
struct FnvHasher(u64);

impl FnvHasher {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x00000100000001B3;

    fn new() -> Self {
        Self(Self::OFFSET_BASIS)
    }
}

impl Hasher for FnvHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// Hash a `serde_json::Value` into a `Hasher` without allocating a String.
///
/// Walks the value tree and hashes type tags + content directly, avoiding
/// the `value.to_string()` allocation that was previously in the hot path.
fn hash_json_value(value: &Value, hasher: &mut impl Hasher) {
    match value {
        Value::Null => 0u8.hash(hasher),
        Value::Bool(b) => {
            1u8.hash(hasher);
            b.hash(hasher);
        }
        Value::Number(n) => {
            2u8.hash(hasher);
            // Number's bit representation via to_string is the simplest way
            // to get consistent hashing (handles i64/u64/f64 variants).
            // This allocates, but numbers in claims are rare and small.
            n.to_string().hash(hasher);
        }
        Value::String(s) => {
            3u8.hash(hasher);
            s.hash(hasher);
        }
        Value::Array(arr) => {
            4u8.hash(hasher);
            arr.len().hash(hasher);
            for item in arr {
                hash_json_value(item, hasher);
            }
        }
        Value::Object(map) => {
            5u8.hash(hasher);
            map.len().hash(hasher);
            for (k, v) in map {
                k.hash(hasher);
                hash_json_value(v, hasher);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use pgvis_core::cache::QualifiedIdentifier;
    use pgvis_core::plan::types::{
        ReadPlan, ResolvedFilter, ResolvedRange, ResolvedSelect, ResolvedTableInfo,
    };
    use pgvis_core::preferences::Preferences;
    use pgvis_core::query_params::types::{FilterValue, Operator};

    fn make_config() -> CacheConfig {
        CacheConfig {
            enabled: true,
            ttl_seconds: 60,
            max_entries: 1000,
            cache_lists: false,
        }
    }

    fn make_pk_read_plan() -> ReadPlan {
        ReadPlan {
            target: QualifiedIdentifier::new("public", "users"),
            table_info: ResolvedTableInfo {
                is_view: false,
                insertable: true,
                updatable: true,
                deletable: true,
                primary_key_columns: vec!["id".to_string()],
            },
            select: vec![ResolvedSelect::Star],
            embeds: vec![],
            filters: vec![ResolvedFilter {
                column: "id".to_string(),
                json_path: vec![],
                operator: Operator::Eq,
                quantifier: None,
                value: FilterValue::Single("42".to_string()),
                negated: false,
                rewrite: None,
            }],
            order: vec![],
            range: ResolvedRange {
                limit: None,
                offset: None,
                cursor: None,
                cursor_column: None,
            },
            logic_filters: vec![],
            aggregates: vec![],
            count: None,
            preferences: Preferences::default(),
        }
    }

    fn make_list_read_plan() -> ReadPlan {
        ReadPlan {
            target: QualifiedIdentifier::new("public", "users"),
            table_info: ResolvedTableInfo {
                is_view: false,
                insertable: true,
                updatable: true,
                deletable: true,
                primary_key_columns: vec!["id".to_string()],
            },
            select: vec![ResolvedSelect::Star],
            embeds: vec![],
            filters: vec![ResolvedFilter {
                column: "active".to_string(),
                json_path: vec![],
                operator: Operator::Eq,
                quantifier: None,
                value: FilterValue::Single("true".to_string()),
                negated: false,
                rewrite: None,
            }],
            order: vec![],
            range: ResolvedRange {
                limit: Some(10),
                offset: None,
                cursor: None,
                cursor_column: None,
            },
            logic_filters: vec![],
            aggregates: vec![],
            count: None,
            preferences: Preferences::default(),
        }
    }

    #[test]
    fn test_pk_key_generation() {
        let cache = DataCache::new(&make_config());
        let plan = make_pk_read_plan();
        let sql = "SELECT * FROM public.users WHERE id = $1";
        let params = vec![serde_json::json!("42")];

        let key = cache.compute_key(&plan, sql, &params, Some("web_user"), None);
        let key = key.expect("PK lookup should be cacheable");
        assert!(key.starts_with("web_user:pk:public.users:"), "got {key}");
    }

    #[test]
    fn test_pk_key_anonymous() {
        let cache = DataCache::new(&make_config());
        let plan = make_pk_read_plan();
        let sql = "SELECT * FROM public.users WHERE id = $1";
        let params = vec![serde_json::json!("42")];

        let key = cache.compute_key(&plan, sql, &params, None, None);
        let key = key.expect("PK lookup should be cacheable");
        assert!(key.starts_with("anon:pk:public.users:"), "got {key}");
    }

    #[test]
    fn test_pk_key_distinguishes_query_shape() {
        // Regression: same PK, different rendered SQL (e.g. different `select`)
        // must NOT collide on one key.
        let cache = DataCache::new(&make_config());
        let plan = make_pk_read_plan();
        let params = vec![serde_json::json!("42")];

        let star = cache
            .compute_key(&plan, "SELECT * FROM public.users WHERE id = $1", &params, None, None)
            .unwrap();
        let projected = cache
            .compute_key(&plan, "SELECT id FROM public.users WHERE id = $1", &params, None, None)
            .unwrap();

        assert_ne!(star, projected);
    }

    #[test]
    fn test_key_distinguishes_claims() {
        // Regression: two users sharing a role but with different JWT claims
        // must get distinct keys (otherwise per-identity RLS leaks across users).
        let cache = DataCache::new(&make_config());
        let plan = make_pk_read_plan();
        let sql = "SELECT * FROM public.users WHERE id = $1";
        let params = vec![serde_json::json!("42")];

        let alice = serde_json::json!({"role": "authenticated", "sub": "alice"});
        let bob = serde_json::json!({"role": "authenticated", "sub": "bob"});

        let key_a = cache
            .compute_key(&plan, sql, &params, Some("authenticated"), Some(&alice))
            .unwrap();
        let key_b = cache
            .compute_key(&plan, sql, &params, Some("authenticated"), Some(&bob))
            .unwrap();

        assert_ne!(key_a, key_b);
    }

    #[test]
    fn test_list_not_cached_by_default() {
        let cache = DataCache::new(&make_config());
        let plan = make_list_read_plan();
        let sql = "SELECT * FROM public.users WHERE active = $1 LIMIT 10";
        let params = vec![serde_json::json!("true")];

        let key = cache.compute_key(&plan, sql, &params, Some("web_user"), None);
        assert_eq!(key, None); // cache_lists = false
    }

    #[test]
    fn test_list_cached_when_enabled() {
        let config = CacheConfig {
            cache_lists: true,
            ..make_config()
        };
        let cache = DataCache::new(&config);
        let plan = make_list_read_plan();
        let sql = "SELECT * FROM public.users WHERE active = $1 LIMIT 10";
        let params = vec![serde_json::json!("true")];

        let key = cache.compute_key(&plan, sql, &params, Some("web_user"), None);
        assert!(key.is_some());
        assert!(key.unwrap().starts_with("web_user:list:public.users:"));
    }

    #[test]
    fn test_embeds_not_cached() {
        let cache = DataCache::new(&make_config());
        let mut plan = make_pk_read_plan();
        // Add a fake embed to make it non-cacheable
        plan.embeds.push(pgvis_core::plan::types::EmbeddedResource {
            name: "orders".to_string(),
            alias: None,
            join: pgvis_core::plan::types::ResolvedJoin::Direct {
                source_columns: vec!["id".to_string()],
                target_columns: vec!["user_id".to_string()],
                target_table: QualifiedIdentifier::new("public", "orders"),
                cardinality: pgvis_core::cache::Cardinality::O2M,
            },
            join_type: None,
            plan: make_list_read_plan(),
            is_spread: false,
        });

        let sql = "SELECT ...";
        let params = vec![];
        let key = cache.compute_key(&plan, sql, &params, None, None);
        assert_eq!(key, None);
    }

    #[test]
    fn test_store_and_get() {
        let cache = DataCache::new(&make_config());
        let key = "anon:pk:public.users:id=42";

        cache.store(
            key,
            serde_json::json!([{"id": 42, "name": "Alice"}]),
            None,
            Some(1),
        );

        let entry = cache.get(key);
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.body, serde_json::json!([{"id": 42, "name": "Alice"}]));
        assert_eq!(entry.page_total, Some(1));
    }

    #[test]
    fn test_invalidate_table() {
        let cache = DataCache::new(&make_config());
        let plan = make_pk_read_plan();

        // Compute key before invalidation
        let key_before = cache
            .compute_key(&plan, "SELECT 1", &[], Some("user1"), None)
            .unwrap();

        cache.store(&key_before, serde_json::json!([{"id": 42}]), None, Some(1));
        assert!(cache.get(&key_before).is_some());

        // Invalidate the table — bumps its generation counter
        let table = QualifiedIdentifier::new("public", "users");
        cache.invalidate_table(&table);

        // Same logical query now computes a different key (new generation)
        let key_after = cache
            .compute_key(&plan, "SELECT 1", &[], Some("user1"), None)
            .unwrap();
        assert_ne!(key_before, key_after);

        // The new key has no entry — cache miss
        assert!(cache.get(&key_after).is_none());
        assert_eq!(cache.stats().invalidations, 1);
    }

    #[test]
    fn test_invalidate_clears_whole_store() {
        let config = CacheConfig {
            cache_lists: true,
            ..make_config()
        };
        let cache = DataCache::new(&config);
        let plan = make_pk_read_plan();

        // Compute key and store
        let key_before = cache
            .compute_key(&plan, "SELECT 1", &[], Some("user1"), None)
            .unwrap();
        cache.store(&key_before, serde_json::json!([{"id": 1}]), None, Some(1));
        assert!(cache.get(&key_before).is_some());

        // Global invalidation bumps global_generation, affecting all tables
        cache.invalidate_all();

        // Same query now computes a different key
        let key_after = cache
            .compute_key(&plan, "SELECT 1", &[], Some("user1"), None)
            .unwrap();
        assert_ne!(key_before, key_after);
        assert!(cache.get(&key_after).is_none());
        assert_eq!(cache.stats().invalidations, 1);
    }
}
