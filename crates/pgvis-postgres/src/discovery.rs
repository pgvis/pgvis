//! Physical streaming discovery. Addresses are untrusted until allow-listed;
//! cluster identity checks happen AFTER authentication and are not that boundary.
use crate::pool::ManagedPool;
use ipnet::IpNet;
use pgvis_core::{config::ReplicaDiscoveryConfig, error::Error};
use std::collections::HashSet;
use std::net::IpAddr;
use tokio_postgres::{
    Config,
    config::{SslMode, TargetSessionAttrs},
    types::PgLsn,
};

pub(crate) struct Discovery {
    primary: Config,
    trusted: Vec<IpNet>,
    port: u16,
    pub config: ReplicaDiscoveryConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Endpoint {
    pub address: IpAddr,
    pub hostname: Option<String>,
    pub port: u16,
}
impl Endpoint {
    pub fn id(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
}

#[derive(Clone)]
pub(crate) struct PrimaryState {
    pub system_id: String,
    pub timeline: i64,
    pub lsn: u64,
}

pub(crate) struct Snapshot {
    pub primary: PrimaryState,
    pub endpoints: Vec<Endpoint>,
}

impl Discovery {
    pub fn new(dsn: &str, config: &ReplicaDiscoveryConfig) -> Result<Self, Error> {
        let invalid = |message: &str| Error::Introspection(message.into());
        if config.refresh_interval_ms == 0
            || config.stale_after_ms < config.refresh_interval_ms
            || config.max_nodes == 0
            || config.max_nodes > 64
        {
            return Err(invalid(
                "replica discovery requires positive intervals, stale_after >= refresh_interval and max_nodes in 1..=64",
            ));
        }
        let mut cfg = deadpool_postgres::Config::new();
        cfg.url = Some(dsn.to_owned());
        let primary = cfg
            .get_pg_config()
            .map_err(|_| invalid("invalid primary connection configuration"))?;
        if !config.allow_insecure && primary.get_ssl_mode() != SslMode::Require {
            return Err(invalid(
                "replica discovery requires primary sslmode=require (allow_insecure is development-only)",
            ));
        }
        let trusted = config
            .trusted_cidrs
            .iter()
            .map(|cidr| cidr.parse::<IpNet>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid("invalid replica discovery trusted_cidrs"))?;
        if trusted.is_empty() {
            return Err(invalid(
                "replica discovery requires trusted_cidrs before sending credentials",
            ));
        }
        let port = match config.port {
            Some(0) => return Err(invalid("replica discovery port must be positive")),
            Some(port) => port,
            None => {
                let ports = primary.get_ports();
                if ports.iter().any(|p| Some(p) != ports.first()) {
                    return Err(invalid(
                        "multi-port primary requires an explicit replica discovery listening port",
                    ));
                }
                ports.first().copied().unwrap_or(5432)
            }
        };
        Ok(Self {
            primary,
            trusted,
            port,
            config: config.clone(),
        })
    }

    /// Copy typed settings into a fresh config: tokio-postgres host()/port()
    /// APPEND, so mutating a clone would retain the primary/multihost targets.
    pub fn connection_config(&self, endpoint: &Endpoint) -> Config {
        let p = &self.primary;
        let mut c = Config::new();
        if let Some(v) = p.get_user() {
            c.user(v);
        }
        if let Some(v) = p.get_password() {
            c.password(v);
        }
        if let Some(v) = p.get_dbname() {
            c.dbname(v);
        }
        if let Some(v) = p.get_options() {
            c.options(v);
        }
        if let Some(v) = p.get_application_name() {
            c.application_name(v);
        }
        c.ssl_mode(p.get_ssl_mode())
            .ssl_negotiation(p.get_ssl_negotiation())
            .channel_binding(p.get_channel_binding())
            .keepalives(p.get_keepalives())
            .keepalives_idle(p.get_keepalives_idle());
        if let Some(v) = p.get_connect_timeout() {
            c.connect_timeout(*v);
        }
        if let Some(v) = p.get_tcp_user_timeout() {
            c.tcp_user_timeout(*v);
        }
        if let Some(v) = p.get_keepalives_interval() {
            c.keepalives_interval(v);
        }
        if let Some(v) = p.get_keepalives_retries() {
            c.keepalives_retries(v);
        }
        // hostaddr binds the network destination even when a hostname supplies
        // the TLS certificate name. Never use application_name or client_port.
        c.host(
            endpoint
                .hostname
                .clone()
                .unwrap_or_else(|| endpoint.address.to_string()),
        )
        .hostaddr(endpoint.address)
        .port(endpoint.port)
        .target_session_attrs(TargetSessionAttrs::ReadOnly);
        c
    }

    pub async fn snapshot(&self, pool: &ManagedPool) -> Result<Snapshot, &'static str> {
        let conn = pool.checkout().await.map_err(|_| "primary_unreachable")?;
        let primary = primary_state_on(conn.client()).await?;
        // Logical senders always have a logical slot; slotless physical senders
        // are supported. Only direct streaming connections appear in this view.
        let limit = (self.config.max_nodes + 1) as i64;
        let rows = conn
            .client()
            .query(
                "SELECT pg_catalog.host(r.client_addr) AS address, r.client_hostname AS hostname \
             FROM pg_catalog.pg_stat_replication r LEFT JOIN pg_catalog.pg_replication_slots s ON s.active_pid = r.pid \
             WHERE r.state = 'streaming' AND (s.slot_type IS NULL OR s.slot_type = 'physical') \
             ORDER BY r.client_addr, r.pid LIMIT $1",
                &[&limit],
            )
            .await
            .map_err(|_| "replication_metadata_unavailable")?;
        if rows.len() > self.config.max_nodes {
            return Err("discovery_capacity_exceeded");
        }
        let mut endpoints = Vec::with_capacity(rows.len());
        let mut seen = HashSet::new();
        for row in rows {
            let addr: Option<String> = row
                .try_get("address")
                .map_err(|_| "invalid_replica_address")?;
            let address: IpAddr = addr
                .ok_or("replica_address_masked_or_local_socket")?
                .parse()
                .map_err(|_| "invalid_replica_address")?;
            if address.is_unspecified()
                || address.is_multicast()
                || !self.trusted.iter().any(|net| net.contains(&address))
            {
                return Err("replica_address_not_trusted");
            }
            if !seen.insert(address) {
                return Err("ambiguous_replica_address");
            }
            let hostname: Option<String> = row
                .try_get("hostname")
                .map_err(|_| "invalid_replica_hostname")?;
            if hostname.as_deref().is_some_and(|s| !valid_hostname(s)) {
                return Err("invalid_replica_hostname");
            }
            endpoints.push(Endpoint {
                address,
                hostname,
                port: self.port,
            });
        }
        conn.recycle();
        Ok(Snapshot { primary, endpoints })
    }
}

pub(crate) async fn primary_state(pool: &ManagedPool) -> Result<PrimaryState, &'static str> {
    let conn = pool.checkout().await.map_err(|_| "primary_unreachable")?;
    let state = primary_state_on(conn.client()).await?;
    conn.recycle();
    Ok(state)
}

async fn primary_state_on(client: &tokio_postgres::Client) -> Result<PrimaryState, &'static str> {
    let row = client.query_one(
            "SELECT NOT pg_catalog.pg_is_in_recovery() AND pg_catalog.pg_has_role(current_user, 'pg_read_all_stats', 'USAGE') AS visible, \
             (pg_catalog.pg_control_system()).system_identifier::text AS system_id, \
             (pg_catalog.pg_control_checkpoint()).timeline_id::bigint AS timeline, pg_catalog.pg_current_wal_lsn() AS lsn", &[])
            .await.map_err(|_| "primary_metadata_unavailable")?;
    if !row.try_get::<_, bool>("visible").unwrap_or(false) {
        return Err("primary_metadata_not_visible");
    }
    let primary = PrimaryState {
        system_id: row
            .try_get("system_id")
            .map_err(|_| "primary_identity_unknown")?,
        timeline: row
            .try_get("timeline")
            .map_err(|_| "primary_timeline_unknown")?,
        lsn: row
            .try_get::<_, PgLsn>("lsn")
            .map_err(|_| "primary_lsn_unknown")?
            .into(),
    };
    Ok(primary)
}

fn valid_hostname(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 253
        && name
            .strip_suffix('.')
            .unwrap_or(name)
            .split('.')
            .all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && label.as_bytes()[0].is_ascii_alphanumeric()
                    && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
}

pub(crate) async fn check_discovered(
    pool: &ManagedPool,
    primary: &PrimaryState,
    max_lag: u64,
) -> Result<(), &'static str> {
    let conn = pool.checkout().await.map_err(|_| "unreachable")?;
    let row = conn.client().query_one(
        "SELECT pg_catalog.pg_is_in_recovery() AS recovery, (pg_catalog.pg_control_system()).system_identifier::text AS system_id, \
         pg_catalog.pg_last_wal_replay_lsn() AS lsn, \
         (SELECT received_tli::bigint FROM pg_catalog.pg_stat_wal_receiver WHERE status = 'streaming') AS timeline", &[])
        .await.map_err(|_| "replica_metadata_unavailable")?;
    if !row.try_get::<_, bool>("recovery").unwrap_or(false) {
        return Err("not_in_recovery");
    }
    if row.try_get::<_, String>("system_id").ok().as_ref() != Some(&primary.system_id) {
        return Err("cluster_mismatch");
    }
    if row.try_get::<_, Option<i64>>("timeline").ok().flatten() != Some(primary.timeline) {
        return Err("timeline_unknown_or_mismatch");
    }
    let lsn = row
        .try_get::<_, PgLsn>("lsn")
        .map_err(|_| "replay_lsn_unknown")?;
    if max_lag > 0 && primary.lsn.saturating_sub(lsn.into()) > max_lag {
        return Err("lagging");
    }
    conn.recycle();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> ReplicaDiscoveryConfig {
        ReplicaDiscoveryConfig {
            trusted_cidrs: vec!["127.0.0.0/8".into()],
            ..Default::default()
        }
    }
    #[test]
    fn discovery_requires_trust_and_tls() {
        assert!(Discovery::new("postgres://localhost/db", &config()).is_err());
        assert!(
            Discovery::new(
                "postgres://localhost/db?sslmode=require",
                &ReplicaDiscoveryConfig::default()
            )
            .is_err()
        );
        assert!(Discovery::new("postgres://localhost/db?sslmode=require", &config()).is_ok());
    }
    #[test]
    fn typed_endpoint_replaces_primary_targets_without_changing_credentials() {
        let d = Discovery::new("host=primary1,primary2 port=5432 user=alice password='p\\ass word' dbname=db options='-c search_path=public' sslmode=require target_session_attrs=read-write", &config()).unwrap();
        let e = Endpoint {
            address: "127.0.0.2".parse().unwrap(),
            hostname: Some("replica.example".into()),
            port: 6543,
        };
        let c = d.connection_config(&e);
        assert_eq!(
            c.get_hosts(),
            &[tokio_postgres::config::Host::Tcp("replica.example".into())]
        );
        assert_eq!(c.get_hostaddrs(), &[e.address]);
        assert_eq!(c.get_ports(), &[6543]);
        assert_eq!(c.get_password(), d.primary.get_password());
        assert_eq!(c.get_options(), d.primary.get_options());
        assert_eq!(c.get_target_session_attrs(), TargetSessionAttrs::ReadOnly);
        assert_eq!(c.get_ssl_mode(), SslMode::Require);
    }
    #[test]
    fn hostnames_and_ports_reject_ambiguous_metadata() {
        for host in [
            "bad name",
            "-bad",
            "bad/host",
            "x..y",
            "x' sslmode=disable",
            "",
        ] {
            assert!(!valid_hostname(host));
        }
        assert!(valid_hostname("replica-1.example."));
        assert!(
            Discovery::new(
                "host=a,b port=5432,5433 dbname=db sslmode=require",
                &config()
            )
            .is_err()
        );
    }
}
