//! Conservative, plan-based replica eligibility shared by all adapters.
use super::{ActionPlan, ReadPlan, ResolvedJoin, ResolvedSelect};
use crate::cache::{SchemaCache, Volatility};

/// Metadata is a routing hint, not proof that arbitrary database functions are
/// side-effect-free. Replica transactions must additionally enforce READ ONLY.
/// Views, foreign/nonpermanent tables, RLS, computed relationships and explicit casts stay on
/// the primary. Unknown/old cache metadata fails closed.
pub fn replica_read_eligible(plan: &ActionPlan, cache: &SchemaCache) -> bool {
    match plan {
        ActionPlan::Read(read) => read_eligible(read, cache),
        ActionPlan::Call(call) => {
            call.function_info.volatility != Volatility::Volatile
                && select_eligible(&call.returning)
        }
        _ => false,
    }
}

fn select_eligible(select: &[ResolvedSelect]) -> bool {
    select.iter().all(|item| match item {
        ResolvedSelect::Column(column) => column.cast.is_none(),
        // Custom aggregate result casts can execute user functions too.
        ResolvedSelect::Aggregate(_) => false,
        _ => true,
    })
}

fn read_eligible(read: &ReadPlan, cache: &SchemaCache) -> bool {
    cache
        .tables
        .get(&read.target)
        .is_some_and(|t| t.replica_readable)
        && select_eligible(&read.select)
        && read.embeds.iter().all(|embed| {
            let safe_join = match &embed.join {
                ResolvedJoin::Computed { .. } => false,
                ResolvedJoin::Junction { junction_table, .. } => cache
                    .tables
                    .get(junction_table)
                    .is_some_and(|t| t.replica_readable),
                ResolvedJoin::Direct { .. } => true,
            };
            safe_join && read_eligible(&embed.plan, cache)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Config,
        cache::{QualifiedIdentifier, Routine, Table},
        dialect::POSTGRES,
        plan::{ApiRequest, RequestMethod, plan_request},
        preferences::Preferences,
        select_ast::SelectItem,
    };
    fn request(target: &str, rpc: bool) -> ApiRequest {
        ApiRequest {
            schema: "public".into(),
            target: target.into(),
            method: RequestMethod::Get,
            is_rpc: rpc,
            select: vec![SelectItem::Star],
            filters: vec![],
            order: vec![],
            range: None,
            preferences: Preferences::default(),
            body: None,
            on_conflict: None,
            columns: None,
            logic_filters: vec![],
            cursor: None,
        }
    }
    #[test]
    fn old_or_unverified_table_metadata_stays_primary() {
        let table: Table = serde_json::from_value(serde_json::json!({
            "ident": {"schema":"public", "name":"items"}, "description":null,
            "is_view":false, "insertable":true, "updatable":true, "deletable":true,
            "pk_cols":[], "unique_constraints":[], "columns":{}
        }))
        .unwrap();
        let ident = table.ident.clone();
        let mut cache = SchemaCache::default();
        cache.tables.insert(ident.clone(), table);
        let plan = plan_request(
            &request("items", false),
            &cache,
            &POSTGRES,
            &Config::default(),
        )
        .unwrap();
        assert!(!replica_read_eligible(&plan, &cache));
        cache.tables.get_mut(&ident).unwrap().replica_readable = true;
        assert!(replica_read_eligible(&plan, &cache));
        let ActionPlan::Read(mut read) = plan.clone() else {
            unreachable!()
        };
        read.embeds.push(super::super::EmbeddedResource {
            name: "computed".into(),
            alias: None,
            join: ResolvedJoin::Computed {
                function_name: "computed".into(),
                function_schema: "public".into(),
                target_table: ident.clone(),
                cardinality: crate::cache::Cardinality::O2M,
            },
            join_type: None,
            plan: read.clone(),
            is_spread: false,
        });
        assert!(!replica_read_eligible(&ActionPlan::Read(read), &cache));
        let ActionPlan::Read(mut read) = plan.clone() else {
            unreachable!()
        };
        read.select = vec![ResolvedSelect::Column(super::super::ResolvedColumn {
            name: "id".into(),
            alias: None,
            json_path: vec![],
            cast: Some("custom_type".into()),
            data_type: "integer".into(),
            nullable: false,
        })];
        assert!(!replica_read_eligible(&ActionPlan::Read(read), &cache));

        cache.tables.clear();
        assert!(!replica_read_eligible(&plan, &cache));
    }
    #[test]
    fn resolved_rpc_volatility_controls_routing_for_get_and_post() {
        for volatility in [
            Volatility::Immutable,
            Volatility::Stable,
            Volatility::Volatile,
        ] {
            let mut cache = SchemaCache::default();
            let ident = QualifiedIdentifier::new("public", "f");
            cache.routines.insert(
                ident.clone(),
                vec![Routine {
                    ident,
                    description: None,
                    is_variadic: false,
                    params: vec![],
                    return_type: "integer".into(),
                    return_type_is_set: false,
                    return_type_is_composite: false,
                    volatility,
                    isolation_level: None,
                    settings: vec![],
                }],
            );
            for method in [RequestMethod::Get, RequestMethod::Post] {
                let mut req = request("f", true);
                req.method = method;
                let plan = plan_request(&req, &cache, &POSTGRES, &Config::default()).unwrap();
                assert_eq!(
                    replica_read_eligible(&plan, &cache),
                    volatility != Volatility::Volatile
                );
            }
        }
    }
}
