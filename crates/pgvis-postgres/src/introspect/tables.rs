//! Tables + columns introspection query and row decoder.

use indexmap::IndexMap;
use pgvis_core::cache::{Column, QualifiedIdentifier, Table, UniqueConstraint};
use pgvis_core::error::Error;
use serde::Deserialize;
use tokio_postgres::Client;
use tokio_postgres::types::Type;

/// SQL query for tables introspection (loaded at compile time).
const TABLES_SQL: &str = include_str!("../sql/tables.sql");

/// Intermediate struct for deserialising the JSON column data from Postgres.
#[derive(Debug, Deserialize)]
struct ColumnJson {
    name: String,
    description: Option<String>,
    nullable: bool,
    #[serde(rename = "type")]
    typ: String,
    nominal_type: String,
    max_len: Option<i32>,
    default: Option<String>,
    is_generated: Option<bool>,
    is_updatable: Option<bool>,
    enum_values: Option<Vec<String>>,
}

/// Query all tables and views in the given schemas.
///
/// Returns an ordered map of `QualifiedIdentifier → Table`.
pub async fn query_tables(
    client: &Client,
    schemas: &[String],
) -> Result<IndexMap<QualifiedIdentifier, Table>, Error> {
    // Use prepare_typed to explicitly tell Postgres the param is TEXT[].
    // Without this, Postgres infers regnamespace[] from the cast in the SQL,
    // and tokio-postgres can't serialize String into regnamespace.
    let stmt = client
        .prepare_typed(TABLES_SQL, &[Type::TEXT_ARRAY])
        .await
        .map_err(|e| Error::Introspection(format!("tables query prepare failed: {e}")))?;
    let rows = client
        .query(&stmt, &[&schemas])
        .await
        .map_err(|e| Error::Introspection(format!("tables query failed: {e}")))?;

    let mut tables = IndexMap::new();

    for row in &rows {
        let schema: String = row.get("table_schema");
        let name: String = row.get("table_name");
        let description: Option<String> = row.get("table_description");
        let is_view: bool = row.get("is_view");
        let insertable: bool = row.get("insertable");
        let updatable: bool = row.get("updatable");
        let deletable: bool = row.get("deletable");
        let pk_cols: Vec<String> = row.get("pk_cols");

        // Decode columns from JSON
        let columns_json: serde_json::Value = row.get("columns");
        let column_rows: Vec<ColumnJson> = serde_json::from_value(columns_json).map_err(|e| {
            Error::Introspection(format!("failed to decode columns for {schema}.{name}: {e}"))
        })?;

        let mut columns = IndexMap::new();
        for (idx, col) in column_rows.iter().enumerate() {
            let is_pk = pk_cols.contains(&col.name);
            columns.insert(
                col.name.clone(),
                Column {
                    name: col.name.clone(),
                    description: col.description.clone(),
                    nullable: col.nullable,
                    is_generated: col.is_generated.unwrap_or(false),
                    updatable: col.is_updatable.unwrap_or(true),
                    typ: col.typ.clone(),
                    nominal_type: if col.nominal_type == col.typ {
                        None
                    } else {
                        Some(col.nominal_type.clone())
                    },
                    max_len: col.max_len,
                    default: col.default.clone(),
                    enum_values: col.enum_values.clone().unwrap_or_default(),
                    is_pk,
                    is_fk: false, // Will be set during relationship processing
                    ordinal: (idx as i32) + 1,
                },
            );
        }

        // Build unique constraints from PK (additional uniques require separate query)
        let unique_constraints = if pk_cols.is_empty() {
            Vec::new()
        } else {
            vec![UniqueConstraint {
                name: format!("{name}_pkey"),
                columns: pk_cols.clone(),
                is_pk: true,
            }]
        };

        let ident = QualifiedIdentifier::new(&schema, &name);
        tables.insert(
            ident.clone(),
            Table {
                ident,
                description,
                is_view,
                replica_readable: row.get("replica_readable"),
                insertable,
                updatable,
                deletable,
                pk_cols,
                unique_constraints,
                columns,
            },
        );
    }

    Ok(tables)
}
