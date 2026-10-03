-- Foreign key relationships introspection query.
-- Ported from PostgREST SchemaCache.hs `allM2OandO2ORels`.
--
-- Parameters:
--   $1 = schemas text[] (e.g. ARRAY['public', 'api'])
--
-- Returns one row per foreign key constraint where at least one side
-- is in the specified schemas:
--   table_schema, table_name, foreign_table_schema, foreign_table_name,
--   is_self, constraint_name, columns (as JSON array of {source, target} pairs),
--   is_one_to_one
WITH
pks_uniques_cols AS (
  SELECT
    conrelid,
    array_agg(key ORDER BY key) AS cols
  FROM pg_constraint,
  LATERAL unnest(conkey) AS _(key)
  WHERE
    contype IN ('p', 'u')
    AND connamespace <> 'pg_catalog'::regnamespace
    -- PG18 temporal keys (WITHOUT OVERLAPS / PERIOD) match the range column by
    -- overlap, not equality: not usable as a plain key. to_jsonb keeps this valid
    -- before PG18, where pg_constraint has no conperiod column.
    AND NOT COALESCE((to_jsonb(pg_constraint)->>'conperiod')::bool, false)
  GROUP BY oid, conrelid
)
SELECT
  ns1.nspname::text AS table_schema,
  tab.relname::text AS table_name,
  ns2.nspname::text AS foreign_table_schema,
  other.relname::text AS foreign_table_name,
  traint.conrelid = traint.confrelid AS is_self,
  traint.conname::text AS constraint_name,
  (
    SELECT json_agg(json_build_object(
      'source', cols.attname::text,
      'target', refs.attname::text
    ) ORDER BY ord)
    FROM unnest(traint.conkey, traint.confkey) WITH ORDINALITY AS _(col, ref, ord)
    JOIN pg_attribute cols ON cols.attrelid = traint.conrelid AND cols.attnum = col
    JOIN pg_attribute refs ON refs.attrelid = traint.confrelid AND refs.attnum = ref
  ) AS columns,
  (
    SELECT array_agg(cols.attnum ORDER BY cols.attnum)
    FROM unnest(traint.conkey) AS _(col)
    JOIN pg_attribute cols ON cols.attrelid = traint.conrelid AND cols.attnum = col
  ) IN (SELECT cols FROM pks_uniques_cols WHERE conrelid = traint.conrelid) AS is_one_to_one
FROM pg_constraint traint
JOIN pg_namespace ns1 ON ns1.oid = traint.connamespace
JOIN pg_class tab ON tab.oid = traint.conrelid
JOIN pg_class other ON other.oid = traint.confrelid
JOIN pg_namespace ns2 ON ns2.oid = other.relnamespace
WHERE traint.contype = 'f'
AND traint.conparentid = 0
-- PG18 temporal keys (WITHOUT OVERLAPS / PERIOD) match the range column by
-- overlap, not equality: not usable as a plain key. to_jsonb keeps this valid
-- before PG18, where pg_constraint has no conperiod column.
AND NOT COALESCE((to_jsonb(traint)->>'conperiod')::bool, false)
AND (ns1.oid = ANY($1::regnamespace[]) OR ns2.oid = ANY($1::regnamespace[]))
ORDER BY traint.conrelid, traint.conname
