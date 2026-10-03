-- Tables + Columns introspection query.
-- Ported from PostgREST SchemaCache.hs `tablesSqlQuery`.
-- Modified: uses json_agg(json_build_object(...)) instead of composite arrays
-- for tokio-postgres compatibility.
--
-- Parameters:
--   $1 = schemas text[] (e.g. ARRAY['public', 'api'])
--
-- Returns one row per table/view with:
--   table_schema, table_name, table_description, is_view,
--   insertable, updatable, deletable, pk_cols, columns (as JSON array)
WITH
-- Recursively resolve domain types to their base type
base_types AS (
  WITH RECURSIVE
  recurse AS (
    SELECT
      oid,
      typbasetype,
      typnamespace AS base_namespace,
      COALESCE(NULLIF(typbasetype, 0), oid) AS base_type
    FROM pg_type
    UNION
    SELECT
      t.oid,
      b.typbasetype,
      b.typnamespace AS base_namespace,
      COALESCE(NULLIF(b.typbasetype, 0), b.oid) AS base_type
    FROM recurse t
    JOIN pg_type b ON t.typbasetype = b.oid
  )
  SELECT
    oid,
    base_namespace,
    base_type
  FROM recurse
  WHERE typbasetype = 0
),
columns AS (
    SELECT
        c.oid AS relid,
        a.attname::text AS column_name,
        d.description AS description,
        -- Handle defaults: domain defaults, identity columns, generated columns
        CASE
          WHEN (t.typbasetype != 0) AND (ad.adbin IS NULL) THEN pg_get_expr(t.typdefaultbin, 0)
          WHEN a.attidentity  = 'd' THEN format('nextval(%L)', seq.objid::regclass)
          WHEN a.attgenerated <> '' THEN null
          ELSE pg_get_expr(ad.adbin, ad.adrelid)::text
        END AS column_default,
        -- PG18 NOT NULL constraints can be NOT VALID: existing rows may hold NULL.
        NOT (
          a.attnotnull AND NOT EXISTS (
            SELECT 1 FROM pg_constraint nn
            WHERE nn.contype = 'n' AND nn.conrelid = a.attrelid
              AND nn.conkey[1] = a.attnum AND NOT nn.convalidated
          )
          OR t.typtype = 'd' AND t.typnotnull
        ) AS is_nullable,
        CASE
            WHEN t.typtype = 'd' THEN
            CASE
                WHEN bt.base_namespace = 'pg_catalog'::regnamespace THEN format_type(bt.base_type, NULL::integer)
                ELSE format_type(a.atttypid, a.atttypmod)
            END
            ELSE
            CASE
                WHEN t.typnamespace = 'pg_catalog'::regnamespace THEN format_type(a.atttypid, NULL::integer)
                ELSE format_type(a.atttypid, a.atttypmod)
            END
        END::text AS data_type,
        format_type(a.atttypid, a.atttypmod)::text AS nominal_data_type,
        information_schema._pg_char_max_length(
            information_schema._pg_truetypid(a.*, t.*),
            information_schema._pg_truetypmod(a.*, t.*)
        )::integer AS character_maximum_length,
        a.attgenerated <> '' AS is_generated,
        -- A column is updatable if not generated and table permits it
        a.attgenerated = '' AND a.attidentity != 'a' AS is_updatable,
        bt.base_type,
        a.attnum::integer AS position
    FROM pg_attribute a
        LEFT JOIN pg_description AS d
            ON d.objoid = a.attrelid AND d.objsubid = a.attnum AND d.classoid = 'pg_class'::regclass
        LEFT JOIN pg_attrdef ad
            ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
        JOIN pg_class c
            ON a.attrelid = c.oid
        JOIN pg_type t
            ON a.atttypid = t.oid
        LEFT JOIN base_types bt
            ON t.oid = bt.oid
        LEFT JOIN pg_depend seq
            ON seq.refobjid = a.attrelid AND seq.refobjsubid = a.attnum AND seq.deptype = 'i'
    WHERE
        NOT pg_is_other_temp_schema(c.relnamespace)
        AND a.attnum > 0
        AND NOT a.attisdropped
        AND c.relkind IN ('r', 'v', 'f', 'm', 'p')
        AND c.relnamespace = ANY($1::regnamespace[])
),
-- Aggregate columns as JSON array per table (instead of composite arrays)
columns_agg AS (
  SELECT
    relid,
    coalesce(json_agg(json_build_object(
      'name', column_name,
      'description', description,
      'nullable', is_nullable,
      'type', data_type,
      'nominal_type', nominal_data_type,
      'max_len', character_maximum_length,
      'default', column_default,
      'is_generated', is_generated,
      'is_updatable', is_updatable,
      'enum_values', coalesce(
        (SELECT json_agg(enumlabel ORDER BY enumsortorder) FROM pg_enum WHERE enumtypid = base_type),
        '[]'::json
      )
    ) ORDER BY position), '[]'::json) AS columns
  FROM columns
  GROUP BY relid
),
tbl_pk_cols AS (
  SELECT
    r.oid AS relid,
    array_agg(a.attname::text ORDER BY a.attname) AS pk_cols
  FROM pg_class r
  JOIN pg_constraint c
    ON r.oid = c.conrelid
  JOIN pg_attribute a
    ON a.attrelid = r.oid AND a.attnum = ANY (c.conkey)
  WHERE
    c.contype IN ('p')
    -- PG18 temporal keys (WITHOUT OVERLAPS / PERIOD) match the range column by
    -- overlap, not equality: not usable as a plain key. to_jsonb keeps this valid
    -- before PG18, where pg_constraint has no conperiod column.
    AND NOT COALESCE((to_jsonb(c)->>'conperiod')::bool, false)
    AND r.relkind IN ('r', 'p')
    AND r.relnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)
    AND NOT pg_is_other_temp_schema(r.relnamespace)
    AND NOT a.attisdropped
  GROUP BY r.oid
)
SELECT
  n.nspname::text AS table_schema,
  c.relname::text AS table_name,
  d.description AS table_description,
  c.relkind IN ('v','m') AS is_view,
  (
    c.relkind IN ('r','p')
    OR (
      c.relkind IN ('v','f')
      AND (pg_relation_is_updatable(c.oid::regclass, TRUE) & 8) = 8
    )
  ) AS insertable,
  (
    c.relkind IN ('r','p')
    OR (
      c.relkind IN ('v','f')
      AND (pg_relation_is_updatable(c.oid::regclass, TRUE) & 4) = 4
    )
  ) AS updatable,
  (
    c.relkind IN ('r','p')
    OR (
      c.relkind IN ('v','f')
      AND (pg_relation_is_updatable(c.oid::regclass, TRUE) & 16) = 16
    )
  ) AS deletable,
  coalesce(tpks.pk_cols, '{}') AS pk_cols,
  coalesce(cols_agg.columns, '[]'::json) AS columns
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0 AND d.classoid = 'pg_class'::regclass
LEFT JOIN tbl_pk_cols tpks ON c.oid = tpks.relid
LEFT JOIN columns_agg cols_agg ON c.oid = cols_agg.relid
WHERE c.relkind IN ('v','r','m','f','p')
AND c.relnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)
AND NOT c.relispartition
AND c.relnamespace = ANY($1::regnamespace[])
ORDER BY table_schema, table_name
