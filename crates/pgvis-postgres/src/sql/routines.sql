-- Stored functions/procedures introspection query.
-- Ported from PostgREST SchemaCache.hs `funcsSqlQuery`.
-- Modified: uses json_agg(json_build_object(...)) for parameters.
--
-- Parameters:
--   $1 = schemas text[] (e.g. ARRAY['public', 'api'])
--   $2 = hoisted_settings text[] (function GUC settings to extract)
--
-- Returns one row per function with:
--   proc_schema, proc_name, proc_description, params (JSON),
--   return_type_schema, return_type_name, rettype_is_setof,
--   rettype_is_composite, rettype_is_composite_alias,
--   volatility, has_variadic, transaction_isolation_level, settings (JSON)
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
arguments AS (
  SELECT
    oid,
    coalesce(json_agg(json_build_object(
      'name', COALESCE(name, ''),
      'type', type::regtype::text,
      'required', idx <= (pronargs - pronargdefaults),
      'is_variadic', COALESCE(mode = 'v', FALSE)
    ) ORDER BY idx), '[]'::json) AS args,
    CASE COUNT(*) - COUNT(name) -- number of unnamed arguments
      WHEN 0 THEN true
      WHEN 1 THEN (array_agg(type))[1] IN ('bytea'::regtype, 'json'::regtype, 'jsonb'::regtype, 'text'::regtype, 'xml'::regtype)
      ELSE false
    END AS callable
  -- proargnames/proargmodes cover every argument; proargtypes only the inputs.
  -- Zip names with proallargtypes (all arguments, aligned) and keep the input
  -- modes, renumbering them: zipping with proargtypes gave an input the name
  -- of an OUT argument declared before it.
  FROM (
    SELECT p.oid, p.pronargs, p.pronargdefaults, a.name, a.type, a.mode,
           row_number() OVER (PARTITION BY p.oid ORDER BY a.ord) AS idx
    FROM pg_proc p,
         unnest(p.proargnames, COALESCE(p.proallargtypes, p.proargtypes::oid[]), p.proargmodes)
           WITH ORDINALITY AS a (name, type, mode, ord)
    WHERE a.type IS NOT NULL
      AND COALESCE(a.mode, 'i') IN ('i', 'b', 'v') -- only input arguments
  ) input_args
  GROUP BY oid
)
SELECT
  pn.nspname::text AS proc_schema,
  p.proname::text AS proc_name,
  d.description AS proc_description,
  COALESCE(a.args, '[]'::json) AS params,
  tn.nspname::text AS return_type_schema,
  COALESCE(comp.relname, t.typname)::text AS return_type_name,
  p.proretset AS rettype_is_setof,
  (t.typtype = 'c'
   OR COALESCE(proargmodes::text[] && '{t,b,o}', false)
  ) AS rettype_is_composite,
  bt.oid <> bt.base_type AS rettype_is_composite_alias,
  p.provolatile::text AS volatility,
  p.provariadic > 0 AS has_variadic,
  lower((regexp_split_to_array((regexp_split_to_array(iso_config, '='))[2], ','))[1]) AS transaction_isolation_level,
  COALESCE(func_settings.kvs, '[]'::json) AS settings
FROM pg_proc p
LEFT JOIN arguments a ON a.oid = p.oid
JOIN pg_namespace pn ON pn.oid = p.pronamespace
JOIN base_types bt ON bt.oid = p.prorettype
JOIN pg_type t ON t.oid = bt.base_type
JOIN pg_namespace tn ON tn.oid = t.typnamespace
LEFT JOIN pg_class comp ON comp.oid = t.typrelid
LEFT JOIN pg_description AS d ON d.objoid = p.oid AND d.classoid = 'pg_proc'::regclass
LEFT JOIN LATERAL unnest(proconfig) iso_config ON iso_config LIKE 'default_transaction_isolation%'
LEFT JOIN LATERAL (
  SELECT
    json_agg(json_build_object(
      'key', substr(setting, 1, strpos(setting, '=') - 1),
      'value', substr(setting, strpos(setting, '=') + 1)
    )) AS kvs
  FROM unnest(proconfig) setting
  WHERE setting ~ ANY($2)
) func_settings ON TRUE
WHERE t.oid <> 'trigger'::regtype AND COALESCE(a.callable, true)
AND prokind = 'f'
AND p.pronamespace = ANY($1::regnamespace[])
