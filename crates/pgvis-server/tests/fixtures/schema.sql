-- pgvis test schema
-- Creates tables, views, functions for integration testing.
-- Run against a Postgres database before running integration tests.

-- Clean up previous test schema if exists
DROP SCHEMA IF EXISTS test CASCADE;
CREATE SCHEMA test;

SET search_path = test, public;

-- ============================================================================
-- Basic tables
-- ============================================================================

CREATE TABLE test.items (
    id serial PRIMARY KEY,
    name text NOT NULL,
    price numeric(10,2) NOT NULL DEFAULT 0,
    category text,
    description text,
    in_stock boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE test.items IS 'Catalog items for sale';
COMMENT ON COLUMN test.items.name IS 'Display name of the item';
COMMENT ON COLUMN test.items.price IS 'Price in USD';

CREATE TABLE test.users (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text UNIQUE,
    role text NOT NULL DEFAULT 'user',
    age integer,
    data jsonb DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE test.users IS 'Application users';

CREATE TABLE test.orders (
    id serial PRIMARY KEY,
    user_id integer NOT NULL REFERENCES test.users(id),
    total numeric(10,2) NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'pending',
    notes text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE test.order_items (
    order_id integer NOT NULL REFERENCES test.orders(id),
    item_id integer NOT NULL REFERENCES test.items(id),
    quantity integer NOT NULL DEFAULT 1,
    PRIMARY KEY (order_id, item_id)
);

COMMENT ON TABLE test.order_items IS 'Junction table for orders ↔ items (M2M)';

CREATE TABLE test.projects (
    id serial PRIMARY KEY,
    name text NOT NULL,
    owner_id integer NOT NULL REFERENCES test.users(id),
    budget numeric(12,2),
    status text NOT NULL DEFAULT 'active',
    metadata jsonb DEFAULT '{}'::jsonb
);

CREATE TABLE test.tasks (
    id serial PRIMARY KEY,
    project_id integer NOT NULL REFERENCES test.projects(id),
    title text NOT NULL,
    done boolean NOT NULL DEFAULT false,
    priority integer NOT NULL DEFAULT 0,
    assigned_to integer REFERENCES test.users(id)
);

-- ============================================================================
-- Type testing tables
-- ============================================================================

CREATE TABLE test.menagerie (
    id serial PRIMARY KEY,
    col_int2 smallint,
    col_int4 integer,
    col_int8 bigint,
    col_float4 real,
    col_float8 double precision,
    col_numeric numeric(20,5),
    col_bool boolean,
    col_text text,
    col_varchar varchar(100),
    col_char char(10),
    col_uuid uuid,
    col_date date,
    col_time time,
    col_timestamp timestamp,
    col_timestamptz timestamptz,
    col_interval interval,
    col_json json,
    col_jsonb jsonb,
    col_text_arr text[],
    col_int_arr integer[],
    col_bytea bytea
);

CREATE TABLE test.json_data (
    id serial PRIMARY KEY,
    data jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata json
);

-- ============================================================================
-- Tables for edge cases
-- ============================================================================

CREATE TABLE test.no_pk (
    a text,
    b integer
);

CREATE TABLE test.compound_pk (
    k1 integer NOT NULL,
    k2 text NOT NULL,
    value text,
    PRIMARY KEY (k1, k2)
);

CREATE TABLE test.empty_table (
    id serial PRIMARY KEY,
    name text
);

CREATE TABLE test.unicode_data (
    id serial PRIMARY KEY,
    label text NOT NULL,
    description text
);

CREATE TABLE test.nullable_cols (
    id serial PRIMARY KEY,
    required_col text NOT NULL,
    optional_col text,
    optional_int integer,
    optional_bool boolean
);

-- ============================================================================
-- Views
-- ============================================================================

CREATE VIEW test.items_view AS
    SELECT id, name, price, category, in_stock FROM test.items;

CREATE VIEW test.expensive_items AS
    SELECT * FROM test.items WHERE price > 50;

-- ============================================================================
-- Functions (RPC)
-- ============================================================================

CREATE FUNCTION test.add(a integer, b integer)
RETURNS integer
LANGUAGE sql STABLE
AS $$ SELECT a + b $$;

COMMENT ON FUNCTION test.add IS 'Add two integers';

CREATE FUNCTION test.get_items()
RETURNS SETOF test.items
LANGUAGE sql STABLE
AS $$ SELECT * FROM test.items $$;

CREATE FUNCTION test.get_item(item_id integer)
RETURNS test.items
LANGUAGE sql STABLE
AS $$ SELECT * FROM test.items WHERE id = item_id $$;

CREATE FUNCTION test.search_items(query text)
RETURNS SETOF test.items
LANGUAGE sql STABLE
AS $$ SELECT * FROM test.items WHERE name ILIKE '%' || query || '%' $$;

CREATE FUNCTION test.void_function()
RETURNS void
LANGUAGE sql VOLATILE
AS $$ SELECT NULL::void $$;

-- An OUT argument declared before the input: introspection must still name
-- the input `x` (zipping names with input-only types called it `doubled`).
CREATE FUNCTION test.out_first(OUT doubled integer, IN x integer)
LANGUAGE sql STABLE
AS $$ SELECT x * 2 $$;

-- Functions that reject the request: plain RAISE (P0001 → 400) and a
-- PostgREST-style custom status (PT402 → 402).
CREATE FUNCTION test.raise_rejected()
RETURNS void LANGUAGE plpgsql
AS $$ BEGIN RAISE EXCEPTION 'rejected'; END $$;

CREATE FUNCTION test.raise_payment()
RETURNS void LANGUAGE plpgsql
AS $$ BEGIN RAISE EXCEPTION 'payment required' USING ERRCODE = 'PT402'; END $$;

CREATE FUNCTION test.echo_params(name text DEFAULT 'world', greeting text DEFAULT 'hello')
RETURNS text
LANGUAGE sql STABLE
AS $$ SELECT greeting || ', ' || name || '!' $$;

CREATE FUNCTION test.get_json()
RETURNS jsonb
LANGUAGE sql STABLE
AS $$ SELECT '{"key": "value", "count": 42}'::jsonb $$;

CREATE FUNCTION test.sleep(seconds float DEFAULT 0.1)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
BEGIN
    PERFORM pg_sleep(seconds);
END;
$$;

-- Overloads resolved by argument names.
CREATE FUNCTION test.overloaded(a integer)
RETURNS text
LANGUAGE sql STABLE
AS $$ SELECT 'one:' || a $$;

CREATE FUNCTION test.overloaded(a integer, b integer)
RETURNS text
LANGUAGE sql STABLE
AS $$ SELECT 'two:' || (a + b) $$;

-- Both overloads accept {"a": …}: ambiguous.
CREATE FUNCTION test.ambiguous(a integer)
RETURNS integer
LANGUAGE sql STABLE
AS $$ SELECT a $$;

CREATE FUNCTION test.ambiguous(a integer, b integer DEFAULT 0)
RETURNS integer
LANGUAGE sql STABLE
AS $$ SELECT a + b $$;

CREATE FUNCTION test.sum_variadic(label text, VARIADIC nums integer[])
RETURNS text
LANGUAGE sql STABLE
AS $$ SELECT label || (SELECT sum(n) FROM unnest(nums) AS n) $$;

-- ============================================================================
-- Pub/sub authorization (tests/pubsub.rs)
-- ============================================================================
-- Kept out of the exposed `test` schema so it is not an RPC endpoint.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgvis_test_anon') THEN
        CREATE ROLE pgvis_test_anon NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgvis_test_user') THEN
        CREATE ROLE pgvis_test_user NOLOGIN;
    END IF;
END
$$;

DROP SCHEMA IF EXISTS test_pubsub CASCADE;
CREATE SCHEMA test_pubsub;
GRANT USAGE ON SCHEMA test_pubsub TO pgvis_test_anon, pgvis_test_user;

-- Every authorization check, recorded as the role and `sub` it ran under.
CREATE TABLE test_pubsub.audit (
    channel text NOT NULL,
    op text NOT NULL,
    role name NOT NULL DEFAULT current_user,
    sub text DEFAULT nullif(current_setting('request.jwt.claims', true), '')::json->>'sub'
);
GRANT INSERT ON test_pubsub.audit TO pgvis_test_anon, pgvis_test_user;

-- public.*         anyone may subscribe; only pgvis_test_user may publish
-- private.<sub>.*  only the caller whose JWT `sub` is <sub>
-- raise.*          raises (an error denies)
-- anything else    NULL (denied)
CREATE FUNCTION test_pubsub.authorize(channel text, op text)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    sub text := nullif(current_setting('request.jwt.claims', true), '')::json->>'sub';
BEGIN
    INSERT INTO test_pubsub.audit (channel, op) VALUES (channel, op);
    IF channel LIKE 'raise.%' THEN
        RAISE EXCEPTION 'pubsub authorize: forbidden';
    ELSIF channel LIKE 'public.%' THEN
        RETURN op = 'subscribe' OR current_user = 'pgvis_test_user';
    ELSIF channel LIKE 'private.%' THEN
        RETURN split_part(channel, '.', 2) = sub;
    END IF;
    RETURN NULL;
END;
$$;
REVOKE EXECUTE ON FUNCTION test_pubsub.authorize(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION test_pubsub.authorize(text, text) TO pgvis_test_anon, pgvis_test_user;

-- ============================================================================
-- PostgreSQL 18+ catalog features (tests/pg18.rs). Skipped on older servers.
-- ============================================================================
DO $$
BEGIN
    IF current_setting('server_version_num')::int < 180000 THEN
        RETURN;
    END IF;
    CREATE EXTENSION IF NOT EXISTS btree_gist;
    -- Virtual generated columns are the PG18 default kind (attgenerated 'v').
    EXECUTE $ddl$
        CREATE TABLE test.pg18_items (
            id serial PRIMARY KEY,
            price integer NOT NULL,
            doubled integer GENERATED ALWAYS AS (price * 2) VIRTUAL,
            note text
        )
    $ddl$;
    -- A NOT VALID not-null constraint: existing rows may still hold NULL.
    EXECUTE 'INSERT INTO test.pg18_items (price, note) VALUES (1, NULL)';
    EXECUTE 'ALTER TABLE test.pg18_items ADD CONSTRAINT note_nn NOT NULL note NOT VALID';
    -- A temporal primary key: rows match by overlap on the range column.
    EXECUTE $ddl$
        CREATE TABLE test.pg18_bookings (
            room integer,
            during tstzrange,
            PRIMARY KEY (room, during WITHOUT OVERLAPS)
        )
    $ddl$;
END
$$;
