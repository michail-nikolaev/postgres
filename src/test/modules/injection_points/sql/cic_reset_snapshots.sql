CREATE EXTENSION injection_points;

SELECT injection_points_set_local();
-- Reset on every page boundary to make resets deterministic.
SET concurrent_index_reset_snapshot_interval = 0;
SELECT injection_points_attach('heap_reset_scan_snapshot_xmin_pinned', 'error');
SELECT injection_points_attach('heap_reset_scan_snapshot_xid_assigned', 'notice');
SELECT injection_points_attach('heap_beginscan_reset_snapshot', 'notice');
SELECT injection_points_attach('table_parallelscan_initialize_reset_snapshot', 'notice');
-- Nothing below deletes rows concurrently, so no uniqueness decision may end
-- up deferred to the validation phase.
SELECT injection_points_attach('bt_load_defer_unique_check', 'error');

CREATE SCHEMA cic_reset_snap;
-- Rows are padded with a non-compressible attribute to get a platform
-- independent layout of 7 rows per page: the 35-row table has 5 pages, so
-- with a zero interval every scan of it swaps the snapshot 5 times.
CREATE TABLE cic_reset_snap.tbl(i int primary key, j int, pad char(1004));
ALTER TABLE cic_reset_snap.tbl ALTER COLUMN pad SET STORAGE PLAIN;
INSERT INTO cic_reset_snap.tbl SELECT i, i * I, 'x' FROM generate_series(1, 35) s(i);

-- Prove rotations actually happen: count the swaps of one scan.
SELECT injection_points_attach('heap_reset_scan_snapshot_swap', 'notice');
CREATE INDEX CONCURRENTLY rotation_probe ON cic_reset_snap.tbl(i);
SELECT injection_points_detach('heap_reset_scan_snapshot_swap');
DROP INDEX CONCURRENTLY cic_reset_snap.rotation_probe;

CREATE FUNCTION cic_reset_snap.predicate_stable(integer) RETURNS bool IMMUTABLE
									  LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'SELECT txid_current()';
    RETURN MOD($1, 2) = 0;
END; $$;

CREATE FUNCTION cic_reset_snap.predicate_stable_no_param() RETURNS bool IMMUTABLE
									  LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'SELECT txid_current()';
    RETURN false;
END; $$;

----------------
ALTER TABLE cic_reset_snap.tbl SET (parallel_workers=0);

CREATE UNIQUE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(MOD(i, 2), j) WHERE MOD(i, 2) = 0;
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i, j) WHERE cic_reset_snap.predicate_stable(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i, j) WHERE cic_reset_snap.predicate_stable_no_param();
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl USING BRIN(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

-- A BRIN summary holds values of many heap pages at a time, so a value
-- stored out of line is fetched as it enters the summary rather than when
-- the range is written out.  The build therefore keeps rotating over such a
-- column exactly as it does over any other: the swaps reported below are one
-- per page of this five page table, for both of the builds.
CREATE TABLE cic_reset_snap.brin_toast(i int, t text, pad char(1004));
ALTER TABLE cic_reset_snap.brin_toast ALTER COLUMN t SET STORAGE EXTERNAL;
ALTER TABLE cic_reset_snap.brin_toast ALTER COLUMN pad SET STORAGE PLAIN;
ALTER TABLE cic_reset_snap.brin_toast SET (parallel_workers=0);
INSERT INTO cic_reset_snap.brin_toast
    SELECT g, repeat(chr(64 + g % 26), 4000), 'x' FROM generate_series(1, 35) g;
SELECT injection_points_attach('heap_reset_scan_snapshot_swap', 'notice');
CREATE INDEX CONCURRENTLY brin_toast_idx
    ON cic_reset_snap.brin_toast USING BRIN(t);
CREATE INDEX CONCURRENTLY brin_plain_idx
    ON cic_reset_snap.brin_toast USING BRIN(i);
SELECT injection_points_detach('heap_reset_scan_snapshot_swap');
SELECT count(*) FROM cic_reset_snap.brin_toast WHERE t > repeat('A', 4000);
DROP TABLE cic_reset_snap.brin_toast;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl USING HASH(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl USING GIST(point(i, j));
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl USING SPGIST(point(i, j));
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl USING GIN((ARRAY[i, j]));
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

-- The same in parallel mode
ALTER TABLE cic_reset_snap.tbl SET (parallel_workers=2);

-- Detach to keep test stable: during leader participation an xid may get
-- assigned at nondeterministic points of the shared scan.
SELECT injection_points_detach('heap_reset_scan_snapshot_xid_assigned');

CREATE UNIQUE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(MOD(i, 2), j) WHERE MOD(i, 2) = 0;
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i, j) WHERE cic_reset_snap.predicate_stable(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i, j) WHERE cic_reset_snap.predicate_stable_no_param();
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i DESC NULLS LAST);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl USING BRIN(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl USING GIN((ARRAY[i, j]));
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

BEGIN TRANSACTION;
CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i);
ROLLBACK ;

SET default_transaction_isolation = 'repeatable read';
CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;

SET default_transaction_isolation = serializable;
CREATE INDEX CONCURRENTLY idx ON cic_reset_snap.tbl(i);
REINDEX INDEX CONCURRENTLY cic_reset_snap.idx;
DROP INDEX CONCURRENTLY cic_reset_snap.idx;
RESET default_transaction_isolation;

-- Real duplicates must produce a clean unique-violation error through every
-- duplicate-detection path.  Each failed attempt leaves an invalid index
-- behind; drop it before the next one.
CREATE TABLE cic_reset_snap.dup(i int, j int);
ALTER TABLE cic_reset_snap.dup SET (parallel_workers=0);
INSERT INTO cic_reset_snap.dup SELECT i, i FROM generate_series(1, 1500) s(i);
INSERT INTO cic_reset_snap.dup VALUES (1500, 1500);
INSERT INTO cic_reset_snap.dup SELECT i, i FROM generate_series(1501, 3000) s(i);

-- The fail-fast check in the tuplesort comparator, in-memory sort.
CREATE UNIQUE INDEX CONCURRENTLY dup_idx ON cic_reset_snap.dup(i);
DROP INDEX cic_reset_snap.dup_idx;

-- The same with sort runs dumped (and compared) while the heap scan is
-- still in progress.
SET maintenance_work_mem = '64kB';
CREATE UNIQUE INDEX CONCURRENTLY dup_idx ON cic_reset_snap.dup(i);
RESET maintenance_work_mem;
DROP INDEX cic_reset_snap.dup_idx;

-- With the fail-fast check disabled the definitive check in _bt_load()
-- reports the duplicate.
SET index_build_duplicate_check_scale_factor = 0;
CREATE UNIQUE INDEX CONCURRENTLY dup_idx ON cic_reset_snap.dup(i);
RESET index_build_duplicate_check_scale_factor;
DROP INDEX cic_reset_snap.dup_idx;

-- INCLUDE columns switch tuple comparison to the per-column path.
CREATE UNIQUE INDEX CONCURRENTLY dup_idx ON cic_reset_snap.dup(i) INCLUDE (j);
DROP INDEX cic_reset_snap.dup_idx;

-- Parallel build: workers run the comparator check, the leader runs the
-- definitive one during the merge.  Any participant may hit the duplicate
-- first, so hide the varying CONTEXT line.
ALTER TABLE cic_reset_snap.dup SET (parallel_workers=2);
\set VERBOSITY terse
CREATE UNIQUE INDEX CONCURRENTLY dup_idx ON cic_reset_snap.dup(i);
\set VERBOSITY default
DROP TABLE cic_reset_snap.dup;

-- Duplicate NULLs must not abort the build unless NULLS NOT DISTINCT.
CREATE TABLE cic_reset_snap.dupnull(i int);
ALTER TABLE cic_reset_snap.dupnull SET (parallel_workers=0);
INSERT INTO cic_reset_snap.dupnull SELECT g FROM generate_series(1, 3000) g;
INSERT INTO cic_reset_snap.dupnull VALUES (NULL), (NULL);
CREATE UNIQUE INDEX CONCURRENTLY dupnull_idx ON cic_reset_snap.dupnull(i);
DROP INDEX CONCURRENTLY cic_reset_snap.dupnull_idx;
CREATE UNIQUE INDEX CONCURRENTLY dupnull_idx ON cic_reset_snap.dupnull(i) NULLS NOT DISTINCT;
DROP TABLE cic_reset_snap.dupnull;

-- Non-allequalimage index (numeric): the definitive duplicate check in
-- _bt_load() must use the btree comparators, not datum-image comparisons
-- (1500 and 1500.000 are equal but have different binary images).  Disable
-- the fail-fast comparator check so the _bt_load() path is the one that
-- detects the duplicate.
CREATE TABLE cic_reset_snap.dupn(n numeric);
ALTER TABLE cic_reset_snap.dupn SET (parallel_workers=0);
INSERT INTO cic_reset_snap.dupn SELECT g FROM generate_series(1, 1500) g;
INSERT INTO cic_reset_snap.dupn VALUES (1500.000);
INSERT INTO cic_reset_snap.dupn SELECT g FROM generate_series(1501, 3000) g;
SET index_build_duplicate_check_scale_factor = 0;
CREATE UNIQUE INDEX CONCURRENTLY dupn_idx ON cic_reset_snap.dupn(n);
RESET index_build_duplicate_check_scale_factor;
DROP TABLE cic_reset_snap.dupn;

-- Comparators of enum values added with BEFORE/AFTER take catalog snapshots
-- through syscache misses, setting MyProc->xmin during sort phases.  Force
-- constant misses with debug_discard_caches (where available) to verify that
-- snapshot-reset builds tolerate that.
CREATE TYPE cic_reset_snap.rainbow AS ENUM ('red','green','blue');
ALTER TYPE cic_reset_snap.rainbow ADD VALUE 'teal' BEFORE 'green';
CREATE TABLE cic_reset_snap.enum_tbl(c cic_reset_snap.rainbow);
ALTER TABLE cic_reset_snap.enum_tbl SET (parallel_workers=0);
INSERT INTO cic_reset_snap.enum_tbl
    SELECT (CASE WHEN g % 2 = 0 THEN 'teal' ELSE 'blue' END)::cic_reset_snap.rainbow
    FROM generate_series(1, 200) g;
SELECT (max_val::int > 0) AS discard_caches_enabled
  FROM pg_settings WHERE name = 'debug_discard_caches' \gset
\if :discard_caches_enabled
SET debug_discard_caches = 1;
\endif
CREATE INDEX CONCURRENTLY enum_idx ON cic_reset_snap.enum_tbl(c);
RESET debug_discard_caches;
DROP TABLE cic_reset_snap.enum_tbl;

-- An "immutable" function may open and leak a cursor; read-only SPI borrows
-- and registers the caller's (scan) snapshot, which then survives snapshot
-- resets.  The build must degrade to keeping xmin rather than fail.
CREATE FUNCTION cic_reset_snap.leak_cursor(i int, at int) RETURNS bool IMMUTABLE
LANGUAGE plpgsql AS $$
DECLARE c refcursor := 'cic_leaked_cursor_' || at;
BEGIN
    IF i = at THEN OPEN c FOR SELECT 1; END IF;
    RETURN true;
END; $$;

-- The leaked registration keeps xmin pinned across resets, so watch the
-- per-reset point instead of failing on it.
SELECT injection_points_detach('heap_reset_scan_snapshot_xmin_pinned');
SELECT injection_points_attach('heap_reset_scan_snapshot_xmin_pinned', 'notice');

-- Small table: no snapshot reset happens after the cursor is opened, so the
-- build simply keeps the snapshot the cursor holds.
CREATE TABLE cic_reset_snap.cur_small(i int);
ALTER TABLE cic_reset_snap.cur_small SET (parallel_workers=0);
INSERT INTO cic_reset_snap.cur_small SELECT g FROM generate_series(1, 10) g;
CREATE INDEX CONCURRENTLY cur_small_idx ON cic_reset_snap.cur_small(i)
    WHERE cic_reset_snap.leak_cursor(i, 5);
DROP TABLE cic_reset_snap.cur_small;

-- Larger table with a reset on every page: the leaked registration is seen
-- at the next snapshot reset.  Detach the point: the number of resets after
-- the cursor is opened depends on the page layout.
SELECT injection_points_detach('heap_reset_scan_snapshot_xmin_pinned');

CREATE TABLE cic_reset_snap.cur_tbl(i int);
ALTER TABLE cic_reset_snap.cur_tbl SET (parallel_workers=0);
INSERT INTO cic_reset_snap.cur_tbl SELECT g FROM generate_series(1, 5000) g;
CREATE INDEX CONCURRENTLY cur_idx ON cic_reset_snap.cur_tbl(i)
    WHERE cic_reset_snap.leak_cursor(i, 3000);
DROP TABLE cic_reset_snap.cur_tbl;

DROP SCHEMA cic_reset_snap CASCADE;

SELECT injection_points_detach('heap_beginscan_reset_snapshot');
SELECT injection_points_detach('table_parallelscan_initialize_reset_snapshot');
SELECT injection_points_detach('bt_load_defer_unique_check');

DROP EXTENSION injection_points;
