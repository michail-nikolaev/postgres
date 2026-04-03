CREATE EXTENSION injection_points;

SELECT injection_points_set_local();
-- Reset on every page boundary to make resets deterministic.
SET concurrent_index_reset_snapshot_interval = 0;
SELECT injection_points_attach('heap_reset_scan_snapshot_xmin_pinned', 'error');
SELECT injection_points_attach('heap_reset_scan_snapshot_xid_assigned', 'notice');
SELECT injection_points_attach('heap_beginscan_reset_snapshot', 'notice');


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

DROP SCHEMA cic_reset_snap CASCADE;

SELECT injection_points_detach('heap_beginscan_reset_snapshot');

DROP EXTENSION injection_points;
