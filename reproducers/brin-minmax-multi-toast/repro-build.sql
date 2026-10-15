-- CREATE INDEX must not retain a pointer to the heap's external TOAST value.
CREATE TABLE build_case (id int, n numeric)
  WITH (autovacuum_enabled = false);

-- Keep the large numeric out of line, without compression.
ALTER TABLE build_case ALTER COLUMN n SET STORAGE EXTERNAL;

INSERT INTO build_case VALUES
  (1, 1),
  (2, ('0.' || repeat('1234567890', 500))::numeric);

CREATE INDEX build_case_idx ON build_case
  USING brin (n numeric_minmax_multi_ops) WITH (pages_per_range = 1);

SET enable_seqscan = off;

SELECT 'before delete' AS phase, count(*) FROM build_case WHERE n = 2;

-- Remove the large value and reclaim its TOAST chunks.
DELETE FROM build_case WHERE id = 2;

VACUUM build_case;

SET enable_seqscan = on;

SET enable_bitmapscan = off;

SELECT 'sequential after vacuum' AS phase, count(*)
  FROM build_case WHERE n = 2;

SET enable_seqscan = off;

SET enable_bitmapscan = on;

EXPLAIN (COSTS OFF) SELECT count(*) FROM build_case WHERE n = 2;

-- Affected versions fail here with "missing chunk number 0" instead of 0.
SELECT 'index after vacuum' AS phase, count(*) FROM build_case WHERE n = 2;
