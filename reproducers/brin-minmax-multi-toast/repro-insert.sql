-- INSERT ... SELECT must not retain a pointer to the source's TOAST value.
CREATE TABLE source_case (id int, n numeric)
  WITH (autovacuum_enabled = false);

ALTER TABLE source_case ALTER COLUMN n SET STORAGE EXTERNAL;

INSERT INTO source_case VALUES
  (2, ('0.' || repeat('1234567890', 500))::numeric);

CREATE TABLE insert_case (id int, n numeric)
  WITH (autovacuum_enabled = false);

ALTER TABLE insert_case ALTER COLUMN n SET STORAGE EXTERNAL;

INSERT INTO insert_case VALUES (1, 1);

CREATE INDEX insert_case_idx ON insert_case
  USING brin (n numeric_minmax_multi_ops) WITH (pages_per_range = 1);

INSERT INTO insert_case SELECT * FROM source_case;

SET enable_seqscan = off;

SELECT 'before source delete' AS phase, count(*)
  FROM insert_case WHERE n = 2;

-- The destination is unchanged; only the source's TOAST chunks are reclaimed.
DELETE FROM source_case;

VACUUM source_case;

SET enable_seqscan = on;

SET enable_bitmapscan = off;

-- Detoasting the destination's numeric still succeeds (lengths 1 and 5002).
SELECT id, length(n::text) AS text_length FROM insert_case ORDER BY id;

SELECT 'sequential after source vacuum' AS phase, count(*)
  FROM insert_case WHERE n = 2;

SET enable_seqscan = off;

SET enable_bitmapscan = on;

EXPLAIN (COSTS OFF) SELECT count(*) FROM insert_case WHERE n = 2;

-- Affected versions fail here with "missing chunk number 0" instead of 0.
SELECT 'index after source vacuum' AS phase, count(*)
  FROM insert_case WHERE n = 2;
