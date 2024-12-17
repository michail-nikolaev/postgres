# Copyright (c) 2024, PostgreSQL Global Development Group

# Test REINDEX CONCURRENTLY with concurrent modifications and HOT updates
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

Test::More->builder->todo_start('filesystem bug')
  if PostgreSQL::Test::Utils::has_wal_read_bug;

my ($node, $result);

#
# Test set-up
#
$node = PostgreSQL::Test::Cluster->new('RC_test');
$node->init;
$node->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node->append_conf('postgresql.conf', 'fsync = off');
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE TABLE tbl(i int primary key,
								c1 money default 0, c2 money default 0,
								c3 money default 0, updated_at timestamp)));
$node->safe_psql('postgres', q(CREATE INDEX CONCURRENTLY idx ON tbl(i, updated_at);));
# create sequence
$node->safe_psql('postgres', q(CREATE UNLOGGED SEQUENCE in_row_rebuild START 1 INCREMENT 1;));
$node->safe_psql('postgres', q(SELECT nextval('in_row_rebuild');));

# Create helper functions for predicate tests
$node->safe_psql('postgres', q(
	CREATE FUNCTION predicate_stable() RETURNS bool IMMUTABLE
	LANGUAGE plpgsql AS $$
	BEGIN
		EXECUTE 'SELECT txid_current()';
		RETURN true;
	END; $$;
));

$node->safe_psql('postgres', q(
	CREATE FUNCTION predicate_const(integer) RETURNS bool IMMUTABLE
	LANGUAGE plpgsql AS $$
	BEGIN
		RETURN MOD($1, 2) = 0;
	END; $$;
));

# Run CIC/RIC in different options concurrently with upserts
$node->pgbench(
	'--no-vacuum --client=30 --jobs=4 --exit-on-abort --transactions=2500',
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent operations with REINDEX/CREATE INDEX CONCURRENTLY',
	{
		'concurrent_ops' => q(
			SELECT pg_try_advisory_lock(42)::integer AS gotlock \gset
			\if :gotlock
				SELECT nextval('in_row_rebuild') AS last_value \gset
				\set variant random(0, 5)
				\set parallels random(0, 4)
				\if :last_value < 3
					ALTER TABLE tbl SET (parallel_workers=:parallels);
					\if :variant = 0
						CREATE INDEX CONCURRENTLY idx_2 ON tbl(i, updated_at);
					\elif :variant = 1
						CREATE INDEX CONCURRENTLY idx_2 ON tbl(i, updated_at) WHERE predicate_stable();
					\elif :variant = 2
						CREATE INDEX CONCURRENTLY idx_2 ON tbl(i, updated_at) WHERE MOD(i, 2) = 0;
					\elif :variant = 3
						CREATE INDEX CONCURRENTLY idx_2 ON tbl(i, updated_at) WHERE predicate_const(i);
					\elif :variant = 4
						CREATE INDEX CONCURRENTLY idx_2 ON tbl(predicate_const(i));
					\elif :variant = 5
						CREATE INDEX CONCURRENTLY idx_2 ON tbl(i, predicate_const(i), updated_at) WHERE predicate_const(i);
					\endif
					--\sleep 200 ms
					SELECT bt_index_check('idx_2', heapallindexed => true, checkunique => true);
					REINDEX INDEX CONCURRENTLY idx_2;
					--\sleep 200 ms
					SELECT bt_index_check('idx_2', heapallindexed => true, checkunique => true);
					DROP INDEX CONCURRENTLY idx_2;
				\endif
				SELECT pg_advisory_unlock(42);
			\else
				\set num random(1000, 100000)
				BEGIN;
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now())
					ON CONFLICT(i) DO UPDATE SET updated_at = now();
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now())
					ON CONFLICT(i) DO UPDATE SET updated_at = now();
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now())
					ON CONFLICT(i) DO UPDATE SET updated_at = now();
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now())
					ON CONFLICT(i) DO UPDATE SET updated_at = now();
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now())
					ON CONFLICT(i) DO UPDATE SET updated_at = now();
				SELECT setval('in_row_rebuild', 1);
				COMMIT;
			\endif
		)
	});

$node->safe_psql('postgres', q(TRUNCATE TABLE tbl;));

# Run CIC/RIC for unique index concurrently with upserts
$node->pgbench(
	'--no-vacuum --client=30 --jobs=4 --exit-on-abort --transactions=2500',
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent operations with REINDEX/CREATE INDEX CONCURRENTLY',
	{
		'concurrent_ops_unique_idx' => q(
			SELECT pg_try_advisory_lock(42)::integer AS gotlock \gset
			\if :gotlock
				SELECT nextval('in_row_rebuild') AS last_value \gset
				\set parallels random(0, 4)
				\if :last_value < 3
					ALTER TABLE tbl SET (parallel_workers=:parallels);
					CREATE UNIQUE INDEX CONCURRENTLY idx_2 ON tbl(i);
					--\sleep 200 ms
					SELECT bt_index_check('idx_2', heapallindexed => true, checkunique => true);
					REINDEX INDEX CONCURRENTLY idx_2;
					--\sleep 200 ms
					SELECT bt_index_check('idx_2', heapallindexed => true, checkunique => true);
					DROP INDEX CONCURRENTLY idx_2;
				\endif
				SELECT pg_advisory_unlock(42);
			\else
				\set num random(1, power(10, random(1, 5)))
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now())
					ON CONFLICT(i) DO UPDATE SET updated_at = now();
				SELECT setval('in_row_rebuild', 1);
			\endif
		)
	});

$node->stop;
done_testing();