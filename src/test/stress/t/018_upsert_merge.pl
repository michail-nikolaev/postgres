# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for unique indexes under concurrent upserts and MERGE
# while the unique index itself is rebuilt concurrently.
#
# INSERT ... ON CONFLICT resolves its arbiter through the unique index,
# and MERGE looks the target row up through it as well; REINDEX INDEX
# CONCURRENTLY and REINDEX TABLE CONCURRENTLY replace that index with a
# freshly built one, and REPACK (CONCURRENTLY) moves the rows out from
# under it.  If a rebuild misses a row that is being upserted at that
# moment, two rows with the same key end up in the table, which no
# longer has a working unique index -- so the readers count distinct
# keys, and amcheck runs with checkunique.
#
# All keys are pre-populated and never deleted, so every upsert and
# every MERGE takes its "matched" path: a unique violation or a missing
# row would be a bug, not a race, and either aborts pgbench.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled upsert/MERGE stress test');

my $duration = 6 * $stressval;

# A small key space, so that upserts collide constantly.
my $nkeys = 500;

my $node;

#
# Test set-up
#
$node = stress_init_node('upsert_merge');
# The surrogate primary key gives REPACK (CONCURRENTLY) a replica
# identity, leaving the unique index on id free to be rebuilt.
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE u(sid bigserial PRIMARY KEY, id int, cnt int NOT NULL,
		updated_at timestamp);
	CREATE UNIQUE INDEX u_id_uidx ON u(id);
	CREATE INDEX u_cnt_idx ON u(cnt);
	INSERT INTO u(id, cnt, updated_at)
		SELECT g, 0, now() FROM generate_series(1, $nkeys) g;
));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'upserts and MERGE with concurrent rebuilds of the unique index',
	{
		'concurrent_ops' => stress_ddl_gate(
			indent => "\t\t",
			ddl => [
				'REPACK (CONCURRENTLY) u;',
				'REINDEX INDEX CONCURRENTLY u_id_uidx;',
				'REINDEX TABLE CONCURRENTLY u;',
				[
					'-- A secondary index can come and go entirely; the',
					'-- unique index has to stay, since the upserts below',
					'-- need it as their arbiter.',
					'DROP INDEX CONCURRENTLY u_cnt_idx;',
					'CREATE INDEX CONCURRENTLY u_cnt_idx ON u(cnt);',
				],
			],
			post =>
			  "SELECT bt_index_check('u_id_uidx', heapallindexed => true, checkunique => true);",
			else => stress_workload(
				mutations => [
					qq(\\set k random(1, $nkeys)
					INSERT INTO u(id, cnt, updated_at) VALUES (:k, 1, now())
						ON CONFLICT (id) DO UPDATE
						SET cnt = u.cnt + 1, updated_at = now();
					\\sleep 1 ms),
					qq(\\set k random(1, $nkeys)
					MERGE INTO u USING (SELECT :k AS id) s ON u.id = s.id
						WHEN MATCHED THEN
							UPDATE SET cnt = u.cnt + 1, updated_at = now()
						WHEN NOT MATCHED THEN
							INSERT (id, cnt, updated_at) VALUES (s.id, 1, now());
					\\sleep 1 ms),
				],
				checks => [
					qq(-- The unique index must still hold exactly one row per key.
					SELECT stress_assert(cnt = $nkeys AND distinct_ids = $nkeys,
						format('rows=%s distinct=%s (want $nkeys each)', cnt, distinct_ids))
					FROM (SELECT COUNT(*) AS cnt, COUNT(DISTINCT id) AS distinct_ids
						FROM u) t;),
					qq(-- Reading a key back must find exactly one row.
					\\set k random(1, $nkeys)
					SELECT stress_assert(COUNT(*) = 1,
						format('key %s has %s rows', :k, COUNT(*)))
					FROM u WHERE id = :k;),
				],
			),
		),
	});

is( $node->safe_psql('postgres', q(SELECT COUNT(*) FROM u)),
	"$nkeys", 'no rows gained or lost after upsert/MERGE churn');

is( $node->safe_psql('postgres', q(SELECT COUNT(DISTINCT id) FROM u)),
	"$nkeys", 'every key still appears exactly once');

# The unique index must still be able to reject a duplicate.
my ($ret, $out, $err) =
  $node->psql('postgres', q(INSERT INTO u(id, cnt) VALUES (1, 0)));
isnt($ret, 0, 'unique index still rejects a duplicate key');
like($err, qr/duplicate key value violates unique constraint/,
	'unique violation reported');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('u_id_uidx', heapallindexed => true,
								 rootdescend => false, checkunique => true);
	SELECT bt_index_parent_check('u_pkey', heapallindexed => true,
								 rootdescend => false, checkunique => true);
	SELECT bt_index_parent_check('u_cnt_idx', heapallindexed => true);
));

$node->stop;

done_testing();
