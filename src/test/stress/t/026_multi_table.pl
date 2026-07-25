# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for several CONCURRENTLY commands running at the same
# time on different tables.
#
# Every other test in this suite serializes its DDL behind a single
# advisory lock, so only one such command is ever in flight.  Here each
# table gets its own DDL client, so several REPACK (CONCURRENTLY)
# commands can overlap -- each of which takes a transient logical
# replication slot of its own, and each of which switches logical
# decoding on and off.  Overlapping activations of logical decoding are
# exactly the kind of thing that goes wrong quietly.
#
# Writers move value between two rows of one table, so each table's sum
# is invariant on its own.  Readers check those sums, and also compare
# an index scan against a sequential scan of the same predicate within
# one snapshot: the two must agree, or an index does not match its
# heap.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled multi-table stress test');

my $duration = 6 * $stressval;
my $ntables = 4;
my $nrows = 2000;

my $node;

#
# Test set-up.  wal_level = replica, so the slots taken by REPACK
# really do toggle logical decoding.
#
$node = stress_init_node('multi_table',
	init => { allows_streaming => 1 },
	extra_conf => [ 'max_connections = 50' ]);

$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
foreach my $t (1 .. $ntables)
{
	$node->safe_psql(
		'postgres', qq(
		CREATE TABLE tbl$t(id int PRIMARY KEY, val int);
		CREATE INDEX tbl_val_idx_$t ON tbl$t(val);
		INSERT INTO tbl$t SELECT g, g FROM generate_series(1, $nrows) g;
	));
}

# Every table starts with the same contents, so one expected sum does
# for all of them.
my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl1));

# Each client picks a role from its client id: the first $ntables
# clients drive DDL on one table each, the rest read and write.
$node->pgbench(
	"--no-vacuum --client=30 --jobs=30 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'several CONCURRENTLY commands at once on different tables',
	{
		# Each client picks a table and takes that table's own advisory
		# lock, so several CONCURRENTLY commands can be in flight at once
		# on different tables.
		'concurrent_ops' => qq(
		\\set t random(1, $ntables)
) . stress_ddl_gate(
			lock => ':t',
			ddl => [
				'REPACK (CONCURRENTLY) tbl:t;',
				'REINDEX INDEX CONCURRENTLY tbl_val_idx_:t;',
				'REINDEX TABLE CONCURRENTLY tbl:t;',
				[
					'DROP INDEX CONCURRENTLY tbl_val_idx_:t;',
					'CREATE INDEX CONCURRENTLY tbl_val_idx_:t ON tbl:t(val);'
				],
			],
			else => stress_workload(
				mutations => [
					qq(\\set num_a random(1, $nrows)
					\\set num_b random(1, $nrows)
					\\set diff random(1, 10000)
					BEGIN;
					UPDATE tbl:t SET val = val + :diff WHERE id = :num_a;
					\\sleep 1 ms
					UPDATE tbl:t SET val = val - :diff WHERE id = :num_b;
					\\sleep 1 ms
					COMMIT;),
				],
				checks => [
					qq(-- Each table keeps its own sum.  REPACK (CONCURRENTLY)
					-- is not MVCC-safe yet, so a snapshot spanning its swap
					-- may find a table empty; that is tolerated, anything
					-- else must be complete and correct.
					SELECT stress_assert(cnt = 0 OR (cnt = $nrows AND sum = $sum),
						format('rows=%s sum=%s (want 0, or $nrows rows sum $sum)',
							cnt, sum))
					FROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl:t) x;),
					qq(-- An index scan and a sequential scan of the same
					-- predicate, in one snapshot, must return the same thing.
					BEGIN ISOLATION LEVEL REPEATABLE READ;
					SET LOCAL enable_seqscan = off;
					SET LOCAL enable_bitmapscan = off;
					SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl:t WHERE val > 0 \\gset idx_
					SET LOCAL enable_seqscan = on;
					SET LOCAL enable_indexscan = off;
					SET LOCAL enable_indexonlyscan = off;
					SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl:t WHERE val > 0 \\gset seq_
					COMMIT;
					SELECT stress_assert(
						:idx_cnt = :seq_cnt AND :idx_sum = :seq_sum,
						format('index scan (rows %s, sum %s) disagrees with seq scan (rows %s, sum %s)',
							:idx_cnt, :idx_sum, :seq_cnt, :seq_sum));),
				],
			),
		),
	});

foreach my $t (1 .. $ntables)
{
	is( $node->safe_psql('postgres', qq(SELECT SUM(val) FROM tbl$t)),
		$sum, "tbl$t sum invariant holds");
	$node->safe_psql(
		'postgres', qq(
		SELECT bt_index_parent_check('tbl${t}_pkey', heapallindexed => true);
		SELECT bt_index_parent_check('tbl_val_idx_${t}', heapallindexed => true);
	));
}

# All the transient slots must be gone, and logical decoding off again.
is( $node->safe_psql('postgres', q(SELECT COUNT(*) FROM pg_replication_slots)),
	'0', 'no replication slot leaked');
$node->poll_query_until('postgres',
	q(SELECT current_setting('effective_wal_level') = 'replica'))
  or die 'timed out waiting for logical decoding to be disabled';
pass('effective_wal_level fell back to replica');

$node->stop;

done_testing();
