# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for index-only scans and the visibility map across
# CONCURRENTLY commands.
#
# An index-only scan trusts the visibility map: on a page marked
# all-visible it returns the index tuple without looking at the heap at
# all.  That makes a wrong VM bit a silent wrong answer rather than an
# error, and everything here moves the map around -- VACUUM sets the
# bits, REPACK (CONCURRENTLY) rewrites the table and with it the map,
# and every modification clears the bit for the page it touches.
#
# The table is in three parts.  Rows up to the hot boundary are churned
# by balanced pairs of updates, so the sum over the val column does not
# move; rows between that boundary and the initial row count are never
# modified at all, so their pages settle as all-visible and an
# index-only scan over them really does skip the heap; and a small range
# above it is repeatedly deleted and inserted again under the same key,
# all with val = 0, so that a page whose VM bit was wrongly left set
# would let a reader see one of those rows twice or not at all.
#
# Readers check that an index-only scan and a sequential scan of the
# same predicate in one snapshot agree, that the untouched middle part
# reads back exactly as it was created, and that the row count and sum
# are the ones the writers maintain.  Afterwards the map itself is
# verified with pg_visibility.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled index-only scan stress test');

my $duration = 6 * $stressval;
my $nrows = 10_000;

# Rows with id <= $nhot are updated; those between $nhot and $nrows are
# never touched, so their pages can stay all-visible for the whole run.
my $nhot = $nrows / 2;

# Rows above $nrows are deleted and re-inserted under the same key, all
# with val = 0 so that they do not enter into the sum.  They are kept
# apart from the updated rows on purpose: an update of a row another
# client is deleting would silently match nothing, which would break the
# invariant without any bug being involved.
my $nchurn = 200;
my $ntotal = $nrows + $nchurn;

my $node;

#
# Test set-up.  Autovacuum is aggressive so that the visibility map is
# being set continuously rather than once at the start.
#
$node = stress_init_node(
	'index_only',
	extra_conf => [
		'autovacuum_naptime = 1s',
		'autovacuum_vacuum_scale_factor = 0.0',
		'autovacuum_vacuum_threshold = 100',
		'autovacuum_vacuum_insert_scale_factor = 0.0',
		'autovacuum_vacuum_insert_threshold = 100',
	]);

$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE EXTENSION pg_visibility;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_ios_idx ON tbl(id, val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
	INSERT INTO tbl SELECT g, 0 FROM generate_series($nrows + 1, $ntotal) g;
	VACUUM (ANALYZE) tbl;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl));
my $cold_sum = $node->safe_psql('postgres',
	qq(SELECT SUM(val) FROM tbl WHERE id > $nhot AND id <= $nrows));
my $cold_rows = $nrows - $nhot;

# The whole point of the checks below is that they run as index-only
# scans, so make sure the settings they use really produce one.
my $plan = $node->safe_psql(
	'postgres', qq(
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	EXPLAIN (COSTS OFF)
		SELECT COUNT(*), COALESCE(SUM(val), 0) FROM tbl
		WHERE id > $nhot AND id <= $nrows;
));
like($plan, qr/Index Only Scan/, 'the reader query plans as an index-only scan');

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'index-only scans and the visibility map across CONCURRENTLY commands',
	{
		'concurrent_ops' => stress_ddl_gate(
			indent => "\t\t",
			ddl => [
				'REPACK (CONCURRENTLY) tbl;',
				'REPACK (CONCURRENTLY) tbl USING INDEX tbl_ios_idx;',
				'REINDEX INDEX CONCURRENTLY tbl_ios_idx;',
				'REINDEX TABLE CONCURRENTLY tbl;',
				[
					'DROP INDEX CONCURRENTLY tbl_ios_idx;',
					'CREATE INDEX CONCURRENTLY tbl_ios_idx ON tbl(id, val);',
				],
			],
			post =>
			  "SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
		)
		  . "\n"
		  . stress_ddl_gate(
			# A second gate, on its own lock: one client at a time also
			# vacuums, which is what sets the visibility map bits the
			# readers then rely on.
			indent => "\t\t\t",
			lock => 43,
			var => 'gotvac',
			ddl =>
			  [ 'VACUUM tbl;', 'VACUUM (FREEZE) tbl;', 'VACUUM (ANALYZE) tbl;' ],
		  )
		  . "\n"
		  . stress_workload(
			indent => "\t\t\t\t",
			mutations => [
				# Balanced pair of updates within the hot part, so the sum
				# over the whole table does not move.
				qq(\\set num_a random(1, $nhot)
				\\set num_b random(1, $nhot)
				\\set diff random(1, 10000)
				BEGIN;
				UPDATE tbl SET val = val + :diff WHERE id = :num_a;
				\\sleep 1 ms
				UPDATE tbl SET val = val - :diff WHERE id = :num_b;
				COMMIT;),

				# Delete a row and put it back under the same key.  The
				# transaction is atomic, so at every commit boundary the row
				# is there exactly once -- but only a reader that really
				# consults visibility information sees it that way.  Two
				# clients may pick the same key, in which case the second
				# one finds the row already deleted and re-inserted, and
				# leaves it alone.
				qq(\\set num_c random(@{[ $nrows + 1 ]}, $ntotal)
				BEGIN;
				DELETE FROM tbl WHERE id = :num_c;
				\\sleep 1 ms
				INSERT INTO tbl VALUES (:num_c, 0) ON CONFLICT DO NOTHING;
				COMMIT;),

				# A delete that is rolled back: the row stays, but the page
				# is dirtied and its all-visible bit has to be cleared even
				# though nothing ends up changing.
				qq(\\set num_c random(@{[ $nrows + 1 ]}, $ntotal)
				BEGIN;
				DELETE FROM tbl WHERE id = :num_c;
				\\sleep 1 ms
				ROLLBACK;),
			],
			checks => [
				# An index-only scan and a sequential scan of the same
				# predicate, in one snapshot, must return the same thing.
				qq(BEGIN ISOLATION LEVEL REPEATABLE READ;
				SET LOCAL enable_seqscan = off;
				SET LOCAL enable_bitmapscan = off;
				SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
					FROM tbl WHERE id > 0 \\gset ios_
				SET LOCAL enable_seqscan = on;
				SET LOCAL enable_indexscan = off;
				SET LOCAL enable_indexonlyscan = off;
				SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
					FROM tbl WHERE id > 0 \\gset seq_
				COMMIT;
				SELECT stress_assert(:ios_cnt = :seq_cnt AND :ios_sum = :seq_sum,
					format('index-only scan (%s rows, sum %s) disagrees with seq scan (%s rows, sum %s)',
						:ios_cnt, :ios_sum, :seq_cnt, :seq_sum));
				-- REPACK (CONCURRENTLY) is not MVCC-safe yet: a snapshot
				-- spanning its swap may find the table empty, which is
				-- tolerated; anything else must be complete and correct.
				\\if :ios_cnt = 0
					SELECT 'repack: empty view tolerated' AS marker;
				\\endif
				SELECT stress_assert(:ios_cnt = 0
						OR (:ios_cnt = $ntotal AND :ios_sum = $sum),
					format('whole table: %s rows, sum %s (want $ntotal rows, sum $sum)',
						:ios_cnt, :ios_sum));),

				# The untouched part, read through the index alone.  These
				# are the pages that stay all-visible, so this is the read
				# that really does trust the visibility map.
				qq(BEGIN;
				SET LOCAL enable_seqscan = off;
				SET LOCAL enable_bitmapscan = off;
				SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
					FROM tbl WHERE id > $nhot AND id <= $nrows \\gset cold_
				COMMIT;
				SELECT stress_assert(:cold_cnt = 0
						OR (:cold_cnt = $cold_rows AND :cold_sum = $cold_sum),
					format('untouched part: %s rows, sum %s (want $cold_rows rows, sum $cold_sum)',
						:cold_cnt, :cold_sum));),
			],
		  )
		  . "\n\t\t\t\\endif\n\t\t\\endif",
	});

is($node->safe_psql('postgres', q(SELECT COUNT(*) FROM tbl)),
	$ntotal, 'no rows lost');
is($node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after index-only scan churn');
is( $node->safe_psql('postgres',
		qq(SELECT SUM(val) FROM tbl WHERE id > $nhot AND id <= $nrows)),
	$cold_sum, 'the untouched part is unchanged');

# The visibility map must describe the table it ended up with: no page
# may claim to be all-visible, or all-frozen, while holding a tuple that
# is not.
is( $node->safe_psql('postgres',
		q(SELECT COUNT(*) FROM pg_check_visible('tbl'))),
	'0', 'no page wrongly marked all-visible');
is( $node->safe_psql('postgres',
		q(SELECT COUNT(*) FROM pg_check_frozen('tbl'))),
	'0', 'no page wrongly marked all-frozen');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_ios_idx', heapallindexed => true);
));

$node->stop;

done_testing();
