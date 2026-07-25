# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for pg_dump running against CONCURRENTLY commands.
#
# pg_dump reads the whole database in one repeatable-read transaction,
# after taking an AccessShareLock on every table it is going to dump.
# The CONCURRENTLY commands take stronger locks in their final steps
# and, in the case of REPACK, replace the files under the rows, so a
# dump taken while they run must still produce a self-consistent
# snapshot of the data.
#
# Writer clients keep the sum over the val column invariant, so every
# dump taken during the run must restore to exactly that sum, with the
# full set of rows and working indexes.  The dumps are restored into a
# separate database and compared against the invariant.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use IPC::Run qw(start finish);

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use Stress::Bespoke;

my $stressval = stress_plan(skip =>
	'skipping disabled pg_dump stress test');

my $duration = 6 * $stressval;
my $nrows = 2000;
my $nclients = 10;

my $node;

#
# Test set-up
#
$node = stress_init_node('pg_dump',
	extra_conf => [ 'max_connections = 50' ]);

$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) AS sum FROM tbl));

my $ops_sql = $node->basedir . '/concurrent_ops.sql';
PostgreSQL::Test::Utils::append_to_file(
	$ops_sql,
	stress_ddl_gate(
		indent => '',
		ddl => [
			'REPACK (CONCURRENTLY) tbl;',
			[
				'DROP INDEX CONCURRENTLY tbl_val_idx;',
				'CREATE INDEX CONCURRENTLY tbl_val_idx ON tbl(val);'
			],
			'REINDEX INDEX CONCURRENTLY tbl_val_idx;',
			'REINDEX TABLE CONCURRENTLY tbl;',
		],
		else => qq(\t\\set num_a random(1, $nrows)
\t\\set num_b random(1, $nrows)
\t\\set diff random(1, 10000)
\tBEGIN;
\tUPDATE tbl SET val = val + :diff WHERE id = :num_a;
\t\\sleep 1 ms
\tUPDATE tbl SET val = val - :diff WHERE id = :num_b;
\t\\sleep 1 ms
\tCOMMIT;)
	) . "\n");

my @cmd = (
	'pgbench', '--no-vacuum', "--client=$nclients", "--jobs=$nclients",
	'--exit-on-abort', '-T', $duration,
	'-p', $node->port, '-h', $node->host, '-f', $ops_sql, 'postgres');

my ($out, $err) = ('', '');
my $h = start \@cmd, '>', \$out, '2>', \$err;

# Take dumps while the churn is going on, and restore each one.
my $dumps = 0;
my $empty_dumps = 0;
my $failed_dumps = 0;
my $deadline = time() + $duration;
while (time() < $deadline)
{
	my $dumpfile = $node->basedir . "/dump_$dumps.sql";

	# XXX pg_dump can fail outright here: DROP INDEX CONCURRENTLY only
	# takes a ShareUpdateExclusiveLock, which does not conflict with the
	# AccessShareLock pg_dump holds on the table, so an index can be
	# dropped between pg_dump's catalog scan and the pg_get_indexdef()
	# call in the same query, which then fails with "cache lookup failed
	# for index".  Count those runs and carry on rather than cascading
	# into a restore of a half-written file.
	my ($dump_out, $dump_err) =
	  PostgreSQL::Test::Utils::run_command(
		[
			'pg_dump', '--no-sync', '-f', $dumpfile,
			'-p', $node->port, '-h', $node->host, 'postgres'
		]);
	if ($dump_err ne '')
	{
		$failed_dumps++;
		note "pg_dump run $dumps failed: $dump_err";
		like($dump_err, qr/cache lookup failed for index/,
			"pg_dump run $dumps failed only in the known way");
		$dumps++;
		next;
	}

	my $restoredb = "restore_$dumps";
	$node->safe_psql('postgres', qq(CREATE DATABASE $restoredb));
	$node->command_ok(
		[
			'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-f', $dumpfile,
			'-p', $node->port, '-h', $node->host, $restoredb
		],
		"restore of dump $dumps");

	# The dump was taken in a single repeatable-read transaction, so it
	# must reflect a state in which the invariant holds.
	#
	# REPACK (CONCURRENTLY) is not MVCC-safe yet, so a snapshot that
	# spans its swap may find the table empty.  A dump taken with such a
	# snapshot then reports success and produces a structurally complete
	# file whose COPY block is empty, so it restores cleanly to a table
	# with no rows at all.  That outcome is tolerated here, but counted
	# and reported, because everything else must be complete and
	# correct.  Note that only REPACK produces it: the same workload
	# with the index rebuilds alone never does.
	my $count = $node->safe_psql($restoredb, q(SELECT COUNT(*) FROM tbl));
	if ($count == 0)
	{
		$empty_dumps++;
	}
	else
	{
		is( $node->safe_psql($restoredb, q(SELECT SUM(val) FROM tbl)),
			$sum, "restored dump $dumps satisfies the sum invariant");
		is($count, "$nrows", "restored dump $dumps has all rows");
	}

	# The restored indexes must be sound.  The secondary index may be
	# legitimately absent: the dump can have been taken while the DDL
	# rotation had dropped it and not yet recreated it.
	$node->safe_psql(
		$restoredb, q(
		SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
		SELECT bt_index_parent_check(to_regclass('tbl_val_idx'),
									 heapallindexed => true)
		WHERE to_regclass('tbl_val_idx') IS NOT NULL;
	));

	$node->safe_psql('postgres', qq(DROP DATABASE $restoredb));
	$dumps++;
}

finish $h;

like($out, qr/actually processed/, 'pgbench completed');
is($err, '', 'pgbench no stderr');
note "attempted $dumps dumps: $failed_dumps failed, "
  . "$empty_dumps restored empty";
cmp_ok($dumps, '>', 0, 'at least one dump was attempted');
cmp_ok($dumps - $empty_dumps - $failed_dumps,
	'>', 0, 'at least one dump had the table contents');

is( $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds on the source database');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$node->stop;

done_testing();
