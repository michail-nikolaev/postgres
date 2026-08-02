
# Copyright (c) 2026, PostgreSQL Global Development Group

# A snapshot older than a REPACK (CONCURRENTLY) reads the table empty.
#
# REPACK (CONCURRENTLY) builds the new heap by inserting the rows as
# itself: heapam_relation_copy_for_cluster() sends the concurrent case
# to heap_insert_for_repack(), which is an ordinary heap_insert(), where
# the non-concurrent case goes to reform_and_rewrite_tuple() and
# preserves each tuple's xmin and xmax.  So every row of the new heap
# carries the repacking transaction's xid.
#
# A transaction whose snapshot predates that commit therefore sees the
# whole table as written by a transaction it does not consider
# committed -- an empty table, for as long as the snapshot lives.  A
# reader that has already touched the table is safe, but only by
# accident: it holds AccessShareLock, so the swap waits for it.  What is
# exposed is the holder of an old snapshot that has not yet opened the
# table.
#
# Three of those, in order of how much they cost:
#
#   1. an ordinary REPEATABLE READ transaction that has taken its
#      snapshot and not yet read the table -- a wrong answer for the
#      rest of the transaction;
#
#   2. a snapshot exported from a logical slot and read under SET
#      TRANSACTION SNAPSHOT, which is what pg_dump --snapshot does and
#      what the documentation recommends for setting a subscriber up by
#      hand -- a dump that silently omits the table's contents;
#
#   3. logical replication's initial table synchronization, which does
#      exactly that internally (CRS_USE_SNAPSHOT in tablesync.c, and
#      SnapBuildInitialSnapshot/RestoreTransactionSnapshot in the
#      walsender) and then marks the table synchronized.  Nothing copies
#      it again, every later change is logged as conflict=update_missing,
#      and the subscriber's copy stays empty until someone
#      resynchronizes it by hand.
#
# The first two are deterministic.  The third needs the repack to land
# in the window between the exported snapshot and the COPY -- which is
# why a *small* table reproduces it and a large one does not: the window
# is slot creation, worker startup and origin advancement, and a table
# that copies instantly spends all of its synchronization inside it.
#
# This test fails on purpose while the gap is open; see
# src/test/stress/REGRESSIONS.  It is skipped unless asked for:
#
#   PG_TEST_EXTRA='repack_snapshots'
use strict;
use warnings FATAL => 'all';

use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (($ENV{PG_TEST_EXTRA} || '') !~ /\brepack_snapshots\b/)
{
	plan skip_all =>
	  'test for an open bug, enable with PG_TEST_EXTRA=repack_snapshots';
}

my $rows = 10;

my $pub = PostgreSQL::Test::Cluster->new('repack_snap_pub');
$pub->init(allows_streaming => 'logical');
$pub->append_conf('postgresql.conf', 'max_worker_processes = 32');
$pub->start;

my $psql = $pub->installed_command('psql');
my $pconn = $pub->connstr('postgres');

#
# 1.  An ordinary repeatable read transaction.
#
{
	$pub->safe_psql(
		'postgres', qq(
		CREATE TABLE t_plain(id int PRIMARY KEY, v int);
		INSERT INTO t_plain SELECT g, g FROM generate_series(1, $rows) g;
	));

	# The snapshot is taken without touching the table: a reader that
	# has read it holds AccessShareLock, and the swap would wait rather
	# than overtake it.
	my $reader = $pub->background_psql('postgres');
	$reader->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$reader->query_safe('SELECT 1');

	# lock_timeout so a repack that cannot get its lock fails instead of
	# hanging the test.
	$pub->safe_psql('postgres',
		"SET lock_timeout = '30s'; REPACK (CONCURRENTLY) t_plain");

	my $after = $reader->query_safe('SELECT count(*) FROM t_plain');
	chomp $after;
	$reader->query_safe('COMMIT');
	$reader->quit;

	is($after, "$rows",
		'a repeatable read snapshot older than the repack still sees the '
		  . 'table');
}

#
# 2.  A snapshot exported from a logical slot, read the way
#     pg_dump --snapshot reads one.
#
{
	$pub->safe_psql(
		'postgres', qq(
		CREATE TABLE t_export(id int PRIMARY KEY, v int);
		INSERT INTO t_export SELECT g, g FROM generate_series(1, $rows) g;
	));

	my $repl = $pub->background_psql('postgres', replication => 'database');
	my $out = $repl->query_safe(
		"CREATE_REPLICATION_SLOT exp_slot LOGICAL pgoutput (SNAPSHOT 'export')");
	my ($snapname) = $out =~ /([0-9A-F]{8}-[0-9A-F]{8}-\d+)/i;

	$pub->safe_psql('postgres',
		"SET lock_timeout = '30s'; REPACK (CONCURRENTLY) t_export");

	my $got = $pub->safe_psql(
		'postgres', qq(
		BEGIN ISOLATION LEVEL REPEATABLE READ;
		SET TRANSACTION SNAPSHOT '$snapname';
		SELECT count(*) FROM t_export;
	));

	$repl->quit;
	$pub->safe_psql('postgres', "SELECT pg_drop_replication_slot('exp_slot')");

	is($got, "$rows",
		"a slot's exported snapshot still sees the table after a repack");
}

#
# 3.  Logical replication's initial synchronization, where the same
#     empty read is permanent.
#
{
	my $sub = PostgreSQL::Test::Cluster->new('repack_snap_sub');
	$sub->init(allows_streaming => 'logical');
	$sub->append_conf('postgresql.conf', 'max_worker_processes = 32');
	$sub->start;

	my $connstr = $pub->connstr . ' dbname=postgres';
	my $lost = 0;
	my $rounds = 4;

	foreach my $round (1 .. $rounds)
	{
		my $t = "t_sync$round";

		$pub->safe_psql(
			'postgres', qq(
			CREATE TABLE $t(id int PRIMARY KEY, v int);
			INSERT INTO $t SELECT g, g FROM generate_series(1, $rows) g;
			CREATE PUBLICATION p$round FOR TABLE $t;
		));
		$sub->safe_psql('postgres', "CREATE TABLE $t(id int PRIMARY KEY, v int)");

		# An external loop rather than a fork: a forked copy of this
		# process would run PostgreSQL::Test::Cluster's END handlers on
		# exit and stop the clusters under test.
		my ($ro, $re) = ('', '');
		my $repacker = IPC::Run::start(
			[
				'sh', '-c',
				"for i in \$(seq 1 40); do "
				  . "'$psql' -X -q -d '$pconn' "
				  . "-c 'REPACK (CONCURRENTLY) $t'; done"
			],
			'>', \$ro, '2>', \$re);

		$sub->safe_psql('postgres',
			"CREATE SUBSCRIPTION s$round CONNECTION '$connstr' "
			  . "PUBLICATION p$round");
		$sub->wait_for_subscription_sync($pub, "s$round");
		IPC::Run::kill_kill($repacker);

		my $want = $pub->safe_psql('postgres', "SELECT count(*) FROM $t");
		my $got = $sub->safe_psql('postgres', "SELECT count(*) FROM $t");
		note "round $round: publisher $want rows, subscriber $got rows";
		$lost++ if $want ne $got;

		$sub->safe_psql('postgres', "ALTER SUBSCRIPTION s$round DISABLE");
		$sub->safe_psql('postgres',
			"ALTER SUBSCRIPTION s$round SET (slot_name = NONE)");
		$sub->safe_psql('postgres', "DROP SUBSCRIPTION s$round");
		$pub->safe_psql('postgres', "DROP PUBLICATION p$round");
	}

	is($lost, 0, 'the initial copy read the table under every repack')
	  or diag("$lost of $rounds rounds copied an empty table, "
		  . 'and nothing copies it again');

	$sub->stop;
}

$pub->stop;
done_testing();
