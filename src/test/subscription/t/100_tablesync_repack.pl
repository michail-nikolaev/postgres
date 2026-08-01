
# Copyright (c) 2026, PostgreSQL Global Development Group

# A subscriber left with an empty table by a REPACK (CONCURRENTLY) of
# the table being copied.
#
# Logical replication's initial table synchronization copies under a
# snapshot exported from the replication slot it creates -- one of the
# two places in the server where a snapshot derived from decoded commit
# records is used for ordinary MVCC visibility.  REPACK (CONCURRENTLY)
# is not MVCC-safe: a snapshot that spans its relfilenode swap can find
# the table empty.
#
# So a REPACK that commits in the window between the tablesync's
# snapshot and its COPY makes the copy succeed having read nothing.
# What follows is silent and permanent: the table is marked
# synchronized, so nothing copies it again, and every change replicated
# afterwards is applied against an empty table and logged as
# conflict=update_missing.  A real subscriber has no invariant to
# notice, and the only repair is resynchronizing the table by hand.
#
# The window is not inside the copy.  It is the gap between the slot's
# exported snapshot and the COPY that reads with it -- slot creation,
# worker startup, the handshake -- which is why a ten-row table
# reproduces this far more readily than a large one, and why the first
# sighting was against pgbench_branches at scale 1.
#
# This test fails on purpose while the underlying gap is open; see
# src/test/stress/REGRESSIONS.  It is skipped unless asked for:
#
#   PG_TEST_EXTRA='repack_tablesync'
use strict;
use warnings FATAL => 'all';

use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if (($ENV{PG_TEST_EXTRA} || '') !~ /\brepack_tablesync\b/)
{
	plan skip_all =>
	  'test for an open bug, enable with PG_TEST_EXTRA=repack_tablesync';
}

my $rounds = 6;
my $rows = 10;

my $pub = PostgreSQL::Test::Cluster->new('tsrepack_pub');
$pub->init(allows_streaming => 'logical');
$pub->append_conf('postgresql.conf', 'max_worker_processes = 32');
$pub->start;

my $sub = PostgreSQL::Test::Cluster->new('tsrepack_sub');
$sub->init(allows_streaming => 'logical');
$sub->append_conf('postgresql.conf', 'max_worker_processes = 32');
$sub->start;

my $connstr = $pub->connstr . ' dbname=postgres';
my $psql = $pub->installed_command('psql');
my $pconn = $pub->connstr('postgres');
my $lost = 0;

foreach my $round (1 .. $rounds)
{
	my $t = "t$round";

	$pub->safe_psql(
		'postgres', qq(
		CREATE TABLE $t(id int PRIMARY KEY, v int);
		INSERT INTO $t SELECT g, g FROM generate_series(1, $rows) g;
		CREATE PUBLICATION p$round FOR TABLE $t;
	));
	$sub->safe_psql('postgres', "CREATE TABLE $t(id int PRIMARY KEY, v int)");

	# Repack the table continuously while the subscription starts, so
	# that a swap lands somewhere in the synchronization's window.  An
	# external loop rather than a fork: a forked copy of this process
	# would run PostgreSQL::Test::Cluster's END handlers on exit and
	# stop the clusters under test.
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
  or diag("$lost of $rounds rounds copied an empty table");

$sub->stop;
$pub->stop;
done_testing();
