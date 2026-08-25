# Copyright (c) 2025, PostgreSQL Global Development Group

# The same race in the other tuple lookup path: RelationFindReplTupleSeq.
#
# A table with REPLICA IDENTITY FULL and no usable index - that is, any table
# without a primary key - is searched with a sequential scan.  Because the
# snapshot is not an MVCC one, the scan runs with page-at-a-time mode disabled
# and releases the buffer lock between tuples, so a concurrent update can put
# the new version of a row at a line pointer the scan has already walked past.
# The scan then sees neither version and the change is lost, exactly as in the
# index case.
#
# This one needs no non-HOT update: a sequential scan follows heap-only tuples
# directly, so any update that reuses a lower line pointer is enough.  Line
# pointers below the scan position are the normal state of a table that has
# been vacuumed after deletes.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

###############################
# Setup
###############################

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	qq(track_commit_timestamp = on));
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf',
	qq(track_commit_timestamp = on));
$node_subscriber->start;

# Check if the extension injection_points is available, as it may be
# possible that this script is run with installcheck, where the module
# would not be installed by default.
if (!$node_subscriber->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

# No primary key, so the publisher has to send whole rows and the subscriber
# has to look them up by comparing every column.
$node_publisher->safe_psql(
	'postgres',
	"CREATE TABLE seq_tab(a int, data text);
	 ALTER TABLE seq_tab REPLICA IDENTITY FULL;");

# Deliberately no index at all on the subscriber: that is what makes the apply
# worker fall back to RelationFindReplTupleSeq.
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE seq_tab(a int, data text);");

$node_subscriber->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE seq_tab");

$node_publisher->safe_psql('postgres',
	"INSERT INTO seq_tab SELECT i, 'frompub' FROM generate_series(1,10) i");

my $appname = 'tap_sub';
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION tap_sub
	 CONNECTION '$publisher_connstr application_name=$appname'
	 PUBLICATION tap_pub");

$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

##############################################################
# Leave free line pointers at the start of the page, the way
# an ordinary delete-and-vacuum cycle does
##############################################################

$node_publisher->safe_psql('postgres', "DELETE FROM seq_tab WHERE a <= 8;");
$node_publisher->wait_for_catchup($appname);

$node_subscriber->safe_psql('postgres', "VACUUM seq_tab;");

# The two surviving rows sit above eight unused line pointers.  Assert the
# layout instead of assuming it, so this test reports a stale assumption rather
# than quietly passing if heap placement ever changes.
is( $node_subscriber->safe_psql(
		'postgres', "SELECT a, ctid FROM seq_tab ORDER BY a"),
	"9|(0,9)\n10|(0,10)",
	'subscriber page has free line pointers below the live rows');

###############################################
# Race: lose the tuple during a sequential scan
###############################################

my $psql_session_subscriber = $node_subscriber->background_psql('postgres');

$node_subscriber->safe_psql('postgres',
	"SELECT injection_points_attach('relation_find_repl_tuple_seq_scanned', 'wait')"
);

my $log_offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres', "DELETE FROM seq_tab WHERE a = 10;");

# The apply worker now scans for the row.  It stops on the first tuple it
# reads, which is the one at (0,9) - not the row it is looking for.
$node_subscriber->wait_for_event(
	'logical replication apply worker',
	'relation_find_repl_tuple_seq_scanned');

# Touch the wanted row locally without changing any value: REPLICA IDENTITY
# FULL matches on every column, so the row still has to be found.  The update
# still writes a new version, and it lands on the lowest free line pointer -
# (0,1), which the scan passed before it stopped.
$psql_session_subscriber->query_until(
	qr/updated/, qq[
	UPDATE seq_tab SET data = data WHERE (a=10);
	\\echo updated
]);

is( $node_subscriber->safe_psql(
		'postgres', "SELECT ctid FROM seq_tab WHERE a = 10"),
	'(0,1)',
	'new row version landed behind the scan position');

$node_subscriber->safe_psql(
	'postgres', "
	SELECT injection_points_detach('relation_find_repl_tuple_seq_scanned');
	SELECT injection_points_wakeup('relation_find_repl_tuple_seq_scanned');
	");

# The row was concurrently modified, so a conflict of some kind is always
# reported.  Which one tells us whether the scan found the tuple.
$node_subscriber->wait_for_log(
	qr/conflict detected on relation \"public.seq_tab\"/, $log_offset);

$node_publisher->wait_for_catchup($appname);

ok( !$node_subscriber->log_contains(
		qr/LOG:  conflict detected on relation \"public.seq_tab\": conflict=delete_missing/,
		$log_offset),
	'sequential scan did not lose the tuple');

ok( $node_subscriber->log_contains(
		qr/LOG:  conflict detected on relation \"public.seq_tab\": conflict=delete_origin_differs/,
		$log_offset),
	'correct conflict detected');

is($node_subscriber->safe_psql('postgres', 'SELECT count(*) FROM seq_tab'),
	1, 'remote DELETE applied on the subscriber');

done_testing();
