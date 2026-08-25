# Copyright (c) 2025, PostgreSQL Global Development Group

# Show that the SNAPSHOT_DIRTY race in RelationFindReplTupleByIndex is not just
# a mislabelled conflict in the log.
#
# When the apply worker fails to find the tuple, the remote DELETE is silently
# skipped and the row stays alive on the subscriber.  As soon as the publisher
# reuses that key the apply worker hits a unique violation, which is reported
# as an insert_exists conflict at ERROR level.  The worker then exits, is
# restarted by the launcher, re-receives the very same transaction and fails
# again, forever: the subscription never advances, the publisher cannot release
# WAL held by the slot, and a DBA has to intervene by hand.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

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

$node_publisher->safe_psql('postgres',
	"CREATE TABLE conf_tab(a int PRIMARY key, data text);");

# The extra index makes the concurrent local UPDATE below a non-HOT one, so it
# inserts a new entry into the primary key index - which is what the apply
# worker is scanning.
$node_subscriber->safe_psql(
	'postgres',
	"CREATE TABLE conf_tab(a int PRIMARY key, data text);
	 CREATE INDEX data_index ON conf_tab(data);");

$node_subscriber->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE conf_tab");

$node_publisher->safe_psql('postgres',
	"INSERT INTO conf_tab(a, data) VALUES (1,'frompub')");

my $appname = 'tap_sub';
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION tap_sub
	 CONNECTION '$publisher_connstr application_name=$appname'
	 PUBLICATION tap_pub");

$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

###############################################
# Step 1: make the apply worker miss the DELETE
###############################################

my $psql_session_subscriber = $node_subscriber->background_psql('postgres');

$node_subscriber->safe_psql('postgres',
	"SELECT injection_points_attach('index_getnext_slot_before_fetch_apply_dirty', 'wait')"
);

my $log_offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres', "DELETE FROM conf_tab WHERE a=1;");

# Wait for the apply worker to reach the point between reading the index entry
# and fetching the heap tuple.
$node_subscriber->wait_for_event('logical replication apply worker',
	'index_getnext_slot_before_fetch_apply_dirty');

# Now update the row locally.  This deletes the heap tuple the apply worker is
# about to fetch and inserts a new one, together with a new primary key index
# entry on the same page.  Waiting for the echo guarantees the UPDATE has
# committed before the apply worker is released, which is exactly the window
# where a SNAPSHOT_DIRTY scan loses the tuple.
$psql_session_subscriber->query_until(
	qr/updated/, qq[
	UPDATE conf_tab SET data = 'fromsub' WHERE (a=1);
	\\echo updated
]);

$node_subscriber->safe_psql(
	'postgres', "
	SELECT injection_points_detach('index_getnext_slot_before_fetch_apply_dirty');
	SELECT injection_points_wakeup('index_getnext_slot_before_fetch_apply_dirty');
	");

# The row was concurrently modified, so a conflict of some kind is always
# reported here.  Which one tells us whether the tuple was found.
$node_subscriber->wait_for_log(
	qr/conflict detected on relation \"public.conf_tab\"/, $log_offset);

ok( !$node_subscriber->log_contains(
		qr/LOG:  conflict detected on relation \"public.conf_tab\": conflict=delete_missing/,
		$log_offset),
	'apply worker did not lose the tuple');

$node_publisher->wait_for_catchup($appname);

is( $node_subscriber->safe_psql('postgres', 'SELECT count(*) FROM conf_tab'),
	0,
	'remote DELETE applied on the subscriber');

##########################################################
# Step 2: the publisher reuses the key that was not deleted
##########################################################

# Nothing exotic here - just the ordinary "the key came back" case.  If step 1
# lost the DELETE, the stale row is still sitting on the subscriber and this
# INSERT can no longer be applied.
my $insert_offset = -s $node_subscriber->logfile;

$node_publisher->safe_psql('postgres',
	"INSERT INTO conf_tab(a, data) VALUES (1,'reinserted')");

# Wait for whichever comes first: the row showing up on the subscriber, or the
# apply worker reporting a conflict at ERROR level.  Polling both keeps the
# test fast in either outcome instead of hanging until the global timeout.
my $insert_error =
  qr/ERROR:  conflict detected on relation \"public.conf_tab\": conflict=insert_exists/;
my $deadline = time() + $PostgreSQL::Test::Utils::timeout_default;
my $replicated = 0;
my $apply_failed = 0;

while (time() < $deadline)
{
	$replicated = $node_subscriber->safe_psql('postgres',
		"SELECT count(*) FROM conf_tab WHERE a = 1 AND data = 'reinserted'");
	last if $replicated eq '1';

	$apply_failed =
	  $node_subscriber->log_contains($insert_error, $insert_offset);
	last if $apply_failed;

	usleep(100_000);
}

if ($apply_failed)
{
	# Not a one-off failure: let the launcher restart the worker a few times
	# and count how often the same transaction is rejected.
	usleep(5_000_000);

	my $log = PostgreSQL::Test::Utils::slurp_file($node_subscriber->logfile,
		$insert_offset);
	my $failures = () = $log =~ /$insert_error/g;

	diag(
		"the apply worker rejected the same transaction $failures time(s) in 5s; "
		  . "the subscription can no longer advance");
}

ok(!$apply_failed,
	'publisher INSERT did not break the subscription with insert_exists');

is($replicated, '1', 'publisher INSERT replicated to the subscriber');

done_testing();
