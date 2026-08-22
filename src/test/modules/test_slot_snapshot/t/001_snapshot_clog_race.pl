# Copyright (c) 2026, PostgreSQL Global Development Group

# Check that a logical slot's initial snapshot is not built while CLOG still
# reports one of the transactions it considers committed as in progress.
#
# The window exists because RecordTransactionCommit() flushes the commit WAL
# record before updating CLOG, while logical decoding derives transaction
# status from WAL.  A snapshot built there says the transaction committed, but
# every visibility check that uses it asks CLOG, is told otherwise, concludes
# the transaction aborted, and stores that conclusion in the tuples' hint bits.
#
# The isolation spec in this directory shows the same race, but it can only
# demonstrate it: once the race is handled, the importing session waits for
# the CLOG update, and the isolation tester has no way to release the writer
# while a step is waiting on something that is not a lock or an injection
# point.  Here we are the ones driving the sessions, so we can.

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

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init(allows_streaming => 'logical');
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;

$node->safe_psql(
	'postgres', q{
	CREATE EXTENSION injection_points;
	CREATE TABLE tbl (i int PRIMARY KEY, j int) WITH (autovacuum_enabled = off);
	INSERT INTO tbl VALUES (1, 10), (2, 20);
});

# Two sessions that just hold an XID, to walk the snapshot builder through
# SNAPBUILD_BUILDING_SNAPSHOT and SNAPBUILD_FULL_SNAPSHOT one state at a time.
my $holder1 = $node->background_psql('postgres');
my $holder2 = $node->background_psql('postgres');

# The session that creates the slot and imports its initial snapshot, the way
# a subscription's initial table copy does.
my $importer = $node->background_psql('postgres');

# The session whose CLOG update is delayed.
my $writer = $node->background_psql('postgres');

my $importer_pid = $importer->query_safe('SELECT pg_backend_pid()');

$importer->query_safe(q{LOAD 'test_slot_snapshot'});
$importer->query_safe(
	q{SELECT injection_points_set_local(); SELECT injection_points_attach('snapbuild-full-snapshot', 'wait')}
);
$writer->query_safe(
	q{SELECT injection_points_set_local(); SELECT injection_points_attach('commit-before-clog-update', 'wait')}
);

# Keep the snapshot builder from reaching a consistent state right away.
$holder1->query_safe('BEGIN; SELECT pg_current_xact_id()');

# Create the slot.  This blocks first on $holder1's XID, and then, once the
# builder switches to SNAPBUILD_FULL_SNAPSHOT, on the injection point.
$importer->query_safe(
	'BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
$importer->query_until(qr//,
	q{SET TRANSACTION SNAPSHOT 'logical-slot:race_slot';} . "\n");

# Wait until the builder is waiting for $holder1.  By then the xl_running_xacts
# record that slot creation logs is in WAL and has been decoded, so the XID
# $holder2 is about to get is not part of it.
$node->poll_query_until(
	'postgres', qq[
	SELECT count(*) > 0 FROM pg_stat_activity
	WHERE pid = $importer_pid AND wait_event = 'transactionid'
]);

# $holder2 makes sure that releasing $holder1 only advances the builder by one
# state.
$holder2->query_safe('BEGIN; SELECT pg_current_xact_id()');
$holder1->query_safe('ROLLBACK');
$node->wait_for_event('client backend', 'snapbuild-full-snapshot');
$holder2->query_safe('ROLLBACK');

# Commit a transaction and leave it between its flushed commit record and its
# CLOG update.
$writer->query_safe('BEGIN');
$writer->query_safe('INSERT INTO tbl VALUES (3, 30)');
$writer->query_safe('UPDATE tbl SET j = j + 1 WHERE i = 1');
$writer->query_safe('DELETE FROM tbl WHERE i = 2');
$writer->query_until(qr//, "COMMIT;\n");
$node->wait_for_event('client backend', 'commit-before-clog-update');

# Let the snapshot builder run.  It decodes the writer's commit record and
# reaches a consistent state, all while CLOG still reports the writer as
# running.
$node->safe_psql('postgres',
	q{SELECT injection_points_wakeup('snapbuild-full-snapshot')});

# Give the importing session time to build its snapshot inside that window.
# It is allowed not to: waiting for the CLOG update is precisely what we want
# it to do, so give up after a while instead of insisting.
my $built = 0;
my $deadline = time() + 10;
while (time() < $deadline)
{
	$built = $node->safe_psql('postgres',
		"SELECT state = 'idle in transaction' FROM pg_stat_activity WHERE pid = $importer_pid"
	) eq 't';
	last if $built;
	usleep(50_000);
}
note $built
  ? 'snapshot was built before the CLOG update'
  : 'snapshot was not built before the CLOG update';

# Use the snapshot.  When it was built inside the window, read the table
# before letting the writer proceed: an initial table copy starts as soon as
# it has the snapshot, and that is where the damage happens.
my $copied;
$copied = $importer->query_safe(q{SELECT i || '|' || j FROM tbl ORDER BY i})
  if $built;

# Release the writer, so that the run finishes either way.
$node->safe_psql('postgres',
	q{SELECT injection_points_wakeup('commit-before-clog-update')});
$writer->query_safe('SELECT 1');

$copied = $importer->query_safe(q{SELECT i || '|' || j FROM tbl ORDER BY i})
  unless $built;

# The snapshot considers the writer committed, so the initial table copy has
# to see its effects: (2, 20) deleted, (1, 10) updated, (3, 30) inserted.
is($copied, "1|11\n3|30",
	'initial slot snapshot sees the transaction it considers committed');
$importer->query_safe('ROLLBACK');

# Wrong visibility answers are recorded in the tuples' hint bits, so the
# damage outlives the CLOG update.
my $after = $node->safe_psql('postgres',
	q{SELECT i || '|' || j FROM tbl ORDER BY i});
is($after, "1|11\n3|30", 'table is intact after the CLOG update');

$holder1->quit;
$holder2->quit;
$importer->quit;
$writer->quit;
$node->stop;

done_testing();
