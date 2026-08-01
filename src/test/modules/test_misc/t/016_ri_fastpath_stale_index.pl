
# Copyright (c) 2026, PostgreSQL Global Development Group

# A foreign key check that accepts a row whose referenced key does not
# exist, by resolving the constraint to an index that is no longer live.
#
# Same window as 015_ri_fastpath_reindex.pl: the RI fast path reads
# conindid before it locks the referenced table, so a concurrent rebuild
# can move the constraint elsewhere in between.  Here the index the check
# opens has not been dropped, only marked dead, which is what an
# interrupted REINDEX CONCURRENTLY leaves behind.
#
# A dead index is left out of RelationGetIndexList(), so VACUUM never
# cleans it.  Its entries keep pointing at line pointers that the heap is
# free to hand out again.  The check then finds a stale entry for a key
# that was deleted, fetches the tuple that took the slot over, and -- a
# btree scan does not recheck the key against the heap tuple -- reports
# the referenced row as present.  The write is accepted and a row with no
# referenced key is committed.
#
# The rebuild has to be out of the way before the vacuum: it holds a
# session-level ShareUpdateExclusiveLock on the table from the moment it
# starts until the old indexes are dropped, and VACUUM wants that lock
# too.  So the rebuild errors out after the old index is dead, which
# releases the lock and leaves the index behind.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');

# One page worth of rows, so the slot freed below is the only one a new
# row can take.  Nothing references 42, so it can be deleted.
$node->safe_psql(
	'postgres', q[
	CREATE TABLE pk (id int PRIMARY KEY);
	INSERT INTO pk SELECT g FROM generate_series(1, 10) g;
	INSERT INTO pk VALUES (42);
	CREATE TABLE fk (id int PRIMARY KEY, pid int REFERENCES pk(id));
	INSERT INTO fk SELECT g, g FROM generate_series(1, 10) g;
]);

my $slot = $node->safe_psql('postgres',
	q[SELECT ctid FROM pk WHERE id = 42]);

# Delete it, but do not vacuum: the entry for 42 stays in the index the
# constraint names right now.
$node->safe_psql('postgres', q[DELETE FROM pk WHERE id = 42]);

my $before = $node->safe_psql('postgres',
	q[SELECT conindid FROM pg_constraint WHERE conname = 'fk_pid_fkey']);

# The rebuild goes first and stops at the swap, past its own waiting.  It
# is also told to fail once the old index is dead.
my $rebuild = $node->background_psql('postgres', on_error_stop => 0);
$rebuild->query_safe(
	q[
	SELECT injection_points_set_local();
	SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
	SELECT injection_points_attach('reindex-relation-concurrently-before-drop', 'error');
]);
$rebuild->query_until(
	qr/rebuilding/, q[
\echo rebuilding
REINDEX INDEX CONCURRENTLY pk_pkey;
]);

ok( $node->poll_query_until(
		'postgres', q[
		SELECT count(*) > 0 FROM pg_stat_activity
		WHERE wait_event = 'reindex-relation-concurrently-before-swap']),
	'the rebuild is past its waiting');

# The writer reads the constraint and stops before it locks the
# referenced table.  It holds no lock there, so the rebuild below never
# waits for it.  Its snapshot is younger than the delete above, so it
# does not hold the vacuum back either.
my $writer = $node->background_psql('postgres', on_error_stop => 0);
$writer->query_safe(
	q[
	SELECT injection_points_set_local();
	SELECT injection_points_attach('ri-before-pk-lock', 'wait');
]);
$writer->query_until(
	qr/writing/, q[
\echo writing
UPDATE fk SET pid = 42 WHERE id = 1;
]);

ok( $node->poll_query_until(
		'postgres', q[
		SELECT count(*) > 0 FROM pg_stat_activity
		WHERE wait_event = 'ri-before-pk-lock']),
	'the foreign key check has read the constraint');

# Let the rebuild swap the constraint over and mark the old index dead,
# then fail.  Its locks go away with it.
$node->safe_psql('postgres',
	q[SELECT injection_points_wakeup('reindex-relation-concurrently-before-swap')]
);
$node->safe_psql('postgres',
	q[SELECT injection_points_detach('reindex-relation-concurrently-before-swap')]
);

ok( $node->poll_query_until(
		'postgres',
		"SELECT NOT indislive FROM pg_index WHERE indexrelid = $before"),
	'the index the check read is dead but still there');

isnt(
	$node->safe_psql('postgres',
		q[SELECT conindid FROM pg_constraint WHERE conname = 'fk_pid_fkey']),
	$before,
	'the constraint names a different index now');

$node->safe_psql('postgres',
	q[SELECT injection_points_detach('reindex-relation-concurrently-before-drop')]
);

# The vacuum frees the slot the deleted row held.  It cleans the live
# index only, so the dead one keeps its entry for 42.
$node->safe_psql('postgres', 'VACUUM pk');
$node->safe_psql('postgres', 'INSERT INTO pk VALUES (999)');

is( $node->safe_psql('postgres', q[SELECT ctid FROM pk WHERE id = 999]),
	$slot,
	'the new row took the slot the deleted row held');

is( $node->safe_psql(
		'postgres', "SELECT count(*) FROM pk WHERE id = 42"),
	'0',
	'there is no row with the key the writer is about to reference');

# The check has to resolve the constraint to the index it names now.  The
# dead one says 42 is there; it is not.
$node->safe_psql('postgres',
	q[SELECT injection_points_wakeup('ri-before-pk-lock')]);
$node->safe_psql('postgres',
	q[SELECT injection_points_detach('ri-before-pk-lock')]);

my $banner = 'done_marker';
$writer->{stdin} .= "\\echo $banner\n\\warn $banner\n";
pump_until($writer->{run}, $writer->{timeout}, \$writer->{stdout},
	qr/$banner/);
pump_until($writer->{run}, $writer->{timeout}, \$writer->{stderr},
	qr/$banner/);
my $err = $writer->{stderr};
$err =~ s/$banner//g;
$err =~ s/\s+/ /g;
$err =~ s/^\s+|\s+$//g;

$writer->quit;
$rebuild->quit;

like(
	$err,
	qr/violates foreign key constraint/,
	'the write is rejected, the referenced key does not exist');

is( $node->safe_psql(
		'postgres', q[
		SELECT count(*) FROM fk f
		WHERE NOT EXISTS (SELECT 1 FROM pk WHERE pk.id = f.pid)]),
	'0',
	'no foreign key row without a referenced row');

$node->stop;
done_testing();
