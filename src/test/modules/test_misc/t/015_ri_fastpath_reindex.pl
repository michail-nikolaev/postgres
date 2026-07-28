
# Copyright (c) 2026, PostgreSQL Global Development Group

# A foreign key check racing a rebuild of the index it resolves through.
#
# The RI fast path looks up the constraint, takes RowShareLock on the
# referenced table, and opens the index named by conindid.  Reading
# conindid before taking that lock is not safe: REINDEX CONCURRENTLY
# repoints the constraint at a new index and drops the old one, and it
# waits only for backends that hold a lock on the referenced table.  A
# backend that has read the constraint but not yet taken that lock is not
# one of them, so the index it is about to open can be gone by then.
#
# The rebuild is pinned first, once its own waiting is behind it.  Doing
# it the other way round does not work: REINDEX CONCURRENTLY waits for
# older snapshots, so a writer paused mid-statement would block the
# rebuild rather than race it.

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

$node->safe_psql(
	'postgres', q[
	CREATE TABLE pk (id int PRIMARY KEY);
	INSERT INTO pk SELECT g FROM generate_series(1, 100) g;
	CREATE TABLE fk (id int PRIMARY KEY, pid int REFERENCES pk(id));
	INSERT INTO fk SELECT g, g FROM generate_series(1, 100) g;
]);

my $before = $node->safe_psql('postgres',
	q[SELECT conindid FROM pg_constraint WHERE conname = 'fk_pid_fkey']);

# The rebuild goes first, and stops at the swap: from here on it wants
# nothing from other backends until it takes its own locks.
my $rebuild = $node->background_psql('postgres', on_error_stop => 0);
$rebuild->query_safe(
	q[
	SELECT injection_points_set_local();
	SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
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

# Now a writer reads the constraint and stops before it locks the
# referenced table.  It holds no lock there, so the rebuild below never
# waits for it.
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

# Let the rebuild finish.  It repoints the constraint and drops the index
# the writer read out of it.
$node->safe_psql('postgres',
	q[SELECT injection_points_wakeup('reindex-relation-concurrently-before-swap')]
);
$node->safe_psql('postgres',
	q[SELECT injection_points_detach('reindex-relation-concurrently-before-swap')]
);

ok( $node->poll_query_until(
		'postgres',
		"SELECT count(*) = 0 FROM pg_class WHERE oid = $before"),
	'the index the check read has been dropped');

isnt(
	$node->safe_psql('postgres',
		q[SELECT conindid FROM pg_constraint WHERE conname = 'fk_pid_fkey']),
	$before,
	'the constraint names a different index now');

# The check has to resolve the constraint to the index it names now.
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

is($err, '', 'the foreign key check survived the rebuild');

$writer->quit;
$rebuild->quit;

is($node->safe_psql('postgres', 'SELECT pid FROM fk WHERE id = 1'),
	'42', 'the row was updated');

# The constraint must still be enforced, not merely not crashing.
my (undef, undef, $viol) = $node->psql('postgres',
	'INSERT INTO fk VALUES (999, 12345);', on_error_stop => 0);
like(
	$viol,
	qr/violates foreign key constraint/,
	'the constraint is still enforced');

$node->stop;
done_testing();
