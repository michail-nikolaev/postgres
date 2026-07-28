
# Copyright (c) 2026, PostgreSQL Global Development Group

# Apply using a replica identity index that is swapped underneath it.
#
# The apply worker takes the index it will search the local relation by
# from the relation map entry, and uses it a little later.  REINDEX
# CONCURRENTLY replaces that index with another one, and its swap needs
# only ShareUpdateExclusiveLock on the table, which does not conflict
# with the RowExclusiveLock apply holds -- so the swap commits in that
# window and the index apply is about to use is no longer the relation's
# identity.
#
# The rebuild is pinned at the swap first, once its own waiting is
# behind it.  The other order does not work: REINDEX CONCURRENTLY waits
# for older snapshots, so an apply worker paused mid-transaction would
# block the rebuild rather than race it.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $publisher = PostgreSQL::Test::Cluster->new('publisher');
$publisher->init(allows_streaming => 'logical');
$publisher->start;

my $subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$subscriber->init;
$subscriber->start;

if (!$subscriber->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$subscriber->safe_psql('postgres', 'CREATE EXTENSION injection_points');

my $ddl = q[CREATE TABLE t (id int PRIMARY KEY, v int);];
$publisher->safe_psql('postgres', $ddl);
$subscriber->safe_psql('postgres', $ddl);

$publisher->safe_psql('postgres',
	q[INSERT INTO t SELECT g, g FROM generate_series(1, 20) g]);

$publisher->safe_psql('postgres', 'CREATE PUBLICATION pub FOR TABLE t');
my $connstr = $publisher->connstr . ' dbname=postgres';
$subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub CONNECTION '$connstr' PUBLICATION pub");
$subscriber->wait_for_subscription_sync($publisher, 'sub');

is($subscriber->safe_psql('postgres', 'SELECT count(*) FROM t'),
	'20', 'initial data copied');

my $before = $subscriber->safe_psql('postgres',
	q[SELECT oid FROM pg_class WHERE relname = 't_pkey']);

# The rebuild goes first and stops at the swap, so that from here on it
# needs nothing from the apply worker.
my $rebuild = $subscriber->background_psql('postgres', on_error_stop => 0);
$rebuild->query_safe(
	q[
	SELECT injection_points_set_local();
	SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
]);
$rebuild->query_until(
	qr/rebuilding/, q[
\echo rebuilding
REINDEX INDEX CONCURRENTLY t_pkey;
]);

ok( $subscriber->poll_query_until(
		'postgres', q[
		SELECT count(*) > 0 FROM pg_stat_activity
		WHERE wait_event = 'reindex-relation-concurrently-before-swap']),
	'the rebuild is past its waiting');

# Now make apply pick up the index and stop before it uses it.  The point
# is attached for the whole server: the apply worker is not a session
# this test can attach anything in.
$subscriber->safe_psql('postgres',
	q[SELECT injection_points_attach('apply-before-find-repl-tuple', 'wait')]);

$publisher->safe_psql('postgres', 'UPDATE t SET v = 99 WHERE id = 7');

ok( $subscriber->poll_query_until(
		'postgres', q[
		SELECT count(*) > 0 FROM pg_stat_activity
		WHERE wait_event = 'apply-before-find-repl-tuple']),
	'apply has taken the index and not used it yet');

# Let the rebuild swap the index.  Only the swap is needed: the old index
# is left in place for now, since dropping it waits for apply's lock.
$subscriber->safe_psql('postgres',
	q[SELECT injection_points_wakeup('reindex-relation-concurrently-before-swap')]
);
$subscriber->safe_psql('postgres',
	q[SELECT injection_points_detach('reindex-relation-concurrently-before-swap')]
);

ok( $subscriber->poll_query_until(
		'postgres',
		"SELECT count(*) = 0 FROM pg_class"
		  . " WHERE oid = $before AND relname = 't_pkey'"),
	'the identity index has been swapped');

# Release apply.  The index it holds is no longer the relation's
# identity, which it has to notice rather than trust.
$subscriber->safe_psql('postgres',
	q[SELECT injection_points_wakeup('apply-before-find-repl-tuple')]);
$subscriber->safe_psql('postgres',
	q[SELECT injection_points_detach('apply-before-find-repl-tuple')]);
$rebuild->quit;

$publisher->wait_for_catchup('sub');

is($subscriber->safe_psql('postgres', 'SELECT v FROM t WHERE id = 7'),
	'99', 'the update was applied through the index in force now');

# Replication still works, which it would not if the worker had died.
$publisher->safe_psql('postgres', 'UPDATE t SET v = 123 WHERE id = 8');
$publisher->wait_for_catchup('sub');
is($subscriber->safe_psql('postgres', 'SELECT v FROM t WHERE id = 8'),
	'123', 'replication continues');

$subscriber->stop;
$publisher->stop;
done_testing();
