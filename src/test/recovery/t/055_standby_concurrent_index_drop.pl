
# Copyright (c) 2026, PostgreSQL Global Development Group

# Planning on a standby while replay removes an index.
#
# get_relation_info() reads a relation's index list from the relcache and
# then opens each index.  On a primary nothing can remove one in between:
# index_drop() waits for every locker of the table before it deletes the
# catalog entry, so the lock the planner already holds on the table is
# enough.
#
# Replay has no such interlock.  Replaying the drop of an index -- the
# one DROP INDEX CONCURRENTLY performs, or the one at the end of REINDEX
# CONCURRENTLY -- takes AccessExclusiveLock on the index alone, and
# releases it when the replayed transaction commits.  Nothing makes it
# wait for a backend that holds only the table's lock and has already
# read the index list, so a query being planned on the standby can find
# the index gone when it goes to open it.

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

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->start;

$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points');

$primary->safe_psql(
	'postgres', q[
	CREATE TABLE t (i int, j int);
	INSERT INTO t SELECT g, g FROM generate_series(1, 1000) g;
	CREATE INDEX t_i_idx ON t (i);
	ANALYZE t;
]);

$primary->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'backup', has_streaming => 1);
# The point of this test is that replay does not wait for a backend
# holding only the table's lock, so that backend must not be cancelled
# for standing in replay's way either -- it is not standing in it.
# hot_standby_feedback keeps the snapshot this query holds from being a
# conflict, which leaves only the lock replay takes on the index -- and
# this backend does not hold that one, which is the whole point.
$standby->append_conf('postgresql.conf',
	"hot_standby_feedback = on\nmax_standby_streaming_delay = 0");
$standby->start;

# The planner on the standby stops after it has the index list and
# before it opens the index.
my $s = $standby->background_psql('postgres', on_error_stop => 0);
# Attached for the whole server rather than this session: the wakeup
# comes from another one.
$standby->safe_psql('postgres',
	q[SELECT injection_points_attach('get-relation-info-before-index-open', 'wait')]
);

$s->query_until(
	qr/planning_started/, q[
\echo planning_started
SELECT count(*) FROM t WHERE i = 42;
]);

ok( $standby->poll_query_until(
		'postgres', q[
		SELECT count(*) > 0 FROM pg_stat_activity
		WHERE wait_event = 'get-relation-info-before-index-open']),
	'standby planner is holding the index list');

# The index goes away underneath it, and the standby replays that.
$primary->safe_psql('postgres', 'DROP INDEX CONCURRENTLY t_i_idx');
$primary->wait_for_catchup($standby, 'replay');

is( $standby->safe_psql(
		'postgres', "SELECT count(*) FROM pg_class WHERE relname = 't_i_idx'"),
	'0',
	'replay has removed the index on the standby');

$standby->safe_psql(
	'postgres', q[
	SELECT injection_points_wakeup('get-relation-info-before-index-open');
	SELECT injection_points_detach('get-relation-info-before-index-open');
]);

# Planning must finish without the index rather than fail on it.
my $out = $s->query('SELECT 1');
my $err = $s->{stderr};
$err =~ s/\s+/ /g;
unlike(
	$err,
	qr/could not open relation with OID/,
	'planning survived an index removed by replay');
is($s->{stderr} =~ /\S/ ? 0 : 1, 1, 'the planning session reported nothing');

$s->quit;

is( $standby->safe_psql('postgres', 'SELECT count(*) FROM t WHERE i = 42'),
	'1',
	'the standby can still read the table');

$standby->stop;
$primary->stop;

done_testing();
