# Copyright (c) 2026, PostgreSQL Global Development Group

# Reading a relation's index list on a standby while replay removes an
# index.
#
# Several places read the list of indexes of a relation out of the
# relcache and then open each index by OID.  On a primary nothing can
# remove one in between: index_drop() waits out every locker of the table
# before it deletes the catalog entry, so the lock the reader already
# holds on the table is enough.
#
# Replay has no such interlock.  Replaying the drop of an index -- the
# one DROP INDEX CONCURRENTLY performs, or the one at the end of REINDEX
# CONCURRENTLY -- takes AccessExclusiveLock on the index alone, and
# releases it when the replayed transaction commits.  Nothing makes it
# wait for a backend that holds only the table's lock and has already
# read the index list, so such a backend on the standby can find the
# index gone when it goes to open it.
#
# Covered here: the planner (get_relation_info()) and detoasting
# (toast_open_indexes()).  calculate_indexes_size() reads the list the
# same way and is fixed the same way, but is left untested rather than
# given an injection point of its own.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->start;

$primary->safe_psql('postgres', 'CREATE EXTENSION injection_points');

# "t" is the relation the planner reads the index list of.  "tt" has no
# index of its own, so that reindexing it touches only the index of its
# toast relation, which is what the detoasting case needs; EXTERNAL
# storage keeps its one row out of line.
$primary->safe_psql(
	'postgres', q[
	CREATE TABLE t (i int);
	INSERT INTO t SELECT g FROM generate_series(1, 1000) g;
	CREATE INDEX t_i_idx ON t (i);
	ANALYZE t;

	CREATE TABLE tt (id int, v text);
	ALTER TABLE tt ALTER COLUMN v SET STORAGE EXTERNAL;
	INSERT INTO tt VALUES (1, repeat('x', 100000));
]);

my $toastname = $primary->safe_psql(
	'postgres', q[
	SELECT c.relname FROM pg_class c
	  JOIN pg_class p ON p.reltoastrelid = c.oid
	 WHERE p.oid = 'tt'::regclass]);

$primary->backup('backup');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'backup', has_streaming => 1);
# The point of this test is that replay does not wait for a backend
# holding only the table's lock, so that backend must not be cancelled
# for standing in replay's way either -- it is not standing in it.
# hot_standby_feedback keeps the snapshot such a query holds from being a
# conflict, which leaves only the lock replay takes on the index, and the
# parked backend does not hold that one, which is the whole point.  With
# max_standby_streaming_delay at 0, a conflict we did not foresee ends
# the query at once instead of quietly holding replay back.
$standby->append_conf(
	'postgresql.conf', q[
hot_standby_feedback = on
wal_receiver_status_interval = 1s
max_standby_streaming_delay = 0
]);
$standby->start;

my $psql = $standby->background_psql('postgres', on_error_stop => 0);
# Every point this test attaches is local to this session and keyed on a
# relation name, so no other backend, and no query over a catalog, ever
# waits at one.
$psql->query_safe('SELECT injection_points_set_local()');

# Park $query on the standby at $point, which it reaches once it has the
# index list of $cond in hand and has opened nothing yet.  Let $ddl on the
# primary remove the index it is about to open, then let it go on: it must
# print $want and report no error.  $gone is a query on the standby that
# must report the index is really no longer there.
sub race_with_index_drop
{
	my (%p) = @_;

	$psql->query_safe(
		"SELECT injection_points_attach('$p{point}', 'wait', '$p{cond}')");
	$psql->query_until(
		qr/started/, qq[
\\echo started
$p{query}
\\echo done
]);
	$standby->wait_for_event('client backend', $p{point});

	# Let the standby's xmin reach the primary before any of this WAL is
	# generated.  Until it does, the primary may clean up rows the parked
	# snapshot still needs, and replay of that cleanup would cancel the
	# backend for a reason that has nothing to do with this test.
	$primary->poll_query_until('postgres',
		'SELECT backend_xmin IS NOT NULL FROM pg_stat_replication')
	  or die "timed out waiting for hot standby feedback";

	# The index goes away underneath the parked backend.
	$primary->safe_psql('postgres', $p{ddl});
	$primary->wait_for_catchup($standby, 'replay');
	is($standby->safe_psql('postgres', $p{gone}),
		'0', "$p{what}: replay has removed the index on the standby");

	# Detach before waking up, so that a retry does not park again.
	$standby->safe_psql(
		'postgres', qq[
		SELECT injection_points_detach('$p{point}');
		SELECT injection_points_wakeup('$p{point}');
	]);

	my $out = $psql->query_until(qr/done/, '');
	like($out, qr/^\Q$p{want}\E$/m, "$p{what}: the parked query finished");
	is($psql->{stderr}, '', "$p{what}: the parked query reported no error");

	# Leave a clean slate, so that one failing case does not take the
	# cases after it down with it.
	$psql->{stderr} = '';
}

# The planner reads the index list in get_relation_info().
race_with_index_drop(
	what => 'planner, DROP INDEX CONCURRENTLY',
	point => 'get-relation-info-before-index-open',
	cond => 't',
	query => q[SELECT 'rows=' || count(*) FROM t WHERE i = 42;],
	want => 'rows=1',
	ddl => 'DROP INDEX CONCURRENTLY t_i_idx',
	gone => "SELECT count(*) FROM pg_class WHERE relname = 't_i_idx'");

# REINDEX CONCURRENTLY reaches the same window from the other end: it
# leaves an index of the same name behind, but the OID the parked backend
# is holding is the one it drops.
$primary->safe_psql('postgres', 'CREATE INDEX t_i_idx ON t (i)');
$primary->wait_for_catchup($standby, 'replay');
my $oid = $primary->safe_psql('postgres', "SELECT 't_i_idx'::regclass::oid");
race_with_index_drop(
	what => 'planner, REINDEX CONCURRENTLY',
	point => 'get-relation-info-before-index-open',
	cond => 't',
	query => q[SELECT 'rows=' || count(*) FROM t WHERE i = 42;],
	want => 'rows=1',
	ddl => 'REINDEX INDEX CONCURRENTLY t_i_idx',
	gone => "SELECT count(*) FROM pg_class WHERE oid = $oid");

# Detoasting reads it in toast_open_indexes().  Here the list named only
# the one index, so leaving it out is not an option: the read has to pick
# up the index REINDEX CONCURRENTLY put in its place.
my $toastoid = $primary->safe_psql(
	'postgres', q[
	SELECT indexrelid FROM pg_index
	 WHERE indrelid = (SELECT reltoastrelid FROM pg_class
						 WHERE oid = 'tt'::regclass)]);
race_with_index_drop(
	what => 'detoasting',
	point => 'toast-open-indexes-before-index-open',
	cond => $toastname,
	query => q[SELECT 'len=' || length(v) FROM tt WHERE id = 1;],
	want => 'len=100000',
	ddl => 'REINDEX TABLE CONCURRENTLY tt',
	gone => "SELECT count(*) FROM pg_class WHERE oid = $toastoid");

$psql->quit;

is($standby->safe_psql('postgres', 'SELECT count(*) FROM t WHERE i = 42'),
	'1', 'the standby can still read the table');
is( $standby->safe_psql('postgres', 'SELECT length(v) FROM tt WHERE id = 1'),
	'100000',
	'the standby can still detoast');

$standby->stop;
$primary->stop;

done_testing();
