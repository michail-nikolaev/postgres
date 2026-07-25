# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for logical replication initial table synchronization
# against concurrent DML and CONCURRENTLY commands on the publisher.
#
# A table sync worker copies a table with COPY under its own snapshot
# and its own temporary slot, then catches up to the apply worker.
# This test keeps restarting that process -- by repeatedly removing a
# table from the subscription and adding it back, which makes the next
# REFRESH PUBLICATION resynchronize it from scratch -- while the
# publisher rewrites the same table and rebuilds its indexes.
#
# The publisher's writers keep the sum over the val column invariant,
# so however many times the table is resynchronized, once the dust
# settles the subscriber must hold exactly the publisher's rows.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use IPC::Run qw(start finish);

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled table sync stress test');

my $duration = 6 * $stressval;
my $nrows = 2000;
my $nclients = 10;

#
# Test set-up
#
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node_publisher->append_conf('postgresql.conf', 'max_connections = 50');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
# Allow several sync workers to be busy at once.
$node_subscriber->append_conf('postgresql.conf',
	'max_sync_workers_per_subscription = 4');
$node_subscriber->start;

$node_publisher->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

$node_subscriber->safe_psql(
	'postgres', q(
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
));

my $sum = $node_publisher->safe_psql('postgres',
	q(SELECT SUM(val) AS sum FROM tbl));

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	q(CREATE PUBLICATION tap_pub FOR TABLE tbl));

my $appname = 'tap_sub';
$node_subscriber->safe_psql(
	'postgres', qq(
	CREATE SUBSCRIPTION tap_sub
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION tap_pub
));

$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

# The publisher rewrites the table and rebuilds its indexes while the
# sync workers are copying it.
my $pub_sql = $node_publisher->basedir . '/pub_ops.sql';
PostgreSQL::Test::Utils::append_to_file(
	$pub_sql,
	stress_ddl_gate(
		indent => '',
		ddl => [
			'REPACK (CONCURRENTLY) tbl;',
			[
				'DROP INDEX CONCURRENTLY tbl_val_idx;',
				'CREATE INDEX CONCURRENTLY tbl_val_idx ON tbl(val);'
			],
			'REINDEX INDEX CONCURRENTLY tbl_val_idx;',
			'REINDEX TABLE CONCURRENTLY tbl;',
		],
		else => qq(\t\\set num_a random(1, $nrows)
\t\\set num_b random(1, $nrows)
\t\\set diff random(1, 10000)
\tBEGIN;
\tUPDATE tbl SET val = val + :diff WHERE id = :num_a;
\t\\sleep 1 ms
\tUPDATE tbl SET val = val - :diff WHERE id = :num_b;
\t\\sleep 1 ms
\tCOMMIT;)
	) . "\n");

my @pub_cmd = (
	'pgbench', '--no-vacuum', "--client=$nclients", "--jobs=$nclients",
	'--exit-on-abort', '-T', $duration,
	'-p', $node_publisher->port, '-h', $node_publisher->host,
	'-f', $pub_sql, 'postgres');

my ($pub_out, $pub_err) = ('', '');
my $pub_h = start \@pub_cmd, '>', \$pub_out, '2>', \$pub_err;

# Meanwhile, keep forcing the table to be synchronized from scratch.
#
# ALTER PUBLICATION needs a ShareUpdateExclusiveLock on the table, and
# the CONCURRENTLY commands running alongside take stronger locks in
# their final steps, so these statements can legitimately deadlock or
# time out against each other.  That is ordinary lock contention rather
# than something this test is meant to catch, so retry instead of
# failing; the run is only interesting if some of them get through,
# which is checked afterwards.
my $resyncs = 0;
my $deadline = time() + $duration;
while (time() < $deadline)
{
	my ($ret) = $node_publisher->psql('postgres',
		q(ALTER PUBLICATION tap_pub DROP TABLE tbl));
	next if $ret != 0;

	$node_subscriber->safe_psql('postgres',
		q(ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION));

	# The table is out of the subscription now, so nothing is being
	# applied to it: empty it, so that the fresh COPY below does not
	# collide with the rows copied last time round.
	$node_subscriber->safe_psql('postgres', q(TRUNCATE tbl));

	# Once the table is out of the publication it must go back in, so
	# that the run ends with a well-defined state; keep trying until it
	# does.
	while (time() < $deadline + 10)
	{
		($ret) = $node_publisher->psql('postgres',
			q(ALTER PUBLICATION tap_pub ADD TABLE tbl));
		last if $ret == 0;
	}

	$node_subscriber->safe_psql('postgres',
		q(ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION));
	$resyncs++;
}

finish $pub_h;

like($pub_out, qr/actually processed/, 'publisher pgbench');

# A REINDEX CONCURRENTLY that loses a deadlock against the ALTER
# PUBLICATION statements above leaves an invalid index behind, and a
# later REINDEX then warns that it is skipping it.  That is documented
# behavior, so filter those warnings out; anything else on stderr is a
# genuine failure.
my $pub_err_filtered = join "\n",
  grep {
	     !/^WARNING:  skipping reindex of invalid index/
	  && !/^HINT:  Use DROP INDEX or REINDEX INDEX\./
  } split(/\n/, $pub_err);
is($pub_err_filtered, '', 'publisher pgbench reported no errors');
note "forced $resyncs resynchronizations";
cmp_ok($resyncs, '>', 0, 'table was resynchronized at least once');

# Let the last resynchronization settle, then compare.
$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);
$node_publisher->wait_for_catchup($appname);

is( $node_publisher->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'publisher sum invariant holds');

my $pub_data = $node_publisher->safe_psql('postgres',
	q(SELECT id, val FROM tbl ORDER BY id));
my $sub_data = $node_subscriber->safe_psql('postgres',
	q(SELECT id, val FROM tbl ORDER BY id));
is($sub_data, $pub_data, 'subscriber matches publisher after resynchronizations');

$node_publisher->safe_psql('postgres',
	q(SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true)));

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
