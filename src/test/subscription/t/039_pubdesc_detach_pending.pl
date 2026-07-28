
# Copyright (c) 2026, PostgreSQL Global Development Group

# A publication descriptor built for a partition that is pending detach.
#
# ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY marks the partition
# and commits, then waits for the transactions that can still see it.
# Between those two the partition is in a state no other path produces:
# pg_class still says relispartition, because that is cleared only when
# the detach finishes, while get_partition_ancestors() already reports
# nothing, because it stops at a partition marked as detaching.
#
# RelationBuildPublicationDesc() took relispartition to mean the ancestor
# list is not empty and asked for its last element, which is an assertion
# failure on an assert-enabled build and a null dereference otherwise.  A
# write to the partition reaches it through CheckCmdReplicaIdentity() as
# soon as the table is published.
#
# The state is easy to arrive at and outlives the command that made it: a
# detach that is interrupted while waiting leaves the partition marked,
# and every later write to it goes down the same path.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init(allows_streaming => 'logical');
$node->start;

$node->safe_psql(
	'postgres', q[
	CREATE TABLE p (id int NOT NULL, v int) PARTITION BY RANGE (id);
	CREATE TABLE p1 PARTITION OF p FOR VALUES FROM (0) TO (100);
	ALTER TABLE p ADD PRIMARY KEY (id);
	INSERT INTO p SELECT g, g FROM generate_series(1, 50) g;
	CREATE PUBLICATION pub FOR TABLE p;
]);

# A session holding the parent makes the detach stop in its wait phase,
# after it has marked the partition and committed.  Cancelling it there
# leaves the mark behind.
my $holder = $node->background_psql('postgres');
$holder->query_safe('BEGIN');
$holder->query_safe('SELECT count(*) FROM p');

my ($rc, $out, $err);
{
	local $ENV{PGOPTIONS} = '-c lock_timeout=2s';
	($rc, $out, $err) = $node->psql('postgres',
		'ALTER TABLE p DETACH PARTITION p1 CONCURRENTLY;',
		on_error_stop => 0);
}
like(
	$err,
	qr/canceling statement due to lock timeout/,
	'the detach was interrupted while waiting');

$holder->query_safe('COMMIT');
$holder->quit;

is( $node->safe_psql(
		'postgres',
		q[SELECT relispartition::text FROM pg_class WHERE relname = 'p1']),
	'true',
	'the partition still says it is one');
is( $node->safe_psql(
		'postgres', q[
		SELECT inhdetachpending::text FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid WHERE c.relname = 'p1']),
	'true',
	'and it is marked as detaching');

# The write must go through rather than take the backend down.
($rc, $out, $err) = $node->psql('postgres',
	'UPDATE p1 SET v = 7 WHERE id = 5;', on_error_stop => 0);
is($rc, 0, 'a write to a partition pending detach succeeds');
is($err, '', 'and reports nothing');

is($node->safe_psql('postgres', 'SELECT v FROM p1 WHERE id = 5'),
	'7', 'the row was updated');

# The same path is reached through the parent.
($rc, $out, $err) = $node->psql('postgres',
	'UPDATE p SET v = 8 WHERE id = 6;', on_error_stop => 0);
is($rc, 0, 'a write routed through the parent succeeds');

# Finishing the detach leaves everything consistent.
$node->safe_psql('postgres', 'ALTER TABLE p DETACH PARTITION p1 FINALIZE');
is( $node->safe_psql(
		'postgres',
		q[SELECT relispartition::text FROM pg_class WHERE relname = 'p1']),
	'false',
	'the finished detach cleared the mark');

$node->stop;
done_testing();
