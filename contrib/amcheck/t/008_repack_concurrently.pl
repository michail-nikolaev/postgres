
# Copyright (c) 2021-2025, PostgreSQL Global Development Group

# Test REPACK CONCURRENTLY with concurrent modifications
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my $node;

#
# Test set-up
#
$node = PostgreSQL::Test::Cluster->new('CIC_test');
$node->init;
$node->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node->append_conf(
	'postgresql.conf', qq(
wal_level = logical
));

my $no_hot = int(rand(2));

$node->start;
$node->safe_psql('postgres', q(CREATE TABLE tbl(i SERIAL PRIMARY KEY, j int)));
if ($no_hot)
{
	$node->safe_psql('postgres', q(CREATE INDEX test_idx ON tbl(j);));
}
else
{
	$node->safe_psql('postgres', q(CREATE INDEX test_idx ON tbl(i);));
}

# Load amcheck
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));

my $sum = $node->safe_psql('postgres', q(
	SELECT SUM(j) AS sum FROM tbl
));

$node->safe_psql('postgres', q(CREATE UNLOGGED SEQUENCE last_j START 1 INCREMENT 1;));


$node->pgbench(
'--no-vacuum --client=30 --jobs=4 --exit-on-abort --transactions=1000',
0,
[qr{actually processed}],
[qr{^$}],
'concurrent operations with REINDEX/CREATE INDEX CONCURRENTLY',
{
	'concurrent_ops' => qq(
		SELECT pg_try_advisory_lock(42)::integer AS gotlock \\gset
		\\if :gotlock
			REPACK (CONCURRENTLY) tbl USING INDEX tbl_pkey;
			SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
			SELECT bt_index_parent_check('test_idx', heapallindexed => true);
			\\sleep 10 ms

			REPACK (CONCURRENTLY) tbl USING INDEX test_idx;
			SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
			SELECT bt_index_parent_check('test_idx', heapallindexed => true);
			\\sleep 10 ms

			REPACK (CONCURRENTLY) tbl;
			SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
			SELECT bt_index_parent_check('test_idx', heapallindexed => true);
			\\sleep 10 ms

			SELECT pg_advisory_unlock(42);
		\\else
			SELECT pg_advisory_lock(43);
				BEGIN;
				INSERT INTO tbl(j) VALUES (nextval('last_j')) RETURNING j \\gset p_
				COMMIT;
			SELECT pg_advisory_unlock(43);
			\\sleep 1 ms

			BEGIN
			--TRANSACTION ISOLATION LEVEL REPEATABLE READ
			;
			SELECT 1;
			\\sleep 1 ms
			SELECT COUNT(*) AS count FROM tbl WHERE j <= :p_j \\gset p_
			\\if :p_count != :p_j
				COMMIT;
				SELECT (:p_count) / 0;
			\\endif

			COMMIT;
		\\endif
	)
});

$node->stop;
done_testing();
