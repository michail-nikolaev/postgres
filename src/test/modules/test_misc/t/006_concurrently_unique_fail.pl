
# Copyright (c) 2024-2024, PostgreSQL Global Development Group

# Test REINDEX INDEX CONCURRENTLY with concurrent INSERT
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my ($node, $result);

#
# Test set-up
#
$node = PostgreSQL::Test::Cluster->new('CIC_test');
$node->init;
$node->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node->start;
$node->safe_psql('postgres', q(CREATE UNLOGGED TABLE tbl(i int primary key, updated_at timestamp)));

$node->pgbench(
		'--no-vacuum --client=2 -j 2 --transactions=1000',
		0,
		[qr{actually processed}],
		[qr{^$}],
		'concurrent INSERTs, UPDATES and RC',
		{
			'01_updates' => q(
				INSERT INTO tbl VALUES(13,now()) ON CONFLICT(i) DO UPDATE SET updated_at = now();
			),
			'02_reindex' => q(
				SELECT pg_try_advisory_lock(42)::integer AS gotlock \gset
				\if :gotlock
					REINDEX INDEX CONCURRENTLY tbl_pkey;
					SELECT pg_advisory_unlock(42);
				\endif
			),
		});
$node->stop;
done_testing();