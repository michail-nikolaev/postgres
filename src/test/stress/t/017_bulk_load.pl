# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands against bulk loads.
#
# Unlike the other tests, the writers here work in batches: each
# transaction either inserts a batch of rows with INSERT ... SELECT,
# loads one with COPY, or deletes a whole batch.  Every batch contains
# as many rows with val = 1 as with val = -1, so the sum over the val
# column is zero at every commit no matter which batches happen to be
# present.  This exercises the multi-insert and COPY paths, and the
# bulk index insertions they drive, while one client rotates through
# REPACK (CONCURRENTLY), DROP/CREATE INDEX CONCURRENTLY, REINDEX INDEX
# CONCURRENTLY and REINDEX TABLE CONCURRENTLY.
#
# Afterwards, a COPY ... WITH (FREEZE) into a fresh table created in the
# same transaction checks that path too, together with an index built
# on the frozen rows.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled bulk load stress test');

my $duration = 6 * $stressval;
my $batchsize = 100;

my $node;

#
# Test set-up
#
$node = stress_init_node('bulk_load');
$node->safe_psql(
	'postgres', q(
	CREATE EXTENSION amcheck;
	CREATE TABLE bulk(rid bigserial PRIMARY KEY, batch int, val int);
	CREATE INDEX bulk_batch_idx ON bulk(batch);
));

# Data file for the COPY variant: a batch that sums to zero.  Batch 0
# is reserved for these rows.
my $copyfile = $node->basedir . '/copy_batch.txt';
my $copydata = '';
foreach my $i (1 .. $batchsize)
{
	$copydata .= "0\t" . ($i % 2 == 0 ? 1 : -1) . "\n";
}
PostgreSQL::Test::Utils::append_to_file($copyfile, $copydata);

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'CONCURRENTLY commands with concurrent bulk loads',
	{
		'concurrent_ops' => stress_ddl_gate(
			ddl => [
				'REPACK (CONCURRENTLY) bulk;',
				[
					'DROP INDEX CONCURRENTLY bulk_batch_idx;',
					'CREATE INDEX CONCURRENTLY bulk_batch_idx ON bulk(batch);',
				],
				'REINDEX INDEX CONCURRENTLY bulk_batch_idx;',
				'REINDEX TABLE CONCURRENTLY bulk;',
			],
			post =>
			  "SELECT bt_index_check('bulk_pkey', heapallindexed => true, checkunique => true);",
			else => stress_workload(
				mutations => [
					qq(\\set batch random(1, 50)
					INSERT INTO bulk(batch, val)
						SELECT :batch, CASE WHEN g % 2 = 0 THEN 1 ELSE -1 END
						FROM generate_series(1, $batchsize) g;
					\\sleep 1 ms),
					qq(\\set batch random(1, 50)
					COPY bulk(batch, val) FROM '$copyfile';
					\\sleep 1 ms),
					qq(-- Whole batches only, so the sum stays balanced.
					\\set batch random(1, 50)
					DELETE FROM bulk WHERE batch = :batch;
					\\sleep 1 ms),
				],
				checks => [
					qq(-- Every batch sums to zero, so the total always does.
					SELECT stress_assert(COALESCE(SUM(val), 0) = 0,
						format('batch total is %s, not 0', COALESCE(SUM(val), 0)))
						FROM bulk;),
				],
			),
		),
	});

is( $node->safe_psql('postgres', q(SELECT COALESCE(SUM(val), 0) FROM bulk)),
	'0', 'batches stay balanced after bulk load churn');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('bulk_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('bulk_batch_idx', heapallindexed => true);
));

# COPY ... WITH (FREEZE) needs a table created or truncated in the same
# transaction; build an index on the frozen rows as well.
$node->safe_psql(
	'postgres', qq(
	BEGIN;
	CREATE TABLE frozen(batch int, val int);
	COPY frozen(batch, val) FROM '$copyfile' WITH (FREEZE);
	CREATE INDEX frozen_val_idx ON frozen(val);
	COMMIT;
));

is( $node->safe_psql('postgres', q(SELECT COALESCE(SUM(val), 0) FROM frozen)),
	'0', 'COPY WITH (FREEZE) loaded a balanced batch');

$node->safe_psql('postgres',
	q(SELECT bt_index_parent_check('frozen_val_idx', heapallindexed => true)));

$node->stop;

done_testing();
