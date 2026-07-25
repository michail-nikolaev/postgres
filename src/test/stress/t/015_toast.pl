# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands and VACUUM on a table with
# out-of-line (TOASTed) values.
#
# Writer clients rewrite wide payloads, storing an md5 of the payload
# alongside it in the same statement, so that at every commit each row
# satisfies md5(payload) = h.  Reader clients verify that invariant
# over the whole table: a torn or stale out-of-line value shows up as a
# mismatch.  Meanwhile one client rotates through REPACK
# (CONCURRENTLY) -- which has to move the TOAST relation along with the
# heap -- REINDEX TABLE CONCURRENTLY, DROP/CREATE INDEX CONCURRENTLY,
# and VACUUM, which reclaims dead TOAST chunks.
#
# Afterwards, the main indexes and the TOAST index must pass amcheck.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled TOAST stress test');

my $duration = 6 * $stressval;

# Few but wide rows: the payloads are several kB each, so they are
# stored out of line, and the whole table stays cheap to verify.
my $nrows = 500;

my $node;

#
# Test set-up
#
$node = stress_init_node('toast');
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, payload text, h text);
	-- EXTERNAL disables compression, so the payloads are stored out of
	-- line whatever their content happens to compress to.
	ALTER TABLE tbl ALTER COLUMN payload SET STORAGE EXTERNAL;
	CREATE INDEX tbl_h_idx ON tbl(h);
	INSERT INTO tbl
		SELECT g, repeat(md5(g::text), 128), md5(repeat(md5(g::text), 128))
		FROM generate_series(1, $nrows) g;
));

# Verify the payloads really are stored out of line.
my $toasted = $node->safe_psql('postgres',
	q(SELECT pg_relation_size(reltoastrelid) > 0 FROM pg_class
	  WHERE relname = 'tbl'));
is($toasted, 't', 'payloads are stored out of line');

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'CONCURRENTLY commands and VACUUM with TOASTed values',
	{
		'concurrent_ops' => stress_ddl_gate(
			indent => "\t\t",
			ddl => [
				'REPACK (CONCURRENTLY) tbl;',
				[
					'DROP INDEX CONCURRENTLY tbl_h_idx;',
					'CREATE INDEX CONCURRENTLY tbl_h_idx ON tbl(h);',
				],
				'REINDEX TABLE CONCURRENTLY tbl;',
				'VACUUM tbl;',
			],
			post =>
			  "SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
			else => stress_workload(
				mutations => [
					qq(-- Rewrite one payload, keeping it consistent with its md5.
					\\set num random(1, $nrows)
					UPDATE tbl SET payload = s.v, h = md5(s.v)
						FROM (SELECT repeat(md5(random()::text), 128) AS v) s
						WHERE id = :num;
					\\sleep 1 ms),
				],
				checks => [
					# Every row must be self-consistent, and none may be lost.
					qq(SELECT stress_assert(bad = 0 AND cnt = $nrows,
						format('bad=%s cnt=%s (want 0 bad, $nrows rows)', bad, cnt))
					FROM (SELECT COUNT(*) FILTER (WHERE md5(payload) <> h) AS bad,
						COUNT(*) AS cnt FROM tbl) t;),
				],
			),
		),
	});

my $bad = $node->safe_psql('postgres',
	q(SELECT COUNT(*) FROM tbl WHERE md5(payload) <> h));
is($bad, '0', 'every row is self-consistent after TOAST churn');

is( $node->safe_psql('postgres', q(SELECT COUNT(*) FROM tbl)),
	"$nrows", 'no rows lost after TOAST churn');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_h_idx', heapallindexed => true);
));

# The TOAST index must be sound too.
$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check(indexrelid, heapallindexed => true)
	FROM pg_index
	WHERE indrelid = (SELECT reltoastrelid FROM pg_class
					  WHERE relname = 'tbl');
));

$node->stop;

done_testing();
