# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for crash recovery in the middle of CONCURRENTLY
# commands.
#
# Writer clients apply balanced pairs of updates (so the sum over the
# val column is invariant at every commit) while one client rotates
# through REPACK (CONCURRENTLY), DROP/CREATE INDEX CONCURRENTLY,
# REINDEX INDEX CONCURRENTLY and REINDEX TABLE CONCURRENTLY.  After a
# couple of seconds the server is killed with SIGKILL, interrupting
# whatever was in flight, and restarted.
#
# After each crash recovery cycle:
# - the sum invariant must hold (interrupted transactions rolled back),
# - no replication slots may have leaked (REPACK (CONCURRENTLY) uses a
#   transient slot internally),
# - leftover invalid indexes (which interrupted concurrent builds may
#   legitimately leave behind) must be droppable, and
# - the surviving indexes must pass amcheck.
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
	'skipping disabled crash recovery stress test');

my $cycles = 1 + $stressval;
my $nrows = 10_000;

my $node;

#
# Test set-up
#
$node = stress_init_node('crash_recovery',
	extra_conf => [ 'max_connections = 50' ]);
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

my $sum = $node->safe_psql('postgres',
	q(SELECT SUM(val) AS sum FROM tbl));

my $ops_sql = $node->basedir . '/concurrent_ops.sql';
PostgreSQL::Test::Utils::append_to_file(
	$ops_sql,
	stress_ddl_gate(
		indent => '',
		# No sleep: the point is to be interrupted mid-command as often
		# as possible.
		sleep_ms => 0,
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

foreach my $cycle (1 .. $cycles)
{
	# Run the churn; its outcome is deliberately ignored, since the
	# server is going to be killed underneath it.
	my @cmd = (
		'pgbench', '--no-vacuum', '--client=20', '--jobs=4',
		'-T', $PostgreSQL::Test::Utils::timeout_default,
		'-p', $node->port, '-h', $node->host,
		'-f', $ops_sql, 'postgres');
	my ($out, $err) = ('', '');
	my $h = start \@cmd, '>', \$out, '2>', \$err;

	sleep(2);
	$node->kill9;
	finish $h;

	# Crash recovery must succeed.
	$node->start;

	is( $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
		$sum, "cycle $cycle: sum invariant holds after crash recovery");

	is( $node->safe_psql('postgres',
			q(SELECT COUNT(*) FROM pg_replication_slots)),
		'0', "cycle $cycle: no leaked replication slots");

	# Interrupted concurrent index builds may leave invalid indexes
	# behind; that is documented.  They must be droppable, though.
	$node->safe_psql(
		'postgres', q(
		DO $$
		DECLARE
			idx regclass;
		BEGIN
			FOR idx IN SELECT indexrelid::regclass FROM pg_index
				WHERE indrelid = 'tbl'::regclass AND NOT indisvalid
			LOOP
				EXECUTE format('DROP INDEX %s', idx);
			END LOOP;
		END;
		$$;
	));

	# The DDL rotation may have been killed between the drop and the
	# re-creation of the secondary index.
	$node->safe_psql('postgres',
		q(CREATE INDEX IF NOT EXISTS tbl_val_idx ON tbl(val)));

	$node->safe_psql(
		'postgres', q(
		SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
		SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
	));
}

$node->stop;

done_testing();
