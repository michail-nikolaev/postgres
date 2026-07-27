
# Copyright (c) 2026, PostgreSQL Global Development Group

# The smallest scale there is: one branch row, so every transaction in
# the mix collides on it, and the DDL runs against tables that are being
# modified as fast as the lock manager allows.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'repack_dml_s1',
	{
		schema => ['pgbench'],
		pgbench_scale => 1,
		# expr_xid belongs at this scale rather than 101's: the expression
		# takes a transaction id every time it is evaluated, which over
		# five million rows costs more than the coverage is worth.
		indexes => [ 'btree_abalance', 'btree_history_delta',
			'toasted_predicate', 'expr_xid' ],
		load => [ 'tpcb_like', 'row_lock' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'index_vs_seq', 'amcheck', 'no_slot_leak' ],
		env => 'standalone',
		clients => 30,
		tags => ['ci'],
	});
