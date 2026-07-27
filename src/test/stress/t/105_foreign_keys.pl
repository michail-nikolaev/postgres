
# Copyright (c) 2026, PostgreSQL Global Development Group

# Foreign key checks, which resolve the parent through the index behind
# its primary key, while that index is rebuilt underneath them.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'foreign_keys',
	{
		schema => [ 'pgbench', 'fk_child' ],
		load => [ 'tpcb_like', 'fk_churn' ],
		# reindex_pkey_concurrently is what makes this scenario guard the
		# RI fast path: the ordinary rotation only rebuilds a primary key
		# as part of reindexing a whole table, which swaps it far too
		# rarely to race against.
		ddl => [ @STANDARD_DDL, 'reindex_pkey_concurrently' ],
		ddl_concurrency => 1,
		checks => [ 'balances', 'no_orphans', 'amcheck' ],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	});
