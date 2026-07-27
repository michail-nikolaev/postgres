
# Copyright (c) 2026, PostgreSQL Global Development Group

# Foreign key checks, which resolve the parent through the index behind
# its primary key, while that index is rebuilt underneath them.
#
# Without the fix in ri_triggers.c that re-reads the constraint under
# the referenced table's lock, this fails in about one run in seven at
# stress_concurrently=4 -- three in twenty measured -- with
#
#   ERROR:  could not open relation with OID <n>
#
# from the referential integrity check: the fast path caches conindid
# before anything locks the referenced table, and REINDEX CONCURRENTLY
# repoints the constraint and drops the index it named.  Nothing at the
# default duration; this one needs the higher stressval.
#
# Two things about the shape here are load-bearing, and both were
# arrived at by measurement rather than taste.  The parent is a small
# table of its own: the race is against its primary key being swapped,
# so the rebuild has to finish quickly, which rules out
# pgbench_accounts for being large and pgbench_tellers for being the
# table every TPC-B transaction updates -- a rebuild there spends its
# time waiting for lockers.  And the rotation includes
# reindex_pkey_concurrently, because the ordinary commands only rebuild
# a primary key as part of a whole table.
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
