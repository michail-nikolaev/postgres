
# Copyright (c) 2026, PostgreSQL Global Development Group

# HOT chains being pruned underneath a concurrent index build.
#
# Every other scenario declares an index on abalance, which is the
# column the load moves -- so no update to pgbench_accounts is ever a
# HOT update and no HOT chain is ever pruned.  This one indexes bid
# instead, which nothing writes, so the updates stay on the page and
# other backends prune them opportunistically while a build is walking
# the table.
#
# The table is large on purpose.  A prune only costs a build anything if
# it reaches a page the build has not scanned yet, so the window is the
# duration of the scan; on a one-page relation there is no window.
#
# Gates the revert e28bb885196, "Revert changes to CONCURRENTLY that
# 'sped up' Xmin advance", which undid d9d076222f5.  With d9d076222f5
# put back -- ComputeXidHorizons() skipping PROC_IN_SAFE_IC backends
# when it computes the horizon for user tables -- this fails with
#
#   ERROR:  heap tuple (50227,18) from table "pgbench_accounts" lacks
#   matching index tuple within index "pgbench_accounts_pkey"
#
# from the in-run check that follows each rebuild, or from amcheck at
# the end.  Ignoring the building backend's xmin lets everyone else
# prune tuples that the build's snapshot still needed, and the index it
# marks valid is missing rows.  Reproduced by hand at roughly one
# rebuild in three; see REGRESSIONS.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'hot_prune',
	{
		schema => ['low_fillfactor'],
		pgbench_scale => 30,
		indexes => ['btree_bid'],
		load => ['hot_churn'],
		# Both forms of concurrent build, on indexes over columns the
		# load leaves alone: the declared one through DROP/CREATE, and
		# the primary key through REINDEX.
		ddl => [ 'drop_create_index', 'reindex_pkey_concurrently' ],
		# hot_churn moves no money between tables, so there is no balance
		# invariant here -- it declares as much, which is what keeps the
		# balances check from joining.  What this scenario asserts is that
		# the indexes it rebuilt still contain every live row.
		clients => 20,
		tags => ['ci'],
	});
