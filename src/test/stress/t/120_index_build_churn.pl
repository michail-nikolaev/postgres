
# Copyright (c) 2026, PostgreSQL Global Development Group

# One small table, one index, and a rotation that does nothing but drop
# and rebuild that index concurrently while rows are inserted.
#
# This is the shape of contrib/amcheck/t/002_cic.pl, and it is here
# because the other scenarios cannot reach what that test reaches.  They
# spread CREATE INDEX CONCURRENTLY across several rotation entries and
# several tables, so any one index is rebuilt rarely; 002_cic spends
# about a third of its transactions on a build-and-check of a single
# index on a tiny relation.  Rate is the whole point.
#
# Gates fdd965d074d, "Avoid race in RelationBuildDesc() affecting CREATE
# INDEX CONCURRENTLY".  Without its retry loop -- a backend that absorbs
# an invalidation while building the descriptor for the new index stops
# maintaining it -- this fails with
#
#   ERROR:  heap tuple (10,13) from table "pgbench_history" lacks
#   matching index tuple within index "pgb_history_delta_idx"
#
# from the check that follows each rebuild.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'index_build_churn',
	{
		indexes => ['btree_history_delta'],
		load => ['history_insert'],
		# Only the concurrent build, so every turn of the rotation is one.
		ddl => ['drop_create_index'],
		chaos => 'relcache_probe',
		clients => 30,
		tags => ['ci'],
	});
