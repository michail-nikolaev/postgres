
# Copyright (c) 2026, PostgreSQL Global Development Group

# Unique keys under upserts and MERGE, and inserts whose commit order is
# pinned by an advisory lock.
#
# Gates two fixes to arbiter index inference, both of which show up as a
# spurious duplicate key error against one of the transient indexes a
# rebuild leaves around.
#
# bc32a12e0db, "Fix infer_arbiter_index during concurrent index
# operations".  Without it -- with inference considering only indisvalid
# indexes -- this fails four runs in eight at stress_concurrently=4 with
#
#   ERROR:  duplicate key value violates unique constraint
#   "pgbench_accounts_pkey_ccnew"
#
# and nothing at the default duration: two transactions whose catalog
# snapshots straddle the rebuild infer different arbiters, so both get
# past the conflict check and the second one collides in an index it did
# not consult.
#
# 2bc7e886fc1, "Fix ON CONFLICT ON CONSTRAINT during REINDEX
# CONCURRENTLY".  Without it, the same failure four runs in eight, but
# only at stress_concurrently=8: the disagreement lasts as long as the
# swap of the constraint's conindid rather than as long as the build.
#
# Both need upsert_contend rather than upsert_merge.  upsert_merge only
# ever meets rows that already exist, so it never reaches speculative
# insertion, which is where an arbiter set two transactions disagree
# about does its damage.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'unique_and_gapless',
	{
		load => [ 'tpcb_like', 'upsert_merge', 'upsert_contend',
			'serial_insert' ],
		# The arbiter every upsert here resolves to is
		# pgbench_accounts_pkey, so what these races are races against is
		# that index being swapped.  Nothing in the standard rotation
		# swaps a primary key at any rate: only reindex_table_concurrently
		# reaches one, and then as one index among all of the table's.
		ddl => [ @STANDARD_DDL, 'reindex_pkey_concurrently' ],
		clients => 30,
		tags => ['ci'],
	});
