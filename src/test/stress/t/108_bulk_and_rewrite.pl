
# Copyright (c) 2026, PostgreSQL Global Development Group

# Bulk loads, two-phase commit and a rewriting ALTER TABLE, which hands
# the table back with every index replaced.
#
# This is the only scenario whose workload prepares transactions, so it
# is the only one that can see a concurrent build that failed to wait
# for one.  That failure is a missing index entry rather than a broken
# invariant, so the scenario has to declare an index for amcheck and
# index_vs_seq to have something to look at.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'bulk_and_rewrite',
	{
		indexes => ['btree_abalance'],
		load => [ 'tpcb_like', 'bulk_copy', 'twophase' ],
		ddl => [ @STANDARD_DDL, 'alter_table_rewrite' ],
		clients => 20,
		tags => ['ci'],
	});
