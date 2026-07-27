
# Copyright (c) 2026, PostgreSQL Global Development Group

# Out-of-line values, stored generated columns and an exclusion
# constraint: three things REPACK has to reproduce exactly when it
# re-applies what it decoded.
#
# Gates two fixes, both in src/backend/commands/repack.c.
#
# 6ca631b9901, "REPACK CONCURRENTLY: fix processing of toasted tuples".
# Reverted, this fails 8 runs in 8 at stress_concurrently=4 with
#
#   stress assertion failed: N rows whose payload does not match its md5
#
# -- silent corruption rather than an error.  Reassembling a spilled
# attribute wrote a plain four-byte varlena header over what was a
# compressed one, so the datum came back as uncompressed garbage.  That
# is why toast_rewrite writes something large AND compressible: the
# value has to be compressed and still exceed the toast threshold after
# compressing, which a short payload or an EXTERNAL column never does.
#
# 3be823486f2, "Fix REPACK CONCURRENTLY for stored generated columns".
# Reverted, this fails 6 runs in 8 at the default duration with
#
#   ERROR:  no generation expression found for column number N of table
#   "pg_temp_NNNNN"
#
# -- the transient table lacking the pg_attrdef entries the replay of a
# concurrent insert or update needs.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'rewrite_fidelity',
	{
		schema => [ 'pgbench', 'toast', 'generated', 'exclusion_slot' ],
		load => [
			'tpcb_like', 'toast_rewrite',
			'generated_update', 'exclusion_churn'
		],
		# REINDEX will not rebuild an exclusion constraint's index
		# concurrently, so that one is left out of the rotation here; the
		# blocking rebuild covers it instead.
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'drop_create_index'
		],
		ddl_concurrency => 1,
		checks => [
			'toast_md5', 'generated_matches',
			'generated_defs_intact', 'distinct_slots',
			'amcheck'
		],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	});
