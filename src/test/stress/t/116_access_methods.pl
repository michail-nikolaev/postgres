
# Copyright (c) 2026, PostgreSQL Global Development Group

# Every access method's index built, rebuilt and dropped concurrently
# while the columns they cover are rewritten.
#
# VACUUM belongs in the rotation here rather than only in the scenario
# named for it: some of what a rebuild leaves behind is only wrong once
# VACUUM reaches it, which is how an SP-GiST redirect written without a
# transaction id used to show up.
#
# Gates 92c49d1062f, "Fix insertion of SP-GiST REDIRECT tuples during
# REINDEX CONCURRENTLY".  Reverted, this fails four runs in eight at the
# default duration with
#
#   TRAP: failed Assert("TransactionIdIsValid(state->myXid)"),
#   File: spgutils.c
#
# taking the backend down: REINDEX CONCURRENTLY rebuilds an SP-GiST
# index from a transaction that has no XID of its own, and the old code
# insisted on one.  Assertion build only -- without assertions the same
# path writes a redirect carrying xid 0, which VACUUM later reads back.
# Gates a904abe2e28, "Fix concurrent indexing operations with temporary
# tables".  Without its four guards -- the ones that quietly force the
# non-concurrent path for a temporary relation -- this fails six runs in
# six at the default duration with
#
#   ERROR:  index "pgb_tmp_idx" already contains data
#
# from the temp_table_cic load: CREATE INDEX CONCURRENTLY uses several
# transactions internally, and the table's ON COMMIT DELETE ROWS empties
# it between them.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'access_methods',
	{
		load => [ 'tpcb_like', 'am_churn', 'temp_table_cic' ],
		# REPACK orders a table by an index, which only btree can do, so
		# the index-ordered variant is left out here.
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'reindex_index_concurrently', 'drop_create_index',
			'vacuum'
		],
		clients => 20,
		tags => ['ci'],
	});
