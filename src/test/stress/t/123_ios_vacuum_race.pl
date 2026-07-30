
# Copyright (c) 2026, PostgreSQL Global Development Group

# An index-only scan returning a row that vacuum has already removed.
#
# GiST and SP-GiST index scans do not interlock with their own index
# vacuum the way btree does.  A scan reads a page, queues the TIDs on it
# and lets the page go; vacuum is then free to remove those index
# entries, remove the heap tuples they pointed at, and mark the pages
# they were on all-visible.  When the scan gets round to returning a
# queued TID, the index-only path sees an all-visible page and returns
# the value straight from the index without ever looking at the heap --
# so a row deleted before the scan's snapshot even began comes back.
#
# The shape is Matthias van de Meent's, from the report: wide rows at
# fillfactor 10, so a handful of rows fill a page and the pages emptied
# by a delete can go all-visible on their own.  What his isolation test
# arranges by hand -- fetch one row, vacuum, fetch the rest -- this does
# by rate: rows die and come back continuously, a vacuum runs against
# those two tables as often as the rotation allows, and the scan holds
# itself open for 20ms between its first fetch and the rest.
#
# The invariant is inside the workload rather than in a check: the scan
# and a count of the same table run under one repeatable-read snapshot,
# so they can only disagree if the scan returned something that is not
# there.
#
# The fix is carried on this branch -- the v02 series from the thread,
# plus a correction to the SP-GiST half of it, which checked the
# visibility of one TID and attributed the answer to another.  Reverting
# either brings the failure back; see REGRESSIONS.
#
# https://www.postgresql.org/message-id/flat/CAEze2WgH13m=MDST58KLo-NkZpbwBEt4xNWcgtghWBwRj3J0+A@mail.gmail.com
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'ios_vacuum_race',
	{
		schema => [ 'pgbench', 'ios_vacuum_race' ],
		pgbench_scale => 1,
		load => [ 'tpcb_like', 'ios_cursor_check', 'ios_churn' ],
		ddl => [ 'vacuum_ios_tables', 'reindex_index_concurrently' ],
		ddl_concurrency => 2,
		checks => ['balances'],
		env => 'standalone',
		clients => 20,
	});
