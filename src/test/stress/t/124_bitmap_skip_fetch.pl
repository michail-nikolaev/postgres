
# Copyright (c) 2026, PostgreSQL Global Development Group

# A bitmap heap scan counting TIDs a vacuum has already removed.
#
# The skip_fetch optimization let a bitmap heap scan that needs no
# columns from the table -- count(*) with an indexable qual and nothing
# else -- avoid reading a page when the visibility map said everything
# on it was visible, taking the tuple count from the bitmap instead.
# The bitmap was built earlier, and a vacuum running in between can have
# removed the dead TIDs it still refers to and marked those pages
# all-visible, so the scan counts rows that are not there.
#
# Removed in 459e7bf8e2f, "Remove HeapBitmapScan's skip_fetch
# optimization".  Reverting that commit puts the optimization, and the
# bug, back.
#
# The invariant lives in the workload because it needs one snapshot: the
# same count taken by a bitmap scan and by a sequential scan inside one
# repeatable-read transaction.  They can only disagree if the bitmap
# scan counted TIDs that no longer exist.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'bitmap_skip_fetch',
	{
		load => [ 'tpcb_like', 'bmskip_check', 'bmskip_churn' ],
		ddl => ['vacuum_bmskip'],
		clients => 20,
	});
