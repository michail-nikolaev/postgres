# Race between logical decoding's snapshot builder and CLOG.
#
# RecordTransactionCommit() flushes the commit WAL record and only then marks
# the transaction committed in CLOG.  Logical decoding derives transaction
# status from WAL, so in that window the snapshot builder can decide that a
# transaction is no longer running while CLOG still reports it as in progress.
# A snapshot built there claims the transaction committed, but the visibility
# checks that use the snapshot ask CLOG, get "not committed", and conclude the
# transaction aborted.
#
# The initial snapshot of a logical slot is the interesting consumer: it is a
# plain MVCC snapshot handed to the initial table copy of a subscription, so
# the damage is not confined to decoding.  Rows inserted by the transaction
# turn invisible and rows it deleted become visible again, and because
# HeapTupleSatisfiesMVCC() records what it concluded, HEAP_XMIN_INVALID and
# HEAP_XMAX_INVALID hint bits are written to the table.  Those hint bits
# outlive the CLOG update, so the publisher's table stays corrupted.
#
# The two permutations differ only in the order of the last two wakeups: the
# first one builds the snapshot inside the window, the second one lets CLOG
# catch up first and shows what the answers are supposed to be.
#
# NOTE: this spec demonstrates the bug, it does not check for it.  The expected
# output of the first permutation is what an unfixed server produces, wrong
# answers included, so the spec passes exactly as long as the race is there.
#
# It cannot be turned into a regression test, because a fix has to make the
# snapshot builder wait for the CLOG update, and the only session that can
# release the writer is the one running the next step -- which the isolation
# tester will not start while a step is running.  It has no way to tell that
# the importing session is waiting either: pg_isolation_test_session_is_blocked()
# only knows about heavyweight locks, safe snapshots and injection points.  The
# TAP test in t/ drives the same sessions by hand and does check the outcome.

setup
{
	CREATE EXTENSION injection_points;
	CREATE TABLE tbl (i int PRIMARY KEY, j int) WITH (autovacuum_enabled = off);
	INSERT INTO tbl VALUES (1, 10), (2, 20);
}

teardown
{
	DROP TABLE tbl;
	DROP EXTENSION injection_points;
}

# Holds an XID so that the snapshot builder has to stop in
# SNAPBUILD_BUILDING_SNAPSHOT, and checks the table at the very end.
session s1
step s1_xid			{ BEGIN; SELECT pg_current_xact_id() IS NOT NULL AS has_xid; }
step s1_rollback	{ ROLLBACK; }
step s1_check		{ SELECT i, j FROM tbl ORDER BY i; }

# Creates a logical slot and imports its initial snapshot, the way a
# subscription's initial table copy does.
session s2
setup
{
	LOAD 'test_slot_snapshot';
	SELECT injection_points_set_local();
	SELECT injection_points_attach('snapbuild-full-snapshot', 'wait');
}
step s2_import
{
	BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
	SET TRANSACTION SNAPSHOT 'logical-slot:race_slot';
}
step s2_scan		{ SELECT i, j FROM tbl ORDER BY i; }
step s2_rollback	{ ROLLBACK; }
teardown			{ SELECT injection_points_detach('snapbuild-full-snapshot'); }

# Holds an XID so that s1's rollback does not take the builder straight to
# SNAPBUILD_CONSISTENT.
session s3
step s3_xid			{ BEGIN; SELECT pg_current_xact_id() IS NOT NULL AS has_xid; }
step s3_rollback	{ ROLLBACK; }

# The transaction whose CLOG update is delayed.
session s4
setup
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('commit-before-clog-update', 'wait');
}
step s4_change
{
	INSERT INTO tbl VALUES (3, 30);
	UPDATE tbl SET j = j + 1 WHERE i = 1;
	DELETE FROM tbl WHERE i = 2;
}
teardown			{ SELECT injection_points_detach('commit-before-clog-update'); }

session s5
step s5_wake_snapbuild	{ SELECT injection_points_wakeup('snapbuild-full-snapshot'); }
step s5_wake_clog		{ SELECT injection_points_wakeup('commit-before-clog-update'); }

# The snapshot is built while s4 sits between its flushed commit record and its
# CLOG update.
#
# s2_scan runs under a snapshot that considers s4 committed, but every
# visibility check asks CLOG and is told otherwise: the INSERT, the UPDATE and
# the DELETE all appear to have never happened, and wrong hint bits are stored
# in the table.  s1_check then re-reads the table with an ordinary snapshot,
# after CLOG has been updated, and still sees the pre-s4 contents.
permutation
	s1_xid
	s2_import
	s3_xid
	s1_rollback
	s3_rollback
	s4_change
	s5_wake_snapbuild
	s2_scan
	s2_rollback
	s5_wake_clog
	s1_check

# Same thing, except that s4 gets to update CLOG before the snapshot is built.
# This is what both scans are supposed to return.
permutation
	s1_xid
	s2_import
	s3_xid
	s1_rollback
	s3_rollback
	s4_change
	s5_wake_clog
	s5_wake_snapbuild
	s2_scan
	s2_rollback
	s1_check
