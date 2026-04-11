# CREATE UNIQUE INDEX CONCURRENTLY with snapshot resets
#
# The first heap scan of such a build replaces its snapshot as it goes, so the
# spool can end up holding two entries for the same key that were never live at
# the same time: an old row deleted by a transaction that has not committed yet
# (still visible under the earlier snapshot) and a row inserted afterwards and
# committed (visible under a later one).  While the deleting transaction is
# still running, the build cannot tell a violation from a legitimate update, so
# it leaves the entry to the validation phase, which runs later and sees how
# that transaction ended.
#
# The injection point "heap_reset_scan_snapshot_swap" stops the scan just after
# it acquires a page and before it replaces its snapshot, and
# "bt_load_defer_unique_check" stops the build once it has decided to leave the
# undecided entry to the validation phase.  Between them the deleting
# transaction can be made to end at a defined moment.

setup
{
    CREATE EXTENSION injection_points;
    CREATE TABLE cic_uniq (k int, pad char(1004));
    ALTER TABLE cic_uniq ALTER COLUMN pad SET STORAGE PLAIN;
    ALTER TABLE cic_uniq SET (parallel_workers = 0);
    -- Wide rows give a platform independent layout of 7 rows per page.  Free
    -- one slot on a page the scan reaches late, so that the row inserted
    -- during the build lands on a page the scan has not read yet.
    INSERT INTO cic_uniq SELECT g, 'x' FROM generate_series(1, 35) g;
    DELETE FROM cic_uniq WHERE k = 25;
}

teardown
{
    DROP TABLE cic_uniq;
    DROP EXTENSION injection_points;
}

session s1
setup
{
    SELECT injection_points_set_local();
    SELECT injection_points_attach('heap_reset_scan_snapshot_swap', 'wait');
    SELECT injection_points_attach('bt_load_defer_unique_check', 'wait');
    SET concurrent_index_reset_snapshot_interval = 0;
}
step build { CREATE UNIQUE INDEX CONCURRENTLY cic_uniq_idx ON cic_uniq(k); }

session s2
step s2_begin    { BEGIN; }
step s2_delete   { DELETE FROM cic_uniq WHERE k = 1; }
step s2_commit   { COMMIT; }
step s2_rollback { ROLLBACK; }

session s3
# Populates the free space map, so that the row inserted during the build
# reuses the freed slot instead of extending the relation.
step s3_vacuum { VACUUM cic_uniq; }
step s3_wake   { SELECT injection_points_wakeup('heap_reset_scan_snapshot_swap'); }
step s3_insert { INSERT INTO cic_uniq VALUES (1, 'y'); }
# Let the scan run to its end; the build then stops at the deferral point.
step s3_scan_end {
    SELECT injection_points_detach('heap_reset_scan_snapshot_swap');
    SELECT injection_points_wakeup('heap_reset_scan_snapshot_swap');
}
# Wait until the build has parked at the deferral point, so that the deleting
# transaction ends at a defined moment relative to the uniqueness decision.
step s3_await_defer {
    DO $$
    DECLARE tries int := 0;
    BEGIN
        WHILE tries < 200 LOOP
            PERFORM 1 FROM pg_stat_activity
             WHERE wait_event = 'bt_load_defer_unique_check';
            EXIT WHEN FOUND;
            PERFORM pg_sleep(0.05);
            tries := tries + 1;
        END LOOP;
    END $$;
}
# Let the build finish and reach the validation phase.
step s3_build_end {
    SELECT injection_points_detach('bt_load_defer_unique_check');
    SELECT injection_points_wakeup('bt_load_defer_unique_check');
}

# The deleting transaction commits: the data is unique in the end, so the build
# has to succeed.
permutation s3_vacuum build s3_wake s2_begin s2_delete s3_insert s3_scan_end s3_await_defer s2_commit s3_build_end

# The deleting transaction rolls back: both rows are live, so the build has to
# report the duplicate.
permutation s3_vacuum build s3_wake s2_begin s2_delete s3_insert s3_scan_end s3_await_defer s2_rollback s3_build_end
