# hashbulkdelete() must not take the relcache's cached metapage before it has
# built its read stream.
#
# - "warm" makes a seq scan, whose own read stream caches the entry for the
#   table's tablespace.  Without it the vacuum reads pg_tablespace much
#   earlier, from lazy_scan_heap()'s read stream, and blocks in the wrong
#   place.
#
# - The index is in a tablespace of its own, so the lookup in hashbulkdelete()
#   still misses and has to open pg_tablespace.
#
# - s2 holds pg_tablespace locked, which stops the vacuum in LockRelationOid()
#   immediately before its AcceptInvalidationMessages() call.  s2 then queues
#   an invalidation for the index and commits, so the vacuum consumes it at
#   exactly the point where the metapage pointer is already held.
#
# ALTER INDEX ... SET (fillfactor) takes ShareUpdateExclusiveLock on the
# index, which does not conflict with the RowExclusiveLock VACUUM holds on it.
#
# Rows are deleted rather than updated, so that dead index entries really
# exist and ambulkdelete() is called at all.

# Each setup block is one simple query: CREATE TABLESPACE cannot run in a
# transaction block, and a multi-statement block is one.  The tablespace
# outlives the test for the same reason -- teardown may appear only once, and
# the table has to go first -- so it is dropped again on the way in.
setup
{
	DROP TABLESPACE IF EXISTS regress_hash_vac_ts;
}

setup
{
	SET allow_in_place_tablespaces = on;
}

setup
{
	CREATE TABLESPACE regress_hash_vac_ts LOCATION '';
}

setup
{
	CREATE TABLE hashvac (a int);
	INSERT INTO hashvac SELECT g FROM generate_series(1, 1000) g;
	CREATE INDEX hashvac_idx ON hashvac USING hash (a)
		TABLESPACE regress_hash_vac_ts;
	DELETE FROM hashvac WHERE a <= 100;
}

teardown
{
	DROP TABLE hashvac;
}

session s1
step warm	{ SELECT count(*) FROM hashvac; }
step vac	{ VACUUM hashvac; }

session s2
step lock	{ BEGIN; LOCK TABLE pg_tablespace IN ACCESS EXCLUSIVE MODE; }
step inval	{ ALTER INDEX hashvac_idx SET (fillfactor = 90); }
step commit	{ COMMIT; }

permutation
	warm
	lock
	vac
	inval
	commit
