# Planning a query while another session rebuilds the extended statistics
# object it is about to use.
#
# get_relation_statistics() decides which kinds of a statistics object are
# built by reading its pg_statistic_ext_data row, and records that in the
# RelOptInfo; the data itself is loaded from that same row much later,
# while the clauses are estimated.  ANALYZE replaces the row in between,
# holding only ShareUpdateExclusiveLock on the table, which does not
# conflict with the lock the planner holds on it.
#
# A row that is there but no longer has the kind the planner recorded used
# to end the query with "requested statistics kind is not yet built".  Only
# an MCV list can go that way: no value narrower than 1kB makes it into
# one, so once every value is wider than that, ANALYZE stores the row
# without an MCV list at all.

setup
{
	CREATE EXTENSION injection_points;

	CREATE TABLE w (x text, y text);
	INSERT INTO w SELECT (i / 10)::text, (i / 10)::text
	  FROM generate_series(1, 1000) i;

	CREATE STATISTICS s_mcv (mcv) ON x, y FROM w;
	ANALYZE w;
}
teardown
{
	DROP TABLE w;
	DROP EXTENSION injection_points;
}

# The point is local to this session and keyed on the table name, so no
# other backend, and no query over a catalog, ever waits at it.
session s1
step s1_park_at_end
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('get-relation-statistics-end', 'wait', 'w');
}
# The MCV pass reads pg_statistic before it loads the MCV list, which is
# where this backend processes the invalidation ANALYZE sent.
step s1_mcv { SELECT count(*) FROM w WHERE x = '4' AND y = '4'; }

session s2
# Every value becomes too wide for an MCV list, so ANALYZE stores the row
# without one.
step s2_widen
{
	UPDATE w SET x = repeat('a', 2000) || x, y = repeat('b', 2000) || y;
	ANALYZE w;
}
step s2_resume_end
{
	SELECT injection_points_detach('get-relation-statistics-end');
	SELECT injection_points_wakeup('get-relation-statistics-end');
}

permutation s1_park_at_end s1_mcv s2_widen s2_resume_end
