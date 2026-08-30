# Planning a query while another session drops the extended statistics
# objects it is about to use.
#
# get_relation_statistics() reads the list of statistics objects of a
# relation out of the relcache and looks up each definition; the data of
# those objects is loaded much later, while the clauses are estimated.
# Nothing keeps the list valid across either step.  DROP STATISTICS locks
# the statistics object itself and takes ShareUpdateExclusiveLock on the
# table, which does not conflict with the lock the planner holds on it,
# and no interlock makes it wait for a backend that has already read the
# list.  Losing the race at either step used to end the query with "cache
# lookup failed for statistics object".

setup
{
	CREATE EXTENSION injection_points;

	CREATE TABLE t (a int, b int, c int, d int, e int, f int, g int);
	INSERT INTO t
	  SELECT i / 10, i / 10, i / 10, i / 10, i / 10, i / 10, i / 10
		FROM generate_series(1, 1000) i;

	CREATE STATISTICS s_ndistinct (ndistinct) ON a, b FROM t;
	CREATE STATISTICS s_dependencies (dependencies) ON c, d FROM t;
	CREATE STATISTICS s_mcv (mcv) ON e, f FROM t;
	CREATE STATISTICS s_expressions ON (g + 1) FROM t;
	ANALYZE t;
}
teardown
{
	DROP TABLE t;
	DROP EXTENSION injection_points;
}

session s1
step s1_park_at_begin
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('get-relation-statistics-begin', 'wait', 't');
}
step s1_park_at_end
{
	SELECT injection_points_set_local();
	SELECT injection_points_attach('get-relation-statistics-end', 'wait', 't');
}
# One query per kind of statistics data.  The backend has to process the
# drop's invalidation before it loads the data, which it does at the
# first catalog lookup that misses its caches.
step s1_ndistinct { SELECT count(*) FROM (SELECT a, b FROM t GROUP BY a, b) s; }
# "e = 4 AND f = 4" is here to make statext_mcv_clauselist_selectivity()
# read pg_statistic before statext_dependencies_load() runs.
step s1_dependencies { SELECT count(*) FROM t WHERE e = 4 AND f = 4 AND c = 4 AND d = 4; }
step s1_mcv { SELECT count(*) FROM t WHERE e = 4 AND f = 4; }
# "a = 4" is here to make examine_variable() read pg_statistic before it
# reaches statext_expressions_load() for "g + 1".
step s1_expressions { SELECT count(*) FROM t WHERE a = 4 AND g + 1 = 5; }

session s2
step s2_drop_ndistinct { DROP STATISTICS s_ndistinct; }
step s2_drop_dependencies { DROP STATISTICS s_dependencies; }
step s2_drop_mcv { DROP STATISTICS s_mcv; }
step s2_drop_expressions { DROP STATISTICS s_expressions; }
step s2_resume_lookup
{
	SELECT injection_points_detach('get-relation-statistics-begin');
	SELECT injection_points_wakeup('get-relation-statistics-begin');
}
step s2_resume_end
{
	SELECT injection_points_detach('get-relation-statistics-end');
	SELECT injection_points_wakeup('get-relation-statistics-end');
}

# The definitions go away before the planner looks them up.
permutation s1_park_at_begin s1_mcv s2_drop_mcv s2_resume_lookup

# The definitions are in hand, but the data goes away before it is
# loaded.  One permutation per loader.
permutation s1_park_at_end s1_ndistinct s2_drop_ndistinct s2_resume_end
permutation s1_park_at_end s1_dependencies s2_drop_dependencies s2_resume_end
permutation s1_park_at_end s1_mcv s2_drop_mcv s2_resume_end
permutation s1_park_at_end s1_expressions s2_drop_expressions s2_resume_end
