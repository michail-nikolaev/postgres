/* src/test/modules/injection_points/injection_points--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION injection_points" to load this file. \quit

--
-- injection_points_attach()
--
-- Attaches the action to the given injection point.
--
CREATE FUNCTION injection_points_attach(IN point_name TEXT,
    IN action text)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_attach'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_attach()
--
-- Attaches a function to the given injection point, with library name,
-- function name and private data.
--
CREATE FUNCTION injection_points_attach(IN point_name TEXT,
    IN library_name TEXT, IN function_name TEXT, IN private_data BYTEA)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_attach_func'
LANGUAGE C PARALLEL UNSAFE;

--
-- injection_points_load()
--
-- Load an injection point already attached.
--
CREATE FUNCTION injection_points_load(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_load'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_run()
--
-- Executes the action attached to the injection point.
--
CREATE FUNCTION injection_points_run(IN point_name TEXT,
    IN arg TEXT DEFAULT NULL)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_run'
LANGUAGE C PARALLEL UNSAFE;

--
-- injection_points_cached()
--
-- Executes the action attached to the injection point, from local cache.
--
CREATE FUNCTION injection_points_cached(IN point_name TEXT,
    IN arg TEXT DEFAULT NULL)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_cached'
LANGUAGE C PARALLEL UNSAFE;

--
-- injection_points_wakeup()
--
-- Wakes up a waiting injection point.
--
CREATE FUNCTION injection_points_wakeup(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_wakeup'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_set_local()
--
-- Trigger switch to link any future injection points attached to the
-- current process, useful to make SQL tests concurrently-safe.
--
CREATE FUNCTION injection_points_set_local()
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_set_local'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_detach()
--
-- Detaches the current action, if any, from the given injection point.
--
CREATE FUNCTION injection_points_detach(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_detach'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_list()
--
-- List of all the injection points currently attached.
--
CREATE FUNCTION injection_points_list(OUT point_name text,
   OUT library text,
   OUT function text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'injection_points_list'
LANGUAGE C STRICT VOLATILE PARALLEL RESTRICTED;

--
-- regress_injection.c functions
--
CREATE FUNCTION removable_cutoff(rel regclass)
RETURNS xid8
AS 'MODULE_PATHNAME'
LANGUAGE C CALLED ON NULL INPUT;

--
-- injection_points_attach_jitter()
--
-- Attach a callback that sleeps for a random time between min_us and max_us
-- with the given probability, leaving what the server does alone.  Meant to
-- stay attached while an ordinary workload runs, so that windows too narrow
-- to hit by repetition become wide enough to lose.
--
CREATE FUNCTION injection_points_attach_jitter(IN point_name TEXT,
    IN probability float8,
    IN min_us int4,
    IN max_us int4,
    IN seed int8)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_attach_jitter'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_stats_jitter()
--
-- Report how many jitter sleeps have happened and how long they lasted.
--
CREATE FUNCTION injection_points_stats_jitter(OUT sleep_count int8,
    OUT sleep_us int8)
RETURNS record
AS 'MODULE_PATHNAME', 'injection_points_stats_jitter'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_stats_jitter_by_point()
--
-- The same numbers, per point: which points slept, how often and for how
-- long.  A point that never slept widened nothing, however busy the rest
-- of the profile was.
--
CREATE FUNCTION injection_points_stats_jitter_by_point(OUT point_name text,
    OUT sleep_count int8,
    OUT sleep_us int8)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'injection_points_stats_jitter_by_point'
LANGUAGE C STRICT VOLATILE PARALLEL RESTRICTED;

--
-- injection_points_stats_reset_jitter()
--
CREATE FUNCTION injection_points_stats_reset_jitter()
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_stats_reset_jitter'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_defined()
--
-- Every injection point call site the backend sources of this build
-- define, from a table generated at compile time.  Attaching validates
-- nothing, so this list is the only way to tell a stale name from a live
-- one.  "kind" is the macro at the call site: run, cached, load, or
-- attached -- the last one marking a point whose mere attachment changes
-- what the server decides.
--
CREATE FUNCTION injection_points_defined(OUT name text,
    OUT file text,
    OUT line int4,
    OUT kind text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'injection_points_defined'
LANGUAGE C STRICT VOLATILE PARALLEL RESTRICTED;
