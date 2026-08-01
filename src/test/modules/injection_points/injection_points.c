/*--------------------------------------------------------------------------
 *
 * injection_points.c
 *		Code for testing injection points.
 *
 * Injection points are able to trigger user-defined callbacks in pre-defined
 * code paths.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/injection_points/injection_points.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "fmgr.h"
#include "funcapi.h"
#include "injection_points.h"
#include "miscadmin.h"
#include "common/pg_prng.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;

/* Maximum number of waits usable in injection points at once */
#define INJ_MAX_WAIT	8
#define INJ_NAME_MAXLEN	64

/*
 * Maximum number of points whose jitter is counted separately.  Sized to
 * MAX_INJECTION_POINTS: more points than that cannot be attached at once
 * anyway.
 */
#define INJ_MAX_JITTER	128

/* The call sites the backend sources define, generated at build time. */
#include "injection_point_defs.h"

/* Thresholds of waits */
#define INJ_WAIT_INITIAL_US		10	/* 10us */
#define INJ_WAIT_MAX_US			100000	/* 100ms */

/*
 * List of injection points stored in TopMemoryContext attached
 * locally to this process.
 */
static List *inj_list_local = NIL;

/*
 * Per-point jitter counters.  A slot is claimed for a name the first time
 * that point sleeps and keeps it from then on: slots are never re-dealt
 * while the server runs, so a backend may cache the pointer it resolved.
 */
typedef struct InjectionJitterStat
{
	/* Point name; claimed under the lock, then immutable */
	char		name[INJ_NAME_MAXLEN];

	pg_atomic_uint64 count;
	pg_atomic_uint64 sleep_us;
} InjectionJitterStat;

/*
 * Shared state information for injection points.
 *
 * This state data can be initialized in two ways: dynamically with a DSM
 * or when loading the module.
 */
typedef struct InjectionPointSharedState
{
	/* Protects access to other fields */
	slock_t		lock;

	/* Counters advancing when injection_points_wakeup() is called */
	pg_atomic_uint32 wait_counts[INJ_MAX_WAIT];

	/* Names of injection points attached to wait counters */
	char		name[INJ_MAX_WAIT][INJ_NAME_MAXLEN];

	/* Number of jitter sleeps performed, and their total duration */
	pg_atomic_uint64 jitter_count;
	pg_atomic_uint64 jitter_sleep_us;

	/* The same, counted per point */
	InjectionJitterStat jitter_stats[INJ_MAX_JITTER];
} InjectionPointSharedState;

/* Pointer to shared-memory state. */
static InjectionPointSharedState *inj_state = NULL;

extern PGDLLEXPORT void injection_error(const char *name,
										const void *private_data,
										void *arg);
extern PGDLLEXPORT void injection_notice(const char *name,
										 const void *private_data,
										 void *arg);
extern PGDLLEXPORT void injection_wait(const char *name,
									   const void *private_data,
									   void *arg);
extern PGDLLEXPORT void injection_jitter(const char *name,
										 const void *private_data,
										 void *arg);

/* track if injection points attached in this process are linked to it */
static bool injection_point_local = false;

static void injection_shmem_request(void *arg);
static void injection_shmem_init(void *arg);

static const ShmemCallbacks injection_shmem_callbacks = {
	.request_fn = injection_shmem_request,
	.init_fn = injection_shmem_init,
};

/*
 * Routine for shared memory area initialization, used as a callback
 * when initializing dynamically with a DSM or when loading the module.
 */
static void
injection_point_init_state(void *ptr, void *arg)
{
	InjectionPointSharedState *state = (InjectionPointSharedState *) ptr;

	SpinLockInit(&state->lock);
	memset(state->name, 0, sizeof(state->name));
	for (int i = 0; i < INJ_MAX_WAIT; i++)
		pg_atomic_init_u32(&state->wait_counts[i], 0);
	pg_atomic_init_u64(&state->jitter_count, 0);
	pg_atomic_init_u64(&state->jitter_sleep_us, 0);
	for (int i = 0; i < INJ_MAX_JITTER; i++)
	{
		state->jitter_stats[i].name[0] = '\0';
		pg_atomic_init_u64(&state->jitter_stats[i].count, 0);
		pg_atomic_init_u64(&state->jitter_stats[i].sleep_us, 0);
	}
}

static void
injection_shmem_request(void *arg)
{
	ShmemRequestStruct(.name = "injection_points",
					   .size = sizeof(InjectionPointSharedState),
					   .ptr = (void **) &inj_state,
		);
}

static void
injection_shmem_init(void *arg)
{
	/*
	 * First time through, so initialize.  This is shared with the dynamic
	 * initialization using a DSM.
	 */
	injection_point_init_state(inj_state, NULL);
}

/*
 * Initialize shared memory area for this module through DSM.
 */
static void
injection_init_shmem(void)
{
	bool		found;

	if (inj_state != NULL)
		return;

	inj_state = GetNamedDSMSegment("injection_points",
								   sizeof(InjectionPointSharedState),
								   injection_point_init_state,
								   &found, NULL);
}

/*
 * Check runtime conditions associated to an injection point.
 *
 * Returns true if the named injection point is allowed to run, and false
 * otherwise.
 */
static bool
injection_point_allowed(const InjectionPointCondition *condition)
{
	bool		result = true;

	switch (condition->type)
	{
		case INJ_CONDITION_PID:
			if (MyProcPid != condition->pid)
				result = false;
			break;
		case INJ_CONDITION_ALWAYS:
			break;
	}

	return result;
}

/*
 * before_shmem_exit callback to remove injection points linked to a
 * specific process.
 */
static void
injection_points_cleanup(int code, Datum arg)
{
	ListCell   *lc;

	/* Leave if nothing is tracked locally */
	if (!injection_point_local)
		return;

	/* Detach all the local points */
	foreach(lc, inj_list_local)
	{
		char	   *name = strVal(lfirst(lc));

		(void) InjectionPointDetach(name);
	}
}

/* Set of callbacks available to be attached to an injection point. */
void
injection_error(const char *name, const void *private_data, void *arg)
{
	const InjectionPointCondition *condition = private_data;
	char	   *argstr = arg;

	if (!injection_point_allowed(condition))
		return;

	if (argstr)
		elog(ERROR, "error triggered for injection point %s (%s)",
			 name, argstr);
	else
		elog(ERROR, "error triggered for injection point %s", name);
}

void
injection_notice(const char *name, const void *private_data, void *arg)
{
	const InjectionPointCondition *condition = private_data;
	char	   *argstr = arg;

	if (!injection_point_allowed(condition))
		return;

	if (argstr)
		elog(NOTICE, "notice triggered for injection point %s (%s)",
			 name, argstr);
	else
		elog(NOTICE, "notice triggered for injection point %s", name);
}

/*
 * Map a point name to its per-point counter slot, claiming one on first
 * use.
 *
 * This runs on every sleep and may run inside a critical section, where
 * nothing can allocate, so the resolved slot is cached backend-locally in a
 * small fixed array and the spinlock is taken once per (backend, point)
 * pair.  Caching the pointer is safe because a claimed slot never changes
 * its name: even the stats reset only zeroes the counters.
 */
#define INJ_JITTER_LOCAL_CACHE	16

static InjectionJitterStat *
injection_jitter_stat(const char *name)
{
	static struct
	{
		char		name[INJ_NAME_MAXLEN];
		InjectionJitterStat *stat;
	}			cache[INJ_JITTER_LOCAL_CACHE];
	static int	cache_used = 0;
	InjectionJitterStat *found = NULL;

	for (int i = 0; i < cache_used; i++)
	{
		if (strcmp(cache[i].name, name) == 0)
			return cache[i].stat;
	}

	SpinLockAcquire(&inj_state->lock);
	for (int i = 0; i < INJ_MAX_JITTER; i++)
	{
		InjectionJitterStat *stat = &inj_state->jitter_stats[i];

		if (stat->name[0] == '\0')
		{
			strlcpy(stat->name, name, INJ_NAME_MAXLEN);
			found = stat;
			break;
		}
		if (strcmp(stat->name, name) == 0)
		{
			found = stat;
			break;
		}
	}
	SpinLockRelease(&inj_state->lock);

	/* Table full: this point is counted in the aggregate alone. */
	if (found == NULL)
		return NULL;

	if (cache_used < INJ_JITTER_LOCAL_CACHE)
	{
		strlcpy(cache[cache_used].name, name, INJ_NAME_MAXLEN);
		cache[cache_used].stat = found;
		cache_used++;
	}
	return found;
}

/*
 * Sleep for a random time, with a given probability.
 *
 * This is the callback that makes a race reachable: most of the windows this
 * suite hunts are microseconds wide, which no amount of running will hit, and
 * widening one to milliseconds turns "once in a few hours" into "every run".
 * It only ever sleeps -- what the server decides is left alone -- so a
 * failure seen with this attached is a failure that exists without it.
 */
void
injection_jitter(const char *name, const void *private_data, void *arg)
{
	const InjectionPointJitter *jitter = private_data;
	static pg_prng_state prng;
	static bool prng_seeded = false;
	int			us;

	if (!injection_point_allowed(&jitter->condition))
		return;

	/*
	 * Seeded from the configured seed and this process, so that a run can be
	 * replayed and two backends still make different choices.
	 */
	if (!prng_seeded)
	{
		pg_prng_seed(&prng, jitter->seed ^ (uint64) MyProcPid);
		prng_seeded = true;
	}

	if (pg_prng_double(&prng) >= jitter->probability)
		return;

	us = jitter->min_us;
	if (jitter->max_us > jitter->min_us)
		us += (int) pg_prng_uint64_range(&prng, 0,
										 jitter->max_us - jitter->min_us);

	/*
	 * Attaching the shared area takes a lock and allocates, so it cannot be
	 * done from a critical section; a backend whose first jitter point is
	 * inside one sleeps uncounted rather than not at all.  Preloading the
	 * module avoids that, which is what a caller who cares about the counts
	 * does.
	 */
	if (inj_state == NULL && CritSectionCount == 0)
		injection_init_shmem();

	if (inj_state != NULL)
	{
		InjectionJitterStat *stat = injection_jitter_stat(name);

		pg_atomic_fetch_add_u64(&inj_state->jitter_count, 1);
		pg_atomic_fetch_add_u64(&inj_state->jitter_sleep_us, (uint64) us);
		if (stat != NULL)
		{
			pg_atomic_fetch_add_u64(&stat->count, 1);
			pg_atomic_fetch_add_u64(&stat->sleep_us, (uint64) us);
		}
	}

	pg_usleep(us);
}

/*
 * Error cleanup callback for injection point waits.
 */
static void
injection_wait_cleanup(int code, Datum arg)
{
	int			index = DatumGetInt32(arg);

	SpinLockAcquire(&inj_state->lock);
	inj_state->name[index][0] = '\0';
	SpinLockRelease(&inj_state->lock);
}

/* Wait until injection_points_wakeup() is called */
void
injection_wait(const char *name, const void *private_data, void *arg)
{
	uint32		old_wait_counts = 0;
	int			index = -1;
	uint32		injection_wait_event = 0;
	const InjectionPointCondition *condition = private_data;
	int			delay_us = 0;

	if (inj_state == NULL)
		injection_init_shmem();

	if (!injection_point_allowed(condition))
		return;

	/*
	 * Use the injection point name for this custom wait event.  Note that
	 * this custom wait event name is not released, but we don't care much for
	 * testing as this should be short-lived.
	 */
	injection_wait_event = WaitEventInjectionPointNew(name);

	/*
	 * Find a free slot to wait for, and register this injection point's name.
	 */
	SpinLockAcquire(&inj_state->lock);
	for (int i = 0; i < INJ_MAX_WAIT; i++)
	{
		if (inj_state->name[i][0] == '\0')
		{
			index = i;
			strlcpy(inj_state->name[i], name, INJ_NAME_MAXLEN);
			old_wait_counts = pg_atomic_read_u32(&inj_state->wait_counts[i]);
			break;
		}
	}
	SpinLockRelease(&inj_state->lock);

	if (index < 0)
		elog(ERROR, "could not find free slot for wait of injection point %s",
			 name);

	/*
	 * Wait until the counter is bumped by injection_points_wakeup().
	 *
	 * This loop starts with a short delay for responsiveness, enlarged to
	 * ease the CPU workload in slower environments.
	 */
	delay_us = INJ_WAIT_INITIAL_US;

	pgstat_report_wait_start(injection_wait_event);
	PG_ENSURE_ERROR_CLEANUP(injection_wait_cleanup, Int32GetDatum(index));
	{
		while (pg_atomic_read_u32(&inj_state->wait_counts[index]) == old_wait_counts)
		{
			CHECK_FOR_INTERRUPTS();
			pg_usleep(delay_us);
			if (delay_us < INJ_WAIT_MAX_US)
				delay_us = Min(delay_us * 2, INJ_WAIT_MAX_US);
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(injection_wait_cleanup, Int32GetDatum(index));
	pgstat_report_wait_end();

	/* Remove this injection point from the waiters. */
	injection_wait_cleanup(0, Int32GetDatum(index));
}

/*
 * SQL function for creating an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_attach);
Datum
injection_points_attach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *action = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *function;
	InjectionPointCondition condition = {0};

	if (strcmp(action, "error") == 0)
		function = "injection_error";
	else if (strcmp(action, "notice") == 0)
		function = "injection_notice";
	else if (strcmp(action, "wait") == 0)
		function = "injection_wait";
	else
		elog(ERROR, "incorrect action \"%s\" for injection point creation", action);

	if (injection_point_local)
	{
		condition.type = INJ_CONDITION_PID;
		condition.pid = MyProcPid;
	}

	InjectionPointAttach(name, "injection_points", function, &condition,
						 sizeof(InjectionPointCondition));

	if (injection_point_local)
	{
		MemoryContext oldctx;

		/* Local injection point, so track it for automated cleanup */
		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		inj_list_local = lappend(inj_list_local, makeString(pstrdup(name)));
		MemoryContextSwitchTo(oldctx);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function for creating an injection point with library name, function
 * name and private data.
 */
PG_FUNCTION_INFO_V1(injection_points_attach_func);
Datum
injection_points_attach_func(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *lib_name;
	char	   *function;
	bytea	   *private_data = NULL;
	int			private_data_size = 0;

	if (PG_ARGISNULL(0))
		elog(ERROR, "injection point name cannot be NULL");
	if (PG_ARGISNULL(1))
		elog(ERROR, "injection point library cannot be NULL");
	if (PG_ARGISNULL(2))
		elog(ERROR, "injection point function cannot be NULL");

	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	lib_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
	function = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (!PG_ARGISNULL(3))
	{
		private_data = PG_GETARG_BYTEA_PP(3);
		private_data_size = VARSIZE_ANY_EXHDR(private_data);
	}

	if (private_data != NULL)
		InjectionPointAttach(name, lib_name, function, VARDATA_ANY(private_data),
							 private_data_size);
	else
		InjectionPointAttach(name, lib_name, function, NULL,
							 0);
	PG_RETURN_VOID();
}

/*
 * SQL function for attaching the jitter callback to an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_attach_jitter);
Datum
injection_points_attach_jitter(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	InjectionPointJitter jitter = {0};

	jitter.probability = PG_GETARG_FLOAT8(1);
	jitter.min_us = PG_GETARG_INT32(2);
	jitter.max_us = PG_GETARG_INT32(3);
	jitter.seed = (uint64) PG_GETARG_INT64(4);

	if (jitter.probability < 0.0 || jitter.probability > 1.0)
		elog(ERROR, "probability must be between 0 and 1");
	if (jitter.min_us < 0 || jitter.max_us < jitter.min_us)
		elog(ERROR, "sleep bounds must be non-negative, and max not below min");

	if (injection_point_local)
	{
		jitter.condition.type = INJ_CONDITION_PID;
		jitter.condition.pid = MyProcPid;
	}

	InjectionPointAttach(name, "injection_points", "injection_jitter",
						 &jitter, sizeof(InjectionPointJitter));

	if (injection_point_local)
	{
		MemoryContext oldctx;

		/* Local injection point, so track it for automated cleanup */
		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		inj_list_local = lappend(inj_list_local, makeString(pstrdup(name)));
		MemoryContextSwitchTo(oldctx);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function reporting what the jitter callbacks have done so far.
 *
 * A run where nothing slept is a run that tested nothing extra, and it would
 * otherwise be indistinguishable from one where the jitter did its work, so
 * the numbers are meant to be reported rather than assumed.
 */
PG_FUNCTION_INFO_V1(injection_points_stats_jitter);
Datum
injection_points_stats_jitter(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {0};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	injection_init_shmem();

	values[0] = Int64GetDatum((int64) pg_atomic_read_u64(&inj_state->jitter_count));
	values[1] = Int64GetDatum((int64) pg_atomic_read_u64(&inj_state->jitter_sleep_us));

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * SQL function reporting the same, per point.
 *
 * The aggregate above says whether the run slept at all; this says which
 * points did, which is what turns "the profile fired" into "every point of
 * the profile fired" -- a point that never slept widened nothing, however
 * busy its neighbours were.
 */
PG_FUNCTION_INFO_V1(injection_points_stats_jitter_by_point);
Datum
injection_points_stats_jitter_by_point(PG_FUNCTION_ARGS)
{
#define NUM_INJECTION_POINTS_JITTER 3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	InjectionJitterStat local[INJ_MAX_JITTER];
	int			nstats = 0;

	InitMaterializedSRF(fcinfo, 0);

	injection_init_shmem();

	/*
	 * Copied out under the lock, so a slot being claimed concurrently is
	 * either seen whole or not yet; tuples are built outside it, since
	 * nothing may allocate while a spinlock is held.
	 */
	SpinLockAcquire(&inj_state->lock);
	for (int i = 0; i < INJ_MAX_JITTER; i++)
	{
		InjectionJitterStat *stat = &inj_state->jitter_stats[i];

		if (stat->name[0] == '\0')
			break;
		memcpy(local[nstats].name, stat->name, INJ_NAME_MAXLEN);
		pg_atomic_init_u64(&local[nstats].count,
						   pg_atomic_read_u64(&stat->count));
		pg_atomic_init_u64(&local[nstats].sleep_us,
						   pg_atomic_read_u64(&stat->sleep_us));
		nstats++;
	}
	SpinLockRelease(&inj_state->lock);

	for (int i = 0; i < nstats; i++)
	{
		Datum		values[NUM_INJECTION_POINTS_JITTER];
		bool		nulls[NUM_INJECTION_POINTS_JITTER] = {0};

		values[0] = PointerGetDatum(cstring_to_text(local[i].name));
		values[1] = Int64GetDatum((int64) pg_atomic_read_u64(&local[i].count));
		values[2] = Int64GetDatum((int64) pg_atomic_read_u64(&local[i].sleep_us));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
#undef NUM_INJECTION_POINTS_JITTER
}

/*
 * SQL function to reset the jitter counters.
 *
 * The per-point slots keep their names -- a backend that resolved a slot
 * goes on using it -- and only the numbers return to zero.
 */
PG_FUNCTION_INFO_V1(injection_points_stats_reset_jitter);
Datum
injection_points_stats_reset_jitter(PG_FUNCTION_ARGS)
{
	injection_init_shmem();

	pg_atomic_write_u64(&inj_state->jitter_count, 0);
	pg_atomic_write_u64(&inj_state->jitter_sleep_us, 0);
	for (int i = 0; i < INJ_MAX_JITTER; i++)
	{
		pg_atomic_write_u64(&inj_state->jitter_stats[i].count, 0);
		pg_atomic_write_u64(&inj_state->jitter_stats[i].sleep_us, 0);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function for loading an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_load);
Datum
injection_points_load(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (inj_state == NULL)
		injection_init_shmem();

	INJECTION_POINT_LOAD(name);

	PG_RETURN_VOID();
}

/*
 * SQL function for triggering an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_run);
Datum
injection_points_run(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *arg = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!PG_ARGISNULL(1))
		arg = text_to_cstring(PG_GETARG_TEXT_PP(1));

	INJECTION_POINT(name, arg);

	PG_RETURN_VOID();
}

/*
 * SQL function for triggering an injection point from cache.
 */
PG_FUNCTION_INFO_V1(injection_points_cached);
Datum
injection_points_cached(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *arg = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!PG_ARGISNULL(1))
		arg = text_to_cstring(PG_GETARG_TEXT_PP(1));

	INJECTION_POINT_CACHED(name, arg);

	PG_RETURN_VOID();
}

/*
 * SQL function for waking up an injection point waiting in injection_wait().
 */
PG_FUNCTION_INFO_V1(injection_points_wakeup);
Datum
injection_points_wakeup(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int			index = -1;

	if (inj_state == NULL)
		injection_init_shmem();

	/* Find the injection point then bump its wait counter */
	SpinLockAcquire(&inj_state->lock);
	for (int i = 0; i < INJ_MAX_WAIT; i++)
	{
		if (strcmp(name, inj_state->name[i]) == 0)
		{
			index = i;
			break;
		}
	}
	if (index < 0)
	{
		SpinLockRelease(&inj_state->lock);
		elog(ERROR, "could not find injection point %s to wake up", name);
	}
	SpinLockRelease(&inj_state->lock);

	pg_atomic_fetch_add_u32(&inj_state->wait_counts[index], 1);
	PG_RETURN_VOID();
}

/*
 * injection_points_set_local
 *
 * Track if any injection point created in this process ought to run only
 * in this process.  Such injection points are detached automatically when
 * this process exits.  This is useful to make test suites concurrent-safe.
 */
PG_FUNCTION_INFO_V1(injection_points_set_local);
Datum
injection_points_set_local(PG_FUNCTION_ARGS)
{
	/* Enable flag to add a runtime condition based on this process ID */
	injection_point_local = true;

	if (inj_state == NULL)
		injection_init_shmem();

	/*
	 * Register a before_shmem_exit callback to remove any injection points
	 * linked to this process.
	 */
	before_shmem_exit(injection_points_cleanup, (Datum) 0);

	PG_RETURN_VOID();
}

/*
 * SQL function for dropping an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_detach);
Datum
injection_points_detach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!InjectionPointDetach(name))
		elog(ERROR, "could not detach injection point \"%s\"", name);

	/* Remove point from local list, if required */
	if (inj_list_local != NIL)
	{
		MemoryContext oldctx;

		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		inj_list_local = list_delete(inj_list_local, makeString(name));
		MemoryContextSwitchTo(oldctx);
	}

	PG_RETURN_VOID();
}

/*
 * SQL function for listing all the injection points attached.
 */
PG_FUNCTION_INFO_V1(injection_points_list);
Datum
injection_points_list(PG_FUNCTION_ARGS)
{
#define NUM_INJECTION_POINTS_LIST 3
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	List	   *inj_points;
	ListCell   *lc;

	/* Build a tuplestore to return our results in */
	InitMaterializedSRF(fcinfo, 0);

	inj_points = InjectionPointList();

	foreach(lc, inj_points)
	{
		Datum		values[NUM_INJECTION_POINTS_LIST];
		bool		nulls[NUM_INJECTION_POINTS_LIST];
		InjectionPointData *inj_point = lfirst(lc);

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(inj_point->name));
		values[1] = PointerGetDatum(cstring_to_text(inj_point->library));
		values[2] = PointerGetDatum(cstring_to_text(inj_point->function));

		/* shove row into tuplestore */
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
#undef NUM_INJECTION_POINTS_LIST
}

/*
 * SQL function for listing every injection point call site this build's
 * backend sources define, from the table generated at compile time.
 *
 * This is the other half of injection_points_list(): that one says what is
 * attached right now, this one says what could be.  Nothing else can answer
 * that -- InjectionPointAttach() accepts any name without complaint, so a
 * caller attaching to a point that no longer exists gets silence rather
 * than an error, and only a comparison against this list can tell a stale
 * name from a live one.
 */
PG_FUNCTION_INFO_V1(injection_points_defined);
Datum
injection_points_defined(PG_FUNCTION_ARGS)
{
#define NUM_INJECTION_POINTS_DEFINED 4
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	for (const InjectionPointDef *def = injection_point_defs;
		 def->name != NULL; def++)
	{
		Datum		values[NUM_INJECTION_POINTS_DEFINED];
		bool		nulls[NUM_INJECTION_POINTS_DEFINED] = {0};

		values[0] = PointerGetDatum(cstring_to_text(def->name));
		values[1] = PointerGetDatum(cstring_to_text(def->file));
		values[2] = Int32GetDatum(def->line);
		values[3] = PointerGetDatum(cstring_to_text(def->kind));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
#undef NUM_INJECTION_POINTS_DEFINED
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	RegisterShmemCallbacks(&injection_shmem_callbacks);
}
