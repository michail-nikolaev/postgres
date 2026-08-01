/*-------------------------------------------------------------------------
 *
 * injection_points.h
 *		Definitions for the injection points module
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/modules/injection_points/injection_points.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INJECTION_POINTS_H
#define INJECTION_POINTS_H

typedef enum InjectionPointConditionType
{
	INJ_CONDITION_ALWAYS = 0,	/* always run */
	INJ_CONDITION_PID,			/* PID restriction */
} InjectionPointConditionType;

typedef struct InjectionPointCondition
{
	/* Type of the condition */
	InjectionPointConditionType type;

	/* ID of the process where the injection point is allowed to run */
	int			pid;
} InjectionPointCondition;

/*
 * A row of the generated catalogue of injection point call sites, built at
 * compile time by generate-injection-points.pl from the backend sources.
 * "kind" records which macro the site uses -- run, cached, load or attached
 * -- because that is what a consumer has to know: LOAD and CACHED mark a
 * point reached from inside a critical section, and an ATTACHED site changes
 * the server's behavior on mere attachment, so a callback that only means to
 * add delay must stay away from that name.
 */
typedef struct InjectionPointDef
{
	const char *name;
	const char *file;			/* relative to src/backend */
	int			line;
	const char *kind;
} InjectionPointDef;

/*
 * Private data of the "jitter" callback, which sleeps for a random time with
 * a given probability.  Unlike the other callbacks this one is meant to be
 * left attached while an ordinary workload runs: it widens the window a race
 * needs without changing what the server decides, so a test that fails with
 * it attached fails for a reason that exists without it.
 */
typedef struct InjectionPointJitter
{
	/* Must come first, so that injection_point_allowed() can be reused */
	InjectionPointCondition condition;

	/* Chance of sleeping when the point is reached, in [0, 1] */
	double		probability;

	/* Bounds of the sleep, in microseconds */
	int			min_us;
	int			max_us;

	/* Seed, so that a run can be replayed */
	uint64		seed;
} InjectionPointJitter;

#endif							/* INJECTION_POINTS_H */
