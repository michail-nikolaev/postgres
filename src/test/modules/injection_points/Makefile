# src/test/modules/injection_points/Makefile

MODULE_big = injection_points
OBJS = \
	$(WIN32RES) \
	injection_points.o \
	regress_injection.o
EXTENSION = injection_points
DATA = injection_points--1.0.sql
PGFILEDESC = "injection_points - facility for injection points"

# The generated catalogue lands in the build directory.
PG_CPPFLAGS = -I.
EXTRA_CLEAN = injection_point_defs.h

REGRESS = injection_points hashagg reindex_conc vacuum
REGRESS_OPTS = --dlpath=$(top_builddir)/src/test/regress

ISOLATION = basic \
	    inplace \
	    reindex_concurrently_deferred \
	    repack \
	    repack_temporal \
	    repack_temporal_multirange \
	    repack_toast \
	    syscache-update-pruned \
	    wait_cleanup \
	    heap_lock_update

# some isolation tests require wal_level=replica
ISOLATION_OPTS = --temp-config $(top_srcdir)/src/test/modules/injection_points/extra.conf

# The injection points are cluster-wide, so disable installcheck
NO_INSTALLCHECK = 1

export enable_injection_points

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# A PGXS build has no backend sources to scan; an empty catalogue keeps
# injection_points_defined() honest about what it can promise.
injection_points.o: injection_point_defs.h
injection_point_defs.h: generate-injection-points.pl
	$(PERL) $(srcdir)/generate-injection-points.pl --stub --outfile $@
else
subdir = src/test/modules/injection_points
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

# XXX: This test is conditional on enable_injection_points in the
# parent Makefile, so we should never get here in the first place if
# injection points are not enabled. But the buildfarm 'misc-check'
# step doesn't pay attention to the if-condition in the parent
# Makefile. To work around that, disable running the test here too.
ifeq ($(enable_injection_points),yes)
include $(top_srcdir)/contrib/contrib-global.mk
else
check:
	@echo "injection points are disabled in this build"
endif

# Rescanned on every build: make cannot depend on "every backend source"
# any more than meson can, the scan is sub-second, and the script rewrites
# its output only when the content changes, so nothing relinks for it.
.PHONY: injection-point-defs-scan
injection_points.o: injection_point_defs.h
injection_point_defs.h: injection-point-defs-scan
	$(PERL) $(srcdir)/generate-injection-points.pl \
		--srcdir $(top_srcdir) --outfile $@

endif
