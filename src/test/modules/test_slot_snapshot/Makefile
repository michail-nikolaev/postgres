# src/test/modules/test_slot_snapshot/Makefile

PGFILEDESC = "test_slot_snapshot - use a logical slot's initial snapshot from SQL"

MODULE_big = test_slot_snapshot
OBJS = \
	$(WIN32RES) \
	test_slot_snapshot.o

EXTRA_INSTALL = src/test/modules/injection_points

ISOLATION = snapshot_clog_race

TAP_TESTS = 1

# The test needs wal_level = logical, and injection points are cluster-wide.
ISOLATION_OPTS = --temp-config $(top_srcdir)/src/test/modules/test_slot_snapshot/extra.conf

NO_INSTALLCHECK = 1

export enable_injection_points

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_slot_snapshot
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

# XXX: This test is conditional on enable_injection_points in the
# parent Makefile, so we should never get here in the first place if
# injection points are not enabled.  But the buildfarm 'misc-check'
# step doesn't pay attention to the if-condition in the parent
# Makefile.  To work around that, disable running the test here too.
ifeq ($(enable_injection_points),yes)
include $(top_srcdir)/contrib/contrib-global.mk
else
check:
	@echo "injection points are disabled in this build"
endif

endif
