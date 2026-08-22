# Copyright (c) 2022-2026, PostgreSQL Global Development Group

if not get_option('injection_points')
  subdir_done()
endif

test_slot_snapshot_sources = files(
  'test_slot_snapshot.c',
)

if host_system == 'windows'
  test_slot_snapshot_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_slot_snapshot',
    '--FILEDESC', 'test_slot_snapshot - use a logical slot\'s initial snapshot from SQL',])
endif

test_slot_snapshot = shared_module('test_slot_snapshot',
  test_slot_snapshot_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_slot_snapshot

tests += {
  'name': 'test_slot_snapshot',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'isolation': {
    'specs': [
      'snapshot_clog_race',
    ],
    'regress_args': [
      '--temp-config', files('extra.conf'),
    ],
  },
  'tap': {
    'env': {
      'enable_injection_points': get_option('injection_points') ? 'yes' : 'no',
    },
    'tests': [
      't/001_snapshot_clog_race.pl',
    ],
  },
}
