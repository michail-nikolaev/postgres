# Copyright (c) 2026, PostgreSQL Global Development Group

tests += {
  'name': 'stress',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/100_repack_dml_s1.pl',
      't/101_repack_dml_s50.pl',
      't/102_locks_and_subxacts.pl',
      't/103_unique_and_gapless.pl',
      't/104_rewrite_fidelity.pl',
      't/105_foreign_keys.pl',
      't/106_partitions.pl',
      't/107_vacuum_and_ios.pl',
      't/108_bulk_and_rewrite.pl',
      't/109_plancache_and_matview.pl',
      't/110_overlapping_ddl.pl',
      't/111_lock_exhaustion.pl',
      't/112_standby.pl',
      't/113_subscription.pl',
      't/114_crash_recovery.pl',
      't/115_cancellation.pl',
      't/116_access_methods.pl',
      't/900_serializable.pl',
      't/901_pg_dump.pl',
    ],
  },
}
