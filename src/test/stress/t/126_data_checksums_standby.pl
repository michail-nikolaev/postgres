
# Copyright (c) 2026, PostgreSQL Global Development Group

# The data checksum scenario (Stress::Feature::DataChecksums) with a
# standby replaying it.  This is the case the checksum worker's own
# comment warns about: a replica can hold a page whose checksum is
# invalid, from unlogged changes made on the primary while checksums
# were off, and only a full page image repairs it.  Getting there takes
# checksums on, then off, then on again -- which is exactly what the
# rotation does -- and the check reads every standby page back too.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_template('data_checksums_standby', 'data_checksums',
	topology => 'standby');
