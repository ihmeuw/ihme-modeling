Running LE Decomp in parallel and saving to the database
=========================================================

The :mod:`le_decomp.le_master` module defines a command line interface
and class for running life expectancy decomposition over
a location hierarchy and one to three sexes.  Parallelization
is handled by a specified number of processes, each of which will
make at least 2 database calls.  For this reason the number of
concurrent processes (specified by the n_processes argument, referenced below)
should be limited.

A full run includes making a process version for the outputs, saving .csv
files to an output_directory (see below), saving the results to the
ADDRESS database (or equivalent test database)
in the table gbd.output_le_decomp_v{process_version_id},
and marking the process_version as 'ACTIVE'.

Currently a full run takes about 30 or 40 minutes for a single time
interval/year pair and 8 concurrent processes (GBD round 6 full cause
and location hierarchies, 3 sexes). Runtime for running multiple
time intervals is probably not linear per interval, but that may be a good
starting point for a 'worst-case' estimate. Time to upload to the database once
file creation has completed is short, currently a few minutes for
3 time intervals. No issues have been encountered
running 3 time intervals on a 10GB qlogin, but full memory profiling hasn't
been performed.


Command line arguments
-----------------------

--version_id: a reference version for the le_decomp run.  Files are saved as
FILEPATH per location/sex
combination.

--output_dir: root directory for saving files.  See above.

--cause_level: Either 'most_detailed' or an integer specifying a
cause_level id. This determines which causes will be included in the output
by referencing the cause hierarchy. The default is 'most_detailed'.

--cause_set_id: Specifies the cause hierarchy to pull causes from.  Defaults
to 2, the computation hierarchy.

--location_set_id: Specifies the location hierarchy which will determine the
locations to be included in the output. Default is 35.

--start_years: a list of year ids comprising the start of each time interval
for decomp. Each start year in start_years has 1-to-1 correspondence
with an end year in end_years, and therefore the lists should be the same
length with start_years[i] < end_years[i] for each index i. Year ids need not
be unique.  Example: start_years = [1990, 1990, 1990, 2000, 2000, 2010]
and end_years = [2000, 2010, 2019, 2010, 2019, 2019] would run decomp for the
intervals 1990-2000, 1990-2010, 1990-2019, 2000-2010, 2000-2019, and
2010-2019. Cannot be used in conjunction with year_ids, below.

--end_years: a list of year ids comprising the end of each time interval,
see above.

--year_ids: if specified decomposition will be run for every available
time interval possible by combining year_ids in the list. Example:
if year_ids = [1990, 1995, 2000, 2010] then decomp will be run for
1990-1995, 1990-2000, 1990-2010, 1995-2000, 1995-2010, and 2000-2010.
Cannot be used in conjunction with year_start_ids or year_end_ids, above.

--compare_version: compare_version from which to pull death data.

--gbd_round_id: gbd_round_id used to pull death data and life tables.

--decomp_step: string identifier for GBD decomp step used to pull
death data and life tables.

--sex_ids: list of sex ids to be included in the outputs.
Default is [1, 2, 3].

--environment: 'prod' or 'dev' to determine whether to upload to the
production or test gbd database. Default is 'dev'.

--verbose: flag, if set, will print a statement for each file created.

--n_processes: number of concurrent processes to run simultaneously.
If set too high this could overload the database with queries.
Default is 8.


Using the LEMaster class
--------------------------
The methods of the LEMaster class are separated by functionality
so that process version creation, file creation, and upload can
be performed individually for testing or validation of results
prior to upload.  The run_decomp method will save files in
the directory specified, which can be uploaded at a later time
either manually or by instantiating another LEMaster object
with identical arguments, creating a process version or
setting the process_version attribute, and running the upload method.
