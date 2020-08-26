
Phases in Overview
==================

The Dalynator has three phases:

1. Most-detailed
#. percentage-change
#. upload

As with the Burdenator, in each phase the central process starts thousands of jobs and waits for them to complete before
moving on to the next phase.

The log files are in the same place as they are for the Burdenator.


Dalynator Arguments
=====================

-r/--resume
    Resumes an existing run

\--input_data_root
    The root directory of all input data
    Default: FILEPATH

-o/--out_dir
    The root directory for the output files. Version will be appended
    Default: FILEPATH

\--log_dir
    The root directory for the log files
    Default: FILEPATH

\--version
    The output version_id for the dalynator, used for default output dir
    REQUIRED

\--cod
    The version of cod results to use, an integer
    Default: current best cod results

\--epi
    The version of epi results to use, an integer
    Default: current best epi results

-g/--gbd_round_id
    The gbd_round_id to run the Dalynator on
    REQUIRED

\--location_set_ids
    Space-separated list of location_set_ids to run the Dalynator over
    REQUIRED

\--measures
    Space-separated list of measures to run the Dalynator on.
    Note: this arg is more for the Burdenator.
    Use the default for the dalynator
    Default: daly

-mdy/--mixed_draw_years
    A string representation of a python dictionary in JSON format to map
    different draw numbers to different year sets. There have to be multiple
    draws being used for this flag and it is mutually exclusive with --years
    and --n_draws. There cannot be any spaces within the string
    Example: {"1000":[1990,1995],"100":[2000,2005]}

\--years
    Space-separated list of year_ids. The first set of years when only one draw
    and year set is being used
    Default: 1990 1995 2000 2005 2010 2016

\--n_draws
    Number of draw_columns for all input and output draw files, for the
    specific year being run when only one draw and year set is being used
    Default: 1000

\--start_years
    Space-separated list of start years for pct-change calculation
    Default: empty list

\--end_years
    Space-separated list of end years for pct-change calculation
    Default: empty list

-s/--sge_project
    The sge_project to run the Dalynator under
    Default: proj_dalynator

-v/--verbose
    Whether or not to print the many debugging messages
    Default: False

\--start_at
    Which phase to start with. Options are 'most_detailed', 'pct_change', or 'upload'
    Default: most_detailed

\--end_at
    Which phase to end with. Options are 'most_detailed', 'pct_change', or 'upload'
    Default: pct_change

-x/--do_not_execute
    Whether or not to actually execute the dalynator
    Default: False, actually run it!

-n/--turn_off_null_and_nan_check
    Whether or not to turn off input restriction for nulls and NaNs

\--upload_to_test
    Whether or not to upload data to test database
    Default: False

\--dual_upload
    Whether or not to upload data to viz databases AND modeling databases concurrently


Running the Dalynator
=====================

The dalynator code is in the same git repository as the Burdenator code, and is installed in exactly the same way.

The following example on one location set (35 == "Model results"), two years, 1000 draws, with percentage-change calculation between those two years::

    python dalynator/tasks/run_all_dalynator.py –-cod 66 -–epi 146 –-location_set_ids 35 --years 1990 2016 -–n_draws 1000 -–start_years 1990 -–end_years 2016 -n -–version 2 -o FILEPATH

This example runs the dalynator over Sub-Saharan Africa (location set 81), store the draw files in the default location
``FILEPATH``, and uploads the summaries to the database::

    python dalynator/tasks/run_all_dalynator.py --cod 43 --epi 96 --no_sex --no_age --location_set_id 81 --years 2005 -n --version 66 --end_at upload

The phrase ``$(whoami)`` runs the ``whoami`` command and adds it into the directory path at that point. ``whoami``
returns your user name.

This example runs the dalynator over Sub-Saharan Africa, stores in the default location ``FILEPATH``
and calculates percentage changes::

    python dalynator/tasks/run_all_dalynator.py --cod 43 --epi 96 --location_set_ids 81 --years 2005 2010 2016 --start_years 2005 2010 --end_years 2010 2016 -n --version 5 --end_at upload

The years in the start and end lists are matched up, so this will compare 2005 vs 2010, and 2010 vs 2016.

The Dalyantor and Burdenator can work with two levels of draw granularity, controlled by two sets of flags:
``--years`` and ``--n_draws``, and ``-mdy/--mixed_draw_years.``
All years in the each set must have exactly the number of draws as specified by the n_draws flag.  For example:
``-mdy {"1000":[2000,2005],"100":[2007,2008]}``
would run years 2000 and 2005 at 1,000 draws, and years 2007 and 2008 at 100 draws. Percentage change calculations
cannot be performed between years with differing numbers of draws.

This example adds a second set of years that only have 100 draws::

    python dalynator/tasks/run_all_dalynator.py --cod 43 --epi 96 --location_set_ids 81 -mdy {"1000":[2005,2010,2016],"100":[2006,2007,2008,2009]} --start_years 2005 2010 --end_years 2010 2016 -n --version 66 --upload

The central run_all script will wait until either all the sub-jobs have finished, or any one phase fails. You can use a sql query
browser to run queries on the jobmon database for more information. This central python script is only a controller,
it does not perform any math itself. It writes to a central log here:
``FILEPATH``

You can follow its progress by running::
``tail -f FILEPATH``

The ``-h`` flag displays some help and usage information. Most flags have short and long forms. All long-forms have
two minus signs, all short forms have one. For example, the following flags are equivalent  ``-p 78`` and ``--paf_version 78``

tHe ``-v/--verbose`` flag turns on DEBUG level logging. The default is INFO level logging.

If you kill the central job or it dies and you need to resume, you can to restart it with the ``--resume`` flag.

.. note::
    Now, to resume, you don't have to pass in all the args! Instead, only pass in the out_dir, the version number, and  --resume. Optionally, you can change the start_at/end_at flags and the verbose flag, and these will not change the shape of the dag. Then the rest of the arguments will be read from a file, that was created upon an earlier run of this dag.


The dalynator will restart from the beginning, repeating all work that was done in the last run. Phases can be turned off
with by using the following flags:

* ``--start_at``  Allows you to control which phases to run. Valid options are "most_detailed", "pct_change", or "upload". Specifying a --start_at of most_detailed and an --end_at of upload will run all three phases.

.. note::
    The default --start_at value is most_detailed phase. However, the default --end_at value is pct_change, NOT upload. If you want to upload data, you have to intentionally opt in, using --end_at upload. Intermediate phases, like percent change if ending at upload cannot be skipped, the dag will be built from the start_at to end_at phases.

The ``--upload`` flag has been deprecated. Instead use --end_at upload

The jobs are submitted using the SGE project ``proj_dalynator.`` The project name can be overriden by using the flag
``--sge_project <other_project_name>.``  This should rarely be necessary.


Aggregation
***********

The dalynator performs sex and age aggreagation, but does not perform ANY location aggregation.
The flags that refer to location sets are expanded to provide the sets of location ids for the
most-detailed phase (dalynation) and upload.



Monitoring a run of the Dalynator
**********************************

The Dalynator has the same basic job control as the Burdenator.
It writes to log files and also send its job status to jobmon. The jobs are visible using qstat,
but jobmon and (especially) the log files have more detailed information.


Restarting a Stuck Run, using Workflow
==========================================

The cluster can be unreliable. Most of the time all the jobs in a phase will complete. However, sometimes
a small percentage of jobs will die due to some instability in the cluster.
The best way to fix this is to resume the the run! Jobmon, under the hood, will use Workflows to automatically pickup where you left off (i.e. ‘resume’). It will not re-run any jobs that completed successfully in prior runs. See note above, you can to restart it with the same version, output directory, and the ``--resume`` flag.::

    python dalynator/tasks/run_all_dalynator.py --version 1 --out_dir FILEPATH --resume
