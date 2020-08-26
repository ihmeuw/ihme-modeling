Burdenator Arguments
====================

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
    The output version_id for the Burdenator, used for default output dir
    REQUIRED

\--cod
    The version of cod results to use, an integer
    Default: current best cod results

\--epi
    The version of epi results to use, an integer
    Default: current best epi results

-p/--paf_version
    The version of paf results to use, an integer
    REQUIRED

-g/--gbd_round_id
    The gbd_round_id to run the Burdenator on
    REQUIRED

\--cause_set_id
    The cause_set_id to use for the cause hierarchy, an integer
    Default: 2

\--location_set_ids
    Space-separated list of location_set_ids to run the Burdenator over
    REQUIRED

\--measures
    Space-separated list of measures to run the Burdenator on.
    Default: death daly yll yld

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

\--star_ids
    Whether or not to write out the star_id column
    Default: False

-s/--sge_project
    The sge_project to run the Burdenator under
    Default: proj_burdenator

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
    Whether or not to actually execute the Burdenator
    Default: False, actually run it!

-n/--turn_off_null_and_nan_check
    Whether or not to turn off input restriction for nulls and NaNs

\--upload_to_test
    Whether or not to upload data to test database
    Default: False

\--dual_upload
    Whether or not to upload data to viz databases AND modeling databases concurrently


Phases in Overview
==================


The Burdenator attributes portions of YLDs, YLLs, and Deaths to a variety of
risk factors. Where necessary, it also back-calculates the fractions of total
YLDs, YLLs, and Deaths that have been attributed to each risk factor (PAFs).

"Attributing portions" means that the Burdenator:

1. Extends the dataframe with risk (rei_id) and cause (cause_id) columns.
#. If there is a risk-cause pair in the PAF file for that demographic,
   then multiply the yll/yld/death by the percentage (metric 2) in the PAF file.
#. Add the new rows to the data-frame.

Step two creates a NUMBER measure for that ylld/yld due to that risk-cause pair.

AB means Attributable Burden

BCP means Back Calculated PAF

"Summarize" means compute mean, median, lower-2.5 percentile, upper-2.5 percentile

The Burdenator has five phases:

1. Most-detailed locations:
    #. function burdenate:
        #. Calculates Attributable Burdens (ABs) for YLL, YLD, deaths, dalys from PAF files and measure files, AB[i] = measure_df[i] * paf_dif[i]
        #. Aggregate these AB's up the cause hierarchy
        #. Back-calculate PAFs by dividing all the ABs by all-cause envelope (rei_id ==0 ie TOTAL_ATTRIBUTABLE), NaNs->0; metric is %
        #. Specific PAFs have all their draw columns set to 1.00. (Older documentation said this was just for the aggregated ABs, but that distinction does not seem to be in the code.)
        #. Write all those ABs and back-calculated PAFs as one HDF draw file by measure, location, year. Do not write rei_id == 0 and any passed-in age/sex aggregates
        #. Return the attribute burden, drop the back calculated PAFs
    #. Calculate and write daly draws, if requested
    #. Write out meta-information for downstream aggregation step
    #. function aggregate_summaries:
        #. Summarize AB in number space (summaries only, no draws)
        #. Summarize the Sex aggregates of the ABs in number space  (summaries only, no draws)
        #. Ditto for Age Aggregates, including age-standardized aggregation
        #. Ditto for Age & Sex aggregates (i.e., complete the square in the hypercube)
        #. Summarize BCP (basic, and sex/age aggregates) of incoming ABs
        #. As above, complete the hypercube for Summaries of BCP for Sex and Age aggregates
        #. Convert the ABs to rate space by dividing by population
        #. Similar to above Summarize the basic draws, and age & sex aggregates in rate space
    #. Write summaries as CSVs for upload, but restrict to defined risks. Draws (including the age/sex BCPs) are not saved.
#. Location aggregation
    #. Aggregate the new AB draw files up the primary location hierarchy
#. Cleanup
    #. Repeats the aggregation and summarization from the most-detailed phase, but for the non-leaf aggregate locations in both primary and supplementary aggregate location sets
#. Percentage-change
    #. Aggregate over age and sex
    #. BCP (basic, and sex/age aggregates) of incoming ABs and rates
    #. Calculate percentage-change of means for all Attribute Burden draws for all demographic indexes
    #. Save the summaries only as multi-year csvs for upload, do not save the aggregated draw files
#. Upload
    #. Upload to risk and eti output tables, single year and multi-year (multi-year are for percentage change)


Phases in Detail
==================

Most Detailed Burdenation
-------------------------

28 cores per job

Read PAF HDF Files. These have 1,000 draws and apply to one specific (risk, cause) pair.

The actual code algorithm is:

::

    QSUB by location, year (therefore run all measures in one qsub):
        Using Multi-processing per measure, Burdenate:
            Apply PAFs (vector dot product with measure-draw) to produce attributable burden
            Write most detailed AB draws as HDF
            Summarize AB

        Calculate DALYs (if required), using code from the Burdenator
            Write DALY draws as hdf

        MP per measure, Summarize:
            Summarize AB (mean/median/upper/lower) in Number space
            Summarize aggregate age AB's in Number space, including age-standardized PAFs
            Summarize aggregate sex AB's in Number space
            Summarize back-calculated pafs from these aggregated AB's
              with “nailing” of 100% pafs (The 100% list is hard-wired in the code)
            Summarize in rate space (divided by all causes for that metric)

        For each measure:
            Write summaries as CSV files for database upload



Location Aggregation
--------------------

20 cores per job

::

  For each non-leaf location in the union of the primary and supplementary location hierarchies:
    Read most detailed AB draw HDF files from most-detailed phase
    Aggregate the AB draws up the location hierarchy


Cleanup
-------

25 cores per job

This appears to redo operations from the most-detailed phase, but it is only operating over aggregated locations.
Phase one operated over most-detailed locations, so there is no repeated work.

::

    QSUB by (aggregate) location, year, measure:
        Re-read the AB draw files from phase 2
        Aggregate age AB's in NUMBER space, including age-standardized PAFs
        Aggregate sex AB's in NUMBER space,
            includes converting rate space to number space, and back again
        Back-calculate PAFs from those AB's
        Write summaries as CSVs (summarize is inside df_to_csv!)
        Save aggregate AB draw files

        Recomputes Dalys


Percentage Change
-----------------

30 cores per job

::

    QSUB over measure, location, (start_year, end_year)
        Over AB draws
        Uses percentage change code from summarizers
        Using code from most-detailed phase:
            Aggregate over age
            Aggregate over sex
            Back-calculate PAFs

Upload
------

20 cores per job

8 jobs total: 4 for single-year uploads, 4 for multi-year (percentage-change pair) uploads. The multi-year uploads
are skipped if there were no percentage-change calculations.

Uploads to two tables:
    1. "output_risk_{}_v{}".format( table_type, pv.gbd_process_version_id)
    #. "output_etiology_{}_v{}".format( table_type, pv.gbd_process_version_id)

::

    Get or create GBD Process Version
    QSUB over: single/multi_year, ETI/RISK, measure:
        Create a giant dataframe from all the expected summary CSV files,
            raises ValueError if any file are missing
        SQL infile on summary files in primary key order

Running the Burdenator
======================

In each phase the central process (BurdenatorJobSwarm) starts potentially thousands of jobs. The central process waits
until all the jobs have completed or died before moving on to the next phase. It will not move on if any jobs failed.
The swarm checks their status by polling jobmon (release Emu). Jobmon has its own its own database of messages and also uses qstat.

With this version of the code, you'll have to navigate
to the proper run file within the source. Sorry again. We'll fix this soon,
too. Assuming you've followed the above steps, you should be in the
root directory of the source code.

Then a run command will look something like this::

    python Burdenator/tasks/run_all_burdenator.py --epi 96 -p 181 --location_set_ids 81 --measures yll daly --o FILEPATH -n --years 2005 2010 --start_years 2005 --end_years 2010 --version 13

.. note::

    "whoami" is a command that "prints the effective username of the current
    user." We use it here to direct outputs to a scratch space that is scoped
    to your username. Feel free to change the output_directory to something
    suitable for your needs.

    The "output_version" is the burdenator version.

    Note: Conda works more reliably if you use the "python Burdenator/tasks/run_all_burdenator.py"
    syntax, rather than  "cd Burdenator; python tasks/run_all_burdenator.py"

That command would use PAFs from PAF-version 181 to attribute deaths and YLLs
to risks, aggregate them up the location hierarchy specified in location set
81, and save the results to a "burdenator" directory in your scratch space.  By
default, it will infer the *best* CodCorrect and COMO versions, though you
could override this with the "-c" and "-m" options respectively. We specified
the COMO version directly here, for example, since we know the current *best*
COMO version to be incomplete at the time of this writing. When you run this
yourself, you will likely want to check your versions and substitute your own
sensical location_set_id (location set 81 is a test hierarchy - smaller than
the full GBD location_set, but large enough to properly exercise the burdenator).

If we want to run all GBD years (1990, 1995, 2000, 2005, 2010, 2017) and have
the Burdenator infer the *best* YLD version for us, we can omit the "-m" and
"-y" options::

    python Burdenator/tasks/run_all_burdenator.py  -p 181 --location_set_id 81 --measures death yll --o FILEPATH -n -v

If we want to run the Burdenator, but want to skip some phases, use the --start_at
and --end_at flags. Be aware that if you tell the Burdenator to skip an earlier phase whose outputs a later phase depends on, you'll have trouble. For instance, if you skip most_detailed phase but want to go all the way through upload, make sure you're running with a Burdenator version that already has all the most_detailed files.

    python Burdenator/tasks/run_all_burdenator.py --epi 96 -p 181 --location_set_ids 81 --measures death yll --o FILEPATH -n -y1 2005 2010 --start_years 2005 --end_years 2010 --version 13 --start_at most_detailed --end_at loc_agg

.. note::
    Now, to resume, you don't have to pass in all the args! Instead, only pass in the out_dir, the version number, and  --resume. Optionally, you can change the start_at/end_at flags and the verbose flag, and these will not change the shape of the dag. Then the rest of the arguments will be read from a file, that was created upon an earlier run of this dag.

.. note::
    The default --start_at value is most_detailed phase. However, the default --end_at value is pct_change, NOT upload. If you want to upload data, you have to intentionally opt in, using --end_at upload. Intermediate phases, like percent change if ending at upload cannot be skipped, the dag will be built from the start_at to end_at phases.

Monitoring a run of the Burdenator
==================================

The Burdenator writes to log files and also send its job status to jobmon. The jobs are visible using qstat,
but jobmon and (especially) the log files have more detailed information.


Finding errors and other information in the log files
-----------------------------------------------------

The Burdenator writes many log files. The main process has a log, as does every sub-job in each phase.
There should be no ERROR-level log messages in any file.

The main log file is written by the central process. It is named
in ``FILEPATH``, for example ``FILEPATH``.
This file contains a record of the parameters, every job that is created using qsub, and the checking that these jobs
have exited or died. "tailing" this file is a good way to monitor the progress, e.g.:
``tail -f FILEPATH``

The **most-detailed** jobs write to the files ``FILEPATH``
For example
``FILEPATH``

These files contain the output from one single year-location job. They end with a message similar to:
``DONE location-year pipeline at 1499811386.5, elapsed seconds= 139.761297941``

The **location_aggregation** jobs write to the files
``FILEPATH``
For example
``FILEPATH``
The **cleanup** jobs write to ``<FILEPATH``
For example
``FILEPATH``

They end with a message similar to:
``2017-07-06 12:15:44,486 - Burdenator.run_pipeline_burdenator_cleanup - INFO - DONE cleanup pipeline at 1499368544.49, elapsed seconds= 496.977260113``

The **percentage change** jobs write to ``<FILEPATH``
For example
``FILEPATH``

The **upload** jobs write to single-year and mult-year logs:
``FILEPATH``
For example
``FILEPATH``
``FILEPATH``

Using logs, qstat, qacct and bash
---------------------------------

Dev-ops for multiple jobs are easier with scripts. For example:

.. code:: bash

  stopped='136414619 136414626 136414649 136414652 136414670 136415197 136415203 136415210 136415213 136415226 136416919'

  for j in $stopped; do
    loc=`qacct -j $j | grep dn_pct | sed -e "s/jobname.*dn_pct_change_//" | tr -d '[:space:]'`
    echo "$j : $loc"
    ls $loc
    for f in `ls $loc`; do
      echo  $loc/$f
      tail -3 $loc/$f
      echo
    done
    echo
  done

.. end

The above script extracts the location_ids for a specified set of SGE job-ids.


Restarting a Stuck Run, using Workflow
--------------------------------------

The cluster can be unreliable. Most of the time all the jobs in a phase will complete. However, sometimes
a small percentage of jobs will die due to some instability in the cluster.
The best way to fix this is to resume the the run! Jobmon, under the hood, will use Workflows to automatically pickup where you left off (i.e. ‘resume’). It will not re-run any jobs that completed successfully in prior runs. See note above, you can to restart it with the same version, output directory, and the ``--resume`` flag.::

    python Burdenator/tasks/run_all_burdenator.py --version 1 --out_dir FILEPATH --resume

