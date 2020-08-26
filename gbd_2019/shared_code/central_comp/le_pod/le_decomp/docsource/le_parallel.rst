Running LE Decomp for one location and one sex
===============================================

The :mod:`le_decomp.le_parallel` module contains functions and classes
for running life expectancy decomposition on a single location/sex combination,
and saving those results to file. The primary entry point is the function
:py:func:`~le_decomp.le_parallel.run_le_parallel`, and the class
:py:func:`~le_decomp.le_parallel.LEDecomp` provides most of the functionality
and finer control for testing, etc.  The former has no return value but will save
results to the directory specified by the output_dir argument, whereas the
latter will not save a file but will return a dataframe through the 'run_all'
method.

Both the function and class mentioned above can be suitably run on a single
thread.  Both will make two or more database calls, and so parallelization
over cluster nodes should be undertaken with caution and is best done by using
the class or command line function found in :mod:`le_decomp.le_master`.


Using run_le_parallel
------------------------
The version of death rates pulled for decomposition depends on the
compare version argument, so make sure that the compare_version, gbd_round_id,
and decomp_step arguments are aligned and that the compare version specified
references appropriate mortality data. This module does not contain validations
for these arguments, and will most likely fail in a messy way if they are
incorrect. Life tables for the decomposition are pulled using the combination
of the gbd_round_id and decomp_step arguments.

Although the function operates for a single location/sex combination,
any number of decompositions for relevant time periods may be performed.
Data is pulled and decomposed for each start year in year_start_ids paired
with each end year in year_end_ids at the same index of each list.
It is up to the user to ensure that the lists are the same length and that
the values of each make sense at every index, as validations for time intervals
are performed in the :mod:`le_decomp.le_master` module.

The cause_list argument should be a list of cause_ids which reference a list
of causes which are mutually exclusive and collectively exhaustive. For example,
all most detailed causes, or all causes in the computing hierarchy which are
cause level 3. There is no validation run to confirm that this is the
case, but the most likely outcome of a cause list which does not meet this
criteria is an :py:func:`~le_decomp.validations.LEDecompError` raised
by the :py:func:`~le_decomp.validations.check_decomp_result` function.
