# Marketscan Pipeline

Code for running the Marketscan pipeline. Any new data must be transformed into the new ICD-mart schema using src/schema

## A Dataset
The Marketscan pipeline creates a series of logical datasets which can be identified at the bundle-estimate-deliverable level.
For each bundle/estimate/deliverable there is a set of demographic values along with an encounter count (val) column and for non-Correction Factor data additional sample_size and mean columns.
The exact demographic values can vary depending on the age-sex restrictions for a bundle.
We'll start at the top-level here and work our way down into more specific processing details. In order to produce a production level set of estimates (or logical datasets) for Marketscan
we will need to initiate a Jobmon workflow.

## Jobmon Workflow
In order to launch the workflow you'll need to run the `submit_marketscan(...)` function defined in submit_jobmon.py.
The docstring contains some example function calls. For a GBD production run the expected call would be-
```python
wf_status = submit_marketscan(cluster='slurm', bundle_id='active_bundles', deliverable_names=['gbd'], run_id=exp_run_id, write_final_data=True, clinical_age_group_set_id=exp_age_set)
```
Let's get into a few of the arguments and what they're doing. The 'active_bundles' shorthand is pulling a list of bundle_ids from the `active_bundle_metadata` table.
`exp_run_id` is not explicitly defined here because it cannot be determined. The run_id for a future production run must be added to the `run_metadata` table before launch.
The deliverable_names list and write_final_data arguments signify that we will be producing data for use in GBD and saving the results of each job within the standard run_id directory `FILEPATH`. Currently the "gbd" and "correction_factors" deliverables are supported but not "ushd."
Finally the `clinical_age_group_set_id` defines a list of age ranges that the data (containing single-year ages) should be converted to. Note that this should be reviewed thoroughly
if a future run uses a group set id other than 1 and also note that the team has found it impossible to extract any detailed ages less than 1 in the data from 2000 to 2017.

The code related to the workflow is [stored here](URL).
To outline what each does-

- build_jobmon_workflow - Appends tasks to the workflow, partially dependent on the input arguments from `submit_marketscan`
- manage_bundle_list - Converts a couple helper strings to a list of bundle_ids
- submit_jobmon - Main interface to instantiate and run a workflow
- worker_jobmon - Each jobmon task runs this script, which simply calls the CreateMarketscanEstimates class

Next let's step into what each task in the workflow is doing.

## Estimate Creation Class
As outlined above, each task in the workflow calls this class with a set of unique input arguments. This class is in large part a task-runner, essentially running through a series of processing
steps via the `self.main` method and performing various transformations, filters and aggregations on the data. `self.main` relies on a set of private helper functions which themselves primarily call other
modules stored in the `pipeline/lib` folder. However there are also processing depedencies on other Clinical team repos. Namely the [deduplication](URL) and [clinical_info](URL) repos.

Let's step through each helper method in the main method
- self._assign_code_systems() - Assigns a `self.code_system_ids` list based on bundle_id and which ICD coding systems it maps to
- self._read() - Reads the input data from the ICD-mart
- self._map_to_bundle() - Maps the data from ICD codes to bundle_id
- self._remove_ineligible_facilities() - If an estimate_id should contain outpatient data and be deduplicated then pipeline must remove certain ineligible facility_ids
- self._apply_age_sex_restrictions(break_if_not_contig=False) = Applies the standard clinical age-sex restrictions by bundle. Does not require that all ages are present because this data is still based on diagnosis coding and not square
- self._deduplicate() - Transforms the data from claims-level observations to individual level observations. Relies on the deduplication repo and depends on estimate_id
- self._aggregate(...) - Groups the data base a set of (mostly) demographic columns and sums the rows into a new "val" column
- self._write_correction_factors() - Writes encounter level counts and ends processing - only used for the "correction_factors" deliverable
- self._incorporate_denominators() - Reads and attaches the Marketscan denominator files which contain a sample_size column
- self._bin_age() - Bins the single-year `age` column into `age_start` and `age_end` columns based on group set id.
- self._aggregate(...) - Aggregates both the sample_size (denominators) and val columns using the new age bins
- self._create_rates() - Creates a new mean column by dividing val by sample_size
- self._retain_us_child_locations() - Uses the GBD 2020 iterative hierarchy to retain all child locations of the United States. Currently this means the 50 US states plus DC. Note if the US hierarchy is updated this **MUST** be updated as well. The underlying release_id is stored in the config file.
- self._convert_age_to_group_id() - Replaces the age_start/end columns with the GBD standard age_group_id
- self._noise_reduce() - Applies the Clinical noise reduction method to Marketscan data
- self._write_final_data() - Writes final data to `FILEPATH` directory. Does nothing is write final data is not set to True

Below is an example call that may be used to review, test, or debug the pipeline. This will not
store any data nor does it rely on a row of the `run_metadata` table via the `run_id` argument. A production workflow must be run with a non-null run_id and a null map_version. Final data must be written to disk as well.

First, instantiate the processing class
```python
from marketscan.src.pipeline.api import internal

example_ms_class = internal.CreateMarketscanEstimates(
        bundle_id=98,
        estimate_id=21,
        deliverable_name="gbd",
        run_id=None,
        map_version=30,
        clinical_age_group_set_id=1,
        write_final_data=False)
```
Second, run the class' main method
```python
example_ms_class.main()
```
Once `main` finishes the pipeline is complete and no additional processing methods should be performed.
The class has various properties you can use to review the data and its processing inputs.
The key ones for a clinical team data professional are-
- `example_ms_class.df` - A pandas DataFrame of the final data
- `example_ms_class.denominator_df` - A pandas DataFrame of sample size values used to create rates
- `example_ms_class.deliverable` - The deliverable class used in processing, based on the input `deliverable_name`
- `example_ms_class.estimate` - The estimate class used in processing, based on the input `estimate_id`