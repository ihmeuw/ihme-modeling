# Poland Pipeline

## Overview
Code for producing aggregated data at the bundle level. The aggregation (e.g. the columns to group by) is determined by the estimate id and deliverable name.

This pipeline takes inspiration from the innovations made in MarketScan and CMS; so if you are familiar with either of these pipelines a lot of the code will feel familiar. If not, then Poland serves as a great introduction to the two since it is considerable less complex than either of them.

## Run Requirements:

PolandClaimsEstimates requires three flat files that are generated outside of the class.

The year file source table, `FILEPATH`, logs the bested file source id for a given year of data. This table is generated after a successful icd mart creation and is used by  PolandClaimsEstimates to determine which file source id to read in for a given year when pulling icd mart data.

Poland uses GBD population for denominators and these estimates are cached within a run directory at `FILEPATH`. The pipeline pulls and stores these estimates before submitting a swarm of tasks by calling `cache_population.main()`

While not a hard requirement the task_mem module produces an approximated memory requirement for a given task. The module relies on `schema/build_bundle_mem.py` to produce bundle level memory requirements, which is versioned by map_version and file_source_id. `Build_bundle_mem` is not automated so it should be ran whenever there is a new map version or file source id.

As an aside, PolandClaimsEstimates reads and writes from a run directory and pulls ids from the run metadata using a ClaimsWrapper object, however, creating both the run directory and a new row in run metadata is outside the scope of this repo.

## Run Instructions

There are two ways to produce outputs for the Poland, using the Jobmon script (link) or instating the PolandClaimsEstimates class.

### Jobmon

Arguably this is the preferred method for producing Poland estimates.

`swarm.submit_swarm()` is responsible for creating and running a collection, swarm, of jobmon tasks. The method requires seven arguments that are listed in the doctoring of the method. One item of note is the `bundles_to_run` argument. Similar to MarketScan, the method accepts either a list of bundles or either of the strings `all_bundles` or `active_bundles`. When providing a string argument the module will fetch a collection of bundles and remove bundles exclusively coded to ICD9. This filtering does not occur with user provided bundles.

 Map version is not a required argument, and is instead pulled from the `clinical.run_metadata` table via a ClaimsWrappers object. As previously mentioned it is the responsibility of the user to create a new row in this table with the appropriate version ids.

Aside creating tasks,  `swarm.build_task_parameters()` and appending those tasks onto a workflow in `FILEPATH`, the method pulls and caches GBD population for both subnational and national locations.

Below is an example of a production run call to the method:

```python

from pol_nhf.pipeline.lib import swarm

swarm.submit_swarm(
	run_id=900,
	bundles_to_run="active_bundles",
	deliverables=["gbd", "correction_factors"],
	write_final_data=True,
	run_plotting=True,
)
```
Once the workflow has completed the final data set is available at `FILEPATH`. This data set is prepped for `clean_final_bundles.py` and is also used for the default plotting method at `FILEPATH`

See [examples](URL) for additional details.

### PolandClaimsEstimates

This class is responsible for ingesting a subset of the ICD mart and producing bundle level results for a given clinical estimate id for all years and all locations (subnational and national).

In truth the class calls a set of modules listed in `FILEPATH` that have very specific tasks and are some what autonomous with respect to the other modules (certain columns are required for certain modules but the modules themselves never call one another).

These modules and order of operations should be recognizable to those familiar with other claims processing. Unlike MarketScan and CMS data for GBD Poland is not noise reduced. As such the pipeline is finished once the data is converted to rate space.

Below is an example of instantiating the class and writing out estimates:

```python

from pol_nhf.pipeline.api import internal

pol_obj = PolandClaimsEstimates(
	run_id = 900,
	bundle_id = 3,
	estimate_id=17,
	deliverable="gbd",
	write_final_data=True,
)

pol_obj.main()
```
The data is written to either `FILEPATH` or `FILEPATH` depending on the deliverable. If there is no data after a mapping step, notably apply_map(), then an empty csv is written to `FILEPATH`
