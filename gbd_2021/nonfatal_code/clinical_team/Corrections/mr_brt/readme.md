MR-BRT Correction factor process

**Prepping MR-BRT models**
`prep_all_cfs.R` is used to prep CF input data after having run the inpatient CF formatting scripts or a run of Poland or Marketscan claims. The script takes input data from FILEPATH for a given run, aggregates sources over year, calculates standard error from sample size and transforms data into log space. Script is set up to run all sources. The output of this script is bundle-cf specific files that are ready to be read in as the input data to a MR-BRT model.

**Running models**
There are three main scripts that run CF models: `cf_launcher.py`, `mr_brt_parent.R`, and `trim_worker.R`. The launcher script's primary purpose is to identify which models should be run (either all active bundles, all active and inactive bundles, or a custom list), launches them, and tracks their completion. Any models that fail will be relaunched and logged with this script. When models are run, the launcher script qsubs `mr_brt_parent.R`, which sets up shared variables across models like age groups and prediction demographics, and sends out worker jobs. Note that all CFs launched within a given CF version will have the same characterstics (knots, input data version, etc.).

`trim_worker.R` is where most of the work happens to run the CF models. If set up to run new models, the script will prep and launch a CF model with covariates based on input data and specified characteristics (i.e. only run with covariates where there is sufficient data, age spline based on user input, etc.). Model will run predictions based on a) the age sex restrictions of the bundle and b) the clinical_age_group_set_id of which age groups the bundle is receiving. If set to predict on existing models, the script will simulate a MR-BRT fit object from specified existing files and run predictions in the same way. Once predictions are run, the script creates flat tails based on data sparsity, transforms back out of log/logit space, and formats data for ingestion by the inpatient pipeline. Model validation plots are also automatically produced from this script.

**Important parameters**
- vers: the CF model run_id
- prep_vers: the id of the set of prepped CF input data
- run_id: clinical run_id to source CF input data
- method: where to get bundles and CFs to run, and whether to run new models or make predictions off existing models
- trim: trim percentage for MR-BRT
- knots: method of knot placement for the spline. Currently supports manual (age 1, 5, 20, every 30 years after) or frequency (3 knots by data density)


**Other scripts**
`cf_compiler.R` was written to compile models across different cf versions for a given bundle for GBD 2020 Refresh 2 & 3. This script is currently pulling from a flat file listing the bundle and cf version, but will eventually pull from the database. Note that cf versions are at the bundle level, not bundle-cf combination.


