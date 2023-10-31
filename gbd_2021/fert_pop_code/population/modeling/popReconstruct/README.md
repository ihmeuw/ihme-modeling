# Population Modeling

**Hub page documentation:** LINK

**Output directory **`output_dir`**:** "FILEPATH"

___

## General run notes

### Launch new full run

To launch a brand new run you will need to run `00a_setup` through to completion. The main settings you might have to change for each run are `test`, `best`, `comment`, `copy_previous_version`, `copy_vid`, `rerun_locations` and the input run ids if you want to pull anything other than the best run ids.

### Relaunching a broken run

After looking at the error/output logs and fixing issues you can relaunch a previous run by running `00b_submit` interactively with `pop_vid` set to the correct run id. The script will automatically check for expected output files before launching each step and will slack updates as important steps finish.

### Running interactively and making changes to one script

When working on updating a part of population modeling I usually run a test version with `test = TRUE`.

If I don't need to test raking then I will just run the test for a couple of locations by modifying `test_locations` to only include a couple of ihme_loc_ids that I want to test on.

If I'm not testing the model fitting step then I also set `copy_previous_version = TRUE` and specify a previous `pop_vid` to copy from so that the time intensive model fitting step is skipped and previous model fits are copied over.

Then after the steps prior to the one I need to work on have finished I can just run the step I need interactively and it has a default for `pop_vid` that is used whenever a script is run interactively. If for some reason you needed to run a step with a different run id you will need to modify the interactive defaults section.

### Image notes

While most steps use R versions > 3.5, set 03a_fit_model does not. In order for this step to read the run settings, step 00a_setup needs to be run in a version < 3.5.

## Set up a new run

**code:** `00a_setup.R`

**output:**

  * settings object: `{output_dir}/run_settings.rdata`

  * location hierarchy: `{output_dir}/inputs/location_hierarchy.csv`

**description:**

  * specify some general run settings.

    * `best`: Whether to mark a new run as internal best automatically once the process is done uploading.

    * `test`: Set to true if you don't need to actually create a new run id. Will create a fake run id `99999` that is stored in `/mnt/team/fertilitypop/pub/population/popReconstruct/tests/99999/`

    * `test_locations`: Specify ihme_loc_ids if you only want to fit the population model for a subset of locations, will skip over raking if this vector is non-empty.

    * `copy_previous_version`: Whether to skip model fitting and just copy over the model fits from the previous version `copy_vid`. Especially useful if we need to just fix a couple locations for a previous run.

    * `copy_vid`: The previous version to copy results over from.

    * `rerun_locations`: Specify ihme_loc_ids that you do want to rerun for rather than copying over results for. Leave this empty if you want to copy over all results from the previous version.

  * generate new run ids `pop_reporting_vid`, `pop_single_vid`, and `migration_vid`

  * specify input run ids and other settings for new run in `settings` list object. These are loaded into the environment as variables using the `list2env` function in all R scripts. Some of the most important/confusing settings are described below.

    * `no_shock_death_number_run_id`: Full lifetables are a confusing input, they are not actually uploaded to a database and the flatfiles they are stored in are actually versioned by the no shock death number. So you need to specify the correct no shock death number that is a parent of the full shock life table version that is wanted.

    * `census_processed_data_vid`: This is the version number for the processed census data that gets used in the model.

  * Generate parent-child relationships.

  * Download modeling and reporting location hierarchies. Use specified settings to determine locations to run model for and locations to copy results over for.

  * Create new run specific directories.

  * census specific settings: `{output_dir}/database/census_specific_settings.csv`

  * location specific settings: `{output_dir}/database/location_specific_settings.csv`

    * `pooling_ages`: TODO

  * Submit `00b_submit.R`.

  * Slack message confirming process launch and inputs versions used.

**notes:** Run this script when you need to launch a completely new run of population modeling.

___

## Launch run

**code:** `00b_submit.R`

**input:**

  * location specific settings: `{output_dir}/inputs/location_specific_settings.csv`

**description:** submits each subsequent step in the process with resubmissions if jobs stop running.

**notes:** This script runs for a long time since it will check for output files from each step to confirm job completion before submitting the next step in the pipeline. Can specify for each step whether to send a slack success messages, number of resubmissions etc.

___

## Download model inputs

**code:** `01a_download_model_inputs.R`

**output:**

  * age groups: `{output_dir}/database/age_groups.csv`. Also defines groupings for different age groups needed later like the most detailed set of age groups, population reporting age groups, migration reporting age groups, migration plot age groups etc.

  * processed census data: `{output_dir}/database/census_data.csv`

  * migration data: `{output_dir}/database/migration_data.csv`

  * migration age patterns: `{output_dir}/database/QAT_migration.csv` and `{output_dir}/database/EUROSTAT_migration.csv`

  * sex ratio at birth estimates: `{output_dir}/database/srb.csv`

  * age-specific fertility rate estimates: `{output_dir}/database/asfr.csv`

  * full lifetables: `{output_dir}/database/full_lt.csv`

  * current gbd round population estimates: `{output_dir}/database/gbd_pop_current_round_best.csv`

**description:** Downloads all inputs needed for prepping inputs and to start model fitting.

___

## Download other inputs

**code:** `01b_download_other_inputs.R`

**output:**

  * under 1 population proportions: `{output_dir}/database/under1_pop.csv`

  * previous gbd round population estimates: `{output_dir}/database/gbd_pop_previous_round_best.csv`

  * comparator population estimates: `{output_dir}/database/comparators.csv`

  * current gbd round migration estimates: `{output_dir}/database/gbd_migration_current_round_best.csv`

  * previous gbd round migration estimates: `{output_dir}/database/gbd_migration_previous_round_best.csv`

  * WPP net migration estimates: `{output_dir}/database/wpp_total_net_migration.csv`

  * previous gbd round drop age: `{output_dir}/database/GBD2017_best_versions.RDS` TODO: update this filepath for GBD2020

  * sdi estimates: `{output_dir}/database/sdi.csv`

**description** Downloads all inputs needed for steps after model fitting is done (predictions, uncertainty, plotting)

___

## Prep demographic inputs

**code:** `02a_prep_demographic_inputs.R`

**input:**

  * location specific settings: `{output_dir}/inputs/location_specific_settings.csv`

  * age-specific fertility rate estimates: `{output_dir}/database/asfr.csv`

  * sex ratio at birth estimates: `{output_dir}/database/srb.csv`

  * full lifetables: `{output_dir}/database/full_lt.csv`

  * migration data: `{output_dir}/database/migration_data.csv`

  * current gbd round population estimates: `{output_dir}/database/gbd_pop_current_round_best.csv`

  * migration age patterns: `{output_dir}/database/QAT_migration.csv` and `{output_dir}/database/EUROSTAT_migration.csv`

**output:**

  * prepped asfr estimates: `{output_dir}/inputs/asfr.csv`

  * prepped srb estimates: `{output_dir}/inputs/srb.csv`

  * prepped survival proportion estimates: `{output_dir}/inputs/survival.csv`

  * prepped net migration proportion estimates: `{output_dir}/inputs/migration.csv`

**description** Prepare prior for all demographic inputs to be used in the population model except for population data.

  * Average adjacent calendar year asfr and srb estimates to produce mid-year to mid-year asfr and srb estimates.

  * Calculates nLx and Tx columns in the full lifetable, then calculates calendar year survival proportions. Averages adjacent calendar year survival proportion estimates to produce mid-year to mid-year survival proportion estimates.

  * Prep the migration prior. This portion of the code is pretty confusing and can hopefully be removed once we have migration estimates.

    * Subsets migration data to just locations where we want to use the data as specified by `use_migration_data` in the location specific settings.

    * For data that is already age and sex specific, use gbd population estimates to convert from counts to proportions.

    * For data that is only all ages and both sexes combined, rescale a net migration proportion age pattern to match the original total. In RWA and ERI use a uniform age pattern, in QAT, SAU, BHR, ARE, and OMN use a previous QAT age-pattern and in all other locations use an average EUROSTAT data age pattern.

___

## Prep population inputs

**code:** `02b_prep_population_inputs.R`

**input:**

  * census specific settings: `{output_dir}/inputs/census_specific_settings.csv`

  * location specific settings: `{output_dir}/inputs/location_specific_settings.csv`

  * processed census data: `{output_dir}/database/census_data.csv`

**output:**

  * prepped population data: `{output_dir}/inputs/population.csv`

**description**

  * In locations where we don't want to use the backprojected baseline `use_backprojected_baseline = F`, swap in the previous gbd population 1950 estimates.

  * Aggregate population data to modeling age groups (either 1 or 5 year age intervals).

  * Mark certain age groups in population data to be excluded from the model.

    * Drop under 5 population counts outside of High‐income and Central Europe, Eastern Europe, and Central Asia super‐regions.

    * Drop age groups from specific censuses as specified by `drop_age_groups`.

___

## Fit population model

**code:** `03a_fit_model.R`

**input:**

  * census specific settings: `{output_dir}/inputs/census_specific_settings.csv`

  * location specific settings: `{output_dir}/inputs/location_specific_settings.csv`

  * prepped asfr estimates: `{output_dir}/inputs/asfr.csv`

  * prepped srb estimates: `{output_dir}/inputs/srb.csv`

  * prepped survival proportion estimates: `{output_dir}/inputs/survival.csv`

  * prepped net migration proportion estimates: `{output_dir}/inputs/migration.csv`

  * prepped population data: `{output_dir}/inputs/population.csv`

**output:**

  * model fit diagnostic output file: `{output_dir}/{loc_id}/outputs/model_fit/fit_drop{drop_above_age}_diagnostics.txt`

  * posterior baseline population mean estimates: `{output_dir}/{loc_id}/outputs/model_fit/baseline_pop_drop{drop_above_age}.csv`

  * posterior migration proportion mean estimates: `{output_dir}/{loc_id}/outputs/model_fit/migration_proportion_drop{drop_above_age}.csv`

**description**

  * For all ages above the drop_age, mark population data in those age groups as to be dropped.

  * Read in all input files and format in matrices for TMB.

  * Fit GBD version of popReconstruct model in TMB.

  * Extract TMB model fit object and the posterior mean estimates.


**notes:**

  * This script is run in parallel by location and `drop_age`.

  * It varies pretty substantially in run time dependent on the inputs. Sometimes the model fails to converge, usually this indicates there is something wrong or unusual about the inputs.

  * The current GBD version of popReconstruct only estimates the baseline population and net migration proportion.

  * Possible to skip the intensive model fitting section by specifying in `00a_setup.R` to copy from a previously completed model version.

___

## Predictions from population model

**code:** `03b_predict_model.R`

**input:**

  * prepped asfr estimates: `{output_dir}/inputs/asfr.csv`

  * prepped srb estimates: `{output_dir}/inputs/srb.csv`

  * prepped survival proportion estimates: `{output_dir}/inputs/survival.csv`

  * prepped net migration proportion estimates: `{output_dir}/inputs/migration.csv`

  * prepped population data: `{output_dir}/inputs/population.csv`

  * posterior baseline population mean estimates: `{output_dir}/{loc_id}/outputs/model_fit/baseline_pop_drop{drop_above_age}.csv`

  * posterior migration proportion mean estimates: `{output_dir}/{loc_id}/outputs/model_fit/migration_proportion_drop{drop_above_age}.csv`

  * under 1 population proportions: `{output_dir}/database/under1_pop.csv`

**output:**

  * posterior migration estimates in reporting age groups: `{output_dir}/{loc_id}/outputs/model_fit/net_migration_posterior_drop{drop_above_age}.csv`

  * prior migration estimates in reporting age groups: `{output_dir}/{loc_id}/outputs/model_fit/net_migration_prior_drop{drop_above_age}.csv`

  * percent and absolute difference between estimates and input population data: `{output_dir}/{loc_id}/outputs/model_fit/errors_drop{drop_above_age}.csv`

  * prior estimates of population and other counts for demographic components: `{output_dir}/{loc_id}/outputs/model_fit/counts_prior_drop{drop_above_age}.csv`

  * posterior estimates of population and other counts for demographic components: `{output_dir}/{loc_id}/outputs/model_fit/counts_drop{drop_above_age}.csv`

**description**

  * Run initial inputs through ccmpp to calculate the prior for population.

  * Run ccmpp using initial inputs + posterior estimates to calculate the unraked posterior mean for population.

  * Split the under 1 population.

  * Aggregate the migration estimates to the reporting age groups.

  * Calculate sex-age specific absolute and percent error for each census.

___

## Select best model version

**code:** `03c_select_best_model.R`

**input:**

  * location specific settings: `{output_dir}/inputs/location_specific_settings.csv`

  * percent and absolute difference between estimates and input population data: `{output_dir}/{loc_id}/outputs/model_fit/errors_drop{drop_above_age}.csv`

  * posterior migration proportion mean estimates: `{output_dir}/{loc_id}/outputs/model_fit/migration_proportion_drop{drop_above_age}.csv`

**output:**

  * information comparing each model version run for each location `{output_dir}/versions_compare.csv`

  * best drop age selected for each location `{output_dir}/versions_best.csv`

**description** Use information about the estimated migration and percent error in order to select a best model version for each location to be used in the next steps.

___

## Create population draws

**code:** `04_create_ui.R`

**input:**

  * best drop age selected for each location `{output_dir}/versions_best.csv`

  * sdi estimates: `{output_dir}/database/sdi.csv`

  * posterior estimates of population and other counts for demographic components: `{output_dir}/{loc_id}/outputs/model_fit/counts_drop{drop_above_age}.csv`

  * prepped population data: `{output_dir}/inputs/population.csv`

**output:**

  * unraked single year age group population draw file (saved in separate files for each location, indexed by `draw`): `{output_dir}/{loc_id}/outputs/draws/population_unraked_ui.h5`

  * unraked single year age group population summary file: `{output_dir}/{loc_id}/outputs/summary/population_unraked_ui.csv`

  * unraked reporting age groups population draw file (saved in separate files for each location, indexed by `draw`): `{output_dir}/{loc_id}/outputs/draws/population_reporting_unraked_ui.h5`

  * unraked reporting age groups population summary file: `{output_dir}/{loc_id}/outputs/summary/population_reporting_unraked_ui.csv`

**description**

  * Determine number of years away each year is from the nearest census and merge on the predicted RMSE.

  * Merge on sdi and for each location-year and then merge on the predicted pes correction variance based on sdi value rounded to the nearest 0.001.

  * Calculate population draws and scale to the original posterior mean.

  * Aggregate to both sexes combined, and reporting age groups.

**notes** Run in parallel by location.

___

## Rake and aggregate draws

**code:** `05_rake_aggregate.R`

**input:**

  * unraked single year age group population draw file (saved in separate files for each location, indexed by `draw`): `{output_dir}/{loc_id}/outputs/draws/population_unraked_ui.h5`

**output:**

  * raked single year age group population draw file (saved in separate files for each draw, indexed by `location_id`): `{output_dir}/raking_draws/population_{draw}.h5`

  * raked reporting age groups population draw file (saved in separate files for each draw, indexed by `location_id`): `{output_dir}/raking_draws/population_reporting_{draw}.h5`

**description**

  * Reads in one draw from all location's unraked population estimate draw files.

  * Rake/Scale population estimates so that the lower levels of the location hierarchy add up to higher levels.

  * Aggregate to both sexes combined, reporting age groups, and aggregate locations (using location scalars).

**notes** Run in parallel by draw number.

___

## Compile location specific draws

**code:** `06_compile_loc_draws.R`

**input:**

  * raked single year age group population draw file (saved in separate files for each draw, indexed by `location_id`): `{output_dir}/raking_draws/population_{draw}.h5`

  * raked reporting age groups population draw file (saved in separate files for each draw, indexed by `location_id`): `{output_dir}/raking_draws/population_reporting_{draw}.h5`

**output:**

  * raked single year age group population draw file (saved in separate files for each location, indexed by `draw`): `{output_dir}/{loc_id}/outputs/draws/population_raked_ui.h5`

  * raked single year age group population summary file: `{output_dir}/{loc_id}/outputs/summary/population_raked_ui.csv`

  * raked reporting age groups population draw file (saved in separate files for each location, indexed by `draw`): `{output_dir}/{loc_id}/outputs/draws/population_reporting_raked_ui.h5`

  * raked reporting age groups population summary file: `{output_dir}/{loc_id}/outputs/summary/population_reporting_raked_ui.csv`

**description** This script just reads in the outputs of the previous step where files are saved separately by draw and instead saves the outputs in separate location specific files in the same format as the unraked population estimates.

**notes** Run in parallel by location.

___

## Create location specific diagnostics

**code:** `07a_plot_diagnostics`

**input:**

  * location specific settings: `{output_dir}/database/location_specific_settings.csv`

  * best drop age selected for each location `{output_dir}/versions_best.csv`

  * prepped population data: `{output_dir}/inputs/population.csv`

  * unraked reporting age groups population summary file: `{output_dir}/{loc_id}/outputs/summary/population_reporting_unraked_ui.csv`

  * raked reporting age groups population summary file: `{output_dir}/{loc_id}/outputs/summary/population_reporting_raked_ui.csv`

  * prior estimates of population and other counts for demographic components: `{output_dir}/{loc_id}/outputs/model_fit/counts_prior_drop{drop_above_age}.csv`

  * posterior estimates of population and other counts for demographic components: `{output_dir}/{loc_id}/outputs/model_fit/counts_drop{drop_above_age}.csv`

  * current gbd round population estimates: `{output_dir}/database/gbd_pop_current_round_best.csv`

  * previous gbd round population estimates: `{output_dir}/database/gbd_pop_previous_round_best.csv`

  * comparator population estimates: `{output_dir}/database/comparators.csv`

  * prior migration estimates in reporting age groups: `{output_dir}/{loc_id}/outputs/model_fit/net_migration_prior_drop{drop_above_age}.csv`

  * posterior migration estimates in reporting age groups: `{output_dir}/{loc_id}/outputs/model_fit/net_migration_posterior_drop{drop_above_age}.csv`

  * current gbd round migration estimates: `{output_dir}/database/gbd_migration_current_round_best.csv`

  * previous gbd round migration estimates: `{output_dir}/database/gbd_migration_previous_round_best.csv`

  * WPP net migration estimates: `{output_dir}/database/wpp_total_net_migration.csv`

  * current gbd round drop age: `ihme/fertilitypop/population/popReconstruct/{pop_current_round_run_id}/versions_best.csv`

  * previous gbd round drop age: `{output_dir}/database/GBD2017_best_versions.RDS` TODO: update this filepath for GBD2020

**output:**

  * "best" drop age location specific diagnostic: `{output_dir}/diagnostics/location_best/model_fit_{ihme_loc_id}_best.pdf`

  * "drop age"" location specific diagnostic: `{output_dir}/diagnostics/location_drop_age/model_fit_{ihme_loc_id}_drop{drop_above_age}.pdf`

**description** Make population and migration graphs.

**notes** This code is actually run twice, first to produce diagnostics for each drop age that is run, then again after the best drop age has been selected.

___

## Create percent change heatmap diagnostic

**code:** `07b_pct_change_heatmap.R`

**input:**

  * raked single year age group population summary file: `{output_dir}/{loc_id}/outputs/summary/population_raked_ui.csv`

  * comparison population: either `{output_dir}/database/comparators.csv` or `{output_dir}/database/gbd_population_{comparison_round}_round_best.csv`

**output:**

  * heatmap diagnostics for each comparison round: `{output_dir}/diagnostics/pct_change_heatmap_{comparison_round}.pdf`

**description** Produces a heatmap showing location specific percent differences between estimate versions. Different page for each age group that is plotted. Is run separately comparing to different population versions (previous GBD round, WPP etc.)

___

## Compile diagnostics and create sandbox diagnostics

**code:** `07c_generate_sandbox_diagnostics.py`

**input:**

  * "best" drop age location specific diagnostic: `{output_dir}/diagnostics/location_best/model_fit_{ihme_loc_id}_best.pdf`

  * census processing location specific diagnostics: "FILEPATH"

  * heatmap diagnostics for each comparison round: `{output_dir}/diagnostics/pct_change_heatmap_{comparison_round}.pdf`

**output:**

  * location specific model and processing diagnostics file: "FILEPATH"

  * compiled location specific diagnostic file: `{output_dir}/diagnostics/compiled_model_fit.pdf` and copied to v

**description**

  * Produce location specific files combining model and processing diagnostics for each location.

  * Compile together location specific model diagnostics into one large file for mass review.

  * Copy heatmaps and compiled mass review file to sandbox directory.

  * Post slack message with link to diagnostics.

___

## Upload population and migration estimates

**code:** `08_upload.R`

**input:**

* population estimates in standard gbd and reporting age groups `{output_dir}/{loc_id}/outputs/summary/population_reporting_raked_ui.csv`

* population estimates in single year age groups `{output_dir}/{loc_id}/outputs/summary/population_raked_ui.csv`

* net migration proportion and count estimates `{output_dir}/{loc_id}/outputs/summary/net_migration_posterior_drop{best_drop_age}.csv`

  * current gbd round drop age: "FILEPATH"

  * previous gbd round drop age: `{output_dir}/database/GBD2017_best_versions.RDS` TODO: update this filepath for GBD2020

**output:**

* net migration in single year age groups for HIV team use `{output_dir}/upload/net_migration_single_year.csv`

* population estimates in single year age groups to be uploaded `{output_dir}/upload/pop.csv`

* population estimates in reporting age groups to be uploaded `{output_dir}/upload/pop_reporting.csv`

* net migration estimates in reporting age groups to be uploaded `{output_dir}/upload/net_migration.csv`

* comparison of the "best" drop ages between different GBD rounds `{output_dir}/diagnostics/drop_ages_changes.csv`

* baby migration diagnostics `{output_dir}/diagnostics/baby_migration.csv`

**description**

* upload population and migration results and mark best if `best = T`

* save single year age group migration results for the HIV team to use

* output baby migration diagnostics and comparison of the best drop ages between different GBD rounds/runs
