# DDM Pipeline

**About**:
Use Death Distribution Methods (DDM) to estimate completeness of vital or civil registration systems. i.e. what proportion of deaths are captured? Scripts below are listed in order.

**Clone to**: `FILEPATH`

**Hub page found here**: WEBSITE

**Launch a new run/restart from**:
- `mort_pipeline_run_all.py` in the shared repo: Run all or part of DDM by changing `run_ddm_pre_5q0`, `run_ddm_mid_5q0`, and `run_ddm_post_5q0` toggles. Ensure input versions are completed runs. NOTE: This will create a new run_id if `gen_new = TRUE`.
- `jobmon_ddm_run_all.py` (DDM specific run all): Submit arguments from command line; run ID, username, ddm step, gbd year, mark best. NOTE: for a new run, create a run_id using our shared function, `gen_new_version`. No need to create the folders for the new run manually (the DDM specific run-all does this).
- For both, use the current mort central conda: `FILEPATH`

**Parent processes**: 
Population estimate, population empirical data, death number empirical data, 5q0 data, 5q0 estimate, ddm data. Default is to use "best" for each.

## Run all:
- `jobmon_ddm_run_all.py`

## DDM Step 0: pre 5q0

### Scripts:
- `c00_compile_empirical_population.r`
- `c00_compile_empirical_deaths.r`
- `set_hyperparameters.r`
- `vr_both_sexes_only.r`
- `sex_ratio_prep.r`
- `prep_input_data.r`
- `c01_format_population_and_deaths.do`
- `c09_compile_denominators.do`

Nothing too much to worry about in this section, most scripts should work smoothly. Compiles population empirical data, death number empirical data, and sets denominators for post-5q0. 

### Potential issues:
- Mostly in c01_format_population_and_deaths.do
- Parallelized by location, not all locations have data
- Any jobmon issues may crop up either here or as a result of failures in c01

## DDM Step 1: mid 5q0

### Scripts:
- `c02_reshape_population_and_deaths.do`
- `c03_combine_population_and_deaths.do`
- `c04_apply_ddm.do`
- `c04_apply_ddm_ccmp.R`
- `c05_format_ddm.do`

### Potential issues:
- Depending on the c01 jobs in step 0, may see failures in c02 jobs
- Make sure the r_shell used is 3501+, CCMP script won't run on R < 3.5

## DDM Step 2: post-5q0

### Scripts:
- `prep_input_data.r`
- `c06_calculate_child_completeness.do`
- `c07_combine_child_and_adult_completeness.do`
- `c08_smooth_ddm.r`
- `c10_calculate_45q15.do`
- `upload/ddm_data_upload.r`
- `upload/ddm_estimate_upload.r`

## Other scripts:
- All helper functions called within the scripts above are in the `functions` folder. Graphing code is in the `ddm_graphs` folder. Scripts to upload process outputs to the database are in the `upload` folder. All other folders are archives.
	