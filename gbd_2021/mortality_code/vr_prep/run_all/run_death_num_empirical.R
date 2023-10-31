## Run the Death Number Empirical (all-age deaths) data prep pipeline
## MUST run under standard RStudio IDE cluster image

###############################################################################################################
## Set up settings and import functions
rm(list=ls())

## Setup cluster vs. local settings
## h_root: mort-data repo dir
if (Sys.info()[1]=="Windows") {
  root <- ""
  user <- Sys.getenv("USERNAME")
  h_root <- ""
} else {
  root <- ""
  user <- Sys.getenv("USER")
  h_root <- ""
}

## Setup libraries
library(data.table)
library(readr)
library(mortdb)
library(mortcore)

## Setup filepaths etc.
code_root <- paste0(h_root, "")
shell_dir <- paste0(h_root, "")
r_shell <- 
path_to_image <- ""
queue <- ""

## Set run comment
new_run <- T
mark_best <- T
prep_new_all_cause <- T
prep_new_cod_vr <- T
with_shock <- "FALSE"
run_comment <- "Run description here"
cluster_project <- "proj_mortenvelope"
release_id <- 9 # used for pulling location set
age_specific_plots <- T # takes 1hr+
age_specific_locs <- NULL
pop_baseline_vers <- 348
prev_pop_baseline_vers <- 342

gbd_year <- 2020

## Initialize run
new_run_id <- mortdb::gen_new_version("death number empirical", "data", comment = run_comment,
                                      gbd_year = gbd_year)

# Set population estimate/data as parents
# Default is best. Change though if there is a specific version to use
population_parent <- mortdb::get_proc_version('population', 'estimate', gbd_year = gbd_year, run_id = 'best')
popdata_parent <- mortdb::get_proc_version('population empirical', 'data', gbd_year = gbd_year, run_id = 'best')

mortdb::gen_parent_child(child_process = 'death number empirical data', child_id = new_run_id,
                         parent_runs = list(`population estimate` = population_parent, `population empirical data` = popdata_parent))

# Set and make folders
run_folder <- paste0('', new_run_id)
input_folder <- paste0(run_folder, "")
output_folder <- paste0(run_folder, "")

dir.create(run_folder)
dir.create(input_folder)
dir.create(output_folder)

gbd_round_id <- get_gbd_round(gbd_year = gbd_year)

# Get age_ids for format_cod_vr.do
source("")
ages = get_age_metadata(age_group_set_id=19, gbd_round_id=gbd_round_id)
write_csv(ages,paste0(output_folder,''))

## If prep_new_all_cause == F or prep_new_cod_vr == F, pull in the most recent prep of all-cause or CoD VR by taking the most recent_completed run
recent_completed_run_id <- get_proc_version("death number empirical", "data", run_id = "recent_completed")

###############################################################################################################
## Save flat metadata files
loc_map <- setDT(get_locations(gbd_type = "ap_old", gbd_year = gbd_year))
subnat_loc_map <- setDT(get_locations(gbd_type = "ap_old", level = "subnational"))

loc_map[is.na(local_id_2013), local_id_2013 := ""]
subnat_loc_map[is.na(local_id_2013), local_id_2013 := ""]

write_csv(loc_map, paste0(input_folder, ""))
write_csv(subnat_loc_map, paste0(input_folder, ""))

source_type_ids <- setDT(get_mort_ids(type="source_type"))[,.(source_type_id, type_short)]
source_type_ids[, type_short := tolower(type_short)]
setnames(source_type_ids, "type_short", "source_type")

method_ids <- setDT(get_mort_ids(type="method"))[,.(method_id, method_short)]
write_csv(source_type_ids, paste0(input_folder, ""))
write_csv(method_ids, paste0(input_folder, ""))

age_map <- get_age_map(type = "all")
write_csv(age_map[,.(age_group_id,age_group_name)], paste0(input_folder, ""))
if(is.null(age_specific_locs)){
  age_specific_locs <- data.table()
} else {
  age_specific_locs <- data.table(location_id = age_specific_locs)
}
write_csv(age_specific_locs, paste0(input_folder, ""))

# Save population
population_vers <- get_proc_lineage("death number empirical", "data", run_id = new_run_id)[parent_process_name == "population estimate", parent_run_id]
population <- get_mort_outputs('population', 'estimate', run_id=population_vers, gbd_year = gbd_year)
population <- population[, .(age_group_id, location_id, year_id, sex_id, population = mean, run_id)]
# GBD2021 - use baseline for subset of countries
pop_baseline <- get_mort_outputs('population', 'estimate', run_id=pop_baseline_vers, gbd_year = gbd_year)
pop_baseline <- pop_baseline[ihme_loc_id %in% c("AZE","GEO","UZB","KGZ","SRB","MDA",
                                                "ECU","ATG","GTM","OMN","QAT")]
pop_baseline <- pop_baseline[, .(age_group_id, location_id, year_id, sex_id, population = mean, run_id)]
population <- population[!location_id %in% unique(pop_baseline$location_id)]
pop_TJK <- get_mort_outputs('population', 'estimate', run_id = 357, gbd_year = gbd_year)
pop_TJK <- pop_TJK[ihme_loc_id == "TJK"]
pop_TJK <- pop_TJK[, .(age_group_id, location_id, year_id, sex_id, population = mean, run_id)]
population <- population[!location_id %in% unique(pop_TJK$location_id)]
population <- rbind(population, pop_baseline, pop_TJK)
fwrite(population ,paste0(input_folder, ""))

## Copy inputs from feeder processes
file.copy("",
          paste0(input_folder, ""))
file.copy("",
          paste0(input_folder, ""))
file.copy("",
          paste0(input_folder, "") )
file.copy("",
          paste0(input_folder, ""))


###############################################################################################################
## Run each step in the process

## Note: Some Stata jobs may fail because of an Invalid Connection String issue -- just relaunch those and report the offending node to infra.

###############################################################################################################
## COD VR
# this makes claude_data.csv
if(prep_new_cod_vr == T) {

  mortcore::qsub(
    "prep_deaths_01a_cod_vr",
    code = paste0(code_root, ""),
    cores = 10,
    mem = 50,
    wallclock = "00:15:00",
    queue = queue,
    proj = cluster_project,
    shell = paste0(shell_dir, ""),
    pass = list(new_run_id, gbd_year),
    archive_node = T,
    submit = T
  )

} else {

  file.copy(paste0("", recent_completed_run_id, ""),
            paste0(output_folder, ""))

}

# Bring in shocks file
mortcore::qsub(
  "prep_deaths_01b_shock_agg",
  code = paste0(code_root, ""),
  cores = 1,
  mem = 10,
  wallclock = "00:50:00",
  queue = queue,
  proj = cluster_project,
  shell = r_shell,
  pass_shell = list(i = path_to_image),
  archive = T,
  pass = list(new_run_id, gbd_year),
  submit = T
)

# this makes cod_VR_no_shocks.csv or cod_VR_with_shocks.csv
mortcore::qsub(
  "prep_deaths_01c_cod_vr",
  code = paste0(code_root, ""),
  cores = 2,
  mem = 20,
  wallclock= "00:30:00",
  queue = queue,
  proj = cluster_project,
  archive_node = F,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id, "TRUE", gbd_round_id, with_shock),
  hold = "prep_deaths_01b_shock_agg",
  submit = T
)

# makes age_sex_split_cod_vr
mortcore::qsub(
  "prep_deaths_01d_cod_vr",
  code = paste0(code_root, ""),
  cores = 2,
  mem = 4,
  wallclock = "00:20:00",
  queue = queue,
  proj = cluster_project,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id, is_cod_vr = 'TRUE', gbd_round_id, release_id, with_shock),
  archive_node = T,
  hold = "prep_deaths_01c_cod_vr",
  submit = T
)

# this makes the file USABLE_ALL_AGE_DEATHS_VR_WHO_GLOBAL_vICD7-10.dta
mortcore::qsub(
  "prep_deaths_01e_cod_vr",
  code = paste0(code_root, ""),
  cores = 4,
  mem= 2,
  wallclock = "00:10:00",
  queue = queue,
  proj = cluster_project,
  archive_node = T,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id),
  hold = "prep_deaths_01d_cod_vr",
  submit = T
)


###############################################################################################################
## Non-COD VR
# this makes allcause_vr_for_mortality.csv
if(prep_new_all_cause == T) {

  mortcore::qsub(
    "prep_deaths_01a_all_cause",
    code = paste0(code_root, ""),
    cores = 2,
    mem = 2,
    wallclock = "00:15:00",
    queue = queue,
    proj = cluster_project,
    shell = paste0(shell_dir, ""),
    pass = list(new_run_id, gbd_round_id),
    submit = T
  )

} else {

  file.copy(paste0("", recent_completed_run_id, ""),
            paste0(output_folder, ""),overwrite = T)

}

# this makes all_cause_vr.dta and not_contiguous.csv
mortcore::qsub(
  "prep_deaths_01b_all_cause",
  code = paste0(code_root, ""),
  cores = 2,
  mem = 5,
  wallclock = "00:20:00",
  queue = queue,
  proj = cluster_project,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id, gbd_round_id),
  hold = "prep_deaths_01a_all_cause",
  archive_node = T,
  submit = T
)

# Here is the part where new non cod and old non cod are combined
# This makes datum_compiled_noncod_deaths and datum_compiled_noncod_deaths_outliers
mortcore::qsub(
  "prep_deaths_01c_all_cause",
  code =  paste0(code_root, ""),
  cores = 4,
  mem = 2,
  wallclock = "00:10:00",
  queue = queue,
  proj = cluster_project,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id),
  hold = "prep_deaths_01b_all_cause",
  archive_node = T,
  submit = T
)

# This makes bearbones_noncod_deaths.csv
mortcore::qsub(
  "prep_deaths_01d_all_cause",
  code =  paste0(code_root, ""),
  cores = 4,
  mem = 2,
  wallclock = "00:10:00",
  queue = queue,
  proj = cluster_project,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id),
  hold = "prep_deaths_01c_all_cause",
  archive_node = T,
  submit = T
)

# this makes noncod_VR_no_shocks.csv
mortcore::qsub(
  "prep_deaths_01e_all_cause",
  code = paste0(code_root, ""),
  cores = 2,
  mem = 20,
  wallclock = "00:30:00",
  queue = queue,
  proj = cluster_project,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id, "FALSE", gbd_round_id, with_shock),
  hold = "prep_deaths_01b_shock_agg,prep_deaths_01d_all_cause",
  archive_node = F,
  submit = T
)

# makes age_sex_split_noncod_vr
mortcore::qsub(
  "prep_deaths_01f_all_cause",
  code = paste0(code_root, ""),
  cores = 2,
  mem = 4,
  wallclock = "00:30:00",
  queue = queue,
  proj = cluster_project,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id, is_cod_vr = F, gbd_round_id, release_id, with_shock),
  hold = "prep_deaths_01e_all_cause",
  archive_node = T,
  submit = T
)

# this makes USABLE_NONCOD_VR
mortcore::qsub(
  "prep_deaths_01g_all_cause",
  code = paste0(code_root, ""),
  cores = 4,
  mem=2,
  wallclock = "00:10:00",
  queue = queue,
  proj = cluster_project,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id),
  hold = "prep_deaths_01f_all_cause",
  submit = T
)


###############################################################################################################
## Check for files

## CoD
# made by prep_deaths_01a_cod_vr / get_cod_data_launcher.py
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 40, sleep_time = 30)
# this is made by prep_deaths_01b_shock_agg
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 20, sleep_time = 30)
# made by prep_deaths_01c_cod_vr / shocks_subtraction.py
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 30, sleep_time = 30)
# this is made by prep_deaths_01d_cod_vr / split_assembled_vr.py
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 20, sleep_time = 30)
# this is made by prep_deaths_01e_cod_vr/ format_cod_vr.do
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 10, sleep_time = 15)

## Non CoD
# made by prep_deaths_01a_all_cause / assemble_allcause_extractions_for_mortality.py
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 20, sleep_time = 30)
# made by prep_deaths_01b_all_cause / convert_bear_bones_to_datum.py
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 20, sleep_time = 30)
# made by prep_deaths_01c_all_cause / non_cod/compile_noncod_deaths.do
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 20, sleep_time = 30)
# made by prep_deaths_01d_all_cause / non_cod/convert_datum_to_bearbones_compiled_noncod.py
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 20, sleep_time = 30)
# this is made by prep_deaths_01b_shock_agg
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 20, sleep_time = 30)
# made by prep_deaths_01e_all_cause / shocks_subtraction.py
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 30, sleep_time = 30)
# made by prep_deaths_01f_all_cause / split_assembled_vr.py
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 20, sleep_time = 30)
# made by prep_deaths_01g_all_cause / non_cod/format_cod_vr.do
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 30, sleep_time = 30)


###############################################################################################################
## Non-COD VR and COD VR combined

# this makes the file d00_compiled_deaths.dta
mortcore::qsub(
  "prep_deaths_03",
  code = paste0(code_root, ""),
  cores = 4,
  mem = 2,
  wallclock = "00:05:00",
  queue = queue,
  proj = cluster_project,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id),
  archive_node = T,
  submit = T
)

Sys.sleep(30)
# this is made by prep_deaths_03 / combine_cod_noncod.do
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 10, sleep_time = 15)

mortcore::qsub(
  "prep_deaths_04",
  code = paste0(code_root, ""),
  cores = 4,
  mem = 10,
  wallclock = "00:10:00",
  queue = queue,
  proj = cluster_project,
  shell = paste0(shell_dir, ""),
  pass = list(new_run_id, gbd_round_id),
  submit = T
)

Sys.sleep(60)
assertable::check_files(paste0(output_folder, ""), continual = T, sleep_end = 20, sleep_time = 15)

# Plotting code
# Change recent_completed_run_id to the run to compare against
mortcore::qsub(
  "graph_deaths",
  code = paste0(code_root, ""),
  cores = 2,
  mem = 20,
  wallclock = "02:30:00",
  queue = queue,
  proj = cluster_project,
  shell = r_shell,
  pass_shell = list(i = path_to_image),
  pass = list(new_run_id, recent_completed_run_id, gbd_year, age_specific_plots, pop_baseline_vers, prev_pop_baseline_vers),
  archive = F,
  submit = T
)

# Toggle F/T depending on whether you want to check the plots before uploading
mortcore::qsub(
  "prep_deaths_05",
  code = paste0(code_root, ""),
  cores = 2,
  mem = 4,
  wallclock = "00:10:00",
  queue = queue,
  proj = cluster_project,
  shell = r_shell,
  pass_shell = list(i = path_to_image),
  pass = list(new_run_id, recent_completed_run_id, mark_best, T),
  archive = T,
  submit = T
)
