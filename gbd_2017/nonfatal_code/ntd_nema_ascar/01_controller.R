## Empty the environment
rm(list = ls())

## Set up focal drives
os <- .Platform$OS.type
if (os=="windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

## Load functions and packages
library(lazyeval)
library(tibble)
library(bindrcpp)
library(glue)
library(argparse)
source("FILEPATH")

## Code directory
code_dir <- "FILEPATH"
my_shell <- "FILEPATH"

#############################################################################################
###                                     Main Script                                       ###
#############################################################################################
## Helper vectors
model_ver <- 43370
my_worm   <- "ascariasis"
meids     <- fread(paste0(code_dir, "child_scripts/sth_meids.csv"))
output_directory <- "FILEPATH"
draw_directory   <- "FILEPATH"

## Get locations to parrelize by
year_ids  <- sort(get_demographics("ADDRESS", gbd_round_id = 5)$year_id)
my_locs   <- sort(get_demographics("ADDRESS", gbd_round_id = 5)$location_id)
meids     <- meids[worm == my_worm]
all_cases <- meids[model == "all"]$meid

## Submit jobs to compute prevalence draws of worm
ifelse(!dir.exists(paste0(output_directory, all_cases, "/")), dir.create(paste0(output_directory, all_cases, "/")), FALSE)
for (loc in my_locs) {

  qsub(jobname = paste0("draws_for_location.id_", loc),
       shell   = my_shell,
       code    = paste0(code_dir, "child_scripts/child_impute_gpr.R"),
       project = "tb",
       args = list("--my_loc",  loc,
                   "--year_ids", paste0(year_ids, collapse = " "),
                   "--output_directory", paste0(output_directory, all_cases, "/"),
                   "--draw_directory", draw_directory,
                   "--meids_directory", paste0(code_dir, "child_scripts/sth_meids.csv"),
                   "--my_worm", my_worm),
       slots = 5,
       mem   = 30)
}

## Wait for jobs to complete
for (loc in my_locs) {
  while (!file.exists(paste0(output_directory, all_cases, "/", loc, ".csv"))) {
    cat(paste0("Waiting for file: ", output_directory, all_cases, "/", loc, ".csv -- ", Sys.time(), "\n"))
    Sys.sleep(60)
  }
}

## Now upload:
my_description <- ""

save_results_epi(input_dir = paste0(output_directory, all_cases, "/"), input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = all_cases, measure_id = 5, metric_id = 3, description = my_description, mark_best = TRUE)

#############################################################################################
###                             Prep Proportions used for splits                          ###
#############################################################################################

## Submit job to prep porportions
qsub(jobname = paste0("compute_sequlae_proportions_", my_worm),
     shell   = my_shell,
     code    = paste0(code_dir, "child_scripts/child_infest_prop.R"),
     project = "tb",
     args = list("--my_worm", my_worm,
                 "--output_directory", output_directory),
     slots = 5,
     mem   = 30)

## Wait for jobs to complete
for (worm in my_worm) {
  while (!file.exists(paste0(output_directory, my_worm, "_prop.csv"))) {
    cat(paste0("Waiting for file: ", output_directory, my_worm, "_prop.csv -- ", Sys.time(), "\n"))
    Sys.sleep(30)
  }
}

#############################################################################################
###                                 Heavy - Sequelae Splits                               ###
#############################################################################################

## Grab correct meids
heavy <- meids[model == "heavy"]$meid
mild  <- meids[model == "mild"]$meid

## Submit jobs to compute prevalence of heavy infestation
ifelse(!dir.exists(paste0(output_directory, heavy, "/")), dir.create(paste0(output_directory, heavy, "/")), FALSE)
for (loc in my_locs) {

  qsub(jobname = paste0("sequlae_split_location_", loc),
       shell   = my_shell,
       code    = paste0(code_dir, "child_scripts/child_sequlea_split.R"),
       project = "tb",
       args = list("--my_loc",  loc,
                   "--is_heavy", 1,
                   "--my_worm", my_worm,
                   "--output_directory", paste0(output_directory, heavy, "/"),
                   "--prop_directory", paste0(output_directory, my_worm, "_prop.csv"),
                   "--meids_directory", paste0(code_dir, "child_scripts/sth_meids.csv")),
       slots = 5,
       mem   = 30)
}

## Wait for jobs to complete
for (loc in my_locs) {
  while (!file.exists(paste0(output_directory, heavy, "/", loc, ".csv"))) {
    cat(paste0("Waiting for file: ", output_directory, heavy, "/", loc, ".csv -- ", Sys.time(), "\n"))
    Sys.sleep(60)
  }
}

## Upload
my_description <- ""

save_results_epi(input_dir = paste0(output_directory, heavy, "/"), input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = heavy, measure_id = 5, metric_id = 3, description = my_description)

#############################################################################################
###                              Medium -   Sequelae Splits                               ###
#############################################################################################

## Submit jobs to compute prevalence of medium infestation
ifelse(!dir.exists(paste0(output_directory, mild, "/")), dir.create(paste0(output_directory, mild, "/")), FALSE)
for (loc in my_locs) {

  qsub(jobname = paste0("Sequlae_split_location_", loc),
       shell   = my_shell,
       code    = paste0(code_dir, "child_scripts/child_sequlea_split.R"),
       project = "tb",
       args = list("--my_loc",  loc,
                   "--is_heavy", 0,
                   "--my_worm", my_worm,
                   "--output_directory", paste0(output_directory, mild, "/"),
                   "--prop_directory", paste0(output_directory, my_worm, "_prop.csv"),
                   "--meids_directory", paste0(code_dir, "child_scripts/sth_meids.csv")),
       slots = 5,
       mem   = 30)
}

## Wait for jobs to complete
for (loc in my_locs) {
  while (!file.exists(paste0(output_directory, mild, "/", loc, ".csv"))) {
    cat(paste0("Waiting for file: ", output_directory, mild, "/", loc, ".csv -- ", Sys.time(), "\n"))
    Sys.sleep(60)
  }
}

## Upload
my_description <- ""

save_results_epi(input_dir = paste0(output_directory, mild, "/"), input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = mild, measure_id = 5, metric_id = 3, description = my_description)
