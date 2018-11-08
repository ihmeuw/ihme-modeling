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

## HELPER VECTORS
my_worm <- "ascariasis"
meids   <- fread("FILEPATH")
output_directory <- "FILEPATH"

my_locs <- sort(get_demographics("ADDRESS", gbd_round_id = 5)$location_id)
meids   <- meids[worm == my_worm]
asymp   <- meids[model == "asymptomatic"]$meid
if (my_worm == "hookworm") asymp <- 3111

## Submit jobs to compute asymptomatic cases
ifelse(!dir.exists(paste0(output_directory, asymp, "/")), dir.create(paste0(output_directory, asymp, "/")), FALSE)
for (loc in my_locs) {

  qsub(jobname = paste0("asymptomatic_computations_location.id_", loc),
       shell   = my_shell,
       code    = paste0(code_dir, "child_scripts/child_asymptomatic.R"),
       project = "tb",
       args  = list("--my_loc",  loc,
                    "--my_worm", my_worm,
                    "--meids_directory", paste0(code_dir, "child_scripts/sth_meids.csv"),
                    "--output_directory", paste0(output_directory, asymp, "/")),
       slots = 5,
       mem   = 30)
}

## Wait for jobs to complete
for (loc in my_locs) {
  while (!file.exists(paste0(output_directory, asymp, "/", loc, ".csv"))) {
    cat(paste0("Waiting for file: ", output_directory, asymp, "/", loc, ".csv -- ", Sys.time(), "\n"))
    Sys.sleep(60)
  }
}

## Upload
my_description <- ""

save_results_epi(input_dir = paste0(output_directory, asymp, "/"), input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = asymp, measure_id = 5, metric_id = 3, description = my_description, mark_best = T)
