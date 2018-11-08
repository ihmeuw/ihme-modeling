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
library(argparse, lib.loc = "FILEPATH") # needs lib.loc path
source(paste0(j, "FILEPATH/save_results_epi.R"))
source(paste0(h, "FILEPATH/primer.R"))

## Code directory
code_dir <- paste0(j, "FILEPATH")
my_shell <- "FILEPATH/r_shell.sh"

#############################################################################################
###                                     Main Script                                       ###
#############################################################################################

## Helper vectors
output_directory <- "FILEPATH"

my_locs <- sort(get_demographics("ADDRESS", gbd_round_id = 5)$location_id)

## Submit jobs to compute asymptomatic cases
ifelse(!dir.exists(paste0(output_directory)), dir.create(paste0(output_directory)), FALSE)
ifelse(!dir.exists(paste0(output_directory, "errors/")), dir.create(paste0(output_directory, "errors/")), FALSE)
for (loc in my_locs) {

  qsub(jobname = paste0("asymptomatic_computations_location.id_", loc),
       shell   = my_shell,
       code    = paste0(code_dir, "child_asymptomatic"),
       project = "tb",
       args  = list("--my_loc",  loc,
                    "--output_directory", output_directory),
       slots = 8,
       mem   = 30)
}

## Wait for jobs to complete
for (loc in my_locs) {
  while (!file.exists(paste0(output_directory, loc, ".csv"))) {
    cat(paste0("Waiting for file: ", output_directory, loc, ".csv -- ", Sys.time(), "\n"))
    Sys.sleep(60)
  }
}

## Upload
my_description <- paste0("")

save_results_epi(input_dir = output_directory, input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = 3107, measure_id = 5, metric_id = 3, description = my_description, mark_best = T)
