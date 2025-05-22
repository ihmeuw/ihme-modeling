################################################################################
## DESCRIPTION Create new version, set any parent-child relationships,
## and save inputs
## INPUTS prepped SRB data
## OUTPUT births data
##
## Steps:
## - Read in configuration files
## - Read in prepped SRB data
##   - Store in input directory
## - Define parent child relatoinships
## - Read in birth data and store in input directory
################################################################################

rm(list = ls())

# load libraries
library(argparse)
library(assertable)
library(data.table)
library(mortdb)

## A. Read in configuration files ----------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir",
  type = "character",
  required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# initial inputs

if(interactive()){
  version_id <- "Run id"
  main_dir <- fs::path("FILEPATH", version_id)
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
}
config <- config::get(
  file = fs::path(main_dir, "/srb_detailed.yml"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

## B. save prepped data into input directory -----------------------------------

prepped_data <- fs::path(prepped_dir, "FILEPATH/processed_data.csv")
new_prepped_path <- fs::path(input_dir, "prepped_data_srb.csv")

if(fs::file_exists(new_prepped_path))
  fs::file_delete(new_prepped_path)
fs::file_copy(prepped_data, new_prepped_path)

## C. Set up parent-child relationships ------------------------------------

births_version <- get_proc_version(
  model_name = "birth",
  model_type = "estimate",
  gbd_year = gbd_year
)

if(!(test)){
  
  parent_list <- list(
    "birth estimate" = births_version,
    "birth sex ratio data" = data_version_id
  )
  
  # if(new_version){
  srb_line <- get_proc_lineage(
    model_name = "birth sex ratio", 
    model_type = "estimate", 
    run_id = version_id
  )
  
  if(nrow(srb_line) == 0){
    mortdb::gen_parent_child(
      parent_runs = parent_list, 
      child_process = "birth sex ratio estimate", 
      child_id = version_id
    )
  }
  # }
}

## D. Save births data into intermediate directory  ------------------------

births <- get_mort_outputs(
  model_name = "birth",
  model_type = "estimate",
  run_id = births_version,
  sex_ids = 3,
  age_group_ids = 169,
  gbd_year = gbd_year
) |>
  _[, .(ihme_loc_id, location_id, year_id, births = mean)]

readr::write_csv(births, paste0(input_dir, "births.csv"))
