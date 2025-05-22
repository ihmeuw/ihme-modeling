################################################################################
## DESCRIPTION Prep and upload final estimates
## INPUTS raked final results
## OUTPUTS uploaded results in the database
## STEPS:
## - Read in configuration files
## - Read in proportion male inputs and obtain mean and upper/lower bounds
##   - convert mean and bounds to SRB space
## - Upload results

## NOTE: no estimates for region/super region/global have been produced at this
## point. This happens in terminator.
################################################################################

rm(list = ls())

# load libraries
library(assertable)
library(argparse)
library(data.table)
library(mortcore)
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
  main_dir <- "FILEPATH"
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
}
config <- config::get(
  file = fs::path(main_dir, "/srb_detailed.yml"), use_parent = FALSE)
list2env(config, .GlobalEnv)

# get loc map
loc_map <- fread(fs::path(input_dir, "loc_map.csv"))

id_cols <- c("location_id", "year_id")
val_cols <- c("mean", "upper", "lower")

# B. Read in proportion male inputs and obtain mean and upper/lower bounds------

# read in all the final estimate files (i.e. summaries) and append
file_paths <- Sys.glob(
  fs::path(output_dir, paste0("prop_male_", loc_map$location_id), ext = "csv")
)

summaries <- import_files(filenames = file_paths)

# convert to SRB space
summaries <- summaries[
  ,
  lapply(.SD, \(prop) prop/(1-prop)),
  .SDcols = val_cols, 
  by = id_cols
]

# C. Upload results -------------------------------------------------------

# save final file
readr::write_csv(summaries, paste0(output_dir, "final_output.csv"))

# upload to mortality database

if(!(test)){
  mortdb::upload_results(
    filepath = paste0(output_dir, "final_output.csv"),
    model_name = "birth sex ratio",
    model_type = "estimate",
    run_id = version_id,
    send_slack = TRUE
  )
  
  # update status
  if(best){
    mortdb::update_status(
      model_name = "birth sex ratio",
      model_type = "estimate",
      run_id = version_id,
      hostname = hostname,
      new_status = "best",
      send_slack = TRUE
    )
  }
}
