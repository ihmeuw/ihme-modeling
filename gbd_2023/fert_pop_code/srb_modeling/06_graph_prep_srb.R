################################################################################
## DESCRIPTION Combined and prep data for graphing
## INPUTS prepped data, final estimates from upload, previous best, and WPP estimates
## OUTPUTS combined data for graphing
## STEPS
## - Read in configuration files
## - Get Comparisons
##   - input previous data (with `compare_version_id`)
##     - if add_compare == T, include gpr data from `compare_version_id`
##   - add WPP comparison
## - Get Current Estimates
##   -	read in stage 1 and 2 estimates and convert mean to SRB space
## - Combine all files to prepare graphing data
##   -	append first and second stage estimates

# NOTE: If there is anything weird in WPP check comparators repo
################################################################################

rm(list = ls())

library(argparse)
library(assertable)
library(data.table)
library(mortcore)
library(mortdb)
library(stringr)

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
  file = fs::path(main_dir, "/srb_detailed.yml"), use_parent = FALSE
)
list2env(config, .GlobalEnv)

# get old loc map
loc_map <- fread(fs::path(input_dir, "loc_map.csv"))

# B. Get Comparisons -----------------------------------------------------------

# get previous best
prev_data <- get_mort_outputs(
  model_name = "birth sex ratio",
  model_type = "estimate",
  run_id = compare_version_id,
  gbd_year = gbd_year
) |> 
  _[, -c("upload_birth_sex_ratio_estimate_id")]

# reformat: only keep estimated locations
prev_data <- prev_data[location_id %in% loc_map$location_id]

# get WPP comparison
prev_data <- fread(fs::path(wpp_dir, "SRB.csv"))

# reformat
setnames(wpp_data, "country_ihme_loc_id", "ihme_loc_id")
wpp_data[loc_map, location_id := i.location_id, on = "ihme_loc_id"]
wpp_data <- wpp_data[
  !is.na(location_id), 
  .(ihme_loc_id, year_id = date_start, mean = value, location_id)
]

# get optional additional comparator
if(add_compare == TRUE){
  # get final results from comparator version
  compare_data <- get_mort_outputs(
    model_name = "birth sex ratio",
    model_type = "estimate",
    run_id = compare_version_id, 
    gbd_year = gbd_year
  ) |> 
    _[, upload_birth_sex_ratio_estimate_id := NULL]
  
  # reformat
  compare_data[, version := paste0("Comp run_id:", compare_version_id)]
  
  ## get space-time estimates from additional comparator
  compr_intermit_data <- fread(paste0(
    str_replace(input_dir, "\\d+", as.character(compare_version_id)),
    "gpr_input.csv"
  ))
  
  setnames(compr_intermit_data, "year", "year_id")
  
  # convert prepped data from proportion male to srb space
  compr_intermit_data[, data := data / (1 - data)]
  
  # subset to second stage predictions
  stage2 <- compr_intermit_data[, .(
    ihme_loc_id,
    location_id,
    year_id,
    val_preds2
  )]

  # # format first and second stage
  stage2 <- unique(stage2)
  stage2 <- melt(
    stage2,
    id.vars = c("ihme_loc_id", "location_id", "year_id"),
    measure.vars = c("val_preds2"),
    variable.name = "version",
    value.name = "mean"
  )

  stage2[
    version == "val_preds2",
    version := paste0("ST run_id:", compare_version_id)
  ]

    # convert first and second stage val to SRB
  stage2[, mean := mean/(1-mean)]
  
  # check merge
  assert_values(stage2, "mean",)
}

# C. get current estimate --------------------------------------------------------------------

# # load final output from gpr/raking
current_data <- get_mort_outputs(
  model_name = "birth sex ratio",
  model_type = "estimate",
  run_id = version_id
) |> 
  _[, upload_birth_sex_ratio_estimate_id := NULL]


## not sure what changed but ihme_loc_id already in current_data
current_data[loc_map, ihme_loc_id := i.ihme_loc_id, on = "location_id"]

# get first/second stage predictions
intermit_data <- fread(paste0(input_dir, "gpr_input.csv")) |> 
  setnames("year", "year_id")

# convert prepped data from proportion male to srb space
intermit_data[, data := data/(1-data) ]

# subset to first and second stage predictions
stage12 <- unique(intermit_data[
  , 
  .(ihme_loc_id, location_id, year_id, val_preds1, val_preds2)
])

# format first and second stage
stage12 <- melt(
  stage12, 
  id.vars = c("ihme_loc_id", "location_id", "year_id"),
  measure = patterns("val"),
  variable.name = "version",
  value.name = "mean"
)

stage12[, version := ifelse(grepl("1", version), "1st", "2nd")]
# convert first and second stage val to SRB
stage12[, `:=` (
  version = paste(version, "stage pred"),
  mean = mean/(1-mean)
)]

# subset to prepped data
intermit_data <- intermit_data[
  ,
  .(
    ihme_loc_id, location_id, year_id, outlier, data, data_density, category,
    nid
  )
]

# merge with current estimates
current_data <- merge(
  current_data,
  intermit_data, 
  by = c("ihme_loc_id", "location_id", "year_id"), 
  all = TRUE
)

# merge prepped_data metadata
prepped_data <- fread(fs::path(
  input_dir, "prepped_data_srb_outlier_adjusted.csv")
) |> 
  _[, .(location_id, year_id, nid, outlier, source)]

current_data <- merge(
  current_data,
  prepped_data,
  by = c("location_id", "year_id", "nid", "outlier"),
  all.x = TRUE
)

# D. combine all files to prepare graphing data---------------------------------

# combine WPP, previous, and current versions
version_names <- c("Current", "Previous", "WPP")
combined <- rbind(
  current_data, prev_data, wpp_data, fill = TRUE, idcol = "version"
) 
combined[, version := factor(version, levels = 1:3, labels = version_names)]
combined[, version := as.character(version)]

# append optional comparison version
if(add_compare == TRUE){
  combined <- rbind(combined, compare_data, fill = TRUE)
}

# append first and second stage estimates
combined <- rbind(combined, stage12, fill = TRUE)

# check `version`
assert_values(combined, "version", test = "not_na")

# output file
readr::write_csv(combined, fs::path(output_dir, "graphing_data.csv"))

