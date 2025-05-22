################################################################
##                                                            ##
## Purpose: Run outliering criteria to check for additional   ##
##          data points that were adjusted to values outside  ##
##          of criteria during crosswalking.                  ##
##                                                            ##
## Note: These outliers will be saved in a file instead of    ##
##       the database because they may change when outliering ##
##       changes                                              ##
##                                                            ##
################################################################

rm(list = ls())
Sys.umask(mode = "0002")

library(argparse)
library(assertable)
library(data.table)

library(mortcore)
library(mortdb)

user <- "USERNAME"
date <- Sys.Date()

root <- "FILEPATH"

## GET ARGUMENTS AND SETTINGS

args <- commandArgs(trailingOnly = TRUE)
settings_dir <- args[1]

if (interactive()) {
  version_data <- "Run id"
  settings_dir <- "FILEPATH/run_settings.csv"
}

load(settings_dir)
list2env(settings, envir = environment())

if (model == "SBR") crosswalk_dir <- "FILEPATH"
if (model == "SBR/NMR") crosswalk_dir <- "FILEPATH"
if (model == "SBR + NMR") crosswalk_dir <- "FILEPATH"

###############
## Get files ##
###############

locs <- fread(paste0(data_dir, version_data, "/FILEPATH/input_locations.csv"))
covariates <- fread(paste0(data_dir, version_data, "/FILEPATH/covariates.csv"))

###############
## Pull data ##
###############

data <- data.table()

for (i in 1:length(defs)) {

  crosswalk_data <- fread(paste0(crosswalk_dir, defs[i], "_weeks/crosswalk_", defs[i], "_weeks.csv"))
  crosswalk_data[, main_std_def := paste0(defs[i], "_weeks")]

  data <- rbind(data, crosswalk_data)

}

data <- merge(
  data,
  locs[, c("ihme_loc_id", "super_region_name")],
  by = "ihme_loc_id",
  all.x = TRUE
)

data <- merge(
  data,
  covariates,
  by = c("ihme_loc_id", "year_id"),
  all.x = TRUE
)

#####################
## Recalculate SBR ##
#####################

if (model == "SBR") data[, sbr_adj := mean_adj]
if (model == "SBR/NMR") data[, sbr_adj := mean_adj * q_nn_med]
if (model == "SBR + NMR") data[, sbr_adj := mean_adj - q_nn_med]

#########################################
## Identify additional data to outlier ##
#########################################

## Outlier if not in location hierarchy

data[!(ihme_loc_id %in% locs$ihme_loc_id), outlier := 1]
data[
  !(ihme_loc_id %in% locs$ihme_loc_id),
  outlier_reason := paste0(user, " ", date, " - location is not in the location hierarchy for modeling")
]

## Outlier based on sbr value

data[sbr_adj < 0.001, outlier := 1]
data[
  sbr_adj < 0.001,
  outlier_reason := paste0(user, " ", date, " - mean too small (less than 0.001)")
]

data[sbr_adj > 0.2, outlier := 1]
data[
  sbr_adj > 0.2,
  outlier_reason := paste0(user, " ", date, " - mean too big (greater than 0.2)")
]

data[sbr_adj > 0.05 & super_region_name == "High-income", outlier := 1]
data[
  sbr_adj > 0.05 & super_region_name == "High-income" & is.na(outlier_reason),
  outlier_reason := paste0(user, " ", date, " - mean too big for High-Income super region (greater than 0.05)")
]

## Outlier if SBR:NMR ratio less than 0.5

data[, ratio_adj := sbr_adj/q_nn_med]
data[ratio_adj < 0.5, outlier := 1]
data[
  ratio_adj < 0.5 & is.na(outlier_reason),
  outlier_reason := paste0(user, " ", date, " - ratio of sbr/nmr too small (less than 0.5)")
]

data <- data[!is.na(mean)] ## cut covariates

########################################################
## Ensure outliers are outliered for both definitions ##
########################################################

data[, loc_yr_nid_def_mean := paste0(ihme_loc_id, " ", year_id, " ", nid, " ", std_def_short, " ", mean)]

outliers <- data[outlier == 1]

data[loc_yr_nid_def_mean %in% outliers$loc_yr_nid_def_mean & is.na(outlier), outlier := 2]
data[
  outlier == 2,
  outlier_reason := paste0(user, " ", date, " - data point outliered in the other definition's dataset")
]

####################
## Prioritization ##
####################

data_outliered <- data[outlier %in% 1:2]

data_prioritize_yes <- data[!(outlier %in% 1:2) & source_type_id %in% c(1, 35)] # includes reports and vr
data_prioritize_no <- data[!(outlier %in% 1:2) & source_type_id %in% c(5, 57, 58)] # includes censuses, surveys, and sci lit

data_prioritize_yes <- demInternal::merge_ghdx_record_fields(data_prioritize_yes)

data_prioritize_yes[
  ihme_loc_id %like% "BRA" & source == "Custom" & title %like% "Brazil Civil Registry Statistics",
  source := "Custom - Brazil CRS"
]
data_prioritize_yes[
  ihme_loc_id %like% "BRA" & source == "Custom" & title %like% "Brazil Mortality Information System",
  source := "Custom - Brazil MIS"
]

data_prioritize_yes[, title := NULL]

data_prioritize_yes[,
  source := factor(
    source,
    levels = c(
      "Custom - Brazil MIS", "Custom - Brazil CRS", "Custom", "DYB",
      "WHO_HFA", "LSEIG"
    )
  )
]

setorderv(
  data_prioritize_yes,
  cols = c("main_std_def", "location_id", "year_id", "std_def_short", "source", "source_type_id"),
  order = c(1, 1, 1, 1, 1, 1),
  na.last = TRUE
)

data_prioritize_yes[, rank := seq(.N), by = c("main_std_def", "location_id", "year_id", "std_def_short")]

data_prioritize_yes[rank > 1, outlier := 3]
data_prioritize_yes[
  outlier == 3,
  outlier_reason := paste0(user, " ", date, " - higher prioritized data available")
]

data_prioritize_yes[source %like% "Custom", source := "Custom"]

data <- rbind(
  data_prioritize_yes,
  data_prioritize_no,
  data_outliered,
  fill = TRUE
)

################
## Save files ##
################

for (i in 1:length(defs)) {

  crosswalk_data_adj <- data[main_std_def == paste0(defs[i], "_weeks")]

  readr::write_csv(
    crosswalk_data_adj[outlier %in% 1:3],
    paste0(crosswalk_dir, "FILEPATH/crosswalk_", defs[i], "_weeks_outliers.csv"),
    na = ""
  )

  readr::write_csv(
    crosswalk_data_adj[!(outlier %in% 1:3)],
    paste0(crosswalk_dir, "FILEPATH/crosswalk_", defs[i], "_weeks_adj.csv"),
    na = ""
  )

}
