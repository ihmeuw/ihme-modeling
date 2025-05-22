################################################################################
##                                                                            ##
## Purpose: Run outliering criteria to outlier data points before             ##
##          crosswalking. This code also checks that outliered data points    ##
##          still meet criteria and should remain outliered.                  ##
##                                                                            ##
## Note: These outlier statuses are permanently saved in the database.        ##
##                                                                            ##
################################################################################

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

###############
## Get files ##
###############

locs <- fread("FILEPATH/input_locations.csv")
covariates <- fread("FILEPATH/covariates.csv")

###############
## Pull data ##
###############

data <- mortdb::get_mort_outputs(
  model_name = "stillbirth",
  model_type = "data",
  run_id = version_data,
  gbd_year = gbd_year,
  outlier_run_id = "active",
  demographic_metadata = TRUE
)

data[is.na(outlier), outlier := 0]

current_outliers <- data[outlier == 1, c("upload_stillbirth_data_id")]

#########################################
## Identify additional data to outlier ##
#########################################

## Outlier based on sbr value

data[mean < 0.001, outlier_new := 1]
data[mean < 0.001,
     outlier_reason := paste0(user, " ", date, " - mean too small (less than 0.001)")]

data[mean > 0.2, outlier_new := 1]
data[mean > 0.2,
     outlier_reason := paste0(user, " ", date, " - mean too big (greater than 0.2)")]

data[mean > 0.05 & super_region_name == "High-income", outlier_new := 1]
data[mean > 0.05 & super_region_name == "High-income" & is.na(outlier_reason),
     outlier_reason := paste0(user, " ", date, " - mean too big for High-Income super region (greater than 0.05)")]

## Outlier if definition is assigned to "other"

data[std_def_id == 9, outlier_new := 1]
data[std_def_id == 9,
     outlier_reason := paste0(user, " ", date, " - definition assigned to other")]

## Outlier based on shock years

shock <- mortdb::get_mort_outputs(
  model_name = "with shock death number",
  model_type = "estimate",
  gbd_year = gbd_year,
  run_id = parents[["with shock death number estimate"]],
  sex_ids = 3,
  location_ids = locs[, location_id],
  age_group_ids = 22
)

shock <- shock[, c("ihme_loc_id", "year_id", "mean")]
setnames(shock, "mean", "shock")

noshock <- mortdb::get_mort_outputs(
  model_name = "no shock death number",
  model_type = "estimate",
  gbd_year = gbd_year,
  run_id = parents[["no shock death number estimate"]],
  estimate_stage_ids = 5,
  sex_ids = 3,
  location_ids = locs[, location_id],
  age_group_ids = 22
)

noshock <- noshock[, c("ihme_loc_id", "year_id", "mean", "location_id")]
setnames(noshock, "mean", "nonshock")

years_to_get <- as.character(unique(noshock$year_id))
locs_to_get <- unique(noshock$location_id)

pop <- mortdb::get_mort_outputs(
  model_name = "population",
  model_type = "estimate",
  gbd_year = gbd_year,
  run_id = parents[["population estimate"]],
  sex_ids = 3,
  age_group_ids = 22
)

pop <- pop[location_id %in% locs_to_get & year_id %in% years_to_get, c("year_id", "location_id", "mean", "ihme_loc_id")]
setnames(pop, "mean", "mean_pop")

noshock <- merge(
  noshock,
  pop,
  by = c("year_id", "location_id", "ihme_loc_id"),
  all.x = TRUE
)
noshock <- noshock[, c("year_id", "nonshock", "mean_pop", "ihme_loc_id")]

shock_compiled <- merge(
  shock,
  noshock,
  by = c("year_id", "ihme_loc_id"),
  all = TRUE
)
shock_compiled[, ratio := (shock - nonshock)/mean_pop]

readr::write_csv(shock_compiled, paste0(data_dir, version_data, "/FILEPATH/shock_ratios.csv"))

shock_outliers <- shock_compiled[ratio > 0.005,]

shock_outliers_sub <- shock_outliers[!is.na(ihme_loc_id), c("year_id", "ihme_loc_id")]
shock_outliers_sub[, shock_outlier := 1]
shock_outliers_sub <- unique(shock_outliers_sub)

data <- merge(
  data,
  shock_outliers_sub,
  by = c("ihme_loc_id", "year_id"),
  all.x = TRUE
)

data[shock_outlier == 1, outlier_new := 1]
data[shock_outlier == 1 & is.na(outlier_reason),
     outlier_reason := paste0(user, " ", date, " - ratio of (shock - nonshock)/pop too big (greater than 0.005)")]
data$shock_outlier <- NULL

## Outlier if SBR:NMR ratio less than 0.5

data <- merge(data, covariates, by = c("location_id", "ihme_loc_id", "year_id"), all = T)

data[, ratio := mean/q_nn_med]
data[ratio < 0.5, outlier_new := 1]
data[ratio < 0.5 & is.na(outlier_reason),
     outlier_reason := paste0(user, " ", date, " - ratio of sbr/nmr too small (less than 0.5)")]

## Outlier if scientific literature not representative (toggle)

if (drop_non_rep_sci_lit) {

  lit_to_outlier <- fread(paste0(data_dir, version_data, "/FILEPATH/lit_to_outlier.csv"))

  lit_to_outlier[, nid_loc_yr := paste0(nid, " ", ihme_loc_id, " ", year_id)]
  data[, nid_loc_yr := paste0(nid, " ", ihme_loc_id, " ", year_id)]

  data[nid_loc_yr %in% lit_to_outlier$nid_loc_yr, outlier_new := 1]
  data[nid_loc_yr %in% lit_to_outlier$nid_loc_yr & is.na(outlier_reason),
       outlier_reason := paste0(user, " ", date, " - non-representative sci lit")]

  data[, nid_loc_yr := NULL]

}

## Cleaning

data <- data[!is.na(mean)] ## cut covariates

data[is.na(outlier_new), outlier_new := 0]

#########################################
## Check for Residual Exact Duplicates ##
#########################################

data[, loc_yr_mean := paste0(location_id, "_", year_id, "_", mean)]
data[, loc_yr_mean_def := paste0(location_id, "_", year_id, "_", mean, "_", std_def_id)]
data[, loc_yr_mean_nid := paste0(location_id, "_", year_id, "_", mean, "_", nid)]

# Function to check for dups

check_for_dups <- function (data) {

  dup_test <- as.data.table(table(data[outlier_new != 1]$loc_yr_mean))
  colnames(dup_test) <- c("loc_year_mean", "N")

  dup_test <- dup_test[N > 1,]
  dups_to_review <- data[loc_yr_mean %in% dup_test$loc_year_mean]

  # Remove rows where dups only caused by similar definitions

  for (loc in unique(dups_to_review$ihme_loc_id)) {

    test <- dups_to_review[ihme_loc_id == loc]

    for (year in unique(test$year_id)) {

      test2 <- test[year_id == year]

      defs <- toString(sort(unique(as.numeric(test2$std_def_id))))

      if (defs %in% c("1, 7", "1, 12", "7, 12", "4, 8", "4, 11", "8, 11",
                      "11, 13", "12, 14")) {

        dups_to_review <- dups_to_review[ihme_loc_id != loc]

      }

      defs <- NA

    }

  }

  return (dups_to_review)

}

# Identify duplicates

dups_to_review <- check_for_dups(data = data)

# Outlier exact duplicates based on prioritization

setorderv(dups_to_review, cols = c("location_id", "year_id", "std_def_id", "source", "source_type_id"),
          order = c(1, 1, 1, 1, 1), na.last = T)

dups_to_review[, rank := seq(.N), by = c("location_id", "year_id", "std_def_id")]

data[loc_yr_mean_nid %in% dups_to_review[rank > 1]$loc_yr_mean_nid, outlier_new := 1]
data[loc_yr_mean_nid %in% dups_to_review[rank > 1]$loc_yr_mean_nid,
     outlier_reason := paste0(user, " ", date, " - duplicate extraction (removed based on prioritization)")]

# Re-run check

dups_to_review2 <- check_for_dups(data = data)
dup_test <- as.data.table(table(dups_to_review2$loc_yr_mean_def))
colnames(dup_test) <- c("loc_year_mean", "N")

dup_test <- dup_test[N > 1,]

if (nrow(dup_test) > 0) readr::write_csv(dups_to_review2, paste0(data_dir, version_data, "/FILEPATH/dups_to_review.csv"))
if (nrow(dup_test) > 0) stop ("There are location-years with duplicate mean values.")

##########################################
## Prep for unoutliering and outliering ##
##########################################

data[, outlier_comment_sub := sub(".* - ", "", outlier_comment)]
data[, outlier_reason_sub := sub(".* - ", "", outlier_reason)]

data_to_review <- copy(data)

# Drop data points

data_to_review <- data_to_review[!(outlier == 0 & outlier_new == 0)]
data_to_review <- data_to_review[!(outlier == 1 & outlier_new == 1 & outlier_comment_sub == outlier_reason_sub)]

# Identify outliers where outlier reason should be updated

data_update_reason <- data_to_review[outlier == 1 & outlier_new == 1 & outlier_comment_sub != outlier_reason_sub]

# Identify outliers which should be unoutliered

data_to_unoutlier <- data_to_review[outlier == 1 & outlier_new == 0 & !(outlier_comment_sub %like% "manual")]

data_to_unoutlier <- rbind(data_to_unoutlier, data_update_reason)

# Identify data which needs to be outliered

data_to_outlier <- data_to_review[outlier == 0 & outlier_new == 1]

data_to_outlier <- rbind(data_to_outlier, data_update_reason)

# Output file with outlier changes

data_changes <- rbind(data_to_unoutlier, data_to_outlier)

readr::write_csv(
  data_changes,
  paste0(data_dir, version_data, "/FILEPATH/outlier_changes.csv")
)

##########################################
## Unoutlier data points (if necessary) ##
##########################################

reason <- paste0(user, " ", date, " - mean no longer breaks outlier criteria")

data_ids <- data_to_unoutlier$upload_stillbirth_data_id

if (length(data_ids) > 0) {

  mortdb::toggle_outlier(
    model_name = "stillbirth",
    data_ids = data_ids,
    new_outlier_value = 0,
    comment = reason,
    hostname = hostname
  )

}

########################################
## Outlier data points (if necessary) ##
########################################

for (reason in unique(data_to_outlier$outlier_reason)) {

  new_outliers <- data_to_outlier[outlier_reason == reason, c("upload_stillbirth_data_id")]

  data_ids <- new_outliers$upload_stillbirth_data_id

  if (nrow(new_outliers) > 0) {

    mortdb::toggle_outlier(
      model_name = "stillbirth",
      data_ids = data_ids,
      new_outlier_value = 1,
      comment = reason,
      hostname = hostname
    )

  }

}
