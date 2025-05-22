###################################################################################################
##
## Purpose: Stillbirths Model for GBD 2023
##
## Model Specifications:
##      - Predicting: *log(SBR/NMR)*, log(SBR + NMR), or log(SBR)
##          - Model run twice: 22 weeks gestation and 28 weeks gestation
##
## Steps:
##      1. Pull in and prep data for modeling
##
##      2. Call in hyperparameters and update as necessary
##
##      3a. Run stage 1 model
##            - Overview
##                 - Fixed effects on: maternal education, standard definition (dummy), data type (dummy)
##                 - Random effects on: smoothed neonatal bins, location, location-source
##                 - Reference for prediction:
##                     - complete vr
##                     - 22 weeks or 28 weeks (depending on what definition is set)
##            - Create neonatal bins and smooth them for a RE
##                -> Order observations by q_nn, split into 20 bins with equal number of observations
##            - Implements standard locations
##                 1. Run stage 1 mixed effects model for standard locations only
##                 2. Take predicted fixed effect coefficients from the mixed effect model and apply to all location-year-sex combinations
##                 3. Subtract FE predictions from all stillbirth data values to generate FE-residuals
##                 4. Run a random effect only model on FE-residuals
##                 5. Add RE predictions to FE predictions to get overall model predictions
##            - Performs data adjustment based on reference definition and source type (vr_complete set as reference)
##                -> In GBD 2016 - raw data adjusted using SB definition-specific scalars taken from LSEIG paper,
##                -> LSEIG = Lancet Stillbirth Epidemiology Investigator Group led by Joy Lawn and Hannah Blencowe
##
##      3b. Use results from crosswalking and stage 1 ensemble model
##            - Adjusted data read in from crosswalking output
##            - Stage 1 predictions read in from stage 1 ensemble model output
##
##      4. Space-Time
##            - Run space time function
##            - Calculate residual and add to stage 1 prediction
##            - Create mean squared error (mse)
##                -> Calculated as the mean of the variance of the difference between the space-time and the first stage regression for
##                   national locations (updated in GBD 2016)
##            - Use median absolute deviation (MAD) to calculate non-sampling variance to input into GPR
##
##      5. Run GPR (code 03)
##
## Code Inputs:
##      - Data from database OR crosswalking/stage 1 ensemble model files
##      - Covariates file
##      - Hyperparameters file
##
## Code Outputs:
##      - Standard locations list
##      - Prediction dataset
##      - Final dataset
##      - Hyperparameters file (overwrites output from calculate_hyperparameters to account for manual changes)
##      - First stage model, stage 1 betas, stage 1 prediction
##      - Input data for space time
##      - Input data for GPR
##
###################################################################################################

rm(list = ls())
Sys.umask(mode = "0002")

library(argparse)
library(assertable)
library(data.table)
library(foreign)
library(haven)
library(lme4)
library(MASS)

library(plyr)
library(dplyr)

library(mortcore)
library(mortdb)

set.seed(145153)

user <- "USERNAME"

# Get settings
args <- commandArgs(trailingOnly = TRUE)
new_settings_dir <- args[1]

if (interactive()) {
  version_estimate <- "Run id"
  main_std_def <- "28_weeks"
  new_settings_dir <- paste0("FILEPATH/new_run_settings_", main_std_def, ".csv")
}

load(new_settings_dir)
list2env(new_settings, envir = environment())

source(paste0(working_dir, "calculate_hyperparameters.R"))
source(paste0(working_dir, "helper_functions.R"))
source(paste0(working_dir, "space_time.R"))
source(paste0(working_dir, "FILEPATH/transformation_functions.R"))

############################
## Hyper-parameter Options ##
############################

use_5q0_density_parameters <- FALSE
use_fertility_density_parameters <- TRUE

##########################################
## Pull Locations and Related Variables ##
##########################################

## Get all locations, including super-regions
all_locs <- mortdb::get_locations(level = "all", gbd_year = gbd_year)
all_locs <- all_locs[, list(ihme_loc_id, super_region_name)]

## Get official location names at the estimate level with parent/child info
locs <- mortdb::get_locations(level = "estimate", gbd_year = gbd_year)
locs_sub <- locs[, list(ihme_loc_id, location_name, location_id, region_name)]

## Get standard locations
national_parents <- locs[level == 4, parent_id]
standard_locs <- unique(get_locations(gbd_type = "standard_modeling", level = "all", gbd_year = gbd_year)$location_id)
st_locs <- locs[, standard := as.numeric(location_id %in% c(standard_locs, national_parents, 44533))]

readr::write_csv(st_locs, paste0(estimate_dir, version_estimate, "/FILEPATH/standard_locs_", main_std_def, ".csv"))

## Get fake regions for space time
st_regs <- mortdb::get_spacetime_loc_hierarchy(
  prk_own_region = FALSE,
  old_ap = FALSE,
  gbd_year = gbd_year
)

#########################
## Data and Covariates ##
#########################

## Pull standard definition
std_defs <- mortdb::get_mort_ids(type = "std_def")
std_defs <- std_defs[, c("std_def_id", "std_def_short")]
setnames(std_defs, "std_def_short", "std_def")

if (use_crosswalk_stage1ensemble) {

  if (model == "SBR") crosswalk_dir <- "FILEPATH"
  if (model == "SBR/NMR") crosswalk_dir <- "FILEPATH"
  if (model == "SBR + NMR") crosswalk_dir <- "FILEPATH"

  ## Read in crosswalked data
  crosswalk_data <- fread(paste0(crosswalk_dir, "/crosswalk_", main_std_def, "_adj.csv"))

  ## Add standard definition
  setnames(crosswalk_data, "std_def_short", "std_def")
  crosswalk_data <- merge(
    crosswalk_data,
    std_defs,
    by = "std_def",
    all.x = TRUE
  )

  ## Get log of mean
  crosswalk_data[, log_mean := log(mean)]
  crosswalk_data[, log_mean_adj := log(mean_adj)]

  crosswalk_data <- crosswalk_data[year_id >= year_start, c("ihme_loc_id", "year_id", "nid", "source_type_id",
                                                            "std_def_id", "std_def", "log_mean", "log_mean_adj")]

  data <- copy(crosswalk_data)

} else {

  ## Read in data
  data <- mortdb::get_mort_outputs(
    model_name = "stillbirth",
    model_type = "data",
    run_id = version_data,
    hostname = hostname,
    outlier_run_id = "active",
    gbd_year = gbd_year,
    demographic_metadata = TRUE
  )

  data[is.na(outlier), outlier := 0]

  if (nrow(data) == 0) {
    data <- fread(paste0(data_dir, version_data, "/FILEPATH/stillbirth_data_database.csv"))
    data <- merge(
      data,
      locs_sub,
      by = "location_id",
      all.x = TRUE
    )
    data[, run_id := version_data]
  }

  setnames(data, "mean", "sbr")

  ## Add standard definition
  data <- merge(
    data,
    std_defs,
    by = "std_def_id",
    all.x = TRUE
  )

  ## Remove outliers
  data_no_outliers <- data[outlier != 1]

  data_no_outliers <- data_no_outliers[year_id >= year_start, c("ihme_loc_id", "year_id", "nid", "source_type_id", "std_def_id", "std_def", "sbr")]

  data <- copy(data_no_outliers)

}

## Add data types
source_types <- mortdb::get_mort_ids(type = "source_type")[, c("source_type_id", "type_short")]

data <- merge(
  data,
  source_types,
  by = "source_type_id",
  all.x = TRUE
)
setnames(data, "type_short", "source_type")

data[source_type == "Standard DHS", source_type := "Survey"]

data[source_type == "Census", data_type := "census"]
data[source_type == "Statistical Report", data_type := "gov_report"]
data[source_type == "Sci Lit", data_type := "literature"]
data[source_type == "Survey", data_type := "survey"]
data[source_type == "VR", data_type := "vr"]

## Merge on covariates
covariates <- fread(paste0(data_dir, version_data, "/FILEPATH/covariates.csv"))
data <- merge(
  data,
  covariates[year_id >= year_start],
  by = c("ihme_loc_id", "year_id"),
  all = TRUE
)

## Merge on superregion column
data <- merge(
  data,
  all_locs,
  by = "ihme_loc_id",
  all.x = TRUE
)

## Additional data prep
if (!use_crosswalk_stage1ensemble) {

  ## Create ratio/dependent variable
  data[, log_mean := sbr_to_lograt(sbr, q_nn_med)]

  ## Create bins based on neonatal mortality
  bin_type <- "nn"
  if (bin_type == "5q0") {
    data <- data[order(q_u5_med),]
  } else {
    data <- data[order(q_nn_med),]
  }

  total <- nrow(data)
  data[, index := 1:total]
  data[, bin := 500]

  for (m in 1:20) {
    data[index >= total * ((m - 1)/20) & index <= total * (m/20), bin := m]
  }

  data[, bin := as.factor(bin)]

  ## Set standard definitions and standard data_type
  data[, std_def := factor(
    std_def,
    levels = c(
      main_std_def, other_std_def,
      "1000_grams", "20_weeks", "24_weeks", "26_weeks", "500_grams", "other",
      "22_weeks_OR_500_grams","22_weeks_AND_500_grams",
      "28_weeks_OR_1000_grams", "28_weeks_AND_1000_grams"
    ),
    ordered = FALSE
  )]

  data[, data_type := factor(data_type, levels = c("vr", "survey", "census", "literature", "gov_report"), ordered = FALSE)]

  if (nrow(data[!is.na(log_mean) & is.na(std_def),]) > 0) stop("missing standard definition")
  if (nrow(data[!is.na(log_mean) & is.na(data_type),]) > 0) stop("missing data type")

  data[, loc_source := paste0(ihme_loc_id, "_", data_type)]

}

readr::write_csv(
  data,
  paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirth_data_final_", main_std_def, ".csv")
)

#########################################
## Create Prediction Dataset (square) ##
########################################

pred <- fread(paste0(data_dir, version_data, "/FILEPATH/covariates.csv"))

if (nrow(pred) != length(unique(pred$ihme_loc_id))*length(unique(pred$year_id))) stop("pred dataset not square")

pred[, year := year_id + .5]
pred[, data_type := "vr"]
pred[, std_def := main_std_def]

pred <- pred[order(ihme_loc_id, year_id),]

readr::write_csv(
  pred,
  paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirth_prediction_data_", main_std_def, ".csv")
)

###############################
## Calculate Hyper-parameters ##
###############################

## Custom function to calculate hyper-parameters and data density (AG 2017)
calculate_hyperparameters(
  data,
  use_5q0_density_parameters = use_5q0_density_parameters,
  use_fertility_density_parameters = use_fertility_density_parameters,
  gbd_year = gbd_year,
  release_id = release_id,
  test_lambda = test_lambda
)

spacetime_parameters <- fread(paste0(estimate_dir, version_estimate, "/FILEPATH/hyperparameters.csv"))

## Add in custom hyperparameters
spacetime_parameters[ihme_loc_id == "ARM", lambda := 0.4]
spacetime_parameters[ihme_loc_id == "BGR", lambda := 0.4]
spacetime_parameters[ihme_loc_id == "BWA", lambda := 0.4]
spacetime_parameters[ihme_loc_id == "COL", lambda := 0.4]
spacetime_parameters[ihme_loc_id == "ISL", lambda := 0.4]
spacetime_parameters[ihme_loc_id == "KWT", lambda := 0.4]
spacetime_parameters[ihme_loc_id == "LUX", lambda := 0.4]
spacetime_parameters[ihme_loc_id == "MNP", lambda := 0.4]
spacetime_parameters[ihme_loc_id == "MUS", lambda := 0.4]
spacetime_parameters[ihme_loc_id %like% "NOR", lambda := 0.4]
spacetime_parameters[ihme_loc_id %like% "NZL", lambda := 0.4]
spacetime_parameters[ihme_loc_id == "PHL_53533", lambda := 0.4]

spacetime_parameters[ihme_loc_id == "AND", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "ASM", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "FRA", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "GUM", lambda := 0.6]
spacetime_parameters[ihme_loc_id %in% c("IDN_4718", "IDN_4732", "IDN_4734"), lambda := 0.6]
spacetime_parameters[ihme_loc_id == "JOR", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "LCA", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "MUS", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "PHL", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "PHL_53581", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "RWA", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "SEN", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "TTO", lambda := 0.6]
spacetime_parameters[ihme_loc_id == "VCT", lambda := 0.6]
spacetime_parameters[ihme_loc_id %like% "ZAF", lambda := 0.6]

## Save updated file
readr::write_csv(spacetime_parameters, paste0(estimate_dir, version_estimate, "/FILEPATH/updated_hyperparameters.csv"))

###################
## Stage 1 Model ##
###################

if (use_crosswalk_stage1ensemble) {

  # Read in stage 1 ensemble model results
  main_std_def_clean <- gsub("_", " ", main_std_def)
  stage_1_dir <- paste0("FILEPATH/Stillbirth >", main_std_def_clean)

  stage_1_ensemble <- get_recent(folder = stage_1_dir, pattern = "custom_prior")

  pred_data <- merge(
    data,
    stage_1_ensemble[location_id != 6],
    by = c("location_id", "year_id"),
    all.x = TRUE
  )
  setnames(pred_data, "cv_custom_stage_1", "pred_log_mean")

  data <- copy(pred_data)

} else {

  ## Step 1. Run stage 1 mixed effects model for standard locations only

  data_backup <- copy(data)

  data <- merge(
    data_backup,
    st_locs[, c("ihme_loc_id", "standard")],
    by = "ihme_loc_id",
    all.x = TRUE
  )

  st_loc_data <- data[standard == 1, ]

  form <- as.formula("log_mean ~ std_def + data_type + maternal_educ_yrs_pc + (1|bin) + (1|ihme_loc_id) + (1|loc_source)")

  print("Current Model Used", font = 2)
  paste(form)

  mod <- lme4::lmer(formula = form, data = st_loc_data)
  summary(mod)

  save(mod, file = paste0(estimate_dir, version_estimate, "/model/stage_1_standard_locations_", main_std_def, ".rdata"))

  ## Step 2. Take predicted fixed effect coefficients from the mixed effect model and apply to all location-year-sex combinations

  betas <- as.data.frame(fixef(mod))
  randoms <- as.data.table(ranef(mod))

  # merge random effects for loc-source
  data_types <- unique(data$data_type)
  data_types <- paste0("_", data_types)
  randoms <- randoms[grpvar == "loc_source"]

  for (dts in data_types) {
    randoms[grepl(dts, grp), ihme_loc_id := gsub(dts, "", grp)]
  }

  randoms[, data_type := gsub(paste0(ihme_loc_id, "_"), "", grp), by = condval]
  randoms <- randoms[, .(ihme_loc_id, data_type, condval)]

  rand_ref <- randoms[data_type == "vr_complete"]
  rand_all <- copy(randoms)
  setnames(rand_all, "condval", "lc_re")
  setnames(rand_ref, c("data_type", "condval"), c("ref_data_type", "ref_lc_re"))

  randoms <- merge(
    rand_ref,
    rand_all,
    all = TRUE
  )

  # output fixed effects
  betas_to_save <- copy(betas)
  betas_to_save <- data.table(betas_to_save, keep.rownames = TRUE)
  setnames(betas_to_save, "rn", "variable")
  readr::write_csv(
    betas_to_save,
    paste0(estimate_dir, version_estimate, "/FILEPATH/stage_1_beta_", main_std_def, ".csv")
  )

  # merge fixed effects
  fixed_effect_intercept <- betas[rownames(betas) == "(Intercept)",]
  fixed_effect_maternal_edu <- betas[rownames(betas) == "maternal_educ_yrs_pc",]

  data[, fixed_effect_intercept := fixed_effect_intercept]
  data[, fixed_effect_maternal_edu := fixed_effect_maternal_edu]

  betas <- tibble::rownames_to_column(betas, "betas_name")
  betas <- as.data.table(betas)

  std_def_betas <- betas[betas_name %like% "std_def"]
  std_def_betas[, betas_name := substring(betas_name, 8)]
  colnames(std_def_betas) <- c("std_def", "fixed_effect_std_def")

  # merge fixed effect data
  data <- merge(
    data,
    std_def_betas,
    by = "std_def",
    all.x = TRUE
  )

  data_type_betas <- betas[betas_name %like% "data_type"]
  data_type_betas[, betas_name := substring(betas_name, 10)]
  colnames(data_type_betas) <- c("data_type", "fixed_effect_data_type")

  data <- merge(
    data,
    data_type_betas,
    by = "data_type",
    all.x = TRUE
  )

  data[is.na(fixed_effect_data_type), fixed_effect_data_type := 0]
  data[is.na(fixed_effect_std_def), fixed_effect_std_def := 0]

  # merge random effect data
  data <- merge(
    data,
    randoms,
    by = c("ihme_loc_id", "data_type"),
    all = TRUE
  )

  ## Step 3. Subtract FE predictions from all stillbirth data values to generate FE-residuals

  # first, adjust data based on estimated std_def and source type differences
  # in addition, adjust by net loc_source random effect if loc has reference data_type i.e. survey
  data[, log_mean_adj := log_mean - fixed_effect_std_def - fixed_effect_data_type]

  # NOTE: can only apply this adjustment to standard locations with reference source
  data[!is.na(ref_lc_re) & (ihme_loc_id %in% unique(st_loc_data$ihme_loc_id)), log_mean_adj := log_mean_adj - lc_re + ref_lc_re]

  # calculate preds and FE-residuals
  data[, pred_log_mean_fe := (fixed_effect_maternal_edu * maternal_educ_yrs_pc) + fixed_effect_intercept]

  data[, fe_resid := log_mean_adj - pred_log_mean_fe]
  if (nrow(data[!is.na(nid) & is.na(fe_resid), ]) != 0) stop("NAs present in fe_resid")

  # Predict w/o random effects or source-specific fixed effects
  readr::write_csv(
    data,
    paste0(estimate_dir, version_estimate, "/FILEPATH/stage_1_model_re_data_", main_std_def, ".csv")
  )

  ## Step 4. Run a random effect only model on FE-residuals

  # Create model input
  stage_1_re_models <- lmer(
    fe_resid ~ 0 + (1|ihme_loc_id) + (1|ihme_loc_id:loc_source),
    data = data,
    REML = FALSE
  )
  summary(stage_1_re_models)

  save(
    stage_1_re_models,
    file = paste0(estimate_dir, version_estimate, "/FILEPATH/stage_1_model_re_", main_std_def, ".rdata")
  )

  re_coef <- ranef(stage_1_re_models)

  # Merge ihme_loc_id random effects into data
  betas.re <- as.data.frame(re_coef[["ihme_loc_id"]])
  colnames(betas.re) <- c("loc_re")

  betas.re$ihme_loc_id = rownames(betas.re)

  data <- merge(
    data,
    betas.re,
    by = "ihme_loc_id",
    all.x = TRUE
  )

  # Merge ihme_loc_id:loc_source random effects into data

  betas.re2 <- as.data.frame(re_coef[["ihme_loc_id:loc_source"]])
  betas.re2 <- tibble::rownames_to_column(betas.re2, "betas_name")
  colnames(betas.re2) <- c("loc_source", "loc_source_re")

  betas.re2 <- as.data.table(betas.re2)
  betas.re2[, loc_source := gsub(".*:", "", loc_source)]

  data <- merge(
    data,
    betas.re2,
    by = "loc_source",
    all.x = TRUE
  )

  data[is.na(loc_re), loc_re := 0]
  data[is.na(loc_source_re), loc_source_re := 0]

  # Get data back in order
  data <- as.data.frame(data)
  data <- data[order(data$ihme_loc_id, data$year),]
  readr::write_csv(
    data,
    paste0(estimate_dir, version_estimate, "/FILEPATH/stage_1_raw_", main_std_def, ".csv"),
    na = ""
  )

  ## Step 5. Add RE predictions to FE predictions to get overall model predictions

  data <- as.data.table(data)

  # Convert to other forms

  data[, pred_log_mean := pred_log_mean_fe]
  data[, pred_mean := exp(pred_log_mean)]
  data[, pred_sbr := pred_mean * q_nn_med]

  data[, mean_adj := exp(log_mean_adj)]
  data[, sbr_adj := ratio_adj * q_nn_med]

}

if (nrow(data[is.na(pred_log_mean), ]) > 0) stop("NAs present in pred_log_mean")

# Get residuals
data[, residual_covariate_model := pred_log_mean - log_mean_adj]

dat <- ddply(
  data, .(ihme_loc_id, year_id),
  function(x) {
    data.frame(
      ihme_loc_id = x$ihme_loc_id[1],
      year_id = x$year_id[1],
      residual = mean(x$residual_covariate_model))
})

dat <- as.data.table(dat)
data <- merge(
  data,
  dat,
  by = c("ihme_loc_id", "year_id"),
  all = TRUE
)

# Get final predictions

pred_data <- copy(data)
pred_data[, data_type2 := data_type]
pred_data[!is.na(nid) & !grepl("vr", pred_data$data_type), data_type2 := paste0(data_type, nid)]

readr::write_csv(
  pred_data,
  paste0(estimate_dir, version_estimate, "/FILEPATH/stage_1_prediction_", main_std_def, ".csv"),
  na = ""
)

################
## Space-Time ##
################

st_data <- ddply(
  pred_data, .(ihme_loc_id, year_id),
  function(x) {
    data.frame(
      ihme_loc_id = x$ihme_loc_id[1],
      year = x$year[1],
      resid = mean(x$residual))
})

st_data <- as.data.table(st_data)
st_data <- st_data[!is.na(year_id)]
st_data[, year := floor(year_id) + 0.5]

## add missing years for space time
year_start_st <- year_start + 0.5
year_end_st <- year_end + 0.5
years <- as.data.table(seq(year_start_st, year_end_st, 1))

## merge synthetic regions for space time
st_regs <- st_regs[, .(ihme_loc_id, region_name, keep)]

st_regs_new <- data.frame(
  year = as.numeric(character()),
  ihme_loc_id = character()
)

for (i in 1:nrow(st_regs)) {
  locyr <- years[, ihme_loc_id := st_regs$ihme_loc_id[i]]
  colnames(locyr) <- c("year", "ihme_loc_id")
  st_regs_new <- rbind(st_regs_new, locyr, fill = TRUE)
}

st_regs_new <- merge(
  st_regs,
  st_regs_new,
  by = c("ihme_loc_id"),
  allow.cartesian = TRUE
)
st_regs_new <- unique(st_regs_new)

st_final <- unique(merge(
  st_data,
  st_regs_new,
  by = c("ihme_loc_id", "year"),
  all = TRUE,
  allow.cartesian = TRUE)
)

## input to spacetime
readr::write_csv(
  st_final,
  paste0(estimate_dir, version_estimate, "/FILEPATH/data_for_space_time_", main_std_def, ".csv")
)

## run space-time
st_pred_output <- resid_space_time(
  data = st_final,
  min_year = year_start,
  max_year = year_end,
  params = spacetime_parameters,
  st_loess = st_loess
)
st_pred <- as.data.table(st_pred_output)

## process space time results
st_pred <- st_pred[keep == 1, ]

## check that every location-year is present
if (nrow(st_pred) != nrow(years) * nrow(st_locs)) "location-years are missing from spacetime prediction"

setnames(st_pred, "year", "year_id")
st_pred[, year_id := floor(year_id)]

st_pred$weight <- NULL

st_pred <- unique(merge(
  st_pred,
  pred_data,
  by = c("ihme_loc_id", "year_id"),
  all = TRUE)
)

st_pred <- merge(st_pred, locs_sub[, c("ihme_loc_id", "region_name")], by = "ihme_loc_id", all.x = T)

st_pred[, pred_log_mean_st := pred_log_mean - pred.2.resid]

## Calculating mse (mean squared error) and use one global value (AG 2017)
national_locs <- mortdb::get_locations(level = "country", gbd_year = gbd_year)
national_locs <- national_locs[location_id != 6]
nationals <- st_pred[ihme_loc_id %in% national_locs$ihme_loc_id | ihme_loc_id == "CHN_44533",]
if (!use_crosswalk_stage1ensemble) nationals[is.na(pred_log_mean), pred_log_mean := pred_log_mean_fe]
diff <- nationals$pred_log_mean_st - nationals$pred_log_mean

if (any(is.na(diff)) == T) stop("you have missing stage2 - stage1 in mse calculation")

variance_diff <- tapply(diff, nationals$ihme_loc_id, function(x) var(x))
amp_value <- mean(variance_diff, na.rm = TRUE)
st_pred[, mse := amp_value]

# see value in output
print(amp_value)

# calculate MAD by data source and location

mad <- copy(st_pred)
mad <- aggregate(mad$diff[!is.na(mad$diff)], mad[!is.na(mad$diff), c("ihme_loc_id", "data_type2")], function(x)median(abs(x - median(x))))

# replace location/sources with only 1 point with regional max
mad <- merge(
  mad,
  locs,
  by = c("ihme_loc_id")
)
mad <- as.data.table(mad)
setkey(mad, region_name)
mad <- mad[, list(x, ihme_loc_id, data_type2, max_region = max(x)), by = key(mad)]
mad <- mad[x == 0, x := max_region]
mad <- mad[, c("max_region", "region_name") := NULL]
setnames(mad, "x", "mad")

st_pred <- merge(
  st_pred,
  mad,
  by = c("ihme_loc_id", "data_type2"),
  all.x = TRUE
)

# creating data variance
st_pred[, data_var := (1.4826 * mad)^2]

## format file for gpr
st_pred[, data := ifelse(!is.na(log_mean), 1, 0)]
st_pred[, category := data_type]

## run checks

st_pred_with_data <- st_pred[data == 1, ]
if (nrow(st_pred_with_data[is.na(data_var), ]) != 0) stop("you have missing mad/data variance going into GPR")
if (nrow(st_pred_with_data[is.na(mse), ]) != 0) stop("you have missing mse going into GPR")

st_pred_final <- st_pred[, c("ihme_loc_id", "year_id", "pred.2.resid", "log_mean",
                             "log_mean_adj", "pred_log_mean", "pred_log_mean_st",
                             "keep", "data_type", "region_name", "mse", "diff",
                             "data_var", "data", "std_def", "category")]

## save file for gpr
readr::write_csv(
  st_pred_final,
  paste0(estimate_dir, version_estimate, "/FILEPATH/gpr_input_", main_std_def, ".csv")
)
