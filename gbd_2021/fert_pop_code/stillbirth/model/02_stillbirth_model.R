###################################################################################################
##
## Purpose: Stillbirths Model for GBD 2019
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

user <- Sys.getenv("USER")

# Get settings
args <- commandArgs(trailingOnly = TRUE)
new_settings_dir <- args[1]

if (interactive()) { # when interactive, only one definition can be run at a time
  version_estimate <- 999
  main_std_def <- "28_weeks"
  new_settings_dir <- "FILEPATH"
}

load(new_settings_dir)
list2env(new_settings, envir = environment())

source(paste0(working_dir, "calculate_hyperparameters.R"))
source(paste0(working_dir, "helper_functions.R"))
source(paste0(working_dir, "space_time.R"))
source(paste0(working_dir, "../model/transformation_functions.R"))

############################
## Hyperparameter Options ##
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

readr::write_csv(st_locs, "FILEPATH")

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
  crosswalk_data <- fread("FILEPATH")

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
    data <- fread("FILEPATH")
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

data[source_type == "Standard DHS", source_type := "Survey"] # eventually change to dhs

data[source_type == "Census", data_type := "census"]
data[source_type == "Statistical Report", data_type := "gov_report"]
data[source_type == "Sci Lit", data_type := "literature"]
data[source_type == "Survey", data_type := "survey"]
data[source_type == "VR", data_type := "vr"]

## Merge on covariates
covariates <- fread("FILEPATH")
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

  ## Create ratio/dependent variable -> log(SBR/NMR)
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
  "FILEPATH"
)

#########################################
## Create Prediction Dataset (square) ##
########################################

pred <- fread("FILEPATH")

if (nrow(pred) != length(unique(pred$ihme_loc_id))*length(unique(pred$year_id))) stop("pred dataset not square")

pred[, year := year_id + .5]
pred[, data_type := "vr"]
pred[, std_def := main_std_def]

pred <- pred[order(ihme_loc_id, year_id),]

readr::write_csv(
  pred,
  "FILEPATH"
)

###############################
## Calculate Hyperparameters ##
###############################

## Custom function to calculate hyperparameters and data density
calculate_hyperparameters(
  data,
  use_5q0_density_parameters = use_5q0_density_parameters,
  use_fertility_density_parameters = use_fertility_density_parameters,
  gbd_year = gbd_year,
  test_lambda = test_lambda
)

spacetime_parameters <- fread("FILEPATH")

## Save updated file (so graphing code has correct value)
readr::write_csv(spacetime_parameters, "FILEPATH")

###################
## Stage 1 Model ##
###################

if (use_crosswalk_stage1ensemble) {

  # Read in stage 1 ensemble model results
  main_std_def_clean <- gsub("_", " ", main_std_def)
  stage_1_dir <- "FILEPATH"

  stage_1_ensemble <- get_recent(folder = stage_1_dir, pattern = "custom_prior")

  pred_data <- merge(
    data,
    stage_1_ensemble[location_id != 6], # removing CHN
    by = c("location_id", "year_id"),
    all.x = TRUE
  )
  setnames(pred_data, "cv_custom_stage_1", "pred_log_mean")

  data <- copy(pred_data)

} else {

  ## Step 1. Run stage 1 mixed effects model for standard locations only

  data_backup <- copy(data) # Addition for interactive tests

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

  save(mod, file = "FILEPATH")

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
    "FILEPATH"
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

  # calculate preds and FE-residuals (preds should be based on reference definition and source type)
  # reference def: see std_def
  # reference source type: vr_complete
  data[, pred_log_mean_fe := (fixed_effect_maternal_edu * maternal_educ_yrs_pc) + fixed_effect_intercept]

  data[, fe_resid := log_mean_adj - pred_log_mean_fe]
  if (nrow(data[!is.na(nid) & is.na(fe_resid), ]) != 0) stop("NAs present in fe_resid")

  # Predict w/o random effects or source-specific fixed effects
  readr::write_csv(
    data,
    "FILEPATH"
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
    file = "FILEPATH"
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
    "FILEPATH",
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
  "FILEPATH",
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

## merging "fake" regions for space time once addl years are added
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
  "FILEPATH"
)

## run space-time (function call)
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

## Calculating mse (mean squared error) and use one global value
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

## calculate residual i.e. diff between the stage 2 model (spacetime) and the adjusted data for ST
st_pred[, diff := pred_log_mean_st - log_mean_adj]

# calculate MAD by data source and location

mad <- copy(st_pred)
mad <- aggregate(mad$diff[!is.na(mad$diff)], mad[!is.na(mad$diff), c("ihme_loc_id", "data_type2")], function(x)median(abs(x - median(x))))

# for location/sources where we only have 1 point, MAD = 0, so we replace this with a regional max
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
  "FILEPATH"
)
