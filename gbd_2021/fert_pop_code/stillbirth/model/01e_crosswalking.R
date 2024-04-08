
#################################################################################
##                                                                             ##
## Description: Uses MR-BRT Crosswalking to run a spline between SBR/NMR and   ##
##              SEV for short gestation for birth weight on matched data to    ##
##              calculate betas and adjust data based on stillbirth definition ##
##                                                                             ##
#################################################################################

Sys.umask(mode = "0002")

library(assertable)
library(data.table)
library(ggplot2)
library(openxlsx)
library(RColorBrewer)
library(readr)

library(plyr)
library(dplyr)

library(mortcore)
library(mortdb)

library(crosswalk002, lib.loc = "FILEPATH")

args <- commandArgs(trailingOnly = TRUE)
settings_dir <- args[1]

if (interactive()) {
  version_estimate <- 999
  main_std_def <- "20_weeks"
  settings_dir <- "FILEPATH"
}

load(settings_dir)
list2env(new_settings, envir = environment())

source(paste0(shared_functions_dir, "get_covariate_estimates.R"))

set.seed(145153)

gold_standard_def <- main_std_def

with_nid <- F

if (model == "SBR") crosswalk_dir <- "FILEPATH"
if (model == "SBR/NMR") crosswalk_dir <- "FILEPATH"
if (model == "SBR + NMR") crosswalk_dir <- "FILEPATH"

dir.create(crosswalk_dir)

# Set restraints (tells model that coefficient estimated for B should be <= coefficient for C)

order_restraints <- list(
  c("22_weeks", "20_weeks"),
  c("24_weeks", "22_weeks"),
  c("26_weeks", "24_weeks"),
  c("28_weeks", "26_weeks"),
  c("1000_grams", "500_grams"),
  c("22_weeks", "22_weeks_OR_500_grams"),
  c("500_grams", "22_weeks_OR_500_grams"),
  c("22_weeks_AND_500_grams", "22_weeks"),
  c("22_weeks_AND_500_grams", "500_grams"),
  c("28_weeks", "28_weeks_OR_1000_grams"),
  c("1000_grams", "28_weeks_OR_1000_grams"),
  c("28_weeks_AND_1000_grams", "28_weeks"),
  c("28_weeks_AND_1000_grams", "1000_grams")
)

# Get location hierarchy

location_hierarchy <- mortdb::get_locations(gbd_year = gbd_year)
location_hierarchy <- location_hierarchy[, .(ihme_loc_id, location_id, location_name)]

######################
## Prepare the Data ##
######################

# Get data

data_orig <- mortdb::get_mort_outputs(
  model_name = "stillbirth",
  model_type = "data",
  run_id = version_data,
  hostname = hostname,
  outlier_run_id = "active",
  gbd_year = gbd_year,
  demographic_metadata = TRUE
)

data_orig[is.na(outlier), outlier := 0]

# Get Summary Exposure Value (SEV) for short gestation for birth weight

sev_gest_bw <- get_covariate_estimates(
  covariate_id = 2093, # age-standardized
  gbd_round_id = mortdb::get_gbd_round(gbd_year),
  year_id = year_start:year_end,
  decomp_step = "iterative"
)

setnames(sev_gest_bw, "mean_value", "sev_gest_bw")

sev_gest_bw <- merge(
  sev_gest_bw,
  location_hierarchy[, c("location_id", "ihme_loc_id")],
  by = c("location_id"),
  all.x = TRUE
)

assertable::assert_values(sev_gest_bw, "ihme_loc_id", test = "not_na")

# Get Summary Exposure Value (SEV) for low birth weight for gestation

if (file.exists(paste0(crosswalk_dir, "nmr_", parents[["no shock life table estimate"]], ".csv"))) {

  nmr <- fread(paste0(crosswalk_dir, "nmr_", parents[["no shock life table estimate"]], ".csv"))

} else {

  nmr <- mortdb::get_mort_outputs(
    model_name = "no shock life table",
    model_type = "estimate",
    run_id = parents[["no shock life table estimate"]],
    life_table_parameter_ids = 3,
    estimate_stage_ids = 5,
    gbd_year = gbd_year,
    sex_ids = 3,
    year_ids = year_start:year_end,
    age_group_ids = 42
  )

  nmr <- nmr[, .(ihme_loc_id, year_id, mean)]
  setnames(nmr, "mean", "nmr")

  readr::write_csv(
    nmr,
    "FILEPATH"
  )

}

data_orig <- merge(
  data_orig,
  nmr,
  by = c("ihme_loc_id", "year_id"),
  all.x = TRUE
)

if (model == "SBR/NMR") {

  setnames(data_orig, "mean", "sbr")

  data_orig[, mean := sbr/nmr] # log ratio calculated later in delta transformation

} else if (model == "SBR + NMR") {

  setnames(data_orig, "mean", "sbr")

  data_orig[, mean := sbr + nmr] # log ratio calculated later in delta transformation

}

# Clean data

data_orig <- mortdb::ids_to_names(data_orig, exclude = c("sex_id"))
data_orig$std_def_id <- NULL
data_orig$std_def_full <- NULL

data_orig <- data_orig[year_id >= year_start, ]

# Calculate standard error

assertable::assert_values(data_orig, "mean", test = "not_na")

se_list <- list()
row <- 1

for (def in unique(data_orig$std_def_short)) {

  data_temp <- data_orig[std_def_short == def & outlier != 1]

  data_sub <- data_temp[mean %between% quantile(mean, c(.1, .9))]

  se_calc <- sd(data_temp$mean)

  data_se <- data.frame(std_def_short = def, se = se_calc)

  se_list[[row]] <- data_se

  row <- row + 1

  data_temp <- NULL
  se_calc <- NULL

}

se_values <- rbindlist(se_list, use.names = TRUE, fill = TRUE)

data_orig <- merge(
  data_orig,
  se_values,
  by = "std_def_short",
  all.x = TRUE
)

# Select matching parameters (final choice: ihme_loc_id + year_id)

df_orig2 <- data_orig[outlier != 1 & !is.na(se), c("ihme_loc_id", "year_id", "source_type_id", "nid", "source", "std_def_short", "mean", "se")]

if (with_nid) {

  df_orig2[, id := paste0(ihme_loc_id, " ", year_id, " ", nid)]

} else {

  df_orig2[, id := paste0(ihme_loc_id, " ", year_id)]

}

setnames(df_orig2, c("mean", "se"), c("prev", "prev_se"))

df_orig2[, orig_row := .I]

method_var <- "std_def_short"
gold_def <- gold_standard_def
keep_vars <- c("orig_row", "ihme_loc_id", "year_id", "nid",
               "source_type_id", "source", "prev", "prev_se")

# Create matched pairs of alternative/reference observations

data_diagnostics_files <- list.files(
  "FILEPATH",
  pattern = ".csv",
  full.names = TRUE,
  recursive = TRUE
)
data_diagnostics <- lapply(data_diagnostics_files, fread) %>% rbindlist(use.names = TRUE, fill = TRUE)

if (nrow(data_diagnostics) == 0) {

  crosswalk_dir_prev <- gsub(version_data, test_version_data, crosswalk_dir)

  df_matched <- fread("FILEPATH")
  readr::write_csv(
    df_matched,
    "FILEPATH")

} else {

  df_matched <- do.call("rbind", lapply(unique(df_orig2$id), function(i) {
    dat_i <- filter(df_orig2, id == i) %>% mutate(dorm = get(method_var))
    keep_vars <- c("dorm", keep_vars)
    row_ids <- expand.grid(idx1 = 1:nrow(dat_i), idx2 = 1:nrow(dat_i))
    do.call("rbind", lapply(1:nrow(row_ids), function(j) {
      dat_j <- row_ids[j, ]
      dat_j[, paste0(keep_vars, "_alt")] <- dat_i[dat_j$idx1, ..keep_vars]
      dat_j[, paste0(keep_vars, "_ref")] <- dat_i[dat_j$idx2, ..keep_vars]
      filter(dat_j, dorm_alt != gold_def & dorm_alt != dorm_ref)
    })) %>% mutate(id = i) %>% select(-idx1, -idx2)
  }))

  # Remove duplicate indirect comparisons

  df_matched <- as.data.table(df_matched)

  df_matched_direct <- df_matched[dorm_ref == gold_def]
  df_matched_indirect <- df_matched[dorm_ref != gold_def]
  df_matched_indirect[, row_id := seq(1, nrow(df_matched_indirect))]

  for (row in 1:nrow(df_matched_indirect)) {

    ref <- df_matched_indirect[row]$orig_row_ref
    alt <- df_matched_indirect[row]$orig_row_alt

    df_matched_indirect[orig_row_ref == alt & orig_row_alt == ref & row_id > row, drop := 1]

  }

  df_matched_indirect <- df_matched_indirect[is.na(drop)]
  df_matched_indirect[, drop := NULL]
  df_matched_indirect[, row_id := NULL]

  df_matched <- rbind(df_matched_direct, df_matched_indirect)

  # Transform to log space

  dat_diff <- as.data.frame(cbind(
    crosswalk002::delta_transform(
      mean = df_matched$prev_alt,
      sd = df_matched$prev_se_alt,
      transformation = "linear_to_log"),
    crosswalk002::delta_transform(
      mean = df_matched$prev_ref,
      sd = df_matched$prev_se_ref,
      transformation = "linear_to_log")
  ))

  names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

  # Get log(alt)-log(ref) as dependent variable

  df_matched[, c("log_diff", "log_diff_se")] <- calculate_diff(
    df = dat_diff,
    alt_mean = "mean_alt", alt_sd = "mean_se_alt",
    ref_mean = "mean_ref", ref_sd = "mean_se_ref" )

  # Merge on SEV

  df_matched <- merge(
    df_matched,
    sev_gest_bw[, c("ihme_loc_id", "year_id", "sev_gest_bw")],
    by.x = c("ihme_loc_id_ref", "year_id_ref"),
    by.y = c("ihme_loc_id", "year_id"),
    all.x = TRUE
  )

  assertable::assert_values(df_matched, "sev_gest_bw", test = "not_na")

}

# Save crosswalk input data

readr::write_csv(
  df_matched,
  paste0(crosswalk_dir, "input_to_crosswalk_", main_std_def, ".csv")
)

# Decide knots for spline

#######################
## Perform Crosswalk ##
#######################

df <- crosswalk002::CWData(
  df = df_matched,            # dataset for metaregression
  obs = "log_diff",           # column name for the observation mean
  obs_se = "log_diff_se",     # column name for the observation standard error
  alt_dorms = "dorm_alt",     # column name of the variable indicating the alternative method
  ref_dorms = "dorm_ref",     # column name of the variable indicating the reference method
  covs = list("sev_gest_bw"), # names of columns to be used as covariates later
  study_id = "id"             # name of the column indicating group membership, usually the matching groups
)


if (gold_def == "20_weeks") {

  fit <- crosswalk002::CWModel(
    cwdata = df,                    # object returned by `CWData()`
    obs_type = "diff_log",          # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
    cov_models = list(              # specifying predictors in the model; see help(CovModel)
      CovModel("intercept"),
      CovModel("sev_gest_bw")),
    gold_dorm = gold_standard_def,  # the level of `ref_dorms` that indicates it's the gold standard
    max_iter = 1000L,
    order_prior = order_restraints  # tells the model that the coefficient estimated for B should be <= the coefficient for C
  )

} else if (gold_def == "28_weeks") {

  fit <- crosswalk002::CWModel(
    cwdata = df,                    # object returned by `CWData()`
    obs_type = "diff_log",          # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
    cov_models = list(              # specifying predictors in the model; see help(CovModel)
      CovModel("intercept"),
      CovModel("sev_gest_bw", prior_beta_uniform = list(`22_weeks_AND_500_grams` = array(c(0, Inf)),
                                                        `28_weeks_AND_1000_grams` = array(c(0, Inf))))),
    gold_dorm = gold_standard_def,  # the level of `ref_dorms` that indicates it's the gold standard
    max_iter = 1500L,
    order_prior = order_restraints  # tells the model that the coefficient estimated for B should be <= the coefficient for C
  )

}

# Save model object

py_save_object(object = fit, filename = paste0(crosswalk_dir, "fit1.pkl"), pickle = "dill")

# Save betas

betas <- as.data.table(fit$create_result_df())
betas <- betas[, 1:4]

setnames(betas, "beta_sd", "beta_se")

betas[, beta_lower := beta - 1.96 * beta_se] # To Do: use draws for this!
betas[, beta_upper := beta + 1.96 * beta_se]

print(betas)

readr::write_csv(betas, "FILEPATH")

#################
## Adjust Data ##
#################

df_orig3 <- merge(
  df_orig2,
  sev_gest_bw[, c("ihme_loc_id", "year_id", "sev_gest_bw")],
  by = c("ihme_loc_id", "year_id"),
  all.x = TRUE
)
setnames(df_orig3, c("prev", "prev_se"), c("mean", "sd"))

preds2 <- crosswalk002::adjust_orig_vals(
  fit_object = fit, # object returned by `CWModel()`
  df = df_orig3,
  orig_dorms = "std_def_short",
  orig_vals_mean = "mean",
  orig_vals_se = "sd",
  study_id = "id",
  data_id = "orig_row"
)

df_orig3[, c("mean_adj", "sd_adj", "mean_adj_factor", "sd_adj_factor", "data_id")] <- preds2

df_orig3[, data_id := NULL][, orig_row := NULL]

df_orig3 <- unique(df_orig3)

##################
## Save Results ##
##################

assertable::assert_values(df_orig3, colnames(df_orig3), test = "not_na")

openxlsx::write.xlsx(
  df_orig3,
  "FILEPATH",
  sheetName = "extraction"
)

readr::write_csv(
  df_orig3,
  "FILEPATH"
)
