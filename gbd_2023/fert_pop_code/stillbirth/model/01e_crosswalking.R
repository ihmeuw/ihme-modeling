
#################################################################################
##                                                                             ##
## Description: Uses MR-BRT Crosswalking to run a spline between SBR/NMR and   ##
##              SEV for short gestation for birth weight on matched data to    ##
##              calculate betas and adjust data based on stillbirth definition ##
##                                                                             ##
#################################################################################

Sys.umask(mode = "0002")
Sys.setenv("PYTHONNOUSERSITE" = "1")

library(assertable)
library(data.table)
library(openxlsx)
library(readr)
library(reticulate)

library(dplyr)

library(mortcore)
library(mortdb)

use_python("FILEPATH", required = TRUE)
xwalk <- import("crosswalk")

args <- commandArgs(trailingOnly = TRUE)
settings_dir <- args[1]

if (interactive()) {
  version_estimate <- "Run id"
  main_std_def <- "20_weeks"
  settings_dir <- paste0("FILEPATH/new_run_settings_", main_std_def, ".csv")
}

load(settings_dir)
list2env(new_settings, envir = environment())

source(paste0(shared_functions_dir, "get_covariate_estimates.R"))

set.seed(145153)

gold_standard_def <- main_std_def

with_nid <- FALSE
sev_prev <- FALSE

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

# Gold Standard: 22 weeks or 28 weeks
# Alternates: 20 weeks, *22 weeks*, 24 weeks, 26 weeks, *28 weeks*, 500 grams, 1000 grams
#             22 weeks OR 500 grams, 28 weeks OR 1000 grams
#             28 weeks AND 500 grams, 28 weeks AND 1000 grams

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

if (sev_prev) {

  sev_gest_bw <- get_covariate_estimates(
    covariate_id = 2093,
    release_id = 9,
    year_id = year_start:year_end
  )

  sev_gest_bw_snnp <- sev_gest_bw[location_id == 44858]

  sev_gest_bw_snnp_new <- sev_gest_bw[location_id == 44858]
  sev_gest_bw_snnp_sidama <- sev_gest_bw[location_id == 44858]
  sev_gest_bw_snnp_sw <- sev_gest_bw[location_id == 44858]

  sev_gest_bw_snnp_new[, location_id := 95069]
  sev_gest_bw_snnp_sidama[, location_id := 60908]
  sev_gest_bw_snnp_sw[, location_id := 94364]

  sev_gest_bw <- rbind(
    sev_gest_bw, sev_gest_bw_snnp_new, sev_gest_bw_snnp_sidama,
    sev_gest_bw_snnp_sw
  )

  sev_gest_bw <- sev_gest_bw[location_id %in% sev_gest_bw$location_id]

  sev_gest_bw_2023 <- sev_gest_bw[year_id == 2022]
  sev_gest_bw_2024 <- sev_gest_bw[year_id == 2022]

  sev_gest_bw_2023[, year_id := 2023]
  sev_gest_bw_2024[, year_id := 2024]

  sev_gest_bw <- rbind(sev_gest_bw, sev_gest_bw_2023, sev_gest_bw_2024)

} else {

  sev_gest_bw <- get_covariate_estimates(
    covariate_id = 2093,
    release_id = release_id,
    year_id = year_start:year_end
  )

}

setnames(sev_gest_bw, "mean_value", "sev_gest_bw")

sev_gest_bw <- merge(
  sev_gest_bw[location_id %in% location_hierarchy$location_id],
  location_hierarchy[, c("location_id", "ihme_loc_id")],
  by = c("location_id"),
  all.x = TRUE
)

assertable::assert_values(sev_gest_bw, "ihme_loc_id", test = "not_na")

if (main_std_def == "20_weeks") {
  readr::write_csv(
    sev_gest_bw,
    paste0(data_dir, version_data, "/FILEPATH/sev_gest_bw.csv")
  )
}

# Transform mean

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
    paste0(crosswalk_dir, "nmr_", parents[["no shock life table estimate"]], ".csv")
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
#   - For each definiton of stillbirth, cut the top and bottom 10% of values and then take the sd of that

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

# Select matching parameters

df_orig2 <- data_orig[outlier != 1 & !is.na(se), c("ihme_loc_id", "year_id", "source_type_id", "nid", "source", "std_def_short", "mean", "se")]

if (with_nid) {

  df_orig2[, id := paste0(ihme_loc_id, " ", year_id, " ", nid)]

} else {

  df_orig2[, id := paste0(ihme_loc_id, " ", year_id)]

}

setnames(df_orig2, c("mean", "se"), c("prev", "prev_se"))

df_orig2$std_def_short <- factor(
  df_orig2$std_def_short,
  levels = c(
    "20_weeks", "500_grams", "22_weeks_OR_500_grams", "22_weeks", "22_weeks_AND_500_grams",
    "24_weeks", "26_weeks", "1000 grams", "28_weeks_OR_1000_grams", "28_weeks", "28_weeks_AND_1000_grams"
  )
)
df_orig2 <- df_orig2[order(std_def_short)]

df_orig2[, orig_row := .I]

method_var <- "std_def_short"
gold_def <- gold_standard_def
keep_vars <- c("orig_row", "ihme_loc_id", "year_id", "nid",
               "source_type_id", "source", "prev", "prev_se")

# Create matched pairs of alternative/reference observations

data_diagnostics_files <- list.files(
  paste0(data_dir, version_data, "/FILEPATH/"),
  pattern = ".csv",
  full.names = TRUE,
  recursive = TRUE
)
data_diagnostics <- lapply(data_diagnostics_files, fread) %>% rbindlist(use.names = TRUE, fill = TRUE)

if (nrow(data_diagnostics) == 0) {

  crosswalk_dir_prev <- gsub(version_data, test_version_data, crosswalk_dir)

  df_matched <- fread(paste0(crosswalk_dir_prev, "input_to_crosswalk_", main_std_def, ".csv"))
  readr::write_csv(
    df_matched,
    paste0(crosswalk_dir, "input_to_crosswalk_", main_std_def, ".csv")
  )

} else {

  df_matched_orig <- do.call("rbind", lapply(unique(df_orig2$id), function(i) {
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

  df_matched <- as.data.table(df_matched_orig)

  df_matched <- df_matched[!(source_type_id_ref %in% c(57, 58)) & !(source_type_id_alt %in% c(57, 58))]

  df_matched_direct <- df_matched[dorm_ref == gold_def]
  df_matched_indirect <- df_matched[dorm_ref != gold_def]
  df_matched_indirect[, row_id := seq(1, nrow(df_matched_indirect))]

  # Remove duplicate indirect comparisons

  for (row in 1:nrow(df_matched_indirect)) {

    ref <- df_matched_indirect[row]$orig_row_ref
    alt <- df_matched_indirect[row]$orig_row_alt

    df_matched_indirect[orig_row_ref == alt & orig_row_alt == ref & row_id > row, drop := 1]

  }

  df_matched_indirect <- df_matched_indirect[is.na(drop)]
  df_matched_indirect[, drop := NULL]
  df_matched_indirect[, row_id := NULL]

  # Specify whether to include indirect matches along with the direct matches

  if (direct_matches_only) {
    df_matched <- copy(df_matched_direct)
  } else {
    df_matched <- rbind(df_matched_direct, df_matched_indirect)
  }

  # Drop improbable match values (earlier wk def < later wk def)

  wk_defs <- c("20_weeks", "22_weeks", "24_weeks", "26_weeks", "28_weeks")

  df_matched[
    dorm_ref %in% wk_defs & dorm_alt %in% wk_defs & dorm_ref != dorm_alt,
    `:=` (dorm_ref_val = as.numeric(substr(dorm_ref, 1, 2)), dorm_alt_val = as.numeric(substr(dorm_alt, 1, 2)))
  ]

  df_matched[, row_num := .I]

  df_matched_improbable <- df_matched[!(
    (dorm_ref_val > dorm_alt_val & prev_ref <= prev_alt) | (dorm_ref_val < dorm_alt_val & prev_ref >= prev_alt)
  )]

  if (gold_def %in% c("20_weeks", "22_weeks")) {

    df_matched_improbable2 <- df_matched[dorm_ref == gold_def & dorm_alt %like% "1000_grams" & prev_ref < prev_alt]

  } else if (gold_def == "24_weeks") {

    df_matched_improbable2 <- df_matched[
      (dorm_ref == gold_def & dorm_alt %like% "1000_grams" & prev_ref < prev_alt) |
        (dorm_ref == gold_def & dorm_alt %like% "500_grams" & prev_ref > prev_alt)
    ]

  } else if (gold_def %in% c("26_weeks", "28_weeks")) {

    df_matched_improbable2 <- df_matched[dorm_ref == gold_def & dorm_alt %like% "500_grams" & prev_ref > prev_alt]

  }

  if (nrow(df_matched_improbable2) > 0) df_matched_improbable <- unique(rbind(df_matched_improbable, df_matched_improbable2))

  df_matched <- df_matched[!(
    row_num %in% unique(df_matched_improbable$row_num)
  )]

  df_matched[, `:=` (row_num = NULL, dorm_ref_val = NULL, dorm_alt_val = NULL)]

  # Transform to log space

  log_alt <- xwalk$utils$linear_to_log(
    array(df_matched$prev_alt),
    array(df_matched$prev_se_alt)
  )
  log_ref <- xwalk$utils$linear_to_log(
    array(df_matched$prev_ref),
    array(df_matched$prev_se_ref)
  )

  df_matched[, log_diff := log_alt[[1]] - log_ref[[1]]]
  df_matched[, log_diff_se := sqrt(log_alt[[2]]^2 + log_ref[[2]]^2)]

  # Merge on SEV

  df_matched <- merge(
    df_matched,
    sev_gest_bw[, c("ihme_loc_id", "year_id", "sev_gest_bw")],
    by.x = c("ihme_loc_id_ref", "year_id_ref"),
    by.y = c("ihme_loc_id", "year_id"),
    all.x = TRUE
  )

  assertable::assert_values(df_matched, "sev_gest_bw", test = "not_na")
  if (main_std_def == "20_weeks") assertable::assert_values(df_matched, "log_diff", test = "lt", test_val = 1)

}

# Save crosswalk input data

readr::write_csv(
  df_orig2,
  paste0(crosswalk_dir, "input_to_crosswalk_", main_std_def, "_unmatched.csv")
)

readr::write_csv(
  df_matched,
  paste0(crosswalk_dir, "input_to_crosswalk_", main_std_def, ".csv")
)

if (nrow(df_matched_improbable) > 0) {
  readr::write_csv(
    df_matched_improbable,
    paste0(crosswalk_dir, "improbable_matches_", main_std_def, ".csv")
  )
}

# Update order restraints based on what's available

order_restraints <- purrr::keep(
  order_restraints,
  function(x) all(x %in% unique(c(df_matched$dorm_ref, df_matched$dorm_alt)))
)

#######################
## Perform Crosswalk ##
#######################

df <- xwalk$CWData(
  df = df_matched,            # dataset for metaregression
  obs = "log_diff",           # column name for the observation mean
  obs_se = "log_diff_se",     # column name for the observation standard error
  alt_dorms = "dorm_alt",     # column name of the variable indicating the alternative method
  ref_dorms = "dorm_ref",     # column name of the variable indicating the reference method
  covs = list("sev_gest_bw"), # names of columns to be used as covariates later
  study_id = "id"             # name of the column indicating group membership, usually the matching groups
)

if (gold_def == "20_weeks") {

  fit <- xwalk$CWModel(
    cwdata = df,                    # object returned by `CWData()`
    obs_type = "diff_log",          # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
    cov_models = list(              # specifying predictors in the model
      xwalk$CovModel("intercept"),
      xwalk$CovModel("sev_gest_bw")),
    gold_dorm = gold_standard_def,  # the level of `ref_dorms` that indicates it's the gold standard
    order_prior = order_restraints  # tells the model that the coefficient estimated for B should be <= the coefficient for C
  )

  fit$fit()

} else if (gold_def %in% c("22_weeks", "24_weeks", "26_weeks")) {

  fit <- xwalk$CWModel(
    cwdata = df,                    # object returned by `CWData()`
    obs_type = "diff_log",          # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
    cov_models = list(              # specifying predictors in the model
      xwalk$CovModel("intercept"),
      xwalk$CovModel("sev_gest_bw")
    ),
    gold_dorm = gold_standard_def,  # the level of `ref_dorms` that indicates it's the gold standard
    order_prior = order_restraints  # tells the model that the coefficient estimated for B should be <= the coefficient for C
  )

  fit$fit()

} else if (gold_def == "28_weeks") {

  fit <- xwalk$CWModel(
    cwdata = df,                    # object returned by `CWData()`
    obs_type = "diff_log",          # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
    cov_models = list(              # specifying predictors in the model
      xwalk$CovModel("intercept"),
      xwalk$CovModel(
        "sev_gest_bw",
        prior_beta_uniform = list(
          `22_weeks_AND_500_grams` = array(c(0, Inf)),
          `28_weeks_AND_1000_grams` = array(c(0, Inf))
        )
      )
    ),
    gold_dorm = gold_standard_def,  # the level of `ref_dorms` that indicates it's the gold standard
    order_prior = order_restraints  # tells the model that the coefficient estimated for B should be <= the coefficient for C
  )

  fit$fit()

}

# Save model object

py_save_object(object = fit, filename = paste0(crosswalk_dir, "fit1.pkl"), pickle = "dill")

# Save betas

betas <- as.data.table(fit$create_result_df())
betas <- betas[, 1:4]

setnames(betas, "beta_sd", "beta_se")

betas[, beta_lower := beta - 1.96 * beta_se]
betas[, beta_upper := beta + 1.96 * beta_se]

readr::write_csv(betas, paste0(crosswalk_dir, "betas_", main_std_def, ".csv"))

#################
## Adjust Data ##
#################

df_orig3 <- merge(
  df_orig2[std_def_short == gold_standard_def | std_def_short %in% df_matched$dorm_alt],
  sev_gest_bw[, c("ihme_loc_id", "year_id", "sev_gest_bw")],
  by = c("ihme_loc_id", "year_id"),
  all.x = TRUE
)
setnames(df_orig3, c("prev", "prev_se"), c("mean", "sd"))

preds2 <- fit$adjust_orig_vals(
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
  paste0(crosswalk_dir, "crosswalk_", main_std_def, ".xlsx"),
  sheetName = "extraction"
)

readr::write_csv(
  df_orig3,
  paste0(crosswalk_dir, "crosswalk_", main_std_def, ".csv")
)

############################
## Additional Diagnostics ##
############################

# Rerun df
df <- xwalk$CWData(
  df = df_matched,            # dataset for metaregression
  obs = "log_diff",           # column name for the observation mean
  obs_se = "log_diff_se",     # column name for the observation standard error
  alt_dorms = "dorm_alt",     # column name of the variable indicating the alternative method
  ref_dorms = "dorm_ref",     # column name of the variable indicating the reference method
  covs = list("sev_gest_bw"), # names of columns to be used as covariates later
  study_id = "id",            # name of the column indicating group membership, usually the matching groups
  dorm_separator = " "
)

# Call plotting functions
plots <- import("FILEPATH")

# Funnel Plots

if (main_std_def == "28 weeks") {

  alt_definitions <- data.table(
    std_def_short = sort(unique(df_orig3$std_def_short)),
    letter = c("F", "A", "B", "G", "I", "C", "D", "MAIN", "H", "J", "E")
  )

} else {

  alt_definitions <- data.table(
    std_def_short = sort(unique(df_orig3$std_def_short)),
    letter = c("F", "MAIN", "A", "G", "I", "B", "C", "D", "H", "J", "E")
  )

}

alt_definitions <- alt_definitions[std_def_short != main_std_def]

for (def in unique(alt_definitions$std_def_short)) {

  xwalk$plots$funnel_plot(
    cwmodel = fit,
    cwdata = df,
    continuous_variables = list("sev_gest_bw"),
    obs_method = def,
    plot_note = "Funnel plot",
    plots_dir = crosswalk_dir,
    file_name = paste0("funnel_plot_for_crosswalk_", def),
    write_file = TRUE
  )

}

# Dose-Response Plots

for (def in unique(alt_definitions$std_def_short)) {

  xwalk$plots$dose_response_curve(
    dose_variable = "sev_gest_bw",
    cwmodel = fit,
    cwdata = df,
    continuous_variables = list(),
    obs_method = def,
    plot_note = paste0(alt_definitions[std_def_short == def]$letter, ": Dose-Response Plot (", gsub("_", " ", def), ")"),
    plots_dir = crosswalk_dir,
    file_name = paste0("dose_response_plot_", def),
    write_file = TRUE
  )
}
