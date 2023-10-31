##########################################################################
### Purpose: Data processing - sex splitting, crosswalking 
### For HBsAg 
##########################################################################

rm(list=ls())

.libPaths(c("/ihme/singularity-images/rstudio/lib/3.6.3", .libPaths()))
if (Sys.info()["sysname"] == "Linux") {
  j <- "/home/j/"
  h <- "~/"
  l <- "/ihme/limited_use/"
} else {
  j <- "J:/"
  h <- "H:/"
  l <- "L:/"
}

date <- gsub("-", "_", Sys.Date())

library(crosswalk, lib.loc = FILEPATH)
library(mortdb, lib = FILEPATH)
pacman::p_load(data.table, openxlsx, ggplot2, boot, gtools, msm, Hmisc)

gbd_round_id <- 7 
decomp_step <- "iterative"

# SET OBJECTS -------------------------------------------------------------

b_id <- OBJECT
a_cause <- OBJECT
bv_id <- OBJECT

draws <- paste0("draw_", 0:999)
mrbrt_dir <- FILEPATH
if (!dir.exists(mrbrt_dir)) dir.create(mrbrt_dir)
cv_drop <- c('cv_healthcare_worker', 'cv_attending_unrelated_clinic', 'cv_schoolchildren', 
             'cv_hospital_referral', 'cv_specific_occupation', 'cv_medical_condition', 'cv_institutionalized')
outlier_status <- c(0, 1)
sex_split <- T
sex_covs <- "" 
id_vars <- "study_id"
sex_remove_x_intercept <- T
keep_x_intercept <- T
logit_transform <- T
reference <- ""
nash_cryptogenic <- F
reference_def <- "reference"
trim <- 0.1
measures <- "prevalence"
model_name <- paste0("sex_split_", date)
if (!dir.exists(paste0(mrbrt_dir, model_name))) dir.create(paste0(mrbrt_dir, model_name))
# modeling <- T

if(logit_transform == T) {
  response <- "ldiff"
  data_se <- "ldiff_se"
  mrbrt_response <- "diff_logit"
} else {
  response <- "ratio_log"
  data_se <- "ratio_se_log"
  mrbrt_response <- "diff_log"
}



if(sex_split == T) { 
  ## Sex splitting only done in log space
  sex_split_response <- "log_ratio"
  sex_split_data_se <- "log_se"
  sex_mrbrt_response <- "diff_log"
}


# SOURCE FUNCTIONS --------------------------------------------------------
source(FILEPATH)

# DATA PROCESSING FUNCTIONS -----------------------------------------------
# GET DATA FOR ADJUSTMENT 
orig_dt <- get_bundle_version(bundle_version_id = bv_id, fetch = "all")
length(unique(orig_dt$nid))
checkpoint <- copy(orig_dt)
hep_sex_dt <- copy(orig_dt)

loc_dt <- get_location_metadata(35, gbd_round_id = 7)
loc_dt1 <- loc_dt[, .(location_id, super_region_name)]

# RUN SEX SPLIT -----------------------------------------------------------
hep_sex_dt <- hep_sex_dt[measure %in% c("prevalence")]
hep_sex_dt <- hep_sex_dt[is_outlier %in% outlier_status]
hep_sex_dt <- hep_sex_dt[cases != 0 & sample_size != 0, ]
hep_sex_dt <- get_cases_sample_size(hep_sex_dt)
hep_sex_dt <- get_se(hep_sex_dt)
hep_sex_dt <- calculate_cases_fromse(hep_sex_dt)
hep_sex_matches <- find_sex_match(hep_sex_dt)
unique(hep_sex_matches[, .N, by = c(sex_covs)])
length(unique(hep_sex_matches$nid))
mrbrt_sex_dt <- calc_sex_ratios(hep_sex_matches)


# Format the data for meta-regression 
if (sex_covs == "") {
  message("Formatting sex data without covariates")
  formatted_data <- CWData(
    df = mrbrt_sex_dt,
    obs = sex_split_response,
    obs_se = sex_split_data_se,
    alt_dorms = "dorm_alt",
    ref_dorms = "dorm_ref",
    study_id = id_vars
  )
} else {
  message("Formatting sex data with covariates")
  formatted_data <- CWData(
    df = mrbrt_sex_dt,
    obs = sex_split_response,
    obs_se = sex_split_data_se,
    alt_dorms = "dorm_alt",
    ref_dorms = "dorm_ref",
    covs = c(list("mid_age")),
    study_id = id_vars
  )
}

## Launch MR-BRT Sex Model -------------------------------------
if (file.exists(paste0(mrbrt_dir, model_name, "/model_object.rds"))){
  sex_results <- readr::read_rds(paste0(mrbrt_dir, model_name, "/model_object.rds"))
} else {
  sex_results <- CWModel(
    cwdata = formatted_data,
    obs_type = sex_mrbrt_response,
    cov_models = list(CovModel("intercept")),
    gold_dorm = "Male",
    inlier_pct = (1 - trim)
  )
  save <- save_model_RDS(sex_results, mrbrt_dir, model_name)
}

save_r <- save_mrbrt(sex_results, mrbrt_dir, model_name)

sex_results$fixed_vars
sex_results$beta_sd
sex_results$gamma

# Make funnel plots
repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = sex_results, 
  cwdata = formatted_data,
  continuous_variables = list(),
  obs_method = "Female",
  plot_note = a_cause, 
  plots_dir = paste0(mrbrt_dir, model_name), 
  file_name = model_name,
  write_file = TRUE
)

hep_dt <- copy(orig_dt)
hep_dt <- hep_dt[measure %in% measures]
hep_dt <- hep_dt[is_outlier %in% outlier_status]
hep_dt <- hep_dt[cases != 0 & sample_size != 0, ]
hep_dt <- get_cases_sample_size(hep_dt)
hep_dt <- get_se(hep_dt)
hep_dt <- calculate_cases_fromse(hep_dt)
defs <- get_definitions(hep_dt)
hep_dt <- defs[[1]]
cvs <- defs[[2]]
hep_dt <- subnat_to_nat(hep_dt, subnat = F)
hep_dt <- calc_year(hep_dt)
age_dts <- get_age_combos(hep_dt)
hep_dt <- age_dts[[1]]
age_match_dt <- age_dts[[2]] ## DON'T ACTUALLY NEED THIS BUT FOUND IT HELPFUL FOR VETTING
pairs <- combn(hep_dt[, unique(definition)], 2)
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) 
  get_matches(n = x, pair_dt = hep_dt, year_span = 10, age_span = 5)))
ratios <- create_ratios(matches)
mrbrt_setup <- create_mrbrtdt(ratios)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]



# RUN MR-BRT MODEL --------------------------------------------------------
model_name <- paste0("network_logit_", date)
if (!dir.exists(paste0(mrbrt_dir, model_name))) dir.create(paste0(mrbrt_dir, model_name))
logit_transform <- T

formatted_data <- CWData(
  df = mrbrt_dt,
  obs = response,
  obs_se = data_se,
  alt_dorms = "dorm_alt",
  ref_dorms = "dorm_ref",
  study_id = id_vars,
  add_intercept = keep_x_intercept
)

# RUN MRBRT MODEL 
if (file.exists(paste0(mrbrt_dir, model_name, "/model_object.RDS"))){
  results <- readr::read_rds(paste0(mrbrt_dir, model_name, "/model_object.RDS"))
} else {
  results <- CWModel(
    cwdata = formatted_data,
    obs_type = mrbrt_response,
    cov_models = list(CovModel("intercept")),
    gold_dorm = reference_def,
    max_iter = 1000L,
    inlier_pct = (1 - trim)
  )
  save <- save_model_RDS(results, mrbrt_dir, model_name)
}

save_r <- save_mrbrt(results, mrbrt_dir, model_name)

results$fixed_vars
results$beta_sd
results$gamma

graphs_modelfit <- graph_combos(model = results)
graphs_modelfit

# ADJUST THE DATA  -------------------------------------------------
full_dt <- get_definitions(full_dt)
full_dt <- full_dt[[1]]
full_dt$definition <- gsub("_cv_", "", full_dt$definition)
full_dt <- full_dt[mean != 1]
adjusted <- make_adjustment(results, full_dt)
epidb <- copy(adjusted$epidb)

epidb[!is.na(crosswalk_parent_seq), seq := NA]
cw_file_path <- FILEPATH
write.xlsx(epidb, cw_file_path, sheetName = "extraction", row.names = FALSE)
