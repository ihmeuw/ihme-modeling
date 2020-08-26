#' @author 
#' @date 2019/09/13
#' @description upload and xwalk new clinical and extracted data for meningitis 
#'              bundle 28 for GBD 2019 decomp 4
#' @note uses GBD 2019 decomp 4 population for sample sizes
#' 
#' # this was the final crosswalking code used for GBD 2019

rm(list=ls())

pacman::p_load(openxlsx, parallel, pbapply, ggplot2)

# SET OBJECTS -------------------------------------------------------------
# Toggles
save_bv <- FALSE # toggle TRUE/FALSE if you want to save a bundle version

bundle_id <- 28
ds <- 'step4'
CF_type <- 'CF2'
# note - bundle is decomp exempt, must use bv_id that includes both step2 and step4 data
bv_id <- 18884 # includes step4 and step2 lit, ALL step claims, ALL step CF2 inp, no bad NIDs

date <- gsub("-", "_", Sys.Date())

upload_dir <- # filepath
in_filepath <- # filepath
out_filepath <- # filepath
xwalk_out_path<- # filepath

mrbrt_dir <- # filepath
model_objects_dir <- # filepath
model_plot_dir <- # filepath

sex_model_name <- "2020_01_12_meningitis_sexsplit_model_obj"

ms2000_xwalk_model_name <- paste0("2020_01_15_claims_2000_", CF_type, "_keep_grouping")
ms_xwalk_model_name <- paste0("2020_01_15_claims_2010_2016_", CF_type, "_keep_grouping")
pop_surv_xwalk_model_name <- paste0("2020_01_15_surveillance_haqi_", CF_type, "_keep_grouping")

# Specify the column names for the reference and alternative study covariates
ref_name  <- c("cv_inpatient")
alt_names <- c("cv_population_surveillance", "cv_marketscan_data", 
               "cv_marketscan_inp_2000")

age_pattern_path <- # filepath

# SOURCE FUNCTIONS --------------------------------------------------------
k <- # filepath
source(paste0(k, "current/r/get_population.R"))
source(paste0(k, "current/r/get_age_metadata.R"))
source(paste0(k, "current/r/get_location_metadata.R"))
source(paste0(k, "current/r/validate_input_sheet.R"))
source(paste0(k, "current/r/upload_bundle_data.R"))
source(paste0(k, "current/r/save_bundle_version.R"))
source(paste0(k, "current/r/get_bundle_version.R"))
source(paste0(k, "current/r/save_crosswalk_version.R"))
source(paste0(k, "current/r/get_covariate_estimates.R"))

helper_dir <- # filepath
source(paste0(helper_dir, "helper_functions.R"))

repo_dir <- # filepath
source(paste0(repo_dir, "mr_brt_functions.R"))
library(gtools)

# XWALK DATA --------------------------------------------------------------
# save bundle version if toggle is on
if (save_bv) {
  request <- save_bundle_version(bundle_id, ds, include_clinical = T)
  
  print(sprintf('Request status: %s', request$request_status))
  print(sprintf('Request ID: %s', request$request_id))
  print(sprintf('Bundle version ID: %s', request$bundle_version_id))
  print(sprintf('Bundle version ID from decomp 2/3 best model: %s', request$previous_step_bundle_version_id))
}

# get bundle version
bundle_version_dt <- get_bundle_version(bv_id, transform = T, fetch = 'all')
bundle_version_dt <- outlier_clinical_data(bundle_version_dt)

# load sex split and sex split
sex_split_model <- readRDS(paste0(model_objects_dir, sex_model_name, ".RDS"))
sex_split_final_dt <- split_data(bundle_version_dt, sex_split_model)

# drop ABC rows, these rows were used for crosswalking, but will not be used in 
# modeling overall meningitis since they did not capture all bacterial meningitis
abc_nids <-
  c(
    404787, 317476, 336847, 408680, 411100, 407536, 408336, 411786, 404395, 
    411787, 354896, 397812, 397813, 397814, 412261, 412260, 412259, 412258, 
    412257, 412256, 412255, 412254, 412252, 412251, 412250, 412249, 412248, 
    412247, 412246, 412245, 412244, 412241, 412240, 412239, 412222, 412266,
    412267, 412268, 412269, 412270, 412271, 412272, 412273, 412274, 412275, 
    412276, 412277, 412278, 412279, 412280, 412287, 412302, 412305, 412306, 
    412307, 412308, 412353, 412352, 412351, 412342, 412341, 412338, 412337, 
    412336, 412333, 412332, 412331, 412330, 412329, 412326, 412323, 412318, 
    412317, 412316, 412315, 412314, 412309
  )
sex_split_final_dt <- sex_split_final_dt[!nid %in% abc_nids]
fwrite(sex_split_final_dt, paste0(mrbrt_dir, date, "_", CF_type, "_", "sex_split_all_data_final.csv"))

# mark each row as one of the alternative case def or reference def
xwalk_dt <- get_study_cov(sex_split_final_dt)

# load crosswalk models and crosswalk
fit_ms2000 <- readRDS(paste0(model_objects_dir, ms2000_xwalk_model_name, ".RDS")) 
fit_ms <- readRDS(paste0(model_objects_dir, ms_xwalk_model_name, ".RDS"))
fit_pop_surv <- readRDS(paste0(model_objects_dir, pop_surv_xwalk_model_name, ".RDS"))

xwalk_dt[, age_mid := (age_start + age_end) / 2]
xwalk_dt[, sex_id := ifelse(sex == "Male", 1, 2)]
xwalk_dt[, year_match := floor((year_start + year_end) / 2)]

#get haq for popuation surveillance xwalk
haq_dt <- get_covariate_estimates(1099, gbd_round_id = 6, decomp_step = ds)
haq_dt <- haq_dt[, .(location_id, year_match = year_id, haqi = mean_value)]
xwalk_dt <- merge(xwalk_dt, haq_dt, by = c("location_id", "year_match"))

# MS 2000 xwalk
alt_name <- "cv_marketscan_inp_2000"
ms_2000_pred_dt <- data.table(X_intercept = 1, Z_intercept = 1, unique(xwalk_dt[, .(age_mid)]))
ms_2000_pred_object <- predict_mr_brt(fit_ms2000, newdata = ms_2000_pred_dt)
ms_2000_model_summary_dt <- ms_2000_pred_object$model_summaries
setDT(ms_2000_model_summary_dt)
ms_2000_model_summary_dt[, Y_se := (Y_mean_hi - Y_mean_lo) / (2 * qnorm(0.975))]
ms_2000_model_summary_dt <- ms_2000_model_summary_dt[, .(age_mid = X_age_mid, logit_adj = Y_mean, logit_adj_se = Y_se)]
fwrite(ms_2000_model_summary_dt, paste0(model_objects_dir,"step4_", CF_type, "_MS_2000_predict_summary.csv"))

# MS DATA xwalk
alt_name <- "cv_marketscan_data"
ms_data_pred_dt <- data.table(X_intercept = 1, Z_intercept = 1, unique(xwalk_dt[, .(age_mid)]))
ms_data_pred_object <- predict_mr_brt(fit_ms, newdata = ms_data_pred_dt)
ms_data_model_summary_dt <- ms_data_pred_object$model_summaries
setDT(ms_data_model_summary_dt)
ms_data_model_summary_dt[, Y_se := (Y_mean_hi - Y_mean_lo) / (2 * qnorm(0.975))]
ms_data_model_summary_dt <- ms_data_model_summary_dt[, .(age_mid = X_age_mid, logit_adj = Y_mean, logit_adj_se = Y_se)]
fwrite(ms_data_model_summary_dt, paste0(model_objects_dir,"step4_", CF_type, "_MS_data_predict_summary.csv"))

# Surveillance xwalk
alt_name <- "cv_population_surveillance"
surv_pred_dt <- data.table(X_intercept = 1, Z_intercept = 1, unique(xwalk_dt[, .(haqi)]))
surv_pred_object <- predict_mr_brt(fit_pop_surv, newdata = surv_pred_dt)
surv_model_summary_dt <- surv_pred_object$model_summaries
setDT(surv_model_summary_dt)
surv_model_summary_dt[, Y_se := (Y_mean_hi - Y_mean_lo) / (2 * qnorm(0.975))]
surv_model_summary_dt <- surv_model_summary_dt[, .(haqi = X_haqi, logit_adj = Y_mean, logit_adj_se = Y_se)]
fwrite(surv_model_summary_dt, paste0(model_objects_dir,"step4_", CF_type, "_surveillance_predict_summary.csv"))

xwalk_dt <- get_def(xwalk_dt, ref_name, alt_names)

xwalk_final_list <- pblapply(1:nrow(xwalk_dt), function(i) {
  row <- xwalk_dt[i]
  adjust_row(row)
})
xwalk_final_dt <- rbindlist(xwalk_final_list, fill = T)

plot_mean_mean_adj(xwalk_final_dt)
fwrite(xwalk_final_dt, paste0(mrbrt_dir, date, "_", CF_type, "_compare_mean_mean_adj_step4_xwalk", ".csv"))
xwalk_final_dt[!is.na(mean_adj), mean := mean_adj]
xwalk_final_dt[!is.na(se_adj), standard_error := se_adj]
fwrite(xwalk_final_dt, paste0(mrbrt_dir, date, "_", CF_type, "_step4_xwalk", ".csv"))

# AGE SPLIT ---------------------------------------------------------------
# pull gbd age group mapping
age_dt <- get_age_metadata(12, gbd_round_id = 6)
age_dt[, age_group_weight_value:= NULL]
setnames(age_dt, c("age_group_years_start", "age_group_years_end"), c("gbd_age_start", "gbd_age_end"))

# age split rows with age range greater than 25 years but exclude the 95+ age group in clinical data and rows with no sample size and group review 0 rows
split_rows <- xwalk_final_dt[, .I[measure == "incidence"
                                  & (age_end - age_start > 25 & !(age_start == 95 & age_end == 124))
                                  & !(is.na(sample_size) & is.na(effective_sample_size))
                                  & (group_review == 1 | is.na(group_review))]] 
split_dt <- xwalk_final_dt[split_rows]
no_split_dt <- xwalk_final_dt[!(split_rows)]
if (nrow(split_dt) + nrow(no_split_dt) != nrow(xwalk_final_dt)) {
  stop("Dataframe to be age-split and dt not to be split are not mutually exclusive and collectively exhauastive")
}
# fill in cases, sample size, mean
split_dt <- get_cases_sample_size(split_dt)
# convert to age_start and age_end to closest gbd age groups
split_dt[, gbd_age_start:= sapply(1:nrow(split_dt), function(i) {
  age <- split_dt[i, age_start]
  get_closest_age(start = T, age)
})]
split_dt[, gbd_age_end:= sapply(1:nrow(split_dt), function(i) {
  age <- split_dt[i, age_end]
  get_closest_age(start = F, age)
})]
# get starting and ending gbd age group ids
split_dt <- get_gbd_age_group_id(split_dt, age_dt)
age_dt[, most_detailed:= NULL]
age_dt$age_group_name <- NULL
# split denominator using location-specific population distribution
denominator_split_dt_list <- pblapply(1:nrow(split_dt), function(i) {
  #print(paste0(i, " out of ", nrow(split_dt), " denominators split"))
  split_denominator(split_dt[i], age_dt)
}, cl = 19)
split_dt <- rbindlist(denominator_split_dt_list)

# split numerator using global age pattern
global_cases_dt <- fread(age_pattern_path)
numerator_split_dt_list <- pblapply(1:nrow(split_dt), function(i) {
  # print(paste0(i, " out of ", nrow(split_dt), " numerators split"))
  split_numerator(split_dt[i], global_cases_dt)
}, cl = 19)
split_dt <- rbindlist(numerator_split_dt_list)

# check that age-split cases and sample sizes sum up to the original
if (!bool_check_age_splits(split_dt)) {
  stop(print("Age-split sample sizes and cases do not sum up to the originals"))
}
split_dt[, crosswalk_parent_seq := seq]
split_dt[, age_split_mean := age_split_cases / age_split_sample_size]
split_dt[!is.na(specificity) & specificity != "", specificity := paste0(specificity, " | age")]
split_dt[is.na(specificity) | specificity == "", `:=`(group = 1, group_review = 1, specificity = "age")]
split_dt[, note_modeler:= paste0(note_modeler, "| multiplied by ", round(age_split_mean / mean, 2), " to age-split")]
split_dt[, `:=`(mean = age_split_mean, cases = age_split_cases, sample_size = age_split_sample_size, 
                effective_sample_size = age_split_sample_size, age_start = gbd_age_start, age_end = gbd_age_end)]
cols.remove <- c("age_split_mean", "age_split_cases", "age_split_sample_size", "age_group_id", "age_group_id_start", 
                 "age_group_id_end", "gbd_age_start", "gbd_age_end")
split_dt[, (cols.remove) := NULL]
final_age_split_dt <- rbind(no_split_dt, split_dt)

final_age_split_dt <- remove_nondismod_locs(final_age_split_dt)
fwrite(final_age_split_dt, paste0(mrbrt_dir, date, "_", CF_type, "_step4_xwalk_with_age_split", ".csv"))

# SAVE AGE-SPLIT XWALK VERSION --------------------------------------------
final_age_split_dt <- final_age_split_dt[group_review != 0 | is.na(group_review)]

# fake "age-split" both sex EMR and CFR rows
faux_split <- final_age_split_dt[sex == "Both"]
faux_split_male <- faux_split[, sex := "Male"][, sex_id := 1]
faux_split_male <- copy(faux_split_male)
faux_split_female <- faux_split[, sex := "Female"] [, sex_id := 2]
faux_split <- rbind(faux_split_male, faux_split_female)
faux_split$seq <- NULL

final_age_split_dt <- final_age_split_dt[sex != "Both"]
final_age_split_dt2 <- rbind(final_age_split_dt, faux_split, fill = T)

write.xlsx(final_age_split_dt2, file = xwalk_out_path, sheetName = "extraction")

result <- save_crosswalk_version(bundle_version_id = bv_id, 
                                 data_filepath = xwalk_out_path, 
                                 description = paste0("GBD 2019", CF_type, "decomp exempt data no bad NIDs"))

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
