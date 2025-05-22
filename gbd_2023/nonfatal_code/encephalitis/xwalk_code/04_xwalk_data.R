#' @author 
#' @date 2019/09/19
#' @description upload and xwalk new clinical and extracted data for encephalitis 

rm(list=ls())

pacman::p_load(openxlsx, parallel, pbapply, boot, data.table)

# SET OBJECTS -------------------------------------------------------------
bundle_id <- 45 # bundle_id for encephalitis
acause <- "encephalitis"
cause <- 337
ds <- 'iterative'   # decomp step
gbd_round_id <- 7
date <- gsub("-", "_", Sys.Date())

# directories
model_objects_dir <- "filepath"
out_dir <- "filepath"
age_pattern_path <- "filepath"

# get bundle version id from bv tracking sheet
bundle_version_dir <- "filepath"
bv_tracker <- fread(paste0(bundle_version_dir, 'bundle_version_tracking.csv'))
bv_row <- bv_tracker[bundle_id == 45 & current_best == 1]
bv_id <- bv_row$bundle_version
print(paste("Crosswalking on bundle version", bv_id, "BV description", bv_row$description))

# model names
sex_model_name <- "2020_08_20_encephalitis_sexsplit_model_obj"
ms2000_xwalk_model_name <- "2020_08_20_claims2000"
ms_xwalk_model_name <- "2020_08_20_claims"
surv_xwalk_model_name <- "2020_01_14_surveillance_null_CF2"

# Specify the column names for the reference and alternative study covariates
ref_name  <- c("cv_inpatient")
alt_names <- c("cv_surveillance", "cv_marketscan_data", "cv_marketscan_inp_2000")

# SOURCE FUNCTIONS --------------------------------------------------------
# Source all GBD shared functions at once
shared.dir <- "filepath"
files.sources <- list.files(shared.dir)
files.sources <- paste0(shared.dir, files.sources) 
invisible(sapply(files.sources, source)) 

helper_dir <- "filepath"
files.sources <- list.files(helper_dir)
files.sources <- paste0(helper_dir, files.sources) # writes the filepath to each function
invisible(sapply(files.sources, source))

repo_dir <- "filepath"
source(paste0(repo_dir, "mr_brt_functions.R"))

# XWALK DATA --------------------------------------------------------------
# get bundle version
bundle_version_dt <- get_bundle_version(bv_id, transform = T, fetch = "all")

# outlier clinical data equal to zero
# bundle_version_dt <- outlier_clinical_data(bundle_version_dt)

# load sex split model and sex split
# only sex split if there is both sex data
if ("Both" %in% unique(bundle_version_dt$sex)){
  sex_split_model <- readRDS(paste0(model_objects_dir, sex_model_name, ".RDS"))
  sex_split_final <- sex_split_data(bundle_version_dt, sex_split_model)
  sex_split_final_dt <- sex_split_final[[1]]
} else if (!"Both" %in% unique(bundle_version_dt$sex)) {
  sex_split_final_dt <- bundle_version_dt
}

sex_split_out_path <- paste0(out_dir, date, "_sex_split_all_data_on_bv_", bv_id, ".xlsx")
write.xlsx(sex_split_final_dt, sex_split_out_path, sheetName = "extraction")
desc <- paste("Sex split only on bv id", bv_id)
result <- save_crosswalk_version(bundle_version_id = bv_id, 
                                 data_filepath = sex_split_out_path, 
                                 description = desc)

# mark each row as one of the alternative case def or reference def
xwalk_dt <- get_study_cov(sex_split_final_dt)

# load crosswalk models and crosswalk
fit_ms2000 <- readRDS(paste0(model_objects_dir, ms2000_xwalk_model_name, ".RDS")) 
fit_ms <- readRDS(paste0(model_objects_dir, ms_xwalk_model_name, ".RDS"))
fit_surv <- readRDS(paste0(model_objects_dir, surv_xwalk_model_name, ".RDS"))

xwalk_dt[, age_mid := (age_start + age_end) / 2]
xwalk_dt[, sex_id := ifelse(sex == "Male", 1, 2)]
xwalk_dt[, year_match := floor((year_start + year_end) / 2)]

# MS 2000 
alt_name <- "cv_marketscan_inp_2000"
ms_2000_pred_dt <- data.table(X_intercept = 1, Z_intercept = 1, unique(xwalk_dt[, .(age_mid)]))
ms_2000_pred_object <- predict_mr_brt(fit_ms2000, newdata = ms_2000_pred_dt)
ms_2000_model_summary_dt <- ms_2000_pred_object$model_summaries
setDT(ms_2000_model_summary_dt)
ms_2000_model_summary_dt[, Y_se := (Y_mean_hi - Y_mean_lo) / (2 * qnorm(0.975))]
ms_2000_model_summary_dt <- ms_2000_model_summary_dt[, .(age_mid = X_age_mid, logit_adj = Y_mean, logit_adj_se = Y_se)]
fwrite(ms_2000_model_summary_dt, paste0(out_dir,date, "_MS_2000_predict_summary.csv"))

# MS DATA xwalk
alt_name <- "cv_marketscan_data"
ms_data_pred_dt <- data.table(X_intercept = 1, Z_intercept = 1, unique(xwalk_dt[, .(age_mid)]))
ms_data_pred_object <- predict_mr_brt(fit_ms, newdata = ms_data_pred_dt)
ms_data_model_summary_dt <- ms_data_pred_object$model_summaries
setDT(ms_data_model_summary_dt)
ms_data_model_summary_dt[, Y_se := (Y_mean_hi - Y_mean_lo) / (2 * qnorm(0.975))]
ms_data_model_summary_dt <- ms_data_model_summary_dt[, .(age_mid = X_age_mid, logit_adj = Y_mean, logit_adj_se = Y_se)]
fwrite(ms_data_model_summary_dt, paste0(out_dir,date, "_MS_claims_predict_summary.csv"))

# Surveillance xwalk
alt_name <- "cv_surveillance"
surv_pred_dt <- data.table(X_intercept = 1, Z_intercept = 1, unique(xwalk_dt[, .(sex_id)]))
surv_pred_object <- predict_mr_brt(fit_surv, newdata = surv_pred_dt)
surv_model_summary_dt <- surv_pred_object$model_summaries
setDT(surv_model_summary_dt)
surv_model_summary_dt[, Y_se := (Y_mean_hi - Y_mean_lo) / (2 * qnorm(0.975))]
surv_model_summary_dt <- surv_model_summary_dt[, .(logit_adj = Y_mean, logit_adj_se = Y_se)]
fwrite(surv_model_summary_dt, paste0(out_dir,date, "surveillance_predict_summary.csv"))

# get definitions
xwalk_dt <- get_def(xwalk_dt, ref_name, alt_names)

# apply crosswalk coefficients to data
xwalk_final_list <- pblapply(1:nrow(xwalk_dt), function(i) {
  row <- xwalk_dt[i]
  adjust_row(row)
}, cl = 19)

xwalk_final_dt <- rbindlist(xwalk_final_list, fill = T)

plot_mean_mean_adj(xwalk_final_dt, acause, out_dir)
fwrite(xwalk_final_dt, paste0(out_dir, date, "_", ds, "_compare_mean_mean_adj_xwalk", ".csv"))
xwalk_final_dt[!is.na(mean_adj), mean := mean_adj]
xwalk_final_dt[!is.na(se_adj), standard_error := se_adj]
fwrite(xwalk_final_dt, paste0(out_dir, date, "_", ds, "xwalk_pre_age_split", ".csv"))

# AGE SPLIT ---------------------------------------------------------------

final_age_split_dt <- age_split_data(xwalk_final_dt, cause, gbd_round_id, ds)

# SAVE AGE-SPLIT XWALK VERSION --------------------------------------------
xwalk_out_path <- paste0(out_dir, date, "_xwalk_with_age_and_sex_split.xlsx")
desc <- paste("gbd 2019 processing, re-crosswalk inpatent-only claims, on bundle version", bv_id)

# DEAL WITH DUMMY SAMPLE SIZE 
final_age_split_dt[note_modeler %like% "dummy" & !note_modeler %like% "WITHIN", group_review := 0]

final_age_split_dt <- find_nondismod_locs(final_age_split_dt)
final_age_split_dt <- final_age_split_dt[group_review != 0 | is.na(group_review)]
final_age_split_dt$unit_value_as_published <- 1
final_age_split_dt[is.na(cv_diag_blood_culture), cv_diag_blood_culture := cv_diag_blood]
final_age_split_dt$cv_diag_blood <- NULL
write.xlsx(final_age_split_dt,
           file = xwalk_out_path,
           sheetName = "extraction")

result <- save_crosswalk_version(bundle_version_id = bv_id, 
                                 data_filepath = xwalk_out_path, 
                                 description = desc)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

if (result$request_status == "Successful") {
  df_tmp <- data.table(bundle_id = bundle_id,
                       bundle_version_id = bv_id,
                       crosswalk_version = result$crosswalk_version_id, 
                       parent_crosswalk_version = NA,
                       is_bulk_outlier = 0,
                       date = date,
                       description = desc)
  
  crosswalk_version_dir <- "filepath"
  cv_tracker <- read.xlsx(paste0(crosswalk_version_dir, 'crosswalk_version_tracking.xlsx'))
  cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
  write.xlsx(cv_tracker, paste0(crosswalk_version_dir, 'crosswalk_version_tracking.xlsx'))
}

