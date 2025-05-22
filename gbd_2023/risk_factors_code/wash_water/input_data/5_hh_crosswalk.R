
### Purpose: crosswalk HH-level WaSH data to individual
#######################################################

## libraries
library(data.table)
library(magrittr)
library(openxlsx)
library(crosswalk, lib.loc = "FILEPATH")

## settings
gbd_cycle <- "GBD2020"
report_dir <- file.path("FILEPATH", gbd_cycle, "FILEPATH")
microdata_dir <- file.path("FILEPATH", gbd_cycle, "FILEPATH")

## files
report_files <- list.files(report_dir, pattern = "prepped", full.names = TRUE)
microdata_files <- list.files(microdata_dir, pattern = "collapse_wash", full.names = TRUE)

## read in data
microdata <- rbindlist(lapply(microdata_files, fread), fill = T)[var %unlike% "missing"]
reports <- rbindlist(lapply(report_files, fread), fill = T)

## edit microdata standard errors
setnames(microdata, c("mean","standard_error"), c("val","se_ubcov"))
# manually calculate standard error (Wilson interval)
microdata[, se_manual := (1/(1+(1.96^2/sample_size)))*sqrt(((val*(1-val))/sample_size)+((1.96^2)/(4*sample_size^2)))]
# use the Wilson interval SE when the SE from ubcov is 0 or near 0; use ubcov SE otherwise
microdata[se_ubcov < 0.001 & var %unlike% "missing", standard_error := se_manual]
microdata[is.na(standard_error) & var %unlike% "missing", standard_error := se_ubcov]

### let's do some crosswalking!
## water ####
water_xw <- rbind(microdata[var %in% c("wash_water_piped","wash_water_imp_prop")], reports[var %in% c("wash_water_piped","wash_water_imp_prop")], fill = TRUE)
water_xw[cv_HH == 0, level := "individual"]
water_xw[cv_HH == 1, level := "household"]
# set up matched data frame, using only studies that have both HH-level and individual-level data
water_xw_hh <- water_xw[cv_HH == 1, .(nid, ihme_loc_id, year_start, year_end, var, val, standard_error)]
setnames(water_xw_hh, c("val","standard_error"), c("mean_hh","se_hh"))
water_xw_ind <- water_xw[cv_HH == 0, .(nid, ihme_loc_id, year_start, year_end, var, val, standard_error)]
setnames(water_xw_ind, c("val","standard_error"), c("mean_ind","se_ind"))
water_xw_matched <-  merge(water_xw_hh, water_xw_ind, by = c("nid","ihme_loc_id","year_start","year_end","var"))
# restrict means to 0 < x < 1 (we will be crosswalking in logit space, and logit(0) = -Inf and logit(1) = Inf)
water_xw_matched <- water_xw_matched[(mean_hh > 0 & mean_hh < 1) & (mean_ind > 0 & mean_ind < 1)]
# transform to logit space
water_xw_matched[, `:=` (mean_hh_logit = delta_transform(mean_hh, se_hh, "linear_to_logit")[, 1],
                         se_hh_logit = delta_transform(mean_hh, se_hh, "linear_to_logit")[, 2],
                         mean_ind_logit = delta_transform(mean_ind, se_ind, "linear_to_logit")[, 1],
                         se_ind_logit = delta_transform(mean_ind, se_ind, "linear_to_logit")[, 2])]
# get logit difference
water_xw_matched[, `:=` (logit_diff = calculate_diff(water_xw_matched, 
                                                alt_mean = "mean_hh_logit", alt_sd = "se_hh_logit", 
                                                ref_mean = "mean_ind_logit", ref_sd = "se_ind_logit")[,1],
                         logit_diff_se = calculate_diff(water_xw_matched, 
                                                        alt_mean = "mean_hh_logit", alt_sd = "se_hh_logit", 
                                                        ref_mean = "mean_ind_logit", ref_sd = "se_ind_logit")[,2])]
# add alt/ref columns
water_xw_matched[, alt_level := "household"]
water_xw_matched[, ref_level := "individual"]

# run model
water_data <- CWData(
  df = water_xw_matched,
  obs = "logit_diff",
  obs_se = "logit_diff_se",
  alt_dorms = "alt_level",
  ref_dorms = "ref_level",
  study_id = "nid"
)

water_model <- CWModel(
  cwdata = water_data,
  obs_type = "diff_logit",
  cov_models = list(
    CovModel("intercept")
  ),
  gold_dorm = "individual",
  max_iter = 100L, use_random_intercept = TRUE
)

# adjust HH data - restricting to values 0 < x < 1, since the crosswalk was in logit space
# also, we only need to adjust report data (since microdata is extracted & prepped at both HH and individual levels)
reports[cv_HH == 0, level := "individual"]
reports[cv_HH == 1, level := "household"]
water_to_adjust <- reports[var %in% c("wash_water_piped","wash_water_imp_prop") & val != 0 & val != 1]

water_pred <- adjust_orig_vals(
  fit_object = water_model,
  df = water_to_adjust,
  orig_dorms = "level",
  orig_vals_mean = "val",
  orig_vals_se = "standard_error"
)
setDT(water_pred)
setnames(water_pred, names(water_pred), c("mean_adj","se_adj","pred_logit","pred_se_logit","data_id"))

water_to_adjust <- cbind(water_to_adjust, water_pred)

# add the unadjusted data
w_reports_final <- rbind(water_to_adjust, reports[var %in% c("wash_water_piped","wash_water_imp_prop") & val %in% c(0,1)], fill = TRUE)
setnames(w_reports_final, c("val","standard_error"), c("mean_unadj","se_unadj"))
w_reports_final[!is.na(mean_adj), val := mean_adj]
w_reports_final[!is.na(se_adj), standard_error := se_adj]
w_reports_final[is.na(mean_adj), val := mean_unadj]
w_reports_final[is.na(se_adj), standard_error := se_unadj]

## sanitation ####
sani_xw <- rbind(microdata[var %in% c("wash_sanitation_piped","wash_sanitation_imp_prop")], reports[var %in% c("wash_sanitation_piped","wash_sanitation_imp_prop")], fill = TRUE)
sani_xw[cv_HH == 0, level := "individual"]
sani_xw[cv_HH == 1, level := "household"]
# set up matched data frame, using only studies that have both HH-level and individual-level data
sani_xw_hh <- sani_xw[cv_HH == 1, .(nid, ihme_loc_id, year_start, year_end, var, val, standard_error)]
setnames(sani_xw_hh, c("val","standard_error"), c("mean_hh","se_hh"))
sani_xw_ind <- sani_xw[cv_HH == 0, .(nid, ihme_loc_id, year_start, year_end, var, val, standard_error)]
setnames(sani_xw_ind, c("val","standard_error"), c("mean_ind","se_ind"))
sani_xw_matched <-  merge(sani_xw_hh, sani_xw_ind, by = c("nid","ihme_loc_id","year_start","year_end","var"))
# restrict means to 0 < x < 1 (we will be crosswalking in logit space, and logit(0) = -Inf and logit(1) = Inf)
sani_xw_matched <- sani_xw_matched[(mean_hh > 0 & mean_hh < 1) & (mean_ind > 0 & mean_ind < 1)]
# transform to logit space
sani_xw_matched[, `:=` (mean_hh_logit = delta_transform(mean_hh, se_hh, "linear_to_logit")[, 1],
                        se_hh_logit = delta_transform(mean_hh, se_hh, "linear_to_logit")[, 2],
                        mean_ind_logit = delta_transform(mean_ind, se_ind, "linear_to_logit")[, 1],
                        se_ind_logit = delta_transform(mean_ind, se_ind, "linear_to_logit")[, 2])]
# get logit difference
sani_xw_matched[, `:=` (logit_diff = calculate_diff(sani_xw_matched, 
                                                    alt_mean = "mean_hh_logit", alt_sd = "se_hh_logit", 
                                                    ref_mean = "mean_ind_logit", ref_sd = "se_ind_logit")[,1],
                        logit_diff_se = calculate_diff(sani_xw_matched, 
                                                       alt_mean = "mean_hh_logit", alt_sd = "se_hh_logit", 
                                                       ref_mean = "mean_ind_logit", ref_sd = "se_ind_logit")[,2])]
# add alt/ref columns
sani_xw_matched[, alt_level := "household"]
sani_xw_matched[, ref_level := "individual"]

# run model
sani_data <- CWData(
  df = sani_xw_matched,
  obs = "logit_diff",
  obs_se = "logit_diff_se",
  alt_dorms = "alt_level",
  ref_dorms = "ref_level",
  study_id = "nid"
)

sani_model <- CWModel(
  cwdata = sani_data,
  obs_type = "diff_logit",
  cov_models = list(
    CovModel("intercept")
  ),
  gold_dorm = "individual",
  max_iter = 100L, use_random_intercept = TRUE
)

# adjust HH data - restricting to values 0 < x < 1, since the crosswalk was in logit space
# also, we only need to adjust report data (since microdata is extracted & prepped at both HH and individual levels)
reports[cv_HH == 0, level := "individual"]
reports[cv_HH == 1, level := "household"]
sani_to_adjust <- reports[var %in% c("wash_sanitation_piped","wash_sanitation_imp_prop") & val != 0 & val != 1]

sani_pred <- adjust_orig_vals(
  fit_object = sani_model,
  df = sani_to_adjust,
  orig_dorms = "level",
  orig_vals_mean = "val",
  orig_vals_se = "standard_error"
)
setDT(sani_pred)
setnames(sani_pred, names(sani_pred), c("mean_adj","se_adj","pred_logit","pred_se_logit","data_id"))

sani_to_adjust <- cbind(sani_to_adjust, sani_pred)

# add the unadjusted data
s_reports_final <- rbind(sani_to_adjust, reports[var %in% c("wash_sanitation_piped","wash_sanitation_imp_prop") & val %in% c(0,1)], fill = TRUE)
setnames(s_reports_final, c("val","standard_error"), c("mean_unadj","se_unadj"))
s_reports_final[!is.na(mean_adj), val := mean_adj]
s_reports_final[!is.na(se_adj), standard_error := se_adj]
s_reports_final[is.na(mean_adj), val := mean_unadj]
s_reports_final[is.na(se_adj), standard_error := se_unadj]

## water treatment #####
w_treat_reports_final <- reports[var %in% c("wash_no_treat","wash_filter_treat_prop")]

## combine all water/sani/water treatment into one dataset
reports_final <- rbindlist(list(w_reports_final,s_reports_final,w_treat_reports_final), fill = TRUE)

## save
out_file <- paste0("wash_reports_crosswalked_", format(Sys.time(), "%m%d%y"), ".csv")
write.csv(reports_final, file.path(report_dir, out_file), row.names = FALSE)
