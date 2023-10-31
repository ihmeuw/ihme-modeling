## 8/31/2020
## crosswalks for lead exposure (crosswalking to arithmetic mean and to national-level urbanicity)

library(data.table)
library(magrittr)
library(openxlsx)
library(crosswalk002, lib.loc = "FILEPATH")
source("FILEPATH/clean_mr_brt.R")

source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_location_metadata.R")
locs <- get_location_metadata(location_set_id = 22, gbd_round_id = 7, decomp_step = "iterative")

# read in dataset (post sex split)
dt <- fread("FILEPATH/lead_sex_split.csv")

## CROSSWALK 1 - mean type ##########################################################################################
mean_xw <- copy(dt)
# add reference/alternative dummy vars (mean type)
mean_xw[mean_type == "am_mean", am_mean := 1]
mean_xw[is.na(am_mean), am_mean := 0]
mean_xw[mean_type == "gm_mean", gm_mean := 1]
mean_xw[is.na(gm_mean), gm_mean := 0]
mean_xw[mean_type == "median", median := 1]
mean_xw[is.na(median), median := 0]

# set up matching data frame
mean_xw_clean <- mean_xw[, .(nid, location_id, ihme_loc_id, location_name, sex, sex_id, age_start, age_end, age_group_id, year_start, year_end, year_id,
                   mean, standard_error, standard_deviation, variance, sample_size, measure, mean_type, am_mean, gm_mean, median)]
mean_xw_matched <- match_mr_brt(mean_xw_clean, reference_def = "am_mean", alternate_defs = c("gm_mean","median"))$data
# transform to log space
mean_xw_matched[, log_ratio := delta_transform(ratio, ratio_se, "linear_to_log")[, 1]] # crosswalk package
mean_xw_matched[, log_ratio_se := delta_transform(ratio, ratio_se, "linear_to_log")[, 2]] # crosswalk package
# clean up
setnames(mean_xw_matched, c("comp_1","comp_2"), c("alt_def","ref_def")) # change these column names to something more informative
mean_xw_matched <- merge(locs[, .(super_region_name, location_id)], mean_xw_matched, by = "location_id") # add super region
mean_xw_matched[, sr_sex := .GRP, by = c("super_region_name","sex")] # add identifier for each unique super-region/sex combo (to be used as a random effect)

# run model
lead_mean_data <- CWData(
  df = mean_xw_matched,
  obs = "log_ratio",
  obs_se = "log_ratio_se",
  alt_dorms = "alt_def",
  ref_dorms = "ref_def",
  study_id = "sr_sex" # random effect
)

lead_mean_model <- CWModel(
  cwdata = lead_mean_data,
  obs_type = "diff_log",
  cov_models = list(
    CovModel("intercept")
  ),
  gold_dorm = "am_mean",
  inlier_pct = 0.9, 
  outer_max_iter = 500L, max_iter = 500L, 
  use_random_intercept = TRUE
)

# save lead_mean_model
py_save_object(object = lead_mean_model, filename = "FILEPATH/lead_mean_model.pkl", pickle = "dill")

# adjust data reported as geometric mean or median
to_adjust <- mean_xw[mean_type != "am_mean"]

lead_mean_pred <- adjust_orig_vals(
  fit_object = lead_mean_model,
  df = to_adjust,
  orig_dorms = "mean_type",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)
setDT(lead_mean_pred)
setnames(lead_mean_pred, names(lead_mean_pred), c("mean_adj","se_adj","pred_log","pred_se_log","data_id"))

to_adjust <- cbind(to_adjust, lead_mean_pred)
to_adjust[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]

# combine crosswalked data with non-crosswalked data
mean_xw_final <- rbind(mean_xw[mean_type == "am_mean"], to_adjust, fill = TRUE)
# clean things up
mean_xw_final[is.na(mean_adj), mean_adj := mean]
setnames(mean_xw_final, c("mean","standard_error","mean_adj"), c("mean_orig","se_orig","val"))
mean_xw_final[mean_type == "am_mean", standard_error := se_orig] # for unadjusted data points, use original (post sex split) SE
mean_xw_final[is.na(standard_error), standard_error := se_adj] # for adjusted data points, use adjusted SE
mean_xw_final[, variance := (standard_error*sqrt(sample_size))^2] # SE = SD/sqrt(n), variance = SD^2

## CROSSWALK 2 - urbanicity ##########################################################################################
# Adjust data urbanicity to the national average
# here, the variable urban_study represents the urbanicity of the datapoint
# whereas the variable urban_loc represents the average urbanicity in that location (taken from the GBD urbanicity covariate)
# we want to crosswalk data point urbanicity to what we would expect it to be if representative of its location

# get urbanicity covariate
prop_urban <- get_covariate_estimates(covariate_id = 854, decomp_step = "iterative", gbd_round_id = 7)
#off-set values of 0 or 1 because they will be logit inputs
prop_urban[mean_value >= 1, mean_value := 0.99999]
prop_urban[mean_value <= 0, mean_value := 0.00001]
setnames(prop_urban, "mean_value", "urban_loc")
prop_urban <- prop_urban[, .(location_id, year_id, urban_loc)]

# merge onto data
urb_xw <- merge(mean_xw_final[, -c("se_adj","pred_log","pred_se_log","data_id")], prop_urban, by = c("location_id","year_id"), all.x=T)
# fix typo
urb_xw[representative_name == "Nationall representative", representative_name := "Nationally representative"]

# When point data is representative, copy over location's prop_urban, otherwise use the data to infer urbanicity
# When urbanicity unknown, copy over location's prop_urban as well
urb_xw[representative_name %in% c("Nationally representative only","Nationally representative","Representative for subnational location only") | 
                urbanicity_type == "0", urban_study := urban_loc] # for representative data points, use the urbanicity from the GBD covariate
urb_xw[is.na(urban_study) & urbanicity_type %in% c("2","Urban"), urban_study := 0.99999] # for urban data points, set to 0.99999
urb_xw[is.na(urban_study) & urbanicity_type %in% c("3","Rural"), urban_study := 0.00001] # for rural data points, set to 0.00001
urb_xw[is.na(urban_study) & urbanicity_type %in% c("Mixed/both","Suburban"), urban_study := 0.5] # for suburban, set to 0.5
urb_xw[is.na(urban_study) & urbanicity_type %in% c("1","Unknown"), urban_study := urban_loc] # for data points missing urbanicity type, use GBD covariate
urb_xw[grepl("IND_",ihme_loc_id), urban_study := urban_loc] # use GBD covariate for all of India's subnationals

# reference data points are the ones where the study urbanicity (urban_study) equals the location average urbanicity (urban_loc)
urb_xw <- urb_xw[urban_loc == urban_study, urb_ref := 1]
urb_xw <- urb_xw[urban_loc != urban_study, urb_ref := 0]
# alternate data points are everything else
urb_xw <- urb_xw[urban_loc != urban_study, urb_alt := 1]
urb_xw <- urb_xw[urban_loc == urban_study, urb_alt := 0]
# summary column
urb_xw[urb_ref == 1, urban_type := "urb_ref"]
urb_xw[urb_alt == 1, urban_type := "urb_alt"]

# set up matching data frame
urb_xw_clean <- urb_xw[, .(nid, location_id, ihme_loc_id, location_name, sex, sex_id, age_start, age_end, age_group_id, year_start, year_end, year_id,
                   val, standard_error, standard_deviation, variance, sample_size, measure, urb_ref, urb_alt)]
urb_xw_matched <- match_mr_brt(urb_xw_clean, reference_def = "urb_ref", alternate_defs = "urb_alt")$data
# transform to log space
urb_xw_matched[, log_ratio := delta_transform(ratio, ratio_se, "linear_to_log")[, 1]] # crosswalk package
urb_xw_matched[, log_ratio_se := delta_transform(ratio, ratio_se, "linear_to_log")[, 2]] # crosswalk package
# clean up
setnames(urb_xw_matched, c("comp_1","comp_2"), c("alt_def","ref_def")) # change these column names to something more informative
urb_xw_matched <- merge(locs[, .(super_region_name, location_id)], urb_xw_matched, by = "location_id") # add super region (to be used as a random effect)

# run model
lead_urb_data <- CWData(
  df = urb_xw_matched,
  obs = "log_ratio",
  obs_se = "log_ratio_se",
  alt_dorms = "alt_def",
  ref_dorms = "ref_def",
  study_id = "super_region_name" # random effect
)

lead_urb_model <- CWModel(
  cwdata = lead_urb_data,
  obs_type = "diff_log",
  cov_models = list(
    CovModel("intercept")
  ),
  gold_dorm = "urb_ref",
  inlier_pct = 0.9, 
  outer_max_iter = 500L, max_iter = 500L, 
  use_random_intercept = TRUE
)

# save lead_urb_model
py_save_object(object = lead_urb_model, filename = "FILEPATH/lead_urb_model.pkl", pickle = "dill")

# adjust data reported as geometric mean or median
to_adjust <- urb_xw[urban_type == "urb_alt"]

lead_urb_pred <- adjust_orig_vals(
  fit_object = lead_urb_model,
  df = to_adjust,
  orig_dorms = "urban_type",
  orig_vals_mean = "val",
  orig_vals_se = "standard_error"
)
setDT(lead_urb_pred)
setnames(lead_urb_pred, names(lead_urb_pred), c("mean_adj","se_adj","pred_log","pred_se_log","data_id"))

to_adjust <- cbind(to_adjust, lead_urb_pred)
to_adjust[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]

# combine crosswalked data with non-crosswalked data
urb_xw_final <- rbind(urb_xw[urban_type == "urb_ref"], to_adjust, fill = TRUE)
# clean things up
urb_xw_final[is.na(mean_adj), mean_adj := val]
setnames(urb_xw_final, c("val","standard_error","mean_adj"), c("mean_post_mean_type_xw", # save a copy of the data post-mean type crosswalk
                                                               "se_post_mean_type_xw", # ditto for standard error
                                                               "val"))
urb_xw_final[urban_type == "urb_ref", standard_error := se_post_mean_type_xw] # for unadjusted data points, use original (post-mean type crosswalk) SE
urb_xw_final[is.na(standard_error), standard_error := se_adj] # for adjusted data points, use adjusted SE
urb_xw_final[, variance := (standard_error*sqrt(sample_size))^2] # SE = SD/sqrt(n), variance = SD^2

# finalize & save
load("FILEPATH/age_start_end.Rdata") # uploading bundle data requires an age_group_id, so for all the non-standard ages (e.g. 6-17), i put in age_group_id 22
                                                  # unfortunately, that coerces all of the age starts to 0 and age ends to 125
                                                  # so, need to add back in the original age starts and age ends here
setnames(urb_xw_final, c("age_start","age_end"), c("age_start_orig","age_end_orig"))
urb_xw_final <- merge(urb_xw_final, age_start_end, by = "index_col", all.x = TRUE)
urb_xw_final[is.na(age_start), age_start := age_start_orig]
urb_xw_final[is.na(age_end), age_end := age_end_orig]
lead_xw_final <- urb_xw_final[, .(nid, underlying_nid, page_num, table_num, source_type, location_id, ihme_loc_id, location_name, smaller_site_unit, site_memo, 
                                  sex, sex_id, year_start, year_end, year_id, age_start, age_end, age_group_id, measure, val, variance, standard_error, 
                                  standard_deviation, sample_size, seq, origin_seq, crosswalk_parent_seq, is_outlier, representative_name, urbanicity_type, 
                                  mean_type, urban_type, note_SR, note_modeler, extractor, year_start_orig, year_end_orig, mean_orig, se_orig, 
                                  mean_post_mean_type_xw, se_post_mean_type_xw)]
lead_xw_final[!is.na(crosswalk_parent_seq), seq := NA]
lead_xw_final[, standard_deviation := sqrt(variance)]
write.csv(lead_xw_final, "FILEPATH/lead_xw_final.csv", row.names = FALSE)
# also save a copy for bradmod (age-splitting)
write.csv(lead_xw_final[, -c("site_memo","note_SR","note_modeler")], # these columns are giving stata trouble, need to delete to run bradmod
          "FILEPATH/bradmod_input.csv", row.names = FALSE)
