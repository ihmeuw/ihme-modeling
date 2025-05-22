## Boilerplate
library(metafor)
library(msm)
library(plyr)
library(dplyr)
library(boot)
library(ggplot2)
library(openxlsx)
library(readxl)
library(reticulate)

save_crosswalks <- FALSE

reticulate::use_python("FILEPATH")
cw <- import("crosswalk")

source('FILEPATH')
source("FILEPATH")

#read in the data
df_orig <- read.csv(paste0(modeling_dir, "FILEPATH",date,".csv"))
df_matched <- read.csv(paste0(modeling_dir, "FILEPATH",date,".csv"))

df_orig <- df_orig %>%
  mutate(obs_method = ifelse(cv_not_poland == 1, "not_poland", "poland"))

df_matched$year_id <- floor((df_matched$year_end + df_matched$year_start)/2)
df_matched$age_mid <- floor((df_matched$age_end + df_matched$age_start)/2)

df_matched$match_id <- paste0(df_matched$ref_nid,"_",df_matched$alt_nid)

df_matched$altvar <- "not_poland"
df_matched$refvar <- "poland"


df_cw_matched <- cw$CWData(
  df = df_matched,          # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  study_id = "match_id",  # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)


fit_not_poland <- cw$CWModel(
  cwdata = df_cw_matched,  # object returned by `CWData()`
  obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(cw$CovModel)
    cw$CovModel(cov_name = "intercept")),
  gold_dorm = "poland"   # the level of `alt_dorms` that indicates it's the gold standard
)



fit_not_poland$fit(
  inlier_pct=0.9 # trim 10%
)


df_tmp <- fit_not_poland$create_result_df()


preds_not_poland <- fit_not_poland$adjust_orig_vals( #no need to put data into logit space beforehand!!
  df = df_orig,
  orig_dorms = "obs_method",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error",
  data_id = "rownum"   # optional argument to add a user-defined ID to the predictions; name of the column with the IDs
)

control <- lapply(preds_not_poland)
df_orig$mean_adj <- preds_not_poland$ref_vals_mean
df_orig$standard_error_adj <- preds_not_poland$ref_vals_sd


###FIX FOR STANDARD ERRORS THAT ARE TOO LARGE###
df_orig$standard_error_adj[df_orig$standard_error_adj>1] <- 1


df_orig$upper_adj <- df_orig$mean_adj + 1.96*df_orig$standard_error_adj
df_orig$lower_adj <- df_orig$mean_adj - 1.96*df_orig$standard_error_adj


# replace upper>=1 with 1- (1-highest_non_zero_upr)/5:
highest_non_zero_upr <- max(df_orig$upper_adj[df_orig$upper_adj < 1])
new_upper_adj <- 1- (1-highest_non_zero_upr)/5
df_orig <- df_orig %>%
  mutate(upper_adj = ifelse(upper_adj >= 1, new_upper_adj, upper_adj))

# Step 1: Calculate half the lowest non-zero value in 'lower'
half_lowest_non_zero_lower_adj <- min(df_orig$lower_adj[df_orig$lower_adj > 0]) / 2
# Step 2: Replace zeros with this value in 'lower_adj'
df_orig <- df_orig %>%
  mutate(lower_adj = ifelse(lower_adj <= 0, half_lowest_non_zero_lower_adj, lower_adj))
# This is very much a hack. Ask what to do properly. ###########################
df_orig <- df_orig %>%
  mutate(lower_adj = ifelse(lower_adj < mean_adj, lower_adj, mean_adj/2))

##########################################################################
## seq processing ##
##########################################################################
df_orig <- df_orig %>%
  mutate(
    crosswalk_parent_seq = case_when(
      obs_method == "not_poland" & is.na(crosswalk_parent_seq) ~ seq,
      TRUE ~ crosswalk_parent_seq
    ),
    seq = case_when(
      obs_method == "not_poland" & is.na(crosswalk_parent_seq) ~ NA_real_,
      TRUE ~ seq
    )
  )

### check columns:
set1 <- c("source_type", "location_id", "sex", "measure", "representative_name", "urbanicity_type", "recall_type", "unit_type", "unit_value_as_published")
#check if set1 exist in the columns:
setdiff(set1, colnames(df_orig))

# check if set2 exist in the columns and is not null:
set2 <- c("nid", "year_start", "year_end", "age_start", "age_end", "is_outlier")
setdiff(set2, colnames(df_orig))
na_count_set2 <- df_orig %>% select(set2) %>% filter(if_any(set2, ~is.na(.))) %>% nrow()
print(na_count_set2)

#check if set3 exist in the columns
set3 <- c("seq", "underlying_nid", "input_type", "design_effect", "recall_type_value", "uncertainty_type", "sampling_type", "effective_sample_size")
setdiff(set3, colnames(df_orig))

set4 <- c(set1, set2, set3)
setdiff(colnames(df_orig), set4)
df_orig <- df_orig %>% select(-starts_with("cv"))
df_orig <- df_orig %>% select(-c("origin_id", "seq_parent", "step2_location_year", "modelable_entity_name1", "modelable_entity_name.1",
                                 "modelable_entity_id.1", "year_id", "rownum", "obs_method"))

# Save final data to upload as crosswalk version:
write.xlsx(df_orig, file = paste0(modeling_dir, "FILEPATH",date,".xlsx"), sheetName = "extraction", overwrite = TRUE)
source("FILEPATH")
data_filepath <- paste0(modeling_dir, "FILEPATH",bundle_id,"_",date,".xlsx")
description <- "clinical data + selected studies"
result <- save_crosswalk_version(
  bundle_version_id=bvid,
  data_filepath=data_filepath,
  description=description)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
