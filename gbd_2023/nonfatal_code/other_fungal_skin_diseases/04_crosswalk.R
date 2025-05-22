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

save_crosswalks <- TRUE

reticulate::use_python("FILEPATH")
cw <- import("crosswalk")

source('FILEPATH')
source("FILEPATH")


#read in the data
df_orig <- read.csv(paste0(modeling_dir, "FILEPATH",bundle_id,"_",date,".csv"))
df_matched <- read.csv(paste0(modeling_dir, "FILEPATH",bundle_id,"_",date,".csv"))


df_orig <- df_orig %>%
  mutate(obs_method = ifelse(cv_not_poland == 1, "not_poland", "poland"))


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

df_result <- df_orig
df_result$mean_adj <- preds_not_poland$ref_vals_mean
df_result$standard_error_adj <- preds_not_poland$ref_vals_sd



### FIX STANDARD ERRORS THAT ARE TOO LARGE###
df_result$standard_error_adj[df_result$standard_error_adj>1] <- 1


df_result$upper_adj <- df_result$mean_adj + 1.96*df_result$standard_error_adj
df_result$lower_adj <- df_result$mean_adj - 1.96*df_result$standard_error_adj


# replace upper>=1 with 1- (1-highest_non_zero_upr)/5:
highest_non_1_upr <- max(df_result$upper_adj[df_result$upper_adj < 1])
new_upper_adj <- 1- (1-highest_non_1_upr)/5
df_result <- df_result %>%
  mutate(upper_adj = ifelse(upper_adj >= 1, new_upper_adj, upper_adj))

# Step 1: Calculate half the lowest non-zero value in 'lower'
half_lowest_non_zero_lower_adj <- min(df_result$lower_adj[df_result$lower_adj > 0]) / 2
# Step 2: Replace zeros with this value in 'lower_adj'
df_result <- df_result %>%
  mutate(lower_adj = ifelse(lower_adj <= 0, half_lowest_non_zero_lower_adj, lower_adj))

df_result <- df_result %>%
  mutate(lower_adj = ifelse(lower_adj < mean_adj, lower_adj, mean_adj/2))

##########################################################################
## seq processing ##
##########################################################################
df_result <- df_result %>%
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

### data cleanup and formatting:
df_result <- df_result %>%
  select(-c(lower, upper, mean, standard_error)) %>%
  dplyr::rename(lower = lower_adj,
         upper = upper_adj,
         mean = mean_adj,
         standard_error = standard_error_adj)

### check columns:
set1 <- c("source_type", "location_id", "sex", "measure", "representative_name", "urbanicity_type", "recall_type", "unit_type", "unit_value_as_published")
#check if set1 exist in the columns:
setdiff(set1, colnames(df_result))

# check if set2 exist in the columns and is not null:
set2 <- c("nid", "year_start", "year_end", "age_start", "age_end", "is_outlier")
setdiff(set2, colnames(df_result))
na_count_set2 <- df_result %>% select(set2) %>% filter(if_any(set2, ~is.na(.))) %>% nrow()
print(na_count_set2)

#check if set3 exist in the columns
set3 <- c("seq", "underlying_nid", "input_type", "design_effect", "recall_type_value", "uncertainty_type", "sampling_type", "effective_sample_size")
setdiff(set3, colnames(df_result))

set4 <- c(set1, set2, set3)
setdiff(colnames(df_result), set4)

cv_columns <- grep("^cv", names(df_result), value = TRUE)
# Exclude 'cv_marketscan', 'cv_notpoland' and 'cv_poland' from the list
cv_columns <- cv_columns[!cv_columns %in% c("cv_marketscan", "cv_poland", "cv_not_poland")]

df_result <- df_result %>% select(-cv_columns)
df_result <- df_result %>% select(-c("origin_id", "year_id", "rownum", "obs_method"))
names(df_result)


unknown_recall_types <-  df_result %>% filter(is.na(recall_type_value) & recall_type!="Point" & recall_type!= "Not Set")
df_final <- anti_join(df_result, unknown_recall_types, by = names(df_result))

# recall_type_value must be null or >0. excluding rows with recall_type_value=0.0:
recall_type_value_0 <-  df_result %>% filter(recall_type_value==0.0)
df_final <- anti_join(df_final, recall_type_value_0, by = names(df_final))

# effective_sample_size>= cases is necessary. excluding rows that does not satisfy this criteria:
sample_szie_check <- df_result %>% filter(effective_sample_size < cases)
df_final <- anti_join(df_final, sample_szie_check, by = names(df_final))

## filter df_result to where group_reviewis 1 or null for crosswalk:
df_filtered <- df_final[df_final$group_review == 1 | is.na(df_final$group_review), ]


# Save final data to upload as crosswalk version:
write.xlsx(df_filtered, file = paste0(modeling_dir, "FILEPATH",bundle_id,".xlsx"), sheetName = "extraction", overwrite = TRUE)


# SAVE CROSSWALK VERSION: ---------------------------------------------------------------
source("FILEPATH")

data_filepath <- paste0(modeling_dir, "FILEPATH",bundle_id,".xlsx")
description <- "GBD2023-age & location based outliers applied"
result <- save_crosswalk_version(
  bundle_version_id=bvid,
  data_filepath=data_filepath,
  description=description)
