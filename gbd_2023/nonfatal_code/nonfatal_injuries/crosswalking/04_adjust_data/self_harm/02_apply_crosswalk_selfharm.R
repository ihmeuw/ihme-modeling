rm(list=ls())

library(reticulate)
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")

library(data.table)

#########################################
#########################################
########### MR-BRT ADJUSTMENT ###########
#########################################
#########################################

df_orig <- fread("FILEPATH")

df_orig$age_midpoint <- (df_orig$age_start + df_orig$age_end) / 2

# deal with mean of 0
df_orig[df_orig$mean == 0, "mean"] <- 0.0000000000000000000000001

# load in MR-BRT model
fit1 <-
  py_load_object(
    filename = "FILEPATH", pickle = "dill")

# make a dummy column to indicate rows to adjust (outpatient) / not adjust (inpatient)
df_orig$dorm <- ifelse(df_orig$cv_inpatient==1, "inpatient", "outpatient")

# make a dummy column to indicate which rows are recv_care
df_orig$adj_rc <- ifelse(df_orig$cv_outpatient==0 & df_orig$cv_inpatient==0, 1, 0)

df_pred <- df_orig

# predict ratios based on age midpoint
df_pred[, c("mean_adjusted", "se_adjusted", "diff", "diff_se", "data_id")] <- fit1$adjust_orig_vals(
  df = df_pred,            # original data with obs to be adjusted
  orig_dorms = "dorm", # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error"  # standard error of original mean
)

# apply crosswalks to mean of non-outpatient data (people who received care but we don"t know what type)
df_pred$mean_adjusted <- ifelse(df_pred$adj_rc==1,
                                df_pred$mean / (exp(df_pred$diff) + 1),
                                df_pred$mean_adjusted)

# create confidence interval
df_pred$lo_adjusted <- df_pred$mean_adjusted - 1.96*df_pred$se_adjusted
df_pred$hi_adjusted <- df_pred$mean_adjusted + 1.96*df_pred$se_adjusted

df_pred$lo_adjusted <-
  ifelse(
    df_pred$mean_adjusted == 0,
    0,
    df_pred$lo_adjusted
  )
df_pred$hi_adjusted <-
  ifelse(
    df_pred$mean_adjusted == 0,
    df_pred$mean_adjusted + 1.96 * df_adj$se_adjusted,
    df_pred$hi_adjusted
  )

# compile final crosswalked data set
final_data <- cbind(df_orig,
                    df_pred[, c("mean_adjusted",
                                "se_adjusted",
                                "lo_adjusted",
                                "hi_adjusted")])

# prepare for uploader
#final_data <- subset(final_data, sex != "Both")
#if ("group_review" %in% colnames(final_data)) {final_data <- subset(final_data, is.na(group_review) | group_review == 1)}
final_data$mean <- final_data$mean_adjusted
final_data$mean <- ifelse(final_data$mean < 0.0000000000000001, 0, final_data$mean)
final_data$mean_adjusted <- ifelse(final_data$mean_adjusted < 0.0000000000000001, 0, final_data$mean_adjusted)
final_data$standard_error <- final_data$se_adjusted
final_data$lower <- final_data$lo_adjusted
final_data$lower <- ifelse(final_data$lower < 0.0000000000000001, 0, final_data$lower)
final_data$lo_adjusted <- ifelse(final_data$lo_adjusted < 0.0000000000000001, 0, final_data$lo_adjusted)
final_data$upper <- final_data$hi_adjusted
final_data$upper <- ifelse(final_data$upper > 1000, 1000, final_data$upper)
final_data$hi_adjusted <- ifelse(final_data$hi_adjusted > 1000, 1000, final_data$hi_adjusted)

final_data <- final_data[dorm == "outpatient" & is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
final_data <- final_data[dorm == "outpatient" & !is.na(crosswalk_parent_seq), seq := NA]

final_data <- final_data[adj_rc == 1 & is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
final_data <- final_data[adj_rc == 1 & !is.na(crosswalk_parent_seq), seq := NA]

fwrite(final_data, paste0("FILEPATH/post_xwalk_", Sys.Date(), ".csv"))

