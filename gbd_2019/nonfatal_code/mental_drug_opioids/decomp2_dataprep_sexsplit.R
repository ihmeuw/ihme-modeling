####################################################################
## Opioid Dependence Data Prep Code
## Purpose: Pull data from bundle, prepare crosswalk version, upload
####################################################################

# Clean up and initialize with the packages we need
rm(list = ls())
library(data.table)
library(mortdb, lib = "FILEPATH")
pacman::p_load(data.table, openxlsx, ggplot2, plyr, parallel, dplyr, RMySQL, stringr, msm)

date <- Sys.Date()
date <- gsub("-", "_", Sys.Date())

draws <- paste0("draw_", 0:999)

# Central functions
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_bundle_data", "save_crosswalk_version", "save_bundle_version", "get_bundle_version")
invisible(lapply(functs, function(x) source(paste0("FILEPATH", x, ".R"))))

mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
                  "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source(paste0("FILEPATH", x, ".R"))))

# Data Prep Functions
source("FILEPATH")

# Set objects
bid<-155
dstep<-"step2"

# Age metadata
age_meta<-get_age_metadata(8)

# For opioid data, downoaded and manually reviewed to:
# A) remove outliers from GBD 2017
# B) keep only parent (raw) rows from age-split data
# C) Remove anything that was group reviewed out in GBD 2017 that was not an age-split parent
df<-as.data.table(read.xlsx("FILEPATH"))
df<-df[use_crosswalk==1]
df<-df[, c("use_crosswalk", "age_sex_split"):=NULL]

# We have four rows where mean = lower = upper; have the uploader fill based on cases/ss
df<-df[standard_error ==0, c("lower", "upper", "standard_error"):=NA]

###########
# Sex-Split
###########

cv_drop<-names(df)[names(df)%like%"cv" & names(df)!="cv_direct"]
dem_sex_dt <- copy(df)
dem_sex_dt <- dem_sex_dt[measure %in% c("prevalence", "incidence")]
dem_sex_dt <- get_cases_sample_size(dem_sex_dt)
dem_sex_dt <- get_se(dem_sex_dt)
dem_sex_dt <- calculate_cases_fromse(dem_sex_dt)
dem_sex_matches <- find_sex_match(dem_sex_dt, measure_vars = c("prevalence", "incidence"))
message("calculating ratios")
mrbrt_sex_dt <- calc_sex_ratios(dem_sex_matches)

mrbrt_dir<- "FILEPATH"
model_name <- paste0("opioid_sexsplit_", date)

mrbrt_sex_dt<-mrbrt_sex_dt[, youth:=(ifelse(midage<20, 1, 0))]

# Youngest age groups have a lot of very high female:male ratios (small number of cases, unstable small values); fitting coefficient on midage>13

  sex_model <- run_mr_brt(
    output_dir = mrbrt_dir,
    model_label = paste0(model_name),
    data = mrbrt_sex_dt[(age_end-age_start <= 25) & midage > 13],
    mean_var = "log_ratio",
    se_var = "log_se",
    list(cov_info("youth", "X")),
    study_id = "id",
    method = "trim_maxL",
    trim_pct = 0.1,
    overwrite_previous = T
  )

preds<-as.data.table(predict_mr_brt(sex_model, newdata = expand.grid(X_intercept = 1, Z_intercept = 1, youth = c(0, 1)), write_draws = F)$model_summaries)
cols<-c("Y_mean", "Y_mean_lo", "Y_mean_hi")
preds<-preds[, (cols):=lapply(.SD, exp), .SDcols = cols]

traindata<-as.data.table(sex_model$train_data)
traindata<-traindata[, trimmed:=ifelse(w==0, "Trimmed", "Not Trimmed")]

preds<-preds[X_youth==1, X_lower:=15]
preds<-preds[X_youth==1, X_upper:=20]
preds<-preds[X_youth==0, X_lower:=20]
preds<-preds[X_youth==0, X_upper:=60]

ggplot(data = traindata, aes(x = midage, y = ratio)) + geom_hline(yintercept = 1) + geom_point(aes(size = 1/ratio_se, color = trimmed), alpha = .4) + geom_pointrange(data = preds, aes(ymin = Y_mean_lo, ymax = Y_mean_hi, y = Y_mean, x = ((X_lower + X_upper)/2))) +
  geom_errorbarh(data = preds, aes(xmin = X_lower, xmax = X_upper, x = ((X_lower + X_upper)/2), y = Y_mean), height = .001) + labs(x = "Age", y = "Female:Male Sex Ratio", title = "Opioid Sex Split") + theme_bw() + theme(text=element_text(size = 20))

# Split the data
reset_db_init()
predict_sex <- sex_split_data(dem_sex_dt, sex_model)
out<-as.data.table(predict_sex$final)

write.xlsx(out, "FILEPATH", sheetName="extraction")





