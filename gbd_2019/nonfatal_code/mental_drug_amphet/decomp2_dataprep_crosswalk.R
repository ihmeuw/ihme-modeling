####################################################################
## Amphetamine Dependence Data Prep Code
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
bid<-157
dstep<-"step2"

###########
# Crosswalk
###########

df<-as.data.table(read.xlsx("FILEPATH"))

# CROSSWALK SETUP ---------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22)
age_dt <- get_age_metadata(12)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]

message("formatting data")
df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)
cv_drop<-names(df)[names(df)%like%"cv" & names(df)!="cv_direct" & names(df)!="cv_use"]
defs <- get_definitions(df)
df <- defs[[1]]
cvs <- defs[[2]]
df <- subnat_to_nat(df)
df <- calc_year(df)
age_dts <- get_age_combos(df)
df <- age_dts[[1]]
pairs <- combn(df[, unique(definition)], 2)
message("finding matches")
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = df, year_span = 1, age_span = 1)))
matches <- create_differences(matches)
matches<-matches[nid!=150676 & nid2!=150676]
mrbrt_setup <- create_mrbrtdt(matches)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

mrbrt_dt<-mrbrt_dt[ldiff_se!="Inf"]

cocaine_matches<-copy(mrbrt_dt)

df<-as.data.table(read.xlsx("FILEPATH"))

# CROSSWALK SETUP ---------------------------------------------------------

message("formatting data")
df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)
cv_drop<-names(df)[names(df)%like%"cv" & names(df)!="cv_direct" & names(df)!="cv_use"]
defs <- get_definitions(df)
df <- defs[[1]]
cvs <- defs[[2]]
df <- subnat_to_nat(df)
df <- calc_year(df)
age_dts <- get_age_combos(df)
df <- age_dts[[1]]
pairs <- combn(df[, unique(definition)], 2)
message("finding matches")
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = df, year_span = 1, age_span = 1)))
matches <- create_differences(matches)
matches<-matches[nid!=222815]
mrbrt_setup <- create_mrbrtdt(matches)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

mrbrt_dt<-mrbrt_dt[ldiff_se!="Inf"]

cocaine_matches<-cocaine_matches[, cocaine:=1]
mrbrt_dt<-mrbrt_dt[, cocaine:=0]

mrbrt_dt<-rbind(mrbrt_dt, cocaine_matches)

# RUN NETWORK MODEL -------------------------------------------------------

message("running model")
model_name <- paste0("cocaine_amphet_network_1_1", date)

create_covs <- function(x){
  if (x == "cutoff_score_diagnosis"){
    return(cov_info(x, "X", uprior_lb = 0))
  } else {
    return(cov_info(x, "X"))
  }
}

cov_list <- lapply(mrbrt_vars, create_covs)

direct_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model_name,
  remove_x_intercept = T,
  data = mrbrt_dt,
  mean_var = "ldiff",
  se_var = "ldiff_se",
  cov_list,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = T
)

# PREDICTION --------------------------------------------------------------

message("adjusting data")
adjust_dt <- as.data.table(read.xlsx("FILEPATH"))
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = direct_model)
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds)

out<-adjusted$epidb
out<-out[is_outlier==0]
out<-out[, group_review:=1]
out<-out[location_id!=95]

add<-as.data.table(read.xlsx("FILEPATH"))
add<-add[drop==11, drop:=1]
add<-add[drop==0]
add<-add[, c("drop", "xwalk"):=NULL]

add<-add[measure%in%c("remission", "mtwith", "relrisk")]

ready<-add[sex%in%c("Male", "Female")]
m<-add[sex=="Both"]
m<-m[, sex:="Male"]
m<-m[, crosswalk_parent_seq:=seq]
m<-m[, seq:=NA]
f<-add[sex=="Both"]
f<-f[, sex:="Female"]
f<-f[, crosswalk_parent_seq:=seq]
f<-f[, seq:=NA]
ready<-rbind(ready, m, fill = T)
ready<-rbind(ready, f, fill = T)

out<-rbind(out, ready, fill = T)

write.xlsx(out, "FILEPATH", sheetName="extraction")

df<-as.data.table(read.xlsx("FILEPATH"))

message("formatting data")
df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)
cv_drop<-names(df)[names(df)%like%"cv" & names(df)!="cv_recall_1yr"]
defs <- get_definitions(df)
df <- defs[[1]]
cvs <- defs[[2]]
df <- subnat_to_nat(df)
df <- calc_year(df)
age_dts <- get_age_combos(df)
df <- age_dts[[1]]
pairs <- combn(df[, unique(definition)], 2)
message("finding matches")
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = df, year_span = 1, age_span = 1)))
matches <- create_differences(matches)
mrbrt_setup <- create_mrbrtdt(matches)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

mrbrt_dt<-mrbrt_dt[ldiff_se!="Inf"]

amphetamine<-copy(mrbrt_dt)

df<-as.data.table(read.xlsx("FILEPATH"))

message("formatting data")
df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)
cv_drop<-names(df)[names(df)%like%"cv" & names(df)!="cv_recall_1yr"]
defs <- get_definitions(df)
df <- defs[[1]]
cvs <- defs[[2]]
df <- subnat_to_nat(df)
df <- calc_year(df)
age_dts <- get_age_combos(df)
df <- age_dts[[1]]
message("finding matches")
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = df, year_span = 1, age_span = 1)))
matches <- create_differences(matches)
mrbrt_setup <- create_mrbrtdt(matches)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

mrbrt_dt<-mrbrt_dt[ldiff_se!="Inf"]

amphetamine<-amphetamine[, amphetamine:=1]
mrbrt_dt<-mrbrt_dt[, amphetamine:=0]
mrbrt_dt<-rbind(mrbrt_dt, amphetamine)

message("running model")
model_name <- paste0("1yr", date)

create_covs <- function(x){
  if (x == "cutoff_score_diagnosis"){
    return(cov_info(x, "X", uprior_lb = 0))
  } else {
    return(cov_info(x, "X"))
  }
}

cov_list <- lapply(mrbrt_vars, create_covs)

direct_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model_name,
  remove_x_intercept = T,
  data = mrbrt_dt,
  mean_var = "ldiff",
  se_var = "ldiff_se",
  cov_list,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = T
)

message("adjusting data")
adjust_dt <- as.data.table(read.xlsx("FILEPATH"))
cv_drop<-names(adjust_dt)[names(adjust_dt)%like%"cv" & names(adjust_dt)!="cv_recall_1yr"]
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = direct_model)
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds)

out<-adjusted$epidb

write.xlsx(out, "FILEPATH", sheetName="extraction")

bundle_version<-save_bundle_version(bundle_id = 157, decomp_step = "iterative", include_clinical = F)
save_crosswalk_version(bundle_version_id = bundle_version$bundle_version_id,
                       data_filepath = "FILEPATH",
                       description = "Sex-Split, Crosswalked, and 1-yr recall adj, all data for age pattern")
