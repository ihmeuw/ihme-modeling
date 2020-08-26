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
cv_drop<-names(df)[names(df)%like%"cv" & names(df)!="cv_direct"]
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

# RUN NETWORK MODEL -------------------------------------------------------

message("running model")
model_name <- paste0("opioid_direct_", date)

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
    covs = cov_list,
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

add<-get_bundle_data(bundle_id = 155, decomp_step = dstep, gbd_round_id = 6)
table(add$measure)

add<-add[is.na(is_outlier) | is_outlier==0]
add<-add[is.na(group_review) | group_review==1]
add<-add[measure%in%c("remission", "mtwith", "relrisk", "mtstandard")]

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

out<-out[is.na(group), group_review:=NA]

write.xlsx(out, "FILEPATH", sheetName="extraction")

save_bundle_version(bundle_id = 155, decomp_step = "iterative", include_clinical = F)
save_crosswalk_version(bundle_version_id = 10793, data_filepath = "FILEPATH", description = "Sex-Split and Crosswalked Data, Add Non-Prevalence Data, Iterative for Age Pattern All Data")

save_bundle_version(bundle_id = 155, decomp_step = "step2", include_clinical = F)
save_crosswalk_version(bundle_version_id = 9380, data_filepath = "FILEPATH", description = "Sex-Split and Crosswalked Data, Add Non-Prevalence Data")




