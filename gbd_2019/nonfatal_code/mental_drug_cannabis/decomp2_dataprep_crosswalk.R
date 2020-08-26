####################################################################
## Cannabis Dependence Data Prep Code
## Purpose: Crosswalk data
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
bid<-158
dstep<-"step2"

##################
# Crosswalk Adults
##################

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
cv_drop<-names(df)[names(df)%like%"cv" & !(names(df)%in%c("cv_cannabis_regular_use"))]
defs <- get_definitions(df)
df <- defs[[1]]
cvs <- defs[[2]]
df <- subnat_to_nat(df)
df <- calc_year(df)
age_dts <- get_age_combos(df)
df <- age_dts[[1]]
pairs <- combn(df[, unique(definition)], 2)
message("finding matches")

df<-df[((age_start + age_end) / 2) >= 25]

matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = df, year_span = 1, age_span = 1)))
matches <- create_differences(matches)
# These were clear outliers, did not want to impact the trimming so dropped pre-model fit. 
matches<-matches[nid2!=126201 & nid!=123977 & nid !=124400 & nid!=222815 & nid!=126201 & nid2!=123977 & nid2!=124400 & nid2!=222815 & nid!=264927 & nid2!=264927]
mrbrt_setup <- create_mrbrtdt(matches, age = T)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

mrbrt_dt<-mrbrt_dt[ldiff_se!="Inf"]
mrbrt_dt<-mrbrt_dt[, study_id:=.GRP, by = c("nid", "nid2")]

# RUN NETWORK MODEL -------------------------------------------------------

message("running model")
model_name <- paste0("cannabis_adult_reguseonly_1_1", date)

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
  study_id = "study_id",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = T
)

direct_predicts<-predict_mr_brt(direct_model, newdata = expand.grid(Z_intercept = 1, cannabis_regular_use = c(0, 1, -1)), write_draws = T)

forrestplot_graphs <- graph_combos(model = direct_model, predictions = direct_predicts)

message("adjusting data")
adjust_dt <- as.data.table(read.xlsx("FILEPATH"))
adjust_dt <- adjust_dt[(age_end + age_start)/2 >=25]
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = direct_model)
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds)

out<-adjusted$epidb

###########
# Crosswalk Youth (ages <=25)
###########

df<-as.data.table(read.xlsx("FILEPATH")))

# CROSSWALK SETUP ---------------------------------------------------------

message("formatting data")
df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)
cv_drop<-names(df)[names(df)%like%"cv" & !(names(df)%in%c("cv_cannabis_regular_use", "cv_students"))]
defs <- get_definitions(df)
df <- defs[[1]]
cvs <- defs[[2]]
df <- subnat_to_nat(df)
df <- calc_year(df)
age_dts <- get_age_combos(df)
df <- age_dts[[1]]
pairs <- combn(df[, unique(definition)], 2)
message("finding matches")

df<-df[((age_start + age_end) / 2) < 25]

matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = df, year_span = 1, age_span = 1)))
matches <- create_differences(matches)
# Drop same sources as adults for consistency in outliering
matches<-matches[nid2!=126201 & nid!=123977 & nid !=124400 & nid!=222815 & nid!=126201 & nid2!=123977 & nid2!=124400 & nid2!=222815 & nid!=264927 & nid2!=264927]
mrbrt_setup <- create_mrbrtdt(matches, age = T)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

mrbrt_dt<-mrbrt_dt[ldiff_se!="Inf"]
mrbrt_dt<-mrbrt_dt[, study_id:=.GRP, by = c("nid", "nid2")]

# RUN NETWORK MODEL -------------------------------------------------------

message("running model")
model_name <- paste0("cannabis_youth_1_1", date)

create_covs <- function(x){
  if (x == "cutoff_score_diagnosis"){
    return(cov_info(x, "X", uprior_lb = 0))
  } else {
    return(cov_info(x, "X"))
  }
}

cov_list <- lapply(mrbrt_vars, create_covs)

direct_model <- run_mr_brt(
  output_dir = "FILEPATH"),
  model_label = model_name,
  remove_x_intercept = T,
  data = mrbrt_dt,
  mean_var = "ldiff",
  se_var = "ldiff_se",
  cov_list,
  study_id = "study_id",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = T
)

direct_predicts<-predict_mr_brt(direct_model, newdata = expand.grid(Z_intercept = 1, cannabis_regular_use = c(0, 1, -1), students = c(0, 1, -1)), write_draws = T)

forrestplot_graphs <- graph_combos(model = direct_model, predictions = direct_predicts)

message("adjusting data")
adjust_dt <- as.data.table(read.xlsx("FILEPATH")))
adjust_dt <- adjust_dt[(age_end + age_start)/2 <25]
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = direct_model)
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds)

kids<-adjusted$epidb

out<-rbind(out, kids)

add<-get_bundle_data(bundle_id = bid, decomp_step = dstep)
add<-add[is_outlier==1, drop:=1]
add<-add[note_modeler%like%"using the region" & group_review==1, drop:=1]
add<-add[group_review==0 & !(note_modeler%like%"using the region"), drop:=1]
add<-add[is.na(drop)]
add<-add[, drop:=NULL]

drops<-get_bundle_data(bundle_id = bid, decomp_step = dstep)
drops<-unique(drops[,.(nid, location_id, year_start, year_end, group_review)])
drops<-drops[is.na(group_review), group_review:=1]
drops<-drops[, sum_gr:=sum(group_review), by = c("location_id", "nid", "year_start", "year_end")]
drops<-drops[, drop:=ifelse(sum_gr==0, 1, 0)]
drops<-unique(drops[drop==1, .(location_id, nid, year_start, year_end, drop)])
add<-merge(add, drops, by = c("location_id", "nid", "year_start", "year_end"), all.x=T)
add<-add[is.na(drop)]
add<-add[, drop:=NULL]

add<-add[measure%in%c("remission")]

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

out<-out[!is.na(group) | group!="", group_review:=1]
out<-out[group=="" | is.na(group), group_review:=NA]
out<-out[is.na(group_review), specificity:=NA]

write_xlsx(out, "FILEPATH"))

save_bundle_version(bundle_id = 158, decomp_step = "iterative", include_clinical = F)
save_crosswalk_version(bundle_version_id = 10802, data_filepath = "FILEPATH"), description = "Sex-Split and Crosswalked Data, All Data for Age Pattern, Add NZL Source")

# Now launch dismod
# Then pull in dismod age pattern for the age split
