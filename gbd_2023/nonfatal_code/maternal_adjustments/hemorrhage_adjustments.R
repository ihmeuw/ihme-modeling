###############################################################################################################################################
# Purpose: data processing for maternal hemorrhage input data
# matchfinding, run mr-brt, apply adjustments, and age-split
###############################################################################################################################################

remove(list = ls())

source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/cw_mrbrt_helper_functions.R")
################################################################################################################################################
hem <- get_bundle_data(bundle_id=74)

# calc mean and SE where mean and SE are NA
hem <- get_cases_sample_size(hem)
hem <- get_se(hem)
hem <- calculate_cases_fromse(hem)
hem <- calc_year(hem)

# identifies reference category -- all cases of maternal hemorrhage, including postpartum and antepartum.
# alternative is any source that reported on only antepartum hemorrhage OR postpartum hemorrhage
hem$is_reference <- ifelse(hem$cv_postparthhemonly==1,0, ifelse(hem$cv_anteparthemonly==1,0,1))

hem <- as.data.table(hem)
hem$age <- (hem$age_start + hem$age_end)/2
hem$age_start <- sapply(1:nrow(hem), function(i) get_closest_age(i, var = "age_start", start=T, dt = hem))
hem$age_end <- sapply(1:nrow(hem), function(i) get_closest_age(i, var = "age_end", start = F, dt = hem))

#set column for alt_dorms and ref_dorms for mrbrt
hem <- hem[cv_postparthhemonly==1, orig_dorms:="postpartum_hem"]
hem <- hem[cv_anteparthemonly==1 , orig_dorms:="antepartum_hem"]
hem$orig_dorms[is.na(hem$orig_dorms)] <- "all_hem"

# offset mean=0 with half of the lowest observed value
hem <- hem[measure=="incidence" & mean==0, adj_zero_mean:=1]
hem <- hem[, orig_mean:=mean]
hem <- hem[measure=="incidence" & mean==0, mean:=(0.5*0.0001088495)]

write.csv(hem,"FILEPATH", row.names = FALSE)

# label for matchfinding
hem <- hem[, mean:=orig_mean]
hem_ref <- subset(hem, is_reference==1)
hem_ref <- hem_ref[, c("nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "year_match", "age_start", "age_end",  "measure",
                     "cv_maternal_direct", "cv_hospital", "mean","group_review", "standard_error", "extractor", "is_outlier", "cv_inpatient", "cv_literature",
                     "cv_anteparthemonly", "cv_postparthhemonly", "cv_severeonly", "cv_severehem_excl", "cv_marketscan", "is_reference", "age", "orig_dorms")]
hem_ref <- plyr::rename(hem_ref, c("mean" = "mean_ref", "standard_error"="std_error_ref"))
hem_alt <- subset(hem, is_reference==0)
hem_alt <- hem_alt[, c("nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "year_match", "age_start", "age_end",  "measure",
                     "cv_maternal_direct", "cv_hospital", "mean", "group_review","standard_error", "extractor", "is_outlier", "cv_inpatient", "cv_literature",
                     "cv_anteparthemonly", "cv_postparthhemonly", "cv_severeonly", "cv_severehem_excl", "cv_marketscan", "is_reference", "age", "orig_dorms")]
hem_alt <- plyr::rename(hem_alt, c("mean"="mean_alt", "standard_error"="std_error_alt"))

#####################################################################################################################################################
# within-study matches
match_hem <- merge(hem_ref, hem_alt, by=c("nid", "measure", "location_id", "year_start", "year_end", "age_start", "age_end", "age"))
match_hem <- subset(match_hem, measure=="incidence")
match_hem <- match_hem[!is.na(match_hem$mean_ref)]
setnames(match_hem, "orig_dorms.x", "ref_dorms")
setnames(match_hem, "orig_dorms.y", "alt_dorms")

write.csv(match_hem, "FILEPATH", row.names = FALSE)

#####################################################################################################################################################
################################################################################################################################################
# mr-brt script for postpartum maternal hemorrhage - to - total hemorrhage adjustment

remove(list = ls())
library(plyr)
library(openxlsx)
library(ggplot2)
library(reticulate)
library(dplyr)
library(data.table)
reticulate::use_python("FILEPATH/python")
mr <- reticulate::import("mrtool")
cw <- reticulate::import("crosswalk")

################################################################################################################################################
# convert ratios to log space
df <- as.data.table(read.csv("FILEPATH"))

# subset to postpartum hem only
cv_posthem <- df[cv_postparthhemonly.y==1]
cv_posthem <- cv_posthem[, alt_dorms:="postpartum_hem"]

# calculate log-transformed means and sds for reference and alternative using cw$utils$linear_to_log from "crosswalk" package
# [1] represents first returned parameter, which is mean; [2] represents second returned parameter, which is sd
cv_posthem$mean_log_ref <-cw$utils$linear_to_log(mean = array(cv_posthem$mean_ref), sd = array(cv_posthem$std_error_ref))[[1]]
cv_posthem$sd_log_ref <- cw$utils$linear_to_log(mean = array(cv_posthem$mean_ref), sd = array(cv_posthem$std_error_ref))[[2]]

cv_posthem$mean_log_alt <-cw$utils$linear_to_log(mean = array(cv_posthem$mean_alt), sd = array(cv_posthem$std_error_alt))[[1]]
cv_posthem$sd_log_alt <- cw$utils$linear_to_log(mean = array(cv_posthem$mean_alt), sd = array(cv_posthem$std_error_alt))[[2]]

# calculate difference of log-transformed means and calculate sd of difference in log space
calculate_diff <- function(cv_posthem, alt_mean, alt_sd, ref_mean, ref_sd) {
  dat_diff <- data.table(mean_alt = cv_posthem[, mean_log_alt], mean_se_alt = cv_posthem[, sd_log_alt], 
                         mean_ref = cv_posthem[, mean_log_ref], mean_se_ref = cv_posthem[, sd_log_ref])
  dat_diff[, `:=` (diff = mean_alt-mean_ref, diff_se = sqrt(mean_se_alt^2 + mean_se_ref^2))] 
  return(dat_diff[,c("diff", "diff_se")])
}

diff <- calculate_diff(
  cv_posthem, alt_mean = "mean_log_alt", alt_sd = "sd_log_alt",
  ref_mean = "mean_log_ref", ref_sd = "sd_log_ref"
)

names(diff) <- c("log_diff_mean", "log_diff_se")
cv_posthem <- cbind(cv_posthem, diff)

################################################################################################################################################
# create crosswalk dataframe
dat1 <- cw$CWData(
  df = cv_posthem,
  obs = "log_diff_mean",
  obs_se = "log_diff_se",
  alt_dorms = "alt_dorms",
  ref_dorms = "ref_dorms",
  dorm_separator = " ",
  covs = list("age"),
  study_id = "nid",
  add_intercept = TRUE
)

# create crosswalk model
fit1 <- cw$CWModel(
  cwdata = dat1, 
  obs_type = "diff_log",
  cov_models = list(
    cw$CovModel("intercept"),
    cw$CovModel(cov_name = "age",
                spline = cw$XSpline(degree = 2L, knots = c(12, 20, 30, 40, 50), 
                                    l_linear = TRUE,
                                    r_linear = TRUE))),
  gold_dorm = "all_hem")

# fit model
fit1$fit()

# create table of results, betas/dorms/cov_names/gammas
results <- fit1$create_result_df() %>%
  select(dorms, cov_names, beta, beta_sd, gamma)

# save model results as .RDS file
save_model_RDS <- function(results, path, model_name){
  names <- c("beta", 
             "beta_sd", 
             "constraint_mat",
             "cov_mat",
             "cov_models",
             "cwdata",
             "design_mat",
             "fixed_vars",
             "gamma",
             "gold_dorm", 
             "lt",
             "num_vars",
             "num_vars_per_dorm",
             "obs_type",
             "order_prior",
             "random_vars",
             "relation_mat",
             "var_idx",
             "vars",
             "w")
  model <- list()
  for (name in names){
    if(is.null(results[[name]])) {
      message(name, " is NULL in original object, will not be included in RDS")
    }
    model[[name]] <- results[[name]]
  }
  saveRDS(model, paste0(path, "/", model_name, "/model_object.RDS"))
  message("RDS object saved to ", paste0(path, "/",model_name, "/model_object.RDS"))
  return(model)
}

save_mrbrt <- function(model, mrbrt_directory, model_name) {
  df_result <- model$create_result_df()
  write.csv(df_result, paste0(mrbrt_directory, "/", model_name, "/df_result_crosswalk.csv"), row.names = FALSE)
  py_save_object(object = model, filename = paste0(mrbrt_directory, "/", model_name, "/results.pkl"), pickle = "dill")
}

save <- save_model_RDS(fit1, "FILEPATH")
save_r <- save_mrbrt(fit1, "FILEPATH")

################################################################################################################################################
# plots

plots <- import("crosswalk.plots")

# funnel plot
plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = dat1,
  continuous_variables = list("age"),
  obs_method = 'postpartum_hem',
  plot_note = 'Funnel plot', 
  plots_dir = "FILEPATH",
  #file.path(getwd(), "plots"), 
  file_name = "funnel_plot",
  write_file = TRUE
)

# dose response curve for age
plots$dose_response_curve(
  dose_variable = 'age',
  obs_method = 'postpartum_hem', 
  continuous_variables = list("age"), 
  cwdata = dat1, 
  cwmodel = fit1, 
  plot_note = "dose-response plot", 
  plots_dir = "FILEPATH", 
  file_name = "doseresponse_plot", 
  write_file = TRUE
)

################################################################################################################################################
# apply adjustment to original vals

# original df
df_orig <- read.csv("FILEPATH")

# adjust postpartum to total
df_orig <- as.data.table(df_orig)
df_orig <- df_orig[orig_dorms=="postpartum_hem" | orig_dorms=="all_hem"]
df_orig <- df_orig[!is.na(df_orig$mean)]
df_orig <- df_orig[!is.na(df_orig$standard_error)]
df_orig <- df_orig[measure=="incidence"]

preds1 <- fit1$adjust_orig_vals(
  df = df_orig,
  orig_dorms = "orig_dorms",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

# add adjustments back to original dataset
df_orig[, c("meanvar_adjusted", "sdvar_adjusted", "pred_log", "pred_se_log", "data_id")] <- preds1

# write adjusted df
write.csv(df_orig, "FILEPATH")
write.csv(results, "FILEPATH")

################################################################################################################################################
# mr-brt script for antepartum maternal hemorrhage - to - total hemorrhage adjustment
#setup

remove(list = ls())
library(plyr)
library(openxlsx)
library(ggplot2)
library(reticulate)
library(dplyr)
library(data.table)
reticulate::use_python("FILEPATH/python")
mr <- reticulate::import("mrtool")
cw <- reticulate::import("crosswalk")

################################################################################################################################################
# convert ratios to log space
df <- as.data.table(read.csv("FILEPATH"))

# subset to antepartum hem only
cv_antehem <- df[cv_anteparthemonly.y==1]
cv_antehem <- cv_antehem[!mean_alt==0]

# calculate log-transformed means and sds for reference and alternative using cw$utils$linear_to_log from "crosswalk" package
# [1] represents first returned parameter, which is mean; [2] represents second returned parameter, which is sd
cv_antehem$mean_log_ref <-cw$utils$linear_to_log(mean = array(cv_antehem$mean_ref), sd = array(cv_antehem$std_error_ref))[[1]]
cv_antehem$sd_log_ref <- cw$utils$linear_to_log(mean = array(cv_antehem$mean_ref), sd = array(cv_antehem$std_error_ref))[[2]]

cv_antehem$mean_log_alt <-cw$utils$linear_to_log(mean = array(cv_antehem$mean_alt), sd = array(cv_antehem$std_error_alt))[[1]]
cv_antehem$sd_log_alt <- cw$utils$linear_to_log(mean = array(cv_antehem$mean_alt), sd = array(cv_antehem$std_error_alt))[[2]]

# calculate difference of log-transformed means and calculate sd of difference in log space
calculate_diff <- function(cv_antehem, alt_mean, alt_sd, ref_mean, ref_sd) {
  dat_diff <- data.table(mean_alt = cv_antehem[, mean_log_alt], mean_se_alt = cv_antehem[, sd_log_alt], 
                         mean_ref = cv_antehem[, mean_log_ref], mean_se_ref = cv_antehem[, sd_log_ref])
  dat_diff[, `:=` (diff = mean_alt-mean_ref, diff_se = sqrt(mean_se_alt^2 + mean_se_ref^2))] 
  return(dat_diff[,c("diff", "diff_se")])
}

diff <- calculate_diff(
  cv_antehem, alt_mean = "mean_log_alt", alt_sd = "sd_log_alt",
  ref_mean = "mean_log_ref", ref_sd = "sd_log_ref"
)

names(diff) <- c("log_diff_mean", "log_diff_se")
cv_antehem <- cbind(cv_antehem, diff)

################################################################################################################################################
# create crosswalk dataframe
dat1 <- cw$CWData(
  df = cv_antehem,
  obs = "log_diff_mean",
  obs_se = "log_diff_se",
  alt_dorms = "alt_dorms",
  ref_dorms = "ref_dorms",
  dorm_separator = " ",
  covs = list("age"),
  study_id = "nid",
  add_intercept = TRUE
)

# create crosswalk model
fit1 <- cw$CWModel(
  cwdata = dat1, 
  obs_type = "diff_log",
  cov_models = list(
    cw$CovModel("intercept"),
    cw$CovModel(cov_name = "age",
                spline = cw$XSpline(degree = 2L, knots = c(12, 20, 30, 40, 50), 
                                    l_linear = TRUE,
                                    r_linear = TRUE))),
  gold_dorm = "all_hem")

# fit model
fit1$fit()

# create table of results, betas/dorms/cov_names/gammas
results <- fit1$create_result_df() %>%
  select(dorms, cov_names, beta, beta_sd, gamma)

# save model results as .RDS file
save_model_RDS <- function(results, path, model_name){
  names <- c("beta", 
             "beta_sd", 
             "constraint_mat",
             "cov_mat",
             "cov_models",
             "cwdata",
             "design_mat",
             "fixed_vars",
             "gamma",
             "gold_dorm", 
             "lt",
             "num_vars",
             "num_vars_per_dorm",
             "obs_type",
             "order_prior",
             "random_vars",
             "relation_mat",
             "var_idx",
             "vars",
             "w")
  model <- list()
  for (name in names){
    if(is.null(results[[name]])) {
      message(name, " is NULL in original object, will not be included in RDS")
    }
    model[[name]] <- results[[name]]
  }
  saveRDS(model, paste0(path, "/", model_name, "/model_object.RDS"))
  message("RDS object saved to ", paste0(path, "/",model_name, "/model_object.RDS"))
  return(model)
}

save_mrbrt <- function(model, mrbrt_directory, model_name) {
  df_result <- model$create_result_df()
  write.csv(df_result, paste0(mrbrt_directory, "/", model_name, "/df_result_crosswalk.csv"), row.names = FALSE)
  py_save_object(object = model, filename = paste0(mrbrt_directory, "/", model_name, "/results.pkl"), pickle = "dill")
}

save <- save_model_RDS(fit1, "FILEPATH")
save_r <- save_mrbrt(fit1, "FILEPATH")

################################################################################################################################################
# plots

plots <- import("crosswalk.plots")

# funnel plot
plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = dat1,
  continuous_variables = list("age"),
  obs_method = 'antepartum_hem',
  plot_note = 'Funnel plot', 
  plots_dir = "FILEPATH",
  #file.path(getwd(), "plots"), 
  file_name = "funnel_plot",
  write_file = TRUE
)

# dose response curve for age
plots$dose_response_curve(
  dose_variable = 'age',
  obs_method = 'antepartum_hem', 
  continuous_variables = list("age"), 
  cwdata = dat1, 
  cwmodel = fit1, 
  plot_note = "dose-response plot", 
  plots_dir = "FILEPATH", 
  file_name = "doseresponse_plot", 
  write_file = TRUE
)

################################################################################################################################################
# apply adjustment to original vals

# original df
df_orig <- read.csv("FILEPATH")

# adjust antepartum to total
df_orig <- as.data.table(df_orig)
df_orig <- df_orig[orig_dorms=="antepartum_hem" | orig_dorms=="all_hem"]
df_orig <- df_orig[!is.na(df_orig$mean)]
df_orig <- df_orig[!mean==0]
df_orig <- df_orig[!is.na(df_orig$standard_error)]
df_orig <- df_orig[measure=="incidence"]

preds1 <- fit1$adjust_orig_vals(
  df = df_orig,
  orig_dorms = "orig_dorms",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

# add adjustments back to original dataset
df_orig[, c("meanvar_adjusted", "sdvar_adjusted", "pred_log", "pred_se_log", "data_id")] <- preds1

# write adjusted df
write.csv(df_orig, "FILEPATH")
write.csv(results, "FILEPATH")

################################################################################################################################################
################################################################################################################################################
# age-split lit data into GBD age bins

remove(list = ls())
library(dplyr)
library(plyr)
library(data.table)
library(readxl)
library('openxlsx')
source("FILEPATH/get_draws.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/cw_mrbrt_helper_functions.R")

gbd_id <- ID
version_id <- MVID
draws <- paste0("draw_", 0:999)

## GET TABLES
ages <- get_age_metadata(release_id=16)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 10 & age_end <=55, age_group_id]
ages[, age_group_weight_value := NULL]
ages[age_start >= 1, age_end := age_end - 1]
ages[age_end == 124, age_end := 102]
super_region_dt <- get_location_metadata(location_set_id = 22, release_id=16)
super_region_dt <- super_region_dt[, .(location_id, super_region_id)]

################################################################################################################################################
# pull in bv 
hemorrhage <- get_bundle_version(bundle_version_id=bvid)

# calc mean and SE where mean and SE are NA
hemorrhage <- get_cases_sample_size(hemorrhage)
hemorrhage <- get_se(hemorrhage)
hemorrhage <- calculate_cases_fromse(hemorrhage)
hemorrhage <- calc_year(hemorrhage)
hemorrhage$is_reference <- ifelse(hemorrhage$cv_postparthhemonly==1,0, ifelse(hemorrhage$cv_anteparthemonly==1,0,1))
hemorrhage <- hemorrhage[cv_postparthhemonly==1, orig_dorms:="postpartum_hem"]
hemorrhage <- hemorrhage[cv_anteparthemonly==1 , orig_dorms:="antepartum_hem"]
hemorrhage$orig_dorms[is.na(hemorrhage$orig_dorms)] <- "all_hem"

hemorrhage_unadj <- hemorrhage[orig_dorms=="all_hem"]

# pull in files with adjusted means & sds
ante_hem <- read.csv("FILEPATH")
ante_hem <- as.data.table(ante_hem)
ante_hem <- ante_hem[orig_dorms=="antepartum_hem"]
ante_hem <- ante_hem[, age_end:=age_end-1]
ante_hem[age_start>age_end, age_start:=age_start-5]
ante_hem <- ante_hem[, mean:=meanvar_adjusted]
ante_hem <- ante_hem[, standard_error:=sdvar_adjusted]
ante_hem <- ante_hem[, upper:=NA][, lower:=NA][, uncertainty_type_value:=NA]

post_hem <- read.csv("FILEPATH")
post_hem <- as.data.table(post_hem)
post_hem <- post_hem[orig_dorms=="postpartum_hem"]
post_hem <- post_hem[, age_end:=age_end-1]
post_hem[age_start>age_end, age_start:=age_start-5]
post_hem <- post_hem[, mean:=meanvar_adjusted]
post_hem <- post_hem[, standard_error:=sdvar_adjusted]
post_hem <- post_hem[, upper:=NA][, lower:=NA][, uncertainty_type_value:=NA]

# merge the three for review
hem <- rbind(hemorrhage_unadj, ante_hem, post_hem, fill=TRUE)
write.csv(hem, "FILEPATH")

#subset to rows with age grange >5 that need split. Set aside age-specific data.
hem <- hem[, age_range:=age_end-age_start]
hem_already_split <- hem[age_range<=5 | mean==0 | cases==0]
hem_to_split <- hem[age_range>5 & mean!=0 & cases!=0]
hem_to_split[, id := 1:.N]

expand_age <- function(small_dt, age_dt = ages){
  dt <- copy(small_dt)
  
  ## ROUND AGE GROUPS
  dt[, age_start := age_start - age_start %%5]
  dt[, age_end := age_end - age_end %%5 + 4]
  dt <- dt[age_end > 102, age_end := 102] 
  
  ## EXPAND FOR AGE
  dt[, n.age:=(age_end+1 - age_start)/5]
  dt[, age_start_floor:=age_start]
  dt[, drop := cases/n.age] 
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age.rep := 1:.N - 1, by =.(id)]
  split[, age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4]
  split <- merge(split, ages[, c("age_start", "age_end", "age_group_id")], by = c("age_start", "age_end"), all.x = T)
  split[age_start == 0 & age_end == 4, age_group_id := 1]
  split <- split[age_group_id>6 & age_group_id<16] 
  return(split)
}

split_df <- expand_age(hem_to_split, age_dt=ages)
setnames(split_df, "year_match", "year_id")

################################################################################################################################################
# which locations and years need split? 
pop_locs <- unique(split_df$location_id)
pop_years <- unique(split_df$year_id)
pop_years <- round_any(pop_years, 5) 

# get age pattern 
get_age_pattern <- function(locs, id, age_groups){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, 
                           location_id = locs, source = "epi", measure_id=6,
                           version_id = version_id, sex_id = 2,  release_id=16, 
                           age_group_id = age_groups, year_id = pop_years) 
  
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, year_id, age_group_id, location_id, se_dismod, rate_dis)]
  
  ## CASES AND SAMPLE SIZE
  age_pattern[, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] 
  age_pattern[is.nan(cases_us), cases_us := 0]
  
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, year_id, sex_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  return(age_pattern)
}

age_pattern <- get_age_pattern(locs = pop_locs, id = gbd_id, age_groups = age)

age_pattern[,location_id:=super_region_id]
age_pattern1 <- copy(age_pattern)
setnames(split_df, "year_id", "orig_year_id")
split_df <- split_df[, year_id:=round_any(orig_year_id, 5)]
split_df <- merge(split_df, age_pattern1, by = c("age_group_id", "year_id", "location_id"))

# get live births info to create incidence ratios
get_births_structure <- function(locs, years, age_groups){
  population <- get_population(location_id = locs, year_id = years, release_id=16, #decomp_step = "step2",
                               sex_id = 2, age_group_id = age_groups)
  asfr <- get_covariate_estimates(covariate_id=13, age_group_id = age_groups, location_id=locs, year_id=years,
                                  release_id=16, sex_id=2)
  setnames(asfr, "mean_value", "asfr")
  demogs <- merge(asfr, population, by=c("location_id", "year_id", "age_group_id"))
  demogs[, live_births:=asfr*population]
  demogs[, c("location_id", "year_id", "age_group_id", "live_births")]
  return(demogs)
}

births_structure <- get_births_structure(locs = pop_locs, years = pop_years, age_groups = age)

# merge births
split_df <- merge(split_df, births_structure, by = c("location_id", "year_id", "age_group_id"))

#################################################################################################################################################
# age-split data
split_data <- function(raw_dt){
  dt1 <- copy(raw_dt)
  dt1[, total_births := sum(live_births), by = "id"]
  dt1[, sample_size := (live_births / total_births) * sample_size]
  dt1[, cases_dis := sample_size * rate_dis]
  dt1[, total_cases_dis := sum(cases_dis), by = "id"]
  dt1[, total_sample_size := sum(sample_size), by = "id"]
  dt1[, all_age_rate := total_cases_dis/total_sample_size]
  dt1[, ratio := mean / all_age_rate]
  dt1[, mean := ratio * rate_dis ]
  dt1 <- dt1[mean < 1, ]
  dt1[, cases := mean * sample_size]
  return(dt1)
}

split_df <- split_data(split_df)
split_df[, year_id:=orig_year_id]
split_df[, standard_error := NA][, upper:=NA][, lower:=NA][, uncertainty_type_value:=NA]
split_df <- get_se(split_df)

# label split rows
split_df <- split_df[, split_row:=1]

# combine with data that were already split
hem <- rbind(hem_already_split, split_df, fill=TRUE)

# label with parent_seqs 
split_ids <- unique(hem$id)
hem$crosswalk_parent_seq <- NA
hem$crosswalk_parent_seq <- as.numeric(hem$crosswalk_parent_seq)
hem[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
hem[, seq:=NA]
hem <- subset(hem, !(mean==0 & is.na(lower) & is.na(cases) & is.na(sample_size) & is.na(standard_error) & is.na(effective_sample_size)))
hem[, sex:="Female"]
hem <- hem[group_review==1 | is.na(group_review)]
hem <- hem[measure=="incidence"]

# outlier sources using clinical envelope
envelope_new <- read.xlsx("FILEPATH")
envelope_new <- envelope_new%>%mutate(nid=ifelse(merged_nid=="na",nid,merged_nid))
envelope_distinct_new <- envelope_new %>% distinct(nid, uses_env,.keep_all = TRUE)
envelope_distinct_new$nid <- as.numeric(envelope_distinct_new$nid)

hem <- as.data.table(merge(hem, envelope_distinct_new[, c("uses_env", "nid")], by="nid", all.x=TRUE))
hem <- hem[uses_env==1, is_outlier:=1][uses_env==1, note_modeler:="outliered because inpatient source uses envelope"]

# outlier terminal age groups
hem <- hem[age_start==10, is_outlier:=1][age_start==10, note_modeler:="outliered terminal age group"]
hem <- hem[age_start==50, is_outlier:=1][age_start==50, note_modeler:="outliered terminal age group"]

# outlier near miss 
hem <- hem[cv_diag_severe==1, is_outlier:=1][cv_diag_severe==1, note_modeler:="outliered for near-miss case definition"]

# outlier marketscan
ms_nids <- c(244369, 244370, 244371, 336847, 336848, 336849, 336850, 408680, 433114, 494351, 494352)
hem <- hem[nid%in%ms_nids, is_outlier:=1][nid%in%ms_nids, note_modeler:="outlier marketscan"]

write.xlsx(hem, "FILEPATH")
write.csv(hem, "FILEPATH")


