###############################################################################################################################################
# Purpose: data processing for maternal sepsis input data
# matchfinding, run mr-brt, apply adjustments, and age-split
###############################################################################################################################################

remove(list = ls())
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/cw_mrbrt_helper_functions.R")
library(dplyr)
library(data.table)
cv_drop <- NULL
###############################################################################################################################################

# get bundle version 
sepsis <- get_bundle_version(bundle_version_id=BVID)

# calc mean and SE where mean and SE are NA
sepsis <- get_cases_sample_size(sepsis)
sepsis <- get_se(sepsis)
sepsis <- calculate_cases_fromse(sepsis)
sepsis <- calc_year(sepsis)

###############################################################################################################################################
# sepsis
# crosswalking claims to inpatient by age, then all clinical to lit by age
# label for matchfinding
sepsis$clinical_data_type <- ifelse(sepsis$clinical_data_type=="", "literature", sepsis$clinical_data_type)
sepsis$cv_marketscan <- ifelse(sepsis$clinical_data_type=="claims, inpatient only", 1, 0)
sepsis$cv_inpatient <- ifelse(sepsis$clinical_data_type=="inpatient", 1, 0)
sepsis$age <- (sepsis$age_start+sepsis$age_end)/2
sepsis$age_start <- sapply(1:nrow(sepsis), function(i) get_closest_age(i, var = "age_start", start=T, dt = sepsis))
sepsis$age_end <- sapply(1:nrow(sepsis), function(i) get_closest_age(i, var = "age_end", start = F, dt = sepsis))

sepsis_clinical <- subset(sepsis, clinical_data_type!="literature")
sepsis_clinical$crosswalk_parent_seq <- ""
sepsis_clinical$is_reference <- ifelse(sepsis_clinical$clinical_data_type=="inpatient", 1, 0)
sepsis_ref <- subset(sepsis_clinical, sepsis_clinical$is_reference==1)

sepsis_clinical <- sepsis_clinical[clinical_data_type=="claims", orig_dorms:="claims"]
sepsis_clinical$orig_dorms[is.na(sepsis_clinical$orig_dorms)] <- "inpatient"
sepsis_clinical <- sepsis_clinical[measure=="incidence" & mean==0, adj_zero_mean:=1]
sepsis_clinical <- sepsis_clinical[, orig_mean:=mean]
sepsis_clinical <- sepsis_clinical[measure=="incidence" & mean==0, mean:=(0.5*7.721812e-06)]
write.csv(sepsis_clinical,"FILEPATH")
sepsis_clinical <- sepsis_clinical[, mean:=orig_mean]

#################################################################################################################################################
# label for matchfinding
sepsis_clinical_ref <- subset(sepsis_clinical, is_reference==1)
sepsis_clinical_ref <- sepsis_clinical_ref[, c("nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "year_match", "age_start", "age_end",  "measure",
                     "cv_maternal_direct", "cv_hospital", "mean","group_review", "standard_error", "extractor", "is_outlier", "cv_inpatient", "cv_literature",
                     "cv_anteparthemonly", "cv_postparthhemonly", "cv_severeonly", "cv_severehem_excl", "cv_marketscan", "is_reference", "age", "orig_dorms")]
sepsis_clinical_ref <- plyr::rename(sepsis_clinical_ref, c("mean" = "mean_ref", "standard_error"="std_error_ref"))
sepsis_clinical_alt <- subset(sepsis_clinical, is_reference==0)
sepsis_clinical_alt <- sepsis_clinical_alt[, c("nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "year_match", "age_start", "age_end",  "measure",
                     "cv_maternal_direct", "cv_hospital", "mean", "group_review","standard_error", "extractor", "is_outlier", "cv_inpatient", "cv_literature",
                     "cv_anteparthemonly", "cv_postparthhemonly", "cv_severeonly", "cv_severehem_excl", "cv_marketscan", "is_reference", "age", "orig_dorms")]
sepsis_clinical_alt <- plyr::rename(sepsis_clinical_alt, c("mean"="mean_alt", "standard_error"="std_error_alt"))

#####################################################################################################################################################
# between-study matches - claims to inpatient
match_sepsis_clinical <- merge(sepsis_clinical_ref, sepsis_clinical_alt, by=c("measure", "location_id", "age_start", "age_end", "age"))
match_sepsis_clinical$year_diff<- abs(match_sepsis_clinical$year_match.x-match_sepsis_clinical$year_match.y)
match_sepsis_clinical<-subset(match_sepsis_clinical, match_sepsis_clinical$year_diff<6)
match_sepsis_clinical <- subset(match_sepsis_clinical, measure=="incidence")
match_sepsis_clinical <- match_sepsis_clinical[!is.na(match_sepsis_clinical$mean_ref)]
setnames(match_sepsis_clinical, "orig_dorms.x", "ref_dorms")
setnames(match_sepsis_clinical, "orig_dorms.y", "alt_dorms")
match_sepsis_clinical <- match_sepsis_clinical%>%
  mutate(matched_id=paste(nid.x,nid.y,age,year_match.x,year_match.y, sep = " - ") )

write.csv(match_sepsis_clinical, "FILEPATH")

#######################################################################################################################################################
# adjust claims to inpatient
# mr-brt script for sepsis

remove(list = ls())
library(plyr)
library(openxlsx)
library(ggplot2)
library(reticulate)
library(dplyr)
reticulate::use_python("FILEPATH/python")
mr <- reticulate::import("mrtool")
cw <- reticulate::import("crosswalk")

################################################################################################################################################
# convert ratios to log space
# using within-study matches for test
sepsis <- as.data.table(read.csv("FILEPATH"))

# drop means of zero. matched pairs with zeros can be adjusted @ end but shouldnt be used as matched pairs
sepsis <- sepsis[!mean_ref==0]
sepsis <- sepsis[!mean_alt==0]

# calculate log-transformed means and sds for reference and alternative using cw$utils$linear_to_log from "crosswalk" package
# [1] represents first returned parameter, which is mean; [2] represents second returned parameter, which is sd
sepsis$mean_log_ref <-cw$utils$linear_to_log(mean = array(sepsis$mean_ref), sd = array(sepsis$std_error_ref))[[1]]
sepsis$sd_log_ref <- cw$utils$linear_to_log(mean = array(sepsis$mean_ref), sd = array(sepsis$std_error_ref))[[2]]

sepsis$mean_log_alt <-cw$utils$linear_to_log(mean = array(sepsis$mean_alt), sd = array(sepsis$std_error_alt))[[1]]
sepsis$sd_log_alt <- cw$utils$linear_to_log(mean = array(sepsis$mean_alt), sd = array(sepsis$std_error_alt))[[2]]

# calculate difference of log-transformed means and calculate sd of difference in log space
calculate_diff <- function(sepsis, alt_mean, alt_sd, ref_mean, ref_sd) {
  dat_diff <- data.table(mean_alt = sepsis[, mean_log_alt], mean_se_alt = sepsis[, sd_log_alt], 
                         mean_ref = sepsis[, mean_log_ref], mean_se_ref = sepsis[, sd_log_ref])
  dat_diff[, `:=` (diff = mean_alt-mean_ref, diff_se = sqrt(mean_se_alt^2 + mean_se_ref^2))] 
  return(dat_diff[,c("diff", "diff_se")])
}

diff <- calculate_diff(
  sepsis, alt_mean = "mean_log_alt", alt_sd = "sd_log_alt",
  ref_mean = "mean_log_ref", ref_sd = "sd_log_ref"
)

names(diff) <- c("log_diff_mean", "log_diff_se")
sepsis <- cbind(sepsis, diff)

################################################################################################################################################
# create crosswalk dataframe
dat1 <- cw$CWData(
  df = sepsis,
  obs = "log_diff_mean",
  obs_se = "log_diff_se",
  alt_dorms = "alt_dorms",
  ref_dorms = "ref_dorms",
  dorm_separator = " ",
  covs = list("age"),
  study_id = "matched_id",
  add_intercept = TRUE
)

# create crosswalk model
fit1 <- cw$CWModel(
  cwdata = dat1, 
  obs_type = "diff_log",
  cov_models = list(
    cw$CovModel("intercept"),
    cw$CovModel(cov_name = "age",
                spline = cw$XSpline(degree = 2L, knots = c(20, 30, 40, 50), 
                                    l_linear = TRUE,
                                    r_linear = TRUE))),
  gold_dorm = "inpatient")

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
  obs_method = 'claims',
  plot_note = 'Funnel plot', 
  plots_dir = "FILEPATH",
  #file.path(getwd(), "plots"), 
  file_name = "funnel_plot",
  write_file = TRUE
)

# dose response curve for age
plots$dose_response_curve(
  dose_variable = 'age',
  obs_method = 'claims', 
  continuous_variables = list("age"), 
  cwdata = dat1, 
  cwmodel = fit1, 
  plot_note = "Dose-response plot", 
  plots_dir = "FILEPATH", 
  file_name = "doseresponse_plot", 
  write_file = TRUE
)

################################################################################################################################################
# apply adjustment to original vals

# original df
df_orig <- read.csv("FILEPATH")

df_orig <- as.data.table(df_orig)
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

# revert adjusted zeroes back to zero
df_orig <- df_orig[adj_zero_mean==1, mean:=0]
df_orig <- df_orig[adj_zero_mean==1, meanvar_adjusted:=0]

# write adjusted df
write.csv(df_orig, "FILEPATH")


#####################################################################################################################################################
###############################################################################################################################################
# set up matchfinding - second crosswalk (clinical-to-lit)
remove(list = ls())
source("/ihme/cc_resources/libraries/current/r/get_bundle_data.R")
source("/ihme/cc_resources/libraries/current/r/get_bundle_version.R")
source("/ihme/cc_resources/libraries/current/r/get_covariate_estimates.R")
source("/ihme/cc_resources/libraries/current/r/get_population.R")
source("/ihme/cc_resources/libraries/current/r/get_age_metadata.R")
source("/mnt/team/rgud/priv/Maternal/jenny/GBD2023/xwalk/cw_mrbrt_helper_functions.R")
library(dplyr)
library(data.table)
###############################################################################################################################################

# get bundle version - has both lit and clinical
sepsis <- get_bundle_version(bundle_version_id=bvid)

# calc mean and SE where mean and SE are NA
sepsis <- get_cases_sample_size(sepsis)
sepsis <- get_se(sepsis)
sepsis <- calculate_cases_fromse(sepsis)
sepsis <- calc_year(sepsis)

sepsis$clinical_data_type <- ifelse(sepsis$clinical_data_type=="", "literature", sepsis$clinical_data_type)
sepsis$cv_marketscan <- ifelse(sepsis$clinical_data_type=="claims, inpatient only", 1, 0)
sepsis$cv_inpatient <- ifelse(sepsis$clinical_data_type=="inpatient", 1, 0)
sepsis$age <- (sepsis$age_start+sepsis$age_end)/2
sepsis$age_start <- sapply(1:nrow(sepsis), function(i) get_closest_age(i, var = "age_start", start=T, dt = sepsis))
sepsis$age_end <- sapply(1:nrow(sepsis), function(i) get_closest_age(i, var = "age_end", start = F, dt = sepsis))

# reading in adjusted means/sds for clinical data from first crosswalk
sepsis_adj <- read.csv("FILEPATH")
sepsis_adj <- as.data.table(sepsis_adj)
sepsis_adj <- sepsis_adj[, mean:=meanvar_adjusted]
sepsis_adj <- sepsis_adj[, standard_error:=sdvar_adjusted]
sepsis_adj <- sepsis_adj[!(orig_dorms=="inpatient"), upper:=NA]
sepsis_adj <- sepsis_adj[!(orig_dorms=="inpatient"), lower:=NA]

# merge with lit data
sepsis$clinical_data_type <- ifelse(sepsis$clinical_data_type=="", "literature", sepsis$clinical_data_type)
sepsis_lit <- sepsis[clinical_data_type=="literature"]
sepsis_lit <- rbind(sepsis_lit, sepsis_adj, fill=TRUE)

sepsis_lit$is_reference <- ifelse(sepsis_lit$clinical_data_type=="literature", 1, 0)
sepsis_lit <- sepsis_lit[clinical_data_type=="claims" | clinical_data_type=="inpatient" | clinical_data_type=="claims - flagged", orig_dorms:="clinical"]

# label rows we do not want to adjust
sepsis_lit$orig_dorms[is.na(sepsis_lit$orig_dorms)] <- "literature"
sepsis_lit <- sepsis_lit[measure=="incidence" & mean==0, adj_zero_mean:=1]
sepsis_lit <- sepsis_lit[, orig_mean:=mean]
sepsis_lit <- sepsis_lit[measure=="incidence" & mean==0, mean:=(0.5*2.200000e-08)]
write.csv(sepsis_lit,"FILEPATH")
sepsis_lit <- sepsis_lit[, mean:=orig_mean]

# first, find matches that just exist as-is (pre-aggregation)
sepsis_lit_ref <- subset(sepsis_lit, is_reference==1)
sepsis_lit_ref <- sepsis_lit_ref[, c("nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "year_match", "age_start", "age_end",  "measure",
                                               "cv_maternal_direct", "cv_hospital", "mean","group_review", "standard_error", "extractor", "is_outlier", "cv_inpatient", "cv_literature",
                                               "cv_anteparthemonly", "cv_postparthhemonly", "cv_severeonly", "cv_severehem_excl", "cv_marketscan", "is_reference", "age", "orig_dorms")]
sepsis_lit_ref <- plyr::rename(sepsis_lit_ref, c("mean" = "mean_ref", "standard_error"="std_error_ref"))
sepsis_lit_alt <- subset(sepsis_lit, is_reference==0)
sepsis_lit_alt <- sepsis_lit_alt[, c("nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "year_match", "age_start", "age_end",  "measure",
                                               "cv_maternal_direct", "cv_hospital", "mean", "group_review","standard_error", "extractor", "is_outlier", "cv_inpatient", "cv_literature",
                                               "cv_anteparthemonly", "cv_postparthhemonly", "cv_severeonly", "cv_severehem_excl", "cv_marketscan", "is_reference", "age", "orig_dorms")]
sepsis_lit_alt <- plyr::rename(sepsis_lit_alt, c("mean"="mean_alt", "standard_error"="std_error_alt"))

#####################################################################################################################################################
# between-study matches: clinical - to - lit
match_sepsis_lit <- merge(sepsis_lit_ref, sepsis_lit_alt, by=c("measure", "location_id", "age_start", "age_end", "age"))
match_sepsis_lit$year_diff<- abs(match_sepsis_lit$year_match.x-match_sepsis_lit$year_match.y)
match_sepsis_lit<-subset(match_sepsis_lit, match_sepsis_lit$year_diff<6)
match_sepsis_lit <- subset(match_sepsis_lit, measure=="incidence")
match_sepsis_lit <- match_sepsis_lit[!is.na(match_sepsis_lit$mean_ref)]
setnames(match_sepsis_lit, "orig_dorms.x", "ref_dorms")
setnames(match_sepsis_lit, "orig_dorms.y", "alt_dorms")
match_sepsis_lit <- match_sepsis_lit%>%
  mutate(matched_id=paste(nid.x,nid.y,age,year_match.x,year_match.y, sep = " - ") )

write.csv(match_sepsis_lit, "FILEPATH", row.names = FALSE)

####################################################################################################################################################

# find unique age patterns in lit
lit_only <- sepsis_lit[clinical_data_type=="literature"]
lit_only$age <- (lit_only$age_start+lit_only$age_end)/2
lit_only <- as.data.table(lit_only)
lit_only$age_start <- sapply(1:nrow(lit_only), function(i) get_closest_age(i, var = "age_start", start=T, dt = lit_only))
lit_only$age_end <- sapply(1:nrow(lit_only), function(i) get_closest_age(i, var = "age_end", start = F, dt = lit_only))

test <- lit_only %>% distinct(location_id, age_start, age_end)

clinical_only <- sepsis_lit[clinical_data_type!="literature"]
clinical_locs <- unique(clinical_only$location_id)
# location/age groups for aggregation available in clinical data
test <- test[location_id%in%clinical_locs]
locs_for_match <- unique(test$location_id)
# subset clinical data to these locs
clinical_for_match <- clinical_only[location_id%in%locs_for_match]
combos <- lit_only %>% distinct(location_id, age_start, age_end, .keep_all = TRUE)

test_10_55 <- test[age_start==10 & age_end==55]
age_10_55_locs <- unique(test_10_55$location_id)

# 10-55s
for (i in age_10_55_locs){
  df <- clinical_for_match[location_id==i] 
  df2 <- test[location_id==i]              
  row_dt <- copy(df)
  row_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by=year_match]
  row_dt[, `:=` (mean = cases/sample_size, age_start = min(age_start), age_end = max(age_end))]
  row_dt[measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  row_dt[measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  row_dt[measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  row_dt <- dplyr::select(row_dt, names(row_dt)[!grepl("2$", names(row_dt))])
  row_dt <- row_dt %>% distinct(nid, year_match, .keep_all = TRUE)
  write.csv(row_dt, paste0("FILEPATH"))
  }


# combine all the agg location files for merging with lit
agg_data <- as.data.table(read.csv("FILEPATH"))
agg_data$age_start <- sapply(1:nrow(agg_data), function(i) get_closest_age(i, var = "age_start", start=T, dt = agg_data))
agg_data$age_end <- sapply(1:nrow(agg_data), function(i) get_closest_age(i, var = "age_end", start = F, dt = agg_data))

data_for_match <- rbind(lit_only, agg_data, fill=TRUE)
sepsis_lit_ref <- subset(data_for_match, is_reference==1)
sepsis_lit_ref <- sepsis_lit_ref[, c("nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "year_match", "age_start", "age_end",  "measure",
                                     "cv_maternal_direct", "cv_hospital", "mean","group_review", "standard_error", "extractor", "is_outlier", "cv_inpatient", "cv_literature",
                                     "cv_anteparthemonly", "cv_postparthhemonly", "cv_severeonly", "cv_severehem_excl", "cv_marketscan", "is_reference", "age", "orig_dorms")]
sepsis_lit_ref <- plyr::rename(sepsis_lit_ref, c("mean" = "mean_ref", "standard_error"="std_error_ref"))
sepsis_lit_alt <- subset(data_for_match, is_reference==0)
sepsis_lit_alt <- sepsis_lit_alt[, c("nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "year_match", "age_start", "age_end",  "measure",
                                     "cv_maternal_direct", "cv_hospital", "mean", "group_review","standard_error", "extractor", "is_outlier", "cv_inpatient", "cv_literature",
                                     "cv_anteparthemonly", "cv_postparthhemonly", "cv_severeonly", "cv_severehem_excl", "cv_marketscan", "is_reference", "age", "orig_dorms")]
sepsis_lit_alt <- plyr::rename(sepsis_lit_alt, c("mean"="mean_alt", "standard_error"="std_error_alt"))

#####################################################################################################################################################
# create matches
match_sepsis_lit <- merge(sepsis_lit_ref, sepsis_lit_alt, by=c("measure", "location_id", "age_start", "age_end", "age"))
match_sepsis_lit$year_diff<- abs(match_sepsis_lit$year_match.x-match_sepsis_lit$year_match.y)
match_sepsis_lit<-subset(match_sepsis_lit, match_sepsis_lit$year_diff<6)
match_sepsis_lit <- subset(match_sepsis_lit, measure=="incidence")
match_sepsis_lit <- match_sepsis_lit[!is.na(match_sepsis_lit$mean_ref)]
setnames(match_sepsis_lit, "orig_dorms.x", "ref_dorms")
setnames(match_sepsis_lit, "orig_dorms.y", "alt_dorms")
match_sepsis_lit <- match_sepsis_lit%>%
  mutate(matched_id=paste(nid.x,nid.y,age,year_match.x,year_match.y, sep = " - ") )

# merge with non-agg matches from earlier
noagg <- read.csv("FILEPATH")

match_sepsis_lit <- rbind(match_sepsis_lit, noagg, fill=TRUE)

write.csv(match_sepsis_lit, "FILEPATH")

################################################################################################################################################
# mr-brt script for sepsis: adjusting clinical to literature
################################################################################################################################################
remove(list = ls())
library(plyr)
library(openxlsx)
library(ggplot2)
library(reticulate)
library(dplyr)
reticulate::use_python("FILEPATH/python")
mr <- reticulate::import("mrtool")
cw <- reticulate::import("crosswalk")

################################################################################################################################################
# convert ratios to log space
# using within-study matches
sepsis <- as.data.table(read.csv("FILEPATH"))

# here, drop means of zero. matched pairs with zeros can be adjusted @ end but shouldnt be used as matched pairs
sepsis <- sepsis[!mean_ref==0]
sepsis <- sepsis[!mean_alt==0]

# calculate log-transformed means and sds for reference and alternative using cw$utils$linear_to_log from "crosswalk" package
# [1] represents first returned parameter, which is mean; [2] represents second returned parameter, which is sd
sepsis$mean_log_ref <-cw$utils$linear_to_log(mean = array(sepsis$mean_ref), sd = array(sepsis$std_error_ref))[[1]]
sepsis$sd_log_ref <- cw$utils$linear_to_log(mean = array(sepsis$mean_ref), sd = array(sepsis$std_error_ref))[[2]]

sepsis$mean_log_alt <-cw$utils$linear_to_log(mean = array(sepsis$mean_alt), sd = array(sepsis$std_error_alt))[[1]]
sepsis$sd_log_alt <- cw$utils$linear_to_log(mean = array(sepsis$mean_alt), sd = array(sepsis$std_error_alt))[[2]]

# take difference of log-transformed means (same mathematically as log ratio), and calculate sd of difference in log space
calculate_diff <- function(sepsis, alt_mean, alt_sd, ref_mean, ref_sd) {
  dat_diff <- data.table(mean_alt = sepsis[, mean_log_alt], mean_se_alt = sepsis[, sd_log_alt], 
                         mean_ref = sepsis[, mean_log_ref], mean_se_ref = sepsis[, sd_log_ref])
  dat_diff[, `:=` (diff = mean_alt-mean_ref, diff_se = sqrt(mean_se_alt^2 + mean_se_ref^2))] 
  return(dat_diff[,c("diff", "diff_se")])
}

diff <- calculate_diff(
  sepsis, alt_mean = "mean_log_alt", alt_sd = "sd_log_alt",
  ref_mean = "mean_log_ref", ref_sd = "sd_log_ref"
)

names(diff) <- c("log_diff_mean", "log_diff_se")
sepsis <- cbind(sepsis, diff)

################################################################################################################################################
# create crosswalk dataframe
dat1 <- cw$CWData(
  df = sepsis,
  obs = "log_diff_mean",
  obs_se = "log_diff_se",
  alt_dorms = "alt_dorms",
  ref_dorms = "ref_dorms",
  dorm_separator = " ",
  covs = list("age"),
  study_id = "matched_id",
  add_intercept = TRUE
)

# create crosswalk model
fit1 <- cw$CWModel(
  cwdata = dat1, 
  obs_type = "diff_log",
  cov_models = list(
    cw$CovModel("intercept"),
    cw$CovModel(cov_name = "age",
                spline = cw$XSpline(degree = 2L, knots = c(20, 30, 40, 50), 
                                    l_linear = TRUE,
                                    r_linear = TRUE))),
  gold_dorm = "literature")

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
  obs_method = 'clinical',
  plot_note = 'Funnel plot', 
  plots_dir = "FILEPATH",
  file_name = "funnel_plot",
  write_file = TRUE
)

# dose response curve for age
plots$dose_response_curve(
  dose_variable = 'age',
  obs_method = 'clinical', 
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

# original df, remove adjusted columns from first xwalk
df_orig <- as.data.table(read.csv("FILEPATH"))
df_orig <- df_orig[, meanvar_adjusted:=NULL][,sdvar_adjusted:=NULL][,pred_log:=NULL][, pred_se_log:=NULL][, data_id:=NULL]

df_orig <- as.data.table(df_orig)
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

# revert adjusted zeroes back to zero
df_orig <- df_orig[adj_zero_mean==1, mean:=0]
df_orig <- df_orig[adj_zero_mean==1, meanvar_adjusted:=0]

# write adjusted df
write.csv(df_orig, "/FILEPATH")

################################################################################################################################################
# age-split data into GBD age bins

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

gbd_id <- id 
version_id <- mvid 
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

# pull in file with adjusted means & sds from second xwalk
sepsis <- read.csv("FILEPATH")
sepsis <- as.data.table(sepsis)
sepsis <- sepsis[, age_end:=age_end-1]
sepsis[age_start>age_end, age_start:=age_start-5]
sepsis <- sepsis[, mean:=meanvar_adjusted]
sepsis <- sepsis[, standard_error:=sdvar_adjusted]
sepsis <- sepsis[orig_dorms!="literature", upper:=NA]
sepsis <- sepsis[orig_dorms!="literature", lower:=NA]


#subset to rows with age grange >5 that need split. Set aside age-specific data. Leave zeroes (cases or mean) alone
sepsis <- sepsis[, age_range:=age_end-age_start]
sepsis_already_split <- sepsis[age_range<=5 | mean==0 | cases==0]
sepsis_to_split <- sepsis[age_range>5 & mean!=0 & cases!=0]
sepsis_to_split[, id := 1:.N]

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
  split <- split[age_group_id>6 & age_group_id<16] ##don't keep where age group id isn't estimated for cause
  return(split)
}

split_df <- expand_age(sepsis_to_split, age_dt=ages)
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
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
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

# merge the births
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
#revert to original year
split_df[, year_id:=orig_year_id]

# re-calculate se with split sample sizes
split_df[, standard_error := NA][, upper:=NA][, lower:=NA][, uncertainty_type_value:=NA]
split_df <- get_se(split_df)

# label split rows
split_df <- split_df[, split_row:=1]

# combine with data that were already split
sepsis <- rbind(sepsis_already_split, split_df, fill=TRUE)

# label with parent_seqs & save 
split_ids <- unique(sepsis$id)
sepsis$crosswalk_parent_seq <- NA
sepsis$crosswalk_parent_seq <- as.numeric(sepsis$crosswalk_parent_seq)
sepsis[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
sepsis[, seq:=NA]
sepsis <- subset(sepsis, !(mean==0 & is.na(lower) & is.na(cases) & is.na(sample_size) & is.na(standard_error) & is.na(effective_sample_size)))
sepsis[, sex:="Female"]
sepsis <- sepsis[group_review==1 | is.na(group_review)]
sepsis <- sepsis[measure=="incidence"]

# outlier sources that use clinical envelope
envelope_new <- read.xlsx("FILEPATH")
envelope_new <- envelope_new%>%mutate(nid=ifelse(merged_nid=="na",nid,merged_nid))
envelope_distinct_new <- envelope_new %>% distinct(nid, uses_env,.keep_all = TRUE)
envelope_distinct_new$nid <- as.numeric(envelope_distinct_new$nid)

sepsis <- as.data.table(merge(sepsis, envelope_distinct_new[, c("uses_env", "nid")], by="nid", all.x=TRUE))
sepsis <- sepsis[uses_env==1, is_outlier:=1][uses_env==1, note_modeler:="outliered because inpatient source uses envelope"]

# outliering terminal age groups
sepsis <- sepsis[age_start==10, is_outlier:=1][age_start==10, note_modeler:="outliered terminal age group"]
sepsis <- sepsis[age_start==50, is_outlier:=1][age_start==50, note_modeler:="outliered terminal age group"]

# test outlier marketscan
ms_nids <- c(244369, 244370, 244371, 336847, 336848, 336849, 336850, 408680, 433114, 494351, 494352)
sepsis <- sepsis[nid%in%ms_nids, is_outlier:=1][nid%in%ms_nids, note_modeler:="test outliering marketscan"]

# outliering identified source
sepsis <- sepsis[nid==468903, is_outlier:=1][nid==468903, note_modeler:="outlier California source"]
sepsis <- sepsis[nid==520657, is_outlier:=1][nid==520657, note_modeler:="outlier California source"]
write.xlsx(sepsis, "FILEPATH")
write.csv(sepsis, "FILEPATH")



