###############################################################################################################################################
# Purpose: data processing for abortion and miscarriage input data
# matchfinding, run mr-brt, apply adjustments, and age-split
###############################################################################################################################################

remove(list = ls())
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/cw_mrbrt_helper_functions.R")
library(dplyr)

abort <- get_bundle_version(bundle_version_id=BVID)

# calc mean and SE where mean and SE are NA
abort <- get_cases_sample_size(abort)
abort <- get_se(abort)
abort <- calculate_cases_fromse(abort)
abort <- calc_year(abort)

abort$is_reference <- ifelse(abort$cv_surveillance==1, 1,0) 
abort[is.na(abort)] <- ""

abort <- subset(abort, input_type!="group_review")
abort$age <- (abort$age_start + abort$age_end)/2
abort$age_start <- sapply(1:nrow(abort), function(i) get_closest_age(i, var = "age_start", start=T, dt = abort))
abort$age_end <- sapply(1:nrow(abort), function(i) get_closest_age(i, var = "age_end", start = F, dt = abort))

#set column for alt and ref
abort <- abort[cv_surveillance==1, orig_dorms:="surveillance"]
abort <- abort[clinical_data_type=="inpatient" | clinical_data_type=="claims", orig_dorms:="clinical"]
abort$orig_dorms[is.na(abort$orig_dorms)] <- "other"

# offset mean=0 with half of the lowest observed value
abort <- abort[measure=="incidence" & mean==0, adj_zero_mean:=1]
abort <- abort[, orig_mean:=mean]
abort <- abort[measure=="incidence" & mean==0, mean:=(0.5*0.0002896947)]

write.csv(abort, FILEPATH, row.names = FALSE)

# label for matchfinding
abort <- abort[, mean:=orig_mean]
abort_ref <- subset(abort, is_reference==1)
abort_ref <- abort_ref[, c("nid", "input_type", "source_type", "location_id", "location_name" , "year_start" , "year_end" , "year_match", "age_start" , "age_end",  "measure",
                            "mean" ,"group_review","standard_error",  "extractor", "is_outlier", "cv_surveillance", "cv_inpatient", "cv_literature", "cv_dhs", "cv_hospital", "cv_diag_severe", 
                            "cv_ectopic_excl", "cv_onlyelectiveab", "cv_not_represent", "cv_spontaneous_abortion", "is_reference", "age", "orig_dorms")]
abort_ref <- plyr::rename(abort_ref, c("mean" = "mean_ref", "standard_error"="std_error_ref"))
abort_alt <- subset(abort, orig_dorms=="clinical")
abort_alt <- abort_alt[, c("nid", "input_type", "source_type", "location_id", "location_name" , "year_start" , "year_end" , "year_match", "age_start" , "age_end",  "measure",
                           "mean" ,"group_review","standard_error", "extractor", "is_outlier", "cv_surveillance", "cv_inpatient", "cv_literature", "cv_dhs", "cv_hospital", "cv_diag_severe", 
                           "cv_ectopic_excl", "cv_onlyelectiveab", "cv_not_represent", "cv_spontaneous_abortion", "is_reference", "age", "orig_dorms")]
abort_alt <- plyr::rename(abort_alt, c("mean"="mean_alt", "standard_error"="std_error_alt"))

#####################################################################################################################################################
# between-study matches
match_abort_between <- merge(abort_ref, abort_alt, by=c("location_id", "year_start", "year_end"))
match_abort_between$age_diff <- abs(match_abort_between$age.x-match_abort_between$age.y)
match_abort_between <- match_abort_between[age_diff<6]
match_abort_between <- match_abort_between[measure.x=="incidence"]
match_abort_between <- match_abort_between[!is.na(match_abort_between$mean_ref)]
setnames(match_abort_between, "orig_dorms.x", "ref_dorms")
setnames(match_abort_between, "orig_dorms.y", "alt_dorms")
match_abort_between <- subset(match_abort_between, mean_ref!=0)

# create matched_id
match_abort_between <- match_abort_between%>%
  mutate(matched_id=paste(nid.x,nid.y,age.x,age.y,year_match.x,year_match.y, sep = " - ") )

write.csv(match_abort_between, FILEPATH)

#####################################################################################################################################################
################################################################################################################################################
# mr-brt script for abortion and miscarriage
# xw clinical data to surveillance data
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
df <- as.data.table(read.csv(FILEPATH))

# calculate log-transformed means and sds for reference and alternative using cw$utils$linear_to_log from "crosswalk" package
# [1] represents first returned parameter, which is mean; [2] represents second returned parameter, which is sd
abort$mean_log_ref <-cw$utils$linear_to_log(mean = array(abort$mean_ref), sd = array(abort$std_error_ref))[[1]]
abort$sd_log_ref <- cw$utils$linear_to_log(mean = array(abort$mean_ref), sd = array(abort$std_error_ref))[[2]]

# offset zeroes with replace with half the lowest observed value
abort <- abort[mean_alt==0, mean_alt:=(0.5*0.001683949)]
abort$mean_log_alt <-cw$utils$linear_to_log(mean = array(abort$mean_alt), sd = array(abort$std_error_alt))[[1]]
abort$sd_log_alt <- cw$utils$linear_to_log(mean = array(abort$mean_alt), sd = array(abort$std_error_alt))[[2]]

# calculate difference of log-transformed means and calculate sd of difference in log space
calculate_diff <- function(abort, alt_mean, alt_sd, ref_mean, ref_sd) {
  dat_diff <- data.table(mean_alt = abort[, mean_log_alt], mean_se_alt = abort[, sd_log_alt], 
                         mean_ref = abort[, mean_log_ref], mean_se_ref = abort[, sd_log_ref])
  dat_diff[, `:=` (diff = mean_alt-mean_ref, diff_se = sqrt(mean_se_alt^2 + mean_se_ref^2))] 
  return(dat_diff[,c("diff", "diff_se")])
}

diff <- calculate_diff(
  abort, alt_mean = "mean_log_alt", alt_sd = "sd_log_alt",
  ref_mean = "mean_log_ref", ref_sd = "sd_log_ref"
)

names(diff) <- c("log_diff_mean", "log_diff_se")
abort <- cbind(abort, diff)

################################################################################################################################################
# create crosswalk dataframe
dat1 <- cw$CWData(
  df = abort,
  obs = "log_diff_mean",
  obs_se = "log_diff_se",
  alt_dorms = "alt_dorms",
  ref_dorms = "ref_dorms",
  dorm_separator = " ",
  covs = list("age"),
  study_id = "nid.x",
  add_intercept = TRUE
)

# create crosswalk model
fit1 <- cw$CWModel(
  cwdata = dat1, 
  obs_type = "diff_log",
  cov_models = list(
    cw$CovModel("intercept"),
    cw$CovModel(cov_name = "age",
                spline = cw$XSpline(degree = 2L, knots = c(12, 20, 30, 40, 45), 
                                    l_linear = TRUE,
                                    r_linear = TRUE))),
  gold_dorm = "surveillance")

# fit model
fit1$fit()

# create table of results
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
  #file.path(getwd(), "plots"), 
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
  plot_note = "Dose-response plot", 
  plots_dir = "FILEPATH", 
  file_name = "doseresponse_plot", 
  write_file = TRUE
)

################################################################################################################################################
# apply adjustment

# original df
df_orig <- as.data.table(read.csv("FILEPATH"))

# remove rows not in alt or ref dorms
df_orig <- as.data.table(df_orig)
df_orig <- df_orig[orig_dorms=="clinical" | orig_dorms=="surveillance"]
df_orig <- df_orig[!is.na(df_orig$mean)]
df_orig <- df_orig[!is.na(df_orig$standard_error)]
df_orig <- df_orig[measure=="incidence"]

# adjust clinical rows
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

#####################################################################################################################################################
################################################################################################################################################
# age-split aggregate data into GBD age bins

#setup

remove(list = ls())
library(dplyr)
library(data.table)
library(readxl)
library('openxlsx')
source("FILEPATH/get_draws.R")
source("FILEPATH/get_population.R")
source("FILEPATHget_covariate_estimates.R")
source("FILEPATHget_bundle_version.R")
source("FILEPATH/cw_mrbrt_helper_functions.R")
source("FILEPATH/get_age_metadata.R")

gbd_id <- ME 
version_id <- MVID
draws <- paste0("draw_", 0:999)

ages <- get_age_metadata(release_id=16)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 10 & age_end <=55, age_group_id]
ages[, age_group_weight_value := NULL]
ages[age_start >= 1, age_end := age_end - 1]
ages[age_end == 124, age_end := 102]
super_region_dt <- get_location_metadata(location_set_id = 22, release_id=16)
super_region_dt <- super_region_dt[, .(location_id, super_region_id)]

################################################################################################################################################
# pull in bv for literature, append adjusted data
abortion <- get_bundle_version(bundle_version_id=BVID)

# calc mean and SE where mean and SE are NA
abortion <- get_cases_sample_size(abortion)
abortion <- get_se(abortion)
abortion <- calculate_cases_fromse(abortion)
abortion <- calc_year(abortion)
abortion <- abortion[cv_surveillance==1, orig_dorms:="surveillance"]
abortion <- abortion[clinical_data_type=="inpatient" | clinical_data_type=="claims", orig_dorms:="clinical"]
abortion$orig_dorms[is.na(abortion$orig_dorms)] <- "other"

#subset
abortion_noadj <- abortion[orig_dorms=="other"]

# pull in file with adjusted means & sds
abort <- read.csv("FILEPATH")
abort <- as.data.table(abort)
abort <- abort[, mean:=meanvar_adjusted]
abort <- abort[, standard_error:=sdvar_adjusted]
abort <- abort[orig_dorms!="surveillance", upper:=NA]
abort <- abort[orig_dorms!="surveillance", lower:=NA]

# merge with unadjusted values
abort <- rbind(abortion_noadj, abort, fill=TRUE)

#subset to rows with age grange >5 that need split. Set aside age-specific data. 
abort <- abort[, age_range:=age_end-age_start]
abort_already_split <- abort[age_range<=5 | mean==0 | cases==0]
abort_to_split <- abort[age_range>5 & mean!=0 & cases!=0]
abort_to_split[, id := 1:.N]
abort_to_split[, age_group_id:=NULL]

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

split_df <- expand_age(abort_to_split, age_dt=ages)
setnames(split_df, "year_match", "year_id")

################################################################################################################################################
# Which locations and years need split? 
pop_locs <- unique(split_df$location_id)
pop_years <- unique(split_df$year_id)
pop_years <- unique(round_any(pop_years, 5))

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
  population <- get_population(location_id = locs, year_id = years, release_id=16,
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

# merge births in
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

# re-calculate se with split sample sizes
split_df[, standard_error := NA][, upper:=NA][, lower:=NA][, uncertainty_type_value:=NA]
split_df <- get_se(split_df)

# label split rows
split_df <- split_df[, split_row:=1]

# combine with data that were already split
abort <- rbind(abort_already_split, split_df, fill=TRUE)

# label with parent seqs
split_ids <- unique(abort$id)
abort$crosswalk_parent_seq <- NA
abort$crosswalk_parent_seq <- as.numeric(abort$crosswalk_parent_seq)
abort[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
abort[, seq:=NA]
abort <- subset(abort, !(mean==0 & is.na(lower) & is.na(cases) & is.na(sample_size) & is.na(standard_error) & is.na(effective_sample_size)))
abort[, sex:="Female"]
abort <- abort[group_review==1 | is.na(group_review)]
abort <- abort[measure=="incidence"]

# outlier sources that are neither clinical nor surveillance
source_types <- unique(abort$source_type)
abort <- abort[source_type=="Survey - cross-sectional" | source_type=="Registry - other/unknown" | source_type=="Vital registration - national", 
               is_outlier:=1]

# outlier clinical envelope sources
envelope_new <- read.xlsx("FILEPATH")
envelope_new <- envelope_new%>%mutate(nid=ifelse(merged_nid=="na",nid,merged_nid))
envelope_distinct_new <- envelope_new %>% distinct(nid, uses_env,.keep_all = TRUE)
envelope_distinct_new$nid <- as.numeric(envelope_distinct_new$nid)

abort <- as.data.table(merge(abort, envelope_distinct_new[, c("uses_env", "nid")], by="nid", all.x=TRUE))
abort <- abort[uses_env==1, is_outlier:=1][uses_env==1, note_modeler:="outliered because inpatient source uses envelope"]

write.xlsx(abort, "FILEPATH", sheetName = "extraction")
write.csv(abort, "FILEPATH")

# outlier terminal age groups
abort <- abort[age_start==10, is_outlier:=1][age_start==10, note_modeler:="outliered terminal age group"]
abort <- abort[age_start==50, is_outlier:=1][age_start==50, note_modeler:="outliered terminal age group"]
write.xlsx(abort, "FILEPATH", sheetName = "extraction")
write.csv(abort, "FILEPATH")

# outlier near miss sources
abort <- as.data.table(read.csv("FILEPATH"))
abort <- abort[cv_diag_severe==1, is_outlier:=1][cv_diag_severe==1, note_modeler:="outliered for near-miss case definition"]
write.xlsx(abort, "FILEPATH")
write.csv(abort, "FILEPATH")

# outlier marketscan
ms_nids <- c(244369, 244370, 244371, 336847, 336848, 336849, 336850, 408680, 433114, 494351, 494352)
abort <- abort[nid%in%ms_nids, is_outlier:=1][nid%in%ms_nids, note_modeler:="outliered marketscan"]

write.xlsx(abort, "FILEPATH")
write.csv(abort, "FILEPATH")

