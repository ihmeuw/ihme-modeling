###############################################################################################################################################
# Purpose: data processing for other maternal infections input data
# matchfinding, run mr-brt, apply adjustments, and age-split
###############################################################################################################################################

remove(list = ls())
source("FILEPATH.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/cw_mrbrt_helper_functions.R")
library(dplyr)
library(data.table)

###############################################################################################################################################
# other maternal infx
infx <- get_bundle_version(bundle_version_id=bvid)

# calc mean and SE where mean and SE are NA
infx <- get_cases_sample_size(infx)
infx <- get_se(infx)
infx <- calculate_cases_fromse(infx)
infx <- calc_year(infx)

infx$clinical_data_type <- ifelse(infx$clinical_data_type=="", "literature", infx$clinical_data_type)
infx<-subset(infx, clinical_data_type!="literature")
infx$is_reference <- ifelse(infx$clinical_data_type=="claims", 0, 1)

infx$age <- (infx$age_start + infx$age_end)/2
infx$age_start <- sapply(1:nrow(infx), function(i) get_closest_age(i, var = "age_start", start=T, dt = infx))
infx$age_end <- sapply(1:nrow(infx), function(i) get_closest_age(i, var = "age_end", start = F, dt = infx))

#set column for alt_dorms and ref_dorms for mrbrt
infx <- infx[is_reference==0, orig_dorms:="claims"]
infx$orig_dorms[is.na(infx$orig_dorms)] <- "inpatient"
infx <- infx[measure=="incidence" & mean==0, adj_zero_mean:=1]
infx <- infx[, orig_mean:=mean]
infx <- infx[measure=="incidence" & mean==0, mean:=(0.5*5.447513e-05)]

write.csv(infx,"FILEPATH")
infx <- infx[, mean:=orig_mean]

####################################################################################################################################################
# label for matchfinding
infx_ref <- subset(infx, is_reference==1)
infx_ref <- infx_ref[, c("nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "year_match", "age_start", "age_end",  "measure",
                         "cv_maternal_direct", "cv_hospital", "mean","group_review", "standard_error", "extractor", "is_outlier", "cv_inpatient", "cv_literature",
                         "cv_anteparthemonly", "cv_postparthhemonly", "cv_severeonly", "cv_severehem_excl", "is_reference", "age", "orig_dorms")]
infx_ref <- plyr::rename(infx_ref, c("mean" = "mean_ref", "standard_error"="std_error_ref"))
infx_alt <- subset(infx, is_reference==0)
infx_alt <- infx_alt[, c("nid", "input_type", "source_type", "location_id", "location_name", "year_start", "year_end", "year_match", "age_start", "age_end",  "measure",
                         "cv_maternal_direct", "cv_hospital", "mean", "group_review","standard_error", "extractor", "is_outlier", "cv_inpatient", "cv_literature",
                         "cv_anteparthemonly", "cv_postparthhemonly", "cv_severeonly", "cv_severehem_excl", "is_reference", "age", "orig_dorms")]
infx_alt <- plyr::rename(infx_alt, c("mean"="mean_alt", "standard_error"="std_error_alt"))

####################################################################################################################################################
# between-study matches
match_infx <- merge(infx_ref, infx_alt, by=c("measure", "location_id", "age_start", "age_end", "age"))
match_infx$year_diff<- abs(match_infx$year_match.x-match_infx$year_match.y)
match_infx<-subset(match_infx, match_infx$year_diff<6)
match_infx <- subset(match_infx, measure=="incidence")
match_infx <- match_infx[!is.na(match_infx$mean_ref)]
setnames(match_infx, "orig_dorms.x", "ref_dorms")
setnames(match_infx, "orig_dorms.y", "alt_dorms")
# create matched_id for between-study matches
match_infx <- match_infx%>%
  mutate(matched_id=paste(nid.x,nid.y,age,year_match.x,year_match.y, sep = " - ") )

write.csv(match_infx, "FILEPATH")

################################################################################################################################################
# mr-brt script for other_infx
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
infx <- as.data.table(read.csv("FILEPATH"))

# here, drop means of zero. matched pairs with zeros can be adjusted @ end but shouldnt be used as matched pairs
infx <- infx[!mean_ref==0]
infx <- infx[!mean_alt==0]

# calculate log-transformed means and sds for reference and alternative using cw$utils$linear_to_log from "crosswalk" package
# [1] represents first returned parameter, which is mean; [2] represents second returned parameter, which is sd
infx$mean_log_ref <-cw$utils$linear_to_log(mean = array(infx$mean_ref), sd = array(infx$std_error_ref))[[1]]
infx$sd_log_ref <- cw$utils$linear_to_log(mean = array(infx$mean_ref), sd = array(infx$std_error_ref))[[2]]

infx$mean_log_alt <-cw$utils$linear_to_log(mean = array(infx$mean_alt), sd = array(infx$std_error_alt))[[1]]
infx$sd_log_alt <- cw$utils$linear_to_log(mean = array(infx$mean_alt), sd = array(infx$std_error_alt))[[2]]

# take difference of log-transformed means (same mathematically as log ratio), and calculate sd of difference in log space
calculate_diff <- function(infx, alt_mean, alt_sd, ref_mean, ref_sd) {
  dat_diff <- data.table(mean_alt = infx[, mean_log_alt], mean_se_alt = infx[, sd_log_alt], 
                         mean_ref = infx[, mean_log_ref], mean_se_ref = infx[, sd_log_ref])
  dat_diff[, `:=` (diff = mean_alt-mean_ref, diff_se = sqrt(mean_se_alt^2 + mean_se_ref^2))] 
  return(dat_diff[,c("diff", "diff_se")])
}

diff <- calculate_diff(
  infx, alt_mean = "mean_log_alt", alt_sd = "sd_log_alt",
  ref_mean = "mean_log_ref", ref_sd = "sd_log_ref"
)

names(diff) <- c("log_diff_mean", "log_diff_se")
infx <- cbind(infx, diff)

################################################################################################################################################
# create crosswalk dataframe
dat1 <- cw$CWData(
  df = infx,
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
                spline = cw$XSpline(degree = 2L, knots = c(12, 20, 30, 40, 50), 
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
  plot_note = "dose-response plot", 
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

################################################################################################################################################
# age-split lit data into GBD age bins
################################################################################################################################################
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

################################################################################################################################################
# pull in bv to grab literature, append on adjusted clinical data
other_infections <- get_bundle_version(bundle_version_id=bvid)

# calc mean and SE where mean and SE are NA
other_infections <- get_cases_sample_size(other_infections)
other_infections <- get_se(other_infections)
other_infections <- calculate_cases_fromse(other_infections)
other_infections <- calc_year(other_infections)
other_infections$clinical_data_type <- ifelse(other_infections$clinical_data_type=="", "literature", other_infections$clinical_data_type)
other_infections_lit <- other_infections[clinical_data_type=="literature"]

# pull in file with adjusted means & sds
other_infx <- as.data.table(read.csv("FILEPATH"))
other_infx <- as.data.table(other_infx)
other_infx <- other_infx[, age_end:=age_end-1]
other_infx[age_start>age_end, age_start:=age_start-5]
other_infx <- other_infx[, mean:=meanvar_adjusted]
other_infx <- other_infx[, standard_error:=sdvar_adjusted]
other_infx <- other_infx[orig_dorms!="inpatient", upper:=NA]
other_infx <- other_infx[orig_dorms!="inpatient", lower:=NA]

#merge with unadj
other_infx <- rbind(other_infections_lit, other_infx, fill=TRUE)

#subset to rows with age grange >5 that need split. Set aside age-specific data. 
other_infx <- other_infx[, age_range:=age_end-age_start]
other_infx_already_split <- other_infx[age_range<=5 | mean==0 | cases==0]
other_infx_to_split <- other_infx[age_range>5 & mean!=0 & cases!=0]
other_infx_to_split[, id := 1:.N]


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

split_df <- expand_age(other_infx_to_split, age_dt=ages)
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

#  merge the births 
split_df <- merge(split_df, births_structure, by = c("location_id", "year_id", "age_group_id"))

# #################################################################################################################################################
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

split_df[, standard_error := NA][, upper:=NA][, lower:=NA][, uncertainty_type_value:=NA]
split_df <- get_se(split_df)

# label split rows
split_df <- split_df[, split_row:=1]

# combine with data that were already split
other_infx <- rbind(other_infx_already_split, split_df, fill=TRUE)

# label with parent_seqs & save 
split_ids <- unique(other_infx$id)
other_infx$crosswalk_parent_seq <- NA
other_infx$crosswalk_parent_seq <- as.numeric(other_infx$crosswalk_parent_seq)
other_infx[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
other_infx[, seq:=NA]
other_infx <- subset(other_infx, !(mean==0 & is.na(lower) & is.na(cases) & is.na(sample_size) & is.na(standard_error) & is.na(effective_sample_size)))
other_infx[, sex:="Female"]
other_infx <- other_infx[group_review==1 | is.na(group_review)]
other_infx <- other_infx[measure=="incidence"]

########################################################################################################################################################
# outlier clinical envelope sources
envelope_new <- read.xlsx("FILEPATH")
envelope_new <- envelope_new%>%mutate(nid=ifelse(merged_nid=="na",nid,merged_nid))
envelope_distinct_new <- envelope_new %>% distinct(nid, uses_env,.keep_all = TRUE)
envelope_distinct_new$nid <- as.numeric(envelope_distinct_new$nid)

other_infx <- as.data.table(merge(other_infx, envelope_distinct_new[, c("uses_env", "nid")], by="nid", all.x=TRUE))
other_infx$note_modeler <- as.character(other_infx$note_modeler)
other_infx <- other_infx[uses_env==1, is_outlier:=1][uses_env==1, note_modeler:="outliered because inpatient source uses envelope"]

# outliering terminal age groups
other_infx <- other_infx[age_start==10, is_outlier:=1][age_start==10, note_modeler:="outliered terminal age group"]
other_infx <- other_infx[age_start==50, is_outlier:=1][age_start==50, note_modeler:="outliered terminal age group"]

# outlier near miss 
other_infx <- other_infx[cv_diag_severe==1, is_outlier:=1][cv_diag_severe==1, note_modeler:="outliered for near-miss case definition"]

# outlier belarus, latvia, lithuania for quantitative reasons
outliers <- c(57,59,60)
other_infx <- other_infx[location_id%in%outliers, is_outlier:=1][location_id%in%outliers, note_modeler:="quantitative outliers: belarus, latvia, lithuania"]

# outlier marketscan
ms_nids <- c(244369, 244370, 244371, 336847, 336848, 336849, 336850, 408680, 433114, 494351, 494352)
other_infx <- other_infx[nid%in%ms_nids, is_outlier:=1][nid%in%ms_nids, note_modeler:="outlier marketscan"]

write.xlsx(other_infx, "FILEPATH")
write.csv(other_infx, "FILEPATH")
