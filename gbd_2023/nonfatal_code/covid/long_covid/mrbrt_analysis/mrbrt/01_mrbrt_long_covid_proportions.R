#--------------------------------------------------------------
# Name: NAME (USERNAME)
# Date: 27 Feb 2021
# Project: GBD nonfatal COVID
# Purpose: estimate long COVID duration among mild/moderate cases and among hospital cases
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())
# setwd("FILEPATH")

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH'
  h_root <- '~/'
} else {
  j_root <- 'FILEPATH/'
  h_root <- 'FILEPATH/'
}


# load packages
pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)

library(reticulate)
library(msm)
library(tidyr)
library(dplyr)

Sys.setenv("RETICULATE_PYTHON" = "FILEPATH")
use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")


folder <- "FILEPATH"
outputfolder <- "FILEPATH"


cvs <- list("hospital", "icu", "female", "male", "follow_up_days", "other_list")

datadate <- '101424'
# datadate <- '100724'
# datadate <- '090924'
# datadate <- '012822ziyad'
# datadate <- '012822'

# "log" for log-linear models, "logit" for logit-linear models
shape <- 'logit'
n_samples <- 1000L
max_draw <- n_samples - 1
year_start <- 2020
year_end <- 2024
drop_cough <- 1
min_cv <- 0.1
# add measurement noise to standard_error: 10% of median per outcome?  0?
meas_noise <- 0
include_gamma <- 0

# make kid estimates sex-specific?
kid_estimate_sex_specific_c <- 0
kid_estimate_sex_specific_h <- 0

# make separate estimates among kids for hosp/ICU models?  likely not given data sparsity
hosp_kids <- 0
# holdover
pop <- 'all'

# drop data with follow_up_days<X
data_followup_cutoff <- 0

follow_up_sd_multiplier <- 10
inlier <- 0.9
version <- 119


# 118 incorporate waning risk by calendar time in proportion models
# 117 BEST FOR GBD 2023: make RE study-variant instead of just study, fix used_data weight to correspond to data se instead of logit space se, updated used_data code to not risk merging on mean and standard error incorrectly
# 116 fixed a couple typos in data, rerun proportion models; ACCIDENTALLY DROPPED A COUPLE COHORTS WITH BLANK MEASURE
# 115 estimate waning risk by calendar time
# 114 testing 'any' envelope with 50 draws
# 113 NAME testing
# 112 logit-linear, no gamma.
# 111 log-linear, no gamma.
# 110 logit-linear, no meas_noise. include gamma.
# 109 log-linear, include gamma. no meas_noise.  FIX RR_SUMMARIES UI!!!  (lower and upper were calculated incorrectly before), remove "administrative" covariate from models because VERY unstable, almost nothing to inform it
# 108 min CV 0.1, meas_noise 1%
# 107 log-linear, no slope prior on any_main. follow-up-days prior factor 10, 1000 draws, min noise 1% of median by outcome
# 106 log-linear, no slope prior on any_main. follow-up-days prior factor 10, 1000 draws, min noise 10% of median by outcome
# 105 logit-linear, no slope prior on any_main. follow-up-days prior factor 10.  drop follow_up_days<=14
# 104 log-linear, no slope prior on any_main. follow-up-days prior factor 10, 100 draws
# 103 log-linear, no slope prior on any_main. follow-up-days prior factor 10, 100 draws
# 102 log-linear, no slope prior on any_main. icu/children/female/male priors. drop follow_up_days<=14
# 101 log-linear, no slope prior on any_main.  icu, children, and female/male priors.  any_main still not converging for community at least
# 100 log-linear, no slope prior on any_main.  icu and female/male priors.  no children prior
# 99 calc duration from any_main model, to see if we can drop duration model altogether.  would potentially result in different duration for kids vs adults, M vs F (but i think same because beta on follow up days will be same within a model)
# 98 removed data points with 0 sample size
# 97 ?
# 96 fixed symptom-specific flags
# 95 Drop UK Virus Watch exclusion
# 94 GBD 2023 with updated extractions
# 93 GBD 2023 data
# 92 create 'any' figures with y-axis 0.4 for NAME's WHO preso 8/17/22
# 91 81 + updated Ziyad data with G93.3 added to both VA and PRA for JAMA RTR response
# 90 rerun 81 to test consistency of coeffs in fat model
# 89 81 + updated Ziyad data with G93.3 for JAMA RTR response
# 88 duration model to include all 'any' data, with 10% trimming, yes slope prior (like v81)
# 87 duration model to include all 'any' data, with no trimming, no slope prior
# 86 duration model to include all 'any' data, with no trimming, yes slope prior
# 85 duration model to include all 'any' data, with 10% trimming, no slope prior
# 84 duration model to include all 'any' data
# 83 test models for JAMA revisions: run envelope with no prior on slope
# 82 test models for JAMA revisions: proportions with no trimming
# 81 fixed Berg standard error (accidentally dropped in v80).  FINAL MODEL FOR GBD AND JAMA PAPER!!!!!!!!!!!!!!!!!!!!!  inlier = 0.9
# 80 no prior on other_list for any long COVID models
# 79 updated CO-FLOW with 12 month and larger sample size, added Berg paper from Denmark
# 78 added 3 studies and re-extracted Zurich
# 77 prior on follow_up_days sd * 1 instead of * 0.5
# 76 prior on follow_up_days sd * 0.5 instead of * 3
# 75 hosp all age, not separate for kids anymore
# 74 back to logit-linear.  updated RUS data where 12 mo ongoing data only counts long COVID cases if they had long COVID at 6mo
# 73 try log(follow_up_days) with outcome in linear space
# 72 separate estimates for hosp kids
# 71 no peds data in comm dur
# 70 (other code file) try hosp dur in log-linear instead of logit-linear (led to much wider uncertainty around very similar estimate, so nothing to gain there)
# 69 add SWE PronMed to hosp dur, only keep "any" in duration models
# 68 update pa-COVID resp algorithm
# 67 including RUS in hosp duration model
# 66 updated data for: SWE, RUS, pa-COVID, Heesakkers et al
# 65 change sd on follow_up_time to sd*3 for comm, sd*2 for hosp
# 64 increase sd on other_list prior for 'any' model to 3*sd.  fixed data going into children prior model
# 63 min CV = 0.1, inlier = 0.9
# 62 min CV = 0.1, inlier = 0.8, fix studies that were accidentally dropped
# 61 min CV = 0.1, cohort data only (exclude published lit)
# 60 min CV = 0.1, fixed Cirulli and other studies where cases were missing, hopefully final model for resubmission
# 59 min CV = 0, calc standard_error of cases<5 based on Wilson's formula
# 58 add 13 new studies, fix follow-up time for "30 days after COVID diagnosis" and "COVID diagnosis" among ICU
# 57 drop cough from resp model
# 56 for hosp/icu any and symptom cluster models, estimate across all ages
# 55 prior on children_comm and children_hosp, fixed priors on female and male (the model had included 2 extra both-sex UK CIS data points by accident)
# 54 min CV = 0.1, change VA follow-up days to midpoint of 6 months after acute phase
# 53 same as 52, but predict for both sexes for kids rather than male vs female, overlap not by follow-up anymore
# 52 kids vs adults, offset, drop Qu, drop Jacobson any
# 51 other code, submission + data, no offset, drop Qu, drop Jacobson any
# 50 other code, submission + data, offset, drop Qu, drop Jacobson any
# 49 kids vs adults, no offset, drop Qu, drop Jacobson any
# 48  kids vs adults, offset
# 47 other code, all data together, offset
# 46 other code, all data together, no offset
# 45 fix graphs for adjusted data, no offset
# 44 changed PRA follow_up_days to be allowed to differ again, because the RE on study/follow-up-days works fine
# 43 all data modeled together with covariate on children
# 42 fixed PRA follow_up_days to be same value within age/sex/hosp/icu/symptom cluster so that the ICU data don't cause the model to want an uptick
# 41 separate models for children vs adults
# 40 TEST RUN of all data.  added 4-5 lit sources, updated UK CIS, updated Faroe of both waves without asymptomatic.  next run needs to model children separate from adults.
# 39 change VA and PRA follow-up days to midpoint of follow-up period
# 38 estimate overlaps over follow-up time.  severities remain single time point
# 37 same, final
# 36 same, issues writing files
# 35 fixed ICU follow-up days if from hospital discharge, added resp severities for CO-FLOW
# 34 beta_hosp_sd*3 and beta_comm_sd*3
# 33 inlier_pct = 0.9 in any/fat/rsp/cog; beta_hosp_sd and beta_comm_sd as priors for fat, rsp, cog
# 32 inlier_pct = 0.8 in any, fat, rsp, cog models;  beta_hosp_sd*3 and beta_comm_sd*3 as priors for fat, rsp, cog
# 31 accidentally had dropped pa-COVID before ... now included
# 30 1000 draws
# 29 fixed RUS adults follow up time
# 28 exclude PRA in overlap models, 1000 draws
# 27 include PRA in overlap models
# 26 data fixes
# 25 updated data with Zurich updated data, post-adjustment for 6 mo data
# 24 updated data with PRA data
# 23 1000 draws
# 22 exclude pa-COVID and HAARVI for now.  wait on email from Frido, and figure out better follow-up time for HAARVI
# 21 x-axis in days
# 20 exclude zeroes; model comm, hosp, ICU separately
# 19 pseudo adjust data without pre-COVID criteria downward by 25%
# 18 like #11 include all symptom clusters with >1 time point in duration models (correctly include UK CIS any), prior on ICU, prior on female, prior on other list
# 17 include zeroes, model hosp/icu and community together
# 16 include all zeroes with offset
# 15 #14 + tighter prior on follow-up days using sd instead of 3*sd for hosp/icu models
# 14 hosp/icu combined again
# 13 prior on female from model with only sex-specific data
# 12 separate cvs for hosp, icu; duration is for "Any symptom cluster" only; prior on icu in hosp/icu model
# 11 updated VA extraction
# 10 updated follow-up times to account for index date
# 9 same as 8, but 1000 draws
# 8 same as 7, looser beta_sd*3 on all
# 7 updated Faroe and ITA ISARIC (Faroe were originally mutually exclusive), same settings as 6
# 6 prior on betas from beta gaussian distribution, looser with beta_sd*2 on cog, rsp, and all overlaps
# 5 prior on betas from beta gaussian distribution
# 4 loose prior on betas, shape size ~normal space
# 3 no prior on betas, shape size ~ se in normal space
# 2 remove prior on betas
# 1 use betas from "any" duration models as priors on betas for hosp and comm follow_up_days




################################################################
# READ DATA
################################################################

dataset_orig <- read.csv(paste0(outputfolder, "prepped_data_", datadate, ".csv"))

# Drop column Omicron
dataset_orig <- dataset_orig[!grepl("Omicron", names(dataset_orig))]

dataset <- data.table(dataset_orig)
dataset <- dataset[!(study_id=="PRA" & outcome %in% c("any","any_main","any_new","sub"))]
if (drop_cough == 1) {
  dataset <- dataset[cough!=1]
}

dataset <- dataset[follow_up_days > data_followup_cutoff]

dataset <- dataset[!is.na(standard_error)]

# for COMMUNITY, keep kids (<20) and adults (>=20) for PRA rather than 10-year age groups
dataset[(study_id %in% c("PRA") & (age_start < 10 & age_end > 90) & hospital_or_icu == 0), is_outlier := 1]
dataset[(study_id %in% c("PRA") & (age_start >= 20 & age_end < 90) & hospital_or_icu == 0), is_outlier := 1]
dataset[(study_id %in% c("PRA") & (age_start == 80) & hospital_or_icu == 0), is_outlier := 1]
dataset[(study_id %in% c("PRA") & (age_start == 10 | age_end == 9) & hospital_or_icu == 0), is_outlier := 1]

# drop VA cumulative incidence
dataset <- dataset[measure != "cumulative incidence"]

# drop CSS single data point
dataset <- dataset[!(study_id == "CSS" & multiple_follow_ups == 0)]

table(dataset$study_id, dataset$measure)


if (hosp_kids == 0) {
  # drop age-specific hosp data for PRA and Iran ICC
  dataset[(study_id %in% c("PRA", "Iran ICC") & (age_end < 20 | age_start >= 20) & hospital_or_icu == 1), is_outlier := 1]
} else {
  # drop all-age hosp data for PRA and Iran ICC
  dataset[(study_id %in% c("PRA", "Iran ICC") & (age_start < 10 & age_end > 90) & hospital_or_icu == 1 &
             outcome %in% c("any", "any_new", "any_main", "cog", "fat", "rsp")), is_outlier := 1]
  # drop age-specific hosp data for PRA overlap and severities
  dataset[(study_id %in% c("PRA", "Iran ICC") & (age_end < 20 | age_start >= 10) & hospital_or_icu == 1 &
             outcome %in% c("cog_rsp", "fat_cog", "fat_cog_rsp", "fat_rsp", "mild_cog", "mild_rsp", "mod_cog", "mod_rsp", "sev_rsp")), is_outlier := 1]
}

dataset <- dataset[is_outlier!=1]

# filter out unused residual symptoms
# dataset <- dataset[outcome!="gbs"]
dataset <- dataset[!outcome %in% c("any_new", "sub")]

dataset[study_id=="Zurich CC" & sample_characteristics=="prospective sample", study_id := "Zurich CC prosp"]
dataset[study_id=="Zurich CC" & sample_characteristics=="retrospective sample", study_id := "Zurich CC retro"]

dataset[follow_up_days <= 0, follow_up_days := max(0, follow_up_days)]

# Create flag for studies with both any_main and any
dataset <- setDT(dataset %>%
                   group_by(study_id) %>%
                   mutate(any_type = case_when(
                     all(c('any_main', 'any') %in% outcome) ~ "both",
                     'any_main' %in% outcome & !('any' %in% outcome) ~ "main only",
                     'any' %in% outcome & !('any_main' %in% outcome) ~ "all only",
                     !('any_main' %in% outcome) & !('any' %in% outcome) ~ "none"
                   )) %>%
                   ungroup())


# add offset and/or meas_noise to symptom cluster data
for (i in c('any', 'any_main', 'fat', 'rsp', 'cog', "fat_cog", "fat_rsp", "fat_cog_rsp", "cog_rsp",
            "mild_cog", "mild_rsp", "mod_cog", "mod_rsp", "sev_rsp")) {
  if (meas_noise > 0) {
    print(paste(i, "noise: ", add_meas_noise <- 0.1 * median(dataset$mean[dataset$outcome == i])))
    dataset[outcome == i, standard_error := standard_error + add_meas_noise]
  }
  print(paste(i, "offset: ", add_offset <- 0.01 * median(dataset$mean[dataset$outcome == i])))
  dataset[outcome == i, mean := (mean + add_offset)]
  dataset[outcome == i, offset := add_offset]
}

min(dataset$mean)
max(dataset$mean)

if (min_cv > 0) {
  dataset$cv <- dataset$standard_error / dataset$mean
  dataset[cv < min_cv, standard_error := mean * min_cv]
  dataset$cv <- NULL
}

if (shape == "log") {
  logvals <- cw$utils$linear_to_log(mean = array(dataset$mean), sd = array(dataset$standard_error))
  dataset$model_mean <- log(dataset$mean)
  logse <- logvals[2]
  dataset$model_se <- logse
} else if (shape == "logit") {
  dataset[mean >= 1, mean := 0.999]
  logitvals <- cw$utils$linear_to_logit(mean = array(dataset$mean), sd = array(dataset$standard_error))
  logitmean <- logitvals[1]
  logitse <- logitvals[2]
  dataset$model_mean <- logitmean
  dataset$model_se <- logitse
}

dataset <- dataset[!is.na(model_se)]

# v59+ remove Garcia-Abellan from symptom cluster models b/c is doesn't really meet our case def or that of other published studies
dataset <- dataset[!study_id %in% c("García‑Abellán J et al", "Garcia‑Abellan J et al")]
dataset <- dataset[age_specific==0 | study_id=="LongCOVIDKidsDK"]
dataset[, follow_up_days_orig := follow_up_days]
dataset <- dataset[!is.na(mean)]



df_ped_orig <- copy(dataset[children==1])
df_adult_orig <- copy(dataset[children!=1])

df_ped <- copy(df_ped_orig)
df_adult <- copy(df_adult_orig)

# function to change size of legend
addSmallLegend <- function(myPlot, pointSize = 0.5, titleSize = 0, textSize = 3, spaceLegend = 0.1) {
  myPlot +
    guides(fill=FALSE) + guides(shape = guide_legend(override.aes = list(size = pointSize)),
                                color = guide_legend(override.aes = list(size = pointSize))) +
    theme(legend.title = element_text(size = titleSize),
          legend.text  = element_text(size = textSize),
          legend.key.size = unit(spaceLegend, "lines"))
}



dir.create(paste0(outputfolder, "prepped_data_cluster/plots"))


#  nonzero_min <- min(df$mean[df$mean!=0 & !is.na(df$mean)])
#  df$log_mean <- log(df$mean+0.01*nonzero_min)
#  df$mean2 <- df$mean+0.1*nonzero_min
#  df[standard_error==0 | is.na(standard_error), standard_error := sqrt(((mean+0.01*nonzero_min) * (1-(mean+0.01*nonzero_min))) /  sample_size_envelope)]

table(dataset$study_id, dataset$outcome)
table(dataset$study_id, dataset$sex)


########################################################
# make sure pa-COVID and PRA data with different follow-up times are treated the same (not as a pattern of recovery)
# random effect = study (and follow-up time for these two sources)

df_ped <- df_ped_orig
df_ped[, re := paste0(study_id, "_", variant)]
df_ped[study_id=="pa-COVID" | study_id=="PRA", re := paste(re, follow_up_value)]

df_adult <- df_adult_orig
df_adult[, re := paste0(study_id, "_", variant)]
df_adult <- df_adult[study_id=="pa-COVID" | study_id=="PRA", re := paste(re, follow_up_value)]

unique(df_adult$re)

############################################################################################################################################

df_ped <- df_ped[!is.na(outcome)]
table(df_ped$symptom_cluster[is.na(df_ped$outcome)])
table(df_ped$symptom_cluster, df_ped$study_id)
df_ped$weight <- 1/(30*df_ped$standard_error)
df_ped$weight[df_ped$weight>10] <- 10

df_adult <- df_adult[!is.na(outcome)]
table(df_adult$symptom_cluster[is.na(df_adult$outcome)])
table(df_adult$symptom_cluster, df_adult$study_id)
df_adult$weight <- 1/(30*df_adult$standard_error)
df_adult$weight[df_adult$weight>10] <- 10

unique(dataset[study_id == "Garcia‑Abellan J et al", .(follow_up_value, follow_up_units)])
table(df_ped$study_id, df_ped$female)
unique(df_adult$study_id)

#  for (pop in c('all', 'children', 'adults')) {
# for (pop in c('all')) {
pop <- "all"

if (pop == 'children') {
  df <- df_ped
} else if (pop == 'adults') {
  df <- df_adult
} else if (pop == 'all') {
  df <- rbind(df_ped, df_adult)
}

length(unique(df$study_id))

write.csv(df, paste0(outputfolder, "prepped_data_cluster/prepped_data_", datadate, "_symptoms_for_gather_", pop, ".csv"))






#    out <- 'any_main'
#    n_samples <- 100L
#    max_draw <- 99
for(out in c('any_main', 'fat', 'rsp', 'cog', 'any')) {
    model_dir <- paste0(out, "_v", version, "/")
  dir.create(paste0(outputfolder, "prepped_data_cluster/", model_dir))
  message(paste0("working on ", out))
  
  if (pop == 'children') {
    df <- df_ped
  } else if (pop == 'adults') {
    df <- df_adult
  } else if (pop == 'all') {
    df <- rbind(df_ped, df_adult)
  }
  
  if (out == 'any') {
    df <- df[!study_id %like% "BRA COVAC Manaus2|Afroze et al"]
    df <- df[outcome=="any"]
  } else if (out == 'any_main') {
    df <- df[(outcome=="any_main" & any_type == "both") | (outcome=="any_main" & any_type == "main only") | (outcome == "any" & lit_nonlit == 'lit')]
    df <- df[!study_id %like% "BRA COVAC Manaus2|Afroze et al"]
  } else {
    df <- df[outcome == out]
  }
  offset <- median(df$offset[df$outcome == out])
  
  df_any <- rbind(df_ped[outcome %in% c('any', 'any_main', 'rsp', 'fat', 'cog')], df_adult[outcome %in% c('any', 'any_main', 'rsp', 'fat', 'cog')])
  df_any <- df_any[age_specific==0]
  
  table(df$study_id, df$other_list)
  df$follow_up_days_hosp <- df$follow_up_days * df$hospital_icu
  df$follow_up_days_comm <- df$follow_up_days * (1-df$hospital_icu)
  
  
  df_orig <- copy(df)
  df <- copy(df_orig)
  
  
  
  #############################
  # GET PRIOR ON CALENDAR TIME
  ## estimate relationship by calendar time to incorporate waning risk over time as proxy for variant
  data_caltime <- fread('FILEPATH/long_covid_by_variant.csv')
  if (out %in% c('any', 'any_main')) {
    data_caltime <- data_caltime[outcome == 'any']
  } else {
    # for now, just use 'any' for all the symptom clusters.  ideally this would be by symptom cluster though, just requires more extraction
    data_caltime <- data_caltime[outcome == 'any']
  }
  logitvals <- cw$utils$linear_to_logit(mean = array(data_caltime$mean), sd = array(data_caltime$standard_error))
  logitmean <- logitvals[1]
  logitse <- logitvals[2]
  data_caltime$model_mean <- logitmean
  data_caltime$model_se <- logitse
  
  for (i in c(unique(data_caltime$study_id), "none")) {
    mr_df <- mr$MRData()
    mr_df$load_df(
      data = data_caltime[study_id != i], col_obs = "model_mean", col_obs_se = "model_se",
      col_covs = list("months_since_01012020", "unknown_dominant_variant"), col_study_id = "study_id")
    
    model <- mr$MRBRT(
      data = mr_df,
      cov_models =list(
        mr$LinearCovModel("intercept", use_re = TRUE),
        mr$LinearCovModel("months_since_01012020", use_re = FALSE),
        mr$LinearCovModel("unknown_dominant_variant", use_re = FALSE)
      ),
      inlier_pct = 1)
    
    # fit model
    model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
    
    coeffs <- data.frame(t(model$beta_soln))
    colnames(coeffs) <- model$cov_names
    print(i)
    print(coeffs$months_since_01012020)
    (coeffs)
  }

  samples <- model$sample_soln(sample_size = 1000L)
  (betaval_months_since_01012020 <- mean(samples[[1]][,2]))
  (betaval_months_since_01012020_sd <- sd(samples[[1]][,2]))
  (beta_unknown_dominant_variant <- mean(samples[[1]][,3]))
  (beta_unknown_dominant_variant_sd <- sd(samples[[1]][,3]))
  
  coeffs$months_since_01012020_sd <- betaval_months_since_01012020_sd
  coeffs$unknown_dominant_variant_sd <- beta_unknown_dominant_variant_sd
  (coeffs)
  
  fwrite(coeffs, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs_calendar_time_beta", out, "_", pop, ".csv"))
  beta_months_since_01012020 <- samples[[1]][,2]
  beta_months_since_01012020 <- data.table(cbind(beta_months_since_01012020, draw = c(0:999)))
  fwrite(beta_months_since_01012020, paste0(outputfolder, 'prepped_data_cluster/final_beta_calendar_risk_adjustment.csv'))
  fwrite(beta_months_since_01012020, 'FILEPATH/final_beta_calendar_risk_adjustment.csv')
  
  # make predictions for 4 years
  predict_matrix <- data.table(intercept = model$beta_soln[1], months_since_01012020=c(0:48), unknown_dominant_variant=0)
  predict_data <- mr$MRData()
  predict_data$load_df(
    data = predict_matrix,
    col_covs=c('months_since_01012020', 'unknown_dominant_variant'))
  
  draws <- model$create_draws(
    data = predict_data,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = FALSE,
    sort_by_data_id = TRUE)
  
  # write draws for pipeline
  draws <- data.table(draws)
  draws <- cbind(draws, predict_matrix)
  draws[1:10,1:10]
  draws$intercept <- NULL
  setnames(draws, paste0("V", c(1:1000)), paste0("draw_", c(0:999)))
  draws$unknown_dominant_variant <- NULL
  draws <- melt(data = draws, id.vars = c("months_since_01012020"))
  setnames(draws, "variable", "draw")
  setnames(draws, "value", "proportion")
  draws[, proportion := exp(proportion) / (1 + exp(proportion)) - offset]
  draws[proportion > 1 | is.na(proportion), proportion := 1]
  draws[proportion < 0, proportion := 0]
  
  draws <- reshape(draws, idvar = c("months_since_01012020"), timevar = "draw", direction = "wide")
  setnames(draws, paste0("proportion.draw_", c(0:999)), paste0("draw_", c(0:999)))
  
  
  predict_matrix$pred_raw <- model$predict(predict_data, sort_by_data_id = TRUE)
  predict_matrix[, pred := exp(pred_raw) / (1 + exp(pred_raw))]
  
  draw_cols <- grep('draw_', names(draws), value = TRUE)
  predict_matrix$pred_lo <- apply(draws[, ..draw_cols], 1, function(x) quantile(x, 0.025))
  predict_matrix$pred_hi <- apply(draws[, ..draw_cols], 1, function(x) quantile(x, 0.975))
  predict_matrix$pred_mean <- apply(draws[, ..draw_cols], 1, function(x) mean(x))
  predict_matrix[pred_lo < 0, pred_lo := 0]
  used_data <- copy(data_caltime)
  
  rr_summaries <- copy(predict_matrix)
  
  coeffs
  used_data$weight <- 1/(4*used_data$model_se)
  used_data$weight[used_data$weight>10] <- 10
  used_data$weight[used_data$weight<2] <- 2
  
  table(used_data$study_id)
  
  used_data[, re := model$extract_re(data_caltime$study_id)]
  used_data[, mean_adj := model_mean - re]
  logitvals <- cw$utils$logit_to_linear(mean = array(used_data$mean_adj), sd = array(used_data$model_se))
  adjmean <- logitvals[1]
  used_data$mean_adj <- adjmean

  
  ##############################
  plot <- ggplot(data=rr_summaries, aes(x=months_since_01012020, y=pred), fill = "blue")+
    geom_ribbon(data= rr_summaries, aes(x=months_since_01012020 , ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=0.5) +
    geom_line(data=rr_summaries, aes(x=months_since_01012020 , y=pred), color = "blue", size=1) +
    ylab("Proportion") +
    xlab("Month since 1/1/2020") +
    ggtitle("Long COVID risk reduction over calendar time") +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) +
    scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summaries$months_since_01012020),4), limits = c(0, 48)) +
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data, aes(x=months_since_01012020 , y=mean, color=used_data[, study_id]),
               size=(used_data[, weight]), shape=16, alpha=0.5, show.legend = TRUE) +
    scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
  plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
  
  
  plot
  ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "risk_reduction_over_calendar_time.pdf"), width = 8, height = 5)
  
  plot <- ggplot(data=rr_summaries, aes(x=months_since_01012020, y=pred), fill = "blue")+
    geom_ribbon(data= rr_summaries, aes(x=months_since_01012020 , ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=0.5) +
    geom_line(data=rr_summaries, aes(x=months_since_01012020 , y=pred), color = "blue", size=1) +
    ylab("Proportion") +
    xlab("Month since 1/1/2020") +
    ggtitle("Long COVID risk reduction over calendar time, adjusted for random effects") +
    theme_minimal() +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) +
    scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summaries$months_since_01012020),4), limits = c(0, 48)) +
    theme(axis.line=element_line(colour="black")) +
    geom_point(data=used_data, aes(x=months_since_01012020 , y=mean_adj, color=used_data[, study_id]),
               size=(used_data[, weight]), shape=16, alpha=0.5, show.legend = TRUE) +
    scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
  plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
  
  
  plot
  ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "risk_reduction_over_calendar_time_adjusted.pdf"), width = 8, height = 5)
  
  # DONE with calendar time!
  ############################
  
  
  #############################
  # GET PRIOR ON FEMALE BETA
  df$sex_specific <- 0
  df$sex_specific[df$female==1 | df$male==1] <- 1
  sex_studies <- unique(df$study_id[df$sex_specific == 1])
  both_studies <- unique(df$study_id[df$female == 0 & df$male == 0])
  
  df_sex <- df[study_id %in% sex_studies & study_id %in% both_studies]
  df_sex$re <- paste(df_sex$re, df_sex$follow_up_days)
  table(df_sex$re, df_sex$variant)
  mr_df <- mr$MRData()
  mr_df$load_df(
    data = df_sex, col_obs = "model_mean", col_obs_se = "model_se",
    col_covs = list("female", "male"), col_study_id = "re")
  
  model <- mr$MRBRT(
    data = mr_df,
    cov_models =list(
      mr$LinearCovModel("intercept", use_re = TRUE),
      mr$LinearCovModel("female", use_re = FALSE),
      mr$LinearCovModel("male", use_re = FALSE)
    ),
    inlier_pct = 1)
  
  # fit model
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  samples <- model$sample_soln(sample_size = n_samples)
  (beta_female <- mean(samples[[1]][,2]))
  (beta_female_sd <- sd(samples[[1]][,2]))
  (beta_male <- mean(samples[[1]][,3]))
  (beta_male_sd <- sd(samples[[1]][,3]))
  
  coeffs <- data.frame(t(model$beta_soln))
  colnames(coeffs) <- model$cov_names
  (coeffs)
  
  write.csv(coeffs, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs_female_beta", out, "_", pop, ".csv"))
  ############################
  
  #############################
  # GET PRIOR ON CHILDREN BETA
  table(df$study_id, df$children)
  df_kids <- copy(df)
  (kid_studies <- paste0(unique(df_kids$study_id[df_kids$children == 1]), "  "))
  (adult_studies <- paste0(unique(df_kids$study_id[df_kids$children == 0]), " "))
  
  df_kids <- df_kids[str_sub(paste0(study_id, " "), start = 1L, end = 4L) %in% str_sub(kid_studies, start = 1L, end = 4L) & 
                       str_sub(paste0(study_id, " "), start = 1L, end = 4L) %in% str_sub(adult_studies, start = 1L, end = 4L)]
  
  df_kids$re <- paste(df_kids$re, df_kids$follow_up_days)
  table(df_kids$study_id, df_kids$children)
  mr_df <- mr$MRData()
  mr_df$load_df(
    data = df_kids, col_obs = "model_mean", col_obs_se = "model_se",
    col_covs = list("children"), col_study_id = "re")
  
  model <- mr$MRBRT(
    data = mr_df,
    cov_models =list(
      mr$LinearCovModel("intercept", use_re = TRUE),
      mr$LinearCovModel("children", use_re = FALSE)
    ),
    inlier_pct = 1)
  
  # fit model
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  samples <- model$sample_soln(sample_size = n_samples)
  (beta_children <- mean(samples[[1]][,2]))
  (beta_children_sd <- sd(samples[[1]][,2]))
  
  coeffs <- data.frame(t(model$beta_soln))
  colnames(coeffs) <- model$cov_names
  (coeffs)
  
  write.csv(coeffs, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs_children_beta", out, "_", pop, ".csv"))
  ############################
  
  #############################
  # GET PRIOR ON ICU BETA
  icu_studies <- unique(df$study_id[df$icu == 1])
  hosp_studies <- unique(df$study_id[df$hospital == 1 & df$icu == 0])
  df_icu <- df[study_id %in% icu_studies & study_id %in% hosp_studies]
  df_icu$re <- paste(df_icu$re, df_icu$follow_up_days)
  
  table(df_icu$study_id, df_icu$icu)
  mr_df <- mr$MRData()
  mr_df$load_df(
    data = df_icu, col_obs = "model_mean", col_obs_se = "model_se",
    col_covs = list("icu"), col_study_id = "re")
  
  model <- mr$MRBRT(
    data = mr_df,
    cov_models =list(
      mr$LinearCovModel("intercept", use_re = TRUE),
      mr$LinearCovModel("icu", use_re = FALSE)
    ),
    inlier_pct = 1)
  
  # fit model
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  samples <- model$sample_soln(sample_size = n_samples)
  (beta_icu <- mean(samples[[1]][,2]))
  (beta_icu_sd <- sd(samples[[1]][,2]))
  
  coeffs <- data.frame(t(model$beta_soln))
  colnames(coeffs) <- model$cov_names
  (coeffs)
  
  write.csv(coeffs, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs_icu_beta", out, "_", pop, ".csv"))
  ############################
  
  
  
  df$sex_specific <- 0
  df$sex_specific[df$female==1 | df$male==1] <- 1
  sex_studies <- unique(df$study_id[df$sex_specific == 1])
  df$both_drop <- 0
  # drop studies that have "both" if male and female are available
  df[study_id %in% sex_studies & (female == 0 & male == 0), both_drop := 1]
  table(df$both_drop, useNA = 'always')
  df <- df[both_drop == 0]
  
  
  df[, omicron_cv := 0]
  df[variant == "Omicron", omicron_cv := 1]
  
  
  #####################################
  # set up data
  table(df$study_id, df$sex)
  df[, data_id := c(1:nrow(df))]

  mr_df <- mr$MRData()
  if(out %in% c("any_main", "any")) {
    if (pop == "all" & hosp_kids == 1) {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "children", "other_list", "months_since_01012020", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "children", "other_list", "months_since_01012020", "data_id", "mean", "standard_error")
    } else if (pop == "all" & hosp_kids == 0) {
#      cvs_h <- list("icu", "female", "male", "follow_up_days", "other_list", "months_since_01012020", "data_id", "mean", "standard_error")
#      cvs_c <- list("female", "male", "follow_up_days", "children", "other_list", "months_since_01012020", "data_id", "mean", "standard_error")
      cvs_h <- list("icu", "female", "male", "follow_up_days", "other_list", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "children", "other_list", "data_id", "mean", "standard_error")
    } else {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "other_list", "months_since_01012020", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "other_list", "months_since_01012020", "data_id", "mean", "standard_error")
    }
    if (out == "any_main") {
      title <- "At least 1 symptom cluster"
    } else if (out == "any") {
      title <- "Any symptom (symptom clusters + other symptoms)"
    }
  } else if (out=="cog") {
    #    df <- df[(sex_specific==1 & (study_id=="Iran ICC" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
    #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
    if (pop == "all" & hosp_kids == 1) {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "children", "memory_problems", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "children", "memory_problems", "data_id", "mean", "standard_error")
    } else if (pop == "all" & hosp_kids == 0) {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "memory_problems", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "children", "memory_problems", "data_id", "mean", "standard_error")
    } else {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "memory_problems", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "memory_problems", "data_id", "mean", "standard_error")
    }
    title <- "Cognitive symptoms"
  } else if (out=="fat") {
    #    df <- df[(sex_specific==1 & (study_id=="Iran ICC" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
    #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
    if (pop == "all" & hosp_kids == 1) {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "children", "fatigue", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "children", "fatigue", "data_id", "mean", "standard_error")
    } else if (pop == "all" & hosp_kids == 0) {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "fatigue", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "children", "fatigue", "data_id", "mean", "standard_error")
    } else {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "fatigue", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "fatigue", "data_id", "mean", "standard_error")
    }
    title <- "Post-acute fatigue syndrome"
  } else if (out=="rsp") {
    #    df <- df[(sex_specific==1 & (study_id=="Iran ICC" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
    #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
    if (pop == "all" & hosp_kids == 1) {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "children", "cough", "shortness_of_breath", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "children", "cough", "shortness_of_breath", "data_id", "mean", "standard_error")
    } else if (pop == "all" & hosp_kids == 0) {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "cough", "shortness_of_breath", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "children", "cough", "shortness_of_breath", "data_id", "mean", "standard_error")
      if (drop_cough) {
        cvs_h <- list("icu", "female", "male", "follow_up_days", "shortness_of_breath", "data_id", "mean", "standard_error")
        cvs_c <- list("female", "male", "follow_up_days", "children", "shortness_of_breath", "data_id", "mean", "standard_error")
        df <- df[cough!=1]
      }
    } else {
      cvs_h <- list("icu", "female", "male", "follow_up_days", "cough", "shortness_of_breath", "data_id", "mean", "standard_error")
      cvs_c <- list("female", "male", "follow_up_days", "cough", "shortness_of_breath", "data_id", "mean", "standard_error")
    }
    title <- "Respiratory symptoms"
  } else {
    #    df <- df[(sex_specific==1 & (study_id=="Iran ICC" | study_id=="Italy ISARIC" | study_id=="Sechenov StopCOVID")) | (sex_specific==0 & (study_id=="Helbok et al" |
    #                                                                                                                                   study_id=="CO-FLOW" | study_id=="Zurich CC" | study_id=="Faroe" | study_id=="Veterans Affairs"))]
    cvs_h <- list("icu", "female", "male", "follow_up_days", "data_id", "mean", "standard_error")
    cvs_c <- list("female", "male", "follow_up_days", "data_id", "mean", "standard_error")
    title <- out
  }
  
  
  mr_df_h <- mr$MRData()
  mr_df_c <- mr$MRData()
  mr_df_h$load_df(
    data = df[hospital==1 | icu==1], col_obs = "model_mean", col_obs_se = "model_se",
    col_covs = cvs_h, col_study_id = "re")
  mr_df_c$load_df(
    data = df[hospital==0 & icu==0], col_obs = "model_mean", col_obs_se = "model_se",
    col_covs = cvs_c, col_study_id = "re")
  table(df[hospital==1 | icu==1]$study_id, df[hospital==1 | icu==1]$outcome)
  table(df[hospital==0 & icu==0]$study_id, df[hospital==0 & icu==0]$outcome)
  
  if(out %in% c("any_main")) {
    if (hosp_kids == 1) {
      model_h <- mr$MRBRT(
        data = mr_df_h,
        cov_models =list(
          mr$LinearCovModel("intercept", use_re = TRUE),
          mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          mr$LinearCovModel("follow_up_days", use_re = FALSE),
          mr$LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children, beta_children_sd))),
          mr$LinearCovModel("other_list", use_re = FALSE),
          mr$LinearCovModel("months_since_01012020", use_re = FALSE, prior_beta_gaussian = array(c(betaval_months_since_01012020, betaval_months_since_01012020_sd)))
        ),
        inlier_pct = inlier)
    } else if (hosp_kids == 0) {
      model_h <- mr$MRBRT(
        data = mr_df_h,
        cov_models =list(
          mr$LinearCovModel("intercept", use_re = TRUE),
          mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          mr$LinearCovModel("follow_up_days", use_re = FALSE),
          mr$LinearCovModel("other_list", use_re = FALSE)
        ),
        inlier_pct = inlier)
    }
    model_c <- mr$MRBRT(
      data = mr_df_c,
      cov_models =list(
        mr$LinearCovModel("intercept", use_re = TRUE),
        mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
        mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
        mr$LinearCovModel("follow_up_days", use_re = FALSE),
        mr$LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children, beta_children_sd))),
        mr$LinearCovModel("other_list", use_re = FALSE)
      ),
      inlier_pct = inlier)
  } else if(out %in% c("any")) {
    if (hosp_kids == 1) {
      model_h <- mr$MRBRT(
        data = mr_df_h,
        cov_models =list(
          mr$LinearCovModel("intercept", use_re = TRUE),
          mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          mr$LinearCovModel("follow_up_days", use_re = FALSE),
          mr$LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children, beta_children_sd))),
          mr$LinearCovModel("months_since_01012020", use_re = FALSE, prior_beta_gaussian = array(c(betaval_months_since_01012020, abs(0.0001 * betaval_months_since_01012020))))
        ),
        inlier_pct = inlier)
    } else if (hosp_kids == 0) {
      model_h <- mr$MRBRT(
        data = mr_df_h,
        cov_models =list(
          mr$LinearCovModel("intercept", use_re = TRUE),
          mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          mr$LinearCovModel("follow_up_days", use_re = FALSE)
        ),
        inlier_pct = inlier)
    }
    model_c <- mr$MRBRT(
      data = mr_df_c,
      cov_models =list(
        mr$LinearCovModel("intercept", use_re = TRUE),
        mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
        mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
        mr$LinearCovModel("follow_up_days", use_re = FALSE),
        mr$LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children, beta_children_sd)))
      ),
      inlier_pct = inlier)
  } else if (out=="cog") {
    if (hosp_kids == 1) {
      model_h <- mr$MRBRT(
        data = mr_df_h,
        cov_models =list(
          mr$LinearCovModel("intercept", use_re = TRUE),
          mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
          mr$LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children, beta_children_sd))),
          mr$LinearCovModel("memory_problems", use_re = FALSE)
        ),
        inlier_pct = inlier)
    } else if (hosp_kids == 0) {
      model_h <- mr$MRBRT(
        data = mr_df_h,
        cov_models =list(
          mr$LinearCovModel("intercept", use_re = TRUE),
          mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
          mr$LinearCovModel("memory_problems", use_re = FALSE)
        ),
        inlier_pct = inlier)
    }
    model_c <- mr$MRBRT(
      data = mr_df_c,
      cov_models =list(
        mr$LinearCovModel("intercept", use_re = TRUE),
        mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
        mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
        mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
        mr$LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children, beta_children_sd))),
        mr$LinearCovModel("memory_problems", use_re = FALSE)
      ),
      inlier_pct = inlier)
  } else if (out=="fat") {
    if (hosp_kids == 1) {
      model_h <- mr$MRBRT(
        data = mr_df_h,
        cov_models =list(
          mr$LinearCovModel("intercept", use_re = TRUE),
          mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
          mr$LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children, beta_children_sd))),
          mr$LinearCovModel("fatigue", use_re = FALSE)
        ),
        inlier_pct = inlier)
    } else if (hosp_kids == 0) {
      model_h <- mr$MRBRT(
        data = mr_df_h,
        cov_models =list(
          mr$LinearCovModel("intercept", use_re = TRUE),
          mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
          mr$LinearCovModel("fatigue", use_re = FALSE)
        ),
        inlier_pct = inlier)
    }
    
    model_c <- mr$MRBRT(
      data = mr_df_c,
      cov_models =list(
        mr$LinearCovModel("intercept", use_re = TRUE),
        mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
        mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
        mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
        mr$LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children, beta_children_sd))),
        mr$LinearCovModel("fatigue", use_re = FALSE)
      ),
      inlier_pct = inlier)
  } else if (out=="rsp") {
    if (hosp_kids == 1) {
      model_h <- mr$MRBRT(
        data = mr_df_h,
        cov_models =list(
          mr$LinearCovModel("intercept", use_re = TRUE),
          mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
          mr$LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children, beta_children_sd))),
          mr$LinearCovModel("shortness_of_breath", use_re = FALSE)
        ),
        inlier_pct = inlier)
    } else if (hosp_kids==0) {
      model_h <- mr$MRBRT(
        data = mr_df_h,
        cov_models =list(
          mr$LinearCovModel("intercept", use_re = TRUE),
          mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
          mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
          mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
          mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier))),
          mr$LinearCovModel("shortness_of_breath", use_re = FALSE)
        ),
        inlier_pct = inlier)
    } 
    model_c <- mr$MRBRT(
      data = mr_df_c,
      cov_models =list(
        mr$LinearCovModel("intercept", use_re = TRUE),
        mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
        mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
        mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier))),
        mr$LinearCovModel("children", use_re = FALSE, prior_beta_gaussian = array(c(beta_children, beta_children_sd))),
        mr$LinearCovModel("shortness_of_breath", use_re = FALSE)
      ),
      inlier_pct = inlier)
  } else if (out=="cog_rsp" | out=="fat_cog" | out=="fat_rsp" | out=="fat_cog_rsp") {
    model_h <- mr$MRBRT(
      data = mr_df_h,
      cov_models =list(
        mr$LinearCovModel("intercept", use_re = TRUE),
        mr$LinearCovModel("icu", use_re = FALSE, prior_beta_gaussian = array(c(beta_icu, beta_icu_sd))),
        mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
        mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
        mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_hosp, beta_hosp_sd*follow_up_sd_multiplier)))
      ),
      inlier_pct = inlier)
    
    model_c <- mr$MRBRT(
      data = mr_df_c,
      cov_models =list(
        mr$LinearCovModel("intercept", use_re = TRUE),
        mr$LinearCovModel("female", use_re = FALSE, prior_beta_gaussian = array(c(beta_female, beta_female_sd))),
        mr$LinearCovModel("male", use_re = FALSE, prior_beta_gaussian = array(c(beta_male, beta_male_sd))),
        mr$LinearCovModel("follow_up_days", use_re = FALSE, prior_beta_gaussian = array(c(beta_comm, beta_comm_sd*follow_up_sd_multiplier)))
      ),
      inlier_pct = inlier)
  }
  
  
  # fit model
  model_h$fit_model(inner_print_level = 5L, inner_max_iter = 1500L)
  model_c$fit_model(inner_print_level = 5L, inner_max_iter = 1500L)
  
  (coeffs_h <- rbind(model_h$cov_names, model_h$beta_soln))
  coeffs_h <- data.table(coeffs_h)
  setnames(coeffs_h, c(names(coeffs_h)), c(model_h$cov_names))
  coeffs_h <- coeffs_h[2,]
  if (length(coeffs_h$other_list) == 0) {
    coeffs_h$other_list <- 0
  }
  if (length(coeffs_h$administrative) == 0) {
    coeffs_h$administrative <- 0
  }
  coeffs_h$hospital_icu <- 1
  coeffs_h$prior_icu <- beta_icu
  coeffs_h$prior_icu_sd <- beta_icu_sd
  coeffs_h$prior_female <- beta_female
  coeffs_h$prior_female_sd <- beta_female_sd
  coeffs_h$prior_male <- beta_male
  coeffs_h$prior_male_sd <- beta_male_sd
  coeffs_h$gamma <- model_h$gamma_soln
  if (hosp_kids == 1) {
    coeffs_h$prior_children <- beta_children
    coeffs_h$prior_children_sd <- beta_children_sd
  }
  if (out == "any" | out == "any_main") {
    coeffs_h$prior_follow_up <- NA
    coeffs_h$prior_follow_up_sd <- NA
  } else {
    coeffs_h$prior_follow_up <- beta_hosp
    coeffs_h$prior_follow_up_sd <- beta_hosp_sd*follow_up_sd_multiplier
  }
  
  (coeffs_c <- rbind(model_c$cov_names, model_c$beta_soln))
  coeffs_c <- data.table(coeffs_c)
  setnames(coeffs_c, c(names(coeffs_c)), c(model_c$cov_names))
  coeffs_c <- coeffs_c[2,]
  if (length(coeffs_c$other_list) == 0) {
    coeffs_c$other_list <- 0
  }
  if (length(coeffs_c$administrative) == 0) {
    coeffs_c$administrative <- 0
  }
  coeffs_c$hospital_icu <- 0
  coeffs_c$prior_female <- beta_female
  coeffs_c$prior_female_sd <- beta_female_sd
  coeffs_c$prior_male <- beta_male
  coeffs_c$prior_male_sd <- beta_male_sd
  coeffs_c$prior_children <- beta_children
  coeffs_c$prior_children_sd <- beta_children_sd
  coeffs_c$gamma <- model_c$gamma_soln
  
  if (out == "any" | out == "any_main") {
    coeffs_c$prior_follow_up <- NA
    coeffs_c$prior_follow_up_sd <- NA
  } else {
    coeffs_c$prior_follow_up <- beta_comm
    coeffs_c$prior_follow_up_sd <- beta_comm_sd*follow_up_sd_multiplier
  }
  
  if (out %in% c("any", "any_main")) {
    beta_hosp <- as.numeric(coeffs_h$follow_up_days)
    beta_comm_all <- as.numeric(coeffs_c$follow_up_days)
    beta_comm <- as.numeric(coeffs_c$follow_up_days)
  }
  
  write.csv(coeffs_h, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs_hospital_icu_", pop, ".csv"))
  write.csv(coeffs_c, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs_community_", pop, ".csv"))
  
  
  # save model object
  py_save_object(object = model_h,
                 filename = paste0(outputfolder, "prepped_data_cluster/", model_dir, "mod1_h_", pop, ".pkl"), pickle = "dill")
  py_save_object(object = model_c,
                 filename = paste0(outputfolder, "prepped_data_cluster/", model_dir, "mod1_c_", pop, ".pkl"), pickle = "dill")
  
  # make predictions for full year
  predict_matrix_midmod_M <- data.table(intercept = model_c$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=0, female=0, male=1, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
  predict_matrix_midmod_F <- data.table(intercept = model_c$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=0, female=1, male=0, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
  predict_matrix_hosp_M <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=1, icu=0, female=0, male=1, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
  predict_matrix_hosp_F <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=1, icu=0, female=1, male=0, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
  predict_matrix_icu_M <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=1, female=0, male=1, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
  predict_matrix_icu_F <- data.table(intercept = model_h$beta_soln[1], follow_up_days=c(0:1460), hospital=0, icu=1, female=1, male=0, other_list=0, memory_problems=0, fatigue=0, administrative=0, cough=0, shortness_of_breath=0)
  predict_matrix_c <- rbind(predict_matrix_midmod_M,predict_matrix_midmod_F)
  predict_matrix_h <- rbind(predict_matrix_hosp_M,predict_matrix_hosp_F,predict_matrix_icu_M,predict_matrix_icu_F)
  predict_matrix_c$children <- 0
  predict_matrix_c_ch <- copy(predict_matrix_c)
  predict_matrix_c_ch$children <- 1
  if (kid_estimate_sex_specific_c == 0) {
    predict_matrix_c_ch$male <- 0
    predict_matrix_c_ch$female <- 0
  }
  predict_matrix_c <- rbind(predict_matrix_c, predict_matrix_c_ch)
  predict_matrix_c <- unique(predict_matrix_c)
  predict_matrix_c[, data_id := 0]
  predict_matrix_c[, mean := 0]
  predict_matrix_c[, standard_error := 0]
  
  predict_matrix_h$children <- 0
  predict_matrix_h_ch <- copy(predict_matrix_h)
  predict_matrix_h_ch$children <- 1
  if (kid_estimate_sex_specific_h == 0) {
    predict_matrix_h_ch$male <- 0
    predict_matrix_h_ch$female <- 0
  }
  predict_matrix_h <- rbind(predict_matrix_h, predict_matrix_h_ch)
  predict_matrix_h <- unique(predict_matrix_h)
  predict_matrix_h[, data_id := 0]
  predict_matrix_h[, mean := 0]
  predict_matrix_h[, standard_error := 0]
  
  predict_data_h <- mr$MRData()
  predict_data_h$load_df(
    data = predict_matrix_h,
    col_covs=cvs_h)
  predict_data_c <- mr$MRData()
  predict_data_c$load_df(
    data = predict_matrix_c,
    col_covs=cvs_c)
  
  
  ## CREATE DRAWS
  
  samples_h <- model_h$sample_soln(sample_size = n_samples)
  samples_c <- model_c$sample_soln(sample_size = n_samples)
  
  if (out %in% c("any", "any_main")) {
    beta_hosp_sd <- sd(samples_h[[1]][,5])
    beta_comm_sd_all <- sd(samples_c[[1]][,4])
    beta_comm_sd <- sd(samples_c[[1]][,4])
  }
  for (i in 1:length(model_h$cov_names)) {
    coeffs_h[, eval(paste0(model_h$cov_names[i], "_sd")) := sd(samples_h[[1]][,i])]
  }
  for (i in 1:length(model_c$cov_names)) {
    coeffs_c[, eval(paste0(model_c$cov_names[i], "_sd")) := sd(samples_c[[1]][,i])]
  }
  write.csv(coeffs_h, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs_hospital_icu_", pop, ".csv"))
  write.csv(coeffs_c, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs_community_", pop, ".csv"))
  
  if (include_gamma == 0) {
    draws_h <- model_h$create_draws(
      data = predict_data_h,
      beta_samples = samples_h[[1]],
      gamma_samples = matrix(rep(0, n_samples), ncol = 1),
      random_study = FALSE,
      sort_by_data_id = TRUE)
  } else if (include_gamma == 1) {
    draws_h <- model_h$create_draws(
      data = predict_data_h,
      beta_samples = samples_h[[1]],
      gamma_samples = samples_h[[2]],
      random_study = TRUE,
      sort_by_data_id = TRUE)
  }
  
  # write draws for pipeline
  draws_h <- data.table(draws_h)
  draws_h <- cbind(draws_h, predict_matrix_h)
  draws_h[1:10,1:10]
  draws_h$intercept <- NULL
  setnames(draws_h, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
  draws_h$other_list <- NULL
  draws_h$shortness_of_breath <- NULL
  draws_h$memory_problems <- NULL
  draws_h$fatigue <- NULL
  draws_h$administrative <- NULL
  draws_h$cough <- NULL
  draws_h$children <- 0
  draws_h <- melt(data = draws_h, id.vars = c("hospital", "icu", "follow_up_days", "female", "male", "children"))
  setnames(draws_h, "variable", "draw")
  setnames(draws_h, "value", "proportion")
  if (shape == "log") {
    draws_h[, proportion := exp(proportion) - offset]
  } else if (shape == "logit") {
    draws_h[, proportion := exp(proportion) / (1 + exp(proportion)) - offset]
  }
  draws_h[proportion > 1 | is.na(proportion), proportion := 1]
  draws_h[proportion < 0, proportion := 0]
  
  if (hosp_kids == 0) {
    draws_h <- unique(draws_h[,!c('children')])
    draws_h <- reshape(draws_h, idvar = c("hospital", "icu", "follow_up_days", "female", "male"), timevar = "draw", direction = "wide")
  } else {
    draws_h <- reshape(draws_h, idvar = c("hospital", "icu", "follow_up_days", "female", "male", "children"), timevar = "draw", direction = "wide")
  }
  setnames(draws_h, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
  draws_save <- draws_h[hospital==1 | icu==1]
  
  #    (lower <- quantile(draws_h$beta0, 0.05, na.rm = TRUE))
  #    (upper <- quantile(dur$beta0, 0.95, na.rm = TRUE))
  #    dur[beta0 > upper, beta0 := upper]
  #    dur[beta0 < lower, beta0 := lower]
  
  
  write.csv(draws_save, file =paste0(outputfolder, "prepped_data_cluster/", model_dir, "predictions_draws_hospital_icu_", pop, ".csv"))
  
  
  if (out == 'any_main') {
    dur <- data.table(draws_h)
    dur <- melt(data = dur, id.vars = c("hospital", "icu", "follow_up_days", "female", "male"))
    head(dur)
    # TODO: make this a proper merge, but does not need to be female/male/age group specific, so 
    #  dur should be unique by hospital/icu/follow_up_days
    dur <- cbind(dur, samples_h[[1]])
    setnames(dur, "value", "prop_start")
    setnames(dur, "V1", "beta0")
    setnames(dur, "V2", "beta_icu")
    setnames(dur, "V3", "beta_f")
    setnames(dur, "V4", "beta_m")
    setnames(dur, "V5", "beta1")
    setnames(dur, "variable", "draw")
    dur$V6 <- NULL
    
    write.csv(dur, file = paste0(outputfolder, "prepped_data_cluster/duration_parameters_hospicu_", pop, "_v", version, ".csv"))
  }
  
  
  if (include_gamma == 0) {
    draws_c <- model_c$create_draws(
      data = predict_data_c,
      beta_samples = samples_c[[1]],
      gamma_samples = matrix(rep(0, n_samples), ncol = 1),
      random_study = FALSE,
      sort_by_data_id = TRUE)
  } else if (include_gamma == 1) {
    draws_c <- model_c$create_draws(
      data = predict_data_c,
      beta_samples = samples_c[[1]],
      gamma_samples = samples_c[[2]],
      random_study = TRUE,
      sort_by_data_id = TRUE)
  }
  
  # write draws for pipeline
  draws_c <- data.table(draws_c)
  draws_c <- cbind(draws_c, predict_matrix_c)
  draws_c$intercept <- NULL
  setnames(draws_c, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
  draws_c$other_list <- NULL
  draws_c$shortness_of_breath <- NULL
  draws_c$memory_problems <- NULL
  draws_c$fatigue <- NULL
  draws_c$administrative <- NULL
  draws_c$cough <- NULL
  draws_c <- melt(data = draws_c, id.vars = c("hospital", "icu", "follow_up_days", "female", "male", "children"))
  setnames(draws_c, "variable", "draw")
  setnames(draws_c, "value", "proportion")
  if (shape == "log") {
    draws_c[, proportion := exp(proportion) - offset]
  } else if (shape == "logit") {
    draws_c[, proportion := exp(proportion) / (1 + exp(proportion)) - offset]
  }
  draws_c[proportion>1 | is.na(proportion), proportion := 1]
  draws_c[proportion<0, proportion := 0]
  
  draws_c <- reshape(draws_c, idvar = c("hospital", "icu", "follow_up_days", "female", "male", "children"), timevar = "draw", direction = "wide")
  setnames(draws_c, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
  draws_save <- draws_c[hospital==0 & icu==0]
  
  write.csv(draws_save, file =paste0(outputfolder, "prepped_data_cluster/", model_dir, "predictions_draws_community_", pop, ".csv"))
  
  # test out using parameters from the envelope model for community duration.  in the end, it doesn't really
  #  make a difference, because the slope prior is pretty tight, which dictates the resulting duration.
  if (out == 'any_main') {
    dur <- data.table(draws_c)
    dur <- melt(data = dur, id.vars = c("hospital", "icu", "follow_up_days", "female", "male", "children"))
    head(dur)
    dur <- cbind(dur, samples_c[[1]])
    setnames(dur, "value", "prop_start")
    setnames(dur, "V1", "beta0")
    setnames(dur, "V2", "beta_f")
    setnames(dur, "V3", "beta_m")
    setnames(dur, "V4", "beta1")
    setnames(dur, "V5", "beta_children")
    setnames(dur, "variable", "draw")
    dur$V6 <- NULL
    write.csv(dur, file = paste0(outputfolder, "prepped_data_cluster/duration_parameters_midmod_", pop, "_v", version, ".csv"))
  }
  
  
  predict_matrix_h$pred_raw <- model_h$predict(predict_data_h, sort_by_data_id = TRUE)
  if (hosp_kids == 0) {
    predict_matrix_h <- data.table(unique(predict_matrix_h[, !c("children")]))
  }
  if (shape == "log") {
    predict_matrix_h[, pred := exp(pred_raw) - offset]
  } else if (shape == "logit") {
    predict_matrix_h[, pred := exp(pred_raw) / (1 + exp(pred_raw)) - offset]
  }
  draw_cols <- grep('draw_', names(draws_h), value = TRUE)
  predict_matrix_h$pred_lo <- apply(draws_h[, ..draw_cols], 1, function(x) quantile(x, 0.025))
  predict_matrix_h$pred_hi <- apply(draws_h[, ..draw_cols], 1, function(x) quantile(x, 0.975))
  predict_matrix_h[pred_lo < 0, pred_lo := 0]
  used_data_h <- cbind(model_h$data$to_df(), data.frame(w = model_h$w_soln))
  used_data_h <- as.data.table(used_data_h)
  used_data_h$follow_up_months <- used_data_h$follow_up_days * 0.032855
  
  rr_summaries_h <- copy(predict_matrix_h)
  
  rr_summaries_h
  rr_summaries_h$pred_hi[rr_summaries_h$pred_hi>1] <- 1
  rr_summaries_h$gamma <- mean(samples_h[[2]])
  
  
  write.csv(rr_summaries_h, file =paste0(outputfolder, "prepped_data_cluster/", model_dir, "predictions_summary_hospital_icu_", pop, ".csv"))
  
  
  
  
  predict_matrix_c$pred_raw <- model_c$predict(predict_data_c, sort_by_data_id = TRUE)
  if (shape == "log") {
    predict_matrix_c[, pred := exp(pred_raw) - offset]
  } else if (shape == "logit") {
    predict_matrix_c[, pred := exp(pred_raw) / (1 + exp(pred_raw)) - offset]
  }
  draw_cols <- grep('draw_', names(draws_c), value = TRUE)
  predict_matrix_c$pred_lo <- apply(draws_c[, ..draw_cols], 1, function(x) quantile(x, 0.025))
  predict_matrix_c$pred_hi <- apply(draws_c[, ..draw_cols], 1, function(x) quantile(x, 0.975))
  predict_matrix_c[pred_lo < 0, pred_lo := 0]
  used_data_c <- cbind(model_c$data$to_df(), data.frame(w = model_c$w_soln))
  used_data_c <- as.data.table(used_data_c)
  used_data_c$follow_up_months <- used_data_c$follow_up_days * 0.032855
  
  rr_summaries_c <- copy(predict_matrix_c)
  
  rr_summaries_c
  rr_summaries_c$pred_hi[rr_summaries_c$pred_hi>1] <- 1
  rr_summaries_c$gamma <- mean(samples_c[[2]])
  
  
  write.csv(rr_summaries_c, file =paste0(outputfolder, "prepped_data_cluster/", model_dir, "predictions_summary_community_", pop, ".csv"))
  
  
  
  
  
  used_data_h$pop[used_data_h$icu==0] <- "hospital"
  used_data_h$pop[used_data_h$icu==1] <- "ICU"
  used_data_c$pop <- "community"
  
#  df_se <- df[,c("study_id", "re", "hospital", "icu", "female", "male",
#                 "follow_up_days", "mean", "standard_error", "cough",
#                 "shortness_of_breath", "fatigue", "memory_problems", "children")]
#  setnames(df_se, "standard_error", "se")
  setnames(used_data_h, "study_id", "re")
  setnames(used_data_c, "study_id", "re")
  used_data_h$hospital <- 0
  used_data_h <- used_data_h[icu==0, hospital := 1]
  used_data_c <- used_data_c[, hospital := 0]
  used_data_c <- used_data_c[, icu := 0]
#  if (hosp_kids == 1) {
#    hmerge_vars <- c("re", "icu", "hospital", "female", "male", "children", "follow_up_days")
#    cmerge_vars <- c("re", "icu", "hospital", "female", "male", "children", "follow_up_days")
#  } else {
#    hmerge_vars <- c("re", "icu", "hospital", "female", "male", "follow_up_days")
#    cmerge_vars <- c("re", "icu", "hospital", "female", "male", "children", "follow_up_days")
#  }
  if (out=="rsp") {
#    if (drop_cough == 1) {
#      used_data_h <- merge(used_data_h, df_se, by=c(hmerge_vars, "shortness_of_breath"))
#      used_data_c <- merge(used_data_c, df_se, by=c(cmerge_vars, "shortness_of_breath"))
#    } else {
#      used_data_h <- merge(used_data_h, df_se, by=c(hmerge_vars, "cough", "shortness_of_breath"))
#      used_data_c <- merge(used_data_c, df_se, by=c(cmerge_vars, "cough", "shortness_of_breath"))
#    }
    used_data_h$other_list <- 0
    used_data_c$other_list <- 0
  } else if (out=="cog") {
#    used_data_h <- merge(used_data_h, df_se, by=c(hmerge_vars, "memory_problems"))
#    used_data_c <- merge(used_data_c, df_se, by=c(cmerge_vars, "memory_problems"))
    used_data_h$other_list <- 0
    used_data_c$other_list <- 0
  } else if (out=="fat") {
#    used_data_h <- merge(used_data_h, df_se, by=c(hmerge_vars, "fatigue"))
#    used_data_c <- merge(used_data_c, df_se, by=c(cmerge_vars, "fatigue"))
    used_data_h$other_list <- 0
    used_data_c$other_list <- 0
  } else if (out %in% c("any_main", "any")) {
#    used_data_h <- merge(used_data_h, df_se[hospital==1 | icu==1], by=c(hmerge_vars))
    fwrite(used_data_h, "FILEPATH/used_data_h.csv")
#    fwrite(df_se[hospital==1 | icu==1], "FILEPATH/df_se_h.csv")
#    used_data_c <- merge(used_data_c, df_se, by=c(cmerge_vars))
    fwrite(used_data_c, "FILEPATH/used_data_c.csv")
#    fwrite(df_se[hospital==0 & icu==0], "FILEPATH/df_se_c.csv")
  }
  
  
  df_studies <- unique(df[, c('study_id', 're')])
  df_studies[study_id == 'Zurich ZSAC prosp - Omicron', study_id := 'Zurich ZSAC prosp']
  df_studies[study_id == 'Zurich ZSAC retro - Omicron', study_id := 'Zurich ZSAC retro']
  used_data_h <- merge(used_data_h, df_studies, by = c('re'))
  used_data_c <- merge(used_data_c, df_studies, by = c('re'))
  used_data_h <- unique(used_data_h)
  used_data_c <- unique(used_data_c)
  used_data_h[, follow_up_months := follow_up_days / (365/12)]
  used_data_c[, follow_up_months := follow_up_days / (365/12)]
  
  for (poppredict in c("children", "adults")) {
    ### ADJUST DATA
    if (poppredict == "children") {
      kids <- 1
    } else {
      kids <- 0
    }
    coeffs_h
    coeffs_c
    used_data_h$administrative <- 0
    used_data_c$administrative <- 0
    used_data_i_f <- used_data_h[female==1 | (female==0 & male==0)]
    used_data_i_m <- used_data_h[male==1 | (female==0 & male==0)]
    used_data_h_f <- used_data_h[female==1 | (female==0 & male==0)]
    used_data_h_m <- used_data_h[male==1 | (female==0 & male==0)]
    used_data_c_f <- used_data_c[female==1 | (female==0 & male==0)]
    used_data_c_m <- used_data_c[male==1 | (female==0 & male==0)]
    
    used_data_i_f$obs_adj <- used_data_i_f$obs
    used_data_i_m$obs_adj <- used_data_i_m$obs
    used_data_h_f$obs_adj <- used_data_h_f$obs
    used_data_h_m$obs_adj <- used_data_h_m$obs
    used_data_c_f$obs_adj <- used_data_c_f$obs
    used_data_c_m$obs_adj <- used_data_c_m$obs
    
    if (kid_estimate_sex_specific_h == 0 & hosp_kids == 1) {
      # adjust all kid data to Both sexes because we are not doing sex-specific child estimates anymore
      used_data_i_f[, obs_adj := obs + (1 - icu) * as.numeric(coeffs_h$icu) - female * as.numeric(coeffs_h$female) - male * as.numeric(coeffs_h$male) + (kids - children) * as.numeric(coeffs_h$children) - other_list * as.numeric(coeffs_h$other_list)]
      used_data_i_m[, obs_adj := obs + (1 - icu) * as.numeric(coeffs_h$icu) - female * as.numeric(coeffs_h$female) - male * as.numeric(coeffs_h$male) + (kids - children) * as.numeric(coeffs_h$children) - other_list * as.numeric(coeffs_h$other_list)]
      used_data_h_f[, obs_adj := obs - icu * as.numeric(coeffs_h$icu) - female * as.numeric(coeffs_h$female) - male * as.numeric(coeffs_h$male) + (kids - children) * as.numeric(coeffs_h$children) - other_list * as.numeric(coeffs_h$other_list)]
      used_data_h_m[, obs_adj := obs - icu * as.numeric(coeffs_h$icu) - female * as.numeric(coeffs_h$female) - male * as.numeric(coeffs_h$male) + (kids - children) * as.numeric(coeffs_h$children) - other_list * as.numeric(coeffs_h$other_list)]
      used_data_c_f[, obs_adj := obs - other_list * as.numeric(coeffs_c$other_list) - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children)]
      used_data_c_m[, obs_adj := obs - other_list * as.numeric(coeffs_c$other_list) - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children)]
    } else if (kid_estimate_sex_specific_h == 1 & hosp_kids == 1) {
      used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_f$female) * as.numeric(coeffs_h$female) + (kids - used_data_i_f$children) * as.numeric(coeffs_h$children) - used_data_i_f$other_list * as.numeric(coeffs_h$other_list)
      used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_m$male) * as.numeric(coeffs_h$male) + (kids - used_data_i_m$children) * as.numeric(coeffs_h$children) - used_data_i_m$other_list * as.numeric(coeffs_h$other_list)
      used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_f$female) * as.numeric(coeffs_h$female) + (kids - used_data_h_f$children) * as.numeric(coeffs_h$children) - used_data_h_f$other_list * as.numeric(coeffs_h$other_list)
      used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_m$male) * as.numeric(coeffs_h$male) + (kids - used_data_h_m$children) * as.numeric(coeffs_h$children) - used_data_h_m$other_list * as.numeric(coeffs_h$other_list)
      used_data_c_f$obs_adj <- used_data_c_f$obs - used_data_c_f$other_list * as.numeric(coeffs_c$other_list) + (1-used_data_c_f$female) * as.numeric(coeffs_c$female) + (kids - used_data_c_f$children) * as.numeric(coeffs_c$children)
      used_data_c_m$obs_adj <- used_data_c_m$obs - used_data_c_m$other_list * as.numeric(coeffs_c$other_list) + (1-used_data_c_m$male) * as.numeric(coeffs_c$male) + (kids - used_data_c_m$children) * as.numeric(coeffs_c$children)
    } else if (kid_estimate_sex_specific_h == 0 & hosp_kids == 0) {
      if (out == 'fat') {
        # adjust all kid data to Both sexes because we are not doing sex-specific child estimates anymore
        used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_f$female) * as.numeric(coeffs_h$female) - used_data_i_f$fatigue * as.numeric(coeffs_h$fatigue) - used_data_i_f$administrative * as.numeric(coeffs_h$administrative)
        used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_m$male) * as.numeric(coeffs_h$male) - used_data_i_m$fatigue * as.numeric(coeffs_h$fatigue) - used_data_i_m$administrative * as.numeric(coeffs_h$administrative)
        used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_f$female) * as.numeric(coeffs_h$female) - used_data_h_f$fatigue * as.numeric(coeffs_h$fatigue) - used_data_h_f$administrative * as.numeric(coeffs_h$administrative)
        used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_m$male) * as.numeric(coeffs_h$male) - used_data_h_m$fatigue * as.numeric(coeffs_h$fatigue) - used_data_h_m$administrative * as.numeric(coeffs_h$administrative)
        if (kids == 1) {
          used_data_c_f[, obs_adj := obs - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - fatigue * as.numeric(coeffs_c$fatigue) - administrative * as.numeric(coeffs_c$administrative)]
          used_data_c_m[, obs_adj := obs - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - fatigue * as.numeric(coeffs_c$fatigue) - administrative * as.numeric(coeffs_c$administrative)]
        } else {
          used_data_c_f[, obs_adj := obs + (1 - female) * as.numeric(coeffs_c$female) + (kids - children) * as.numeric(coeffs_c$children) - fatigue * as.numeric(coeffs_c$fatigue) - administrative * as.numeric(coeffs_c$administrative)]
          used_data_c_m[, obs_adj := obs + (1 - male) * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - fatigue * as.numeric(coeffs_c$fatigue) - administrative * as.numeric(coeffs_c$administrative)]
        }
      } else if (out == 'rsp') {
        if (drop_cough == 1) {
          used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_f$female) * as.numeric(coeffs_h$female) - used_data_i_f$shortness_of_breath * as.numeric(coeffs_h$shortness_of_breath) - used_data_i_f$administrative * as.numeric(coeffs_h$administrative)
          used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_m$male) * as.numeric(coeffs_h$male) - used_data_i_m$shortness_of_breath * as.numeric(coeffs_h$shortness_of_breath) - used_data_i_m$administrative * as.numeric(coeffs_h$administrative)
          used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_f$female) * as.numeric(coeffs_h$female) - used_data_h_f$shortness_of_breath * as.numeric(coeffs_h$shortness_of_breath) - used_data_h_f$administrative * as.numeric(coeffs_h$administrative)
          used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_m$male) * as.numeric(coeffs_h$male) - used_data_h_m$shortness_of_breath * as.numeric(coeffs_h$shortness_of_breath) - used_data_h_m$administrative * as.numeric(coeffs_h$administrative)
          if (kids == 1) {
            used_data_c_f[, obs_adj := obs - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - shortness_of_breath * as.numeric(coeffs_c$shortness_of_breath) - administrative * as.numeric(coeffs_c$administrative)]
            used_data_c_m[, obs_adj := obs - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - shortness_of_breath * as.numeric(coeffs_c$shortness_of_breath) - administrative * as.numeric(coeffs_c$administrative)]
          } else {
            used_data_c_f[, obs_adj := obs + (1 - female) * as.numeric(coeffs_c$female) + (kids - children) * as.numeric(coeffs_c$children) - shortness_of_breath * as.numeric(coeffs_c$shortness_of_breath) - administrative * as.numeric(coeffs_c$administrative)]
            used_data_c_m[, obs_adj := obs + (1 - male) * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - shortness_of_breath * as.numeric(coeffs_c$shortness_of_breath) - administrative * as.numeric(coeffs_c$administrative)]
          }
        } else {
          used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_f$female) * as.numeric(coeffs_h$female) - used_data_i_f$cough * as.numeric(coeffs_h$cough) - used_data_i_f$shortness_of_breath * as.numeric(coeffs_h$shortness_of_breath) - used_data_i_f$administrative * as.numeric(coeffs_h[1,8])
          used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_m$male) * as.numeric(coeffs_h$male) - used_data_i_m$cough * as.numeric(coeffs_h$cough) - used_data_i_m$shortness_of_breath * as.numeric(coeffs_h$shortness_of_breath) - used_data_i_m$administrative * as.numeric(coeffs_h[1,8])
          used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_f$female) * as.numeric(coeffs_h$female) - used_data_h_f$cough * as.numeric(coeffs_h$cough) - used_data_h_f$shortness_of_breath * as.numeric(coeffs_h$shortness_of_breath) - used_data_h_f$administrative * as.numeric(coeffs_h[1,8])
          used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_m$male) * as.numeric(coeffs_h$male) - used_data_h_m$cough * as.numeric(coeffs_h$cough) - used_data_h_m$shortness_of_breath * as.numeric(coeffs_h$shortness_of_breath) - used_data_h_m$administrative * as.numeric(coeffs_h[1,8])
          used_data_c_f[, obs_adj := obs  - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - cough * as.numeric(coeffs_c$cough) - shortness_of_breath * as.numeric(coeffs_c$shortness_of_breath) - administrative * as.numeric(coeffs_c[1,8])]
          used_data_c_m[, obs_adj := obs  - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - cough * as.numeric(coeffs_c$cough) - shortness_of_breath * as.numeric(coeffs_c$shortness_of_breath) - administrative * as.numeric(coeffs_c[1,8])]
        }
      } else if (out == 'cog') {
        used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_f$female) * as.numeric(coeffs_h$female) - used_data_i_f$memory_problems * as.numeric(coeffs_h$memory_problems)
        used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_m$male) * as.numeric(coeffs_h$male) - used_data_i_m$memory_problems * as.numeric(coeffs_h$memory_problems)
        used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_f$female) * as.numeric(coeffs_h$female) - used_data_h_f$memory_problems * as.numeric(coeffs_h$memory_problems)
        used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_m$male) * as.numeric(coeffs_h$male) - used_data_h_m$memory_problems * as.numeric(coeffs_h$memory_problems)
        if (kids == 1) {
          used_data_c_f[, obs_adj := obs - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - memory_problems * as.numeric(coeffs_c$memory_problems)]
          used_data_c_m[, obs_adj := obs - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - memory_problems * as.numeric(coeffs_c$memory_problems)]
        } else {
          used_data_c_f[, obs_adj := obs + (1 - female) * as.numeric(coeffs_c$female) + (kids - children) * as.numeric(coeffs_c$children) - memory_problems * as.numeric(coeffs_c$memory_problems)]
          used_data_c_m[, obs_adj := obs + (1 - male) * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children) - memory_problems * as.numeric(coeffs_c$memory_problems)]
        }
      } else if (out %in% c('any_main', 'any')) {
        used_data_i_f$obs_adj <- used_data_i_f$obs + (1 - used_data_i_f$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_f$female) * as.numeric(coeffs_h$female) - used_data_i_f$other_list * as.numeric(coeffs_h$other_list)
        used_data_i_m$obs_adj <- used_data_i_m$obs + (1 - used_data_i_m$icu) * as.numeric(coeffs_h$icu) + (1-used_data_i_m$male) * as.numeric(coeffs_h$male) - used_data_i_m$other_list * as.numeric(coeffs_h$other_list)
        used_data_h_f$obs_adj <- used_data_h_f$obs - used_data_h_f$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_f$female) * as.numeric(coeffs_h$female) - used_data_h_f$other_list * as.numeric(coeffs_h$other_list)
        used_data_h_m$obs_adj <- used_data_h_m$obs - used_data_h_m$icu * as.numeric(coeffs_h$icu) + (1-used_data_h_m$male) * as.numeric(coeffs_h$male) - used_data_h_m$other_list * as.numeric(coeffs_h$other_list)
        if (kids == 1) {
          used_data_c_f[, obs_adj := obs - other_list * as.numeric(coeffs_c$other_list) - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children)]
          used_data_c_m[, obs_adj := obs - other_list * as.numeric(coeffs_c$other_list) - female * as.numeric(coeffs_c$female) - male * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children)]
        } else {
          used_data_c_f[, obs_adj := obs - other_list * as.numeric(coeffs_c$other_list) + (1 - female) * as.numeric(coeffs_c$female) + (kids - children) * as.numeric(coeffs_c$children)]
          used_data_c_m[, obs_adj := obs - other_list * as.numeric(coeffs_c$other_list) + (1 - male) * as.numeric(coeffs_c$male) + (kids - children) * as.numeric(coeffs_c$children)]
        }
      }
    }
    
    
    used_data_i_f[, obs_exp := exp(obs) / (1 + exp(obs)) - offset]
    used_data_i_f[, obs_exp_adj := exp(obs_adj) / (1 + exp(obs_adj)) - offset]
    used_data_i_m[, obs_exp := exp(obs) / (1 + exp(obs)) - offset]
    used_data_i_m[, obs_exp_adj := exp(obs_adj) / (1 + exp(obs_adj)) - offset]
    used_data_h_f[, obs_exp := exp(obs) / (1 + exp(obs)) - offset]
    used_data_h_f[, obs_exp_adj := exp(obs_adj) / (1 + exp(obs_adj)) - offset]
    used_data_h_m[, obs_exp := exp(obs) / (1 + exp(obs)) - offset]
    used_data_h_m[, obs_exp_adj := exp(obs_adj) / (1 + exp(obs_adj)) - offset]
    used_data_c_f[, obs_exp := exp(obs) / (1 + exp(obs)) - offset]
    used_data_c_f[, obs_exp_adj := exp(obs_adj) / (1 + exp(obs_adj)) - offset]
    used_data_c_m[, obs_exp := exp(obs) / (1 + exp(obs)) - offset]
    used_data_c_m[, obs_exp_adj := exp(obs_adj) / (1 + exp(obs_adj)) - offset]
    used_data_i_f[obs_exp_adj < 0, obs_exp_adj := 0]
    used_data_i_m[obs_exp_adj < 0, obs_exp_adj := 0]
    used_data_h_f[obs_exp_adj < 0, obs_exp_adj := 0]
    used_data_h_m[obs_exp_adj < 0, obs_exp_adj := 0]
    used_data_c_f[obs_exp_adj < 0, obs_exp_adj := 0]
    used_data_c_m[obs_exp_adj < 0, obs_exp_adj := 0]
    
    if (hosp_kids == 0) {
      rr_summaries_h$children <- 0
      temp <- copy(rr_summaries_h)
      temp$children <- 1
      rr_summaries_h <- rbind(rr_summaries_h, temp)
    }
    rr_summaries <- rbind(rr_summaries_h, rr_summaries_c, fill = TRUE)
    
    # Convert from days to months for plotting
    rr_summaries$follow_up_months <- rr_summaries$follow_up_days * (12 / 365)
    
    
    if (poppredict == 'children' & hosp_kids == 0) {
      rr_summariesF <- rr_summaries[((female==1 & (hospital==1 | icu==1)) |
                                       (female==0 & male==0 & hospital==0 & icu==0)) &
                                      children==kids]
      rr_summariesM <- rr_summaries[((male==1 & (hospital==1 | icu==1)) |
                                       (female==0 & male==0 & hospital==0 & icu==0)) &
                                      children==kids]
    } else if (poppredict == 'children' & hosp_kids == 1) {
      rr_summariesF <- rr_summaries[((female==0 & male==0 & (hospital==1 | icu==1)) |
                                       (female==0 & male==0 & hospital==0 & icu==0)) &
                                      children==kids]
      rr_summariesM <- rr_summaries[((female==0 & male==0 & (hospital==1 | icu==1)) |
                                       (female==0 & male==0 & hospital==0 & icu==0)) &
                                      children==kids]
    } else {
      rr_summariesF <- rr_summaries[female == 1 & children==kids]
      rr_summariesM <- rr_summaries[male == 1 & children==kids]
    }
    
    multiplier_h <- 10
    multiplier_c <- 20

    used_data_i_f$weight <- 1/(multiplier_h*used_data_i_f$standard_error)
    used_data_i_f$weight[used_data_i_f$weight>10] <- 10
    used_data_i_f$weight[used_data_i_f$weight<2] <- 2
    used_data_i_m$weight <- 1/(multiplier_h*used_data_i_m$standard_error)
    used_data_i_m$weight[used_data_i_m$weight>10] <- 10
    used_data_i_m$weight[used_data_i_m$weight<2] <- 2
    used_data_h_f$weight <- 1/(multiplier_h*used_data_h_f$standard_error)
    used_data_h_f$weight[used_data_h_f$weight>10] <- 10
    used_data_h_f$weight[used_data_h_f$weight<2] <- 2
    used_data_h_m$weight <- 1/(multiplier_h*used_data_h_m$standard_error)
    used_data_h_m$weight[used_data_h_m$weight>10] <- 10
    used_data_h_m$weight[used_data_h_m$weight<2] <- 2
    used_data_c_f$weight <- 1/(multiplier_c*used_data_c_f$standard_error)
    used_data_c_f$weight[used_data_c_f$weight>10] <- 10
    used_data_c_f$weight[used_data_c_f$weight<2] <- 2
    used_data_c_m$weight <- 1/(multiplier_c*used_data_c_m$standard_error)
    used_data_c_m$weight[used_data_c_m$weight>10] <- 10
    used_data_c_m$weight[used_data_c_m$weight<2] <- 2
    
    used_data <- rbind(used_data_h_f, used_data_h_m, used_data_c_f, used_data_c_m, fill=TRUE)
    
    table(used_data$study_id)
    
    
    
    
    ##############################
    # Community
    rr_summariesF$pred_hi[rr_summariesF$pred_hi>1 & rr_summariesF$hospital==0 & rr_summariesF$icu==0] <- 1
    rr_summariesM$pred_hi[rr_summariesM$pred_hi>1 & rr_summariesF$hospital==0 & rr_summariesF$icu==0] <- 1
    plot <- ggplot(data=rr_summariesF, aes(x=follow_up_months, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=0.5) +
      geom_line(data=rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (months)") +
      ggtitle(paste(title, "among community, females", poppredict)) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) +
      scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesF$follow_up_months),2), limits = c(0, ceiling(max(used_data[male!=1 & pop=="community", follow_up_months])))) +
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data[w==1 & male!=1 & pop=="community"], aes(x=follow_up_months, y=mean, color=used_data[w ==1 & male!=1 & pop=="community", study_id]),
                 size=(used_data[w==1 & male!=1 & pop=="community", weight]), shape=16, alpha=0.5) +
      geom_point(data=used_data[w ==0 & male!=1 & pop=="community",], aes(x=follow_up_months, y=mean, color=used_data[w ==0 & male!=1 & pop=="community", study_id]),
                 size=(used_data[w==0 & male!=1 & pop=="community", weight]), shape=1, alpha=0.5, show.legend = FALSE) +
      scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
    plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
    
    
    plot
    ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_comm.pdf"), width = 8, height = 5)
    
    plot <- ggplot(data=rr_summariesM, aes(x=follow_up_months, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=0.5) +
      geom_line(data=rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (months)") +
      ggtitle(paste(title, "among community, males", poppredict)) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) +
      scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesM$follow_up_months),2), limits = c(0, ceiling(max(used_data[female!=1 & pop=="community", follow_up_months])))) +
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data[w==1 & female!=1 & pop=="community"], aes(x=follow_up_months, y=mean, color=used_data[w ==1 & female!=1 & pop=="community", study_id]),
                 size=(used_data[w==1 & female!=1 & pop=="community", weight]), shape=16, alpha=0.5) +
      geom_point(data=used_data[w ==0 & female!=1 & pop=="community",], aes(x=follow_up_months, y=mean, color=used_data[w ==0 & female!=1 & pop=="community", study_id]),
                 size=(used_data[w==0 & female!=1 & pop=="community", weight]), shape=1, alpha=0.5, show.legend = FALSE) +
      scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
    plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
    
    plot
    #pdf(paste0(folder,"plots/v", version, "/", exp, "_", out, "_log.pdf"),width=10, height=7.5)
    #ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_comm.pdf"), width = 8, height = 5)
    
    ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_comm.pdf"), width = 8, height = 5)
    
    # ADJUSTED
    
    plot <- ggplot(data=rr_summariesF, aes(x=follow_up_months, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=0.5) +
      geom_line(data=rr_summariesF[hospital==0 & icu==0], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (months)") +
      ggtitle(paste(title, "among community, females, adjusted values", poppredict)) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) +
      scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesF$follow_up_months),2), 
                         limits = c(0, ceiling(
                           max(
                             max(used_data[w==1 & male!=1 & pop=="community", follow_up_months]),
                             max(used_data[male!=1 & pop=="community", follow_up_months])
                           )))
      ) +
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data_c_f[w==1 & male!=1 & pop=="community"], aes(x=follow_up_months, y=obs_exp_adj, color=used_data_c_f[w ==1 & male!=1 & pop=="community", study_id]),
                 size=(used_data_c_f[w==1 & male!=1 & pop=="community", weight]), shape=16, alpha=0.5) +
      geom_point(data=used_data_c_f[w ==0 & male!=1 & pop=="community"], aes(x=follow_up_months, y=obs_exp_adj, color=used_data_c_f[w ==0 & male!=1 & pop=="community", study_id]),
                 size=(used_data_c_f[w==0 & male!=1 & pop=="community", weight]), shape=1, alpha=0.5, show.legend = FALSE) +
      scale_colour_discrete("Study") + theme(legend.spacing.x = unit(0.2, 'cm'))
    plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
    
    plot
    
    ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_comm_adj.pdf"), width = 8, height = 5)
    
    plot <- ggplot(data=rr_summariesM, aes(x=follow_up_months, y=pred), fill = "blue")+
      geom_ribbon(data= rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=0.5) +
      geom_line(data=rr_summariesM[hospital==0 & icu==0], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
      ylab("Proportion") +
      xlab("Follow up (months)") +
      ggtitle(paste(title, "among community, males, adjusted values", poppredict)) +
      theme_minimal() +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) +
      scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesM$follow_up_months),2), 
                         limits = c(0, ceiling(
                           max(
                             max(used_data[w==1 & female!=1 & pop=="community", follow_up_months]),
                             max(used_data[w ==0 & female!=1 & pop=="community",follow_up_months])
                           )))
      ) +
      theme(axis.line=element_line(colour="black")) +
      geom_point(data=used_data_c_m[w==1 & female!=1 & pop=="community"], aes(x=follow_up_months, y=obs_exp_adj, color=used_data_c_m[w ==1 & female!=1 & pop=="community", study_id]),
                 size=(used_data_c_m[w==1 & female!=1 & pop=="community", weight]), shape=16, alpha=0.5) +
      geom_point(data=used_data_c_m[w==0 & female!=1 & pop=="community"], aes(x=follow_up_months, y=obs_exp_adj, color=used_data_c_m[w ==0 & female!=1 & pop=="community", study_id]),
                 size=(used_data_c_m[w==0 & female!=1 & pop=="community", weight]), shape=1, alpha=0.5, show.legend = FALSE) +
      scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
    plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
    
    plot
    
    ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_comm_adj.pdf"), width = 8, height = 5)
    
    
    
    
    
    
    if ((hosp_kids == 1) | (hosp_kids == 0 & poppredict == 'adults')) {
      #####################################
      # Hospital
      
      plot <- ggplot(data=rr_summariesF, aes(x=follow_up_months, y=pred), fill = "blue")+
        geom_ribbon(data= rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
        geom_line(data=rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
        ylab("Proportion") +
        xlab("Follow up (months)") +
        ggtitle(paste(title, "among hospitalized, females", poppredict)) +
        theme_minimal() +
        scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) +
        scale_x_continuous(expand=c(0,0.05), breaks = seq(0,24,2)) +
        scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesF$follow_up_months),2), 
                           limits = c(0, max(used_data$follow_up_months))) +
        theme(axis.line=element_line(colour="black")) +
        geom_point(data=used_data[w==1 & male!=1 & pop!="community",], aes(x=follow_up_months, y=obs_exp,
                                                                           color=used_data[w ==1 & male!=1 & pop!="community", study_id],
                                                                           shape=used_data[w ==1 & male!=1 & pop!="community", pop]),
                   size=(used_data[w==1 & male!=1 & pop!="community", weight]), alpha=0.5) +
        geom_point(data=used_data[w ==0 & male!=1 & pop=="hospital",], aes(x=follow_up_months, y=obs_exp,
                                                                           color=used_data[w ==0 & male!=1 & pop=="hospital", study_id],
                                                                           shape=used_data[w ==0 & male!=1 & pop=="hospital", pop]),
                   size=(used_data[w==0 & male!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
        geom_point(data=used_data[w ==0 & male!=1 & pop=="ICU",], aes(x=follow_up_months, y=obs_exp,
                                                                      color=used_data[w ==0 & male!=1 & pop=="ICU", study_id],
                                                                      shape=used_data[w ==0 & male!=1 & pop=="ICU", pop]),
                   size=(used_data[w==0 & male!=1 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
        scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
      plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
      plot <- plot + guides(color = guide_legend(ncol = 2))
      plot
      ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_hosp.pdf"), width = 8, height = 5)
      
      
      plot <- ggplot(data=rr_summariesM, aes(x=follow_up_months, y=pred), fill = "blue")+
        geom_ribbon(data= rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
        geom_line(data=rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
        ylab("Proportion") +
        xlab("Follow up (months)") +
        ggtitle(paste(title, "among hospitalized, males", poppredict)) +
        theme_minimal() +
        scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) +
        scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesM$follow_up_months),2), 
                           limits = c(0, max(used_data$follow_up_months))) +
        theme(axis.line=element_line(colour="black")) +
        geom_point(data=used_data[w==1 & female!=1 & pop!="community",], aes(x=follow_up_months, y=obs_exp,
                                                                             color=used_data[w ==1 & female!=1 & pop!="community", study_id],
                                                                             shape=used_data[w ==1 & female!=1 & pop!="community", pop]),
                   size=(used_data[w==1 & female!=1 & pop!="community", weight]), alpha=0.5) +
        geom_point(data=used_data[w ==0 & female!=1 & pop=="hospital",], aes(x=follow_up_months, y=obs_exp,
                                                                             color=used_data[w ==0 & female!=1 & pop=="hospital", study_id],
                                                                             shape=used_data[w ==0 & female!=1 & pop=="hospital", pop]),
                   size=(used_data[w==0 & female!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
        geom_point(data=used_data[w ==0 & female!=1 & pop=="ICU",], aes(x=follow_up_months, y=obs_exp,
                                                                        color=used_data[w ==0 & female!=1 & pop=="ICU", study_id],
                                                                        shape=used_data[w ==0 & female!=1 & pop=="ICU", pop]),
                   size=(used_data[w==0 & female!=1 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
        scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
      plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
      plot <- plot + guides(color = guide_legend(ncol = 2))
      plot
      ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_hosp.pdf"), width = 8, height = 5)
      
      # ADJUSTED
      
      plot <- ggplot(data=rr_summariesF, aes(x=follow_up_months, y=pred), fill = "blue")+
        geom_ribbon(data= rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
        geom_line(data=rr_summariesF[hospital==1 & icu==0], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
        ylab("Proportion") +
        xlab("Follow up (months)") +
        ggtitle(paste(title, "among hospitalized, females, adjusted values", poppredict)) +
        theme_minimal() +
        scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) +
        scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesF$follow_up_months),2), 
                           limits = c(0, max(used_data$follow_up_months))) +
        theme(axis.line=element_line(colour="black")) +
        geom_point(data=used_data_h_f[w==1 & male!=1,], aes(x=follow_up_months, y=obs_exp_adj,
                                                            color=used_data_h_f[w ==1 & male!=1, study_id],
                                                            shape=used_data_h_f[w ==1 & male!=1, pop]),
                   size=(used_data_h_f[w==1 & male!=1, weight]), alpha=0.5) +
        geom_point(data=used_data_h_f[w ==0 & male!=1 & pop=="hospital",], aes(x=follow_up_months, y=obs_exp_adj,
                                                                               color=used_data_h_f[w ==0 & male!=1 & pop=="hospital", study_id],
                                                                               shape=used_data_h_f[w ==0 & male!=1 & pop=="hospital", pop]),
                   size=(used_data_h_f[w==0 & male!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
        geom_point(data=used_data_h_f[w ==0 & male!=1 & pop=="ICU",], aes(x=follow_up_months, y=obs_exp_adj,
                                                                          color=used_data_h_f[w ==0 & male!=1 & pop=="ICU", study_id],
                                                                          shape=used_data_h_f[w ==0 & male!=1 & pop=="ICU", pop]),
                   size=(used_data_h_f[w==0 & male!=1 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
        scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
      plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
      plot <- plot + guides(color = guide_legend(ncol = 2))
      plot
      ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_hosp_adj.pdf"), width = 8, height = 5)
      
      plot <- ggplot(data=rr_summariesM, aes(x=follow_up_months, y=pred), fill = "blue")+
        geom_ribbon(data= rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
        geom_line(data=rr_summariesM[hospital==1 & icu==0], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
        ylab("Proportion") +
        xlab("Follow up (months)") +
        ggtitle(paste(title, "among hospitalized, males, adjusted values", poppredict)) +
        theme_minimal() +
        scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) +
        scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesM$follow_up_months),2), 
                           limits = c(0, max(used_data$follow_up_months))) +
        theme(axis.line=element_line(colour="black")) +
        geom_point(data=used_data_h_m[w==1 & female!=1,], aes(x=follow_up_months, y=obs_exp_adj,
                                                              color=used_data_h_m[w ==1 & female!=1, study_id],
                                                              shape=used_data_h_m[w ==1 & female!=1, pop]),
                   size=(used_data_h_m[w==1 & female!=1, weight]), alpha=0.5) +
        geom_point(data=used_data_h_m[w ==0 & female!=1 & pop=="hospital",], aes(x=follow_up_months, y=obs_exp_adj,
                                                                                 color=used_data_h_m[w ==0 & female!=1 & pop=="hospital", study_id],
                                                                                 shape=used_data_h_m[w ==0 & female!=1 & pop=="hospital", pop]),
                   size=(used_data_h_m[w==0 & female!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
        geom_point(data=used_data_h_m[w ==0 & female!=1 & pop=="ICU",], aes(x=follow_up_months, y=obs_exp_adj,
                                                                            color=used_data_h_m[w ==0 & female!=1 & pop=="ICU", study_id],
                                                                            shape=used_data_h_m[w ==0 & female!=1 & pop=="ICU", pop]),
                   size=(used_data_h_m[w==0 & female!=1 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
        scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
      plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
      plot <- plot + guides(color = guide_legend(ncol = 2))
      plot
      ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_hosp_adj.pdf"), width = 8, height = 5)
      
      ################################
      # ICU
      
      plot <- ggplot(data=rr_summariesF, aes(x=follow_up_months, y=pred), fill = "blue")+
        geom_ribbon(data= rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
        geom_line(data=rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
        ylab("Proportion") +
        xlab("Follow up (months)") +
        ggtitle(paste(title, "among ICU, females", poppredict)) +
        theme_minimal() +
        scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) +
        scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesF$follow_up_months),2), 
                           limits = c(0, max(used_data$follow_up_months))) +
        theme(axis.line=element_line(colour="black")) +
        geom_point(data=used_data[w==1 & male!=1 & pop!="community",], aes(x=follow_up_months, y=obs_exp, color=used_data[w ==1 & male!=1 & pop!="community", study_id]),
                   size=(used_data[w==1 & male!=1 & pop!="community", weight]), shape=16, alpha=0.5) +
        geom_point(data=used_data[w ==0 & male!=1 & pop=="hospital",], aes(x=follow_up_months, y=obs_exp,
                                                                           color=used_data[w ==0 & male!=1 & pop=="hospital", study_id],
                                                                           shape=used_data[w ==0 & male!=1 & pop=="hospital", pop]),
                   size=(used_data[w==0 & male!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
        geom_point(data=used_data[w ==0 & male!=1 & pop=="ICU",], aes(x=follow_up_months, y=obs_exp,
                                                                      color=used_data[w ==0 & male!=1 & pop=="ICU", study_id],
                                                                      shape=used_data[w ==0 & male!=1 & pop=="ICU", pop]),
                   size=(used_data[w==0 & male!=1 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
        scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
      plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
      plot <- plot + guides(color = guide_legend(ncol = 2))
      plot
      ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_icu.pdf"), width = 8, height = 5)
      
      plot <- ggplot(data=rr_summariesM, aes(x=follow_up_months, y=pred), fill = "blue")+
        geom_ribbon(data= rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
        geom_line(data=rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
        ylab("Proportion") +
        xlab("Follow up (months)") +
        ggtitle(paste(title, "among ICU, males", poppredict)) +
        theme_minimal() +
        scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) +
        scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesM$follow_up_months),2), 
                           limits = c(0, max(used_data$follow_up_months))) +
        theme(axis.line=element_line(colour="black")) +
        geom_point(data=used_data[w==1 & female!=1 & pop!="community",], aes(x=follow_up_months, y=obs_exp, color=used_data[w ==1 & female!=1 & pop!="community", study_id]),
                   size=(used_data[w==1 & female!=1 & pop!="community", weight]), shape=16, alpha=0.5) +
        geom_point(data=used_data[w ==0 & female!=1 & pop=="hospital",], aes(x=follow_up_months, y=obs_exp,
                                                                             color=used_data[w ==0 & female!=1 & pop=="hospital", study_id],
                                                                             shape=used_data[w ==0 & female!=1 & pop=="hospital", pop]),
                   size=(used_data[w==0 & female!=1 & pop=="hospital", weight]), shape=1, alpha=1, show.legend = FALSE) +
        geom_point(data=used_data[w ==0 & female!=1 & pop=="ICU",], aes(x=follow_up_months, y=obs_exp,
                                                                        color=used_data[w ==0 & female!=1 & pop=="ICU", study_id],
                                                                        shape=used_data[w ==0 & female!=1 & pop=="ICU", pop]),
                   size=(used_data[w==0 & pop=="ICU", weight]), shape=2, alpha=1, show.legend = FALSE) +
        scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
      plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
      plot <- plot + guides(color = guide_legend(ncol = 2))
      plot
      ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_icu.pdf"), width = 8, height = 5)
      
      # ADJUSTED
      
      plot <- ggplot(data=rr_summariesF, aes(x=follow_up_months, y=pred), fill = "blue")+
        geom_ribbon(data= rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
        geom_line(data=rr_summariesF[hospital==0 & icu==1], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
        ylab("Proportion") +
        xlab("Follow up (months)") +
        ggtitle(paste(title, "among ICU, females, adjusted values", poppredict)) +
        theme_minimal() +
        scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) +
        scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesF$follow_up_months),2), 
                           limits = c(0, max(used_data$follow_up_months))) +
        theme(axis.line=element_line(colour="black")) +
        geom_point(data=used_data_i_f[w==1 & male!=1,], aes(x=follow_up_months, y=obs_exp_adj,
                                                            color=used_data_i_f[w ==1 & male!=1, study_id],
                                                            shape=used_data_i_f[w ==1 & male!=1, pop]),
                   size=(used_data_i_f[w==1 & male!=1, weight]), alpha=0.5) +
        geom_point(data=used_data_i_f[w ==0 & male!=1 & pop=="hospital",], aes(x=follow_up_months, y=obs_exp_adj,
                                                                               color=used_data_i_f[w ==0 & male!=1 & pop=="hospital", study_id],
                                                                               shape=used_data_i_f[w ==0 & male!=1 & pop=="hospital", pop]),
                   size=(used_data_i_f[w==0 & male!=1 & pop=="hospital", weight]), shape = 1, alpha=1, show.legend = FALSE) +
        geom_point(data=used_data_i_f[w ==0 & male!=1 & pop=="ICU",], aes(x=follow_up_months, y=obs_exp_adj,
                                                                          color=used_data_i_f[w ==0 & male!=1 & pop=="ICU", study_id],
                                                                          shape=used_data_i_f[w ==0 & male!=1 & pop=="ICU", pop]),
                   size=(used_data_i_f[w==0 & male!=1 & pop=="ICU", weight]), shape = 2, alpha=1, show.legend = FALSE) +
        scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
      plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
      plot <- plot + guides(color = guide_legend(ncol = 2))
      plot
      ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "F_icu_adj.pdf"), width = 8, height = 5)
      
      plot <- ggplot(data=rr_summariesM, aes(x=follow_up_months, y=pred), fill = "blue")+
        geom_ribbon(data= rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_months, ymin=pred_lo, ymax=pred_hi),  fill="lightblue", alpha=.5) +
        geom_line(data=rr_summariesM[hospital==0 & icu==1], aes(x=follow_up_months, y=pred), color = "blue", size=1) +
        ylab("Proportion") +
        xlab("Follow up (months)") +
        ggtitle(paste(title, "among ICU, males, adjusted values", poppredict)) +
        theme_minimal() +
        scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) +
        scale_x_continuous(expand=c(0,0.05), breaks = seq(0,max(rr_summariesM$follow_up_months),2), 
                           limits = c(0, max(used_data$follow_up_months))) +
        theme(axis.line=element_line(colour="black")) +
        geom_point(data=used_data_i_m[w==1 & female!=1,], aes(x=follow_up_months, y=obs_exp_adj,
                                                              color=used_data_i_m[w ==1 & female!=1, study_id],
                                                              shape=used_data_i_m[w ==1 & female!=1, pop]),
                   size=(used_data_i_m[w==1 & female!=1, weight]), alpha=0.5) +
        geom_point(data=used_data_i_m[w ==0 & female!=1 & pop=="hospital",], aes(x=follow_up_months, y=obs_exp_adj,
                                                                                 color=used_data_i_m[w ==0 & female!=1 & pop=="hospital", study_id],
                                                                                 shape=used_data_i_m[w ==0 & female!=1 & pop=="hospital", pop]),
                   size=(used_data_i_m[w==0 & female!=1 & pop=="hospital", weight]), shape = 1, alpha=1, show.legend = FALSE) +
        geom_point(data=used_data_i_m[w ==0 & female!=1 & pop=="ICU",], aes(x=follow_up_months, y=obs_exp_adj,
                                                                            color=used_data_i_m[w ==0 & female!=1 & pop=="ICU", study_id],
                                                                            shape=used_data_i_m[w ==0 & female!=1 & pop=="ICU", pop]),
                   size=(used_data_i_m[w==0 & female!=1 & pop=="ICU", weight]), shape = 2, alpha=1, show.legend = FALSE) +
        scale_colour_discrete("Study") + theme(legend.spacing.x = unit(.2, 'cm'))
      plot <- addSmallLegend(plot, pointSize = 3, titleSize = 0, textSize = 6, spaceLegend = 0.5)
      plot <- plot + guides(color = guide_legend(ncol = 2))
      plot
      ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_all_", poppredict, "M_icu_adj.pdf"), width = 8, height = 5)
    }
  }
}



















beta_hosp
#beta_comm_children
#beta_comm_adults
beta_comm_all
beta_comm_sd_all






######################################################################################
#   run model with all data of overlaps among any long COVID
######################################################################################

df <- dataset[outcome %in% c("mild_cog", "mod_cog", "mild_rsp", "mod_rsp", "sev_rsp")]
write.csv(df, paste0(outputfolder, "prepped_data_cluster/prepped_data_", datadate, "_severities_for_gather.csv"))

df <- dataset[outcome %in% c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp")]
# ignore sample size <=10 and PRA data, throwing things off
# ignore PRA, not using overlap within administrative data
df <- df[sample_size_envelope>10 & study_id!="PRA"]
df$log_mean <- log(df$mean)
df$delta_log_se <- sapply(1:nrow(df), function(i) {
  ratio_i <- df[i, "mean"] # relative_risk column
  ratio_se_i <- df[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})
df$log_se <- df$delta_log_se
df <- df[!is.na(standard_error) & !is.na(log_se)]
df <- df[age_specific==0]
table(df$study_id, df$female)
df <- df[sex=="Both"]
df <- df[!(study_id %in% c("Zurich CI -  Ticino", "Zurich CI -  Zurich"))]
write.csv(df, paste0(outputfolder, "prepped_data_cluster/prepped_data_", datadate, "_overlap_for_gather.csv"))

outcomes <- c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp", "mild_cog", "mod_cog", "mild_rsp", "mod_rsp", "sev_rsp")

for(out in outcomes) {
  #out <- "cog_rsp"
  title <- out
  if (out=="cog_rsp" | out=="fat_cog" | out=="fat_rsp" | out=="fat_cog_rsp") {
    xmax <- 1
    envelope <- "among all long-COVID patients"
    filename <- "amongLongCOVID"
    cvs <- list('hospital_or_icu', 'mean', 'standard_error')
  } else if (out=="mild_rsp" | out=="mod_rsp" | out=="sev_rsp") {
    xmax <- 1
    envelope <- "among all respiratory long-COVID patients"
    filename <- "amongRsp"
    cvs <- list('hospital_or_icu', 'mean', 'standard_error')
  } else if (out=="mild_cog" | out=="mod_cog") {
    xmax <- 1
    envelope <- "among all cognitive long-COVID patients"
    filename <- "amongCog"
    cvs <- list('hospital_or_icu', 'mean', 'standard_error')
  }
  model_dir <- paste0(out, "_v", version, "/")
  dir.create(paste0(outputfolder, "prepped_data_cluster/", model_dir))
  message(paste0("working on ", out))
  df <- dataset[outcome==out]
  
  # ignore sample size <=10 and PRA data, throwing things off
  # ignore PRA, not using overlap within administrative data
  df <- df[sample_size_envelope>10 & study_id!="PRA"]
  df <- df[age_specific==0]
  df <- df[sex=="Both"]
  df <- df[!(study_id %in% c("Zurich CI -  Ticino", "Zurich CI -  Zurich"))]
  
  # calculate standard errors using the cases of any long covid as the denominator
  df$log_mean <- log(df$mean)
  ### basically a loop that goes through each row and calcs the se in log space
  df$delta_log_se <- sapply(1:nrow(df), function(i) {
    ratio_i <- df[i, "mean"] # relative_risk column
    ratio_se_i <- df[i, "standard_error"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  df$log_se <- df$delta_log_se
  df <- df[!is.na(standard_error) & !is.na(log_se)]
  
  
  df <- df[age_specific==0]
  table(df$study_id, df$female)
  df <- df[sex=="Both"]
  offset <- median(df$offset[df$outcome == out])
  
  
  # set up data
  mr_df <- mr$MRData()
  
  mr_df$load_df(
    data = df, col_obs = "model_mean", col_obs_se = "model_se",
    col_covs = cvs, col_study_id = "study_id")
  
  model <- mr$MRBRT(
    data = mr_df,
    cov_models =list(
      mr$LinearCovModel("intercept", use_re = TRUE),
      mr$LinearCovModel("hospital_or_icu", use_re = FALSE)
    ),
    inlier_pct = inlier)
  
  # fit model
  model$fit_model(inner_print_level = 5L, inner_max_iter = n_samples)
  
  coeffs <- data.frame(t(model$beta_soln))
  colnames(coeffs) <- model$cov_names
  (coeffs)
  
  write.csv(coeffs, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs.csv"))
  
  # save model object
  py_save_object(object = model, filename = paste0(outputfolder, "prepped_data_cluster/", model_dir, "mod1.pkl"), pickle = "dill")
  
  # make predictions for full year
  predict_matrix <- data.table(intercept = model$beta_soln[1], hospital_or_icu=c(0,1), administrative=0, mean=0, standard_error=0)
  
  
  predict_data <- mr$MRData()
  predict_data$load_df(
    data = predict_matrix,
    col_covs=cvs)
  
  samples <- model$sample_soln(sample_size = n_samples)
  coeffs <- data.table(coeffs)
  for (i in 1:length(model$cov_names)) {
    coeffs[, eval(paste0(model$cov_names[i], "_sd")) := sd(samples[[1]][,i])]
  }
  write.csv(coeffs, paste0(outputfolder, "prepped_data_cluster/", model_dir, "coeffs.csv"))
  
  if (include_gamma == 0) {
    draws <- model$create_draws(
      data = predict_data,
      beta_samples = samples[[1]],
      gamma_samples = matrix(rep(0, n_samples), ncol = 1),
      random_study = FALSE,
      sort_by_data_id = TRUE)
  } else if (include_gamma == 1) {
    draws <- model$create_draws(
      data = predict_data,
      beta_samples = samples[[1]],
      gamma_samples = samples[[2]],
      random_study = TRUE,
      sort_by_data_id = TRUE)
  }
  
  # write draws for pipeline
  draws <- data.table(draws)
  draws <- cbind(draws, predict_matrix)
  draws$intercept <- NULL
  setnames(draws, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
  
  draws$administrative <- NULL
  draws <- melt(data = draws, id.vars = c("hospital_or_icu"))
  setnames(draws, "variable", "draw")
  setnames(draws, "value", "proportion")
  if (shape == "log") {
    draws[, proportion := exp(proportion) - offset]
  } else if (shape == "logit") {
    draws[, proportion := exp(proportion) / (1 + exp(proportion)) - offset]
  }
  draws[proportion < 0, proportion := 0]
  draws[proportion > 1, proportion := 1]
  
  draws <- reshape(draws, idvar = c("hospital_or_icu"), timevar = "draw", direction = "wide")
  setnames(draws, paste0("proportion.draw_", c(0:max_draw)), paste0("draw_", c(0:max_draw)))
  draws_save <- draws
  setnames(draws_save, 'hospital_or_icu', 'hospital_icu')
  
  
  write.csv(draws, file =paste0(outputfolder, "prepped_data_cluster/", model_dir, "predictions_draws.csv"))
  
  
  
  
  
  predict_matrix$pred_raw <- model$predict(predict_data, sort_by_data_id = TRUE)
  if (shape == "log") {
    predict_matrix[, pred := exp(pred_raw) - offset]
  } else if (shape == "logit") {
    predict_matrix[, pred := exp(pred_raw) / (1 + exp(pred_raw)) - offset]
  }
  draw_cols <- grep('draw_', names(draws), value = TRUE)
  predict_matrix$pred_lo <- apply(draws[, ..draw_cols], 1, function(x) quantile(x, 0.025))
  predict_matrix$pred_hi <- apply(draws[, ..draw_cols], 1, function(x) quantile(x, 0.975))
  used_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
  used_data <- as.data.table(used_data)
  rr_summaries <- copy(predict_matrix)
  
  rr_summaries
  rr_summaries$pred_hi[rr_summaries$pred_hi>1] <- 1
  rr_summaries$gamma <- mean(samples[[2]])
  write.csv(rr_summaries, file =paste0(outputfolder, "prepped_data_cluster/", model_dir, "predictions_summary.csv"))
  
  
  
  
  used_data$pop[used_data$hospital_or_icu==1] <- "hosp/ICU"
  used_data$pop[used_data$hospital_or_icu==0] <- "community"
  used_data$weight <- 0.3*(1/used_data$standard_error)
  
  if (shape == "log") {
    used_data[, obs_exp := exp(obs) - offset]
  } else if (shape == "logit") {
    used_data[, obs_exp := exp(obs) / (1 + exp(obs)) - offset]
  }
  used_data[obs_exp < 0, obs_exp := 0]
  
  used_data <- used_data[order(obs_exp)]
  df <- df[order(mean)]
  used_data$se <- df$standard_error
  
  
  if (length(unique(used_data$hospital_or_icu))==1) {
    rr_summaries <- rr_summaries[hospital_or_icu==1]
  }
  
  rr_summaries$study_id[rr_summaries$hospital_or_icu==1] <- " ESTIMATE hosp/ICU"
  rr_summaries$pop[rr_summaries$hospital_or_icu==1] <- "hosp/ICU"
  rr_summaries$study_id[rr_summaries$hospital_or_icu==0] <- " ESTIMATE community"
  rr_summaries$pop[rr_summaries$hospital_or_icu==0] <- "community"
  used_data$obs_lo <- used_data$obs-2*used_data$obs_se
  used_data$obs_hi <- used_data$obs+2*used_data$obs_se
  
  if (shape == "log") {
    used_data$obs_lo <- exp(used_data$obs_lo) - offset
    used_data$obs_hi <- exp(used_data$obs_hi) - offset
  } else if (shape == "logit") {
    used_data$obs_lo <- exp(used_data$obs_lo) / (1 + exp(used_data$obs_lo)) - offset
    used_data$obs_hi <- exp(used_data$obs_hi) / (1 + exp(used_data$obs_hi)) - offset
  }
  used_data[obs_lo < 0, obs_low := 0]
  used_data$obs_hi[used_data$obs_hi>xmax] <- xmax
  
  plot <- ggplot(data=rr_summaries, aes(x=study_id, y=pred, ymin=pred_lo, ymax=pred_hi)) +
    geom_pointrange(data=rr_summaries[pop=="community"], aes(x=study_id, y=pred, ymin=pred_lo, ymax=pred_hi), shape = 16, size = .8, color="blue") +
    geom_pointrange(data=rr_summaries[pop=="hosp/ICU"], aes(x=study_id, y=pred, ymin=pred_lo, ymax=pred_hi), shape = 17, size = .8, color="blue") +
    geom_pointrange(data=used_data[w==1 & pop=="community"], aes(x=study_id, y=mean, ymin=obs_lo, ymax=obs_hi), shape = 16, size = .8) +
    geom_pointrange(data=used_data[w==1 & pop=="hosp/ICU"], aes(x=study_id, y=mean, ymin=obs_lo, ymax=obs_hi), shape = 17, size = .8) +
    geom_pointrange(data=used_data[w==0 & pop=="community"], aes(x=study_id, y=mean, ymin=obs_lo, ymax=obs_hi), shape=1, size = .8) +
    geom_pointrange(data=used_data[w==0 & pop=="hosp/ICU"], aes(x=study_id, y=mean, ymin=obs_lo, ymax=obs_hi), shape=2, size = .8) +
    coord_flip() +  # flip coordinates (puts labels on y axis)
    ylab("Proportion") +
    xlab("Study") +
    ggtitle(paste(title, envelope)) +
    theme_minimal() +
    theme(axis.line=element_line(colour="black")) +
    scale_y_continuous(expand=c(0,0.02), breaks = seq(0,xmax,0.1), limits=c(0,xmax)) +
    guides(fill=FALSE)
  
  plot
  
  ggsave(plot, filename=paste0(outputfolder,"FILEPATH", version, "_", out, "_", filename, ".pdf"), width = 6, height = 5)
}




# Create compiled PDF -----------------------------------------------------

# Combine PDFs into a single PDF
# 1. By outcome "any_main", "fat", "resp", "cog"
# a. Unpaired files i.e. v99_any_main_all_adultsF.pdf
# b. Create paired PDFs with adjusted/unadjusted side by side
# 2. Overlaps, i.e. *_amongLongCOVID
# 3. Severities "mild", "mod", "sev"

#' Combine unadjusted with adjusted plot
#'
#' @param plot_dir [str] Directory containing the plots
#' @param version [int] Version number
#' @param outcome [str] Outcome of interest. Choices = "any_main", "fat", "rsp", "cog"
#' @param output_dir [str] Directory to save the combined PDFs
#'
#' @return [str] List of file paths ordered by existing unpaired pdfs, followed by created side by side adj/unadjusted pdfs
#' @export
#'
#' @examples
combine_unadjusted_with_adjusted_plot <- function(plot_dir, version, outcome, output_dir) {
  
  # i.e v99_any_main
  version_outcome <- glue::glue("v{version}_{outcome}")
  
  # all files with that version and outcome
  dt <- data.table(
    file = list.files(plot_dir, pattern = version_outcome, full.names = FALSE)
  )
  
  # Extract the base file name (the common part of the filename)
  # v99_rsp_all_childrenF_comm_adj.pdf
  # v99_rsp_all_childrenF_comm.pdf
  # base_file = rsp_all_childrenF_comm
  dt[, base_file := gsub(glue::glue("{version_outcome}_"), '', file)]
  dt[, base_file := gsub(".pdf", "", base_file)]
  dt[, base_file := gsub("_adj", "", base_file)]
  
  # Remove overlaps
  dt <- dt[!grepl("amongLongCOVID", base_file)]
  
  setorder(dt, base_file)
  
  # Keep only those with pairs to create new pdf, separate unpaired to put first
  unpaired_dt <- dt[, .SD[.N == 1], by = base_file]
  dt <- dt[, .SD[.N == 2], by = base_file]
  
  # Create a column to indicate whether the file is adjusted or unadjusted
  dt[, type := ifelse(grepl("_adj", file), "adjusted", "unadjusted")]
  
  # Pivot the data.table to wide format
  dt_wide <- dcast(dt, base_file ~ type, value.var = "file")
  
  ## Combine the adjusted and unadjusted plots side-by-side
  # Convert PDF pages to images combine then save as a PDF
  output_files <- c(file.path(plot_dir, unpaired_dt[, file])) 
  for (i in 1:nrow(dt_wide)) {
    base_file <- dt_wide$base_file[i]
    adj_file <- file.path(plot_dir, dt_wide$adjusted[i])
    unadj_file <- file.path(plot_dir, dt_wide$unadjusted[i])
    
    img1 <- pdftools::pdf_convert(adj_file, dpi = 300)
    img2 <- pdftools::pdf_convert(unadj_file, dpi = 300)
    
    # Read the images into magick image objects
    image1 <- magick::image_read(img1[1]) 
    image2 <- magick::image_read(img2[1]) 
    
    # Combine images side by side
    combined_image <- magick::image_append(c(image1, image2), stack = FALSE)
    
    # Save the combined image as a PDF
    output_file <- file.path(output_dir, paste0(outcome, "_", base_file, "_.pdf"))
    magick::image_write(combined_image, output_file, format = "pdf")
    
    # Delete the temporary images
    unlink(c(img1, img2))
    # Memory clean up required by magick
    gc() 
    
    output_files <- c(output_files, output_file)
  }
  
  return(output_files)
}

# Create temp directory for intermediate files
temp_dir <- tempdir()

plot_dir <- file.path(outputfolder, "prepped_data_cluster/plots")

# 1. By outcome
outcome_files <- lapply(c("any_main", "fat", "rsp", "cog"), function(outcome) {
  combine_unadjusted_with_adjusted_plot(
    plot_dir = plot_dir, 
    version = version, 
    outcome = outcome, 
    output_dir = plot_dir
  )
})

outcome_files <- unlist(outcome_files)

# 2. Overlaps
overlap_files <- list.files(plot_dir, 
                            pattern = glue::glue("v{version}.*amongLongCOVID"), 
                            full.names = TRUE)

# 3. Severities
severity_files <- list.files(plot_dir, 
                             pattern = glue::glue("v{version}.*mild|v{version}.*mod|v{version}.*sev"), 
                             full.names = TRUE)

# Combine all files
all_files <- c(outcome_files, overlap_files, severity_files)
out_file <- file.path(plot_dir, glue::glue("v{version}_all.pdf"))
qpdf::pdf_combine(all_files, output = out_file)

