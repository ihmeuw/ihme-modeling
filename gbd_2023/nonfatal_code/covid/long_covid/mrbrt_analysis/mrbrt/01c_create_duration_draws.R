#--------------------------------------------------------------
# Name: NAME (USERNAME)
# Date: 27 Feb 2021
# Project: GBD nonfatal COVID
# Purpose: estimate long COVID duration among mild/moderate cases and among hospital cases
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())
setwd("FILEPATH")

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
library(plyr)
library(msm)




version <- 117
# 'log' if models were run as log-linear; 'logit' if logit-linear
shape <- 'logit'
year_start <- 2020
year_end <- 2024
estimation_years <- c(2020, 2021, 2022, 2023, 2024)


folder <- "FILEPATH"
outputfolder <- "FILEPATH"
model_dir <- paste0('duration_v', version, '/')
dir.create(file.path(paste0(outputfolder, model_dir)))

dur_cols_seq <- seq(length(estimation_years) - 1)
dur_cols_keep <- paste0("dur", dur_cols_seq) # dur1, dur2, dur3,etc
dur_who_cols_keep <- paste0("dur_who", dur_cols_seq) # dur_who1, dur_who2, dur_who3, etc
dur_who_hosp_cols_keep <- paste0("dur_who_hosp", dur_cols_seq) # dur_who_hosp1, dur_who_hosp2, dur_who_hosp3, etc
dur_who_icu_cols_keep <- paste0("dur_who_icu", dur_cols_seq) # dur_who_icu1, dur_who_icu2, dur_who_icu3, etc

#####################
# Hosp/ICU long COVID duration calculation from duration model parameters
#####################

dur <- fread(
  file.path(outputfolder, 
            paste0("duration_parameters_hospicu_all_v", version, ".csv"))
)

dur <- dur[female==1]
dur[, V1 := NULL]
summary(dur$beta0)
(lower <- quantile(dur$beta0, 0.025, na.rm = TRUE))
(upper <- quantile(dur$beta0, 0.975, na.rm = TRUE))
dur[beta0 > upper, beta0 := upper]
dur[beta0 < lower, beta0 := lower]
summary(dur$beta1)
(lower <- quantile(dur$beta1, 0.025, na.rm = TRUE))
(upper <- quantile(dur$beta1, 0.975, na.rm = TRUE))
dur[beta1 > upper, beta1 := upper]
dur[beta1 < lower, beta1 := lower]

# day end for threshold of proportion going below 0.001, for calculation of 
# overall median duration 
# (not used directly in analysis, but rather for presentations, etc)
if (shape == 'logit') {
  dur[, day_end_whole := (log(0.001 / (1 - 0.001)) - beta0) / beta1]
} else if (shape == 'log') {
  dur[, day_end_whole := (log(0.001) - beta0) / beta1]
}
dur[, day_end := 364]

setnames(dur, c('follow_up_days'), c('day'))
dur_1 <- copy(dur)

# calculate duration of long COVID from end of acute phase:
if (shape == 'logit') {
  dur_1[, integral_start := (1 / beta1) * log(abs(1 + exp(beta0) *
                                                    exp(beta1 * 0)))]
  dur_1[, integral_end := (1 / beta1) * log(abs(1 + exp(beta0) *
                                                  exp(beta1 * (day_end - day))))]
  dur_1[, integral_60 := (1 / beta1) * log(abs(1 + exp(beta0) *
                                                 exp(beta1 * (day_end - 60))))]
  dur_1[, integral_53 := (1 / beta1) * log(abs(1 + exp(beta0) *
                                                 exp(beta1 * (day_end - 53))))]
  dur_1[, integral_very_end := (1 / beta1) * log(abs(1 + exp(beta0) *
                                                       exp(beta1 * day_end_whole)))]
  dur_1[, prop_start := exp(beta0) / (1 + exp(beta0))]
  dur_1[, prop_start_30 := exp(beta0 + beta1 * 30) / (1 + exp(beta0 + beta1 * 30))]
  dur_1[icu == 0, prop_start_who := exp(beta0 + beta1 * 60) / (1 + exp(beta0 + beta1 * 60))]
  dur_1[icu == 1, prop_start_who := exp(beta0 + beta1 * 53) / (1 + exp(beta0 + beta1 * 53))]
} else if (shape == 'log') {
  dur_1[, integral_start := (1 / beta1) * 
          (exp(beta0 + beta1 * 0)) - 
          (exp(beta0) / beta1)]
  dur_1[, integral_60 := (1 / beta1) * 
          (exp(beta0 + beta1 * 60)) - 
          (exp(beta0) / beta1)]
  dur_1[, integral_53 := (1 / beta1) * 
          (exp(beta0 + beta1 * 53)) - 
          (exp(beta0) / beta1)]
  dur_1[, integral_end := (1 / beta1) * 
          (exp(beta0 + beta1 * (day_end - day))) - 
          (exp(beta0) / beta1)]
  dur_1[, integral_very_end := (1 / beta1) * 
          (exp(beta0 + beta1 * day_end_whole)) - 
          (exp(beta0) / beta1)]
  dur_1[, prop_start := exp(beta0)]
  dur_1[icu == 0, prop_start_who := exp(beta0 + beta1 * 60)]
  dur_1[icu == 1, prop_start_who := exp(beta0 + beta1 * 53)]
}
dur_1[, integral := integral_end - integral_start]
dur_1[, integral_whole := integral_very_end - integral_start]
# calculate hospital duration of long COVID from 3 months after symptom onset 
# 95 days from infection is WHO's def of 3 months from symptom onset 
# (which is 5 days after infection), so 95 - 35 = 60
dur_1[, integral_who_hosp := integral_very_end - integral_60]
# calculate ICU duration of long COVID from 3 months after symptom onset 
# 95 days from infection is WHO's def of 3 months from symptom onset 
# (which is 5 days after infection), so 95 - 42 = 53
dur_1[, integral_who_icu := integral_very_end - integral_53]


# calculate duration by year (for the pipeline) and overall (for reporting)
dur_1[, duration := integral / prop_start]
dur_1[, duration_whole := integral_whole / prop_start]
dur_1[, duration_who_hosp := integral_who_hosp / prop_start_who]
dur_1[, duration_who_icu := integral_who_icu / prop_start_who]
dur_1[duration_who_hosp < 0, duration_who_hosp := 0]
dur_1[duration_who_icu < 0, duration_who_icu := 0]

# mean duration in months of long COVID since end of acute phase for hosp
mean(dur_1[day == 0 & icu==0, duration_whole]) / (365 / 12)
quantile(dur_1[day == 0 & icu==0, duration_whole], 0.025) / (365 / 12)
quantile(dur_1[day == 0 & icu==0, duration_whole], 0.975) / (365 / 12)

# mean duration in months of long COVID since end of acute phase for icu
mean(dur_1[day == 0 & icu==1, duration_whole]) / (365 / 12)
quantile(dur_1[day == 0 & icu==1, duration_whole], 0.025) / (365 / 12)
quantile(dur_1[day == 0 & icu==1, duration_whole], 0.975) / (365 / 12)

# mean duration in months of long COVID since end of acute phase for hosp
mean(dur_1[day == 60 & icu==0, duration_who_hosp]) / (365 / 12)
quantile(dur_1[day == 60 & icu==0, duration_who_hosp], 0.025) / (365 / 12)
quantile(dur_1[day == 60 & icu==0, duration_who_hosp], 0.975) / (365 / 12)

# mean duration in months of long COVID since end of acute phase for icu
mean(dur_1[day == 53 & icu==1, duration_who_icu]) / (365 / 12)
quantile(dur_1[day == 53 & icu==1, duration_who_icu], 0.025) / (365 / 12)
quantile(dur_1[day == 53 & icu==1, duration_who_icu], 0.975) / (365 / 12)

#  dur_1
dur_1 <- dur_1[day < 366]

# calculate day-specific durations ... from a given day until the end of the 
# year, and then rollover durations for following years

dur <- copy(dur_1)
for (yr in seq((year_start+1):year_end)) {
  
  dur[, day_end := 365 * (yr + 1) - 1]
  dur[, day_start := 365 * yr]
  
  if (shape == 'logit') {
    dur[, eval(paste0("integral_start", yr)) :=
          (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_start - day))))]
    dur[, eval(paste0("integral_end", yr)) :=
          (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end - day))))]
    dur[get(paste0("integral_end", yr)) < 0, eval(paste0("integral_end", yr)) := 0]
    dur[, eval(paste0("integral", yr)) :=
          get(paste0("integral_end", yr)) - get(paste0("integral_start", yr))]
    dur[, eval(paste0("prop_start", yr)) :=
          (exp(beta0) * exp(beta1 * (day_start - day))) / 
          (1 + (exp(beta0) * exp(beta1 * (day_start - day))))]
    dur[, eval(paste0('dur', yr)) := get(paste0('integral', yr)) / prop_start]
    
    # calculate hosp duration from WHO definition
    dur[, eval(paste0("prop_start_who", yr)) :=
          (exp(beta0) * exp(beta1 * (day_start - day + 60))) /
          (1 + (exp(beta0) * exp(beta1 * (day_start - day + 60))))]
    dur[, eval(paste0("integral_start_who", yr)) :=
          (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_start - day + 60))))]
    dur[, eval(paste0("integral_end_who", yr)) :=
          (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end - day))))]
    dur[get(paste0("integral_end_who", yr)) < 0, eval(paste0("integral_end_who", yr)) := 0]
    dur[, eval(paste0("integral_who", yr)) :=
          get(paste0("integral_end_who", yr)) - get(paste0("integral_start_who", yr))]
    dur[, eval(paste0("dur_who_hosp", yr)) :=
          get(paste0("integral_who", yr)) / prop_start_who]
    
    # calculate ICU duration from WHO definition
    dur[, eval(paste0('prop_start_who', yr)) := 
          (exp(beta0) * exp(beta1 * (day_start - day + 53))) / 
          (1+(exp(beta0) * exp(beta1 * (day_start - day + 53))))]
    dur[, eval(paste0('integral_start_who', yr)) := 
          (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_start - day + 53))))]
    dur[, eval(paste0('integral_end_who', yr)) := 
          (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end-day))))]
    dur[get(paste0('integral_end_who', yr)) < 0, eval(paste0('integral_end_who', yr)) := 0]
    dur[, eval(paste0('integral_who', yr)) := 
          get(paste0('integral_end_who', yr)) - get(paste0('integral_start_who', yr))]
    dur[, eval(paste0('dur_who_icu', yr)) := 
          get(paste0('integral_who', yr)) / prop_start_who]
  } else if (shape == 'log') {
    dur[, eval(paste0("integral_start", yr)) := (1 / beta1) * exp(beta0 + beta1 * (day_start - day))]
    dur[, eval(paste0("integral_end", yr)) := (1 / beta1) * exp(beta0 + beta1 * (day_end - day))]
    dur[get(paste0("integral_end", yr)) < 0, eval(paste0("integral_end", yr)) := 0]
    dur[, eval(paste0("integral", yr)) := get(paste0("integral_end", yr)) - get(paste0("integral_start", yr))]
    dur[, eval(paste0("prop_start", yr)) := exp(beta0 + beta1 * (day_start - day))]
    dur[, eval(paste0('dur', yr)) := get(paste0('integral', yr)) / prop_start]
    
    # calculate hosp duration from WHO definition
    dur[, eval(paste0("integral_start_who", yr)) := (1 / beta1) * exp(beta0 + beta1 * (day_start - day + 60))]
    dur[, eval(paste0("integral_end_who", yr)) := (1 / beta1) * exp(beta0 + beta1 * (day_end - day + 60))]
    dur[get(paste0("integral_end_who", yr)) < 0, eval(paste0("integral_end_who", yr)) := 0]
    dur[, eval(paste0("integral_who", yr)) := get(paste0("integral_end_who", yr)) - get(paste0("integral_start_who", yr))]
    dur[, eval(paste0("prop_start_who", yr)) := exp(beta0 + beta1 * (day_start - day + 60))]
    dur[, eval(paste0('dur_who_hosp', yr)) := get(paste0('integral_who', yr)) / prop_start_who]
    
    # calculate ICU duration from WHO definition
    dur[, eval(paste0("integral_start_who", yr)) := (1 / beta1) * exp(beta0 + beta1 * (day_start - day + 53))]
    dur[, eval(paste0("integral_end_who", yr)) := (1 / beta1) * exp(beta0 + beta1 * (day_end - day + 53))]
    dur[get(paste0("integral_end_who", yr)) < 0, eval(paste0("integral_end_who", yr)) := 0]
    dur[, eval(paste0("integral_who", yr)) := get(paste0("integral_end_who", yr)) - get(paste0("integral_start_who", yr))]
    dur[, eval(paste0("prop_start_who", yr)) := exp(beta0 + beta1 * (day_start - day + 53))]
    dur[, eval(paste0('dur_who_hosp', yr)) := get(paste0('integral_who', yr)) / prop_start_who]
  }
  dur[, eval(paste0('integral_start', yr)) := NULL]
  dur[, eval(paste0('integral_end', yr)) := NULL]
  dur[, eval(paste0('integral', yr)) := NULL]
  dur[, eval(paste0('prop_start', yr)) := NULL]
  dur[, eval(paste0('integral_start_who', yr)) := NULL]
  dur[, eval(paste0('integral_end_who', yr)) := NULL]
  dur[, eval(paste0('integral_who', yr)) := NULL]
  dur[, eval(paste0('prop_start_who', yr)) := NULL]
}

# for these, column 'day' denotes the day of the end of the acute phase, 
# after which symptoms may persist. in the pipeline, these durations should be
# merged to the at-risk cases according to that day, after the day of at-risk 
# cases has shifted from infection day to end of the acute phase.  No date 
# shift needed in the pipeline.

dur[, hospital_icu := 1]
hospdur <- median(dur$duration_whole)
dur[, outcome := "fat_or_resp_or_cog"]

# write all duration draws for average duration spent in year of long COVID 
# incidence based on day of incidence
fwrite(dur, 
       file = file.path(
         outputfolder, model_dir, "duration_draws_hospicu_full_long.csv"))

fwrite(dur[day == 0], 
       file =file.path(outputfolder, model_dir, "duration_draws_hospicu_whole_long.csv"))

dur[1:5,]

dur <- dur[, !c("day_end_whole", "integral_start", "integral_end", 
                "integral_60", "integral_53", "integral_who_hosp", "integral_who_icu",
                "integral_very_end", "integral", "integral_whole", 
                "integral_start_who", "prop_start_who", "integral_whole_who", 
                "integral_who", "prop_start", "duration_whole", "day_start", 
                "day_end", "beta0", "beta1", "duration_whole_who_icu", 
                "duration_whole_who_hosp", 'V1', 'V2', 'V3', 'V4', 'V5', 'V6',
                "beta_icu", "beta_f", "beta_m", "V7")]

mean(dur$dur1)
mean(dur$dur2)
mean(dur$dur3)
fwrite(dur, 
       file = file.path(outputfolder, model_dir, 
                        "duration_draws_hospicu_clean_long.csv"))
fwrite(dur, 
       file = file.path(outputfolder, 
                        "duration_draws_hospicu_clean_by_day_long.csv"))


# write clean concise duration draws for average duration spent in year of 
# long COVID incidence based on day of incidence
keep_cols <- c('draw', 'day', 'duration_who_hosp', dur_who_hosp_cols_keep , 'outcome', 'duration', dur_cols_keep)
durhosp <- dur[, ..keep_cols]

durhosp[, population := 'hosp']

keep_cols <- c('draw', 'day', 'duration_who_icu', dur_who_icu_cols_keep, 'outcome', 'duration', dur_cols_keep)
duricu <- dur[, ..keep_cols]
duricu[, population := 'icu']

fwrite(durhosp, 
       file = file.path(outputfolder, model_dir, "duration_draws_hosp_clean.csv"))
fwrite(durhosp, 
       file = file.path(outputfolder, "duration_draws_hosp_clean_by_day.csv"))
fwrite(duricu, 
       file = file.path(outputfolder, model_dir, "duration_draws_icu_clean.csv"))
fwrite(duricu, 
       file = file.path(outputfolder, "duration_draws_icu_clean_by_day.csv"))

fwrite(duricu, 
       file =file.path("FILEPATH/duration_draws_icu_clean_by_day.csv"))
fwrite(durhosp, 
       file = file.path("FILEPATH/duration_draws_hosp_clean_by_day.csv"))

fwrite(duricu, 
       file =file.path("FILEPATH/duration_draws_icu_clean.csv"))


#####################
# Community long COVID duration calculation from duration model parameters
#####################

dur <- data.table(fread(file.path(outputfolder, 
                                  paste0("duration_parameters_midmod_all_v", version, ".csv"))))
dur <- dur[draw != 'proportion.children']

dur <- dur[female==1]

(lower <- quantile(dur$beta0, 0.025, na.rm = TRUE))
(upper <- quantile(dur$beta0, 0.975, na.rm = TRUE))
dur[beta0 > upper, beta0 := upper]
dur[beta0 < lower, beta0 := lower]
(lower <- quantile(dur$beta1, 0.025, na.rm = TRUE))
(upper <- quantile(dur$beta1, 0.975, na.rm = TRUE))
dur[beta1 > upper, beta1 := upper]
dur[beta1 < lower, beta1 := lower]

# day end for threshold of proportion going below 0.001, for calculation of 
# overall median duration 
# (not used directly in analysis, but rather for presentations, etc)
if (shape == 'logit') {
  dur[, day_end_whole := (log(0.001 / (1 - 0.001)) - beta0) / beta1]
} else if (shape == 'log') {
  dur[, day_end_whole := (log(0.001) - beta0) / beta1]
}
dur[, day_end := 364]

setnames(dur, c('follow_up_days'), c('day'))
dur_1 <- copy(dur)

# calculate duration of long COVID from end of acute phase:
if (shape == 'logit') {
  dur_1[, integral_start := (1 / beta1) * log(abs(1 + exp(beta0) *
                                                    exp(beta1 * 0)))]
  dur_1[, integral_end := (1 / beta1) * log(abs(1 + exp(beta0) *
                                                  exp(beta1 * (day_end - day))))]
  dur_1[, integral_start_who := (1 / beta1) * log(abs(1 + exp(beta0) *
                                                        exp(beta1 * (day_end - 81))))]
  dur_1[, integral_very_end := (1 / beta1) * log(abs(1 + exp(beta0) *
                                                       exp(beta1 * day_end_whole)))]
  dur_1[, prop_start := exp(beta0) / (1 + exp(beta0))]
  dur_1[, prop_start_who := exp(beta0 + beta1 * 81) / (1 + exp(beta0 + beta1 * 81))]
} else if (shape == 'log') {
  dur_1[, integral_start := (1 / beta1) * 
          (exp(beta0 + beta1 * 0)) - 
          (exp(beta0) / beta1)]
  dur_1[, integral_start_who := (1 / beta1) * 
          (exp(beta0 + beta1 * 81)) - 
          (exp(beta0) / beta1)]
  dur_1[, integral_end := (1 / beta1) * 
          (exp(beta0 + beta1 * (day_end - day))) - 
          (exp(beta0) / beta1)]
  dur_1[, integral_very_end := (1 / beta1) * 
          (exp(beta0 + beta1 * day_end_whole)) - 
          (exp(beta0) / beta1)]
  dur_1[, prop_start := exp(beta0)]
  dur_1[, prop_start_who := exp(beta0 + beta1 * 81)]
}
# calculate community duration of long COVID from 3 months after symptom 
# onset (WHO definition), 95 - 14 = 81 days after end of acute phase:
dur_1[, integral_whole_who := integral_very_end - integral_start_who]
dur_1[, duration_whole_who_comm := integral_whole_who / prop_start_who]
dur_1[, integral_who := integral_end - integral_start_who]

dur_1[, integral := integral_end - integral_start]
dur_1[, integral_whole := integral_very_end - integral_start]

# calculate duration by year (for the pipeline) and overall (for reporting)
dur_1[, duration := integral / prop_start]
dur_1[, duration_whole := integral_whole / prop_start]
dur_1[, duration_who_comm := integral_who / prop_start_who]
dur_1[duration_who_comm < 0, duration_who_comm := 0]


mean(dur_1[day == 0, duration_whole]) / (365 / 12)
quantile(dur_1[day == 0, duration_whole], 0.025) / (365 / 12)
quantile(dur_1[day == 0, duration_whole], 0.975) / (365 / 12)
mean(dur_1[day == 0, duration_whole])
quantile(dur_1[day == 0, duration_whole], 0.025)
quantile(dur_1[day == 0, duration_whole], 0.975)




# median duration in days of long COVID since 12 weeks post-symptom onset (WHO definition)
mean(dur_1[day == 0 & !is.na(duration_whole_who_comm), duration_whole_who_comm]) / (365 / 12)
quantile(dur_1[day == 0 & !is.na(duration_whole_who_comm), duration_whole_who_comm], 0.025) / (365 / 12)
quantile(dur_1[day == 0 & !is.na(duration_whole_who_comm), duration_whole_who_comm], 0.975) / (365 / 12)
mean(dur_1[day == 0 & !is.na(duration_whole_who_comm), duration_whole_who_comm])
quantile(dur_1[day == 0 & !is.na(duration_whole_who_comm), duration_whole_who_comm], 0.025)
quantile(dur_1[day == 0 & !is.na(duration_whole_who_comm), duration_whole_who_comm], 0.975)

dur_1 <- dur_1[day<=365]
dur <- copy(dur_1)
for (yr in seq((year_start+1):year_end)) {
  
  dur[, day_end := 365 * (yr + 1) - 1]
  dur[, day_start := 365 * yr]
  
  if (shape == 'logit') {
    dur[, eval(paste0("integral_start", yr)) :=
          (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_start - day))))]
    dur[, eval(paste0("integral_end", yr)) :=
          (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end - day))))]
    dur[get(paste0("integral_end", yr)) < 0, eval(paste0("integral_end", yr)) := 0]
    dur[, eval(paste0("integral", yr)) :=
          get(paste0("integral_end", yr)) - get(paste0("integral_start", yr))]
    dur[, eval(paste0("prop_start", yr)) :=
          (exp(beta0) * exp(beta1 * (day_start - day))) / 
          (1 + (exp(beta0) * exp(beta1 * (day_start - day))))]
    dur[, eval(paste0('dur', yr)) := get(paste0('integral', yr)) / prop_start]
    
    # calculate comm duration from WHO definition
    dur[, eval(paste0("prop_start_who", yr)) :=
          (exp(beta0) * exp(beta1 * (day_start - day + 81))) /
          (1 + (exp(beta0) * exp(beta1 * (day_start - day + 81))))]
    dur[, eval(paste0("integral_start_who", yr)) :=
          (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_start - day + 81))))]
    dur[, eval(paste0("integral_end_who", yr)) :=
          (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end - day))))]
    dur[get(paste0("integral_end_who", yr)) < 0, eval(paste0("integral_end_who", yr)) := 0]
    dur[, eval(paste0("integral_who", yr)) :=
          get(paste0("integral_end_who", yr)) - get(paste0("integral_start_who", yr))]
    dur[, eval(paste0("dur_who", yr)) :=
          get(paste0("integral_who", yr)) / prop_start_who]
    
  } else if (shape == 'log') {
    dur[, eval(paste0("integral_start", yr)) := (1 / beta1) * exp(beta0 + beta1 * (day_start - day)) + (exp(beta0) / beta1)]
    dur[, eval(paste0("integral_end", yr)) := (1 / beta1) * exp(beta0 + beta1 * (day_end - day)) + (exp(beta0) / beta1)]
    dur[, eval(paste0("integral", yr)) := get(paste0("integral_end", yr)) - get(paste0("integral_start", yr))]
    dur[, eval(paste0('dur', yr)) := get(paste0('integral', yr)) / prop_start]
    
    # calculate comm duration from WHO definition
    dur[, eval(paste0("integral_start_who", yr)) := (1 / beta1) * exp(beta0 + beta1 * (day_start - day + 81))]
    dur[, eval(paste0("integral_end_who", yr)) := (1 / beta1) * exp(beta0 + beta1 * (day_end - day + 81))]
    dur[, eval(paste0("integral_who", yr)) := get(paste0("integral_end_who", yr)) - get(paste0("integral_start_who", yr))]
    dur[, eval(paste0("prop_start_who", yr)) := exp(beta0 + beta1 * (day + 81))]
    dur[, eval(paste0('dur_who', yr)) := get(paste0('integral_who', yr)) / prop_start_who]
  }
  dur[, eval(c(paste0('integral', yr), paste0('integral_who', yr),
               paste0('integral_start', yr), paste0('integral_end', yr),
               paste0('integral_start_who', yr), paste0('integral_end_who', yr),
               paste0('prop_start', yr), paste0('prop_start_who', yr))) := NULL]
}

dur_leapyear <- dur[day == 364]
dur_leapyear[, day := 365]
dur <- rbind(dur, dur_leapyear)


dur[, hospital_icu := 0]

(commdur <- median(dur$dur1))
(commdur <- median(dur$dur2))
(commdur <- median(dur$dur3))
(commdur <- median(dur$duration_whole))
(commdur <- median(dur$duration_whole_who_comm))
(commdur <- median(dur$duration_who_comm))
(commdur <- median(dur$dur_who1))
(median(dur$dur1))
(commdur <- median(dur$dur_who2))
(commdur <- median(dur$dur_who3))

dur$outcome <- "fat_or_resp_or_cog"

# write all duration draws for average duration spent in year of long COVID 
# incidence based on day of incidence
fwrite(dur, 
       file = file.path(outputfolder, model_dir, "duration_draws_midmod_full.csv"))

fwrite(dur[day == 0], 
       file = file.path(outputfolder, model_dir, "duration_draws_midmod_whole.csv"))

dur <- dur[, !c('duration_whole', 'day_start', 'day_end', 'integral_whole', 
                'integral', 'integral_very_end', 'integral_end', 
                'integral_start', 'day_end_whole', 'beta0', 
                'beta1', 'prop_start', 'integral_start_who', 'prop_start_who',
                'integral_whole_who', 'integral_who', 'V1', 'V2', 'V3', 'V4', 
                'V5', 'V6', 'beta_f', 'beta_m', 'beta_children', 'V7')]
dur[1:5,]
names(dur)


# write clean concise duration draws for average duration spent in year of long 
# COVID incidence based on day of incidence
keep_cols <- c('draw', 'day', 'duration_who_comm', dur_who_cols_keep, 'outcome', 'duration', dur_cols_keep)
dur <- dur[, ..keep_cols]
dur[, population := 'midmod']

dur
fwrite(dur, 
       file =file.path(outputfolder, model_dir, "duration_draws_midmod_clean.csv"))

fwrite(dur, 
       file =file.path(outputfolder, "duration_draws_midmod_clean_by_day.csv"))

fwrite(dur, 
       file ="FILEPATH/duration_draws_midmod_clean_by_day.csv")


#  prevcomm <- read.csv(file =paste0(outputfolder, model_dir, "predictions_draws_community.csv"))


