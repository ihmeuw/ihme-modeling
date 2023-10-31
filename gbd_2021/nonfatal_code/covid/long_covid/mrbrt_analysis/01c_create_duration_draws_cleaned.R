  #--------------------------------------------------------------
  # Name:
  # Date: 27 Feb 2021
  # Project: GBD nonfatal COVID
  # Purpose: estimate long COVID duration among mild/moderate cases and among hospital cases
  #--------------------------------------------------------------
  
  # setup -------------------------------------------------------
  
  # clear workspace
  rm(list=ls())
  setwd("FILEPATH")

  
  # load packages
  pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)
  library(plyr)
  library(msm)

  
  
  
  version <- 57
  year_start <- 2020
  year_end <- 2023
  estimation_years <- c(2020, 2021, 2022, 2023)
  
  
  folder <- "FILEPATH"
  outputfolder <- "FILEPATH"
  model_dir <- paste0('FILEPATH')

  
  #####################
  # Hosp/ICU long COVID duration calculation from duration model parameters
  #####################
  
  dur <- data.table(read.csv(paste0(outputfolder, "duration_parameters_hospicu.csv")))
  dur$X <- NULL
#  setnames(dur, "value", "prop_start")
#  setnames(dur, "V1", "beta0")
#  setnames(dur, "V2", "beta1")
#  setnames(dur, "variable", "draw")
  
  
  # day end for threshold of proportion going below 0.001, for calculation of overall median duration 
  # (not used directly in analysis, but rather for presentations, etc)
  dur$day_end_whole <- (log(0.001/(1-0.001))-dur$beta0)/dur$beta1
  dur$day_end <- 364
  
#  dur_yr1 <- copy(dur)
#  dur_1 <- data.table()
#  for (d in c(0:364)) {
#    print(paste0(d, '...'))
#    dur_yr1$day <- d
#    dur_1 <- rbind(dur_1, dur_yr1)
#  }
  dur_1 <- copy(dur)

  dur_1 <- dur_1[, C := -(1 / beta1) * log(abs(1 + exp(beta0)))]
  dur_1 <- dur_1[, integral_start := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * 0))) + C]
  dur_1 <- dur_1[, integral_end := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end-day)))) + C]
  dur_1 <- dur_1[, integral_very_end := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * day_end_whole))) + C]
  dur_1 <- dur_1[, integral := integral_end - integral_start]
  dur_1 <- dur_1[, integral_whole := integral_very_end - integral_start]
  dur_1 <- dur_1[, prop_start := exp(beta0) / (1+exp(beta0))]
  # calculate duration by year (for the pipeline) and overall (for reporting)
  dur_1$duration <- dur_1$integral / dur_1$prop_start
  dur_1$duration_whole <- dur_1$integral_whole / dur_1$prop_start
  dur_1
  
  dur <- dur_1
  for (yr in seq((year_start+1):year_end)) {
    dur$day_end <- 365*(yr+1)-1
    dur$day_start <- 365*yr
    dur[, eval(paste0('integral_start', yr)) := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_start-day)))) + C]
    dur[, eval(paste0('integral_end', yr)) := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end-day)))) + C]
    dur[get(paste0('integral_end', yr)) < 0, eval(paste0('integral_end', yr)) := 0]
    dur[, eval(paste0('integral', yr)) := get(paste0('integral_end', yr)) - get(paste0('integral_start', yr))]
    dur[, eval(paste0('prop_start', yr)) := (exp(beta0) * exp(beta1 * (day_start-day))) / (1+(exp(beta0) * exp(beta1 * (day_start-day))))]
    dur[, eval(paste0('dur', yr)) := get(paste0('integral', yr)) / get(paste0('prop_start', yr))]
    dur[, eval(paste0('integral_start', yr)) := NULL]
    dur[, eval(paste0('integral_end', yr)) := NULL]
    dur[, eval(paste0('integral', yr)) := NULL]
    dur[, eval(paste0('prop_start', yr)) := NULL]
  }
  dur_leapyear <- dur[day==364]
  dur_leapyear$day <- 365
  dur <- rbind(dur, dur_leapyear)
  
  
  dur$hospital_icu <- 1
  (hospdur <- median(dur$duration_whole))
  dur$outcome <- "fat_or_resp_or_cog"
  # write all duration draws for average duration spent in year of long COVID incidence based on day of incidence
  write.csv(dur, file =paste0(outputfolder, model_dir, "duration_draws_hospicu_full.csv"))
  dur[1:5,]
  dur <- dur[, !c('duration_whole', 'day_start', 'day_end', 'integral_whole', 
                  'integral', 'integral_very_end', 'integral_end', 
                  'integral_start', 'C', 'day_end_whole', 'beta0', 
                  'beta1', 'prop_start')]
  # write clean concise duration draws for average duration spent in year of long COVID incidence based on day of incidence
  write.csv(dur, file =paste0(outputfolder, model_dir, "duration_draws_hospicu_clean.csv"))
  write.csv(dur, file =paste0(outputfolder, "duration_draws_hospicu_clean_by_day.csv"))

  
  
  
  
  #####################
  # Community long COVID duration calculation from duration model parameters
  #####################
  
  dur <- data.table(read.csv(paste0(outputfolder, "duration_parameters_midmod_all.csv")))
  dur$X <- NULL
#  setnames(dur, "value", "prop_start")
#  setnames(dur, "V1", "beta0")
#  setnames(dur, "V2", "beta1")
#  setnames(dur, "variable", "draw")
  
  
  # day end for threshold of proportion going below 0.001, for calculation of overall median duration 
  # (not used directly in analysis, but rather for presentations, etc)
  dur$day_end_whole <- (log(0.001/(1-0.001))-dur$beta0)/dur$beta1
  dur$day_end <- 364
  
  dur_yr1 <- copy(dur)
  dur_1 <- data.table()
  for (d in c(0:364)) {
    dur_yr1$day <- d
    dur_1 <- rbind(dur_1, dur_yr1)
  }
  
  dur_1 <- dur_1[, C := -(1 / beta1) * log(abs(1 + exp(beta0)))]
  dur_1 <- dur_1[, integral_start := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * 0))) + C]
  dur_1 <- dur_1[, integral_end := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end-day)))) + C]
  dur_1 <- dur_1[, integral_very_end := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * day_end_whole))) + C]
  dur_1 <- dur_1[, integral := integral_end - integral_start]
  dur_1 <- dur_1[, integral_whole := integral_very_end - integral_start]
  dur_1 <- dur_1[, prop_start := exp(beta0) / (1+exp(beta0))]
  # calculate duration by year (for the pipeline) and overall (for reporting)
  dur_1$duration <- dur_1$integral / dur_1$prop_start
  dur_1$duration_whole <- dur_1$integral_whole / dur_1$prop_start
  dur_1
  
  dur <- dur_1
  for (yr in seq((year_start+1):year_end)) {
    dur$day_end <- 365*(yr+1)-1
    dur$day_start <- 365*yr
    dur[, eval(paste0('integral_start', yr)) := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_start-day)))) + C]
    dur[, eval(paste0('integral_end', yr)) := (1 / beta1) * log(abs(1 + exp(beta0) * exp(beta1 * (day_end-day)))) + C]
    dur[get(paste0('integral_end', yr)) < 0, eval(paste0('integral_end', yr)) := 0]
    dur[, eval(paste0('integral', yr)) := get(paste0('integral_end', yr)) - get(paste0('integral_start', yr))]
    dur[, eval(paste0('prop_start', yr)) := (exp(beta0) * exp(beta1 * (day_start-day))) / (1+(exp(beta0) * exp(beta1 * (day_start-day))))]
    dur[, eval(paste0('dur', yr)) := get(paste0('integral', yr)) / get(paste0('prop_start', yr))]
    dur[, eval(paste0('integral_start', yr)) := NULL]
    dur[, eval(paste0('integral_end', yr)) := NULL]
    dur[, eval(paste0('integral', yr)) := NULL]
    dur[, eval(paste0('prop_start', yr)) := NULL]
  }
  dur_leapyear <- dur[day==364]
  dur_leapyear$day <- 365
  dur <- rbind(dur, dur_leapyear)
  
  
  dur$hospital_icu <- 0
  (commdur <- median(dur$duration_whole))
  dur$outcome <- "fat_or_resp_or_cog"
  # write all duration draws for average duration spent in year of long COVID incidence based on day of incidence
  write.csv(dur, file =paste0(outputfolder, model_dir, "duration_draws_midmod_full.csv"))
  dur <- dur[, !c('duration_whole', 'day_start', 'day_end', 'integral_whole', 
                  'integral', 'integral_very_end', 'integral_end', 
                  'integral_start', 'C', 'day_end_whole', 'beta0', 
                  'beta1', 'prop_start')]
  dur[1:5,]
  # write clean concise duration draws for average duration spent in year of long COVID incidence based on day of incidence
  write.csv(dur, file =paste0(outputfolder, model_dir, "duration_draws_midmod_clean.csv"))
  write.csv(dur, file =paste0(outputfolder, "duration_draws_midmod_clean_by_day.csv"))
  
#  prevcomm <- read.csv(file =paste0(outputfolder, model_dir, "predictions_draws_community.csv"))
  
  
  