###########################################################
### Author: 
### Date: 11/9/17
### Project: Frequency Duration Headache Analysis
### Purpose: GBD 2017 Nonfatal Analysis
###########################################################

## SET-UP
rm(list=ls())

if (Sys.info()[1] == "Linux"){
  j_root <- "/home/j/"
  h_root <- "~/"
} else if (Sys.info()[1] == "Windows"){
  j_root <- "J:/"
  h_root <- "H:/"
}

pacman::p_load(data.table, plyr)
library("openxlsx", lib.loc = paste0(j_root, FILEPATH))
library("metafor", lib.loc = paste0(j_root, FILEPATH))

## SET OBJECTS
data_in_path <- paste0(j_root, FILEPATH)
output_dir <- paste0(j_root, FILEPATH)
functions_dir <- paste0(j_root, FILEPATH)
mig_acause <- "neuro_migraine"
mig_bundle <- "neuro_tensache"
mig_bundle <- 1385
tth_bundle <- 1352
draws <- paste0("draw_", 0:999)
freq_draws <- paste0("draw_", 0:999, "_Frequency")
dur_draws <- paste0("draw_", 0:999, "_Duration")
date <- Sys.Date()
date <- gsub("-", "_", date)

## SOURCE FUNCTIONS
source(paste0(functions_dir, "get_epi_data.R"))

## USER FUNCTIONS
scale_inputs <- function(dt){
  by_vars <- c("nid", "acause", "parameter", "sex", "type")
  dt[measure == "proportion" & is.na(cases), cases := value_prop * sample_size / 100] 
  missing_map <- dt[parameter_scale == "missing", .(nid, acause, parameter, sex, type, cases, sample_size)]
  missing_map[, new_sample_size := sample_size - cases]
  missing_map[, c("cases", "sample_size") := NULL]
  dt <- dt[!parameter_scale == "missing"]
  dt <- merge(dt, missing_map, by = by_vars, all.x = T)
  dt[!is.na(new_sample_size), sample_size := new_sample_size]
  dt[, new_sample_size := NULL]
  ## GET CASES TO ADD UP
  dt[measure == "proportion", case_total := sum(cases), by = by_vars]
  dt[measure == "proportion" & !is.na(case_total), case_mult := sample_size/case_total]
  dt[measure == "proportion" & !is.na(case_total), cases := cases * case_mult]
  dt[, c("value_prop", "case_total", "case_mult") := NULL]
  dt[measure == "continuous", standard_error := standard_dev / sqrt(sample_size)]
  return(dt)
}

beta_dis <- function(dt){
  by_vars <- c("nid", "acause", "parameter", "sex", "type")
  dt[measure == "proportion", cat_mean := rowMeans(.SD), .SDcols = c("cat_start", "cat_end")]
  dt[, non_cases := sample_size - cases]
  beta_rows <- copy(dt)
  beta_rows <- beta_rows[measure == "proportion"]
  beta_rows[, row_count := 1:.N]
  beta_cols <- data.table()
  for (x in unique(beta_rows$row_count)){
    row <- beta_rows[row_count == x]
    beta_draws <- as.data.table(rbeta(n = 1000, shape1 = row$cases, shape2 = row$non_cases, ncp = 0))
    beta_draws[, row_count := x]
    beta_draws[, draw := paste0("draw_", 0:999)]
    beta_draws <- dcast(beta_draws, row_count ~ draw, value.var = "V1")
    beta_cols <- rbind(beta_cols, beta_draws)
  }
  beta_rows <- merge(beta_rows, beta_cols, by = "row_count")
  beta_rows[, (draws) := lapply(.SD, function(x) x * cat_mean), .SDcols = draws]
  beta_rows[, (draws) := lapply(.SD, function(x) sum(x)), by = by_vars, .SDcols = draws]
  beta_rows[, c("row_count", "non_cases") := NULL]
  return(beta_rows)
}

normal_dis <- function(dt){
  contin_rows <- copy(dt)
  contin_rows <- contin_rows[measure == "continuous"]
  contin_rows[, row_count := 1:.N]
  contin_cols <- data.table()
  for (x in unique(contin_rows$row_count)){
    row <- copy(contin_rows)
    row <- contin_rows[row_count == x]
    contin_draws <- as.data.table(rnorm(n = 1000, mean = row$value_mean, sd = row$standard_error))
    contin_draws[, row_count := x]
    contin_draws[, draw := paste0("draw_", 0:999)]
    contin_draws <- dcast(contin_draws, row_count ~ draw, value.var = "V1")
    contin_cols <- rbind(contin_cols, contin_draws)
  }
  contin_rows <- merge(contin_rows, contin_cols, by = "row_count")
  contin_rows[, c("row_count", "non_cases") := NULL]
  return(contin_rows)
}

reformat <- function(dt){
  by_vars <- c("nid", "acause", "type", "sex", "parameter")
  sum_vars <- c("nid", "acause", "type", "sex")
  unique_data <- unique(dt, by = by_vars)
  long_dt <- melt(unique_data, measure.vars = draws, variable.name = "draw")
  long_dt <- dcast(long_dt, draw + nid + acause + type + sex + kids + assume ~ parameter, value.var = "value")
  dt <- copy(long_dt)
  dt[, mean_dur := mean(Duration), by = sum_vars]
  dt[, sd_dur := sd(Duration), by = sum_vars]
  dt[, mean_freq := mean(Frequency), by = sum_vars]
  dt[, sd_freq := sd(Frequency), by = sum_vars]
  dt <- unique(dt, by = sum_vars)
  dt <- dt[, c("Frequency", "Duration", "draw") := NULL]
  return(dt)
}

meta_analysis <- function(name){
  dt <- get(name)
  freq_data <- copy(dt[!is.na(mean_freq)])
  dur_data <- copy(dt[!is.na(mean_dur)])
  if (nrow(freq_data)>0 & nrow(dur_data)>0){
    meta_freq <- rma(yi = freq_data$mean_freq, sei = freq_data$sd_freq)
    meta_dur <- rma(yi = dur_data$mean_dur, sei = dur_data$sd_dur)
    meta_freq <- rma(yi = freq_data$mean_freq, sei = freq_data$sd_freq)
    meta_dur <- rma(yi = dur_data$mean_dur, sei = dur_data$sd_dur)
    pdf(paste0(output_dir, name, ".pdf"))
    forest(meta_freq, slab = paste0(freq_data$nid, " ", freq_data$sex), showweights = T, xlab = "Frequency")
    forest(meta_dur, slab = paste0(dur_data$nid, " ", dur_data$sex), showweights = T, xlab = "Duration")
    dev.off()
    results <- data.table(type = name, freq_mean = as.numeric(meta_freq$beta),
                          freq_se = as.numeric(meta_freq$se), freq_n = nrow(freq_data), 
                          dur_mean = as.numeric(meta_dur$beta), dur_se = as.numeric(meta_dur$se),
                          dur_n = nrow(dur_data))
  } else {
    results <- data.table(type = name, freq_mean = NA, freq_se = NA, freq_n = nrow(freq_data),
                          dur_mean = NA, dur_se = NA, dur_n = nrow(dur_data))
  }
  return(results)
}

calculate_time <- function(dt){
  results <- copy(dt)
  results[!is.na(freq_mean), time_ictal := (freq_mean * dur_mean/24)/365.25]
  results[!is.na(freq_mean), dur_se2 := (dur_se/24)/365.25]
  results[!is.na(freq_mean), dur_mean2 := (dur_mean/24)/365.25]
  results[!is.na(freq_mean), time_ictal_se := sqrt(freq_se^2 * dur_se2^2 + freq_se^2 * dur_mean2^2 + freq_mean^2 * dur_se2^2)]
  results[, c("dur_mean2", "dur_se2") := NULL]
  return(results)
}

## SET UP ANALYSIS
in_data <- get_epi_data(bundle_id = 1385)
in_data[parameter == "Duration ", parameter := "Duration"]
dt <- scale_inputs(in_data)
dt <- dt[!outlier_type_id == 1] ## exclude outliered studies
prop_draws <- beta_dis(dt)
contin_draws <- normal_dis(dt)
all_draws <- rbind(prop_draws, contin_draws)
formatted <- reformat(all_draws)

## DEFINE SUBGROUPS
no_kids <- copy(formatted)
all_migraine <- no_kids[acause == "neuro_migraine"]
all_tth <- no_kids[acause == "neuro_tensache"]
migraine_f <- all_migraine[sex == "Female"]
migraine_m <- all_migraine[sex == "Male"]
tth_f <- all_tth[sex == "Female"]
tth_m <- all_tth[sex == "Male"]
both_migraine <- all_migraine[type == "definite,probable"]
definite_migraine <- all_migraine[type == "definite"]
probable_migraine <- all_migraine[type == "probable"]
both_tth <- all_tth[type == "definite,probable"]
definite_tth <- all_tth[type == "definite"]
probable_tth <- all_tth[type == "probable"]
moh <- no_kids[acause == "neuro_medache"]
groups <- c("all_migraine", "all_tth", "migraine_f", "migraine_m", "tth_f", "tth_m", "both_migraine", 
            "definite_migraine", "probable_migraine", "both_tth", "definite_tth", "probable_tth", "moh")

## RUN META ANALYSIS AND CALCULATE
results <- rbindlist(lapply(groups, meta_analysis))
results <- calculate_time(results)
write.csv(results, paste0(output_dir, "results", date, ".csv"), row.names = F)
