################################################################################
## DESCRIPTION: Apply regression coefficients to physical activity categories
## to get crosswalked MET-min values ##
## INPUTS: Compiled data from all 12 original activity categories  ##
## OUTPUTS: Predicted mean MET ##
## AUTHOR: ##
## DATE: 16 May 2020
## Edit: 3 August 2020 - updated LMER models and also created model to predict for lowmodhigh and modhigh activity categories
## 26 October 2020 - applied SR coefficient to input data and variance (also updated variance formula)
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

# Base filepaths
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- 'FILEPATH'
## Load dependencies
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, 'FILEPATH'))
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
library(lme4, lib.loc = paste0(h, "FILEPATH"))
library(arm, lib.loc = paste0(h, "FILEPATH"))
library(merTools, lib.loc = paste0(h, "FILEPATH"))
library(crosswalk, lib.loc = "FILEPATH") # For delta transformation
# Parse args
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "parent directory where data will be pulled from",
                    default = 'FILEPATH', type = "character")
parser$add_argument("--processing_version_id", help = "version number for whole run",
                    default = 6, type = "integer")
parser$add_argument("--processing_version_note", help = "note for changes in version ",
                    default = 'GBD 2020 with regression predictions and SR adjustment', type = "character")
parser$add_argument("--cycle_year", help = "current GBD cycle",
                    default = "gbd_2020", type = "character")
parser$add_argument("--decomp_step", help = "decomp step we're uploading for",
                    default = "iterative", type = "character")

args <- parser$parse_args()
list2env(args, environment()); rm(args)


# Helper datasets
locs <- get_location_metadata(22, decomp_step = 'iterative')
ages <- fread(paste0(h, "all_ages.csv"))
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

# Make tracking data frame
me_ids <- c(9356:9361, 23714:23719)
me_name <- c(paste("gpaq", c("inactive", "low_active", "mod_active", "high_active", "lowmodhigh_active", "modhigh_active"), sep = "_"), 
             paste("ipaq", c("inactive", "low_active", "mod_active", "high_active", "lowmodhigh_active", "modhigh_active"), sep = "_"))
tracking <- data.table('modelable_entity_id' = me_ids, 'modelable_entitiy_name' = me_name)

# Read in data
all_data <- fread(sprintf("FILEPATH", data_dir, 1, 1))

# Data operations
# 1) age split the data
# 2) sex split the data
# 3) add sex_id and age_group_id
as_split <- function(all_data){
  dt <- all_data[modelable_entity_id %in% me_ids] # only keep the data from granular levels of activity
  dt[,split_id := 1:.N]
  
  # 1) age split
  age_data <- copy(dt)
  age_data[age_start < 25 & age_end < 25, keep := 0] # Identify rows with age start and age end less than 25
  age_data[is.na(keep), keep := 1]
  age_data <- age_data[keep == 1]
  age_data$keep <- NULL
  # For rows where age start and age end are close to the GBD age bins, add 1 to age end
  age_data[age_end == age_start + 4, age_end := age_end + 1]
  age_data[, age_split := ifelse(age_end == age_start + 5, 0, 1)] # identify rows with GBD age bins
  
  age_split <- age_data[age_split == 1]
  age_split[age_start < 25, age_start := 25] # For non-standard age data with age start less than 25, set age start to 25
  age_split[age_start > 80, age_start := 80] # Only going to predict for age group 21
  age_split[age_end > 80, age_end := 125]
  # Round to nearest 5 year age bin
  age_split[,age_start := round_any(age_start, 5, floor)][,age_end := round_any(age_end, 5, ceiling)]
  age_split[age_end != 125, n.age := (age_end - age_start)/5][age_end == 125, n.age := (80 - age_start)/5 + 1]
  
  # Set up rows for splitting
  expanded <- rep(age_split$split_id, age_split$n.age) %>% data.table('split_id' = .)
  age_split <- merge(age_split, expanded, by = "split_id", all = T)
  age_split[, age.rep := 1:.N - 1, by = "split_id"]
  age_split[, age_start := age_start + age.rep * 5]
  age_split[age_start != 80, age_end := age_start + 5][age_start == 80, age_end := 125]
  
  # Expand for sex
  age_split[sex == "Male", sex_id := 1][sex == "Female", sex_id := 2][sex == "Both", sex_id := 3]
  age_split[, n.sex := ifelse(sex == "Both", 2, 1)]
  age_split[, sex_split_id := paste0(split_id, "_", age_start)]
  expanded <- rep(age_split$sex_split_id, age_split$n.sex) %>% data.table("sex_split_id" = .)
  age_split <- merge(age_split, expanded, by = "sex_split_id", all = T)
  age_split[sex_id == 3, sex_id := 1:.N, by = "sex_split_id"]
  age_split[,sex := ifelse(sex_id == 1, "Male", "Female")]
  age_split <- age_split[,(names(age_data)), with = F]
  
  # Get final dataframe
  out <- age_data[age_split == 0]
  out[age_start >= 80, `:=` (age_start = 80, age_end = 125)] # Only going to predict for age group 21
  out <- rbind(out, age_split)
  
  # Final touch ups for sex ID and age group ID
  out[,sex_id := ifelse(sex == "Male", 1, 2)]
  out <- merge(out, ages[,.(age_group_id, age_start, age_end)], by = c("age_start", "age_end"))
  
  return(out)
}

all_data_split <- as_split(all_data)

##################################################
# Apply coefficients from mixed effects regression to the data
##################################################

# Read in the model objects
inactive <- readRDS(paste0(work_dir, "FILEPATH"))
low_active <- readRDS(paste0(work_dir, "FILEPATH"))
mod_active <- readRDS(paste0(work_dir, "FILEPATH"))
high_active <- readRDS(paste0(work_dir, "FILEPATH"))
lowmodhigh_active <- readRDS(paste0(work_dir, "FILEPATH"))
modhigh_active <- readRDS(paste0(work_dir, "FILEPATH"))
# Incorporate uncertainty in the fixed effects coefficients of the 6 models

apply_draws <- function(model, num_draws, df){
  set.seed(3197)
  
  fe <- fixef(model)
  cov <- vcov(inactive)
  
  # Get draws of the fixed effects
  draws <- data.table(mvrnorm(num_draws, fe, cov), 'draw' = 1:num_draws)
  
  # Duplicate super-region random effects
  sr_re <- data.table(t(ranef(model)$super_region_name))
  setnames(sr_re, names(sr_re), c("sr_31", "sr_64", "sr_103", "sr_137", "sr_158", "sr_4", "sr_166"))
  sr_re <- sr_re[rep(seq_len(nrow(sr_re)), each = num_draws)]
  
  # Duplicate estimator random effects
  est_re <- data.table(t(ranef(model)$estimator))
  est_re <- est_re[rep(seq_len(nrow(est_re)), each = num_draws)]
  
  # Combine the dataframe of all draws of regression
  draws <- cbind(draws, sr_re, est_re)
  # changing the names of the coefficients for easier application
  setnames(draws, c("(Intercept)", grep("mean_", names(draws), value = T)), c("int", "act_coef"))
  
  ######
  # Replicate the data
  ######
  data <- copy(df)
  orig.cols <- names(data)
  
  data[,split_id := 1:.N]
  expanded <- rep(data$split_id, num_draws) %>% data.table('split_id' = .)
  data <- merge(data, expanded, by = "split_id", all.x = T, allow.cartesian = T)
  data[, draw := 1:num_draws, by = "split_id"]
  
  # Merge on the draws
  data <- merge(data, draws, by = "draw", all = T)
  
  # Create dummy variables
  data[,dummy_31 := ifelse(super_region_id == 31, 1, 0)]
  data[,dummy_64 := ifelse(super_region_id == 64, 1, 0)]
  data[,dummy_103 := ifelse(super_region_id == 103, 1, 0)]
  data[,dummy_137 := ifelse(super_region_id == 137, 1, 0)]
  data[,dummy_158 := ifelse(super_region_id == 158, 1, 0)]
  data[,dummy_4 := ifelse(super_region_id == 4, 1, 0)]
  data[,dummy_166 := ifelse(super_region_id == 166, 1, 0)]
  
  data[,sex_dummy := ifelse(sex_id == 2, 1, 0)]
  
  data[,gpaq_dummy := ifelse(questionnaire_type == "gpaq", 1, 0)]
  data[,ipaq_dummy := ifelse(questionnaire_type == "ipaq", 1, 0)]
  
  # Apply the coefficients
  message("Calculating the predicted MET for all draws.")
  data[,log_MET := int + act_coef*mean + sex_dummy*sex_id2 + age_mid_val*age_mid 
       + dummy_31*sr_31 + dummy_64*sr_64 + dummy_103*sr_103 + dummy_137*sr_137 
       + dummy_158*sr_158 + dummy_4*sr_4 + dummy_166*sr_166 + gpaq_dummy*gpaq + ipaq_dummy*ipaq]
  
  # Get the mean across split_id
  message("Calculating the mean, lower, and upper across draws.")
  data[,mean_log_MET := mean(log_MET), by = "split_id"]
  data[,low_log_MET := lapply(.SD, function(x) quantile(x, probs = 0.025)), .SDcols = "log_MET", by = "split_id"]
  data[,upp_log_MET := lapply(.SD, function(x) quantile(x, probs = 0.975)), .SDcols = "log_MET", by = "split_id"]
  
  final <- unique(data[,c(orig.cols, "mean_log_MET", "low_log_MET", "upp_log_MET"), with = F])
  
  if(nrow(final) != nrow(df)){
    stop(message("The rows of the returned data frame are not the same as the original data frame."))
  } else (
    message("Success!")
  )
  
  return(final)
}

predict_mets <- function(model, measure, df){
  
  
  if(! "sex_id" %in% names(df)){stop(message("Looks like your dataframe does not have sex_id column."))}
  df$sex_id <- as.factor(df$sex_id)
  if(measure == "inactive"){
    out <- df[modelable_entity_id %in% c(9356, 23714)]
    # Make new column called mean_inactive (equal to mean)
    out[, mean_inactive := mean]
    # Make age_mid column
    out[, age_mid := (age_end + age_start)/2]
    # Make estimator column
    out[, estimator := questionnaire_type]
    out$estimator <- as.factor(out$estimator)
    # Use predict interval
    est <- predictInterval(model, out, which = "fixed",  level = 0.95, stat = "mean", seed = 3197)
    # Append
    out <- cbind(out, est)
    out$mean_inactive <- NULL # Drop the new column made
  }

  if(measure == "low"){
    out <- df[modelable_entity_id %in% c(9357, 23715)]
    # Make new column called mean_lowactive (equal to mean)
    out[, mean_lowactive := mean]
    # Make age_mid column
    out[, age_mid := (age_end + age_start)/2]
    # Make estimator column
    out[, estimator := questionnaire_type]
    out$estimator <- as.factor(out$estimator)
    # Use predict interval
    est <- predictInterval(model, out, which = "fixed",  level = 0.95, stat = "mean", seed = 3197)
    # Append
    out <- cbind(out, est)
    out$mean_lowactive <- NULL # Drop the new column made
  }
  
  if(measure == "mod"){
    out <- df[modelable_entity_id %in% c(9358, 23716)]
    # Make new column called mean_modactive (equal to mean)
    out[, mean_modactive := mean]
    # Make age_mid column
    out[, age_mid := (age_end + age_start)/2]
    # Make estimator column
    out[, estimator := questionnaire_type]
    out$estimator <- as.factor(out$estimator)
    # Use predict interval
    est <- predictInterval(model, out, which = "fixed",  level = 0.95, stat = "mean", seed = 3197)
    # Append
    out <- cbind(out, est)
    out$mean_modactive <- NULL # Drop the new column made
  }
  
  if(measure == "high"){
    out <- df[modelable_entity_id %in% c(9359, 23717)]
    # Make new column called mean_highactive (equal to mean)
    out[, mean_highactive := mean]
    # Make age_mid column
    out[, age_mid := (age_end + age_start)/2]
    # Make estimator column
    out[, estimator := questionnaire_type]
    out$estimator <- as.factor(out$estimator)
    # Use predict interval
    est <- predictInterval(model, out, which = "fixed",  level = 0.95, stat = "mean", seed = 3197)
    # Append
    out <- cbind(out, est)
    out$mean_highactive <- NULL # Drop the new column made
  }
  
  if(measure == "lowmodhigh"){
    out <- df[modelable_entity_id %in% c(9360, 23718)]
    # Make new column called mean_lowmodhighactive (equal to mean)
    out[, mean_lowmodhighactive := mean]
    # Make age_mid column
    out[, age_mid := (age_end + age_start)/2]
    # Make estimator column
    out[, estimator := questionnaire_type]
    out$estimator <- as.factor(out$estimator)
    # Use predict interval
    est <- predictInterval(model, out, which = "fixed",  level = 0.95, stat = "mean", seed = 3197)
    # Append
    out <- cbind(out, est)
    out$mean_lowmodhighactive <- NULL # Drop the new column made
  }
  
  if(measure == "modhigh"){
    out <- df[modelable_entity_id %in% c(9361, 23719)]
    # Make new column called mean_modhighactive (equal to mean)
    out[, mean_modhighactive := mean]
    # Make age_mid column
    out[, age_mid := (age_end + age_start)/2]
    # Make estimator column
    out[, estimator := questionnaire_type]
    out$estimator <- as.factor(out$estimator)
    # Use predict interval
    est <- predictInterval(model, out, which = "fixed",  level = 0.95, stat = "mean", seed = 3197)
    # Append
    out <- cbind(out, est)
    out$mean_modhighactive <- NULL # Drop the new column made
  }
  
  return(out)
}

# Merge on SR/reg/loc for random effects
all_data_split <- merge(all_data_split, locs[,.(location_id, super_region_name, region_name, ihme_loc_id)], by = "location_id")

# Predict with predict_mets function
inactive_pred <- predict_mets(inactive, "inactive", all_data_split)
low_pred <- predict_mets(low_active, "low", all_data_split)
mod_pred <- predict_mets(mod_active, "mod", all_data_split)
high_pred <- predict_mets(high_active, "high", all_data_split)
lowmodhigh_pred <- predict_mets(lowmodhigh_active, "lowmodhigh", all_data_split)
modhigh_pred <- predict_mets(modhigh_active, "modhigh", all_data_split)

t_data <- rbind(inactive_pred, low_pred, mod_pred, high_pred, lowmodhigh_pred, modhigh_pred)

# Identify the duplicated observations
keep_cols <- c("nid", "location_id", "year_start", "year_end", "age_start", "age_end", "age_group_id", "sex", "sex_id", "mean",
               "sample_size", "urbanicity_type", "seq", "modelable_entity_id", "questionnaire_type", "split_id", "fit", "upr", "lwr")
uniq_cols <- c("nid", "location_id", "year_start", "age_group_id", "sex_id", "questionnaire_type", "urbanicity_type", "modelable_entity_id") # only want 1 unique observation for these identifiers
id_cols <- c("nid", "location_id", "year_start", "age_group_id", "sex_id", "questionnaire_type", "urbanicity_type") # split by these columns to take the mean of predictions

t_data <- t_data[, keep_cols, with = F]
t_data <- unique(t_data, by = uniq_cols)
t_data[, id:= 1:.N, by = id_cols]
unique(t_data$id) # Greatest should be 6 since there could be 6 ME IDs for a given source

# Take the mean across fit if id is not 1
t_data[, log_val := mean(fit), by = id_cols]
t_data[, log_lwr := mean(lwr), by = id_cols]
t_data[, log_upr := mean(upr), by = id_cols]

final <- unique(t_data[,.(nid, location_id, year_start, year_end, age_start, age_end, age_group_id, sex_id, sex, 
                         sample_size, urbanicity_type, questionnaire_type, log_val, log_lwr, log_upr)])

# Calculate variance in log space and use delta transformation to get variance in normal space
final[, log_sd := (log_upr - log_val)/(2*qnorm(0.975))]

final <- cbind(final, delta_transform(mean = final$log_val, sd = final$log_sd, transformation = "log_to_linear"))
final$variance <- (final$sd_linear)^2
# Exponentiate log_val to get mean MET
final[,val := mean_linear][, mean_linear := NULL]
final[,lower := exp(log_lwr)]
final[,upper := exp(log_upr)]

# Add measure, year_id, seq, underlying_nid, is_outlier, sample_size
final[,measure := "continuous"]
final[, `:=` (orig_year_start = year_start, orig_year_end = year_end)]
final[year_start == year_end, year_id := year_start]
final[year_start != year_end, year_id := year_start + round((year_end - year_start)/2)]
final[year_start != year_end, `:=` (year_start = year_id, year_end = year_id)]
final[,underlying_nid := NA]
final[,is_outlier := 0]
final[,seq := NA]

# Applying self-report to measured coefficient to mean and variance columns
sr_coef <- 1/1.71
setnames(final, c("val", "lower", "upper", "variance"), c("sr_val", "sr_lower", "sr_upper", "sr_variance"))
final[, `:=` (val = sr_val * sr_coef, variance = sr_variance * sr_coef^2)]

# Save it
if(!dir.exists(paste0(data_dir, "FILEPATH", processing_version_id, "/"))){
  dir.create(paste0(data_dir, "FILEPATH", processing_version_id))
  save_dir <- paste0(data_dir, "FILEPATH", processing_version_id, "/")
} else {
  save_dir <- paste0(data_dir, "FILEPATH", processing_version_id, "/")
}

fwrite(final, paste0(save_dir, "FILEPATH"))
