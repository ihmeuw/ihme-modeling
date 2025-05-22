################################################################################
## DESCRIPTION: Run separate LMER regression models to predict relationship 
## between activity category and mean MET using scaled training dataset ##
## INPUTS: Scaled training dataset  ##
## OUTPUTS: Coefficients to apply to activity category proportion to predict mean METs ##
## AUTHOR:  ##
## DATE:
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

## Load dependencies
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, 'FILEPATH'))
library(lme4)
source('FILEPATH')
source('FILEPATH')

# Parse args
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "parent directory where data will be pulled from",
                    default = 'FILEPATH/', type = "character")
parser$add_argument("--save_dir", help = "parent directory where results will be stored",
                    default = 'FILEPATH', type = "character")
parser$add_argument("--processing_version_id", help = "version number for whole run",
                    default = 1, type = "integer")
parser$add_argument("--processing_version_note", help = "note for changes in version ",
                    default = 'v1 for GBD 2020', type = "character")
parser$add_argument("--cycle_year", help = "current GBD cycle",
                    default = "gbd_2020", type = "character")
parser$add_argument("--decomp_step", help = "decomp step we're uploading for",
                    default = "iterative", type = "character")

args <- parser$parse_args()
list2env(args, environment()); rm(args)

# Read in helper datasets
locs <- get_location_metadata(22, decomp_step = "iterative")
ages <- fread(paste0(h, "FILEPATH")) # Must have all ages in h drive
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

# Read in training data
training  <- fread(paste0(data_dir, "FILEPATH"))

# Merge on some meta data
training <- merge(training, locs[,.(location_id, super_region_name, region_name, ihme_loc_id)], by = "location_id")
training <- merge(training, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")

# Calculate age mid
training[age_start != 80 , age_mid := age_start + (age_end-age_start)/2]
training[age_start == 80, age_mid := 82.5] # Since age end for age group 21 is 125, set age mid of this group to 82.5 so that the age covariate makes sense

# Make sure super-region names, sex_id, and estimator are factors
training$super_region_name <- as.factor(training$super_region_name)
training$region_name <- as.factor(training$region_name)
training$ihme_loc_id <- as.factor(training$ihme_loc_id)
training$sex_id <- as.factor(training$sex_id)
# Large discrepancy between GPAQ and IPAQ sources (10312 GPAQ and 2062 IPAQ)
# Also, no GPAQ data for Central Europe, Eastern Europe and Central Asia SR so will put random effect on estimator
training$estimator <- as.factor(training$estimator)

# Log transform mean METs to make response behave more normally
training[,log_MET := log(data)]

# Run the LMER models
lowmodhigh_active <- lmer(log_MET ~ mean_lowmodhighactive + sex_id + age_mid + (1|super_region_name/region_name/ihme_loc_id) + estimator + estimator*mean_lowmodhighactive, data = training)

modhigh_active <- lmer(log_MET ~ mean_modhighactive + sex_id + age_mid + (1|super_region_name/region_name/ihme_loc_id) + estimator + estimator*mean_modhighactive, data = training)

inactive <- lmer(log_MET ~ mean_inactive + sex_id + age_mid + (1|super_region_name/region_name/ihme_loc_id) + estimator + estimator*mean_inactive, data = training)

low_active <- lmer(log_MET ~ mean_lowactive + sex_id + age_mid + (1|super_region_name/region_name/ihme_loc_id) + estimator + estimator*mean_lowactive, data = training)

mod_active <- lmer(log_MET ~ mean_modactive + sex_id + age_mid + (1|super_region_name/region_name/ihme_loc_id) + estimator + estimator*mean_modactive, data = training)

high_active <- lmer(log_MET ~ mean_highactive + sex_id + age_mid + (1|super_region_name/region_name/ihme_loc_id) + estimator + estimator*mean_highactive, data = training)

# Function to create activity coefficients
make_coef_grid <- function(model){
  
  # Set up the grid template
  dt <- data.table(expand.grid(estimator = c("gpaq", "ipaq"), sex_id = c(1:2), super_region_name = unique(training$super_region_name)))
  
  # Extract fixed effects first
  dt[, int := fixed.effects(model)[1]] # intercept
  dt[, act_coef := fixed.effects(model)[2]]
  dt[sex_id == 1, sex_coef := 0][sex_id == 2, sex_coef := fixed.effects(model)[3]]
  dt[, age_coef := fixed.effects(model)[4]]
  
  # Extract SR random effects
  sr_re <- data.table(random.effects(model)$super_region_name, keep.rownames = T)
  setnames(sr_re, names(sr_re), c("super_region_name", "sr_re"))
  dt <- merge(dt, sr_re, by = "super_region_name")
  
  # Extract estimator random effects
  est_re <- data.table(random.effects(model)$estimator, keep.rownames = T)
  setnames(est_re, names(est_re), c("estimator", "est_re"))
  dt <- merge(dt, est_re, by = "estimator")
  
  # Return
  return(dt)
}

inactive_coefs <- make_coef_grid(inactive)
lowactive_coefs <- make_coef_grid(low_active)
modactive_coefs <- make_coef_grid(mod_active)
highactive_coefs <- make_coef_grid(high_active)

# Save the coefficients
if(!dir.exists(paste0(save_dir, "lmer_predictions"))){
  dir.create(paste0(save_dir, "lmer_predictions"))
}

# Save the model objects
saveRDS(inactive, paste0(save_dir, "FILEPATH"))
saveRDS(low_active, paste0(save_dir, "FILEPATH"))
saveRDS(mod_active, paste0(save_dir, "FILEPATH"))
saveRDS(high_active, paste0(save_dir, "FILEPATH"))
saveRDS(lowmodhigh_active, paste0(save_dir, "FILEPATH"))
saveRDS(modhigh_active, paste0(save_dir, "FILEPATH"))

