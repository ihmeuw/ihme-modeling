################################################################################
## DESCRIPTION: Self-report adjustment (systematic error) for anthropometric data (height, weight, BMI, and derivatives) ##
## INPUTS: Age-sex split bundle version data ##
## OUTPUTS: Adjusted bundle version data ready for upload to crosswalk version ##
## AUTHOR: 
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
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") paste0("FILEPATH", user, "FILEPATH")

## LOAD DEPENDENCIES
source("FILEPATH")
source("FILEPATH")
source(paste0(code_dir, 'FILEPATH'))
library(msm, lib.loc = paste0(h, "FILEPATH"))
library(reticulate, lib.loc = paste0(h, "FILEPATH"))
library(crosswalk, lib.loc = "FILEPATH")
source(paste0(code_dir, 'FILEPATH')) # loads `match.data`` and `gen.ko`` functions
## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--processing_version_id", help = "Internal version ID of data processing run",
                    default = 1, type = "integer")
parser$add_argument("--cycle_year", help = "current GBD cycle",
                    default = "gbd_2020", type = "character")
parser$add_argument("--ow_bundle_version_id", help = "Prevalence overweight bundle version id being processed",
                    default = 26018, type = "integer")
parser$add_argument("--ob_bundle_version_id", help = "Proportion obese bundle version id being processed",
                    default = 26021, type = "integer")
parser$add_argument("--fuzzy_match", help = "Whether or not to create matched dataset based on exact year match",
                    default = T, type = "logical")
parser$add_argument("--year_match_window", help = "+/- window of years across which to match gs and non gs data",
                    default = 2, type = "integer")

args <- parser$parse_args()
list2env(args, environment()); rm(args)

# Functions from old script
encode_one_hot <- function(df, col, reference_cat){
  
  if(!is.character(df[, get(col)])) {
    
    message(sprintf("Converting %s to character variable to generate factors", col))
    df[, paste0('copy_', col) := as.character(get(col))]
    
  }
  
  encoded <- data.table(unique(df[, get(paste0('copy_', col))])) %>% setnames(., names(.), paste0('copy_', col))
  
  for(lev in encoded[, get(paste0('copy_', col))]) encoded[, paste0(col,'_', lev) := as.integer(get(paste0('copy_', col)) == lev)]
  
  ref_col <- grep(sprintf('_%d$', reference_cat), names(encoded), value = T)
  encoded[, (ref_col) := NULL]
  
  encoded <- merge(df[, c(col, paste0('copy_', col)), with = F], encoded, by = paste0('copy_', col))
  encoded[, paste0('copy_', col) := NULL]
  df[, paste0('copy_', col) := NULL]
  
  return(encoded)
  
}

unencode_one_hot <- function(df, one_hot_map, var){
  
  encoded_cols <- grep(sprintf('%s_', var), names(df), value = T)
  unencoded <- merge(df, one_hot_map, by = encoded_cols)
  unencoded[, (encoded_cols) := NULL]
  return(unencoded)
}


## LOAD RELEVANT DATA
locs <- get_location_metadata(22, decomp_step = "iterative")
ages <- fread(paste0(h, "FILEPATH")) # All age data frame from shiny table

## Read in adjustment factors (differences in logit space for prev ow and prev ob)
## Model 1 refers to the sex-specific model fit with the results from the both sex MR-BRT model as priors and only untrimmed data from the both sex model
ow_adj <- fread('FILEPATH')[model == 1, .(region_id, age_group_id, sex_id, pred, pred_var)]
ob_adj <- fread('FILEPATH')[model == 1, .(region_id, age_group_id, sex_id, pred, pred_var)]

ow_data <- fread(sprintf('FILEPATH', cycle_year, ow_bundle_version_id))
ob_data <- fread(sprintf('FILEPATH', cycle_year, ob_bundle_version_id))

# Adjustment applied to age 15+ even though fit on only data from 20+ (given switch in definition of ow/ob for under 20). Current adjustment doesn't vary by age so copy adjustment factors for age 20

ow_adj <- rbind(ow_adj,
                ow_adj[age_group_id == 9, .(age_group_id = 8, sex_id, region_id, pred, pred_var)], use.names = T, fill = T)

ob_adj <- rbind(ob_adj,
                ob_adj[age_group_id == 9, .(age_group_id = 8, sex_id, region_id, pred, pred_var)], use.names = T, fill = T)

# Mark self-reported data in children under 15 as is_outlier = 1
ow_data[age_end <= 15 & diagnostic == "self-report", is_ouliter := 1]
ob_data[age_end <= 15 & diagnostic == "self-report", is_ouliter := 1]

# Mark rows with sample size under 10 as is_outlier = 1
ow_data[sample_size < 10, is_outlier := 1]
ob_data[sample_size < 10, is_outlier := 1]

# Drop outliers and under 20
ow_not_adj <- ow_data[is_outlier == 1| age_end <= 15]
ob_not_adj <- ob_data[is_outlier == 1| age_end <= 15]

ow_long <- ow_data[is_outlier == 0 & age_end > 15]
ob_long <- ob_data[is_outlier == 0 & age_end > 15]

# Deal with rows where mean is 0 and 1
const <- 0.005
ow_long[val == 0, val := const][val == 1, val := 1-const]
ob_long[val == 0, val := const][val == 1, val := 1-const]

# Make SE column
ow_long[, se := sqrt((val*(1-val))/sample_size)]
ob_long[, se := sqrt((val*(1-val))/sample_size)]

# Make variance column
ow_long[, variance := se^2]
ob_long[, variance := se^2]

# Merge on the coefficients
ow_long <- merge(ow_long, locs[, .(location_id, region_id)], by = 'location_id')
ob_long <- merge(ob_long, locs[, .(location_id, region_id)], by = 'location_id')

merge_ids <- c('age_group_id', 'sex_id', 'region_id')
ow_long <- merge(ow_long, ow_adj, by = merge_ids, all.x = T)
ob_long <- merge(ob_long, ob_adj, by = merge_ids, all.x = T)

# Apply adjustment and uncertainty to data. Needs to be done in logit space. 
# using the crosswalk package's delta_transform function

ow_long <- cbind(ow_long, crosswalk::delta_transform(mean = ow_long$val, sd = ow_long$se, transformation = "linear_to_logit"))
ow_long[, logit_adj_data := ifelse(diagnostic == 'self-report', mean_logit - pred, mean_logit)]
ow_long[, logit_adj_var := ifelse(diagnostic == 'self-report', sd_logit^2 + pred_var, sd_logit^2)]
ow_long[, `:=` (mean_logit = NULL, sd_logit = NULL)]
ow_long <- cbind(ow_long, crosswalk::delta_transform(mean = ow_long$logit_adj_data, sd = sqrt(ow_long$logit_adj_var), transformation = "logit_to_linear"))


ob_long <- cbind(ob_long, crosswalk::delta_transform(mean = ob_long$val, sd = ob_long$se, transformation = "linear_to_logit"))
ob_long[, logit_adj_data := ifelse(diagnostic == 'self-report', mean_logit - pred, mean_logit)]
ob_long[, logit_adj_var := ifelse(diagnostic == 'self-report', sd_logit^2 + pred_var, sd_logit^2)]
ob_long[, `:=` (mean_logit = NULL, sd_logit = NULL)]
ob_long <- cbind(ob_long, crosswalk::delta_transform(mean = ob_long$logit_adj_data, sd = sqrt(ob_long$logit_adj_var), transformation = "logit_to_linear"))

# Set some names
setnames(ow_long, c("val", "se", "variance", "mean_linear", "sd_linear"), c("orig_val", "orig_se", "orig_variance", "val", "se"))
setnames(ob_long, c("val", "se", "variance", "mean_linear", "sd_linear"), c("orig_val", "orig_se", "orig_variance", "val", "se"))

# Recalculate the variance and drop the unneccessary cols
ow_long[, variance := se^2]
ob_long[, variance := se^2]

ow_long[, `:=` (pred = NULL, pred_var = NULL, logit_adj_data = NULL, logit_adj_var = NULL, region_id = NULL)]
ob_long[, `:=` (pred = NULL, pred_var = NULL, logit_adj_data = NULL, logit_adj_var = NULL, region_id = NULL)]

# Make indicator to indicate that the data were adjusted
ow_long[, cv_sr_adj := ifelse(diagnostic == "self-report", 1, 0)]
ob_long[, cv_sr_adj := ifelse(diagnostic == "self-report", 1, 0)]

ow_not_adj[, cv_sr_adj := 0]
ob_not_adj[, cv_sr_adj := 0]
# Append the non-adjusted data back on
ow_clean <- rbind(ow_not_adj, ow_long, fill = TRUE)
ob_clean <- rbind(ob_not_adj, ob_long, fill = TRUE)

if(nrow(ow_clean) != nrow(ow_data)){message("Looks like some rows are missing.")}
if(nrow(ob_clean) != nrow(ob_data)){message("Looks like some rows are missing.")}

# Save dataset
fwrite(ow_clean, sprintf('FILEPATH', cycle_year, ow_bundle_version_id), row.names = F)
fwrite(ob_clean, sprintf('FILEPATH', cycle_year, ob_bundle_version_id), row.names = F)