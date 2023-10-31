 ################################################################################
## DESCRIPTION: Using saved regression coefficients from XX (NEW SCRIPT NAME), predicts mean BMI at draw level ##
## INPUTS: Draws of prev ow (st-gpr), draws of prev ob (st-gpr), location-specific coefficients ##
## OUTPUTS: Draws of mean bmi ##
## AUTHOR: 
## Replacing GBD 2017 stata script from repo: `08_bmi_regression_parallel.do``
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
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, 'FILEPATH'))

## SCRIPT SPECIFIC FUNCTIONS

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "parent directory where results will be stored",
                    default = 'FILEPATH', type = "character")
parser$add_argument("--anthro_me_type", help = "me for run bmi, height, weight",
                    default = 'bmi', type = "character")
parser$add_argument("--anthro_version_id", help = "version number for whole run",
                    default = 2, type = "integer")
parser$add_argument("--mean_bmi_coeff_version_id", help = "version number of mean bmi regression to use",
                    default = 1, type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

data_dir <- sprintf(data_dir, anthro_me_type)

## Load configurations for run
load_config(paste0(code_dir, 'FILEPATH'), c('gbd', anthro_me_type))

## LOAD TASK INFORMATION
task_id <- ifelse(interactive(), 7, Sys.getenv("SGE_TASK_ID"))
task_map <- fread(sprintf('%s/v%d/task_map.csv', data_dir, anthro_version_id))
loc_id <- task_map[task_num == task_id, location_id]
loc_map <- fread(sprintf('%s/v%d/loc_map.csv', data_dir, anthro_version_id))[location_id == loc_id]

## LOAD RELEVANT DATA
id_cols <- c('location_id', 'age_group_id', 'sex_id', 'year_id')

## LOAD RELEVANT DATA
ob_df <- fread(sprintf('%s/v%d/prev_ob/%d.csv', data_dir, anthro_version_id, loc_id))
ow_df <- fread(sprintf('%s/v%d/prev_ow/%d.csv', data_dir, anthro_version_id, loc_id))

coeff_df <- readRDS(sprintf('%s/FILEPATH/v%d/%d.RDS', data_dir, mean_bmi_coeff_version_id, loc_map$region_id))[location_id == loc_id & age_group_id > 8]

## BODY

# Merge together all data
setnames(ob_df, 'value', unique(ob_df$measure)); ob_df[, c('anthro_version_id', 'measure', 'measure_id', 'metric_id') := NULL]
setnames(ow_df, 'value', unique(ow_df$measure)); ow_df[, c('anthro_version_id', 'measure', 'measure_id', 'metric_id') := NULL]

prev_df <- merge(ob_df, ow_df, by = c(id_cols, 'draw')); rm(ob_df, ow_df)
pred_df <- merge(coeff_df, prev_df, by = c('location_id', 'age_group_id', 'sex_id', 'draw'))

# Predict mean bmi (regression fit on log(mean_bmi), so need to exponentiate result)
# Line 56 from original stata script
# gen draw_`x' = intercept`x' + (overweight_`x'*ow_coeff`x') + (obesity_`x'*ob_coeff`x') + (re1*overweight_`x') + (re2*obesity_`x') + re3 + (re4*overweight_`x') + (re5*obesity_`x') + re6 + (re7*overweight_`x') + (re8*obesity_`x') + re9

pred_df[, log_mean_bmi := intercept + prev_ow*ow_coeff + prev_ob*ob_coeff + prev_ow*re1 + prev_ob*re2 + re3 + prev_ow*re4 + prev_ob*re5 + re6 + prev_ow*re7 + prev_ob*re8 + re9]
pred_df[, mean_bmi := exp(log_mean_bmi)]

## PREP FOR SAVE
pred_df[, grep('location|age|sex|year|draw|^mean_bmi$', names(pred_df), invert = T, value = T) := NULL]
pred_df[, c('anthro_version_id', 'measure', 'measure_id', 'metric_id') := .(anthro_version_id, 'mean_bmi', 19, 3)]
setnames(pred_df, 'mean_bmi', 'value')
setcolorder(pred_df, c('anthro_version_id', 'location_id', 'age_group_id', 'sex_id', 'year_id', 'measure', 'measure_id', 'metric_id', 'draw', 'value'))

## VALIDATE
anthro.validate(pred_df, loc_id, age_ids = detailed_age_ids[!detailed_age_ids %in% c(34, 6:8)], sex_ids = detailed_sex_ids, year_ids =  prod_year_ids, prod_draws, measures = 'mean_bmi')

## SAVE OUTPUTS
write.csv(pred_df, sprintf('%s/v%d/FILEPATH/%d.csv', data_dir, anthro_version_id, loc_id), row.names = F)