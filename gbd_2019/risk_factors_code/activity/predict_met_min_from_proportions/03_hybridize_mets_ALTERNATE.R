################################################################################
## DESCRIPTION: Hybridizes rf and xgb draw level predictions of mets within and across activity variants##
## INPUTS: Draws of total mets,  GPAQ, IPAQ x xgb, rf models ##
## OUTPUTS: Hybridized draws of total mets saved by locations ##
## AUTHOR:  ##
## DATE: 28 October 2019 ##
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- FILEPATH
code_dir <- if (os == "Linux") FILEPATH else if (os == "Windows") ""

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH/primer.R'))
source(paste0(code_dir, 'FILEPATH/activity_utilities.R'))
library(stringr)
library(dplyr)

## SCRIPT SPECIFIC FUNCTIONS

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Location ID to be rescaled",
                    default = 'FILEPATH', type = "character")
parser$add_argument("--activity_version_id", help = "Location ID to be rescaled",
                    default = 1, type = "integer")
parser$add_argument("--resub", help = "Whether or not this job is being resubmitted",
                    default = 0, type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

## Load configurations for run
load_config(paste0(code_dir, 'FILEPATH/00_master_config.ini'), c('gbd', 'activity', 'demo_validation'))

## LOAD TASK INFORMATION
task_id <- ifelse(interactive(), 39, Sys.getenv("SGE_TASK_ID"))
task_id <- as.integer(task_id)
task_map <- fread(sprintf('%s/v%d/task_map.csv', data_dir, activity_version_id))
task_var <- ifelse(!as.logical(resub), 'task_num', 'resub_task_num')
loc_id <- task_map[get(task_var) == task_id, location_id]
loc_map <- fread(sprintf('%s/v%d/loc_map.csv', data_dir, activity_version_id))[location_id == loc_id]
version_map <- fread(paste0(code_dir, 'FILEPATH/activity_versions.csv'))[activity_version_id == get("activity_version_id", .GlobalEnv)]

## LOAD RELEVANT DATA
ml_output_dir <- FILEPATH
draw_files <- c(sprintf('%s/predicting_gpaq/with_gpaq/draws/rf_unseen_predictions_draw%s.csv', ml_output_dir, task_id - 1),
                sprintf('%s/predicting_gpaq/with_gpaq/draws/xgb_unseen_predictions_draw%s.csv', ml_output_dir, task_id - 1),
                sprintf('%s/predicting_ipaq/with_ipaq/draws/rf_unseen_predictions_draw%s.csv', ml_output_dir, task_id - 1),
                sprintf('%s/predicting_ipaq/with_ipaq/draws/xgb_unseen_predictions_draw%s.csv', ml_output_dir, task_id - 1))
draws <- mclapply(draw_files, function(f){
  
  message(sprintf('Reading in draws saved in %s, (%d of %d), relabeling to loc_id %d', basename(f), grep(f, draw_files), length(draw_files), loc_id))
  start <- Sys.time()
  df <- fread(f)
  df <- filter(df, location_id == loc_id, draw != 10000)    # Removing the filler values added in 01_location_specific_reformatting.R
  df <- data.table(df)
  message(sprintf('Reading in %s took %d seconds', basename(f), round(difftime(Sys.time(), start, unit = 'secs'))))
  
  algorithm <- strsplit(basename(f), '_') %>% unlist %>% .[1]
  dnum <- gsub('[^[:digit:]]', '', basename(f)) %>% as.numeric
  variant <- str_extract(dirname(f), 'predicting_[[:alpha:]]{4}') %>% gsub('predicting_', '', .)
  
  setnames(df, grep('_prediction', names(df), value = T), 'value')
  df[, grep('location_id|age_group_id|sex_id|year_id|value|draw', names(df), invert = T, value = T) := NULL]
  df[, c('activity_version_id','activity_variant', 'algorithm', 'measure', 'measure_id', 'metric_id') := .(activity_version_id, variant, algorithm, 'total_mets', 19, 3)]
  
  setcolorder(df, c('activity_version_id', 'location_id', 'age_group_id', 'sex_id', 'year_id', 'measure', 'measure_id', 'metric_id', 'activity_variant', 'algorithm', 'draw', 'value'))
  
  return(df)
  
  
}, mc.cores = 4) %>% rbindlist


## BODY
hybrid_by_variant <- draws[, .(value = mean(value)), by = eval(grep('algorithm|value', names(draws), invert = T, value = T))]
hybrid_across_variant <- draws[, .(value = mean(value)), by = eval(grep('algorithm|activity_variant|value', names(draws), invert = T, value = T))]

# Combine all
all_mets <- rbind(draws, hybrid_by_variant, hybrid_across_variant, use.names = T, fill = T)

# Break out oldest age group
old_all_mets <- copy(all_mets[age_group_id == 21]) %>% .[, .(age_group_id = c(30:32, 235)), by = eval(grep('age_group_id', names(.), invert = T, value = T))]

# Combine old age groups with rest
all_mets <- rbind(all_mets[age_group_id != 21], old_all_mets, use.names = T, fill = T)

write.csv(all_mets, sprintf('%s/v%d/total_mets/%d.csv', data_dir, activity_version_id, loc_id), row.names = F)
