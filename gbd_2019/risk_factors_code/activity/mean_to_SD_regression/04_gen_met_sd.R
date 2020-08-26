################################################################################
## DESCRIPTION: Runs save results for all activity related MEs from pipeline ##
## INPUTS: Draws of activity results for specified me  ##
## OUTPUTS: None ##
## AUTHOR: ##
## DATE ##
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") "FILEPATH" else if (os == "Windows") ""

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH/primer.R'))
source(paste0(code_dir, 'FILEPATH/activity_utilities.R'))
source('FILEPATH/exp_sd.R')

## SCRIPT SPECIFIC FUNCTIONS

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Site where data will be stored",
                    default = 'FILEPATH', type = "character")
parser$add_argument("--activity_version_id", help = "Site where data will be stored",
                    default = 2, type = "integer")


args <- parser$parse_args()
list2env(args, environment()); rm(args)


## LOAD DEMOGRAPHICS FOR QUERY AND VALIDATION
load_config(paste0(code_dir, 'FILEPATH'), c('gbd', 'activity'))

## LOAD RELEVANT DATA

loc_map <- fread(sprintf('%s/v%d/loc_map.csv', data_dir, activity_version_id))
task_map <- fread(sprintf('%s/v%d/task_map.csv', data_dir, activity_version_id))
coeff_df <- fread(sprintf('FILEPATH/sd_mean_mets_coeff.csv'))
library(stringr)
library(dplyr)
source("FILEPATH")
names(coeff_df) <- str_replace_all(names(coeff_df), c(" " = "", "-" = "", "," = "", "age_group_years_start" = "age_start"))
age_data <- get_age_metadata(age_group_set_id = 12)
location_data <- get_location_metadata(location_set_id = 35)

mclapply(task_map$location_id, function(loc_id){
  
  message(sprintf('Generating predictions of SD in METs for %s', loc_map[location_id == loc_id, location_name]))
  
  df <- fread(sprintf('%s/v%d/FILEPATH/%d.csv', data_dir, activity_version_id, loc_id))[is.na(activity_variant) & is.na(algorithm)]

  df <- merge(df, coeff_df, by = 'draw')

  df <- merge(df, select(location_data, location_id, super_region_name), by = "location_id")
  df <- merge(df, select(age_data, age_group_id, age_group_years_start), by = "age_group_id")
  df[, value := exp(log(value)*log_beta_1 + log_beta_0 + (sex_id - 1)*female + age_group_years_start*age_start +
                      (super_region_name == "High-income")*super_region_nameHighincome + 
                      (super_region_name == "Latin America and Caribbean")*super_region_nameLatinAmericaandCaribbean +
                      (super_region_name == "North Africa and Middle East")*super_region_nameNorthAfricaandMiddleEast +
                      (super_region_name == "South Asia")*super_region_nameSouthAsia +
                      (super_region_name == "Southeast Asia, East Asia, and Oceania")*super_region_nameSoutheastAsiaEastAsiaandOceania +
                      (super_region_name == "Sub-Saharan Africa")*super_region_nameSubSaharanAfrica)]
  df[, grep('log_beta|activity|algorithm|super_region_name|age_start|female|age_group_years', names(df), value = T) := NULL][, measure := NULL][, draw := paste0('draw_', draw)]                               
  df <- dcast(df, ...~draw, value.var = 'value')
  df[, modelable_entity_id := 18704]
  
  setcolorder(df, c('location_id', 'age_group_id', 'sex_id', 'year_id', 'modelable_entity_id', 'measure_id', 'metric_id', paste0('draw_', 0:999)))
  
  ## SAVE OUTPUTS
  write.csv(df, sprintf('%s/v%d/met_sd/%d.csv', data_dir, activity_version_id, loc_id), row.names = F)
  
}, mc.cores = 20)



