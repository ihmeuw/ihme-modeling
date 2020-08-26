################################################################################
## DESCRIPTION: Runs save results for all activity related MEs from pipeline ##
## INPUTS: Draws of activity results for specified me  ##
## OUTPUTS: None ##
## AUTHOR:
## DATE: ##
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
library(mvtnorm)
source(paste0(code_dir, 'FILEPATH/primer.R'))
source(paste0(code_dir, 'FILEPATH/activity_utilities.R'))
source('FILEPATH/exp_sd.R')
library(mvtnorm)

## SCRIPT SPECIFIC FUNCTIONS

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Site where data will be stored",
                    default = 'FILEPATH', type = "character")
parser$add_argument("--activity_version_id", help = "Site where data will be stored",
                    default = 2, type = "integer")
parser$add_argument("--mark_best", help = "Whether or not to mark results as best",
                    default = 1, type = "integer")

args <- parser$parse_args()
list2env(args, environment()); rm(args)


## LOAD DEMOGRAPHICS FOR QUERY AND VALIDATION
load_config(paste0(code_dir, 'FILEPATH'), c('gbd', 'activity'))

## LOAD RELEVANT DATA

## Load run info to generate note string
version_map <- fread(paste0(code_dir, 'FILEPATH'))[activity_version_id == get('activity_version_id', .GlobalEnv)]
version_map[, upload_note := sprintf('%s; linked to internal vers %d', note, activity_version_id)]

message(sprintf('Uploading draws for total mets, from activity version %d', activity_version_id))

data_df <- fread('FILEPATH')
source("FILEPATH")
library(dplyr)
age_data <- get_age_metadata(age_group_set_id = 12)
location_data <- get_location_metadata(location_set_id = 35)
data_df <- merge(data_df, select(location_data, location_id, super_region_name), by = "location_id")
data_df <- merge(data_df, select(age_data, age_group_id, age_group_years_start), by = "age_group_id")
data_df <- mutate(data_df, st_dev = se_total_mets * sqrt(sample_size), mean = data, female = sex_id - 1)
data_df <- data.table(data_df)

data_df[, outlier := (st_dev / mean > 1) | (st_dev / mean < .1)]

## Run mean-sd regressions

sd_mean_mod_mets <- lm(log(st_dev)~log(mean) + age_group_years_start + super_region_name + female, data = data_df[outlier == F])


# Save diagnostic plots
dir.create(sprintf('FILEPATH', activity_version_id))
pdf(sprintf('FILEPATH', activity_version_id), width = 9, height = 6)

ggplot(data = data_df, aes(x = log(mean), y = log(st_dev))) + geom_point(aes(color = outlier)) + stat_smooth(data = data_df[outlier == F], method = "lm", se = F, color = 'black') + theme_minimal()

dev.off()


# gen draws of exp sd
draws_mets <- rmvnorm(n = 1000, sd_mean_mod_mets$coefficients, vcov(sd_mean_mod_mets)) %>% as.data.table 
setnames(draws_mets, 1:2, c('log_beta_0', 'log_beta_1'))
draws_mets[, draw := seq(0, .N-1)]


## Save model objects and draws of coefficients
saveRDS(sd_mean_mod_mets, sprintf('FILEPATH', activity_version_id))
write.csv(draws_mets, sprintf('FILEPATH', activity_version_id), row.names = F)
