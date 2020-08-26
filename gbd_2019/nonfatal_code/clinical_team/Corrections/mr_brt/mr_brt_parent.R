#############################################################

##Date: 3/4/2019
##Purpose: Master script to submit MR-BRT models
##Notes: Parallelize your submission of the model
##Notes: IMPORTANT: must be run on Rstudio image 3612 or newer
##Updates: 3/12: Run with 20% trim with new code
##Updates: 3/26: Set up to take more arguments where you can run different CFs at bundle or ICG level
## Updates: 3/29: update for qsub on new cluster
###########################################################
# Setup -----
rm(list = ls())

library(mortcore, lib = FILEPATH)
library(tidyverse)
library(data.table)
library(parallel)
library(RMySQL)
library(readr)

source('get_age_metadata.R')

args <- commandArgs(trailingOnly = TRUE)[1]
args <- strsplit(args, ',')
print(args)
run <- args[[1]][1] %>% print()

method <- args[[1]][2] %>% print()
bundle_file <- args[[1]][3] %>% print()

# Validate input args
stopifnot(!is.na(run) & !is.na(method))
if(method=='custom'){
  stopifnot(!is.na(bundle_file))
}

user <- Sys.info()[["user"]]
write_folder <- paste0(FILEPATH)
dir.create(paste0(write_folder,'by_agesex/'), recursive = TRUE)

# Reference file prep -----
# Prep CSVs so that trim worker can populate rows by appending
write_csv(data.frame(date = character(), run = character(), bundle = numeric(), cf = character(), trim = numeric(), sex_cov = logical(), 
                     age_cov = logical(), min_age = numeric(), max_age = numeric(), error = character()), 
          paste0(write_folder, 'mr_brt_run_log.csv'))
write_csv(data.frame(age_start = numeric(), age_group_id = factor(), sex_id = factor(), bundle_id = numeric(), cf = character()), 
          paste0(write_folder, 'cf_estimates.csv'))

# Write preddf and age_groups to folder to reference in trim worker
# # Make prediction dataframes. Make the hospital prediction dataset of age and sex ##
ages <- get_age_metadata(12) %>%
  setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
age_1 <- data.table(age_group_id = 28, age_group_years_start = 0, age_group_years_end = 1)%>%
  setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
ages <- rbind(ages, age_1, fill = TRUE)

age_groups <- ages %>%
  filter(age_group_id > 4) %>%
  mutate(age_midpoint = case_when(
    age_start == 95 ~ 97.5,
    TRUE ~ (age_start + age_end)/2)) %>%
  select(-age_group_weight_value, -age_end)
write_csv(age_groups, paste0(write_folder,'age_groups.csv'))

ages[, index := 1]
sexes <- data.table(sex_id = c(1,2), index = 1)

preddf <- merge(ages, sexes, by = 'index', allow.cartesian = TRUE)
#preddf[, age_start := (age_start + age_end)/2]
preddf[, age_group_weight_value := NULL]
write_csv(preddf, paste0(write_folder,'age_sex_preddf.csv'))


# Get estimates -----
loadEstimates <- function() {
  db_con = fread(paste0(FILEPATH))
  
  # Get estimate ids and bundles
  con <- dbConnect(dbDriver("MySQL"),
                   username = db_con$username,
                   password = db_con$pass,
                   host = db_con$host)
  df <- dbGetQuery(QUERY)
  
  dbDisconnect(con)
  df <- data.table(df)
  df <- df %>%
    mutate(cf = case_when(
      estimate_id == 9 | estimate_id == 4 ~ 'cf3',
      estimate_id == 8 | estimate_id == 3 ~ 'cf2',
      estimate_id == 7 | estimate_id == 2 ~ 'cf1',
    )) %>%
    filter(!is.na(cf)) %>%
    select(-estimate_id)
  
  return(df)
}


#### JOB SUBMISSION #####
## Arguments:
# run_id (should be the same run in the folders because it will assume that and make new folders to save in)
# make_draws: Boolean true or false. Should always say true. Was originally made when MR-BRT was slower to make draws
# trim: The percentage trim you want. Default is set above at 0.2
# cf: The Correction factor you are running for. Takes in CF1, CF2, and CF3
# method: specify whether to create CF for default estimates as in clinical.bundle, or whether to create all CFs. Takes 'default', 'all', or 'custom'.
## if using custom estimate, need to include a file bundle_file with a column for bundle_id and one column for cf to predict on.

# Args: bundle_id, 'icg' or 'bundle', 'cf1' or 'cf2' or 'cf3', a run_name, make_draws (T or F), make plots (T or F)
make_draws <- 'T'
make_plots <- 'F'
trim = 0.2

# Running with default estimates
if(method=='default'){
  df <- loadEstimates()
} else if(method == 'all'){
  bundles <- fread(paste0(FILEPATH))
  bundles <- unique(bundles$bundle_id)
  cfs <- c('cf1', 'cf2', 'cf3')
  df <- expand.grid(bundle_id = bundles, cf = cfs)
} else if(method == 'custom'){
  df <- fread(paste0(write_folder, bundle_file))
} 

df$index <- seq_len(nrow(df))
df <- as.data.table(df)

# Kicking off qsubs for bundles
bundle_models <- function(i){
  type <- 'bundle'
  bundle <- df[index == i, "bundle_id"] %>% as.numeric()
  cf <- df[index == i, "cf"] %>% as.character()
  cat(paste('run = ', run, 'make_draws = ', make_draws, 'trim = ', trim, 'cf = ', cf, 'type = ', type,
            'bundle = ', bundle, sep = "\n"))
  jobname <- paste0('cofa_',bundle, '_', type, '_', cf)
  passvars <- paste0(c(bundle, type, cf, run, make_draws, trim, write_folder), collapse = ',')
  
  qsub <- paste0(QSUB)
  system(qsub)
}
cores <- get_cores
mclapply(df$index, bundle_models, mc.cores = 10)

=======
bundle_models <- mclapply(unique(df$bundle_id), function(bundle){
  for(cf in c('cf1', 'cf2', 'cf3')){
    type <- 'bundle'
    cat(paste('run = ', run, 'make_draws = ', make_draws, 'trim = ', trim, 'cf = ', cf, 'type = ', type,
              'bundle = ', bundle, sep = "\n"))
    qsub(
        jobname = paste0('mrbrt_',bundle, '_', type, '_', cf),
        pass = list(paste(c(bundle, type, cf, run, make_draws, trim), collapse = ',')), 
        slots = 3, 
        #cores = 2,
        #mem = 7,
        submit = T,
        wallclock = '01:00:00',
        log = T,
        proj = 'proj_hospital',
        #archive_node = T,
        shell = paste0(FILEPATH),
        code = paste0(FILEPATH))
  }
  
 
}, mc.cores = 30)

icg_df <- fread(FILEPATH)
icg_mdoels <- mclapply(unique(icg_df$icg_id), function(bundle){
  for(cf in c('cf1', 'cf2', 'cf3')){
    type <- 'icg'
    cat(paste('run = ', run, 'make_draws = ', make_draws, 'make_plots = ', make_plots, 'cf = ', cf, 'type = ', type,
              'bundle = ', bundle, sep = "\n"))
    qsub(
      jobname = paste0('mrbrt_',bundle, '_', type, '_', cf),
      pass = list(paste(c(bundle, type, cf, run, make_draws, make_plots), collapse = ',')),
      #slots = 3,
      cores = 2,
      mem = 5,
      submit = T,
      wallclock = '01:00:00',
      log = T,
      proj = 'proj_hospital',
      archive_node = T,
      shell = paste0(FILEPATH),
      code = paste0(FILEPATH))
  }
}, mc.cores = 10)
# 
# 
