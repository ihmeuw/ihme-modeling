#############################################################
##Purpose: Master script to submit MR-BRT models
##Notes: Parallelize your submission of the model
###########################################################
# Setup -----
rm(list = ls())

suppressWarnings(library(tidyverse))
library(data.table)
library(parallel)
library(RMySQL)
suppressWarnings(source(FILEPATH))

args <- commandArgs(trailingOnly = TRUE)[1]
args <- strsplit(args, ',')
print(args)
vers <- args[[1]][1] %>% print()
prep_vers <- args[[1]][2] %>% print()
method <- args[[1]][3] %>% print()
bundle_file <- args[[1]][4] %>% print()
trim <- args[[1]][5] %>% print()
knot_vers <- args[[1]][6] %>% print()

# Validate input args
stopifnot(!is.na(vers) & !is.na(method))
if(method=='run_custom' | method=='pred_only_custom'){
  stopifnot(!is.na(bundle_file))
}

user <- Sys.info()[["user"]]
write_folder <- FILEPATH
dir.create(write_folder)
dir.create(paste0(write_folder,'by_agesex/'), recursive = TRUE)
dir.create(paste0(write_folder,'by_cfbundle/'), recursive = TRUE)
dir.create(paste0(write_folder,'validation/'), recursive = TRUE)
dir.create(paste0(write_folder,'split/'), recursive = TRUE)
dir.create(paste0(write_folder,'split/by_agesex/'), recursive = TRUE)
dir.create(paste0(write_folder,'split/cf_estimates/'), recursive = TRUE)


# Reference file prep -----
# Prep CSVs so that trim worker can populate rows by appending
write_csv(data.frame(date = character(), vers = numeric(), prep_vers = numeric(), bundle = numeric(), CF = character(), trim = numeric(), knots = numeric(), sex_cov = logical(), 
                     age_cov = logical(), min_age = numeric(), max_age = numeric(), spline = numeric()), 
          paste0(write_folder, 'mr_brt_run_log.csv'), append = TRUE)

# Write preddf and age_groups to folder to reference in trim worker
# # Make prediction dataframes. Make the hospital prediction dataset of age and sex ##
ages <- get_age_metadata(19,7) %>%
  setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end')) %>%
  select(age_group_id, age_group_name, age_start, age_end) %>%
  rbind(data.table(age_group_id = 28, age_group_name = '<1 year', age_start = 0, age_end = 1))

age_groups <- ages %>%
  mutate(age_midpoint = case_when(
    age_start == 95 ~ 97.5,
    TRUE ~ (age_start + age_end)/2)) %>%
  select(-age_end)
write_csv(age_groups, paste0(write_folder,'age_groups.csv'))

ages[, index := 1]
sexes <- data.table(sex_id = c(1,2), index = 1)

preddf <- merge(ages, sexes, by = 'index', allow.cartesian = TRUE)
preddf[, age_group_weight_value := NULL]
write_csv(preddf, paste0(write_folder,'age_sex_preddf.csv'))

# Get estimates -----
loadEstimates <- function() {
  db_con = fread(FILEPATH)
  
  # Get estimate ids and bundles
  con <- dbConnect(dbDriver("MySQL"),
                   username = db_con$username,
                   password = db_con$pass,
                   host = db_con$host)
  df <- dbGetQuery(con, QUERY)
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
# bundle_id
# cf: The Correction factor you are running for. Takes in CF1, CF2, and CF3
# vers: version of the CFs to model
# prep_vers: version of input data to use
# pred_only: specify whether to create new CF models or whether to predict off existing ones

# Args: bundle_id, 'icg' or 'bundle', 'cf1' or 'cf2' or 'cf3', a run_name, make_draws (T or F), make plots (T or F)
if(method == 'pred_only' | method == 'pred_only_custom'){
  pred_only <- TRUE
}else{
  pred_only <- FALSE
}


# Find bundle-cf combos to run based on method parameter
if(method=='pred_only'){
  folders <- list.dirs(paste0(write_folder,"by_cfbundle"), full.names = FALSE)
  df <- as.data.table(str_split_fixed(folders, '_', 3))
  df <- df[!(df$V1 == ""), ]
  df[, 'V1'] <- NULL
  setnames(df, c('V2', 'V3'), c('bundle_id', 'cf'))
} else if(method=='run_active_only'){
  df <- loadBundles()
} else if(method == 'run_all'){
  bundles <- get_hosp_data()
  bundles <- unique(bundles$bundle_id)
  cfs <- c('cf1', 'cf2', 'cf3')
  df <- expand.grid(bundle_id = bundles, cf = cfs)
  bundles <- Sys.glob(FILEPATH)
} else if(method == 'run_custom' | method == 'pred_only_custom'){
  df <- fread(bundle_file)
} 

df$index <- seq_len(nrow(df))
df <- as.data.table(df)

# Kicking off qsubs for bundles
bundle_models <- function(i){
  type <- 'bundle'
  bundle <- df[index == i, "bundle_id"][[1]] %>% as.numeric()
  cf <- df[index == i, "cf"][[1]] %>% as.character()
  cat(paste('cf version = ', vers, 'prep data version = ', prep_vers, 'cf = ', cf, 
            'bundle = ', bundle, 'pred only =', pred_only, 'trim =',trim, 'knots = ',knot_vers, sep = "\n"))
  jobname <- paste0('cofa_',bundle, '_', type, '_', cf)
  passvars <- paste0(c(bundle, cf, vers, prep_vers, pred_only, trim, knot_vers), collapse = ',')
  
  qsub <- QSUB
  system(qsub)
}
mclapply(df$index, bundle_models, mc.cores = ceiling(nrow(df)/20))


