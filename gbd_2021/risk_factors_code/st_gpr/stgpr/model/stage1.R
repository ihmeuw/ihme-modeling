###########################################################
### Author: Patrick Liu
### Date: 1/26/2015
### Project: ST-GPR
### Purpose: Linear Stage Estimation
###########################################################

library(data.table)
library(magrittr)
library(lme4)
library(rhdf5)

args = commandArgs(trailingOnly = TRUE)

run_root = args[1]
model_root = args[2]
holdout_num = as.integer(args[3])

source(sprintf('%s/functions.R', model_root))

param <- fread(sprintf('%s/parameters.csv', run_root))
  
if (as.integer(param$custom_stage1 == 1)) {
  
  data_df <- prep.df(run_root, holdout_num, gbd_round_id = param$gbd_round_id)
  data_df <- data_df[, c("location_id","year_id", "age_group_id","sex_id", "data", "level"), with = F]
  
  df <- fread(sprintf('%s/custom_stage1_df.csv', run_root))
  setnames(df, 'cv_custom_stage_1', 'stage1')
  
  df <- merge(df, data_df, by = c("location_id","year_id", "age_group_id","sex_id"))
  
} else{
  df <- prep.df(run_root, holdout_num, gbd_round_id = param$gbd_round_id)
  
  ## Run regression by sex
  df <- run.lm(df, param$stage_1_model_formula, param$predict_re, 
               standard_locations_only = param$gbd_round_id >= 6,  # standard locs didn't exist before this
               holdout_num = holdout_num)
}

# Assign data density to each country, according to density cutoffs or # of density categories set by the user
df <- count.country.years(df, "location_id","year_id", "age_group_id","sex_id", "data", "level")

## Clean
df <- clean.prior(df)

#basic checks 
if(nrow(df[is.na(stage1)]) != 0){
  mi <- df[is.na(stage1), location_id] %>% unique %>% sort
  stop(sprintf('There are NA values in your stage1 for the following locations: %s', paste(mi, collapse = ', ')))
}

## Save prior or return to memory
outpath <- sprintf('%s/temp_%i.h5', run_root, holdout_num)
H5close()
h5createFile(file=outpath)
system(paste("chmod 777", outpath))
h5write(df, outpath, 'stage1')
print(sprintf('Saved stage1 estimate to %s', outpath))

