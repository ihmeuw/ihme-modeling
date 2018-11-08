#!FILEPATH

##########################################################################################
####### This is a child script of pre_dismod_submitProcessing.R and does the following:
####### 1. Fetch codcorrected death draws (best) from all non-composite HF causes
####### 2. Calculate mean, upper, and lower from the 1000 draws 
####### 3. Collapse results on c('cause_id', 'sex_id', 'age_group_id', 'location_id')
##########################################################################################

#######################################
## PREP ENVIRONMENT & GLOBAL VAR
#######################################
rm(list = ls())
if (Sys.info()[1] == "Linux") folder_path <- "FOLDER"
if (Sys.info()[1] == "Windows") folder_path <- "FOLDER"

# add libs
library(data.table)
library(parallel)
library(plyr)

# source central functions
source(paste0(folder_path, "/FUNCTION_PATH/get_draws.R"))
source(paste0(folder_path, "/FUNCTION_PATH/get_location_metadata.R"))

# cvd output folder
#cvd_path = "FILEPATH"
cvd_path = "/CVD_FOLDER"

# qsub diagnostic file path
qsub_output = paste0("/LOG_FOLDER/HF_predismod/output")
qsub_errors = paste0("/LOG_FOLDER/HF_predismod/errors")

year_ids <- c(1990:2017)
sex_ids  <- c(1,2)
age_group_ids = c(2:20,30,31,32,235)
loc_id   <- commandArgs()[4]

message('Getting codcorrect draws for location ', loc_id)

# hf etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list.csv'))

#######################################
## 1. Fetch codcorrected death draws (best) from all non-composite HF causes
#######################################
# necessary columns
group_cols = c('cause_id', 'sex_id', 'age_group_id', 'location_id', 'year_end')
measure_cols = c('mean_death', 'upper_death', 'lower_death')

# Query death draws for all non-composite cause
COD_CAUSE_IDS <- c(unique(composite[!(cause_id%in%unique(composite$composite_id))&(cause_id!=346)]$cause_id))
gbd_id_fields  <- rep('cause_id',each=length(COD_CAUSE_IDS))
df <- get_draws(gbd_id_fields, 
                COD_CAUSE_IDS,
                'codcorrect',
                location_id=loc_id,
                age_group_id=age_group_ids,
                gbd_round_id=5,
                year_id=year_ids,
                status="latest",
                sex_id =sex_ids,
                measure_id=1,
                num_workers=5)

# make sure get_draws gets what we need
df <- df[year_id %in% year_ids & measure_id==1]

# replace empty numeric columns with 0
for(j in seq_along(df)){
  set(df, i = which(is.na(df[[j]]) & is.numeric(df[[j]])), j = j, value = 0)
}

#######################################
## 2. Calculate mean, upper, and lower from the 1000 draws 
#######################################
draw_cols = grep("^draw_[0-999]?", names(df), value=TRUE)

# calculate upper and lower quartile of 1000 draws 
quantile.upper <- function(x) as.numeric(quantile(x, .75))
quantile.lower <- function(x) as.numeric(quantile(x, .25))

df <- df[, ':='(mean_death = rowMeans(df[,mget(draw_cols)]),
                upper_death = apply(df[,mget(draw_cols)], 1, quantile.upper),
                lower_death = apply(df[,mget(draw_cols)], 1, quantile.lower))]

#######################################
## 3. Collapse results on c('cause_id', 'sex_id', 'age_group_id', 'location_id')
#######################################
# collapse all marketscan years
df[, year_end:=2017]
summ_df <- df[,.(mean_death=sum(mean_death), upper_death=sum(upper_death), lower_death=sum(lower_death)), by=group_cols]

# Fill in missing values for any age-sex-cause-location-year combination with zeros
square <- expand.grid(age_group_id=unique(summ_df$age_group_id), sex_id=unique(summ_df$sex_id), cause_id=unique(summ_df$cause_id),
                      location_id=unique(summ_df$location_id),year_end=unique(summ_df$year_end))
summ_df <- as.data.table(join(summ_df, square, type='right'))
for(j in seq_along(summ_df)){
  set(summ_df, i = which(is.na(summ_df[[j]]) & is.numeric(summ_df[[j]])), j = j, value = 0)
}

#######################################
## 4. Save output to 0_codcorrected_deaths folder to be read back in the main script
#######################################
saveRDS(summ_df, paste0(cvd_path, '/pre_dismod/0_codcorrected_deaths/version/codcorrect_', loc_id, '.rds'))
