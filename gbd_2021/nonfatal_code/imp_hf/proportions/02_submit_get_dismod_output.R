#!/usr/bin/Rscript

##########################################################################################
####### Steps the script perform:
####### 1. Launch qsub parallelized by locs to generate corrected prevalence 
####### for HF etiologies (23 total - 21 CVD-processed causes & 3 Chagas severity causes) 
####### 2. Save_results 
####### HF_2017 code rewritten by USER & USER from HF_2016 & before code by USER, USER, & USER
####### old: ADDRESS
####### new: 
##########################################################################################
##########################################################################################
####### Steps to running script:
####### qlogin -P proj_custom_models -pe multi_slot 36 -now no
####### 1. qlogin to cluster prod
####### 2. cd FILEPATH
####### 3. R CMD BATCH post_dismod_submitProcessing.R #######
####### error folder: FILEPATH 
##########################################################################################

## Profiling - these often take <10 minutes and <2G. Once in Decomp 4 they took >6 hours because of file system load and get_draws speed. 

#######################################
## PREP ENVIRONMENT & GLOBAL VAR
#######################################
rm(list = ls())


user <- Sys.getenv('USER')
# source the central functions
central <- 'FILEPATH'
for (func in paste0(central, list.files(central))) source(paste0(func))

datetime <- format(Sys.time(), '%Y_%m_%d')

# add libs
library(data.table)
library(foreign)
library(parallel)
library(rhdf5)

gbd_round_id <- VALUE

# Metadata
metadata <- get_demographics(gbd_team = "VALUE", gbd_round_id = VALUE)

year_ids <- unique(metadata$year_id)
sex_ids  <- unique(metadata$sex_id)
age_group_ids = unique(metadata$age_group_id)
age_groups <- get_age_metadata(gbd_round_id = gbd_round_id, age_group_set_id = VALUE)[, .(age_group_years_start, age_group_id)]
setnames(age_groups, "age_group_years_start", "age_start")

locations <- get_location_metadata(location_set_id=VALUE, gbd_round_id=gbd_round_id)
dismod_locs <- unique(metadata$location_id)
loc_choices <- locations[location_type_id==VALUE | location_id %in%dismod_locs]$location_id

# cvd output folder
cvd_path = "FILEPATH"

# hf etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list_2020.csv'))
'%ni%' <- Negate('%in%')
composite <- composite[prev_folder %ni% c('congenital_mild_2179',
                                          'congenital_mod_2180',
                                          'congenital_severe_2181',
                                          'congenital_asymp_20090')]
# qsub diagnostic file paths
qsub_output = paste0("FILEPATH", datetime, "/output") 
qsub_errors = paste0("FILEPATH", datetime, "/errors") 
dir.create(qsub_output, showWarnings = FALSE, recursive = TRUE)
dir.create(qsub_errors, showWarnings = FALSE, recursive = TRUE)

#######################################
## RUNS CHILD SCRIPT IN PARALLELIZATION
#######################################
file_list <- NULL
outputDir <- paste0(cvd_path, '/post_dismod/')
dir.create(outputDir, showWarnings = FALSE)

locs <- expand.grid(loc_choices)
loc_path <- "FILEPATH"
write.csv(locs, loc_path, row.names=F)

rscript <- "FILEPATH"
n_jobs <- nrow(locs)

shell <- 'FILEPATH'

# Don't parallelize population calls
pop <- get_population(age_group_id = unique(age_groups$age_group_id), location_id = loc_choices, sex_id = c(1, 2), 
                             year_id = year_ids, decomp_step = 'VALUE', gbd_round_id = gbd_round_id)
pop_path <- "FILEPATH"
saveRDS(pop, pop_path)


code_command <- paste0(shell, " -s ", rscript, " ", loc_path, " ", pop_path)
full_command <- paste0("sbatch -J hf_post -A proj_cvd --mem=15G -c 2 -t 1:00:00 -C archive -p all.q ",
                       "-a ", paste0("1-", n_jobs), " ",
                       "-o FILEPATH",user,"/output_logs/%x.o%j ",
                       "-e FILEPATH",user,"/error_logs/%x.e%j ",
                       code_command)

print(full_command)
system(full_command)


folder_list <- unique(na.omit(composite[!cause_name%like%'Chagas']$prev_folder))
for (loc in locs$Var1){
  for (folder in folder_list) {
    if (folder !=""){
      output_path <- paste0(outputDir, '1_corrected_prev_draws/', datetime, '/', folder , '/', loc, '.csv')
      file_list <- c(file_list, output_path)
    }
  }
}
## Wait until all jobs are done

missing <- c()
# Stop if file missing
  if (!file.exists(file)) missing <- c(missing, file)
}

if (length(missing) > 0) {
  
  locs <- gsub('.*/ ?(\\w+)', '\\1', missing)
  locs <- unique(as.integer(gsub(".csv", "", locs)))
  
  loc_path <- "FILEPATH"
  write.csv(data.table(Var1=locs), loc_path, row.names=F)
  
  rscript <- "FILEPATH"
  n_jobs <- length(locs)
  
  
  code_command <- paste0(shell, " -s ", rscript, " ", loc_path, " ", pop_path)
  full_command <- paste0("sbatch -J hf_post_missing -A proj_cvd --mem=20G -c 5 -t 3:00:00 -C archive -p all.q ",
                         "-a ", paste0("1-", n_jobs), " ",
                         "-o FILEPATH",user,"/output_logs/%x.o%j ",
                         "-e FILEPATH",user,"/error_logs/%x.e%j ",
                         code_command)

  print(full_command)
  system(full_command)
  
  
}

#######################################
## UPLOAD PREV TO APPRORIATE MES
######################d#################
save_results <- T

composite[prev_me_id==VALUE, prev_me_id:=VALUE]
me_ids <- unique(composite[!is.na(prev_me_id), prev_me_id])
me_ids <- expand.grid(me_ids)
m_path <- 'FILEPATH'
write.csv(me_ids, m_path)

rscript <- "FILEPATH"
n_jobs <- nrow(me_ids)

shell <- 'FILEPATH'

code_command <- paste0(shell, " -s ", rscript, " ", m_path, " ", datetime)
full_command <- paste0("sbatch -J upload_draws -A proj_cvd --mem=40G -c 5 -t 10:00:00 -C archive -p all.q ",
                       "-a ", paste0("1-", n_jobs), " ",
                       "-o FILEPATH",user,"/output_logs/%x.o%j ",
                       "-e FILEPATH",user,"/error_logs/%x.e%j ",
                       code_command)

print(full_command)
if (save_results) system(full_command)

## Check that everything has saved properly (that date is today for all causes)
me_ids <- data.table(me_ids)
for (me_id in me_ids$Var1){
  print(me_id)
  d <- get_best_model_versions("modelable_entity", me_id, 7, "iterative")
  me_ids[Var1==me_id, date := d$best_start]
}
