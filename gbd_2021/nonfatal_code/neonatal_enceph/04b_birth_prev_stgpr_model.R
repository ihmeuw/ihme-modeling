## ******************************************************************************
##
## Purpose: register and run an st-gpr model for neonatal enceph birth prevalence.
##          also include st-gpr plotting code, and code to save the stgpr model to 
##          the epi database once you decide to.
## Input:   
## Output:  
## Created: 2020-05-12
## Last updated: 2020-08-10
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "PATHNAME"
  h <-"PATHNAME"
  my_libs <- "PATHNAME"
} else {
  j<- "PATHNAME"
  h<- "PATHNAME"
  my_libs <- "PATHNAME"
}

central_root <- "PATHNAME"
setwd(central_root)
source('PATHNAME/register.R')
source('PATHNAME/sendoff.R')

filepath <- "PATHNAME"
config <- fread(paste0(filepath, 'config_stgpr_birth_prev.csv'))
#aggregate Poland, New Zealand, Japan, USA, Italy, Norway, UK, Mexico, Brazil, South Africa
config$agg_level_4_to_3 <- as.character(config$agg_level_4_to_3)
config$agg_level_5_to_4 <- as.character(config$agg_level_5_to_4)
config[2,agg_level_4_to_3 := '51,72,67,102,86,90,95,130,135,196']
config[2,agg_level_5_to_4 := '95']
config[2,agg_level_6_to_5 := '95']
config$gpr_draws <- 0

new_row <- config[11,]
new_row$notes <- 'same settings as row 6, new outliering of all USA inpatient, change use to rake to states, 1000 draws'
new_row$crosswalk_version_id <- 27350 
new_row$model_index_id <- 12

config <- rbind(config, new_row)

write.csv(config, file = paste0(filepath, 'config_stgpr_birth_prev.csv'), row.names = FALSE)

run_id <- register_stgpr_model(path_to_config = paste0(filepath, 'config_stgpr_birth_prev.csv'),
                               model_index_id = 12)
stgpr_sendoff(run_id, 
              project = 'proj_neonatal',
              nparallel = 50,
              log_path = "PATHNAME")

#------------------------------------------------------------------------------------------------
# Wait for model to complete, then run the plotting code below
#------------------------------------------------------------------------------------------------
source('PATHNAME/utilities.R')

#check outputs
run_id <- 159986
run_id2 <- 156107

#plot gpr

source('PATHNAME/plot_gpr.R')
plot_gpr(run.id = run_id,
         run.id2 = run_id2,
         output.path = paste0('PATHNAME',run_id,'_',run_id2,'_08_10.pdf'),
         add.regions = TRUE,
         add.outliers = TRUE,
         cluster.project = 'proj_neonatal')


#launch qsub to save results st_gpr
job_flag <- paste0("-N ne_save_results_stgpr")
project_flag <- paste0("-P proj_neonatal")
thread_flag <- "-l fthread=4"
mem_flag <- "-l m_mem_free=60G"
runtime_flag <- "-l h_rt=03:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q all.q"
next_script <- paste0("PATHNAME/birth_prev_stgpr_save_results.R")
errors_flag <- paste0("-e PATHNAME")
outputs_flag <- paste0("-o PATHNAME")
cwd_flag <- "-cwd"
shell_script <- "PATHNAME/r_shell_ide.sh"

qsub <- paste("qsub ", job_flag, project_flag, thread_flag, mem_flag, runtime_flag, jdrive_flag, queue_flag,
              errors_flag, outputs_flag, cwd_flag, shell_script, next_script)

system(qsub)


