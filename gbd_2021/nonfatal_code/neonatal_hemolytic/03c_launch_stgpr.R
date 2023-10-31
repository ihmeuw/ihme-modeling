## ******************************************************************************
##
## Purpose: register and run an st-gpr model
## Input:   
## Output:  
## Created: 2020-05-04
## Last updated: 2020-05-04
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
  h<-"PATHNAME"
  my_libs <- "PATHNAME"
}

central_root <- "PATHNAME"
setwd(central_root)
source('PATHNAME/register.R')
source('PATHNAME/sendoff.R')

run_id <- register_stgpr_model(path_to_config = 'PATHNAME/config_stgpr.csv',
                               model_index_id = 7)
stgpr_sendoff(run_id, 
              project = 'proj_nch',
              nparallel = 50,
              log_path = 'PATHNAME')

#run id 148316

#check outputs
run_id <- 152474
run_id2 <- 152465

#152219
#152222

output_path <- paste0('PATHNAME',run_id)

betas <- fread(paste0(output_path, '/stage1_summary.csv'))

#plot gpr

source('PATHNAME/plot_gpr.R')
plot_gpr(run.id = run_id,
         run.id2 = run_id2,
         output.path = "PATHNAME",
         add.regions = TRUE,
         add.outliers = TRUE,
         cluster.project = 'proj_neonatal')


#launch qsub to save results st_gpr
job_flag <- paste0("-N bord_save_results")
project_flag <- paste0("-P proj_neonatal")
thread_flag <- "-l fthread=10"
mem_flag <- "-l m_mem_free=80G"
runtime_flag <- "-l h_rt=06:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q all.q"
next_script <- paste0("PATHNAME/07_save_results_stgpr.R")
errors_flag <- paste0("-e PATHNAME")
outputs_flag <- paste0("-o PATHNAME")
cwd_flag <- "-cwd"
shell_script <- "PATHNAME/r_shell_ide.sh"

qsub <- paste("qsub ", job_flag, project_flag, thread_flag, mem_flag, runtime_flag, jdrive_flag, queue_flag,
              errors_flag, outputs_flag, cwd_flag, shell_script, next_script)

system(qsub)


