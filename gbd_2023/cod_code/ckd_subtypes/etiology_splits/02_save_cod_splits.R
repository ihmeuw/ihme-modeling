# ---HEADER--------------------------------------------------------------------------------------------------
# Project: Split CKD cod model

#------------------------------------------------------------------------------------------------------------

# ---SETTINGS------------------------------------------------------------------------------------------------
rm(list = ls())
require(data.table)
user <- Sys.info()["user"]
# Source shared functions
ckd_repo <- paste0("FILEPATH", user, "FILEPATH")
source(paste0(ckd_repo, "general_func_lib.R"))
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# ---SETTINGS-------------------------------------------------------------------------------------------------
release_id <- 16
q <- "long.q"
project <- "proj_birds"
threads <- 40
mem <- "80GB"
runtime <- "10:00:00" 
script_02 <- paste0(ckd_repo, 'FILEPATH/save_ckd_cod_script.R') 
shell <- "FILEPATH" 

# update this to where you saved the draws for the split
directory <- 'FILEPATH'
# the description is the same name of the folder that you are uploading
description <- "codem_refresh3_drop_fpg_dm_cov"

logs <- paste0("FILEPATH", user, "FILEPATH", description, "/")
dir.create(logs, recursive = TRUE, showWarnings = TRUE)
#-------------------------------------------------------------------------------------------------------------



# --- LAUNCH SPLIT AND SAVE-----------------------------------------------------------------------------------
# Current workflow:
# (1) screen, (2) make sure .bashrc does not specify PATH to codem/anaconda (3) qlogin (4) gbd_env (5) R (6) specify
# and create outdir (7) source split_cod_mod (8) run
job_name <- "save_split_ckd_cod_mod"

cause_ids <- c(997, 998, 591, 592, 593)

for (id in cause_ids){
  out_dir_id <- paste0(directory, description, "/", id, "/")
  print(out_dir_id)
  
  job_name <- paste0('save_cod_etio_', id)
  
  pass <- list (out_dir_id,description,id,release_id)
  
  construct_sbatch(memory = mem, threads = threads, runtime = runtime, script = script_02, 
                   job_name = job_name, pass = pass,
                   project = project, partition = q, submit = TRUE,
                   output = paste0(logs, job_name, ".o%j"),
                   errors = paste0(logs, job_name, ".e%j"))
}