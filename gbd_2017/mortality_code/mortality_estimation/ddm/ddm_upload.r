# Author: 
# Date: 09/07/17


rm(list=ls())
library(data.table); library(haven); library(readstata13); library(assertable); library(DBI); library(mortdb, lib = "FILEPATH"); library(mortcore, lib = "FILEPATH")

if (Sys.info()[1] == "Linux") {
  root <- "/home/j/"
  username <- Sys.getenv("USER")
} else {
  root <- "J:/"
}

child_version_est <- 83
gbd_year <- 2017

proc_lineage <- data.table(get_proc_lineage(model_name = "ddm", model_type = "estimate", run_id = child_version_est))
child_version_data <- proc_lineage[parent_process_name == "ddm data", parent_run_id]

# Set proper file paths
code_dir <- paste0("/FILEPATH/", username, "/adult-mortality/ddm/")
r_shell <- "/FILEPATH/r_shell_singularity_3402.sh"
project_flag <- "proj_mortenvelope"


qsub("ddm_generate_parent_child", paste0(code_dir, "generate_ddm_parent_child.r"), pass = list(child_version_est, child_version_data), slots = 1, mem= 2, submit=T, proj = "proj_mortenvelope", shell = r_shell)


qsub("ddm_upload_data", paste0(code_dir, "upload/ddm_data_upload.r"), hold = "ddm_generate_parent_child", pass = list(child_version_est, child_version_data, gbd_year, best = FALSE), slots = 2, mem= 4, submit=T, proj = "proj_mortenvelope", shell = r_shell)
qsub("ddm_upload_estimates", paste0(code_dir, "upload/ddm_estimate_upload.r"), hold = "ddm_upload_data", pass = list(child_version_est, gbd_year, best = FALSE), slots= 2, mem= 4, submit=T, proj = "proj_mortenvelope", shell = r_shell)


qsub("ddm_compile_vr_for_graphs", paste0(code_dir, "compile_vr.r"), pass = list(child_version_est), hold = "ddm_generate_parent_child", slots = 2, mem= 4, submit=T, proj = project_flag, shell = r_shell)

# DONE