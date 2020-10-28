#--------------------------------------------------------------
# Name: Carrie Purcell
# Date: 2018-06-08
# Project: Non-fatal CKD etiology proportion estimation
# Purpose: Create stage-specific etiology proportions for all 
# age/sex/years in a location -- adjust DM1/2 props based on 
# DM1/2 prev for that location 
#--------------------------------------------------------------

# setup -------------------------------------------------------

user <- Sys.info()["user"]
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  code_general <- '/share/epi/ckd/ckd_code'
  share_path <- paste0("/share/code/",user,"/ckd")
} else { 
  stop("This code needs to be run on the cluster")
}

source(paste0(code_general,"/function_lib.R"))

# set objects  ------------------------------------------------------------
args<-commandArgs(trailingOnly = TRUE)
#if(length(args)==0) {
#  args <- c(4709, pass)
#}
getit <- job.array.child()
loc_id <- getit[[1]] # grab the unique PARAMETERS for this task id
loc_id <- as.numeric(loc_id)
message("loc_id: ", loc_id)
dm_1_me<-as.numeric(args[2])
dm_2_me<-as.numeric(args[3])
dm_correction<-args[4]
func_lib<-args[5]
coef_filepath<-args[6]
covmat_filepath<-args[7]
proportion_dir<-args[8]
extrap<-args[9]
ds<-args[10]

# source functions --------------------------------------------------------

source(func_lib)

# output predictions ------------------------------------------------------

print(paste("Co-efficient file path", coef_filepath))
preds<-run_process_preds(coef_filepath, covmat_filepath,extrapolate_under_20=extrap)
format_draws(preds,dm_correction,dm_1_me,dm_2_me,loc_id,proportion_dir,decompstep=ds)
