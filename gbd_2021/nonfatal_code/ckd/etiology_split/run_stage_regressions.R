#--------------------------------------------------------------
# USERNAME
# Date Created: 2018-06-08
# Project: Non-fatal CKD etiology proportion estimation
# Purpose: Create stage-specific etiology proportions for all 
# age/sex/years in a location -- adjust DM1/2 proportions based on 
# DM1/2 prevalence for that location 
#--------------------------------------------------------------

# setup -------------------------------------------------------
code_general <- "FILEPATH"
source(paste0(code_general,"/function_lib.R"))

# set objects  ------------------------------------------------------------
args<-commandArgs(trailingOnly = TRUE)

getit <- job.array.child()
loc_id <- getit[[1]] 
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