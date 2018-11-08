#--------------------------------------------------------------
# Date: 2018-06-08
# Project: Non-fatal CKD etiology proportion estimation
# Purpose: Create stage-specific etiology proportions for all 
# age/sex/years in a location -- adjust DM1/2 props based on 
# DM1/2 prev for that location 
#--------------------------------------------------------------

# setup -------------------------------------------------------

if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}


# set objects  ------------------------------------------------------------
args<-commandArgs(trailingOnly = TRUE)
loc_id<-as.numeric(args[1])
dm_1_me<-as.numeric(args[2])
dm_2_me<-as.numeric(args[3])
dm_correction<-args[4]
func_lib<-args[5]
coef_filepath<-args[6]
covmat_filepath<-args [7]
proportion_dir<-args[8]
extrap<-args[9]

# source functions --------------------------------------------------------

source(func_lib)

# output predictions ------------------------------------------------------

preds<-run_process_preds(coef_filepath, covmat_filepath,extrapolate_under_20=extrap)
format_draws(preds,dm_correction,dm_1_me,dm_2_me,loc_id,proportion_dir)
