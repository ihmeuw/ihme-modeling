#--------------------------------------------------------------
# Date: 2018-06-08
# Project: Non-fatal CKD etiology proportion estimation 
# Purpose: Launch stage-specific etiology proportion analysis
#--------------------------------------------------------------


# setup -------------------------------------------------------------------

rm(list=ls())

if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}
source(paste0(h_root,"FILEPATH"))
func_lib<-paste0(h_root,"FILEPATH")
source(func_lib)

#  set options ------------------------------------------------------------

shell_script<-'FILEPATH'
submit_jobs<-T

cohort<-"cohort_name"

plot<-F
plot_version<-4

dm_correction<-T
dm_1_me<-18655
dm_2_me<-18656

dir<-paste0(j_root, "FILEPATH")
coef_filepath<-paste0(dir,"FILEPATH",cohort,".xlsx")
covmat_filepath<-paste0(dir,"FILEPATH",cohort,".csv")

code_dir<-paste0(h_root,"FILEPATH")
proportion_dir<-'FILEPATH'

description<-"save_results"
map<-as.data.table(read.xlsx(paste0(h_root,"FILEPATH.xlsx")))

extrapolate_under_20<-F

# diagnostic plots  -------------------------------------------------------

# create diagnostic plots 
if (plot==T){
  preds<-run_process_preds(coef_filepath, covmat_filepath)
  plot_predictions(dir,preds,plot_version,cohort)
}


# generate results  -------------------------------------------------------

locs<-get_demographics("epi")[["location_id"]]

for (loc_id in locs){
  qsub(jobname = paste0("predict_etios_",loc_id),
       code = paste0(code_dir,"FILEPATH.R"),
       pass = list(loc_id, dm_1_me, dm_2_me, dm_correction, func_lib, coef_filepath, covmat_filepath,proportion_dir,extrapolate_under_20),
       slots = 1,
       submit = submit_jobs,
       proj = "proj_custom_models",
       shell = shell_script)
}

# save --------------------------------------------------------------------

source_mes<-unique(map[grepl("end-stage",source_me_name)==F,proportion_me_id])

for (me in source_mes){
  qsub(jobname = paste0("save_etio_props_",me),
       code = paste0(h_root, 'FILEPATH.R'),
       pass = list(description,me),
       slots = 20,
       submit = T, 
       proj = "proj_custom_models",
       shell=shell_script)
}

