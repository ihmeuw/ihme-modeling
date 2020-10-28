#--------------------------------------------------------------
# Name: Carrie Purcell
# Date: 2018-06-08
# Project: Non-fatal CKD etiology proportion estimation 
# Purpose: Launch stage-specific etiology proportion analysis
# NOTE: Had to run about 6 times to getestimates for all locations.
#--------------------------------------------------------------

# setup -------------------------------------------------------------------

rm(list=ls())
date<-gsub("_", "_", Sys.Date())
user <- Sys.info()["user"]
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  code_general <- '/share/epi/ckd/ckd_code'
} else { 
  stop("This code needs to be run on the cluster")
}
source(paste0(code_general,"/function_lib.R"))
source_shared_functions("get_location_metadata")
func_lib<-paste0(code_general,"/etiology_splits/epi/function_library.R")
source(func_lib)
library(ggplot2)


#  set options ------------------------------------------------------------

# Should this be step3?
ds <- "iterative"
process <- 2

cohort<-"geisinger"

plot <- F
plot_version <- 2020

dm_correction<-T
dm_1_me<-24633
dm_2_me<-24634

dir<-paste0(j_root, "WORK/12_bundle/ckd/collab_data/CKDPC/GBD2019")
coef_filepath<-paste0(dir,"/etio_regression_coefs_",cohort,".xlsx")
covmat_filepath<-paste0(dir,"/covmat_",cohort,".csv")

code_dir<-paste0(code_general,"/etiology_splits/epi/")
proportion_dir<-paste0('/share/epi/ckd/ckd_epi_splits/stage_specific_proportions/', date, '/')

description<-"save_geisinger_results_with_dm_correction_no_extrapolation_use_gbd2019_dm_step4_for_inputs"
map<-as.data.table(read.xlsx(paste0(code_general,"/etiology_splits/epi/me_measure_map.xlsx")))

extrapolate_under_20<-F

shell<-'/share/singularity-images/health_fin/forecasting/shells/health_fin_forecasting_shell_singularity.sh'
script_01 <- paste0(code_dir,"run_stage_regressions.R")
script_02 <- paste0(code_dir,'save_stage_specific_regressions.R')

threads <- 1
mem_free <- 1
runtime <- "2:00:00"
q <- "all.q"
project <- "proj_yld "
errors <- paste0("/share/scratch/users/",user,"/ckd/errors/stage_predictions")
if (!file.exists(errors)){dir.create(errors,recursive=T)}

# diagnostic plots  -------------------------------------------------------

# create diagnostic plots -- if these look ok , run the next steps 
if (plot==T){
  preds<-run_process_preds(coef_filepath, covmat_filepath)
  plot_predictions(dir,preds,plot_version,cohort)
}

# generate results  -------------------------------------------------------

if (process == 1) {
  
  ## GET PARAMS
  message("getting params")
  all_params <- get_demographics(gbd_team = "epi")
  params <- all_params[1] ##only parallelizing over location_id
  pass <- c(dm_1_me, dm_2_me, dm_correction, func_lib, coef_filepath, covmat_filepath,
            proportion_dir, extrapolate_under_20, ds)
  params <- as.list(params)
  job.array.master(tester = F, paramlist = params, username = user, project = project,
                   threads = threads,
                   mem_free = mem_free,
                   runtime = runtime,
                   q = q,
                   jobname = "predict_etios",
                   errors = errors,
                   childscript = script_01,
                   shell = shell,
                   args = pass)
  
  # check if all locations finished
  for (stage_dir in c("alb", "stage3", "stage4_5")) {
   output_dir <- paste0(proportion_dir, stage_dir)
   for (etiology_dir in c("oth", "htn", "gn", "dm1", "dm2")) {
     output_dir <- paste0(proportion_dir, stage_dir, "/", etiology_dir)
     print(output_dir)
     check<-check_missing_locs(output_dir,filepattern = "{location_id}.csv",team = "epi")
   }
  }
  
} else if (process == 2) {
  # save --------------------------------------------------------------------
  
  source_mes<-unique(map[grepl("end-stage",source_me_name)==F,proportion_me_id])
  threads <- 1
  mem_free <- 50
  runtime <- "4:00:00"
  q <- "all.q"
  date <- "2020-07-25"
  
  for (me in source_mes){
    jname <- paste0("save_etio_props_",me)
    bid <- map$bid[map$proportion_me_id==me]
    cvid <- map$cvid[map$proportion_me_id==me]
    pass <- paste(description,me,ds,bid,cvid,date,collapse = " ")
    sys_sub<- paste0('qsub -P ', project, " -N ", jname , ' -e ', errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q)
    if(me!=20376) {
    system(paste(sys_sub, shell, script_02, pass))
    }
    print(paste(sys_sub, shell, script_02, pass))
  }
} else {
  stop(paste0("Process value ", process, " is not a valid argument."))
}
