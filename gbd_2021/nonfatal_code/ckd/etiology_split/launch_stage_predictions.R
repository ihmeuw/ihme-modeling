#--------------------------------------------------------------
# USERNAME
# Date Created: 2018-06-08
# Project: Non-fatal CKD etiology proportion estimation 
# Purpose: Launch stage-specific etiology proportion analysis
#--------------------------------------------------------------

# setup -------------------------------------------------------------------
code_general <-paste0("FILEPATH")
source(paste0(code_general,"/function_lib.R"))
source_shared_functions("get_location_metadata")
func_lib <-paste0("FILEPATH")
source(func_lib)
library(ggplot2)

#  set options ------------------------------------------------------------
ds <- "iterative"
process <- 2
cohort<-"DESCRIPTION"
plot <- F
plot_version <- 2020
dm_correction<-T
dm_1_me<-24633
dm_2_me<-24634

dir<-paste0("FILEPATH")
coef_filepath<-paste0("FILEPATH")
covmat_filepath<-paste0("FILEPATH")

code_dir<-paste0("FILEPATH")
proportion_dir<-paste0("FILEPATH")

description<-"DESCRIPTION"
map<-as.data.table(read.xlsx(paste0("FILEPATH")))

extrapolate_under_20<-F

shell<- "SHELL"
script_01 <- paste0(code_dir,"run_stage_regressions.R")
script_02 <- paste0(code_dir,'save_stage_specific_regressions.R')

threads <- 1
mem_free <- 1
runtime <- "2:00:00"
q <- "all.q"
project <- "proj_yld "
errors <- paste0("FILEPATH")
if (!file.exists(errors)){dir.create(errors,recursive=T)}

# diagnostic plots  -------------------------------------------------------
# create diagnostic plots
if (plot==T){
  preds<-run_process_preds(coef_filepath, covmat_filepath)
  plot_predictions(dir,preds,plot_version,cohort)
}

# generate results  -------------------------------------------------------
if (process == 1) {
  message("getting params")
  all_params <- get_demographics(gbd_team = "epi")
  params <- all_params[1] 
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
  
  for (me in source_mes){
    description <- paste0("save_etio_props_",me)
    bid <- map$bid[map$proportion_me_id==me]
    cvid <- map$cvid[map$proportion_me_id==me]
    pass <- paste(description,me,ds,bid,cvid,date,collapse = " ")
    sys_sub<- paste0('qsub -P ', project, " -N ", description , ' -e ', errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q)
    if(me!=20376) {
    system(paste(sys_sub, shell, script_02, pass))
    }
    print(paste(sys_sub, shell, script_02, pass))
  }
} else {
  stop(paste0("Process value ", process, " is not a valid argument."))
}