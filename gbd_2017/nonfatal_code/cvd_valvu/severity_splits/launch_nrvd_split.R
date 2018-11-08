####################
##Author: USERNAME
##Date: 1/4/2018
##Purpose: Model and save asymptomatic, treatment, and symptomatic severity splits
## 
########################

rm(list=ls())
os <- .Platform$OS.type


library(data.table)
library(ggplot2)

date<-gsub("-", "_", Sys.Date())

################### ARGS AND PATHS #########################################
######################################################

descr<-"Small adjustment to hosp cw; No norway hosp data;"
upload_mes<-c("aort")
proj<-"proj_ensemble"

asympt_by_age<-T ##USERNAME: if T, will use age-specific asymptomatic model
calculate_prevalences<-T ##USERNAME: if true, this will launch jobs to save draws. If F, will just upload currently save draws
save_upload<-T
save_other<-F ##USERNAME: to save zeros to the 'other nrvd' for 
best<-T

if(asympt_by_age==T){
  aasympt_folder<-paste0("FILEPATH")
}else{
  aUSERNAMEmpt_folder<-paste0("FILEPATH")
}
output_folder<-paste0("FILEPATH")

central<-paste0("FILEPATH")


################### SCRIPTS #########################################
######################################################

source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "save_results_epi.R"))

source(paste0("FILEPATH/utility/job_hold.R"))


################### LAUNCH JOBS  #########################################
######################################################

if(calculate_prevalences==T){
  for(me in c(upload_mes)){
    
    ##USERNAME: clean out old folders
    invisible(lapply(c("tx", "sympt", "asympt"), function(x){
      temp_folder<-paste0(output_folder, "/", me, "/", x, "_draws/")
      unlink(temp_folder, recursive=T)
      dir.create(temp_folder)
    }))
    
    prev_me<-ifelse(me=="aort", 16601, 16602)
    message("Creating symptomatic for ", me)
    expected_files<-list()
    for(year in c(seq(from=1990, to=2010, by=5), 2017)){
      for(sex in c(1, 2)){
        if(F){
          year<-2017
          prev_me<-16601
          me<-"aort"
          sex<-1
        }
        command <- paste0("qsub -pe multi_slot ", 2, " -P ", proj," -N ", paste0("sympt_", me, "_", year, "_", sex)," FILEPATH ",
                          paste0(c(prev_me, year, sex, asympt_by_age), collapse=" "))
        
        system(command)
        expected_files[[length(expected_files)+1]]<-data.table(year=year, sex=sex, file=paste0(year, "_", sex, "_5.csv"))
      }
    }
  }
  message("Waiting on jobs...")
  job_hold("sympt")
  message("Jobs done")
  

  
  ################### CHECK RESULTS #########################################
  ######################################################
  
  expected_files<-rbindlist(expected_files)
  for(me in c("aort", "mitral")){
    folder<-paste0(output_folder, "/", me, "/sympt_draws/")
    files<-list.files(folder)
    miss<-setdiff(expected_files$file, files)
    if(length(miss)>0){
      message("Missing ", length(miss), " output files for ", me, "; resubmitting ")
      message("Missing ", length(miss), " output files for ", me, "; resubmitting ")
      
    }
  }

}else{
  message("Uploading existing draws")
}
  
################### SAVE RESULTS #########################################
######################################################
if(save_upload==T){
  for(me in c(upload_mes)){
    USERNAMEmpt_me<-ifelse(me=="aort", 19671, 19672)
    aUSERNAMEmpt_me<-ifelse(me=="aort", 19386, 19387)
    tx_me<-ifelse(me=="aort", 19578, 19582)
    
    
    folder<-paste0(output_folder, "/", me, "/tx_draws/")
    save_results_epi(input_dir=folder, input_file_pattern="{year_id}_{sex_id}_{measure_id}.csv",
                     modelable_entity_id=tx_me, measure_id=5, mark_best=best, description=descr)
    
    folder<-paste0(output_folder, "/", me, "/sympt_draws/")
    save_results_epi(input_dir=folder, input_file_pattern="{year_id}_{sex_id}_{measure_id}.csv",
                     modelable_entity_id=sympt_me, measure_id=5, mark_best=best, description=descr)
    
    ##USERNAME: save both prevalence and incidence to asymptomatic
    folder<-paste0(output_folder, "/", me, "/asympt_draws/") 
    save_results_epi(input_dir=folder, input_file_pattern="{year_id}_{sex_id}_{measure_id}.csv",
                     modelable_entity_id=asympt_me, measure_id=c(5,6), mark_best=best, description=descr)
    
    
  }
}

