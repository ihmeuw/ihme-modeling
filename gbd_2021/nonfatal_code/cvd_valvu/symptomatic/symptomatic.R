
## Purpose: Model and save symptomatic severity splits

library(data.table)
library(ggplot2)

date<-gsub("-", "_", Sys.Date())

################### ARGS AND PATHS #########################################
######################################################

descr<-"Descr"
upload_mes <- c("aort", "mitral")
decomp_step<-"4"
proj<-"Proj"

asympt_by_age<-T 
calculate_prevalences<-T
save_upload<-T
save_other<-T 
best<-T

asympt_folder<-"FILEPATH"

output_folder<-"FILEPATH"

central<-"FILEPATH"


################### SCRIPTS #########################################
######################################################

source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "save_results_epi.R"))

source("job_hold.R")


################### LAUNCH JOBS  #########################################
######################################################

if(calculate_prevalences==T){
  for(me in c(upload_mes)){
    
    ## clean out old folders
    invisible(lapply(c("tx", "sympt", "asympt"), function(x){
      temp_folder<-paste0("FILEPATH/")
      unlink(temp_folder, recursive=T)
      dir.create(temp_folder)
    }))
    
    prev_me<-ifelse(me=="aort", 16601, 16602)
    message("Creating symptomatic for ", me)
    expected_files<-list()
    for(year in c(seq(from=1990, to=2010, by=5), 2015, 2017, 2019)){
      for(sex in c(1, 2)){
        if(F){
          year<-2017
          prev_me<-16601
          me<-"aort"
          sex<-1
        }
        command <- paste0("QSUB", paste0("sympt_", me, "_", year, "_", sex),
                          " -e FILEPATH -o FILEPATH FILEPATH ",
                          "FILEPATH ",
                          paste0(c(prev_me, year, sex, asympt_by_age, decomp_step), collapse=" "))
        
        #system(command)
        expected_files[[length(expected_files)+1]]<-data.table(year=year, sex=sex, file=paste0(year, "_", sex, "_5.csv"))
      }
    }
  }
  message("Waiting on jobs...")
  job_hold(paste0("sympt_", me))
  message("Jobs done")
  

  
  ################### CHECK RESULTS #########################################
  ######################################################
  
  expected_files<-rbindlist(expected_files)
  for(me in upload_mes){
    folder<-paste0("FILEPATH/")
    files<-list.files(folder)
    miss<-setdiff(expected_files$file, files)
    if(length(miss)>0){
      message("Missing ", length(miss), " output files for ", me, "; resubmitting ")

      for(miss_file in miss){
        
        year<-expected_files[file==miss_file, year]
        sex<-expected_files[file==miss_file, sex]
        
        command <- paste0("QSUB", paste0("sympt_", me, "_", year, "_", sex),
                          " -e FILEPATH -o FILEPATH FILEPATH ",
                          "FILEPATH ",
                          paste0(c(prev_me, year, sex, asympt_by_age, decomp_step), collapse=" "))
        
        system(command)
        
      }
      message("Waiting on re-submitted jobs...")
      job_hold("sympt")
      message("Resubmit jobs done for ", me)
      
      if(!all(file.exists(paste0("FILEPATH/", miss)))){stop("Resubmission jobs broke!")}
      
    }
  }

}else{
  message("Uploading existing draws")
}
  
################### SAVE RESULTS #########################################
######################################################
if(save_upload==T){
  for(me in c(upload_mes)){
    sympt_me<-"VALUE"
    asympt_me<-"VALUE"
    tx_me<-"VALUE"
    
    
    folder<-paste0("FILEPATH/")
    save_results_epi(input_dir=folder, input_file_pattern="{year_id}_{sex_id}_{measure_id}.csv",
                     modelable_entity_id=tx_me, measure_id=5, mark_best=best, description=descr,
                     decomp_step=paste0("step", decomp_step))
    
    folder<-paste0("FILEPATH/")
    save_results_epi(input_dir=folder, input_file_pattern="{year_id}_{sex_id}_{measure_id}.csv",
                     modelable_entity_id=sympt_me, measure_id=5, mark_best=best, description=descr,
                     decomp_step=paste0("step", decomp_step))
    
    ## save both prevalence and incidence to asymptomatic
    folder<-paste0("FILEPATH/") 
    save_results_epi(input_dir=folder, input_file_pattern="{year_id}_{sex_id}_{measure_id}.csv",
                     modelable_entity_id=asympt_me, measure_id=c(5,6), mark_best=best, description=descr,
                     decomp_step=paste0("step", decomp_step))
    
    
  }
}

