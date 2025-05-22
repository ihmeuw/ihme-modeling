#################################
### Save severity split draws ###

rm(list=ls())

# Source shared functions -------------------------------------------------
source("/FILEPATH/save_results_epi.R")

# Load metadata -----------------------------------------------------------
gbd_release <- 16
mes_map <- data.table(me_id=9424:9427, severity=c("mild", "moderate", "severe", "profound"))

submit <- F
if(submit){
  #slots<- 24
  #mem <- "200G"
  slots<- 10
  mem <- "40G"
  shell <-"/FILEPATH/execRscript.sh"
  script <- "-s /FILEPATH/id_severity_save.R"
  project <- 'proj_uq'
  sge_output_dir <- '-o /FILEPATH/%x.o%j -e /sFILEPATH/errors/%x.o%j '
  
  for(me in mes_map$me_id){
    job_name<- paste0('-J me_',me)
    
    sys_sub<- paste0("sbatch ", job_name, " --mem=", mem, " -c ", slots, " -t 720 -A ", project, " -p long.q ", sge_output_dir)
    
    system(paste(sys_sub, shell, script, me))
    print(paste(sys_sub, shell, script, me))
  }
  print("All causes submitted")
}

args<-commandArgs(trailingOnly = TRUE)
me <- args[1]
  
draw_files <- paste0('/FILEPATH/id/', me, "/")

draws <- 1000

description <- paste0("")

measures_to_save <- c(5)

best <- T

#years_to_save <- c(1990:2022)
years_to_save <- c(1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022, 2023, 2024)

# Save results ------------------------------------------------------------
save_results_epi(modelable_entity_id = me, year_id = years_to_save, description=description, input_dir=draw_files,
                 input_file_pattern="id_split_draws_{location_id}.csv", mark_best = best, release_id = gbd_release, measure_id=measures_to_save, bundle_id = 291, crosswalk_version_id = 44431)





