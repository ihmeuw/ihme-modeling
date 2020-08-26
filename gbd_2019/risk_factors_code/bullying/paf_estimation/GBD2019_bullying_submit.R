####################################################################
### Purpose: Submit bullying paf jobs in parallel by location
####################################################################

slots<- 4
mem <- paste0(1024 * slots*2, "M")
shell <-"/FILEPATH.sh"
script <- "/FILEPATH.R"
project <- 'PROJECT '
sge_output_dir <- '-o /FILEPATH -e /FILEPATH'

source("/FILEPATH/get_location_metadata.R")
location_list <- get_location_metadata(location_set_id=35)[is_estimate==1,]
for (location in location_list$location_id){
  job_name<- paste0('-N loc_',location)
  sys_sub<- paste0("qsub -P ", project, " ", job_name, " ", sge_output_dir, " -l m_mem_free=", mem, " -l fthread=", slots, " -l h_rt=24:00:00 -q all.q -l archive=TRUE")
  system(paste(sys_sub, shell, script, location))
  print(paste(sys_sub, shell, script, location))
}
print("All causes submitted")
