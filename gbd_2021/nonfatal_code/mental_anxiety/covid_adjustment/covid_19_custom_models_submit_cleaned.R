
slots<- 10
mem <- paste0(1024 * slots*2, "M")
shell <-"/FILEPATH/execRscript.sh -i /FILEPATH/ihme_rstudio_3631.img"

script <- "-s /FILEPATH/covid_19_custom_models_do.R"

project <- 'PROJECT '
sge_output_dir <- '-o /FILEPATH/%x.o%j -e /FILEPATH/%x.o%j '

source("/FILEPATH/get_location_metadata.R")
location_list <- get_location_metadata(location_set_id=35, gbd_round_id = 7, decomp_step = 'iterative')[is_estimate==1,]

location_list <- location_list$location_id

filelist <- list.files(path="/FILEPATH/", pattern = "csv", full.names = F)
filelist <- gsub("prev_", "", filelist)
filelist <- gsub(".csv", "", filelist)

files_needed <- paste0(location_list, "_", 2022)
files_needed <- files_needed[!(files_needed %in% filelist)]

files_needed <- unique(as.numeric(gsub("_2022", "", files_needed)))
files_needed

for (location in files_needed){
  job_name<- paste0('-J loc_',location)
  
  sys_sub<- paste0("sbatch ", job_name, " --mem=", mem, " -c ", slots, " -t 120 -A ", project, " -p QUEUE -C archive ", sge_output_dir)
  system(paste(sys_sub, shell, script, location))
  print(paste(sys_sub, shell, script, location))
}
print("All causes submitted")
