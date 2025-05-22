####################################################################
### Purpose: Submit bullying paf jobs in parallel by location
####################################################################

slots <- 5
mem <- paste0(1024 * slots*2, "M")

shell <-"/FILEPATH/execRscript.sh -i /FILEPATH/rstudio/latest.img"
script <- "-s /FILEPATH/GBD2023_bullying_do.R"
project <- ''
sge_output_dir <- '-o /FILEPATH/%x.o%j -e /FILEPATH/errors/%x.o%j '

source("/FILEPATH/get_location_metadata.R")
location_list <- get_location_metadata(location_set_id=35, release_id = 16)[is_estimate==1,]

files_locations <- paste0("/FILEPATH/paf_yld_", location_list$location_id, "_")
files_locations_years <- paste0(files_locations, 1990)
for(y in 1991:2024){
  files_locations_years <- c(files_locations_years, paste0(files_locations, y))
}
files_required <- c(paste0(files_locations_years, "_1.csv"), paste0(files_locations_years, "_2.csv"))

filelist <- list.files(path="/FILEPATH/pafs", pattern = "csv", full.names = T)
missing <- files_required[!(files_required %in% filelist)]

missing <- strsplit(missing, "_")
missing_locations <- unique(unlist(lapply(missing, function(x){x[3]})))

for (location in as.integer(missing_locations)){
  job_name<- paste0('-J paf_',location)
  
  sys_sub<- paste0("sbatch ", job_name, " --mem=", mem, " -c ", slots, " -t 720 -A ", project, " -p all.q ", sge_output_dir)

  system(paste(sys_sub, shell, script, location))
  print(paste(sys_sub, shell, script, location))
}
print("All causes submitted")



