
slots<- 4
mem <- paste0(1024 * slots*2, "M")
shell <-"/FILEPATH/prod_cluster_shell.sh"
script <- "/FILEPATH/GBD2020_bullying_do.R"
project <- 'proj_uq'
sge_output_dir <- '-o /FILEPATH/output -e /FILEPATH/errors '

source("/FILEPATH/get_location_metadata.R")
location_list <- get_location_metadata(location_set_id=35, gbd_round_id = 7, decomp_step = 'iterative')[is_estimate==1,]

files_locations <- paste0("/FILEPATH/paf_yld_", location_list$location_id, "_")
files_locations_years <- paste0(files_locations, 1990)
for(y in 1991:2022){
  files_locations_years <- c(files_locations_years, paste0(files_locations, y))
}
files_required <- c(paste0(files_locations_years, "_1.csv"), paste0(files_locations_years, "_2.csv"))

filelist <- list.files(path="/FILEPATH/pafs", pattern = "csv", full.names = T)
missing <- files_required[!(files_required %in% filelist)]

missing <- strsplit(missing, "_")

missing_locations <- unique(unlist(lapply(missing, function(x){x[3]})))

for (location in as.integer(missing_locations)){
  job_name<- paste0('-N paf_',location)
  sys_sub<- paste0("qsub -P ", project, " ", job_name, " ", sge_output_dir, " -l m_mem_free=", mem, " -l fthread=", slots, " -l h_rt=24:00:00 -q all.q -l archive=TRUE")
  system(paste(sys_sub, shell, script, location))
  print(paste(sys_sub, shell, script, location))
}
print("All causes submitted")

# covid_adjustment --------------------------------------------------------
files_locations_covid <- paste0("/FILEPATH/paf_yld_", location_list$location_id, "_")
files_locations_covid <- paste0(files_locations_covid, 2020)
files_locations_covid <- c(paste0(files_locations_covid, "_1.csv"), paste0(files_locations_covid, "_2.csv"))

filelist_covid <- list.files(path="/FILEPATH/pafs_2020_aftercovid", pattern = "csv", full.names = T)
missing_covid <- files_locations_covid[!(files_locations_covid %in% filelist_covid)]

missing_covid <- strsplit(missing_covid, "_")

missing_locations_covid <- unique(unlist(lapply(missing_covid, function(x){x[5]})))

script_covid <- "/FILEPATH/GBD2020_remove_covid_proportion.R"

for (location in as.integer(missing_locations_covid)){
  job_name<- paste0('-N paf_cov_',location)
  sys_sub<- paste0("qsub -P ", project, " ", job_name, " ", sge_output_dir, " -l m_mem_free=", mem, " -l fthread=", slots, " -l h_rt=24:00:00 -q all.q -l archive=TRUE")
  system(paste(sys_sub, shell, script_covid, location))
  print(paste(sys_sub, shell, script_covid, location))
}

print("All causes submitted")


# Apply covid correction --------------------------------------------------

##### Back up original unadjustd files
filelist_original <- list.files(path="/FILEPATH/pafs", pattern = "csv", full.names = T)
filelist_original <- filelist_original[grepl("_2020_", filelist_original)]

file.copy(filelist_original, "/FILEPATH/pafs_2020_beforecovid")

file_names <- gsub("/FILEPATH/", "", filelist_original)

# Check all files copied properly
file_names[!(file_names %in% list.files(path="/FILEPATH/pafs_2020_beforecovid", pattern = "csv", full.names = F))]

# Remove original files
file.remove(filelist_original)

##### Check all covid_adjusted files are present
file_names[!(file_names %in% list.files(path="/FILEPATH/pafs_2020_aftercovid", pattern = "csv", full.names = F))]

##### Move over covid files to paf folder
filelist_new <- list.files(path="/FILEPATH/pafs_2020_aftercovid", pattern = "csv", full.names = T)

file.copy(filelist_new, "/FILEPATH/pafs")

gsub("/FILEPATH/pafs/", "", filelist_original)[!(gsub("/FILEPATH/pafs/", "", filelist_original) %in% list.files(path="/FILEPATH/pafs_2020_beforecovid", pattern = "csv", full.names = F))]

file_names[!(file_names %in% list.files(path="FILEPATH/pafs", pattern = "csv", full.names = F))]

