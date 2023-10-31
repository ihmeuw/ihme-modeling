####################################################
## Author: USERNAME
## Date: 8/19/2019
## Description: Epilepsy CSMR Post-Hoc Adjustment Script to try fix Mali female
####################################################
#Gets all step 2 CSMR data including Mali female, will be dropped later
rm(list=ls())

library(data.table)

shared_functions_dir <- "FILEPATH"
home_dir <- "FILEPATH" 
version <- "v2"

## LOCATIONS
source(paste0(shared_functions_dir,"/get_location_metadata.R"))
loc_table <- get_location_metadata(gbd_round_id = 7, location_set_id=35)
loc_table <- loc_table[most_detailed == 1, ]
loc_table <- loc_table[,c("location_id","location_name")]
locations <- data.table(location = loc_table$location_id)
locations[, task_num := 1:.N]
main_dir <- "FILEPATH"
save_dir <- paste0(main_dir, version, "/")
if(!dir.exists(save_dir)) dir.create(save_dir)
map_path <- paste0(main_dir, "location_map.csv")
write.csv(locations, map_path, row.names = F)


## SUBMIT JOBS

project <- "-P proj_yld "
sge_output_dir <- "-o FILEPATH "
sge_error_dir <- "-e FILEPATH "
q <- " -q i.q "
memory <- 10
threads <- 5 
script <- paste0(home_dir, "/2020_iter_step3_277_01_create_csmr.R")
shell <- " FILEPATH -i FILEPATH " 
n_jobs <- nrow(locations)
job_name <- "-N epilepsy_csmr "

#Checking file existence
file.exists(script) #Without the space script exists
file.exists(save_dir) #exists
file.exists(map_path) #exists

code_command <- paste(shell, "-s", script, save_dir, map_path)
full_command <- paste0("qsub ", job_name, project, sge_output_dir, sge_error_dir, "-t ", 
                       paste0("1:", n_jobs), q, "-l archive=True ", "-l fthread=", threads, ' -l m_mem_free=', memory, 'G', 
                       code_command)
print(full_command)
system(full_command)



# ONCE ALL THE JOBS FINISH ---------------------------------------------------
# Bind together 
files <- list.files(path = save_dir, full.names = TRUE)
length(files)
temp <- lapply(files, fread, sep=",")
dt_all <- rbindlist(temp, fill = TRUE, use.names = TRUE)

drop_cols <- c("cause_id", "measure_id", "metric_id", "run_id")
dt_all[, (drop_cols) := NULL]
#Fixing age_start and age_end to make it run faster
dt_all[age_end > 99, age_end := 99]
# dt_all[, age_mid := (age_start + age_end) / 2] #Point estimates of 
# dt_all[, age_start := age_mid]
# dt_all[, age_end := age_mid]
# dt_all[, age_mid := NULL]
dt_all[, nid := 416752] # this is the NID specific for cause specific EMR 

date <- gsub("-", "_", Sys.Date())

write.csv(dt_all, paste0("FILEPATH", date, "iterative_277_cod_step2_CSMR_data.csv"), row.names = F)
