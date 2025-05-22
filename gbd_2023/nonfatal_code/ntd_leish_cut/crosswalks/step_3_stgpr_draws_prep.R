# NTDS: Cutaneous leishmaniasis
# Purpose: create estimates from ST-GPR inputs 
# Description: copy ST/GPR results from CL incidence model into location-specific folders

pacman::p_load(data.table, dplyr,tidyr,stringr)

data_root <- "FILEPATH"
run_file <- fread(paste0(data_root, "/FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
interms_dir    <- paste0(run_dir, "/interms/")

run_id <- ID

param_map <- fread("FILEPATH")
task_id <- as.integer(Sys.getenv("ADDRESS"))
i <- param_map[task_id, location_id]

dir.create(paste0(interms_dir, 'FILEPATH/'))

###Write st/gpr draws out to location specific csv files

upload_file <- fread(paste0("/FILEPATH"))
upload_file$modelable_entity_id <- ID
upload_file$measure_id <- 6

upload_file[, paste0("draw_",0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) / 100000)] #convert rates from per 100000 to 1

write.csv(upload_file,(paste0(interms_dir, "/FILEPATH")))  