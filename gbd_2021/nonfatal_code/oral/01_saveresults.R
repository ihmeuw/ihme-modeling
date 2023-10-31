
arg <- commandArgs(trailingOnly = TRUE)
me <- arg[1]

library("openxlsx")

#############################################

source("FILEPATH")

#############################################

upload_map <- read.csv(paste0("FILEPATH"))

bundle_map <- read.xlsx(paste0("FILEPATH"))

if(me == 3083){
  draw_me <- 2336
} else if(me == 3084){
  draw_me <- 2335
} else{
  draw_me <- me
}

comment <- paste0(upload_map$message[upload_map$me_id == me])

bundle_id <- bundle_map$bundle_id[bundle_map$me_id == me]

crosswalk_version_id <- bundle_map$crosswalk_version_id[bundle_map$me_id == me]


print("Starting save")

save_results_epi(modelable_entity_id = me,
                 input_dir = paste0("FILEPATH", draw_me, "/01_draws"),
                 input_file_pattern = "{measure_id}_{location_id}_{year_id}_{sex_id}.csv",
                 description = paste0(me, "Upload for GBD 2020, run_id 10101, split off of parent dental model"),
                 gbd_round_id = 7,
		 decomp_step = 'iterative',
		 bundle_id = bundle_id,
		 crosswalk_version_id = crosswalk_version_id,
		 mark_best=TRUE)
