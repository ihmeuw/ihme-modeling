### Squeeze Code -----------------------------------------

### Description 
# 1) Squeeze Stage 3, Stage 4, and Stage 5 CKD into the parent CKD 3-5 model
# 2) Squeeze ESRD Type 1 Diabetes and Type 2 Diabetes proportions into the parent ESRD diabetes dismod model
#     2a) Nonfatal: Used to split transplant and dialysis models by etiologies
#     2b) Fatal: Used to split fatal CKD models by etiologies

### Process
# 1) Give a csv mapping file to specify model numbers, database values. Example given in code
# 2) Run squeeze code
# 3) Save results

############### ------------------ ###############
############### Running code below ###############
############### ------------------ ###############

source("FILEPATH/squeeze_split_save_functions.R")

path_to_map <- "FILEPATH/ckd_stages_squeeze_map.csv"
path_to_error <- "FILEPATH"
path_to_shell <- 'FILEPATH'

run_squeeze_code(path_to_errors = path_to_error, 
                 path_to_settings = path_to_map,
                 are_you_testing = 1,
                 shell = path_to_shell)

### Check Missing Locations -------------------------------
saved <- mapping[!is.na(mapping$target_me_id), ]
for (i in 1:nrow(saved)) {
  check_missing_locs(indir = paste0(saved$directory_to_save[i], saved$target_me_id[i], "/"),
                     filepattern = "{location_id}.csv",
                     team = "epi")
}

### Save Squeeze Draws ---------------------------------------
save_results_function(path_to_errors = path_to_error, 
                   path_to_settings = path_to_map,
                   description = "Enter Model Description Here")

