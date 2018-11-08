#####################################INTRO#############################################
#' Author: 
#' 6/14/18
#' Purpose: Age-restirct PID prevalence to limit what we report as PID to those in younger ages (15 - 60)
#'          1) Get draws for prevalence in given location
#'          2) Drop measures to 0 for ages outside of age range
#'          3) Save as a csv
#'
#' OUTPUTS: {location_id}.csv
#'
#####################################INTRO#############################################

library("ihme", lib.loc = "FILEPATH")
setup()

source_functions(get_draws = T)
library(data.table)

# Read in commandline args ------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
out_dir         <- args[1]
location_id     <- args[2]
age_group_start <- args[3]
age_group_end   <- args[4]
# location_id <- 10

# save-results will fail if you upload with the wrong me_id
upload_me_id <- 20419

message(paste(Sys.time(), "Beginning pelvic inflammatory disease age-restriction in location_id", location_id))

# Get prev & inc draws ----------------------------------------------------

# get_draws parameters

gbd_type_id <- "modelable_entity_id"
me_id       <- 2423                # PID envelope 
measure_ids <- c(5, 6)             # measure ids for prevalence and incidence
females     <- 2                   # only females are affected by PID
age_groups  <- c(2:20, 30:32, 235) # 5 year age groups
status      <- "best"              # grab the best model

message(paste(Sys.time(), "Getting draws from parent model in location", location_id))

draws <- get_draws(gbd_id_type = gbd_type_id, gbd_id = me_id, source = "epi", 
                   sex_id = females, location_id = location_id, measure_id = measure_ids, 
                   age_group_id = age_groups, status = status)

adjusted <- copy(draws)

# Apply age-restriction and save  ------------------------------------------
# setting draws to 0 if NOT in age range (15 - 60 currently)
message(paste(Sys.time(), "Adjusting prevalence and incidence to 0 for age groups outside of", age_group_start, "-", age_group_end, "for location", location_id))

draw_names <- paste0("draw_", 0:999)
adjusted[!age_group_id %in% c(age_group_start:age_group_end), (draw_names) := lapply(.SD,  function(x) { x = 0 } ), .SDcols = draw_names]
adjusted[, modelable_entity_id := upload_me_id]


message(paste(Sys.time(), "Saving adjusted prevalence for location", location_id))

readr::write_csv(adjusted, paste0(out_dir, location_id, ".csv"))



message(paste(Sys.time(), "COMPLETE"))


