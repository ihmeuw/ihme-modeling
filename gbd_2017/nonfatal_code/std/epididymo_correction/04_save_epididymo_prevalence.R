#####################################INTRO#############################################
#' Author: 
#' 5/21/18
#' Purpose: After running epididymo-orchitis dismod model(s) for chlamydia/gonorrhea, 
#'          we need to take ONLY prevalence and save the draws wide for upload using shared function
#'          1) Get draws
#'          2) ...
#'          3) Save!
#'
#' OUTPUTS: csv for each location with name: epididymo_prev_draws/{me_id}_{location_id}.csv
#'
#####################################INTRO#############################################


library("ihme", lib.loc = "FILEPATH")
ihme::setup()

library(readr)
library(magrittr)
library(data.table)

source_functions(get_draws = T)


# Read in commandline args and set up -------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
location_id <- args[1]
me_id       <- args[2]
main_dir    <- args[3]
# location_id <- 102
# me_id <- 20393

# ME ID of the final model we want to upload to,
# NOT the intermediate dismod model me id
upload_me_id <- if (me_id == 20394) {
  1630 # EO due to chlamydia
} else if (me_id == 20393) {
  1636 # EO due to gonorrhea
}

message(paste(Sys.time(), "Beginning epididymo-orchitis prevalence saving for cause id", me_id,
               "in location_id", location_id))


# Get draws and save ------------------------------------------------------


gbd_type_id <- "modelable_entity_id"
prevalence  <- 5                   # measure id for prevalence
males       <- 1                   # only males are affected by EO
age_groups  <- c(2:20, 30:32, 235) # 5 year age groups
status      <- "best"              # grab the best model


message(paste(Sys.time(), "Getting prevalence draws from parent model in location", location_id))

prevalence <- get_draws(gbd_id_type = gbd_type_id, gbd_id = me_id, source = "epi", 
                        sex_id = males, location_id = location_id, measure_id = prevalence, 
                        age_group_id = age_groups, status = status)

# ME ID must match the me_id draws are uploaded to
prevalence[, modelable_entity_id := upload_me_id]


message(paste(Sys.time(), "Saving parent incidence for location", location_id))

# save parent incidence in the form of: {me_id}_{location_id}.csv
readr::write_csv(prevalence, paste0(main_dir, "EX/", me_id, "_", location_id, ".csv"))














