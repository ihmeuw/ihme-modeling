# Scale prevalence of neurcysticercosis by epilepsy envelope and muslim proportion
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH")

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
    library(argparse)
    print(commandArgs())
    parser <- ArgumentParser()
    parser$add_argument("--params_dir", type = "character")
    parser$add_argument("--draws_dir", type = "character")
    parser$add_argument("--interms_dir", type = "character")
    parser$add_argument("--logs_dir", type = "character")
    parser$add_argument("--release_id", type = "character")
    parser$add_argument("--location_id", type = "character")
    args <- parser$parse_args()
    print(args)
    list2env(args, environment()); rm(args)
    sessionInfo()
} else {
    data_root <- "FILEPATH"
    params_dir <- "FILEPATH"
    draws_dir <- "FILEPATH"
    interms_dir <- "FILEPATH"
    logs_dir <- "FILEPATH"
}

# Source relevant libraries
library(data.table)
library(stringr)
source("FILEPATH")

# Constants
release_id <- ADDRESS
study_dems <- get_demographics(gbd_team="epi", release_id=release_id)
epilepsy_version_id <- ID
neurocc_version_id <- ID

### ======================= MAIN ======================= ###
# Load and merge cysticercosis and epilepsy by location
neurocc_draws<- get_draws(gbd_id_type="modelable_entity_id", gbd_id=ID, source="epi", measure_id=5, version_id=neurocc_version_id,
    location_id=location_id, age_group_id=study_dems$age_group_id, year_id=study_dems$year_id, release_id = release_id)
setnames(neurocc_draws, old=grep("draw*", names(neurocc_draws), value = TRUE), new=paste0("ncc_prev_", 0:999))

epilepsy_draws<- get_draws(gbd_id_type="modelable_entity_id", gbd_id=ID, source="epi", measure_id=5, version_id=epilepsy_version_id,
    location_id=location_id, age_group_id=study_dems$age_group_id, year_id=study_dems$year_id, release_id = release_id)

setnames(epilepsy_draws, old=grep("draw*", names(epilepsy_draws), value = TRUE), new=paste0("epilepsy_", 0:999))

all_draws <- merge(neurocc_draws, epilepsy_draws, all.x=TRUE, all.y=TRUE, by=c("location_id", "year_id", "sex_id", "age_group_id"))

# read and merge religion covariate and population not at risk draws
par_draws <- as.data.table(read.csv("FILEPATH"))
all_draws <- as.data.table(merge(all_draws, par_draws, all.x=TRUE, all.y=FALSE, by=c("location_id", "year_id")))

### final results
all_draws <- all_draws[, paste0("draw_", 0:999) := lapply( 0:999, function(x) 
                                                                  get(paste0("epilepsy_", x)) * 
                                                                  ( get(paste0("ncc_prev_", x)) * get(paste0("religion_muslim_prop_", x)) - get(paste0("ncc_prev_", x)) ) / 
                                                                  ( get(paste0("ncc_prev_", x)) * get(paste0("religion_muslim_prop_", x)) - 1 )
                                                          )]

all_draws[, "modelable_entity_id" := ID]
all_draws[, "measure_id" := 5]
all_draws[, "metric_id" := 3]
sub_cols <- append(c("modelable_entity_id", "measure_id", "metric_id", "location_id", "year_id", "age_group_id", "sex_id"), 
                   grep("draw_*", names(all_draws), value = TRUE))
all_draws <- all_draws[, ..sub_cols]

path_all_draws <-  paste0(draws_dir, "/FILEPATH/")
if (!dir.exists(path_all_draws)){
    dir.create(path_all_draws)
}

write.csv(all_draws, "FILEPATH", row.names=FALSE)
