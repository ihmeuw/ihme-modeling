 # Scale prevalence of neurcysticercosis by epilepsy envelope and muslim proportion
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7], "/")
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev)
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
    library(argparse)
    print(commandArgs())
    parser <- ArgumentParser()
    parser$add_argument("--params_dir", type = "character")
    parser$add_argument("--draws_dir", type = "character")
    parser$add_argument("--interms_dir", type = "character")
    parser$add_argument("--logs_dir", type = "character")
    parser$add_argument("--location_id", type = "character")
    args <- parser$parse_args()
    print(args)
    list2env(args, environment()); rm(args)
    sessionInfo()
} else {
    params_dir <- paste0(data_root, "FILEPATH/params")
    draws_dir <- paste0(data_root, "FILEPATH/draws")
    interms_dir <- paste0(data_root, "FILEPATH/interms")
    logs_dir <- paste0(data_root, "FILEPATH/logs")
    location_id <- 122
}

# Source relevant libraries
library(data.table)
library(stringr)
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_draws.R")

# Constants
gbd_round_id <- 7
study_dems <- readRDS(paste0(data_root, "FILEPATH", gbd_round_id, ".rds"))
epilepsy_version_id <- ADDRESS
neurocc_version_id <- ADDRESS
### ======================= MAIN ======================= ###

# Load and merge neurocc and epilepsy for location
neurocc_draws<- get_draws(gbd_id_type="ADDRESS", gbd_id=ADDRESS, source="ADDRESS", measure_id=5, version_id=neurocc_version_id,
    location_id=location_id, age_group_id=study_dems$age_group_id, year_id=study_dems$year_id, gbd_round_id=gbd_round_id, decomp_step="ADDRESS")
setnames(neurocc_draws, old=grep("draw*", names(neurocc_draws), value = TRUE), new=paste0("ncc_prev_", 0:999))


epilepsy_draws<- get_draws(gbd_id_type="ADDRESS", gbd_id=ADDRESS, source="ADDRESS", measure_id=5, version_id=epilepsy_version_id,
    location_id=location_id, age_group_id=study_dems$age_group_id, year_id=study_dems$year_id, gbd_round_id=gbd_round_id, decomp_step="ADDRESS")


setnames(epilepsy_draws, old=grep("draw*", names(epilepsy_draws), value = TRUE), new=paste0("epilepsy_", 0:999))

all_draws <- merge(neurocc_draws, epilepsy_draws, all.x=TRUE, all.y=TRUE, by=c("location_id", "year_id", "sex_id", "age_group_id"))

# Read and merge religion cov and npar draws
par_draws <- as.data.table(read.csv(paste0(interms_dir, "FILEPATH")))
all_draws <- as.data.table(merge(all_draws, par_draws, all.x=TRUE, all.y=FALSE, by=c("location_id", "year_id")))

### Final Combine / Save Out

all_draws <- all_draws[, paste0("draw_", 0:999) := lapply( 0:999, function(x) 
                                                                  get(paste0("epilepsy_", x)) * 
                                                                  ( get(paste0("ncc_prev_", x)) * get(paste0("religion_muslim_prop_", x)) - get(paste0("ncc_prev_", x)) ) / 
                                                                  ( get(paste0("ncc_prev_", x)) * get(paste0("religion_muslim_prop_", x)) - 1 )
                                                          )]
all_draws[, "model_id" := ADDRESS]
all_draws[, "measure_id" := 5]
all_draws[, "metric_id" := 3]
sub_cols <- append(c("model_id", "measure_id", "metric_id", "location_id", "year_id", "age_group_id", "sex_id"), 
                   grep("draw_*", names(all_draws), value = TRUE))
all_draws <- all_draws[, ..sub_cols]
path_all_draws <-  paste0(draws_dir, "FILEPATH")
if (!dir.exists(path_all_draws)){
    dir.create(path_all_draws)
}
write.csv(all_draws, paste0(path_all_draws, location_id, ".csv"), row.names=FALSE)
