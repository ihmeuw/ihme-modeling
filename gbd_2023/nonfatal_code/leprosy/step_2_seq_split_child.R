# Purpose: upload unadjusted grades 1 & 2 of leprosy
# Notes: write out draws for final MEIDs using proportions split
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH/", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common   IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
    library(argparse, lib.loc = "FILEPATH")
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
    params_dir <- paste0(data_root, "FILEPATH")
    draws_dir <- paste0(data_root, "FILEPATH")
    interms_dir <- paste0(data_root, "FILEPATH")
    logs_dir <- paste0(data_root, "FILEPATH")
}
unloadNamespace("argparse")

# Source relevant libraries
library(data.table)
library(stringr)
source("FILEPATH/get_draws.R")

# Constants
gbd_round_id <- 'ADDRESS'
decomp_step <- 'ADDRESS'

study_dems <- readRDS(paste0(data_root, "FILEPATH", gbd_round_id, ".rds"))
years <- study_dems$year_id

### ======================= MAIN ======================= ###

# # grade 2
grade_2 <- get_draws(gbd_id_type = "model_id",
                     gbd_id = ADDRESS,
                     source = "ADDRESS",
                     measure_id = c(5,6),
                     location_id = location_id,
                     year_id = years,
                     version_id = ADDRESS,
                     gbd_round_id = gbd_round_id,
                     decomp_step = decomp_step)

# hard zero where age is less than 2-4
grade_2[age_group_id <= 5 | age_group_id %in% c(388,389,238), paste0("draw_", 0:999) := 0]

# write out grade 2
grade_2[, model_id:= ADDRESS]
grade_2[, metric_id := 3]
write.csv(grade_2, paste0(draws_dir, "/ADDRESS/", location_id, ".csv"), row.names = FALSE)

#####################

# grade 1
grade_01 <- get_draws(gbd_id_type = "model_id", 
                      gbd_id = ADDRESS, 
                      source = "ADDRESS", 
                      measure_id = c(5,6), 
                      location_id = location_id, 
                      year_id = years,
                      decomp_step = decomp_step, 
                      version_id = 'ADDRESS',
                      gbd_round_id = gbd_round_id)


# hard zero where age is less than 2-4
grade_01[age_group_id <= 5 | age_group_id %in% c(388,389,238), paste0("FILEPATH", 0:999) := 0]
setnames(grade_01, paste0("FILEPATH", 0:999), paste0("FILEPATH", 0:999))

# get grade 1 proportion draws
grade_1_draws <- fread(paste0(data_root, "FILEPATH"))
setnames(grade_1_draws, paste0("FILEPATH", 0:999), paste0("FILEPATH", 0:999))

# merge and calculate gr 1
grade_01 <- merge(grade_01, grade_1_draws, by = c("age_group_id", "sex_id"))

# calculate gr 1 prevalence
grade_01[, paste0("FILEPATH", 0:999) := lapply(0:999, function(x) get(paste0("FILEPATH", x)) * get(paste0("FILEPATH", x)))]

# calculate gr 0 prevalence
grade_01[, paste0("FILEPATH", 0:999) := lapply(0:999, function(x) get(paste0("FILEPATH", x)) * (1 - get(paste0("FILEPATH", x))))]

# split out grade 1, grade0
grade_1 <- grade_01[, c("age_group_id", "sex_id", "location_id", "year_id", "measure_id", paste0("FILEPATH", 0:999))]
grade_0 <- grade_01[, c("age_group_id", "sex_id", "location_id", "year_id", "measure_id", paste0("FILEPATH", 0:999))]

# write grade 1
setnames(grade_1, paste0("FILEPATH", 0:999), paste0("FILEPATH", 0:999))
grade_1[, model_id:= ADDRESS]
grade_1[, metric_id := 3]
write.csv(grade_1, paste0(draws_dir, "/FILEPATH/", location_id, ".csv"), row.names = FALSE)

