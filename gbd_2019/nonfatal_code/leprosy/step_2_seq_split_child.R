# pass gr2, split gr1
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-"FILEPATH"
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common IHME IO Paths
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
    params_dir <- "FILEPATH"
    draws_dir <- "FILEPATH"
    interms_dir <- "FILEPATH"
    logs_dir <- "FILEPATH"
    location_id <- ADDRESS
}
unloadNamespace("argparse")

# Source relevant libraries
library(data.table)
library(stringr)
source("FILEPATH")

# Constants
gbd_round_id <- 7
decomp_step <- 'step2'

study_dems <- readRDS("FILEPATH")    ## demographics
years <- study_dems$year_id

### ======================= MAIN ======================= ###

# grade 2
grade_2 <- get_draws(gbd_id_type = "modelable_entity_id", 
                     gbd_id = ADDRESS0, 
                     source = "epi", 
                     measure_id = c(5,6), 
                     location_id = location_id, 
                     year_id = years, 
                     status = "best", 
                     gbd_round_id = gbd_round_id,
                     decomp_step = decomp_step)

# hard zero where age is less than 2-4
grade_2[age_group_id <= ADDRESS1 | age_group_id %in% c(ADDRESS2,ADDRESS3,ADDRESS4), paste0("draw_", 0:999) := 0]

# write out grade 2
grade_2[, modelable_entity_id := ADDRESS]
grade_2[, metric_id := 3]
write.csv(grade_2, paste0(draws_dir, "/ADDRESS/", location_id, ".csv"), row.names = FALSE)

#####################

# grade 1
grade_01 <- get_draws(gbd_id_type = "modelable_entity_id", 
                      gbd_id = ADDRESS5, 
                      source = "epi", 
                      measure_id = c(5,6), 
                      location_id = location_id, 
                      year_id = years,
                      decomp_step = decomp_step, 
                      status = "best", 
                      gbd_round_id = gbd_round_id)


# hard zero where age is less than 2-4
grade_01[age_group_id <= ADDRESS1 | age_group_id %in% c(ADDRESS2,ADDRESS3,ADDRESS4), paste0("draw_", 0:999) := 0]
setnames(grade_01, paste0("draw_", 0:999), paste0("grade_01_draw_", 0:999))

# get grade 1 proportion draws
grade_1_draws <- fread("FILEPATH")
setnames(grade_1_draws, paste0("draw_", 0:999), paste0("prop_g1_draw_", 0:999))

# merge and calculate gr 1
grade_01 <- merge(grade_01, grade_1_draws, by = c("age_group_id", "sex_id"))

# calculate gr 1 prevalence
grade_01[, paste0("grade_1_draw_", 0:999) := lapply(0:999, function(x) get(paste0("grade_01_draw_", x)) * get(paste0("prop_g1_draw_", x)))]

# calculate gr 0 prevalence
grade_01[, paste0("grade_0_draw_", 0:999) := lapply(0:999, function(x) get(paste0("grade_01_draw_", x)) * (1 - get(paste0("prop_g1_draw_", x))))]

# split out grade 1, grade0
grade_1 <- grade_01[, c("age_group_id", "sex_id", "location_id", "year_id", "measure_id", paste0("grade_1_draw_", 0:999))]
grade_0 <- grade_01[, c("age_group_id", "sex_id", "location_id", "year_id", "measure_id", paste0("grade_0_draw_", 0:999))]

# write grade 1
setnames(grade_1, paste0("grade_1_draw_", 0:999), paste0("draw_", 0:999))
grade_1[, modelable_entity_id := ADDRESS2]
grade_1[, metric_id := 3]
write.csv(grade_1, "FILEPATH"), row.names = FALSE)

# write grade0 (currently does not have a disability weight / is not uploaded)
# setnames(grade_0, paste0("grade_0_draw_", 0:999), paste0("draw_", 0:999))
# grade_0[, modelable_entity_id := NULL]
# write.csv(grade_0, "FILEPATH"), row.names = FALSE)
