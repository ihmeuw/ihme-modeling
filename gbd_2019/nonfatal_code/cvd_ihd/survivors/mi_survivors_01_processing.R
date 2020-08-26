###########################################################################################################
# CALCULATE MI SURVIVORSHIP
###########################################################################################################

###########################################################################################################
# SET UP
###########################################################################################################

rm(list=ls())

#location <- 81
decomp_step <- 'step4'

j <- "FILEPATH"
central <- paste0(j, "FILEPATH")

source("FILEPATH/get_ids.R")
source("FILEPATH/get_draws.R")

# get parameters
meid <- 24694 # Acute myocardial infarction
ages <- c(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235) # all ages above 1

# pull in parameters
args <- commandArgs(trailingOnly = TRUE)
parameters_filepath <- args[1]
print(parameters_filepath)
decomp_step <- args[2]

## Retrieving array task_id
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
print(paste("task_id: ", task_id))
parameters <- fread(parameters_filepath)
location <- parameters[task_id, locs]
print(paste("location_id: ", location))

# load libraries
library(data.table)

# set directory paths
out_path <- paste0("FILEPATH")
dir.create(out_path, showWarnings = FALSE)

###########################################################################################################
# OBTAIN AND FORMAT DRAWS
###########################################################################################################
# Pull in draws for excess mortality and incidence
dt <- get_draws(gbd_id_type = "modelable_entity_id",
                gbd_id = meid,
                measure_id = c(6,9),
                location_id = location,
                age_group_id = ages,
                status = "best",
                source = "epi",
                decomp_step = decomp_step,
                gbd_round_id=6)

# generate case fatality rate from excess mortality rate
cfr <- dt[measure_id==9]
cfr[, paste0("cf_", c(0:999)) := lapply(c(0:999), function(x) get(paste0("draw_", x)) / (12+get(paste0("draw_", x))))]
cfr <- cfr[, grep("draw_", names(cfr)) := NULL]

# get incidence data
incidence <- dt[measure_id==6]
for (i in 0:999) {
  colnames(incidence)[colnames(incidence)==paste0("draw_", i)] <- paste0("incidence_", i)
}

# merge and transform incidence and cfr data
survivors <- merge(cfr, incidence, by=c('age_group_id', 'location_id', 'year_id', 'sex_id'), type='inner')
survivors[, paste0("chronic_incidence_", c(0:999)) := lapply(c(0:999), function(x) get(paste0("incidence_", x)) * (1-get(paste0("cf_", x))))]
survivors[, mean := rowMeans(.SD), .SDcols = paste0("chronic_incidence_", 0:999)]
survivors[, upper := apply(.SD, 1, quantile, 0.975), .SDcols = paste0("chronic_incidence_", 0:999)]
survivors[, lower := apply(.SD, 1, quantile, 0.025), .SDcols = paste0("chronic_incidence_", 0:999)]
survivors <- survivors[,.(location_id, year_id, age_group_id, sex_id, mean, lower, upper)]

###########################################################################################################
# SAVE AND OUTPUT FILE
###########################################################################################################
saveRDS(survivors, paste0(out_path, "me_survivors_", location, ".rds"))
