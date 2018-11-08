## ******************************************************************************
##
## Purpose: Save a dataset of incidence results for total hepatitis, with one
##          incidence data point for every matching hospital data point
##            Steps:  1. Generate a list of all the unique demographic
##                       combinations that are in the hospital data
##                    2. Pull the incidence for all four hep viruses for
##                       each of the specified demographic combos
##                    3. Save the incid results by virus and summed together
## Input:   - GBD 2017 hospital data
##          - GBD 2017 DisMod incidence
## Output:  Files of incidence draws, both for individual viruses, and aggregated 
##          into total hepatitis
##
## ******************************************************************************

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "/FILEPATH/" 
  h <- "/FILEPATH/"
} else { 
  j <- "J:/"
  h <- "H:/"
}

library(data.table)
source(paste0(j, "FILEPATH/get_model_results.R"))
source(paste0(j, "FILEPATH/interpolate.R"))

task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
loc <- param_map[task_id, location_id]
sex <- param_map[task_id, sex_id]

#current hospital data
dt_total <- fread(paste0(j,"FILEPATH/hep_cfr_hospital_data.csv"))
param_map <- dt_total[!duplicated(dt_total, by = c('location_id', 'sex_id')), c('location_id', 'sex_id')]

draw_cols <- paste0("draw_", 0:999)

inc <- data.table(age_group_id = NA, location_id = NA, modelable_entity_id = NA, sex_id = NA, year_id = NA, mean = NA)

for (type_i in c(18834,18835,18836,18837)) {
  
  model_res <- interpolate(gbd_id_type = "modelable_entity_id",
                     gbd_id = type_i,
                     source = "epi",
                     measure_id = 6,
                     location_id = loc,
                     sex_id = sex,
                     gbd_round_id = 5,
                     status = "best",
                     reporting_year_start = 1980)
  
  model_res$mean <- rowMeans(model_res[,2:1001], na.rm = T)
  model_res <- model_res[, c(1,1002:1009)]
  
  model_res$measure_id <- NULL
  model_res$metric_id <- NULL
  model_res$model_version_id <- NULL
  
  inc <- rbind(inc, model_res)
}

#delete the first placeholder row
inc <- inc[2:nrow(inc),]

#export the virus-specific incidence draws
data_dir <- paste0(j, "FILEPATH/dismod_subtype_pulls/")
write.csv(inc, paste0(data_dir, loc, "_", sex, ".csv"), row.names = FALSE)

#add the four subtypes together and export the total hepatitis draws
inc[, `:=`(total_incid = sum(.SD$mean)),
    by = c('location_id', 'year_id', 'sex_id', 'age_group_id')]
inc <- unique(inc[, .(location_id, year_id, sex_id, age_group_id, total_incid)])

data_dir <- paste0(j, "FILEPATH/dismod_hep_total_pulls/")
write.csv(inc, paste0(data_dir, loc, "_", sex, ".csv"), row.names = FALSE)
