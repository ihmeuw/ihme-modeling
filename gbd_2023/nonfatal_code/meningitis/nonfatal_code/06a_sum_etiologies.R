#####################################################################################################################################################################################
#####################################################################################################################################################################################
##                                                                                                                                                                                 ##
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code                                                                                        ##
## Description:	Sum the population-denominator incidence of sequela for different etiologies (bacterial and viral) to get total incidence of each sequela for all meningitis       ##
##                                                                                                                                                                                 ##
#####################################################################################################################################################################################
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
pacman::p_load(R.utils, openxlsx, data.table, dplyr)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path("FILEPATH"))
years <- demographics$year_id
sexes <- demographics$sex_id
locations <- demographics$location_id

# Source shared functions-------------------------------------------------------
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# Parallelized across MEID: get metadata for each MEID
dim.dt <- fread(file.path("FILEPATH"))
parent_meid <- dim.dt[grouping == "cases" & acause == cause & healthstate == "_parent"]$modelable_entity_id

group <- unique(dim.dt[modelable_entity_id == meid, grouping])
state <- unique(dim.dt[modelable_entity_id == meid, healthstate])

# Make the temp healthstate table for later calculation of YLDs
healthstates <- read.xlsx("FILEPATH") %>% as.data.table
# Make a fake row for "epilepsy" 
epilepsy_mean <- healthstates[sequela_name %like% "epilepsy", (mean = mean(mean, na.rm = TRUE))]
healthstates <- healthstates[sequela_name %like% "meningitis"]
healthstates <- healthstates[,.(sequela_name, healthstate_name, mean)]
hearing_mean <- healthstates[sequela_name %like% "hearing loss", (mean = mean(mean))]
vision_mean <- healthstates[sequela_name %like% "Blindness" | sequela_name %like% "vision impairment", (mean = mean(mean))]
healthstates <- healthstates[!sequela_name %like% "hearing loss" & !sequela_name %like% "Blindness" & !sequela_name %like% "vision impairment"]
healthstates <- rbind(healthstates,
                      data.table(sequela_name = c("Hearing loss due to meningitis unsqueezed", "Vision impairment due to meningitis unsqueezed"),
                                 healthstate_name = c("Custom hearing", "Custom vision"),
                                 mean = c(hearing_mean, vision_mean)))
healthstates[sequela_name %like% "Epilepsy", mean := epilepsy_mean]

# # Upload results ---------------------------------------------------------------
# For each sex, location, & year
# Pull the bacterial results and viral results
# Sum them
# Write out

# upload prev and incidence for acute meningitis (BACTERIAL AND VIRAL)
if (meid == 24068 | meid == 24161) {
  # Don't run this aggregation
  print("No aggregation needed for prevalence and incidence of bacterial/viral meningitis")
} else {
  temp_list <- list()
  for (y in years) {
    for (s in sexes) {
      etio_list <- lapply(c("bacterial", "viral"), function(e){
        # Define patterns
        my_filedir <- file.path("FILEPATH")
        # Set exceptions for hearing, vision, and epilepsy, whose output files remain in their original step directories
        if (group %like% "hearing" | group %like% "vision"){
          my_filedir <- file.path("FILEPATH")
        }
        if(group %like% "epilepsy"){
          my_filedir <- file.path("FILEPATH")
        }
        # Read in the file - set exception for epilepsy
        if (!group %like% "epilepsy") {
          dt <- fread(file.path("FILEPATH"))
        } else if (group %like% "epilepsy") {
          dt <- fread(file.path("FILEPATH"))
          dt <- dt[location == location & year_id == y & sex_id == s]
        }
        # Remove impairment_category if exists
        if("impairment_category" %in% names(dt)) dt$impairment_category <- NULL
        # Set ID columns globally so they stay assigned outside this function
        id_cols <<- names(dt)[!names(dt) %like% "draw"]
        # Set etiology column for YLD calculation later
        dt$etiology <- e
        return(dt)
      })
      # Sum the bacterial and viral results
      combined_etio_dt <- rbindlist(etio_list, use.names=TRUE)[, etiology := NULL][, lapply(.SD, sum, na.rm = TRUE), by = id_cols]
      dir.create(file.path("FILEPATH"), showWarnings = FALSE, recursive = TRUE)
      fwrite(combined_etio_dt, file.path("FILEPATH"))
      
      # END WRITE FOR NONFATAL UPLOAD
      
      # Start write inputs needed for etiology YLD upload later.
      # For each prevalence value, convert to YLDs by merging on the healthstates data table
      combined_healthstate_dt <- rbindlist(etio_list, use.names=TRUE)
      # Grab the correct healthstate
      description <- unique(dim.dt[modelable_entity_id == meid, modelable_entity_name])
      healthstate_row <- healthstates[sequela_name == description]
      combined_healthstate_dt <- cbind("DW_mean" = healthstate_row$mean, combined_healthstate_dt)
      combined_healthstate_dt[, (paste0("draw_",0:999)) := lapply(.SD, "*", DW_mean), .SDcols = paste0("draw_",0:999)]
      temp_list[[paste(y, s, sep = "_")]] <- combined_healthstate_dt
    }
  }
  yld_dt <- rbindlist(temp_list)
  dir.create(file.path("FILEPATH"), showWarnings = FALSE, recursive = TRUE)
  fwrite(yld_dt, file.path("FILEPATH"))
}