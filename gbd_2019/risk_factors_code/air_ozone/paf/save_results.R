#----HEADER----------------------------------------------------------------------------------------------------------------------
# Author: NAME
# Date: 01/05/2018
# Purpose: Save results for ozone (PAFs)
# source("FILEPATH.R", echo=T)

#------------------------Setup -------------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
} else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
}

# load packages, install if missing
pacman::p_load(data.table, magrittr, ini)

#Versions for upload:
paf.version <- 19
#parameters
years <- c(1990:2019)
location_set_version_id <- 35

#draw directories
paf.dir <- file.path("FILEPATH", paf.version,"FILEPATH")
exp.dir <- file.path(j_root, "FILEPATH", paf.version)

#----------------------------Functions---------------------------------------------------

#Save Results Epi (EXP)
source(file.path(central_lib,"FILEPATH.R"))

#Save Results Risk (RR, PAF)
source(file.path(central_lib,"FILEPATH.R"))

#get pops
source(file.path(central_lib,"FILEPATH.R"))

#get locations
source(file.path(central_lib,"FILEPATH.R"))

locs <- get_location_metadata(35)

#custom fx
"%ni%" <- Negate("%in%")

#--------------------SCRIPT-----------------------------------------------------------------

# save PAFs
save_results_risk(input_dir = paf.dir,
                  input_file_pattern = "FILEPATH.csv",
                  year_id = years,
                  modelable_entity_id = 8748,
                  description = "decomp step 4: 3-year aggregation strategy",
                  decomp_step = "step4",
                  risk_type = "paf",
                  measure_id = "4",
                  mark_best=TRUE)

#PAF scatters
source("FILEPATH.R")

paf_scatter(rei_id=88,measure_id=4,decomp_step="step4",
            file_path=paste0(j_root,"FILEPATH",paf.version,".pdf"),add_isos=T)

paf_scatter(rei_id=88,measure_id=4,decomp_step="step4", year_id=1990,
            file_path=paste0(j_root,"FILEPATH",paf.version,".pdf"),add_isos=T)