
#-------------------Header------------------------------------------------
# Author: 
# Date: 1/24/2020
# Purpose: Launch BWGA PAFs for air pollution

#------------------Set-up--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}


library(
  "ihme.cc.paf.calculator",
  lib.loc = "FILEPATH"
)

packages <- c("data.table","magrittr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

project <- "-P proj_erf" 
sge.output.dir <- " -o FILEPATH "


#------------------Launch PAFs--------------------------------------------------

estimation_years <- c(1990,2000,2010,2020,2022,2023)



model_version_id <- launch_paf_calculator(
  rei_id = 380,
  cluster_proj = "proj_erf",
  release_id = 16,
  year_id = estimation_years,
  n_draws = 100,
  skip_save_results = TRUE,
  description = "Run #1 for launch_bwga_paf",
)

message("My PAF model version is ", model_version_id)
