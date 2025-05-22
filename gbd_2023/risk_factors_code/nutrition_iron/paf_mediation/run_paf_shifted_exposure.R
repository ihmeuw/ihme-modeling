################################################################################
## DESCRIPTION: Run the pafs for each model and rei_id called by parent script ("launch_pafs.R").
## INPUTS: Passed from launch_pafs.R or set in PARSE ARGS section
################################################################################


## LOAD DEPENDENCIES -----------------------------------------------------------
library(argparse)
library(reticulate)
library(data.table)
library(
  "ihme.cc.paf.calculator",
  lib.loc = "FILEPATH"
)

## Get bested version 

me_id = 8882
shift = "shifted" # or unshifted
source("FILEPATH/get_best_model_versions.R")

bested <- get_best_model_versions(entity = "modelable_entity", ids = me_id,
                                          release_id = 16)
exposure_version_id = unique(bested$model_version_id)

rei_id = 95


me <- "nutrition_iron" # change here.
description =paste0("pafs for_",  shift,  "_hgb:mvid_",exposure_version_id)
proj_name <- "proj_diet"

##Load args---------------------------------------------------------------------------#
if (interactive()){
  ids <- fread("FILEPATH/mnd_ids.csv")  
  me <- me
  
}else{
  args <- commandArgs(trailingOnly = TRUE) 
  print(args)
  me <- args[1]
  print(me)
  rei_id <- args[2]
  print(rei_id)
  release_id <- args[3]
  print(release_id)
  years <- args[4]
  print(years)
  draws <- args[5]
  print(draws)
  proj_name <- args[6]
  print(proj_name)
  resume_stat <- args[7]
  print(resume_stat)
  skip_save <- as.logical(args[8])
  print(skip_save)

}



## BODY ------------------------------------------------------------------------


launch_paf_calculator(rei_id,
                      cluster_proj = proj_name,
                      release_id = 16,
                      skip_save_results = FALSE,
                       year_id = c(1990:2024),
                      description = description, 
                      n_draws = 250)

