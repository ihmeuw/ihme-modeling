####################################################################################
## This file launches diarrhea etiology PAF calculation, parallel by location_id ##
####################################################################################

## Load packages and functions
library(plyr)
source("/PATH/get_location_metadata.R")

## Set objects
user <- "NAME"
gbd_round_id <- 7
decomp_step <- "iterative"
create_scalar <- F
upload_rei <- ID # "all" or specify a subset of REI ids
param_map_path <- paste0(getwd(), "/param_map_temp.csv")
script_path <- "/PATH/01_pafs_annual_diarrhea.R" # check lowercase/upper on .r here, has been weird
shell_file <- "/PATH/execRscript.sh"

## Get locations 
locations <- get_location_metadata(location_set_id=35, gbd_round_id=7, decomp_step = "iterative")
locations <- locations$location_id[locations$is_estimate==1]
# run failed locations or test

## Get parameter map
eti_meta <- read.csv("/PATH/eti_rr_me_ids.csv")
eti_meta <- subset(eti_meta, cause_id==302) # diarrhea only
if(upload_rei != "all") eti_meta <- subset(eti_meta, rei_id %in% upload_rei)

###############################################
## Launch array job for all locations and etiologies
param_map <- expand.grid(location_id = locations, me_id = eti_meta$modelable_entity_id)
write.csv(param_map, param_map_path, row.names=F)
n_jobs <- nrow(param_map)
my_tasks <- paste0("1:", n_jobs)

args <- paste(gbd_round_id, decomp_step, param_map_path)
# store qsub command
qsub = paste0("QSUB")
# submit job
system(qsub)
