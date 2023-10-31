#' @author 
#' @date 2019/10/08
#' @description upload 

rm(list=ls())

pacman::p_load(data.table, boot, ggplot2, argparse)

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--meid", help = "modelable entity id for upload", default = NULL, type = "integer")
parser$add_argument("--upload_dir", help = "directory for upload", default = NULL, type = "character")
parser$add_argument("--check_dir", help = "directory for to write finished check file", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step4', type = "character")
parser$add_argument("--gbd_round_id", help = "specify gbd round", default = 7L, type = "integer")
parser$add_argument("--bundle", help = "specify bundle id", default = NULL, type = "integer")
parser$add_argument("--crosswalk_version_id", help = "specify crosswalk version id associated with model", default = NULL, type = "integer")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# SOURCE FUNCTIONS --------------------------------------------------------
# Source GBD shared functions
invisible(sapply(list.files("/filepath/", full.names = T), source))

# SAVE RESULTS ------------------------------------------------------------
# get "parent" model version ID for description
# Incidence propotion dismod MEIDs
meid_list <- list()
meid_list[["29"]]   <- 1298
meid_list[["33"]]   <- 1328
meid_list[["37"]]   <- 1358
meid_list[["41"]]   <- 1388
meid_list[["7958"]] <- 25360
print('meid list pulled')

# get crosswalk version automatically for nonfatal
if (bundle != 7181){
  mv_id <- get_best_model_versions("modelable_entity", meid_list[[as.character(bundle)]], gbd_round_id = gbd_round_id, decomp_step = ds)
  mv_id <- mv_id$model_version_id
  print(mv_id)
  desc <- paste("squeezed PAF from parent model version ID", mv_id)
  print(desc)
  meta <- get_elmo_ids(bundle_id = bundle, gbd_round_id = gbd_round_id, decomp_step = ds)
  # take the best crosswalk version associated with the given mv_id
  crosswalk_version_id <- unique(meta[is_best==1 & model_version_id==mv_id]$crosswalk_version_id)
} else desc <- paste("squeezed fatal PAF")

my_measure_id <- 18 # for proportion
my_metric_id <- 3 # for rate

save_results_epi(
  input_dir = upload_dir,
  input_file_pattern = "{location_id}.csv",
  description = desc,
  modelable_entity_id = meid,
  gbd_round_id = gbd_round_id,
  decomp_step = ds,
  mark_best = T,
  measure_id = my_measure_id,
  metric_id = my_metric_id,
  bundle_id = bundle,
  crosswalk_version_id = crosswalk_version_id
)