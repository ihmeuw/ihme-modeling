#####################################################################################################################################################################################
#####################################################################################################################################################################################                                                                                                                                                                           ##
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code                                                                                        ##                                                                                                                                                      ##
## Description:	Parallelization of 09_collate_outputs                                                                                                                              ##
##                                                                                                                                                                                 ##
#####################################################################################################################################################################################
#####################################################################################################################################################################################
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "/home/j/" 
  h <- paste0("/ihme/homes/", Sys.info()["user"], "/")
  l <- "/ihme/limited_use/"
  k <- "/ihme/cc_resources/libraries/"
} else { 
  j <- "J:/"
  h <- "H:/"
  l <- "L:/"
  k <- "K:/libraries/"
}

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse)
library(data.table)
library(openxlsx)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)", default = NULL, type = "character")
parser$add_argument("--step_num", help = "step number of this step (i.e. 01a)", default = NULL, type = "character")
parser$add_argument("--step_name", help = "name of current step (i.e. first_step_name)", default = NULL, type = "character")
parser$add_argument("--code_dir", help = "code directory", default = NULL, type = "character")
parser$add_argument("--in_dir", help = "directory for external inputs", default = NULL, type = "character")
parser$add_argument("--out_dir", help = "directory for this steps checks", default = NULL, type = "character")
parser$add_argument("--tmp_dir", help = "directory for this steps intermediate draw files", default = NULL, type = "character")
parser$add_argument("--root_j_dir", help = "base directory on J", default = NULL, type = "character")
parser$add_argument("--root_tmp_dir", help = "base directory on clustertmp", default = NULL, type = "character")
parser$add_argument("--cause", help = "potential values are: meningitis_pneumo, meningitis_hib, meningitis_meningo, meningitis_other", default = NULL, type = "character")
parser$add_argument("--group", help = "potential values are: hearing, vision, and epilepsy", default = NULL, type = "character")
parser$add_argument("--state", help = "healthstate in in_dir/.../meningitis_dimension.csv", default = NULL, type = "character")
parser$add_argument("--meid", help = "meid for custom model", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step1', type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# User specified options -------------------------------------------------------
functional <- "encephalitis"
measure <- 5 
gbd_round <- 7
bundle <- 45
bundle_version_dir <- "filepath"
cv_tracker <- as.data.table(read.xlsx(paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx')))
cv_row <- cv_tracker[bundle_id == bundle & current_best == 1]
crosswalk_version_id <- cv_row$crosswalk_version
source(paste0(k, "current/r/save_results_epi.R"))
source(paste0(k, "current/r/get_model_results.R"))

# Get the best parent MEID for documentation
parent <- get_model_results('epi', 
                            1419, 
                            location_id = 1,
                            gbd_round_id = gbd_round,
                            decomp_step = ds, 
                            status = 'best')
parent_mvid <- unique(parent$model_version_id)

# # Upload results ---------------------------------------------------------------
my_filedir <- file.path(tmp_dir, "03_outputs", "01_draws", cause, group, state)
my_file_pat <- "{measure_id}_{location_id}_{year_id}_{sex_id}.csv"
my_desc <- paste("Custom", functional, cause, group, state, date, "from parent model version ID", parent_mvid)
save_results_epi(
  input_dir = my_filedir,
  input_file_pattern = my_file_pat,
  modelable_entity_id = meid,
  description = my_desc,
  measure_id = measure,
  sex_id = c(1,2),
  decomp_step = ds,
  gbd_round_id = gbd_round,
  bundle_id = bundle,
  crosswalk_version_id = crosswalk_version_id,
  mark_best = T
)
# # ------------------------------------------------------------------------------

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "/02_temp/01_code/checks/", "finished_save_", meid,".txt"), overwrite=T)