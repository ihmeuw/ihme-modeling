# Purpose: VL crosswalking --- just age model
#
rm(list = ls())

# root path
code_root <- paste0("FILEPATH", Sys.info()[7], "/")
data_root <- "FILEPATH"

# MR-BRT

library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")
library(data.table)
library(ggplot2)
library(openxlsx)
source(paste0('FILEPATH/get_ids.R'))
source(paste0('FILEPATH/get_location_metadata.R'))
source(paste0('FILEPATH/get_age_metadata.R'))
source(paste0('FILEPATH/get_population.R'))
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')
source("FILEPATH/get_bundle_data.R")
repo_dir <- paste0('FILEPATH')
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))

# Custom
source(paste0(code_root, 'FILEPATH/custom_functions/processing.R'))
source(paste0(code_root, "FILEPATH/apply_sex_crosswalk.R"))
source(paste0(code_root, "FILEPATH/apply_age_crosswalk.R"))

decomp_step <- ADDRESS
gbd_round_id <- ADDRESS

#############################################################################################
###                                      Set-Up                                           ###
#############################################################################################

#' [Set-up run directory / tracker]

gen_rundir(data_root = data_root, acause = 'FILEPATH', message = MESSAGE)
run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
fwrite(tracker, paste0(crosswalks_dir, "FILEPATH"))

#' [Sex Crosswalk] this pulls from bundle ADDRESS
fit1 <- readRDS(paste0(crosswalks_dir, "FILEPATH"))

age_split_bundle <- get_bundle_version(ADDRESS)
age_split_ss <- apply_sex_crosswalk(mr_brt_fit_obj =  fit1, all_data = age_split_bundle, decomp_step = decomp_step)
age_split_ss <- age_split_ss[age_end - age_start < 25]
age_split_ss <- age_split_ss[is.na(group_review)]
age_split_ss <- age_split_ss[, specificity := NA]
age_split_ss[, c("specificity", "group_review", "group") := NULL]

agesplit_out_file <- paste0(crosswalks_dir, "FILEPATH")
 
openxlsx::write.xlsx(age_split_ss, agesplit_out_file,  sheetName = "extraction")
agesplit_description <- DESCRIPTION
 
agesplit_cw_md  <- save_crosswalk_version(bundle_version_id = ADDRESS, 
                                           data_filepath =  agesplit_out_file,
                                           description = agesplit_description)
