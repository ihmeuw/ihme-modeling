# 1 ensure proper sexcrosswalk
# 2 ensure proper age model version ids
# 3 ensure proper decomp steps
# 4 ensure proper bundle version ids
# 5 ensure proper description

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

# clear environment
rm(list = ls())

# root path
root <- paste0("FILEPATH")

# packages
library(data.table)
library(ggplot2)
library(openxlsx)

# central functions
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')
source("FILEPATH/get_bundle_data.R")

# MR-BRT
repo_dir <- paste0("FILEPATH")
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))

# custom crosswalk scripts
source(paste0(root, "FILEPATH/apply_sex_crosswalk.R"))
source(paste0(root, "FILEPATH/apply_age_crosswalk.R"))

# load run file
run_file <- fread(paste0("FILEPATH/run_file.csv"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")

#'[1) load Sex Split fit from 00_sex_crosswalk_model
sex_cw_mod_fit <- readRDS(paste0(crosswalks_dir, "FILEPATH//model_fit_obj.rds"))

#'[2) set proper age model version id
age_model_version_id <- ADDRESS

#'[3) set decomp step
decomp_step <- ADDRESS
gbd_round_id <- ADDRESS
age_model_gbd_round_id <- ADDRESS
age_model_decomp_step <- ADDRESS

#############################################################################################
###'                           [1) Sex Split Agesplit Bundles 6452]                       ###
#############################################################################################

#'[4a) ensure proper bundle version ids
data_a <- get_bundle_version(ADDRESS)

# cleaning
locs <- get_location_metadata(35)
locs <- locs[, .(location_id, ihme_loc_id)]
data_a <- merge(data_a, locs, by = "location_id")
data_a[is.na(sample_size), sample_size := effective_sample_size]

# sex split
data_a <- apply_sex_crosswalk(mr_brt_fit_obj = sex_cw_mod_fit, all_data = data_a, decomp_step = ADDRESS)
data_a[upper > 1, upper := 0.99]

agesplit_out_file <- paste0(crosswalks_dir, "agesplit_cw.xlsx")
openxlsx::write.xlsx(data_a, agesplit_out_file,  sheetName = "extraction")
#'[5a) ensure proper description
agesplit_description <- "Description"
agesplit_cw_md <- save_crosswalk_version(bundle_version_id = ADDRESS,
                                       data_filepath =  agesplit_out_file,
                                       description = agesplit_description)
  
#############################################################################################
###'                               [2) Sex Split Input Bundle Data]                       ###
#############################################################################################

# 2020 sync bundle data

#'[4b) ensure proper bundle version ids
data_a <- get_bundle_version(ADDRESS)
data_b <- get_bundle_version(ADDRESS)
data_c <- get_bundle_version(ADDRESS)
data_d <- get_bundle_version(ADDRESS)
data_e <- get_bundle_version(ADDRESS)

locs <- get_location_metadata(35)
locs <- locs[, .(location_id, ihme_loc_id)]

# sex split data

data_a <- merge(data_a, locs, by = "location_id")
data_a_ss <- apply_sex_crosswalk(mr_brt_fit_obj = sex_cw_mod_fit, all_data = data_a, decomp_step = decomp_step)

data_b <- merge(data_b, locs , by = "location_id")
data_b_ss <- apply_sex_crosswalk(mr_brt_fit_obj = sex_cw_mod_fit, all_data = data_b, decomp_step = decomp_step)

data_c <- merge(data_c, locs, by = "location_id")
data_c_ss <- apply_sex_crosswalk(mr_brt_fit_obj = sex_cw_mod_fit, all_data = data_c, decomp_step = decomp_step)

data_d <- merge(data_d, locs, by = "location_id")
data_d[is.na(sample_size), sample_size := effective_sample_size]
data_d_ss <- apply_sex_crosswalk(mr_brt_fit_obj = sex_cw_mod_fit, all_data = data_d, decomp_step = decomp_step)

data_e <- merge(data_e, locs, by = "location_id")
data_e_ss <- apply_sex_crosswalk(mr_brt_fit_obj = sex_cw_mod_fit, all_data = data_e, decomp_step = decomp_step)
 

# age split data
#' [4) ensure proper age model version ids
data_a_as <- apply_age_split(data = data_a_ss,
                         dismod_meid = ADDRESS,
                         dismod_mvid = age_model_version_id,
                         loc_pattern = 1,
                         decomp_step_pop = decomp_step,
                         decomp_step_meid = age_model_decomp_step)

data_b_as <- apply_age_split(data = data_b_ss,
                         dismod_meid = ADDRESS,
                         dismod_mvid = age_model_version_id,
                         loc_pattern = 1,
                         decomp_step_pop = decomp_step,
                         decomp_step_meid = age_model_decomp_step)

data_c_as <- apply_age_split(data = data_c_ss,
                         dismod_meid = ADDRESS ,
                         dismod_mvid = age_model_version_id,
                         loc_pattern = 1,
                         decomp_step_pop = decomp_step,
                         decomp_step_meid = age_model_decomp_step)

data_d_ss[, note_modeler := NA]
data_d_as <- apply_age_split(data = data_d_ss,
                         dismod_meid = ADDRESS,
                         dismod_mvid = age_model_version_id,
                         loc_pattern = 1,
                         decomp_step_pop = decomp_step,
                         decomp_step_meid = age_model_decomp_step)

data_e_as <- apply_age_split(data = data_e_ss,
                         dismod_meid = ADDRESS ,
                         dismod_mvid = age_model_version_id,
                         loc_pattern = 1,
                         decomp_step_pop = decomp_step,
                         decomp_step_meid = age_model_decomp_step)

# save post-crosswalk

#'[5b) ensure proper description

all_description <- "Description"

data_a_as <- data_a_as[cases < sample_size]
data_a_as[upper > 1, upper := 0.99] # three points 
data_a_as <- data_a_as[standard_error < 1]
data_a_out_file <- paste0(crosswalks_dir, "data_a_post_crosswalks.xlsx")
openxlsx::write.xlsx(data_a_as, data_a_out_file,  sheetName = "extraction")

data_b_out_file <- paste0(crosswalks_dir, "data_b_post_crosswalks.xlsx")
data_b_as <- data_b_as[standard_error < 1]
openxlsx::write.xlsx(data_b_as, data_b_out_file,  sheetName = "extraction")

data_c_out_file <- paste0(crosswalks_dir, "data_c_post_crosswalks.xlsx")
data_c_as <- data_c_as[standard_error < 1]
openxlsx::write.xlsx(data_c_as, data_c_out_file,  sheetName = "extraction")

data_d_as <- data_d_as[cases < sample_size]
data_d_as <- data_d_as[standard_error < 1]
data_d_out_file <- paste0(crosswalks_dir, "data_d_post_crosswalks.xlsx")
openxlsx::write.xlsx(data_d_as, data_d_out_file,  sheetName = "extraction")

data_e_as <- data_e_as[standard_error < 1]
data_e_out_file <- paste0(crosswalks_dir, "data_e_post_crosswalks.xlsx")
openxlsx::write.xlsx(data_e_as, data_e_out_file,  sheetName = "extraction")

#'[4c) ensure proper bundle version ids
data_a_cw_md <- save_crosswalk_version(bundle_version_id = ADDRESS,
                                      data_filepath =  data_a_out_file,
                                      description = all_description)
data_b_cw_md <- save_crosswalk_version(bundle_version_id = ADDRESS,
                                      data_filepath =  data_b_out_file,
                                      description = all_description)
data_c_cw_md <- save_crosswalk_version(bundle_version_id = ADDRESS,
                                      data_filepath =  data_c_out_file,
                                      description = all_description)
data_d_cw_md <- save_crosswalk_version(bundle_version_id = ADDRESS,
                                      data_filepath =  data_d_out_file,
                                      description = all_description)
data_e_cw_md <- save_crosswalk_version(bundle_version_id = ADDRESS,
                                      data_filepath =  data_e_out_file,
                                      description = all_description)
