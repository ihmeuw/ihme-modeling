#' [Title: CL Crosswalking
#' [Notes: Sex and age splits for CL


#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())

# packages

library(metafor, lib.loc = "FILEPATH")
library(msm)
library(data.table)
library(ggplot2)
library(openxlsx)

# central functions

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

# MR-BRT

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))

# Custom crosswalk scrupts

source("FILEPATH")
source("FILEPATH")

# Run file

run_file <- fread("FILEPATH")
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")

#############################################################################################
###'                                  [Split Bundles]                                     ###
#############################################################################################

# Load Sex Split fit from 00_sex_crosswalk_model
fit1 <- readRDS(paste0("FILEPATH"))

#############################################################################################
###'                           [1) Sex Split Agesplit Bundles ADDRESS1]                   ###
#############################################################################################

# bundle version is ADDRESS2

#' [Age Split Bundle Prep]
#' 
agesplit_bundle <- get_bundle_version(ADDRESS)
agesplit_bundle_ss <- apply_sex_crosswalk(mr_brt_fit_obj = fit1, all_data = agesplit_bundle, decomp_step = "iterative")

agesplit_bundle_ss <- agesplit_bundle_ss[field_citation_value != ""]
agesplit_bundle_ss[, c("group", "specificity", "group_review") := NA]
agesplit_out_file <- paste0(crosswalks_dir, "FILEPATH")


agesplit_bundle_ss <- agesplit_bundle_ss[age_end - age_start < 25]
agesplit_bundle_ss[mean > 0.3, is_outlier := 1] # one point
agesplit_bundle_ss[mean > 0.024, is_outlier := 1] # two points
# 
agesplit_bundle_ss[field_citation_value %like% 'Ministry of Health (Brazil). Brazil Information System for Notifiable Disea', is_outlier := 1]

openxlsx::write.xlsx(agesplit_bundle_ss, agesplit_out_file,  sheetName = "extraction")
agesplit_description <- "outliered points, no SINAN, age-split test 2, sex splitting data outliered Sinan"

agesplit_cw_md  <- save_crosswalk_version(bundle_version_id = ADDRESS, 
                                          data_filepath =  agesplit_out_file,
                                          description = agesplit_description)



#############################################################################################
###                  [2) Sex and Age Split Input data Bundles ADDRESS3]                   ###
#############################################################################################

input_data <- get_bundle_version(ADDRESS, fetch = 'all')
input_data_ss <- apply_sex_crosswalk(mr_brt_fit_obj = fit1, all_data = input_data, decomp_step = "step4")

#'[DX Crosswalk]

#' [Age Split]

input_data_as <- apply_age_split(data = input_data_ss, 
                                 dismod_meid = ADDRESS, 
                                 dismod_mvid = ADDRESS,
                                 loc_pattern = 1, 
                                 decomp_step_pop = "step4",
                                 decomp_step_meid = "iterative", 
                                 round_id = ADDRESS)

#cleaning
#age split changes age start to 1
input_data_as[age_end < age_start, age_end := 1]


input_data_as[, age_diff := NULL]
input_data_as[is.na(upper), upper := mean + (1.96 * standard_error)]
input_data_as[is.na(lower), lower := mean - (1.96 * standard_error)]

input_out_file <- paste0(crosswalks_dir, "FILEPATH")
input_data_as <- input_data_as[!(upper > 1)]
input_data_as[lower < 0, lower := 0]
input_data_as[, uncertainty_type_value := 95]
openxlsx::write.xlsx(input_data_as, input_out_file,  sheetName = "extraction")
input_description <- "1/21 fix silent dropping, binomial sex-split, age split ADDRESS global pattern"

input_cw_md  <- save_crosswalk_version(bundle_version_id = ADDRESS, 
                                          data_filepath =  input_out_file,
                                          description = input_description)
