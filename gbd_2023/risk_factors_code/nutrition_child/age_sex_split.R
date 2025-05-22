dir_proj <- fs::path_real("FILEPATH")
source(fs::path(dir_proj, "FILEPATH"))
library(data.table)
library(tidyverse)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
library(agesexsplit)
library(nch)
library(openxlsx)
"%ni%" <- Negate("%in%")

args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1] 

task_id <- ifelse(Sys.getenv('SLURM_ARRAY_TASK_ID') != '', as.integer(Sys.getenv('SLURM_ARRAY_TASK_ID')), NA)
param_map <- fread(param_map_filepath)

bundle_id = param_map[task_id, stgpr_bundle_id] 
me_name = param_map[task_id, me_name] 
release_id = param_map[task_id, release_id]
me_id = param_map[task_id, stgpr_me_id]
run_id = param_map[task_id, run_id]
age_sex_xw_col = param_map[task_id, age_sex_xw_col]
age_sex_bv_col = param_map[task_id, age_sex_bv_col]
split_data_output_fp = param_map[task_id, split_data_output_fp]

message(paste0("------------------------------ ", me_name," ------------------------------"))


cli::cli_progress_step("Getting bundle data...", msg_done = "Have bundle data.")
bundle <- get_bundle_data(bundle_id)

cli::cli_progress_step("Converting bundle...", msg_done = "Bundle converted.")  
bundle <- as.data.table(bundle)
bundle <- agesexsplit::convert_stgpr_bundle(bundle_data = bundle, age_start = "orig_age_start", age_end = "orig_age_end") 

if ("cases" %in% colnames(bundle)) {
  bundle[measure == "proportion" & is.na(cases), cases := mean * sample_size]
  bundle[measure == "continuous", cases := 0]
}
if ("cases" %ni% colnames(bundle)) {
  bundle[, cases := NA]
}


bundle[orig_age_end >= 1 & orig_age_start < 1 & age_demographer == 0 | age_group_name == "6-11 months", orig_age_end := 0.999]
bundle[age_end >= 1 & age_start < 1 & age_demographer == 0 | age_group_name == "6-11 months", age_end := 0.999]
bundle[age_demographer > 1, age_demographer := 1]
bundle[measure == "continuous" & mean < 0, mean := 0]

cli::cli_progress_step("Getting draw ids...", msg_done = "Have draw ids.")
draw_ids <- agesexsplit::get_draws_ids(bundle_data = bundle, 
                                       model_type = "stgpr", 
                                       release_id = release_id)


cli::cli_progress_step("Getting weight draws...", msg_done = "Have weight draws.")
weight_draws <- get_draws(
  gbd_id_type = "modelable_entity_id",
  gbd_id = me_id,
  source = 'stgpr',
  age_group_id = draw_ids$age_group_id,
  sex_id = draw_ids$sex_id,
  location_id = draw_ids$location_id,
  year_id = draw_ids$year_id,
  version_id = run_id,
  release_id = release_id,
)


cli::cli_progress_step("Splitting bundle...", msg_done = "Bundle split.")
age_sex_split_bundle <- agesexsplit::age_sex_split(bundle_data = bundle, 
                                                   model_type = "stgpr", 
                                                   release_id = release_id, 
                                                   weight_draws = weight_draws)


cli::cli_progress_step("Reverting bundle...", msg_done = "Bundle reverted.")
age_sex_split_bundle <- agesexsplit::revert_stgpr_bundle(age_sex_split_bundle)


age_sex_split_bundle <- age_sex_split_bundle[measure == "proportion" & val >= 1, val := 0.999]
age_sex_split_bundle <- age_sex_split_bundle[measure == "proportion" & val <= 0.001, val := 0.001]
age_sex_split_bundle <- age_sex_split_bundle[measure == "proportion" & val >= 1, is_outlier := 1]
age_sex_split_bundle <- age_sex_split_bundle[measure == "proportion" & val <= 0.001, is_outlier := 1]
age_sex_split_bundle <- age_sex_split_bundle[measure == "continuous", cases := NA]
age_sex_split_bundle <- age_sex_split_bundle[cv_subnational == 1, variance := (variance*10)]


cli::cli_progress_step("Writing bundle...", msg_done = "Bundle written.")
dir.create(split_data_output_fp, recursive = TRUE)
openxlsx::write.xlsx(age_sex_split_bundle, paste0(split_data_output_fp, "/", bundle_id, "_",me_name, ".xlsx"), rowNames = FALSE, sheetName = "extraction")



cli::cli_progress_step("Saving bundle version...", msg_done = "Bundle version saved.")
bv <- save_bundle_version(bundle_id = bundle_id,
                          automatic_crosswalk = FALSE,
                          description = paste0(me_name, "_as_split_bundle_version"))
bv_id <- bv$bundle_version_id

cli::cli_progress_step("Saving crosswalk version...", msg_done = "Crosswalk version saved.")
result <- save_crosswalk_version(bundle_version_id = bv_id,
                                 data_filepath = paste0(split_data_output_fp, "/", bundle_id, "_",me_name, ".xlsx") ,
                                 description = paste0(me_name, "_as_split_xw_version"))

value <- result$crosswalk_version_id

cli::cli_progress_step("Updating ids to cgf_ids.csv...", msg_done = "IDs updated.")
ids <- fread("FILEPATH")

age_sex_bv_col
data.table::set(
  ids,
  i = which(ids$me_name == me_name),
  j = age_sex_bv_col,
  value = bv_id
)
set_ids(ids)


data.table::set(
  ids,
  i = which(ids$me_name == me_name),
  j = age_sex_xw_col,
  value = value
)
set_ids(ids)

cli::cli_progress_done()