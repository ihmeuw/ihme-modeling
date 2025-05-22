##' ***************************************************************************
##' Title: 1_run_pooling.R
##' Purpose: Compile other hemoglobinopathies: Get CoD VR data for 618 and pool cod data across 10 year spans 
##'           a. Get CoD data for 618
##'           b. Create year range variable by doing (year_id-5 : year_id +5) (2019-2024 are all set to 2014:2024)
##'           c. For each year range, grouping by age and sex, get global deaths
##'           d. Generate 1000 draws of the mortality rate
##' ***************************************************************************

source("cod_pipeline/src_functions/compile_other_hemog.R")

# Set parameters ----------------------------------------------------------

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
} else {
  # option to run interactively 
  task_id <- 1L
  param_map_filepath <- PARAM_MAP_FP
}

# set values based on param map
param_map <- qs::qread(param_map_filepath)

# Pull cod data for dr locations ------------------------------------------

cli::cli_progress_step("Pulling cod data for other hemog...",
                       msg_done = "Finished pulling cod data for other hemog.")

cod_data <- ihme::get_cod_data(cause_id = 618, 
                               release_id = param_map$release_id, 
                               location_id = param_map$dr_loc_id, 
                               include_data_type_name = TRUE)[data_type_name == "VR"]

missing_locs <- dplyr::setdiff(dr_loc_ids, unique(cod_data$location_id))
if (length(missing_locs) > 0) {
  message("No cod data returned for location ID(s): ", 
          paste(missing_locs, collapse = ", "))
}

# Start pooling -----------------------------------------------------------

cli::cli_progress_step("Pooling cod data for other hemog...",
                       msg_done = "Finished pooling cod data for other hemog.")

pooled_df <- compile_other_hemog(
  cod_data = cod_data,
  year_id = param_map$year_ids,
  age_group_id = get_age_group_ids(release_id = param_map$release_id),
  sex_id = param_map$sex_id
)

qs::qsave(pooled_df,
          file = file.path(param_map$out_dir, "new_618_draws.qs"),
          sheetName = "extraction", 
          rowNames = FALSE)

print("Pooled other hemog created - Cause ID 618")