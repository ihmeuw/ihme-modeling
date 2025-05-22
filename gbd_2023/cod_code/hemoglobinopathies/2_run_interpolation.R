# The interpolate function is used to interpolate draws that only exist for
# estimation years, in order to get a full timeseries of draws.

invisible(sapply(list.files("cod_pipeline/src_functions/", full.names = T), source))

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

if (param_map$release_id == 16) {
  param_map$location_id <- c(param_map$location_id[!param_map$location_id %in% c(60908L, 94364L, 95069L)], 44858)
}
loc_id <- param_map$location_id[task_id]
cause_ids <- param_map$cause_id
sex_ids  <- param_map$sex_id
year_list_mes <- c(1980:2022)
year_list_parent <- param_map$year_ids

# Interpolate for causes 614, 615 and 616 ---------------------------------

cli::cli_progress_step("Interpolating...", msg_done = "Interpolated.")

interp_df <- dismod_interpolate(
  input_map = data.table::fread(fs::path(param_map$out_dir, "input_map.csv")),
  loc_id = loc_id,
  year_start = year_list_mes[1], 
  year_end = tail(year_list_mes, n = 1), 
  release_id = param_map$release_id21
)

# Pull in other hemog and add to file -------------------------------------

cli::cli_progress_step("Pulling in other hemog...",
                       msg_done = "Pulled in other hemog.")

file_path <- fs::path(param_map$out_dir, "new_618_draws.qs")
other_hemog <- data.table::as.data.table(qs::qread(file_path))
other_hemog[other_hemog < 0] <- 0
other_hemog[, c("pooled_rate", "wilson_se") := NULL]
other_hemog$cause_id <- 618
other_hemog$location_id <- loc_id

# Save interpolated results -----------------------------------------------

cli::cli_progress_step("Saving interpolated results...",
                       msg_done = "Interpolated results saved.")

# write out results for each cause-sex combination
for (cause in cause_ids) {
  for (sex in sex_ids) {
    message("Writing cause ID: ", cause, " ", sex)
    if (cause == 618) {
      result <- other_hemog[other_hemog$cause_id == cause & 
                              other_hemog$sex_id == sex, ]
    } else {
      result <- interp_df[interp_df$cause_id == cause & 
                            interp_df$sex_id == sex, ] 
    }
    if (!any(2023 %in% result$year_id)) {
      new_rows_2023 <- result |> 
        dplyr::filter(year_id == 2022) |> 
        dplyr::mutate(year_id = 2023)
      new_rows_2024 <- result |> 
        dplyr::filter(year_id == 2022) |> 
        dplyr::mutate(year_id = 2024)
      result <- dplyr::bind_rows(result, new_rows_2023, new_rows_2024)
    } 
    dir_path <- file.path(param_map$out_dir, "interp_files", cause, sex)
    fs::dir_create(dir_path)
    qs::qsave(
      result,
      file = fs::path(dir_path, glue::glue("interp_hemog_{loc_id}.qs"))
    )
  }
}

# Save location -----------------------------------------------------------

cli::cli_progress_step("Saving location...", msg_done = "Location saved.")
saveRDS(
  data.table::data.table(location_id = loc_id),
  file = fs::path(param_map$interp_check_path, paste0("loc_", loc_id, ".RDS")),
  compress = FALSE
)
cli::cli_progress_done()
