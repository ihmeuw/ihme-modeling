
# Meta --------------------------------------------------------------------

# Upload abridged life tables and envelope to the mortality database


# Load packages -----------------------------------------------------------

library(data.table)


# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

# Node arguments
parser$add_argument(
  "--upload_process",
  type = "character",
  required = !interactive(),
  default = "wslt",
  help = "Short code for upload process name"
)
parser$add_argument(
  "--upload_run_id",
  type = "integer",
  required = !interactive(),
  default = 575,
  help = "Run id associated with the uploaded process"
)

# Version arguments
parser$add_argument(
  "--version",
  type = "integer",
  required = !interactive(),
  default = 465,
  help = "Version of canonizer process"
)

# Process arguments
parser$add_argument(
  "--start_year",
  type = "integer",
  required = !interactive(),
  default = 1950,
  help = "First year of estimation"
)
parser$add_argument(
  "--end_year",
  type = "integer",
  required = !interactive(),
  default = 2022,
  help = "Last year of estimation"
)
parser$add_argument(
  "--code_dir",
  type = "character",
  required = !interactive(),
  default = here::here(),
  help = "Working directory for code"
)

args <- parser$parse_args()


# Set parameters ----------------------------------------------------------

cfg <- config::get(file = fs::path(args$code_dir, "config.yml"))

dir <- list(main = fs::path(cfg$dir$base, args$version))
dir$cache <- fs::path(dir$main, "cache")
dir$upload <- fs::path(dir$main, "upload")

valid_process_args <- c("nslt", "wslt", "nsdn", "wsdn")
stopifnot(args$upload_process %in% valid_process_args)

model_name <- switch(
  args$upload_process,
  nslt = "no shock life table",
  wslt = "with shock life table",
  nsdn = "no shock death number",
  wsdn = "with shock death number"
)

estimate_stages <- switch(
  args$upload_process,
  nslt = "with_hiv",
  wslt = "with_shock",
  nsdn = "with_hiv",
  wsdn = "with_shock"
)

subdir_output <- switch(
  args$upload_process,
  nslt = "final_abridged_lt_summary",
  wslt = "final_abridged_lt_summary",
  nsdn = "final_abridged_env_summary",
  wsdn = "final_abridged_env_summary"
)

run_years <- (args$start_year):(args$end_year)

id_cols <- c("location_id", "year_id", "sex_id", "age_group_id", "estimate_stage_id")
value_cols <- c("mean", "lower", "upper")


# Load maps ---------------------------------------------------------------

locs_all <- arrow::read_ipc_file(fs::path(dir$cache, "locs_all.arrow"))

map_age_gbd <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_gbd.arrow"))
map_age_summary_env <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_summary_envelope.arrow"))
map_age_summary_lt <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_summary_lt.arrow"))

map_estimate_stage <- arrow::read_ipc_file(fs::path(dir$cache, "estimate_stage_map.arrow"))
map_estimate_stage <- map_estimate_stage[estimate_stage_name %in% estimate_stages]
map_lt_param <- arrow::read_ipc_file(fs::path(dir$cache, "lifetable_parameter_map.arrow"))
map_lt_param <- map_lt_param[life_table_parameter_name != "pred_ex"]

map_sex <- arrow::read_ipc_file(fs::path(dir$cache, "sex_map.arrow"))


# Load data ---------------------------------------------------------------

ds <-
  fs::path(dir$main, "output", subdir_output) |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::right_join(map_estimate_stage, by = c("type" = "estimate_stage_name")) |>
  dplyr::select(-type)

if (grepl("table", model_name)) {

  id_cols <- union(id_cols, "life_table_parameter_id")

  ds <- ds |>
    dplyr::right_join(map_lt_param, by = "life_table_parameter_name") |>
    dplyr::select(-life_table_parameter_name)

}

dt <- ds |>
  dplyr::select(dplyr::all_of(c(id_cols, value_cols))) |>
  dplyr::collect() |>
  setDT() |>
  setkeyv(id_cols)

stopifnot(!anyNA(dt))


# Validate data -----------------------------------------------------------

list_ids <- list(
  location_id = locs_all$location_id,
  year_id = run_years,
  sex_id = map_sex$sex_id,
  estimate_stage_id = map_estimate_stage$estimate_stage_id
)

if (grepl("table", model_name)) {

  list_ids$age_group_id <- map_age_summary_lt$age_group_id
  list_ids$life_table_parameter_id <- map_lt_param$life_table_parameter_id

} else if (grepl("death", model_name)) {

  list_ids$age_group_id <- map_age_summary_env$age_group_id

}

dt_ids <- do.call(CJ, list_ids)

check_col_names <- setequal(names(dt), c(id_cols, value_cols))
check_duplicates <- dt[, .N, by = names(dt_ids)][N != 1]
check_id_combos <- fsetequal(dt[, .SD, .SDcols = names(dt_ids)], dt_ids)

stopifnot(
  "Expected column names not present" = check_col_names,
  "Duplicate entries for an ID combo" = nrow(check_duplicates) == 0,
  "Required ID combos aren't exclusively present" = check_id_combos
)

rm(dt_ids)


# Upload ------------------------------------------------------------------

file_out <- paste0(gsub(" ", "_", model_name), "-v", args$upload_run_id)
path_out <- fs::path(dir$upload, file_out, ext = "csv")
readr::write_csv(dt, path_out)

mortdb::upload_results(
  filepath = path_out,
  model_name = model_name,
  model_type = "estimate",
  run_id = args$upload_run_id
)
