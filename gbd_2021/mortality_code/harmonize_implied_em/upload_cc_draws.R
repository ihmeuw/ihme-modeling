
# Meta --------------------------------------------------------------------

# Upload results to CoD database
#
# Note that CoD expect results from 1980-2022, so we have previously prepped
# zero-filled files in a constant upload directory, then link the versioned
# outputs to that directory to save space.


# Load Packages -----------------------------------------------------------

library(data.table)
source("FILEPATH")


# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

parser$add_argument(
  "--dir_output",
  type = "character",
  required = !interactive(),
  default = "FILEPATH",
  help = "Output directory for em adjustment"
)
parser$add_argument(
  "--current_sex_id",
  type = "integer",
  required = !interactive(),
  default = 2L,
  help = "sex_id to upload"
)
parser$add_argument(
  "--cause_id",
  type = "integer",
  required = !interactive(),
  default = 1058L,
  help = "cause_id to upload"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)


# Set parameters ----------------------------------------------------------

dir_cc_covid <- "FILEPATH"
dir_final <- fs::path(dir_output, "draws/covid_oprm_draws")
dir_upload <- fs::path(fs::path_dir(dir_output), "constant_upload")
subdir_upload <- fs::path(cause_id, current_sex_id)

mark_cause_best <- F

years_pandemic <- 2020:2021

gbd_round_id <- demInternal::get_gbd_round(gbd_year = 2021)

run_id_harmonizer <- fs::path_file(dir_output)


# Load maps ---------------------------------------------------------------

map_locs_lowest <- demInternal::get_locations(gbd_year = 2021, level = "lowest")
map_locs_lowest <- map_locs_lowest[level >= 3]

map_ages <- fread(fs::path(dir_output, "age_map.csv"))

map_cause <- data.table(
  id = c(1048, 1058),
  name = c("covid", "pos_oprm")
)

cause_name <- map_cause[id == cause_id, name]


# Load Data ---------------------------------------------------------------

dt_harmonizer <- fs::path(dir_output, "draws/scaled_env") |>
  fs::dir_ls(regexp = "scaled.*\\.arrow$") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(
    location_id %in% map_locs_lowest$location_id,
    sex_id == current_sex_id,
    age_group_id %in% map_ages$age_group_id
  ) |>
  dplyr::select(
    location_id, year_id, age_group_id, draw, dplyr::matches(cause_name)
  ) |>
  dplyr::collect() |>
  setDT() |>
  dcast(... ~ draw, value.var = cause_name) |>
  setnames(as.character(0:999), paste0("draw_", 0:999))

assertable::assert_ids(
  dt_harmonizer,
  id_vars = list(
    location_id = map_locs_lowest$location_id,
    age_group_id = map_ages$age_group_id,
    year_id = years_pandemic
  )
)


# Save data ---------------------------------------------------------------

dt_harmonizer |>
  split(by = "year_id", keep.by = FALSE) |>
  purrr::iwalk(\(x, y) readr::write_csv(
    x,
    fs::path(dir_final, paste(cause_id, current_sex_id, y, sep = "_"), ext = "csv"))
  )

paths_final <- fs::path(dir_final, paste(cause_id, current_sex_id, years_pandemic, sep = "_"), ext = "csv")
paths_links <- fs::path(dir_upload, subdir_upload, years_pandemic, ext = "csv")

# symlink
fs::link_delete(paths_links[fs::link_exists(paths_links)])
fs::link_create(paths_final, paths_links)
stopifnot(all(fs::link_exists(paths_links)))


# Upload ------------------------------------------------------------------

save_results_cod(
  input_dir = fs::path(dir_upload, cause_id),
  input_file_pattern = "{sex_id}/{year_id}.csv",
  cause_id = cause_id,
  model_version_type_id = 5,
  description = paste("HARMONIZER: final", cause_name, run_id_harmonizer),
  gbd_round_id = gbd_round_id,
  decomp_step = "iterative",
  sex_id = current_sex_id,
  mark_best = mark_cause_best
)
