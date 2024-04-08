
# Meta --------------------------------------------------------------------

# Prepare final HIV deaths for upload


# Load packages -----------------------------------------------------------

library(data.table)


# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

# Node arguments
parser$add_argument(
  "--loc_id",
  type = "integer",
  required = !interactive(),
  default = 163
)

# Version arguments
parser$add_argument(
  "--version",
  type = "integer",
  required = !interactive(),
  default = 463,
  help = "Version of canonizer process"
)

# Process arguments
parser$add_argument(
  "--start_year",
  type = "integer",
  required = !interactive(),
  default = 1980,
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
dir$out <- fs::path(dir$main, "output")
dir$upload_hiv <- fs::path(dir$main, "upload/hiv_deaths")

dir_hiv_free <- fs::path(dir$out, "final_abridged_lt_env", "type=hiv_free")
dir_with_hiv <- fs::path(dir$out, "final_abridged_lt_env", "type=with_hiv")

file_input <- paste0(args$loc_id, "-0.arrow")

year_range <- (args$start_year):(args$end_year)
draw_range <- do.call(seq, as.list(cfg$draw_range))

id_cols <- c("year_id", "sex_id", "age_group_id", "draw")

# Tolerance value for acceptable negative HIV deaths
tol_neg_hiv <- 1e-9


# Load maps ---------------------------------------------------------------

map_age_gbd <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_gbd.arrow"))


# Load data ---------------------------------------------------------------

prep_ds <- \(path) path |>
    arrow::open_dataset(format = "arrow") |>
    dplyr::filter(
      year_id %in% year_range,
      age_group_id %in% map_age_gbd$age_group_id,
      sex_id != 3
    ) |>
    dplyr::select(dplyr::all_of(id_cols), deaths)

ds_hiv_free <- prep_ds(fs::path(dir_hiv_free, file_input))
ds_with_hiv <- prep_ds(fs::path(dir_with_hiv, file_input))

dt_hiv <-
  dplyr::full_join(
    ds_hiv_free,
    ds_with_hiv,
    by = id_cols,
    suffix = c(".hiv_free", ".with_hiv")
  ) |>
  dplyr::mutate(deaths_hiv = deaths.with_hiv - deaths.hiv_free) |>
  dplyr::select(dplyr::all_of(id_cols), deaths_hiv) |>
  dplyr::arrange(year_id, sex_id, draw) |>
  dplyr::collect()

# Ensure that there are no negative HIV deaths within a small floating-point
# rounding error
stopifnot(
  !anyNA(dt_hiv),
  nrow(dt_hiv[deaths_hiv < -tol_neg_hiv]) == 0
)


# Prep data ---------------------------------------------------------------

dt_hiv[deaths_hiv < 0, deaths_hiv := 0]

dt_hiv_prep <- dcast(dt_hiv, ... ~ draw, value.var = "deaths_hiv")
setnames(dt_hiv_prep, as.character(draw_range), paste0("draw_", draw_range))


# Save data ---------------------------------------------------------------

dt_hiv_prep |>
  split(by = "sex_id", keep.by = FALSE) |>
  purrr::iwalk(\(x, y) readr::write_csv(
    x,
    fs::path(dir$upload_hiv, y, args$loc_id, ext = "csv")
  ))
