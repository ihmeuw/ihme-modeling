
# Meta --------------------------------------------------------------------

# Cache database inputs for faster loading and to avoid I/O issues


# Load packages -----------------------------------------------------------

library(data.table)


# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

# Version arguments
parser$add_argument(
  "--version",
  type = "integer",
  required = !interactive(),
  default = 463,
  help = "Version of canonizer process"
)
parser$add_argument(
  "--run_id_pop",
  type = "integer",
  required = !interactive(),
  default = 359,
  help = "Version of population estimate"
)
parser$add_argument(
  "--run_id_pop_sy",
  type = "integer",
  required = !interactive(),
  default = 296,
  help = "Version of population single year estimate"
)

# Process arguments
parser$add_argument(
  "--gbd_year",
  type = "integer",
  required = !interactive(),
  default = 2021
)
parser$add_argument(
  "--special_aggregates",
  type = "logical",
  required = !interactive(),
  default = TRUE,
  help = "Create special reporting location aggregates"
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
devtools::load_all(fs::path(args$code_dir, "canonizer"))

dir_out <- fs::path(cfg$dir$base, args$version, "cache")

stopifnot(fs::dir_exists(dir_out))


# Load maps ---------------------------------------------------------------

## Locations ----

map_loc <- demInternal::get_locations(gbd_year = args$gbd_year)
map_loc_lowest <- demInternal::get_locations(gbd_year = args$gbd_year, level = "lowest")[level > 2]

map_loc[, is_lowest := location_id %in% map_loc_lowest$location_id]

if (isTRUE(args$special_aggregates)) {

  map_loc_special <- cfg$special_location_sets |>
    rlang::set_names() |>
    lapply(\(x) demInternal::get_locations(
      gbd_year = args$gbd_year,
      location_set_name = x
    )) |>
    rbindlist(idcol = "location_set")

}

## HIV metadata ----

dt_hiv_groups <- fread(fs::path(
  cfg$constant_inputs$hiv_metadata_dir, paste0("hiv_model_strategy_", args$gbd_year, ".csv")
))[, .(location_id, ihme_loc_id, location_name, group)]

map_loc[dt_hiv_groups, hiv_group := i.group, on = "location_id"]
stopifnot(map_loc[is.na(hiv_group) & level > 2, .N] == 0)

locs_2c <- c(
  "PHL_53598", "PHL_53609", "PHL_53610", "PHL_53611", "PHL_53612", "PHL_53613",
  "IDN_4740","IDN_4741","IDN_4739",
  "VNM"
)
locs_1a <- c("MRT","COM","STP")

map_loc[, hiv_group_load := hiv_group]
map_loc[ihme_loc_id %in% locs_2c | ihme_loc_id %like% "CHN", hiv_group_load := "2C"]
map_loc[ihme_loc_id %in% locs_1a, hiv_group_load := "1A"]

## Ages ----

cols_keep_age <- c("age_group_id", "age_group_name", "age_group_name_short", "age_start", "age_end")

map_ages_all <- demInternal::get_age_map(
  gbd_year = args$gbd_year, type = "all", all_metadata = TRUE
)[, ..cols_keep_age]

list_map_ages <-
  c("lifetable", "gbd", "single_year") |>
  rlang::set_names() |>
  lapply(\(x) demInternal::get_age_map(
    gbd_year = args$gbd_year,
    type = x,
    all_metadata = TRUE
  )[, ..cols_keep_age])

list_map_ages$single_year[, abridged_age_start := c(0, 1, rep(2, 3),  rep(seq(5, 105, 5), each = 5), 110)]

# Create age maps with life table and envelope specific reporting age groups
list_map_ages$reporting_lt <- map_ages_all[
  age_group_id %in% c(1, 4, 5, 28, 42, 199)
][order(age_end, -age_start)]
list_map_ages$reporting_envelope <-
  list(
    demInternal::get_age_map(
      gbd_year = args$gbd_year, type = "envelope", all_metadata = TRUE,
    )[, ..cols_keep_age],
    map_ages_all[age_group_id == 197]
  ) |>
  rbindlist(use.names = TRUE) |>
  unique() |>
  fsetdiff(y = rbindlist(list_map_ages[c("gbd", "reporting_lt")], use.names = TRUE)) |>
  setorder(age_end, -age_start)

# Create age maps with final age groups in summary results
list_map_ages$summary_lt <- rbindlist(
  list_map_ages[c("gbd", "reporting_lt")],
  use.names = TRUE,
  idcol = "source"
)
list_map_ages$summary_envelope <- rbindlist(
  list_map_ages[c("gbd", "reporting_lt", "reporting_envelope")],
  use.names = TRUE,
  idcol = "source"
)

# Check that there is no overlap between the GBD, LT reporting, and envelope
# reporting age groups
map_age_check <- list_map_ages$summary_envelope[, .(age_group_id, age_start, age_end)]
stopifnot(fsetequal(map_age_check, unique(map_age_check)))
rm(map_age_check)

list_map_ages$lifetable_mdu5 <- rbind(
  list_map_ages$gbd[age_start < 5],
  list_map_ages$lifetable[age_start >= 5]
)

list_map_ages$single_year_mdu1 <- rbind(
  list_map_ages$gbd[age_start < 1],
  list_map_ages$single_year[age_start >= 1],
  fill = TRUE
)

list_map_ages$single_year_mdu1[is.na(abridged_age_start), abridged_age_start := age_start]

age_id_term_gbd <- list_map_ages$gbd[is.infinite(age_end), age_group_id]

age_id_ext_lt <- list_map_ages$single_year[
  age_start >= list_map_ages$gbd[is.infinite(age_end), age_start],
  age_group_id
]

## Sex ----

map_sex <- data.table(
  sex_id = 1L:3L,
  sex = c("male", "female", "all")
)


# Load data ---------------------------------------------------------------

dt_pop <- demInternal::get_dem_outputs(
  "population estimate",
  run_id = args$run_id_pop,
  age_group_ids = list_map_ages$gbd$age_group_id
)

dt_pop[, c("upload_population_estimate_id", "lower", "upper") := NULL]
dt_pop[list_map_ages$gbd, age_start := i.age_start, on = "age_group_id"]

dt_popsy <- demInternal::get_dem_outputs(
  "population single year estimate",
  run_id = args$run_id_pop_sy
)

dt_popsy[, c("upload_population_single_year_estimate_id", "lower", "upper") := NULL]
dt_popsy[list_map_ages$single_year, age_start := i.age_start, on = "age_group_id"]
dt_popsy[list_map_ages$gbd[is.infinite(age_end)], age_start := i.age_start, on = "age_group_id"]

stopifnot(!anyNA(dt_pop), !anyNA(dt_popsy))

# NOTE: All ages above 95 have the same population as 95+. This is only
# suitable for doing round-trip conversions between rate and count space.
dt_pop_ext <- replicate_age(dt_popsy, age_id_term_gbd, age_id_ext_lt)
dt_pop_ext[list_map_ages$single_year, age_start := i.age_start, on = "age_group_id"]

dt_pop_detailed <- rbind(
  dt_pop[age_start < 1],
  dt_popsy[age_start %between% c(1, 94)],
  dt_pop_ext,
  use.names = TRUE
)

stopifnot(all(sort(unique(dt_pop_detailed$age_group_id)) == sort(list_map_ages$single_year_mdu1$age_group_id)))

setorderv(dt_pop_detailed, c("location_id", "year_id", "sex_id", "age_start"))


# Save --------------------------------------------------------------------

arrow::write_ipc_file(map_loc, fs::path(dir_out, "loc_map.arrow"))
arrow::write_ipc_file(map_loc_lowest, fs::path(dir_out, "loc_map_lowest.arrow"))

if (isTRUE(args$special_aggregates)) {
  arrow::write_ipc_file(map_loc_special, fs::path(dir_out, "loc_map_special.arrow"))
}

purrr::iwalk(list_map_ages, \(x, y) arrow::write_ipc_file(
  x,
  fs::path(dir_out, paste0("age_map_", y), ext = "arrow")
))

arrow::write_ipc_file(map_sex, fs::path(dir_out, "sex_map.arrow"))

arrow::write_ipc_file(dt_pop, fs::path(dir_out, "pop.arrow"))
arrow::write_ipc_file(dt_popsy, fs::path(dir_out, "pop_single_year.arrow"))
arrow::write_ipc_file(dt_pop_detailed, fs::path(dir_out, "pop_detailed.arrow"))
