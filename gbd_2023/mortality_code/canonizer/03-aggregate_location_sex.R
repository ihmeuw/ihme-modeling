
# Meta --------------------------------------------------------------------

# Calculate aggregate location and both-sex mortality


# Load packages -----------------------------------------------------------

library(data.table)
library(mortdb) # Needs to be loaded for mortcore::agg_results()


# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

# Node arguments
parser$add_argument(
  "--agg_year",
  type = "integer",
  required = !interactive(),
  default = 2020
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
  "--special_aggregates",
  type = "logical",
  required = !interactive(),
  default = TRUE,
  help = "Create special reporting location aggregates"
)
parser$add_argument(
  "--gbd_year",
  type = "integer",
  required = !interactive(),
  default = 2021
)
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
dir$out <- fs::path(dir$main, "output")

year_range <- (args$start_year):(args$end_year)
year_idx <- which(year_range == args$agg_year) - 1

draw_range <- do.call(seq, as.list(cfg$draw_range))

id_cols <- c("location_id", "year_id", "age_group_id", "draw")
id_cols_pop <- c("location_id", "year_id", "age_group_id")


# Load maps ---------------------------------------------------------------

map_age <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_canonical.arrow"))
map_loc <- arrow::read_ipc_file(fs::path(dir$cache, "loc_map.arrow"))

if (isTRUE(args$special_aggregates)) {
  map_loc_special <- arrow::read_ipc_file(fs::path(dir$cache, "loc_map_special.arrow"))
}


# Load data ---------------------------------------------------------------

## Life tables ----

path_canon_lt <- fs::path(dir$out, "most_detailed_lt") |>
  fs::dir_ls(type = "file", regexp = "\\.arrow$")

load_canon_lts <- function(path, year_idx) {
  file_obj <- arrow::ReadableFile$create(path)
  on.exit(file_obj$close())
  reader <- arrow::RecordBatchFileReader$create(file_obj)
  as.data.table(reader$get_batch(year_idx))
}

dt_canon <- path_canon_lt |>
  lapply(load_canon_lts, year_id = year_idx) |>
  rbindlist()

stopifnot(
  unique(dt_canon$year_id) == args$agg_year,
  setequal(dt_canon$age_group_id, map_age$age_group_id)
)

## Population ----

dt_pop <- fs::path(dir$cache, "pop_detailed.arrow") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(year_id == args$agg_year) |>
  dplyr::select(-run_id, -age_start) |>
  dplyr::collect() |>
  dcast(... ~ sex_id, value.var = "mean") |>
  setnames(c("1", "2", "3"), c("pop_male", "pop_female", "pop_all"))

stopifnot(all(map_loc$location_id) %in% unique(dt_pop$location_id))
if (isTRUE(args$special_aggregates)) {
  stopifnot(all(map_loc_special$location_id) %in% unique(dt_pop$location_id))
}


# Format data -------------------------------------------------------------

cols_mx_male <- grep("^mx.*_male$", names(dt_canon), value = TRUE)
cols_mx_female <- grep("^mx.*_female$", names(dt_canon), value = TRUE)

check_pop_canon_ids <- fsetdiff(
  unique(dt_canon[, .(location_id, age_group_id)]),
  unique(dt_pop[, .(location_id, age_group_id)])
)
stopifnot(nrow(check_pop_canon_ids) == 0)

dt_canon[
  dt_pop,
  (cols_mx_male) := lapply(.SD, \(x) i.pop_male * x),
  .SDcols = cols_mx_male,
  on = id_cols_pop
]

dt_canon[
  dt_pop,
  (cols_mx_female) := lapply(.SD, \(x) i.pop_female * x),
  .SDcols = cols_mx_female,
  on = id_cols_pop
]

dt_canon[, `:=`(
  axdeaths_male = ax_male * mx_male,
  axdeaths_female = ax_female * mx_female
)]
dt_canon[, c("ax_male", "ax_female") := NULL]

setnames(dt_canon, gsub("^mx_", "deaths_", names(dt_canon)))


# Aggregate locations -----------------------------------------------------

agg_cols <- setdiff(names(dt_canon), id_cols)

## Standard GBD ----

dt_agg_nonscaled <- mortcore::agg_results(
  dt_canon,
  value_vars = agg_cols,
  id_vars = id_cols,
  agg_hierarchy = TRUE,
  end_agg_level = 0,
  loc_scalars = F,
  gbd_year = args$gbd_year
)

# Pull out non-scaled regional shock deaths
dt_agg_shocks <- dt_agg_nonscaled[
  location_id %in% map_loc[level < 3, location_id],
  .(
    location_id, year_id, age_group_id, draw,
    shocks_male = deaths_with_shock_male - deaths_male,
    shocks_female = deaths_with_shock_female - deaths_female
  )
]

dt_agg_scaled <- mortcore::agg_results(
  dt_agg_nonscaled[
    location_id %in% map_loc[level == 3, location_id],
    -c("deaths_with_shock_male", "deaths_with_shock_female")
  ],
  value_vars = grep("shock", agg_cols, invert = TRUE, value = TRUE),
  id_vars = id_cols,
  agg_hierarchy = TRUE,
  start_agg_level = 2,
  end_agg_level = 0,
  loc_scalars = TRUE,
  gbd_year = args$gbd_year
)

dt_agg_scaled[
  dt_agg_shocks,
  `:=`(
    deaths_with_shock_male = deaths_male + i.shocks_male,
    deaths_with_shock_female = deaths_female + i.shocks_female
  ),
  on = id_cols
]

check_region_withshock <- dt_agg_scaled[
  (is.na(deaths_with_shock_male) | is.na(deaths_with_shock_female)) &
    location_id %in% map_loc[level < 3, location_id]
]
stopifnot("not all regions have shock deaths" = nrow(check_region_withshock) == 0)

loc_nonregion_agg <- map_loc[level > 2 & !is_lowest, location_id]

dt_canon_agg <-
  list(
    dt_canon,
    dt_agg_nonscaled[location_id %in% loc_nonregion_agg],
    dt_agg_scaled[location_id %in% map_loc[level < 3, location_id]]
  ) |>
  rbindlist(use.names = TRUE) |>
  unique(by = id_cols)

stopifnot(
  !anyNA(dt_canon_agg),
  all(map_loc$location_id %in% unique(dt_canon_agg$location_id)),
  dt_canon_agg[, .N, by = .(location_id, age_group_id)][N != length(draw_range)] == 0
)

rm(dt_agg_nonscaled, dt_agg_scaled, dt_agg_shocks, dt_canon)
gc()

## Reporting location sets ----

if (isTRUE(args$special_aggregates)) {

  locs_special_agg <- setdiff(map_loc_special$location_id, map_loc$location_id)
  locs_all <- unique(c(map_loc$location_id, map_loc_special$location_id))

  agg_list <-
    map_loc_special[
      j = .(child = location_id, parent = parent_id),
      by = .(
        location_set,
        top_parent = tstrsplit(path_to_top_parent, ",", keep = 1, type.convert = TRUE)[[1]]
      )
    ][child != parent] |>
    split(by = c("location_set", "top_parent"), keep.by = FALSE)

  dt_agg_special <- agg_list |>
    lapply(\(x) hierarchyUtils::agg(
      dt = dt_canon_agg[location_id %in% x$child],
      id_cols = id_cols,
      value_cols = agg_cols,
      col_stem = "location_id",
      col_type = "categorical",
      mapping = x,
      present_agg_severity = "none"
    )) |>
    rbindlist(use.names = TRUE)

  stopifnot(
    !anyNA(dt_agg_special),
    all(locs_special_agg %in% unique(dt_agg_special$location_id))
  )

  dt_canon_agg <-
    list(dt_canon_agg, dt_agg_special[location_id %in% locs_special_agg]) |>
    rbindlist(use.names = TRUE) |>
    unique(by = id_cols)

  stopifnot(
    !anyNA(dt_canon_agg),
    setequal(locs_all, dt_canon_agg$location_id),
    dt_canon_agg[, .N, by = .(location_id, age_group_id)][N != length(draw_range)] == 0
  )

  rm(dt_agg_special)

}


# Aggregate sex -----------------------------------------------------------

c("deaths", "deaths_with_shock", "axdeaths") |>
  purrr::walk(\(x) {
    col_all <- paste0(x, "_all")
    col_male <- paste0(x, "_male")
    col_female <- paste0(x, "_female")
    dt_canon_agg[, (col_all) := get(col_male) + get(col_female)]
  })


# Convert to life table ---------------------------------------------------

check_pop_canon_ids <- fsetdiff(
  unique(dt_canon_agg[, .(location_id, age_group_id)]),
  unique(dt_pop[, .(location_id, age_group_id)])
)

stopifnot(nrow(check_pop_canon_ids) == 0)

purrr::walk(c("male", "female", "all"), \(sex) {

  dt_canon_agg[, paste0("ax_", sex) := get(paste0("axdeaths_", sex)) / get(paste0("deaths_", sex))]
  dt_canon_agg[, paste0("axdeaths_", sex) := NULL]

  col_deaths <- grep(paste0("^deaths.*_", sex, "$"), names(dt_canon_agg), value = TRUE)
  col_mx <- gsub("deaths", "mx", col_deaths)

  dt_canon_agg[
    dt_pop,
    (col_deaths) := lapply(.SD, \(x) x / get(paste0("i.pop_", sex))),
    .SDcols = col_deaths,
    on = id_cols_pop
  ]

  setnames(dt_canon_agg, col_deaths, col_mx)

})


# Save --------------------------------------------------------------------

dt_canon_agg |>
  split(by = "location_id", keep.by = FALSE) |>
  purrr::iwalk(\(x, y) arrow::write_ipc_file(
    x,
    fs::path(dir$out, "agg_full_lt", paste0("location_id=", y), args$agg_year, ext = "arrow")
  ))
