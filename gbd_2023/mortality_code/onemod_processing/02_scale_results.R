# Meta --------------------------------------------------------------------

# Input: Onemod draws
# Method:
# Script takes in onemod draws, interpolates them into canonical (deatiled u1 + single-year)
# age groups, rakes them by location and then


# Load packages -----------------------------------------------------------

library(data.table)
library(mortdb) # Needs to be loaded for mortcore::agg_results()

# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

# Node arguments
parser$add_argument(
  "--dir_output",
  type = "character",
  required = !interactive(),
  default = "FILEPATH"
)
parser$add_argument(
  "--code_dir",
  type = "character",
  required = !interactive(),
  default = here::here()
)
parser$add_argument(
  "--year_id",
  type = "integer",
  required = !interactive(),
  default = 2024L
)

args <- parser$parse_args()


# Set parameters ----------------------------------------------------------

cfg <- config::get(file = fs::path(args$dir_output, "config.yml"))

id_vars <- c("location_id", "year_id", "sex_id", "age_group_id")


# Load maps ---------------------------------------------------------------

map_loc <- fread(fs::path(args$dir_output, "loc_map.csv"))
map_age <- fread(fs::path(args$dir_output, "age_map_gbd.csv"))
map_age_canon <- fread(fs::path(args$dir_output, "age_map_canonical.csv"))


# Define Helper Functions -------------------------------------------------

# NOTE: not used now, but could be considered in the future
logit_mean <- function(x) {
  4 * boot::inv.logit(mean(boot::logit(x / 4)))
}

resample_draws <- function(dt, pct, seed) {

  withr::local_seed(seed)

  quantiles <- quantile(dt$mx, probs = c(pct, 1 - pct))

  middle_values <- dt[mx > quantiles[1] & mx < quantiles[2], mx]

  dt[mx <= quantiles[1] | mx >= quantiles[2], mx := sample(middle_values, .N, replace = TRUE)]

}

save_loc_year <- function(dt) {

  current_loc_id <- unique(dt$location_id)
  yr <- unique(dt$year_id)

  filename_out <- glue::glue("{current_loc_id}-{yr}")

  dt |>
    arrow::write_ipc_file(fs::path(args$dir_output, "draws", filename_out, ext = "arrow"))
}


# Load data ---------------------------------------------------------------

dt <-
  cfg$fp_inputs$onemod_draws |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::filter(year_id == args$year_id) |>
  dplyr::select(dplyr::all_of(id_vars), dplyr::starts_with("draw")) |>
  dplyr::collect() |>
  setDT() |>
  melt(
    id.vars = id_vars,
    variable.name = "draw",
    value.name = "mx",
    variable.factor = FALSE
  ) |>
  _[, draw := as.integer(gsub("draw_", "", draw))]

dt_pred <-
  cfg$fp_inputs$onemod_means |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::filter(year_id == args$year_id) |>
  dplyr::select(dplyr::all_of(id_vars), mx = mx_final) |>
  dplyr::collect() |>
  dplyr::mutate(draw = max(dt$draw) +  1)

dt <- rbind(dt, dt_pred)

dt_pop <-
  fs::path(args$dir_output, "population", ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::filter(year_id == args$year_id) |>
  dplyr::collect() |>
  setDT()

dt_pop_canon <-
  fs::path(args$dir_output, "population_canon", ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::filter(year_id == args$year_id) |>
  dplyr::collect() |>
  setDT()


# convert to deaths -------------------------------------------------------

dt[dt_pop, population := i.mean, on = id_vars]
dt[, deaths := population * mx]


# Scale subnationals to national ------------------------------------------

dt_scaled <-
  dt |>
  mortcore::scale_results(
    id_vars = c(id_vars, "draw"),
    value_var = "deaths",
    gbd_year = cfg$gbd_year,
    exclude_parent = "CHN"
  ) |>
  _[, mx := deaths / population]


# Resample Draws ----------------------------------------------------------

if(cfg$draw_resample_pct > 0) {

  dt_pred_temp <- dt_scaled[draw == max(draw)]

  dt_scaled <-
    dt_scaled[draw != max(draw)] |>
    split(by = id_vars) |>
    purrr::map(\(x) resample_draws(x, pct = cfg$draw_resample_pct, seed = 27)) |>
    rbindlist() |>
    rbind(dt_pred_temp)

  rm(dt_pred_temp)
}


# Interpolate to canonical ages -------------------------------------------

# We want to linearly interpolate log(mx) to single year age groups
dt_scaled[map_age, c("age_start", "age_end") := .(i.age_start, i.age_end), on = .(age_group_id)]

dt_scaled[, log_mx := log(mx)]

dt_scaled[, age := (age_start + age_end) / 2]

# remove under 2 and 95+ because we won't change those
dt_scaled_ends <- dt_scaled[age_start < 2 | age_start == 95]

dt_canon <- copy(dt_scaled)

dt_canon <- dt_canon |>
  setorder(location_id, sex_id, year_id, age) |>
  _[,
    approx(x = age, y = log_mx, xout = map_age_canon$age_start),
    by = .(location_id, sex_id, year_id, draw)
  ] |>
  setnames(
    c("x", "y"),
    c("age_start", "log_mx")
  )

dt_canon[dt_scaled_ends, log_mx := i.log_mx, on = .(location_id, sex_id, year_id, draw, age_start)]

dt_canon[
  map_age_canon,
  `:=`(
    age_group_id = i.age_group_id,
    age_end = i.age_end
  ),
  on = .(age_start)
]

dt_canon[, mx := exp(log_mx)]

dt_canon[dt_pop_canon, population := i.mean, on = id_vars]
dt_canon[, deaths := mx * population]


# Scale canonical ages mx to gbd ages mx ----------------------------------

keep_cols <- c(id_vars, "draw", "population", "deaths")

dt_canon_agg <- dt_canon[, ..keep_cols] |> copy()

dt_canon_agg <-
  dt_canon_agg |>
  mortcore::agg_results(
    id_vars = c(id_vars, "draw"),
    value_vars = c("population", "deaths"),
    age_aggs = map_age$age_group_id,
    agg_sex = FALSE,
    agg_hierarchy = FALSE
  ) |>
  _[age_group_id %in% map_age$age_group_id] |>
  _[, mx := deaths / population] |>
  _[dt_scaled, mx_onemod_raked := i.mx, on = c(id_vars, "draw")] |>
  _[, mx_scalar := mx_onemod_raked / mx] |>
  _[map_age, c("abgd_age_start", "abgd_age_end") := .(i.age_start, i.age_end), on = .(age_group_id)]

dt_canon[
  dt_canon_agg,
  mx_scalar := i.mx_scalar,
  on = .(
    location_id, sex_id, year_id, draw,
    age_start >= abgd_age_start, age_end <= abgd_age_end
  )
] |>
  _[, mx_age_scaled := mx_scalar * mx] |>
  _[, deaths_age_scaled := mx_age_scaled * population]


# Validate ----------------------------------------------------------------

# deaths and mx should be greater than 0
assertable::assert_values(
  dt_scaled,
  colnames = c("mx_age_scaled", "deaths_age_scaled"),
  test = "gt",
  test_val = 0
)


# Format ------------------------------------------------------------------

dt_canon[mx_age_scaled > 4, mx_age_scaled := 3.99] |>
  _[, deaths_age_scaled := mx_age_scaled * population]

keep_cols <- c(id_vars, "draw", "population", "deaths_age_scaled")
dt_canon <- dt_canon[, ..keep_cols]

dt_canon <-
  dt_canon |>
  mortcore::agg_results(
    id_vars = c(id_vars, "draw"),
    value_vars = c("population", "deaths_age_scaled"),
    age_aggs = map_age$age_group_id,
    agg_sex = FALSE,
    agg_hierarchy = FALSE
  ) |>
  _[, mx_age_scaled := deaths_age_scaled / population]

dt_pred_adj <- dt_canon[draw == max(draw)]
dt_canon <- dt_canon[draw != max(draw)]

dt_canon_summary <-
  dt_canon |>
  demUtils::summarize_dt(
    id_cols = c(id_vars, "draw"),
    summarize_cols = "draw",
    summary_fun = "mean",
    value_cols = c("mx_age_scaled", "deaths_age_scaled", "population")
  ) |>
  _[, c("population_q2.5", "population_q97.5") := NULL]

dt_summary <-
  dt[draw != max(draw)] |>
  demUtils::summarize_dt(
    id_cols = c(id_vars, "draw"),
    summarize_cols = "draw",
    value_cols = c("mx", "deaths"),
    probs = NULL
  )

dt_canon_summary[
  dt_summary,
  `:=`(
    mx_onemod_unraked = i.mx_mean,
    deaths_onemod_unraked = i.deaths_mean
  ),
  on = id_vars
]

dt_scaled_summary <-
  dt_scaled[draw != max(draw)] |>
  demUtils::summarize_dt(
    id_cols = c(id_vars, "draw"),
    summarize_cols = "draw",
    value_cols = c("mx", "population", "deaths"),
    probs = NULL
  )

dt_canon_summary[
  dt_scaled_summary,
  `:=`(
    mx_onemod_raked = i.mx_mean,
    deaths_onemod_raked = i.deaths_mean
  ),
  on = id_vars
]


# Save --------------------------------------------------------------------

dt_canon |>
  split(by = c("location_id", "year_id")) |>
  purrr::map(as.data.frame) |>
  purrr::walk(save_loc_year)

dt_canon_summary |>
  as.data.frame() |>
  arrow::write_ipc_file(
    fs::path(args$dir_output, "summaries", args$year_id, ext = "arrow")
  )

dt_pred_adj |>
  as.data.frame() |>
  arrow::write_ipc_file(
    fs::path(args$dir_output, "msca_pred", args$year_id, ext = "arrow")
  )
