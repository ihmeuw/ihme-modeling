
# Meta --------------------------------------------------------------------

# Description: Complete stage-1 model
# Steps:
#   1. Create "last year" model where expected mortality is equal to
#      previous year's mortality.
#   2. Create ensemble of all stage-1 models
#   3. Invert time offset if included
#   4. Create summaries and save all summaries and draws


# Load libraries ----------------------------------------------------------

message(Sys.time(), " | Setup")

library(argparse)
library(arrow)
library(assertable)
library(data.table)
library(dplyr)
library(fs)
library(hierarchyUtils)
library(magrittr)
library(lubridate)


# Command line arguments --------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", type = "character", required = !interactive(),
  help = "base directory for this version run"
)
parser$add_argument(
  "--loc_id", type = "integer", required = !interactive(),
  default = 102,
  help = "location ID to process"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)


# Setup -------------------------------------------------------------------

# get config
config <- config::get(
  file = paste0(main_dir, "/covid_em_detailed.yml"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# set the global cpu thread pool as specified in job submission
arrow::set_cpu_count(5)

# convert strings to arrow schema format
draws_schema <- mapply(function(col) eval(parse(text=col)), draws_schema)
draws_schema <- do.call(arrow::schema, draws_schema)

draw_id_cols <- unique(c(partitioning_cols, id_cols, draw_col))


# Helper functions --------------------------------------------------------

rmse_to_weight <- function(rmse) {
  weight <- 1 / (rmse^2)
  weight / sum(weight)
}

save_draws <- function(dt) {

  # type conversions to match schema
  dt[, `:=`(
    location_id = as.integer(location_id),
    year_start = as.integer(year_start),
    time_start = as.integer(time_start),
    age_start = as.numeric(age_start),
    age_end = as.numeric(age_end),
    draw = as.integer(draw),
    value = as.numeric(value)
  )]

  # save formatted draws (only needed for covid years)
  for (yy in covid_years) {
    arrow::write_dataset(
      dataset = dt[year_start == yy],
      path = fs::path(main_dir, "outputs/draws/"),
      format = draws_format,
      partitioning = partitioning_cols,
      basename_template = paste0(yy, "-{i}.", draws_format)
    )
  }

  invisible(dt)

}


# Inputs ------------------------------------------------------------------

message(Sys.time(), " | Inputs")

# get mappings
process_locations <- fread(paste0(main_dir, "/inputs/process_locations.csv"))
process_sexes <- fread(paste0(main_dir, "/inputs/process_sexes.csv"))
process_ages <- fread(paste0(main_dir, "/inputs/process_ages.csv"))

ihme_loc <- process_locations[location_id == loc_id, ihme_loc_id]

# ensemble weights
model_rmse <- fread(paste0(main_dir, "/inputs/stage1_oos_rmse.csv"))
model_rmse_loc <- fread(paste0(main_dir, "/inputs/stage1_oos_rmse_by_loc.csv"))


# Get Poisson ---------------------------------------------------------------

message(Sys.time(), " | Get Poisson")

if ("poisson" %in% run_model_types) {

  # open the arrow dataset
  all_draws_dataset <- arrow::open_dataset(
    sources = fs::path(main_dir, "outputs/draws/"),
    schema = draws_schema,
    unify_schemas = FALSE
  )

  draws <- all_draws_dataset %>%
    dplyr::filter(location_id == loc_id & model_type == "poisson") %>%
    dplyr::collect()
  draws <- data.table(draws)

}


# Append regmod summaries ---------------------------------------------------

if ("regmod" %in% run_model_types) {

  message(Sys.time(), " | Append regmod")

  for (ts in tail_size_month) {

    message(Sys.time(), "| ... tail size ", ts)

    files <- list.files(
      paste0(main_dir, "/outputs/summaries_regmod_", ts),
      pattern = paste0(ihme_loc, "-")
    )

    regmod <- assertable::import_files(
      filenames = files,
      folder = paste0(main_dir, "/outputs/summaries_regmod_", ts)
    )

    if (n_draws > 1) {
      # melt long on draws
      cols <- intersect(names(regmod), c(id_cols, "age_name", "deaths", "population"))
      draw_cols <- names(regmod)[names(regmod) %like% "mortality_pattern_draw_"]
      regmod <- regmod[, .SD, .SDcols = c(cols, draw_cols)]
      regmod <- melt(regmod, id.vars = cols, value.name = draw_value_cols)
      regmod[, draw := gsub("mortality_pattern_draw_", "", variable)]
      regmod[, draw := as.integer(draw)]
      regmod[, variable := NULL]
      setnames(regmod, "value", "deaths_expected")
    } else {
      cols <- intersect(names(regmod), c(id_cols, "age_name", "deaths", "population"))
      regmod <- regmod[, .SD, .SDcols = c(cols, "mortality_pattern")]
      regmod[, draw := 1]
      setnames(regmod, "mortality_pattern", "deaths_expected")
    }

    # compute excess mortality rate
    regmod[, value := (deaths - deaths_expected) / population]

    # fill in time unit
    t <- config::get(
      file = paste0(main_dir, "/inputs/data_all_cause/meta_", ts, ".yaml"),
      config = ihme_loc
    )$time_unit
    regmod[, time_unit := t]

    # format age
    regmod[, age_start := tstrsplit(age_name, " to ", keep = 1)]
    regmod[, age_end := tstrsplit(age_name, " to ", keep = 2)]
    regmod[, c("age_start", "age_end") := lapply(.SD, as.numeric),
           .SDcols = c("age_start", "age_end")]

    # add partitioning variables
    regmod[, `:=` (
      model_type = paste0("regmod_", ts),
      process_stage = "final",
      groupings = "estimated"
    )]

    # subset and sort columns
    regmod <- regmod[, .SD, .SDcols = c(draw_id_cols, draw_value_cols)]
    setkeyv(regmod, draw_id_cols)

    save_draws(regmod)

    # add onto poisson
    if (exists("draws")) {
      draws <- rbind(draws, regmod)
    } else {
      draws <- regmod
    }

    rm(regmod)

  }
}


# Last-year model ----------------------------------------------------------

# use previous year's observed mortality as the "expected mortality" for
# the current year

if ("previous_year" %in% run_model_types) {

  message(Sys.time(), " | Last year model")

  dt_last_yr <- fread(paste0(main_dir, "/inputs/data_all_cause/", ihme_loc, ".csv"))
  dt_last_yr[, model_type := "previous_year"]
  dt_last_yr[, death_rate_observed := deaths / person_years]
  setkeyv(dt_last_yr, c("location_id", "year_start", "time_start"))
  dt_last_yr[
    !(year_start %in% covid_years),
    death_rate_observed_non_covid := death_rate_observed
  ]
  dt_last_yr[,
    death_rate_expected := shift(death_rate_observed_non_covid, 1),
    by = setdiff(id_cols, "year_start")
  ]
  dt_last_yr <- dt_last_yr %>%
    dplyr::group_by(location_id, time_start, age_name, sex) %>%
    tidyr::fill(death_rate_expected, .direction = "down") %>%
    tidyr::fill(death_rate_expected, .direction = "up") %>%
    dplyr::ungroup() %>% setDT

  # compute excess mortality rate
  dt_last_yr[, value := death_rate_observed - death_rate_expected]

  # clean up
  dt_last_yr <- dt_last_yr[!is.na(value)]
  dt_last_yr[, `:=` (draw = 1, process_stage = "final", groupings = "estimated")]
  dt_last_yr <- dt_last_yr[, .SD, .SDcols = c(draw_id_cols, draw_value_cols)]

  save_draws(dt_last_yr)

  # expand to draws
  draw_frame <- data.table(draw = 1, new_draw = 1:n_draws)
  dt_last_yr <- merge(dt_last_yr, draw_frame, by = "draw", allow.cartesian = T)
  dt_last_yr[, `:=` (draw = new_draw, new_draw = NULL)]

  # combine with other models
  draws <- rbind(draws, dt_last_yr)
}


# Ensemble ----------------------------------------------------------------

if ("ensemble_short" %in% run_model_types) {

  message(Sys.time(), " | Ensemble - short regmod tails")

  dt_ensemble_short <- draws[
    model_rmse[model_type %in% ensemble_types$ensemble_short],
    on = "model_type"
  ]

  assertable::assert_values(dt_ensemble_short, "rmse", test = "not_na", quiet = T)

  dt_ensemble_short <- dt_ensemble_short[
    j = list(
      value = sum(value * rmse_to_weight(rmse)),
      model_type = "ensemble_short"
    ),
    by = setdiff(draw_id_cols, "model_type")
  ]

  save_draws(dt_ensemble_short)

  # combine with existing results
  draws <- rbind(draws, dt_ensemble_short, fill = T)

}

if ("ensemble_long" %in% run_model_types) {

  message(Sys.time(), " | Ensemble - long regmod tails")

  dt_ensemble_long <- draws[
    model_rmse[model_type %in% ensemble_types$ensemble_long],
    on = "model_type"
  ]

  assertable::assert_values(dt_ensemble_long, "rmse", test = "not_na", quiet = T)

  dt_ensemble_long <- dt_ensemble_long[
    j = list(
      value = sum(value * rmse_to_weight(rmse)),
      model_type = "ensemble_long"
    ),
    by = setdiff(draw_id_cols, "model_type")
  ]

  save_draws(dt_ensemble_long)

  # combine with existing results
  draws <- rbind(draws, dt_ensemble_long, fill = T)

}

if ("ensemble_loc_best" %in% run_model_types) {

  message(Sys.time(), " | Ensemble - location specific bests")

  stopifnot(
    "Location not present in location-specific RMSE file" = loc_id %in% model_rmse_loc$location_id
  )

  dt_ensemble_loc_best <- draws[
    model_rmse_loc[order(rmse), .SD[1:ensemble_loc_best_top_n], by = "location_id"],
    on = c("location_id", "model_type"),
    nomatch = NULL
  ]

  assertable::assert_values(dt_ensemble_loc_best, "rmse", test = "not_na", quiet = T)

  dt_ensemble_loc_best <- dt_ensemble_loc_best[
    j = list(
      value = sum(value * rmse_to_weight(rmse)),
      model_type = "ensemble_loc_best"
    ),
    by = setdiff(draw_id_cols, "model_type")
  ]

  save_draws(dt_ensemble_loc_best)

  # combine with existing results
  draws <- rbind(draws, dt_ensemble_loc_best, fill = T)

}


# Invert year-offset ------------------------------------------------------

if (month_offset > 0 | week_offset > 0) {

  message(Sys.time(), " | Undo time offsets")

  # undo year offset for monthly results
  draws[
    time_unit == "month",
    d := lubridate::as_date(paste0(year_start, "/", time_start, "/15"))
  ]
  draws[time_unit == "month", d := d - months(month_offset)]
  draws[
    time_unit == "month",
    `:=` (year_start = lubridate::year(d),
          time_start = lubridate::month(d))
  ]
  draws[, d := NULL]

  # undo year offset for weekly results
  draws[
    time_unit == "week" & time_start <= week_offset,
    `:=` (year_start = year_start - 1,
          time_start = time_start + 52)
  ]
  draws[time_unit == "week", time_start := time_start - week_offset]

  # check
  assertthat::assert_that(
    max(draws[time_unit == "month"]$time_start) <= 12,
    min(draws[time_unit == "month"]$time_start) > 0,
    msg = paste0("error introduced in year offset algorithm for monthly data")
  )

  assertthat::assert_that(
    max(draws[time_unit == "week"]$time_start) <= 52,
    min(draws[time_unit == "week"]$time_start) > 0,
    msg = paste0("error introduced in year offset algorithm for weekly data")
  )

}


# Create summaries ---------------------------------------------------------

message(Sys.time(), " | Create summaries")

summaries <- demUtils::summarize_dt(
  dt = draws,
  id_cols = draw_id_cols,
  summarize_cols = draw_col,
  value_col = draw_value_cols,
  summary_fun = summary_functions,
  probs = summary_probs
)

rm(draws)


# Save summaries ----------------------------------------------------------

message(Sys.time(), " | Save summaries")

readr::write_csv(summaries, paste0(main_dir, "/outputs/summaries/", loc_id, ".csv"))
