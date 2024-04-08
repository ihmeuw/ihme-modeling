
# Meta --------------------------------------------------------------------

# Description: Downloads/compiles all inputs needed to run the COVID excess
#  mortality model
# Steps:
#   1. Downloads data from processing pipeline outputs
#   2. Creates year offset
# Inputs:
#   * Detailed configuration file.
# Outputs:
#   * All-cause mortality data
#   * COVID death data


# Load libraries ----------------------------------------------------------

message(Sys.time(), " | Setup")

library(argparse)
library(assertable)
library(data.table)
library(demInternal)
library(lubridate)


# Command line arguments --------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", type = "character", required = !interactive(),
  help = "base directory for this version run"
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

# get mappings
process_locations <- fread(paste0(main_dir, "/inputs/process_locations.csv"))
process_sexes <- fread(paste0(main_dir, "/inputs/process_sexes.csv"))
process_ages <- fread(paste0(main_dir, "/inputs/process_ages.csv"))


# Processed data ---------------------------------------------------------

message(Sys.time(), " | Processed all-cause data with population")

# make directory
dir.create(paste0(main_dir, "/inputs/data_all_cause/"))
dir.create(paste0(main_dir, "/inputs/data_all_cause_not_offset/"))

# loop over location
for (l in process_locations[(is_estimate_1) | (is_estimate_2), ihme_loc_id]) {

  if (l == "UKR_50559") {
    l <- "UKR"
  }

  dt <- fread(fs::path(base_dir_data, external_id_covid_em_data, "outputs",
                       paste0(l, ".csv")))

  if (l == "UKR") {
    dt <- dt[year_start >= 2014]
    dt[, location_id := 50559]
    l <- "UKR_50559"
  }

  assertable::assert_colnames(
    dt, c(id_cols, "age_name", "deaths", "deaths_covid", "person_years"),
    only_colnames = F, quiet = T
  )
  assertthat::assert_that(
    "0 to 125" %in% unique(dt$age_name),
    msg = paste0("Missing all-age aggregates for ", l)
  )

  if (l == "GBR_434") dt[age_name != "0 to 125", deaths := NA]

  dt <- dt[
    year_start %in% estimation_year_start:estimation_year_end
  ]

  dt[, population := person_years]

  # optional subset to all-age both-sex. set in config and done for speed in tests.
  if (only_all_age_both_sex) dt <- dt[sex == "all" & age_name == "0 to 125"]

  if (l == "TWN") {
    dt <- dt[time_unit == "month"]
  }

  # select time unit to use based on config
  # "auto" option uses weekly if weekly is available
  if (run_time_units %in% c("month", "week")) {
    dt <- dt[time_unit == run_time_units]
  } else if (run_time_units == "auto") {
    has_weekly <- ("week" %in% unique(dt[!is.na(deaths)]$time_unit))
    if (has_weekly) {
      dt <- dt[time_unit == "week"]
    } else {
      dt <- dt[time_unit == "month"]
    }
  } else {
    stop(paste0("invalid `run_time_units` in config: ", run_time_units))
  }

  # find time of first covid death for this location
  # fill in with default cutoff from config if missing COVID deaths
  if (use_loc_specific_time_cutoff) {

    if (nrow(dt[!is.na(deaths_covid) & deaths_covid > 0]) > 0) {
      dt[, year_cutoff := min(year_start[!is.na(deaths_covid) & deaths_covid > 0])]
      dt[, time_cutoff := min(
        time_start[
          !is.na(deaths_covid) & deaths_covid > 0 & year_start == year_cutoff
        ]
      )]

    } else {
      warning("No COVID deaths for ", l, ". Using default cutoff from config.")
      dt[time_unit == "week",
         `:=` (time_cutoff = default_week_cutoff, year_cutoff = 2020)]
      dt[time_unit == "month",
         `:=` (time_cutoff = default_month_cutoff, year_cutoff = 2020)]
    }

  } else {
    dt[time_unit == "week",
       `:=` (time_cutoff = default_week_cutoff, year_cutoff = 2020)]
    dt[time_unit == "month",
       `:=` (time_cutoff = default_month_cutoff, year_cutoff = 2020)]
  }

  # if running out of sample validation, set year_cutoff back 1
  if (run_validation) dt[, year_cutoff := year_cutoff - 1]

  # save
  readr::write_csv(
    dt, paste0(main_dir, "/inputs/data_all_cause_not_offset/", l, ".csv")
  )

  # create year offset (ex: artificial year Nov-Oct, exact offset set in config)
  if (week_offset > 0 | month_offset > 0) {

    # modeling by month or by week?
    model_monthly <- (max(dt$time_start) <= 12)

    if (model_monthly) {

      dt[, d_start := as.Date(paste0(year_start, "/", time_start, "/15"))]
      dt[, d_cutoff := as.Date(paste0(year_cutoff, "/", time_cutoff, "/15"))]
      dt[, d_start := d_start + months(month_offset)]
      dt[, d_cutoff := d_cutoff + months(month_offset)]
      dt[, `:=` (year_start = lubridate::year(d_start),
                 time_start = lubridate::month(d_start),
                 year_cutoff = lubridate::year(d_cutoff),
                 time_cutoff = lubridate::month(d_cutoff))]
      dt[, c("d_start", "d_cutoff") := NULL]

      assertthat::assert_that(
        max(dt$time_start) <= 12, max(dt$time_cutoff) <= 12,
        min(dt$time_start) > 0, min(dt$time_cutoff) > 0,
        msg = paste0("error introduced in year offset algorithm for monthly data")
      )

    } else {

      dt[time_start > (52 - week_offset),
         `:=` (year_start = year_start + 1,
               time_start = time_start - 52)
      ]
      dt[time_cutoff > (52 - week_offset),
         `:=` (year_cutoff = year_cutoff + 1,
               time_cutoff = time_cutoff - 52)
      ]
      dt[, `:=` (time_start = time_start + week_offset,
                 time_cutoff = time_cutoff + week_offset)]

      assertthat::assert_that(
        max(dt$time_start) <= 52, max(dt$time_cutoff) <= 52,
        min(dt$time_start) > 0, min(dt$time_cutoff) > 0,
        msg = paste0("error introduced in year offset algorithm for weekly data")
      )
    }
  }

  # save
  readr::write_csv(
    dt, paste0(main_dir, "/inputs/data_all_cause/", l, ".csv")
  )

  dt[, test := .N, by = id_cols]
  assertable::assert_values(dt, "test", test = "equal", test_val = 1)

}


# Ensemble weights for stage-1 --------------------------------------------

fs::file_copy(
  fs::path(code_dir, "constant_inputs", c(rmse_file, rmse_loc_file)),
  fs::path(main_dir, "inputs", c(rmse_file, rmse_loc_file)),
  overwrite = T
)


