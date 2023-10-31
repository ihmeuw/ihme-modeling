## Title: Empirical Life Table generation
## Authors: AUTHOR
# (1) Generate life table parameters (ax, qx, lx, dx)
# (2) Extend life tables to age 105-109
# (3) Perform smoothing (3, 5, 7 years)

# setup -------------------------------------------------------------------

rm(list=ls())

# libraries
library(pacman)
pacman::p_load(readr, data.table, assertable, haven, tidyr, parallel, rhdf5,
               stringr, argparse, plyr)
library(mortdb,   lib.loc = "FILEPATH")
library(mortcore, lib.loc = "FILEPATH")
library(ltcore,   lib.loc = "FILEPATH")

# arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type = "integer", required = TRUE,
                    help = 'The version_id for this run')
parser$add_argument("--covid_years", type="integer", nargs="+", required = TRUE,
                    help = "Years where VR data should be treated as including COVID")
parser$add_argument('--ihme_loc_id', type = "character", required = TRUE,
                    help = 'The location for this child job')
parser$add_argument('--code_dir', type = "character",
                    required = TRUE,
                    help = "Directory where ELT code is cloned")

args <- parser$parse_args()
empir_run_id <- args$version_id
covid_years <- args$covid_years
loc <- args$ihme_loc_id
code_dir <- args$code_dir

moving_average_weights <- c(1, 1.5, 2, 6, 2, 1.5, 1)

# import process-specific functions
# TODO: make into a formal compiled package
empir_lt_func_dir <- paste0(code_dir, "/empirical_lt_gen/empir_gen_funcs/R")
empir_funcs <- list.files(empir_lt_func_dir)
for(func in empir_funcs) {
  source(paste0(empir_lt_func_dir, "/", func))
}

# set working folders
elt_folder        <- paste0("/mnt/team/mortality/pub/empirical_life_tables")
run_folder        <- paste0(elt_folder, "/", empir_run_id)
run_input_folder  <- paste0(run_folder, "/inputs")
run_output_folder <- paste0(run_folder, "/outputs")

# import HMD ax extension regression results (from mx and from qx)
hmd_ax_regression <- fread(paste0(run_input_folder, "/hmd_ax_extension.csv"))
ax_from_qx <-
  setDT(haven::read_dta(paste0(run_input_folder, "/ax_par_from_qx.dta")))
hmd_qx_extension <-
  fread(paste0(elt_folder, "/constant_inputs/hmd_qx_extension_diff.csv"))

# set unique identifying variables
empir_ids <- c("ihme_loc_id", "source_type", "sex", "year", "deaths_nid",
               "deaths_underlying_nid")

# get prepped data from 01 script & subset by location
empir_deaths_all <- fread(paste0(run_output_folder, "/all_lts_1.csv"))
empir_deaths_all <- empir_deaths_all[ihme_loc_id == loc]

# stop if we don't have any ELTs for this location, or if missing young ages
if (nrow(empir_deaths_all) == 0) {
  quit(status = 0)
  message(paste("No ELT data exist for location", loc))
}
if (nrow(empir_deaths_all[age == 15]) == 0) {
  quit(status = 0)
  message(paste("Missing young ages for location", loc))
}

# build complete life tables -----------------------------------------------

assertable::assert_values(empir_deaths_all, "mx", "not_na")

check_qx_drops <- TRUE
check_qx_drops_iter <- 1
check_qx_drops_iter_max <- 10

while (check_qx_drops & check_qx_drops_iter < check_qx_drops_iter_max) {

  empir_deaths <- empir_deaths_all[age_end < 120]

  # generate initial ax values, and drop sources without a 0-1 age group
  empir_deaths <- gen_initial_ax(empir_deaths, id_vars = c(empir_ids, "age"))
  assertable::assert_values(empir_deaths, "ax", "not_na")

  # apply ax extension
  empir_deaths <- extend_ax(empir_deaths, hmd_ax_regression)
  empir_deaths[ax < 0, ax := .01]
  empir_deaths[ax > 4, ax := 4]

  # prep to apply ax iteration
  gen_age_length(empir_deaths, process_terminal = F)
  empir_deaths[, qx := mx_ax_to_qx(mx, ax, age_length)]
  empir_deaths[qx > 1, qx := 0.99]
  setkeyv(empir_deaths, c(empir_ids, "age"))
  qx_to_lx(empir_deaths)
  empir_deaths[, dx := lx * qx]

  # first remove any qx from ages older than an age 65 with mx <= 0.00001
  empir_deaths[age >= 80 & mx <= 0.00001, age_mx_low := min(age), by = empir_ids]
  empir_deaths[is.na(age_mx_low), age_mx_low := 999]
  empir_deaths[, min_age_mx_low := min(age_mx_low, na.rm=T), by = empir_ids]
  empir_deaths <- empir_deaths[age < min_age_mx_low]

  # ax iteration
  empir_deaths <- iterate_ax(empir_deaths, id_vars = c(empir_ids, "age"),
                             n_iterations = 50)

  dt_qx_drop <- get_min_qx_drop_age(empir_deaths, empir_ids, age_threshold = 65)

  if (isTRUE(nrow(dt_qx_drop) > 0)) {

    dt_new_term <- empir_deaths_all[
      dt_qx_drop,
      on = .(
        ihme_loc_id, source_type, sex, year, deaths_nid, deaths_underlying_nid,
        age >= min_age_qx_change_neg
      )
    ][
      j = .(
        age = min(age),
        age_end = 120,
        deaths = sum(deaths),
        population = sum(population)
      ),
      by = empir_ids
    ]

    dt_new_term[, mx := deaths / population]

    empir_deaths_all <- rbindlist(
      list(
        empir_deaths_all[
          !dt_qx_drop,
          on = .(
            ihme_loc_id, source_type, sex, year, deaths_nid, deaths_underlying_nid,
            age >= min_age_qx_change_neg
          )
        ],
        dt_new_term
      ),
      use.names = TRUE
    )

    setorderv(empir_deaths_all, c(empir_ids, "age"))

  } else {

    check_qx_drops <- FALSE

  }

  check_qx_drops_iter <- check_qx_drops_iter + 1

}

stopifnot(!check_qx_drops & (check_qx_drops_iter <= check_qx_drops_iter_max))

# separate out terminal age group
terminal <- empir_deaths_all[age_end == 120]
setnames(terminal, c("age", "mx"), c("terminal_age_start", "mx_term"))

empir_deaths[, c("deaths", "population") := NULL]
terminal[, c("deaths", "population") := NULL]

# apply qx extension using HMD regression
empir_deaths <- extend_qx_diff(empir_deaths, hmd_qx_extension,
                               by_vars = empir_ids)

# calculate mx, lx, dx if missing
empir_deaths[is.na(mx) & age >= 5, mx := qx_ax_to_mx(qx, ax, 5)]
setkeyv(empir_deaths, c(empir_ids, "age"))
qx_to_lx(empir_deaths, assert_na = T)
empir_deaths[is.na(dx), dx := lx * qx]

# terminal scaling --------------------------------------------------------

# only scale to terminal if terminal exists & not ZAF
scale_terminal <- T
if (loc %like% "ZAF") scale_terminal <- F

if (nrow(terminal) > 0 & scale_terminal == T) {

  # merge on terminal_age_start and mx_term
  empir_deaths <- merge(empir_deaths, terminal, by = empir_ids, all.x = T,
                        allow.cartesian = T)

  # iterate qx to scale to observed terminal mx
  scaled <- iterate_qx(empir_deaths, ax_from_qx, id_vars = c(empir_ids, "age"),
                       n_iterations = 30)
  empir_deaths <- scaled[[1]]
  reset        <- scaled[[2]] # metadata about scaling successs

  # Assert that there are no decreases in 95+ qx
  assert_increase <- empir_deaths[age >= 95][order(age), qx_diff_prev := qx - shift(qx, type = "lag"), by = empir_ids]
  assert_increase <- assert_increase[!is.na(qx_diff_prev), .(all_increase = all(qx_diff_prev >= 0)), by = empir_ids]

  if (nrow(assert_increase[!(all_increase)]) > 0) {
    stop("There are some decreases in 95+ plus qx after qx iteration")
  }

  readr::write_csv(
    reset,
    paste0(run_output_folder, "/loc_specific/terminal_scale_info_", loc, ".csv"),
  )

}

# recalculate lx and dx (since some life tables had lx get set to 0 or near 0
setkeyv(empir_deaths, c(empir_ids, "age"))
qx_to_lx(empir_deaths)
empir_deaths[, dx := lx * qx]

# smoothing over years ----------------------------------------------------

empir_deaths <- setkeyv(empir_deaths, c(empir_ids, "age"))
smoothing_by_vars <- c("ihme_loc_id", "source_type", "sex", "age")

# smooth width 3 years
if(nrow(empir_deaths[!year %in% covid_years]) != 0) {
  
  smoothed_3_year <- smooth_lts(
    empir_deaths[!year %in% covid_years],
    smoothing_by_vars,
    moving_average_weights = moving_average_weights[3:5]
  )
  smoothed_3_year[, smooth_width := 3]
  
  # smooth width 5 years
  smoothed_5_year <- smooth_lts(
    empir_deaths[!year %in% covid_years],
    smoothing_by_vars,
    moving_average_weights = moving_average_weights[2:6]
  )
  smoothed_5_year[, smooth_width := 5]
  
  # smooth width 7 years
  smoothed_7_year <- smooth_lts(
    empir_deaths[!year %in% covid_years],
    smoothing_by_vars,
    moving_average_weights = moving_average_weights
  )
  smoothed_7_year[, smooth_width := 7]
  
} else {
  
  smoothed_3_year <- data.table()
  smoothed_5_year <- data.table()
  smoothed_7_year <- data.table()
  
}

# combine
empir_deaths[, smooth_width := 1]
all_lts <- rbindlist(
  list(empir_deaths, smoothed_3_year, smoothed_5_year, smoothed_7_year),
  use.names = T
)

# keep only needed cols
all_lts <- all_lts[, c("sex", "age", "ihme_loc_id", "source_type", "year",
                       "smooth_width", "deaths_nid", "deaths_underlying_nid",
                       "mx", "ax", "qx", "lx", "dx")]

# write life tables to .csv
readr::write_csv(
  all_lts,
  paste0(run_output_folder,"/loc_specific/all_lts_", loc, ".csv")
)

## END
