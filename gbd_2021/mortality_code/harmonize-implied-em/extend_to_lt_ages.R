# Meta --------------------------------------------------------------------
#' Author: AUTHOR
#'
#' Summary: Take the harmonized envelope and convert it into mortality
#' rates. Then, extend the 95+ age group to the LT ages (110+)
#'
#' Inputs:
#' 1. Harmonized no-shock envelope
#' 2. baseline population
#' 3. No-shock life table (unadjusted)
#'
#' Methods:
#' 1. Convert deaths into death rates
#' 2. calculate the ratio between the 95+ age group and each of the
#'    95-99, 100-104, 105-109, 110+ age groups
#' 3. use these ratios to calculate older age mortality for the adjusted envelope
#' 4. (Maybe) use the ax values from the envelope to construct a life table with all variables
#' 5. Save results

# Load packages -----------------------------------------------------------

library(data.table)


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
  "--run_id_finalizer",
  type = "integer",
  required = !interactive(),
  default = 556L,
  help = "With-shock death number estimate run id of finalizer run for no-2020 VR no-shock envelope"
)
parser$add_argument(
  "--loc_id",
  type = "integer",
  required = !interactive(),
  default = 74L,
  help = "Location ID"
)
parser$add_argument(
  "--current_year",
  type = "integer",
  required = !interactive(),
  default = 2021L,
  help = "year ID, for this step will be 2021"
)
parser$add_argument(
  "--run_id_em",
  type = "character",
  required = !interactive(),
  default = "s3-2023-04-18-14-51",
  help = "Version of EM splitting to use implied EM from"
)
parser$add_argument(
  "--run_id_splitting",
  type = "character",
  required = !interactive(),
  default = "2023_04_18-16_01_39",
  help = "Version of EM splitting to use implied EM from"
)
parser$add_argument(
  "--run_id_oprm",
  type = "integer",
  required = !interactive(),
  default = 82L,
  help = "Version of Central Comp COVID balancing process"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)


# Load Location and Age Maps ----------------------------------------------

age_map <- fread(fs::path(dir_output, "age_map", ext = "csv"))
ages_most_detailed <- c(age_map[is_aggregate == 0, age_group_id], 235)
lt_age_map <- fread(fs::path(dir_output, "lt_age_map.csv"))

loc_map <- fread(fs::path(dir_output, "loc_map", ext = "csv"))


# Set parameters ----------------------------------------------------------

# Input directories
dir_mort <- "FILEPATH"
dir_mlt <- fs::path(dir_mort, "model_life_tables")
dir_splitting <- fs::path(dir_mort, "gbd_2021_shocks_scaling")
dir_age_sex <- fs::path(dir_mort, "age_sex_split")

id_cols <- c("location_id", "year_id", "sex_id", "age_group_id")

ihme_loc <- loc_map[location_id == loc_id, ihme_loc_id]

# Load helper functions
source("functions/get_current_cc_draws.R")


# Load data ---------------------------------------------------------------

# Load population
dt_pop <- fread(
  fs::path(
    dir_output,
    "pop",
    ext = "csv"
  )
)

# load no-shock life tables
dt_lt <- mortcore::load_hdf(
  fs::path(
    dir_mlt, run_id_finalizer,
    "lt_with_hiv", "scaled", glue::glue("with_hiv_lt_{current_year}.h5")
  ),
  by_val = loc_id
) |>
  setDT()

#hotfix: change column names to match other processes
setnames(dt_lt, "sim", "draw")
setnames(dt_lt, "year", "year_id")
dt_lt <- dt_lt[sex != "both"]
dt_lt[, sex_id := ifelse(sex == "male", 1, 2)]

# Maybe hotfix: append age_group_ids to lt
dt_lt[lt_age_map, age_group_id := i.age_group_id, on = .(age = age_start)]


# load adjusted stuff
dt_adj <- fs::path(dir_output, "draws/scaled_env") |>
  fs::dir_ls(regexp = "scaled.*\\.arrow$") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(
    location_id == loc_id,
    year_id == current_year
  ) |>
  dplyr::collect() |>
  setDT()

# load unadjusted envelope
# this is the easiest way to get mx for the 95+ age group
file_ns_env <- glue::glue("with_hiv_env_{current_year}.h5")
dt_env <- setDT(mortcore::load_hdf(
  fs::path(
    dir_mlt, run_id_finalizer,
    "env_with_hiv", "scaled", file_ns_env
  ),
  by_val = loc_id
))

# HOTFIX: for now just change sim to draw
setnames(dt_env, "sim", "draw")

# Load indirect covid (for checking total mortality rate)
dt_indirect_draws <- get_current_cc_draws(
  "indirect_covid_draws_corrected.csv", 9999, run_id_oprm, loc_id, current_year, loc_map
)


# Format Data -------------------------------------------------------------

old_age_ids <- c(33, 44, 45, 148)

u5_age_ids <- c(2, 3, 388, 389, 238, 34, 5, 28)

# get u5 age_ids for age-sex processing
dt_u5_lt <- dt_lt[age_group_id %in% u5_age_ids]

# subset lts to just oldest ages (over 95)
dt_lt <- dt_lt[age_group_id %in% old_age_ids]

# Envelope
# subset env to just 95+ and calculate mx
dt_env_term <- dt_env[age_group_id == 235]
dt_env_term[dt_pop, pop := i.mean, on = id_cols]
dt_env_term[, mx := deaths / pop]

dt_adj[dt_indirect_draws, indirect_covid := i.deaths, on = c(id_cols, "draw")]

# convert harmonized envelopes into rate space
dt_adj[dt_pop, pop := i.mean, on = id_cols]
dt_adj[, mx := no_shock / pop]


# Extend into LT ages -----------------------------------------------------

dt_term <- dt_adj[age_group_id == 235]
dt_adj <- dt_adj[age_group_id != 235]

setnames(dt_term, "mx", "mx_no_shock")
dt_term[, `:=`(
  mx_covid = covid / pop,
  mx_pos_oprm = pos_oprm / pop,
  mx_indirect_covid = indirect_covid / pop
)]

# calculate ratio between m95 and the older ages
dt_lt[dt_env_term, mx_95 := i.mx, on = c("location_id", "year_id", "sex_id", "draw")]
dt_lt[, mx_ratio := mx / mx_95]

dt_term_extended <- lapply(old_age_ids, function(age_id) {
  dt_temp <- copy(dt_term)
  dt_temp[, age_group_id := age_id]
  dt_temp[dt_lt, mx_ratio := i.mx_ratio, on = c(id_cols, "draw")]
  dt_temp[, new_mx_no_shock := mx_no_shock * mx_ratio]
}) |> rbindlist()


# Save mean level of the adjustments performed in this step
dt_term_extended_mean <- demUtils::summarize_dt(
  dt_term_extended,
  id_cols = c(id_cols, "draw"),
  summarize_cols = "draw",
  value_cols = c("mx_no_shock", "mx_ratio", "new_mx_no_shock"),
  probs = NULL
)

setnames(dt_term_extended_mean, "mx_no_shock_mean", "mx_no_shock_95_mean")

saveRDS(
  dt_term_extended_mean,
  fs::path(
    dir_output,
    "summary",
    "loc_specific_lt",
    glue::glue("mx_extension_{loc_id}_{current_year}"),
    ext = "RDS"
  )
)

# format and rbind extended ages onto the rest of the lt
dt_term_extended[, mx_no_shock := new_mx_no_shock]
dt_term_extended[, c("mx_ratio", "new_mx_no_shock") := NULL]

# Check that "total mortality" qx is not >= 1, removing mortality from
# no-shock deaths if so
dt_term_extended[dt_lt, ax := i.ax, on = c(id_cols, "draw")]
dt_term_extended[, mx_total := mx_no_shock + mx_covid + mx_pos_oprm + mx_indirect_covid]
dt_term_extended[lt_age_map, age_length := i.age_end - i.age_start, on = "age_group_id"]
dt_term_extended[, qx_total := demCore::mx_ax_to_qx(mx_total, ax, age_length)]

if (isTRUE(dt_term_extended[qx_total >= 1 & is.finite(age_length), .N > 0])) {

  dt_term_extended[
    qx_total >= 1 & is.finite(age_length),
    mx_diff := mx_total - demCore::qx_ax_to_mx(0.99999, ax, age_length)
  ]

  # Where possible, remove "extra" total mx from no-shock and OPRM mx
  remove_mx_diff <- function(noshock, oprm, diff) {
    subtotal <- noshock + oprm
    adj_ratio <-  1 - diff / subtotal
    list(noshock = noshock * adj_ratio, oprm = oprm * adj_ratio)
  }

  dt_term_extended[
    !is.na(mx_diff) & (mx_no_shock + mx_pos_oprm) >= mx_diff,
    c("mx_no_shock", "mx_pos_oprm") := remove_mx_diff(mx_no_shock, mx_pos_oprm, mx_diff)
  ]

  dt_term_extended[, mx_total := mx_no_shock + mx_covid + mx_pos_oprm + mx_indirect_covid]
  dt_term_extended[, qx_total := demCore::mx_ax_to_qx(mx_total, ax, age_length)]

  # Remaining "extra" total mx leading to qx > 1 will be saved and flagged for
  # envelope adjustment
  dt_check <- dt_term_extended[qx_total >= 1 & is.finite(age_length)]

  if (nrow(dt_check) > 0) {

    dir_diag_out <- fs::path(dir_output, "diagnostics/max_adj/loc_specific")
    file_diag_out <- fs::path(paste(ihme_loc, current_year, sep = "-"), ext = "csv")

    if (length(fs::dir_ls(dir_diag_out)) > 0) {
      dir_diag_out <- fs::path(fs::path_dir(dir_diag_out), "loc_specific-v2")
    }

    readr::write_csv(dt_check, fs::path(dir_diag_out, file_diag_out))

    # Intentionally break code so problematic locations can be rerun through
    # initial envelope harmonization
    stop("Check diagnostics!")

  }

  dt_term_extended[, "mx_diff" := NULL]

}

remove_cols <- c(
  "mx_covid", "mx_pos_oprm", "mx_indirect_covid", "mx_total",
  "ax", "qx_total", "age_length"
)

setnames(dt_term_extended, "mx_no_shock", "mx")

dt_adj <- rbindlist(
  list(dt_adj, dt_term_extended[, -c(..remove_cols)]),
  use.names = TRUE
)

dt_adj[, c("covid", "pos_oprm") := NULL]


# Checks ------------------------------------------------------------------

# check that all values for covid, oprm, and covid are positive
assertable::assert_values(dt_adj, c("no_shock", "covid", "pos_oprm"), "gte", 0)

# Summarize Results -------------------------------------------------------

dt_adj_mean <- demUtils::summarize_dt(dt_adj,
                                      id_cols = c(id_cols, "draw"),
                                      summarize_cols = "draw",
                                      value_cols = c("mx"),
                                      probs = c())
setnames(dt_adj_mean, "mean", "mx")



# Save Results ------------------------------------------------------------

saveRDS(
  dt_adj,
  fs::path(
    dir_output,
    "draws",
    "loc_specific_lt",
    glue::glue("harmonized_lt_{loc_id}_{current_year}"),
    ext = "RDS"
  )
)

saveRDS(
  dt_adj_mean,
  fs::path(
    dir_output,
    "summary",
    "loc_specific_lt",
    glue::glue("harmonized_lt_{loc_id}_{current_year}"),
    ext = "RDS"
  )
)

