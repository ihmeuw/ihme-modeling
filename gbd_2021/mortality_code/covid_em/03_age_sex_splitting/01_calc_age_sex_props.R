# Meta --------------------------------------------------------------------
#'
#' Summary: Calculates age-sex distribution of excess for a given location year
#'
#' Inputs: (all inputs are draw level)
#' 1. No-shock envelope from a run with no 2020-21 data
#' 2. No-shock envelope from a run with 2020-21 data (to capture pandemic)
#'
#' Methods:
#' 1. If we have VR for this location-year, use the implied excess
#'    (difference between the 2 envelopes) as adjusted EM
#' 2. Calculate the proportion of excess and excess mortality rate
#' 3. Save results


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
  "--version_finalizer_wshock",
  type = "integer",
  required = !interactive(),
  default = 543L,
  help = "With-shock death number estimate run id of finalizer run for with-2020 VR no-shock envelope"
)
parser$add_argument(
  "--version_finalizer_nshock",
  type = "integer",
  required = !interactive(),
  default = 544L,
  help = "With-shock death number estimate run id of finalizer run for no-2020 VR no-shock envelope"
)
parser$add_argument(
  "--loc_id",
  type = "integer",
  required = !interactive(),
  default = 109L,
  help = "Location ID"
)
parser$add_argument(
  "--current_year",
  type = "integer",
  required = !interactive(),
  default = 2020L,
  help = "year ID"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)

# Finalizer versions (versioned by with-shock death number run ID) of no-shock
# envelopes to get no 2020 VR (ns) and with 2020 VR (ws) results
version_finalizer <- list(
  ns = version_finalizer_nshock,
  ws = version_finalizer_wshock
)

age_map <- fread(fs::path(dir_output, "age_map", ext = "csv"))
ages_most_detailed <- c(age_map[is_aggregate == 0, age_group_id], 235)

loc_map <- fread(fs::path(dir_output, "loc_map", ext = "csv"))


# Set parameters ----------------------------------------------------------

# Input directories
dir_finalizer <- "FILEPATH"

file_em_draws_base <- glue::glue("implied_excess_{loc_id}_{current_year}")

id_cols <- c("location_id", "year_id", "sex_id", "age_group_id")
em_draw_ids <- c("location_id", "year_id", "draw")


# Load data ---------------------------------------------------------------

dt_env <- lapply(version_finalizer, function(run) {

  dt_temp <- setDT(mortcore::load_hdf(
    fs::path(dir_finalizer, run, "env_no_shock", glue::glue("combined_env_aggregated_{current_year}.h5")),
    by_val = loc_id
  ))

  # subset to most detailed ages and sexes
  dt_temp <- dt_temp[age_group_id %in% ages_most_detailed &
                       sex_id != 3]
})

# Load population
dt_pop <- fread(
  fs::path(
    dir_output,
    "pop",
    ext = "csv"
  )
)


# Prep data ---------------------------------------------------------------

setnames(dt_env$ns, "deaths", "deaths_ns")
setnames(dt_env$ws, "deaths", "deaths_ws")


# Calculate implied shocks ------------------------------------------------

dt_excess_implied <- merge(
  dt_env$ws,
  dt_env$ns,
  by = c(id_cols, "draw"),
  all = TRUE
)

dt_excess_implied[, excess_implied := deaths_ws - deaths_ns]

# Check that join didn't drop anything
stopifnot(all(!is.na(dt_excess_implied)))


# Calculate age-sex distribution ------------------------------------------

# calculate total implied excess (per location-year-draw)
# and then the proportion of total excess for wach age-sex combination
dt_excess_implied[, total_excess_implied := sum(excess_implied),
                  by = em_draw_ids]

dt_excess_implied[, prop := excess_implied / total_excess_implied]


# Calculate Excess Mortality Rate -----------------------------------------

dt_excess_implied[dt_pop, pop := i.mean, on = id_cols]

dt_excess_implied[, em_rate := excess_implied / pop]

# Summarize results -------------------------------------------------------

dt_excess_implied_summary <- demUtils::summarize_dt(
  dt_excess_implied,
  id_cols = c(id_cols, "draw"),
  summarize_cols = "draw",
  value_cols = setdiff(names(dt_excess_implied), c(id_cols, "draw")),
  summary_fun = c("mean"),
  probs = c()
)


# Save results -------------------------------------------------------------

saveRDS(
  dt_excess_implied,
  fs::path(dir_output, "draws", "01_implied_em",
           file_em_draws_base, ext = "RDS")
)

saveRDS(
  dt_excess_implied_summary,
  fs::path(dir_output, "summary", "01_implied_em",
           file_em_draws_base, ext = "RDS")
)

