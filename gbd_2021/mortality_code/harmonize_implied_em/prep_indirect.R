# Meta --------------------------------------------------------------------
#'
#' Summary: Prep data for graphing. CC outputs which we add lt ages too
#'
#' Inputs:
#' 1. total COVID
#' 2. Positive OPRM
#' 5. Indirect covid shocks
#'
#' Methods:
#' 1. Load data
#' 2. convert to mx and extend to life table ages
#' 3. Summarize and save

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
  "--run_id_oprm",
  type = "integer",
  required = !interactive(),
  default = 82L,
  help = "Version of Central Comp COVID balancing process"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)

# Load Package ------------------------------------------------------------

library(data.table)
library(mortdb)
library(furrr)

plan("multisession")


# Load Location and Age Maps ----------------------------------------------

age_map <- fread(fs::path(dir_output, "age_map", ext = "csv"))
loc_map <- fread(fs::path(dir_output, "loc_map", ext = "csv"))

# get locations with VR data
vr_loc_map <- fread(fs::path(dir_output, "vr_pandemic_years_locations", ext = "csv"))
vr_loc_ids <- unique(vr_loc_map$location_id)

id_cols <- c("location_id", "year_id","sex_id", "age_group_id")

dir_cc_covid <- "FILEPATH"

# Load Data ---------------------------------------------------------------

# load indirect covid
dt_indirect <- fread(
  fs::path(
    dir_cc_covid,
    run_id_oprm,
    "pipeline_draws",
    "indirect_covid_draws_corrected.csv"
  )
)


dt_indirect_ind <- fread(fs::path(dir_cc_covid, 85, "pipeline_draws/indirect_covid_draws_corrected.csv"))
ind_locs <- loc_map[ihme_loc_id %like% "IND", location_id]
dt_indirect <- rbind(
  dt_indirect[!location_id %in% ind_locs],
  dt_indirect_ind[location_id %in% ind_locs]
)

# Load population
dt_pop <- fread(
  fs::path(
    dir_output,
    "pop",
    ext = "csv"
  )
)

# load harmonized env (to get adjusted covid and OPRM)
dt_harmonizer <- fs::path(dir_output, "summary/scaled_env") |>
  fs::dir_ls(regexp = "scaled") |>
  future_map(readRDS) |>
  rbindlist()

# Format data -------------------------------------------------------------

dt_indirect <- melt(
  data = dt_indirect,
  id.vars = c("location_id", "year_id","sex_id", "age_group_id"),
  measure.vars = patterns("draw"),
  variable.name = "draw",
  variable.factor = FALSE,
  value.name = "deaths"
)

dt_indirect_summary <- dt_indirect[, .(deaths = mean(deaths)),
                                   by = id_cols]

# calculate the age group id 5 (1-4) deaths
dt_indirect_1_4 <- dt_indirect_summary[age_group_id %in% c(34, 238)]
dt_indirect_1_4 <- dt_indirect_1_4[, .(deaths = sum(deaths),
                                       age_group_id = 5),
                                   by = .(location_id, year_id, sex_id)]

dt_indirect_summary <- rbind(dt_indirect_summary, dt_indirect_1_4)

dt_harmonizer <- melt(dt_harmonizer,
                      id.vars = id_cols,
                      measure.vars = c("no_shock_mean",
                                       "covid_mean",
                                       "pos_oprm_mean"),
                      variable.name = "measure",
                      value.name = "deaths")
dt_harmonizer <- dt_harmonizer[measure != "no_shock_mean"]

# Summarize and Calculate Rates -------------------------------------------

dt_indirect_summary <- mortcore::agg_results(data = dt_indirect_summary,
                                             id_vars = id_cols,
                                             value_vars = "deaths",
                                             gbd_year = 2021)
dt_indirect_summary[, measure := "indirect_mean"]

dt_combined <- rbind(dt_indirect_summary, dt_harmonizer)

# calculate summary EM
dt_em <- dt_combined[, .(deaths = sum(deaths)), by = id_cols]
dt_em[, measure := "EM"]

dt_combined <- rbind(dt_combined, dt_em)

# calculate rates
dt_combined[dt_pop, pop := i.mean, on = id_cols]
dt_combined[, mx := deaths / pop]


# Extend to LT ages -------------------------------------------------------

old_age_ids <- c(33, 44, 45, 148)

dt_term_extended <- lapply(old_age_ids, function(age_id) {

  dt_temp <- copy(dt_combined[age_group_id == 235])

  dt_temp[, age_group_id := age_id]

}) |>
  rbindlist()

dt_combined <- rbind(dt_combined, dt_term_extended)

# Save Results ------------------------------------------------------------

readr::write_csv(
  dt_combined,
  fs::path(
    dir_output,
    "diagnostics",
    "adjusted_cc_inputs_for_graphing.csv"
  )
)
