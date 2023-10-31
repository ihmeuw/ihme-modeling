# Save .csv for ASLY where EM is 1.5X no-shock-mx or more

# Read Arguments ----------------------------------------------------------

parser <- argparse::ArgumentParser()

parser$add_argument(
  "--dir_output",
  type = "character",
  required = !interactive(),
  default = "FILEPATH",
  help = "Output directory for em adjustment"
)
parser$add_argument(
  "--run_id_balancing",
  type = "integer",
  required = !interactive(),
  default = 82L,
  help = "Output directory for em adjustment"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)

# Load packages -----------------------------------------------------------

library(data.table)
library(furrr)


# Set parameters ----------------------------------------------------------

plan("multisession")

analysis_years <- 2020:2021

dir_diagnostics <- fs::path(dir_output, "diagnostics")
file_name_out <- "high_em_to_no_shock_ratio"


# Load maps ---------------------------------------------------------------

map_locs <- demInternal::get_locations(gbd_year = 2020)
map_ages <- fread(fs::path(dir_output, "age_map", ext = "csv"))

# Data subsets ------------------------------------------------------------

cols_final <- c(
  "ihme_loc_id", "location_name",
  "year_id",
  "sex_name",
  "age_group_name", "age_start",
  "deaths"
)

subset_locs <- map_locs[level >= 3, location_id]


# Load data ---------------------------------------------------------------

dt_harmonizer <-
  fs::path(dir_output, "summary/loc_specific_lt") |>
  fs::dir_ls(regexp = "harmonized") |>
  future_map(readRDS) |>
  rbindlist()


dt_indirect_covid <- fread(
  fs::path(
    dir_diagnostics,
    "adjusted_cc_inputs_for_graphing.csv"
  )
)


# Prep data ---------------------------------------------------------------

# calculate em-specific mx
dt_indirect_covid <- dt_indirect_covid[measure == "EM"]

# merge
dt_harmonizer[dt_indirect_covid, em_mx := i.mx, on = .(location_id, year_id, sex_id, age_group_id)]

setnames(
  dt_harmonizer,
  c("mx"),
  c("no_shock_mx")
)

# calculate EM / no-shock ratio
dt_harmonizer[, em_no_shock_ratio := em_mx / no_shock_mx]

# calculate em + no_shock
dt_harmonizer[, with_em_mx := em_mx + no_shock_mx]

# Format ------------------------------------------------------------------

dt_harmonizer <- demInternal::ids_names(dt_harmonizer, extra_output_cols = c("ihme_loc_id", "location_name", "age_group_name"))

# save data ---------------------------------------------------------------


readr::write_csv(
  dt_harmonizer[em_no_shock_ratio > 1.5],
  fs::path(
    dir_diagnostics,
    file_name_out,
    ext = ".csv"
  )
)

# also save places where no-shock mx is higher than 1
readr::write_csv(
  dt_harmonizer[with_em_mx > 1],
  fs::path(
    dir_diagnostics,
    "with_em_mx_greater_than_1",
    ext = ".csv"
  )
)

# also save places where no-shock mx is higher than 1
readr::write_csv(
  dt_harmonizer[no_shock_mx < 0.0001],
  fs::path(
    dir_diagnostics,
    "no_shock_mx_less_than_0.0001",
    ext = ".csv"
  )
)
