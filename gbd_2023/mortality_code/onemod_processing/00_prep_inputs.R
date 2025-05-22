# Meta --------------------------------------------------------------------

# Downloads and preps database inputs to be used later in the process


# Load packages -----------------------------------------------------------

library(data.table)


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

args <- parser$parse_args()


# Load Config and Set up --------------------------------------------------

cfg <- config::get(file = fs::path(args$dir_output, "config.yml"))

map_age_canon <- fread(fs::path(args$dir_output, "age_map_canonical.csv"))


# Save Pop ----------------------------------------------------------------

dt_pop <- demInternal::get_dem_outputs(
  process_name = "population estimate",
  run_id = cfg$run_ids_parents$pop
)

dt_pop |>
  as.data.frame() |>
  arrow::write_parquet(fs::path(args$dir_output, "population", ext = "parquet"))


# Save Single-year pop ----------------------------------------------------

dt_pop_sy <- demInternal::get_dem_outputs(
  process_name = "population single year estimate",
  run_id = cfg$run_ids_parents$pop_sy
)

dt_pop_sy |>
  as.data.frame() |>
  arrow::write_parquet(fs::path(args$dir_output, "population_sy", ext = "parquet"))


# Save Canonical pop ------------------------------------------------------

dt_pop_canon <- rbind(
  dt_pop[age_group_id %in% map_age_canon$age_group_id],
  dt_pop_sy[age_group_id %in% map_age_canon$age_group_id],
  fill = TRUE
)
dt_pop_canon[, grep("estimate", names(dt_pop_canon)) := NULL]

dt_pop_canon |>
  as.data.frame() |>
  arrow::write_parquet(fs::path(args$dir_output, "population_canon", ext = "parquet"))
