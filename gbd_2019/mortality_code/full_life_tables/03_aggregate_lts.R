####################################################################################################
## Description: Create aggregate life tables based on lowest-level full-to-single-year results
####################################################################################################

# Import
library(data.table)
library(readr)
library(rhdf5)
library(assertable)
library(slackr)
library(parallel)
library(argparse)

library(ltcore, lib = "FILEPATH/r-pkg")
library(mortdb, lib = "FILEPATH/r-pkg")
library(mortcore, lib = "FILEPATH/r-pkg")


# Get settings ------------------------------------------------------------


# Parse arguments
parser <- ArgumentParser()
parser$add_argument('--no_shock_death_number_estimate_version', type="integer", required=TRUE,
                    help='The run id of no shock death number estimate')
parser$add_argument('--loc', type="integer", required=TRUE,
                    help='The location_id to run')
parser$add_argument('--lt_type', type="character", required=TRUE,
                    help='Life table type')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help="GBD year")
parser$add_argument('--enable_assertions_flag', type="character", required=TRUE,
                    help="Enable assertions flag")


args <- parser$parse_args()
VERSION_NO_SHOCK_DEATH_NUMBER <- args$no_shock_death_number_estimate_version
LOCATION_ID <- args$loc
LT_TYPE <- args$lt_type
GBD_YEAR <- args$gbd_year
ENABLE_ASSERTIONS <- as.logical(args$enable_assertions_flag)

MAX_AGE <- 110
INPUT_YEARS <- c(1950:GBD_YEAR)

CENTRAL_COMP_FUNCTIONS <- "FILEPATH"

MAIN_DIR <- "FILEPATH + VERSION_NO_SHOCK_DEATH_NUMBER"
INPUT_DIR <- paste0(MAIN_DIR, "/inputs")


# Load cached datasets ------------------------------------------------------------
loc_map <- fread(paste0(INPUT_DIR, "/location_hierarchy.csv"))
agg_ihme <- loc_map[location_id == LOCATION_ID, ihme_loc_id]
child_locations <- setDT(get_locations(level = "lowest", subnat_only = agg_ihme))
child_locations <- unique(child_locations$location_id)

# Pull child locations
age_groups <- fread(paste0(INPUT_DIR, "/age_groups.csv"))
age_groups_sy <- fread(paste0(INPUT_DIR, "/age_groups_sy.csv"))
age_groups_sy <- age_groups_sy[, .(age_group_id, age = age_group_years_start)]

population_sy <- fread(paste0(INPUT_DIR, "/population_sy.csv"), drop = c("lower", "upper", "ihme_loc_id"))
setnames(population_sy, "population", "mean_population")
population_sy <- population_sy[location_id %in% child_locations]

fill_old_populations <- function(target_age) {
  target_age_id <- age_groups_sy[age == target_age, age_group_id]
  pop_old <- population_sy[age_group_id == 235]
  pop_old[, age_group_id := target_age_id]
  return(pop_old)
}

old_populations <- rbindlist(lapply(95:110, fill_old_populations))

population_sy <- rbindlist(list(population_sy[!age_group_id %in% c(2,3,4,235)],
                                old_populations),
                           use.names = T)


# Build processing function ------------------------------------------------------------
process_lifetables <- function(target_sex, child_locations, hiv_shock_type) {
  import_folder <- paste0(MAIN_DIR, "/full_lt/", hiv_shock_type)
  import_filenames <- paste0("lt_full_draw_", child_locations, ".csv")

  draw_ids <- c("location_id", "year_id", "sex_id", "age_group_id", "draw")

  print(paste0("File Import Start ", Sys.time()))

  import_function <- function(filename) {
    dt <- fread(filename,
                select = c("location_id", "year_id", "sex_id", "draw", "age", "ax", "mx"))
    dt <- dt[sex_id == target_sex]
  }

  results <- import_files(import_filenames, folder = import_folder,
                          multicore = T, mc.cores = 10,
                          FUN = import_function)

  print(paste0("Files Imported ", Sys.time()))

  results <- merge(results, age_groups_sy, by = "age")
  results <- merge(results, population_sy, by = draw_ids[draw_ids != "draw"])
  results[, age := NULL]

  results[, mx := mx * mean_population]
  results[, ax := ax * mx]

  print(paste0("Aggregation Started ", Sys.time()))

  results <- agg_results(results,
                         value_vars = c("mx", "ax", "mean_population"),
                         id_vars = draw_ids,
                         tree_only = agg_ihme,
                         agg_hierarchy = T, end_agg_level = 3,
                         loc_scalars = F)

  results <- results[!location_id %in% child_locations]
  gc()

  setkey(results, location_id)

  results[, ax := ax / mx]
  results[, mx := mx / mean_population]
  results[, mean_population := NULL]

  print(paste0("Summarization Started ", Sys.time()))

  result_summary <- gen_summary_lt(results,
                                   id_vars = c(draw_ids[!draw_ids %in% c("draw", "age_group_id")], "age_group_id"),
                                   rescale_nn = F,
                                   gen_qx_aggs = F,
                                   lt_age_type = "single_year")

  result_summary <- result_summary$summary_lt

  result_summary[lt_parameter == "mx", life_table_parameter_id := 1]
  result_summary[lt_parameter == "ax", life_table_parameter_id := 2]
  result_summary[lt_parameter == "qx", life_table_parameter_id := 3]
  result_summary[lt_parameter == "lx", life_table_parameter_id := 4]
  result_summary[lt_parameter == "ex", life_table_parameter_id := 5]
  result_summary[lt_parameter == "nLx", life_table_parameter_id := 7]
  result_summary[lt_parameter == "Tx", life_table_parameter_id := 8]
  result_summary[, lt_parameter := NULL]

  results[, qx := mx_ax_to_qx(mx, ax, 1)]

  write_outputs <- function(location, target_sex) {
    if(target_sex == 1) append_flag <- F
    else append_flag <- T
    write_csv(results[location_id == location], paste0(import_folder, "/lt_full_draw_", location, "_aggregated.csv"), append = append_flag)
    write_csv(result_summary[location_id == location], paste0(import_folder, "/summary_full_", location, "_aggregated.csv"), append = append_flag)
  }

  lapply(unique(results$location_id), write_outputs, target_sex = target_sex)
  rm(results, result_summary)
  gc()
}

## Chunk into 2 sets of results because otherwise memory will overload
run_lifetables <- function(child_locations, hiv_shock_type) {
  lapply(c(1:2), process_lifetables, child_locations = child_locations, hiv_shock_type = hiv_shock_type)
}

## Call all of the functions
run_lifetables(child_locations = child_locations, hiv_shock_type = LT_TYPE)
