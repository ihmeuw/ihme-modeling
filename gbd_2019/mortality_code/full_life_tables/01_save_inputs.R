
rm(list=ls())
Sys.umask(mode = "002")
library(data.table); library(assertable); library(DBI); library(readr); library(plyr); library(argparse); library(mortdb, lib = "FILEPATH/r-pkg");

if (Sys.info()[1] == "Linux") {
  root <- "FILEPATH"
  username <- Sys.getenv("USER")
} else {
  root <- "FILEPATH"
}

parser <- ArgumentParser()
parser$add_argument('--no_shock_death_number_estimate_version', type="integer", required=TRUE,
                    help='The no shock death number estimate version for this run')
parser$add_argument('--population_estimate_version', type="integer", required=TRUE,
                    help='Population estimate version')
parser$add_argument('--population_single_year_estimate_version', type="integer", required=TRUE,
                    help='Population estimate single year version')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD Year')

args <- parser$parse_args()
version_id <- args$no_shock_death_number_estimate_version
population_estimate_version <- args$population_estimate_version
population_single_year_estimate_version <- args$population_single_year_estimate_version
gbd_year <- args$gbd_year

input_dir <- "FILEPATH + version_id"

create_draws <- function(location_id) {
  new_seed <- location_id * 100 + 121 # Just to avoid any unintended correlation with other seeds that rely on location_id
  set.seed(new_seed)
  data <- data.table(expand.grid(location_id = location_id, old_draw = c(0:999)))
  data[,draw_sort := rnorm(1000)]
  data <- data[order(location_id, draw_sort), ]
  data[, new_draw := seq_along(draw_sort) - 1, by = location_id] # Creates a new variable with the ordering based on the values of draw_sort
  data[, draw_sort := NULL]
}

create_single_abridged_age_map <- function() {
  abridged_ages <- c(0, 1, seq(5, 110, 5))
  ages <- data.table("age" = 0:110)
  ages[, abridged_age := cut(age, breaks = c(abridged_ages, Inf), labels = abridged_ages, right = F)]
  ages[, abridged_age := as.integer(as.character(abridged_age))]
  return(ages)
}

# Pull location hierarchy
location_hierarchy <- get_locations(level = "all", hiv_metadata = T, gbd_year = gbd_year)
fwrite(location_hierarchy, paste0(input_dir, "/location_hierarchy.csv"))

# Create draw map
draw_map <- rbindlist(lapply(unique(location_hierarchy$location_id), create_draws))
fwrite(draw_map, paste0(input_dir, "/draw_map.csv"))

# Save lifetable age groups
age_groups <- get_age_map(type = "lifetable")
age_groups <- age_groups[, list(age_group_id, age = as.integer(age_group_name_short), age_group_years_end, age_group_years_start)]
age_groups[, age_length := age_group_years_end - age_group_years_start]
age_groups[, c("age_group_years_end", "age_group_years_start") := NULL]
fwrite(age_groups, paste0(input_dir, "/age_groups.csv"))

# Save single year age groups
age_groups_sy <- setDT(get_age_map(type = "single_year"))
assertthat::assert_that(nrow(age_groups_sy[duplicated(age_groups_sy[, .(age_group_years_start, age_group_years_end)])]) == 0)
fwrite(age_groups_sy, paste0(input_dir, "/age_groups_sy.csv"))

# Create single abridged age map
single_abridged_age_map <- create_single_abridged_age_map()
fwrite(single_abridged_age_map, paste0(input_dir, "/single_abridged_age_map.csv"))

# Save population
population <- get_mort_outputs("population", "estimate", run_id = population_estimate_version, gbd_year = gbd_year)
population[, c("upload_population_estimate_id", "run_id") := NULL]
assert_values(population, 'mean', test = "gt", test_val = 0)
setnames(population, "mean", "mean_population")
fwrite(population, paste0(input_dir, "/population.csv"))

# Save single year population
population_sy <- get_mort_outputs("population single year", "estimate", run_id = population_single_year_estimate_version, gbd_year = gbd_year)
population_sy[, c("upload_population_single_year_estimate_id", "run_id") := NULL]
assert_values(population_sy, 'population', test = "gt", test_val = 0)
fwrite(population_sy, paste0(input_dir, "/population_sy.csv"))

# DONE
