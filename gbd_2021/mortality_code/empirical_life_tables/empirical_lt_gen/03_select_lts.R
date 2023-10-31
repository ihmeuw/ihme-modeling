## Title : Select ELTs
## Authors: 
## Date : June 2019
## Purpose : Select empirical life tables to include in MLT database

## Now, apply cascading selection process
## Keep the least-smoothed lifetable that fits within each classification cutoff
## But keep duplicates if one fits for universal and the other fits for location-specific
## Even if the universal one is high-quality, we want the location-specific to still exist
## since it's less smoothed and can be prioritized for the country

## set-up ==================================================================================

rm(list=ls())
user <- Sys.getenv("USER")

## Setup libraries
library(pacman)
p_load(readr, data.table, assertable, haven, tidyr, parallel, rhdf5, stringr, argparse)
library(mortdb)
library(mortcore)
library(ltcore)

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run')
parser$add_argument('--mark_best', type="character", required=TRUE,
                    help='Mark run as best')
parser$add_argument('--run_mv', type="character", required=TRUE,
                    help='Whether we predicted from machine vision on this run')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD year')
parser$add_argument("--covid_years", type="integer", nargs="+", required = TRUE,
                    help = "Years where VR data should be treated as including COVID")
parser$add_argument('--code_dir', type='character', required=TRUE,
                    help = "Directory where ELT code is cloned")

args <- parser$parse_args()
empir_run_id <- args$version_id
mark_best <- as.logical(args$mark_best)
run_mv <- as.logical(args$run_mv)
gbd_year <- args$gbd_year
code_dir <- args$code_dir
covid_years <- args$covid_years

stopifnot(covid_years == 2020:2021)

## Import process-specific functions
empir_lt_func_dir <- paste0(code_dir, "/empirical_lt_gen/empir_gen_funcs/R")
empir_funcs <- list.files(empir_lt_func_dir)
for(func in empir_funcs) {
  suppressWarnings(source(paste0(empir_lt_func_dir, "/", func)))
}
source(paste0(code_dir, "/empirical_lt_gen/elt_functions.R"))

## Set working folder
master_folder     <- paste0("FILEPATH", empir_run_id)
run_input_folder  <- paste0(master_folder, "/inputs")
run_output_folder <- paste0(master_folder, "/outputs")

## parent versions
parent_version_dt <- get_proc_lineage("life table empirical", "data", run_id = empir_run_id)
empir_parents <- list()
for(proc in c("5q0 estimate", "45q15 estimate", "population estimate")) {
  empir_parents[[proc]] <- parent_version_dt[parent_process_name == proc & exclude == 0, parent_run_id]
}

## get empirical deaths from previous script
loc_files <- list.files(paste0(run_output_folder, "/loc_specific"), pattern = "all_lts", full.names = T)
empir_deaths <- do.call(rbind, lapply(loc_files, fread))
readr::write_csv(empir_deaths, paste0(run_output_folder, "/compiled_02_lts.csv"))

## other inputs
adult_hiv_covariate    <- fread("hiv_covariate.csv")
adult_hiv_covariate[, year := floor(year)]
locations              <- setDT(get_locations(level = "all", hiv_metadata = T))
group_1_countries      <- unique(locations[group %in% c("1A", "1B"), ihme_loc_id])
completeness_outliers  <- fread(paste0(run_output_folder, "/completeness_outliers.csv"))
comp_loc_specific      <- fread(paste0(run_output_folder, "/comp_loc_specific.csv"))
pop_age_pools          <- fread(paste0(run_input_folder, "/pop_age_pools.csv"))

## Import country total population counts
country_pop <- get_mort_outputs("population", "estimate",
                                run_id = empir_parents[["population estimate"]],
                                age_group_id = 22,
                                sex_id = 3,
                                gbd_year = gbd_year)
country_pop <- country_pop[, .(ihme_loc_id, year = year_id, mean_pop = mean)]

## Set unique identifying variables
empir_ids <- c("ihme_loc_id", "source_type", "sex", "year")
output_empir_ids <- c(empir_ids, "deaths_nid", "deaths_underlying_nid", "life_table_category",
                      "outlier_type_id", "smooth_width")

## Import age map and source tables
age_map         <- get_age_map(gbd_year = gbd_year, type = "lifetable")
source_type_map <- get_mort_ids("source_type")

# read in and prep machine vision decisions ======================================================================

if(run_mv == T){
  mv_decisions <- list.files(paste0(master_folder,"/machine_vision"), pattern = "elt_predictions", full.names = T)
  mv <- data.table()
  for(file in mv_decisions){
    print(file)
    temp <- fread(file)
    temp <- parse_file_path(temp)
    mv <- rbind(mv, temp, fill=T)
  }
  mv <- mv[!is.na(predicted)]
  mv <- mv[,.(predicted, ihme_loc_id, year_id, sex, source_name, smooth_width)]
  setnames(mv, "source_name", "source_type")
  
  # categorize based on threshold 60% certainty
  mv[predicted > 0.6, outlier_type_id := 13]
  mv[predicted < 0.6, outlier_type_id := 0]
  mv[is.na(outlier), outlier := 0]
  
  # clean up
  mv[, predicted := NULL]
  mv <- mv[outlier_type_id == 13]
  mv[, life_table_category_id := 5]

  outlier_manual <- copy(mv)
}

## Perform outliering =============================================================================================

# If not running machine vision, use manual outliers
if(run_mv == F){
  outlier_manual_all <- get_mort_outputs("life table empirical", "data",
                                     run_id = 500,
                                     life_table_parameter_ids = c(3),
                                     demographic_metadata = T)
  outlier_manual_all <- outlier_manual_all[, c("year_id", "ihme_loc_id", "sex", "source_name", "smooth_width",
                                       "life_table_category_id", "outlier_type_id")]
  setnames(outlier_manual_all, c("year_id", "source_name"), c("year", "source_type"))
  outlier_manual_all[, sex := tolower(sex)]
  outlier_manual_all <- unique(outlier_manual_all)
  
  # manual location specific
  loc_spec_manual <- outlier_manual_all[life_table_category_id == 2]
  loc_spec_manual[, manual_loc_specific := 1]
  loc_spec_manual <- loc_spec_manual[, .(ihme_loc_id, sex, year, source_type, smooth_width, manual_loc_specific)]
  
  # get all outliers, and for now label as manual outliers
  outlier_manual <- outlier_manual_all[life_table_category_id == 5]
  outlier_manual[, manual_outlier := 1]
  outlier_manual <- outlier_manual[, .(ihme_loc_id, sex, year, source_type, smooth_width, manual_outlier)]
}

# Get outliers from 5q0 and 45q15 to be passed to cascade_select_lts function
child_adult_outliers <- outlier_data(run_id_5q0_estimate = empir_parents[["5q0 estimate"]],
                                     run_id_45q15_estimate = empir_parents[["45q15 estimate"]],
                                     gbd_year = gbd_year)

# Apply all outliering in cascading fashion
empir_deaths <- cascade_select_lts(empir_lt = empir_deaths,
                                   id_vars = empir_ids,
                                   hiv_cdr = adult_hiv_covariate,
                                   group_1_list = group_1_countries,
                                   pop_age_pools = pop_age_pools,
                                   country_pop = country_pop,
                                   loc_spec_comp = comp_loc_specific,
                                   loc_spec_manual = loc_spec_manual,
                                   outliers_5q0_45q15 = child_adult_outliers,
                                   outliers_comp = completeness_outliers,
                                   outliers_manual = outlier_manual)

# Clean up
empir_deaths <- empir_deaths[, .SD, .SDcols = c(output_empir_ids, "age", "qx", "ax")]

## Generate summary metrics ==================================================================================

# Go from qx to lx
setkeyv(empir_deaths, c(output_empir_ids, "age"))
qx_to_lx(empir_deaths, assert_na = F)

## Outlier if qx is over 1 or under 0
over_1 <- empir_deaths[(qx > 1 | lx < 0 | qx < 0 | is.na(qx)) & outlier_type_id != 1]
over_1 <- unique(over_1[, .SD, .SDcols = output_empir_ids])
over_1[, drop := 1]
empir_deaths <- merge(empir_deaths, over_1, by = output_empir_ids, all.x = T)
empir_deaths[drop == 1, outlier_type_id := 4] # TODO: make new outlier_type for this
empir_deaths[, drop := NULL]

## Generate 5q0, 45q15, and other summary metrics
empir_deaths[, px := 1 - qx]
sq5 <- empir_deaths[age <= 1, list(mean = 1 - prod(px)), by = output_empir_ids]
sq5[, life_table_parameter_id := 3]
sq5[, age_group_id := 1]

sq45 <- empir_deaths[age >= 15 & age <= 55, list(mean = 1 - prod(px)), by = output_empir_ids]
sq45[, life_table_parameter_id := 3]
sq45[, age_group_id := 199]

## Format to life table parameter IDs etc.
empir_deaths <- empir_deaths[, .SD, .SDcols = c(output_empir_ids, "age", "qx", "lx", "ax")]
empir_deaths <- merge(empir_deaths, age_map[, .(age_group_id, age_group_years_start)], by.x = "age", by.y = "age_group_years_start")
empir_deaths[, age := NULL]

empir_deaths <- melt(empir_deaths, id.vars = c(output_empir_ids, "age_group_id"))
empir_deaths[variable == "qx", life_table_parameter_id := 3]
empir_deaths[variable == "lx", life_table_parameter_id := 4]
empir_deaths[variable == "ax", life_table_parameter_id := 2]
empir_deaths[, variable := NULL]
setnames(empir_deaths, "value", "mean")

## Combine
empir_formatted <- rbindlist(list(empir_deaths, sq5, sq45), use.names = T)

## Variable conversions
setnames(empir_formatted, "year", "year_id")
empir_formatted[sex == "male", sex_id := 1]
empir_formatted[sex == "female", sex_id := 2]

## Source type mapping
empir_formatted <- merge(empir_formatted, source_type_map[, .(source_type_id, type_short)], by.x = "source_type", by.y = "type_short", all.x = T)

## Merge on locations, drop any non-standard locations such as Old AP, USSR, Yugoslavia, etc.
empir_formatted <- merge(empir_formatted, locations[, .(location_id, ihme_loc_id)], by = "ihme_loc_id")
assert_values(empir_formatted, "location_id", "not_na", quiet = T)

## Partition countries into universal vs. country-specific lifetable database
empir_formatted[life_table_category == "universal", life_table_category_id := 1]
empir_formatted[life_table_category == "location specific", life_table_category_id := 2]
empir_formatted[life_table_category == "outlier", life_table_category_id := 5]
empir_formatted[life_table_category == "location year specific", life_table_category_id := 6]
assert_values(empir_formatted, "life_table_category_id", "not_na", quiet = T)

## Create new version, output file, and upload ==============================================================

## Convert NaN values to NA to allow them to be uploaded appropriately
empir_formatted[is.nan(mean), mean := NA]

## Output final dataset
empir_formatted <- empir_formatted[, .(year_id, location_id, sex_id, age_group_id, life_table_parameter_id,
                                       nid = deaths_nid, underlying_nid = deaths_underlying_nid,
                                       source_type_id, life_table_category_id,
                                       smooth_width, outlier_type_id, mean)]

assert_values(empir_formatted, 
              c("year_id", "location_id", "sex_id", "age_group_id", "life_table_parameter_id",
                "source_type_id", "life_table_category_id", "smooth_width", "outlier_type_id"),
              "not_na")

testing_dups <- empir_formatted[outlier_type_id==1]
id_vars <- c("year_id", "location_id", "sex_id", "life_table_category_id", "source_type_id", "age_group_id", "life_table_parameter_id")
if(nrow(testing_dups[duplicated(testing_dups, by = c(id_vars))]) > 0) stop("Duplicates found")

out_filepath <- paste0(run_output_folder, "/empir_lt.csv")
write_csv(empir_formatted, out_filepath)
upload_results(out_filepath, "life table empirical", "data", run_id = empir_run_id)

if(mark_best == T) update_status("life table empirical", "data", run_id = empir_run_id, new_status = "best")

## END
