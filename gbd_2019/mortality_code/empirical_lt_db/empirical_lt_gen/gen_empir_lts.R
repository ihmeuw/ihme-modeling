## Title: Create empirical life tables

## Create the empirical life table database based off of input deaths from DDM processing
##     (alongside completeness factors) and GBD populations. The input deaths from DDM originally come
##      from empirical deaths process.

## Set up settings and import functions =====================================================
rm(list=ls())
user <- Sys.getenv("USER")
root <- "FILEPATH/root"

## Setup libraries
library(readr) # For check_files, import_files, etc.
library(data.table) # 1.10.4
library(assertable) # For check_files, import_files, etc.
library(haven)
library(tidyr)
library(parallel)
library(rhdf5)
library(stringr)
library(argparse)

library(mortdb, lib= "FILEPATH/r-pkg") # For check_files, import_files, etc.
library(mortcore, lib= "FILEPATH/r-pkg") # For check_files, import_files, etc.
library(ltcore, lib= "FILEPATH/r-pkg") # For check_files, import_files, etc.

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run')
parser$add_argument('--mark_best', type="character", required=TRUE,
                    help='Mark run as best')
parser$add_argument('--apply_outliers', type="character", required=TRUE,
                    help='Apply outliers')
parser$add_argument('--moving_average_weights', type="character", required=TRUE,
                    nargs="+", help='Moving moving_average_weights for smoothing')

args <- parser$parse_args()
empir_run_id <- args$version_id
mark_best <- as.logical(args$mark_best)
apply_outliers <- as.logical(args$apply_outliers)
moving_average_weights <- as.numeric(strsplit(args$moving_average_weights, ",")[[1]])

## Import process-specific functions
empir_lt_func_dir <- paste0("FILEPATH/empir_lt_func_dir")
empir_funcs <- list.files(empir_lt_func_dir)
for(func in empir_funcs) {
  source(paste0(empir_lt_func_dir, "/", func))
}

## Set working folders
main_input_folder <- paste0("FILEPATH/root/inputs")
master_folder <- paste0("FILEPATH/version-dir")
run_input_folder <- paste0(master_folder, "/inputs")
run_output_folder <- paste0(master_folder, "/outputs")

## Generate folder structure
dir.create(master_folder, showWarnings = T)
dir.create(paste0(master_folder, "/inputs"), showWarnings = T)
dir.create(paste0(master_folder, "/outputs"), showWarnings = T)
if(interactive()==F) system(paste0("chmod -R 775 ", master_folder))

# Version outlier file used for this run
file.copy(paste0(main_input_folder, "/empir_lt_outliers_gbd2017.csv"), paste0(master_folder, "/inputs/empir_lt_outliers.csv"))
file.copy("FILEPATH/compiled_outliering.csv", paste0(master_folder, "/inputs/machine_vision_compiled_outliers.csv"))
file.copy(paste0(main_input_folder, "/hmd_ax_extension.csv"), paste0(master_folder, "/inputs/hmd_ax_extension.csv"))
file.copy(paste0(main_input_folder, "/hmd_qx_extension.csv"), paste0(master_folder, "/inputs/hmd_qx_extension.csv"))
file.copy(paste0(main_input_folder, "/hmd_qx_diff_regression.csv"), paste0(master_folder, "/inputs/hmd_qx_diff_regression.csv"))
file.copy(paste0(root, "FILEPATH/mlt_db_categories.csv"), paste0(master_folder, "/inputs/mlt_db_categories.csv"))
file.copy(paste0(main_input_folder, "/hmd_qx_bounds.csv"), paste0(master_folder, "/inputs/hmd_qx_bounds.csv"))
file.copy(paste0("FILEPATH/ax_par.dta"), paste0(master_folder,"/ax_par_from_qx.dta"))

# Get parent_versions
parent_version_dt <- get_proc_lineage("life table empirical", "data", run_id = empir_run_id)
empir_parents <- list()
for(proc in c("ddm estimate", "5q0 estimate", "45q15 estimate", "population estimate")) {
  empir_parents[[proc]] <- parent_version_dt[parent_process_name == proc & exclude == 0, parent_run_id]
}
best_5q0_run <- empir_parents[["5q0 estimate"]]
best_45q15_run <- empir_parents[["45q15 estimate"]]
best_ddm_run <- empir_parents[["ddm estimate"]]

## Set unique identifying variables
empir_ids <- c("ihme_loc_id", "source_type", "sex", "year", "deaths_nid", "deaths_underlying_nid")
output_empir_ids <- c(empir_ids, "life_table_category", "outlier_type_id", "smooth_width")

## Import age map and source tables
age_map <- setDT(get_age_map(type="lifetable"))
source_type_map <- setDT(get_mort_ids("source_type"))

## Import location table and country lifetable-type designation
locations <- setDT(get_locations(level = "all", hiv_metadata = T))

## Import pop & mort, calculate mortality rates from deaths/population ==============================================

## Import HMD ax and qx extension regression results
hmd_ax_regression <- fread(paste0(run_input_folder, "/hmd_ax_extension.csv"))
hmd_qx_extension_regression <- fread(paste0(run_input_folder, "/hmd_qx_extension.csv"))
hmd_qx_diff_regression <- fread(paste0(run_input_folder, "/hmd_qx_diff_regression.csv"))

## Import DDM deaths
empir_deaths <- tryCatch({
      setDT(haven::read_dta(paste0("DDM-FILEPATH/", best_ddm_run, "/d10_45q15.dta"), encoding = "latin1"))
  }, error = function(e) {
      print("Re-trying file with readstata13")
      setDT(readstata13::read.dta13(paste0("DDM-FILEPATH/", best_ddm_run, "/d10_45q15.dta")))
  })

empir_deaths <- empir_deaths[ihme_loc_id != "IND_44849"]

vr_colnames <- colnames(empir_deaths)[grepl("vr_", colnames(empir_deaths))]
census_colnames <- colnames(empir_deaths)[grepl("c1_", colnames(empir_deaths))]
empir_deaths <- empir_deaths[, .SD, .SDcols = c(empir_ids, "comp_u5", "comp", vr_colnames, census_colnames, "max_age_gap")]

empir_deaths <- empir_deaths[year >= 1950]

empir_deaths <- empir_deaths[max_age_gap <= 5]
empir_deaths[, max_age_gap := NULL]

## Drop ages 80-84 and upwards for ZAF because of unrealistic old-age trends and time discontinuities (2005-2006)
na_cols <- c("vr_80to84", "vr_85to89", "vr_90to94", "vr_95to99", "vr_90plus", "vr_95plus",
             "vr_80plus", "vr_70plus", "vr_75plus", "vr_85plus")
empir_deaths[grepl("ZAF", ihme_loc_id), (na_cols) := NA]

## Restrict to certain source types
## CR is short for Civil Registration such as in Iran
empir_deaths[source_type == "CR", source_type := "Civil Registration"]
empir_deaths <- empir_deaths[grepl("VR", source_type) | source_type %in% c("SRS", "DSP", "Civil Registration")]


## completeness adjustment and outliering based on adult completeness ===================================================

completeness_adjustment <- function(empir_deaths, drop_exclusion_countries) {
  adult_ages <- seq(15, 95, 5)
  adult_ages <- paste0("vr_", adult_ages, "to", adult_ages + 4)

  child_ages <- c("0to0", "1to4")
  child_ages <- paste0("vr_", child_ages)

  empir_deaths <- copy(empir_deaths)

  empir_deaths[comp_u5 > 1, comp_u5 := 1]
  empir_deaths[comp > 1, comp := 1]

  comp_outliers <- empir_deaths[comp < .5 & !ihme_loc_id %in% drop_exclusion_countries, .(ihme_loc_id, year, sex, source_type)]
  comp_loc_specific <- empir_deaths[comp < .85, .(ihme_loc_id, year, sex, source_type)]

  ## For 5-9 and 10-14, proportionally weight the under-5 and adult completeness as they're partially contained within both.
  empir_deaths[, comp_5_9 := (comp_u5 * 2/3) + (comp * 1/3)]
  empir_deaths[, comp_10_14 := (comp_u5 * 1/3) + (comp * 2/3)]

  empir_deaths[, (adult_ages) := lapply(adult_ages, function(x) get(x) / comp)]
  empir_deaths[, (child_ages) := lapply(child_ages, function(x) get(x) / comp_u5)]
  empir_deaths[, vr_5to9 := vr_5to9 / comp_5_9]
  empir_deaths[, vr_10to14 := vr_10to14 / comp_10_14]
  return(list(empir_deaths, comp_outliers, comp_loc_specific))
}

## Run completeness adjustment, first defining countries to keep even if they don't meet the completeness threshold
keep_countries <- c("SAU", unique(locations[grepl("ZAF", ihme_loc_id), ihme_loc_id]))
empir_deaths <- completeness_adjustment(empir_deaths, keep_countries)
completeness_outliers <- empir_deaths[[2]]
write.csv(completeness_outliers, paste0(master_folder,"/outputs/completeness_outliers.csv"), row.names=F)
comp_loc_specific <- empir_deaths[[3]]
empir_deaths <- empir_deaths[[1]]

comp_loc_specific[, comp_under_85 := 1]
write.csv(comp_loc_specific, paste0(master_folder,"/outputs/comp_loc_specific.csv"), row.names=F)

empir_deaths[, c("comp_u5", "comp", "comp_5_9", "comp_10_14") := NULL]


## Additional_formatting ================================================================================================

## Generate age variable from wide dataset, drop NA values
empir_deaths <- melt(empir_deaths, id.vars = empir_ids)

empir_deaths <- separate(empir_deaths, "variable", into = c("stub", "age_range"))
left_hand_formula <- paste(c(empir_ids, "age_range"), collapse = "+")
empir_deaths <- dcast(empir_deaths, as.formula(paste0(left_hand_formula, "~stub")))
setnames(empir_deaths, c("c1", "vr"), c("population", "deaths"))

# drops null deaths
empir_deaths <- empir_deaths[!is.na(deaths)]

empir_deaths <- separate(empir_deaths, "age_range", into = c("age", "age_end"), sep = "to")
empir_deaths[age %like% "plus", age_end := "120"]
empir_deaths[, age := gsub("plus", "", age)]
empir_deaths[, age := as.integer(age)]
empir_deaths[, age_end := as.integer(age_end)]

assert_values(empir_deaths, "deaths", "not_na")

## Drop location/ages without corresponding population (e.g. age 95-99)
empir_deaths <- empir_deaths[!is.na(population)]

## check that pop isn't missing
assertable::assert_values(empir_deaths, "deaths", "not_na")
assertable::assert_values(empir_deaths, "population", "not_na")
empir_deaths[, mx := deaths / population]

## Vetting to see if old-age mx is low due to redistribution gone wrong
assert_values(empir_deaths[age > 80], "mx", "gte", .001, warn_only = T)

## Empirical deaths assertions
assert_values(empir_deaths[age < 80], "mx", "lte", 1, warn_only = T)
assert_values(empir_deaths[age >= 80], "mx", "lte", 1, warn_only = T)

## Drop lifetables where there is a value of mx = 0 [small numbers not useful]
## Also drop lifetables where the value of mx is less than .00001
# don't drop if zeros are above age 80, we will drop those ages, not the full LT
empir_deaths_0 <- empir_deaths[age < 80 & mx <= 0.00001]
empir_deaths_0 <- unique(empir_deaths_0[, .SD, .SDcols = c(empir_ids)])
write.csv(empir_deaths_0, paste0(master_folder, "/outputs/mx_below_one_in_100000.csv"))
empir_deaths_0[, outlier_zeroes := 1]
empir_deaths <- merge(empir_deaths, empir_deaths_0, by = empir_ids, all.x =T)
empir_deaths <- empir_deaths[is.na(outlier_zeroes) | outlier_zeroes != 1]
empir_deaths[, outlier_zeroes := NULL]

## separate out terminal age group
terminal <- empir_deaths[age_end == 120]
empir_deaths <- empir_deaths[age_end < 120]
setnames(terminal, c("age", "mx"), c("terminal_age_start", "mx_term"))

## Initialize lifetable variables and calculate lifetable metrics ===========================================================
## ax and qx extentions

empir_deaths <- empir_deaths[, .SD, .SDcols = c(empir_ids, "mx", "age")]
assertable::assert_values(empir_deaths, "mx", "not_na")

## Generate initial ax values, and drop sources without a 0-1 age group
empir_deaths <- gen_initial_ax(empir_deaths, id_vars = c(empir_ids, "age"))
assertable::assert_values(empir_deaths, "ax", "not_na")

## Apply ax extension
empir_deaths <- extend_ax(empir_deaths, hmd_ax_regression)
empir_deaths[ax < 0, ax := .01] # This happens mostly in age 90 and above, such as GRL 2004 males

## Prep to apply ax iteration
gen_age_length(empir_deaths, process_terminal = F)
empir_deaths[, qx := mx_ax_to_qx(mx, ax, age_length)]
setkeyv(empir_deaths, c(empir_ids, "age"))
qx_to_lx(empir_deaths)
empir_deaths[, dx := lx * qx]

## first remove any qx from ages older than an age with mx <= 0.00001
empir_deaths[age >= 65 & mx <= 0.00001, age_mx_low := min(age), by = empir_ids]
empir_deaths[is.na(age_mx_low), age_mx_low := 999]
empir_deaths[, min_age_mx_low := min(age_mx_low, na.rm=T), by = empir_ids]
empir_deaths <- empir_deaths[age < min_age_mx_low]

## Ax iteration
empir_deaths <- iterate_ax(empir_deaths, id_vars = c(empir_ids, "age"), n_iterations = 50)

## Apply qx extension
empir_deaths <- extend_qx(empir_deaths, hmd_qx_extension_regression, by_vars = empir_ids)

# calculate dx if missing
empir_deaths[is.na(dx), dx := lx * qx]

## Save pre-outliering diagnostic lifetables
write_csv(empir_deaths, paste0(run_output_folder, "/empir_lt_pre_outlier_diagnostic.csv"))

## Scale terminal age group to observed mx ===================================================================================

# use same method as in MLT to get ax from qx for 80+

ax_from_qx <- read_dta(paste0(master_folder,"/ax_par_from_qx.dta"))
ax_from_qx <- as.data.table(ax_from_qx)

gen_80plus_ax <- function(dt, ax_params) {
  dt <- merge(dt, ax_params, by = c("sex", "age"), all.x=T)
  dt[age >= 80, ax := par_qx * qx + par_sqx * (qx^2) + par_con]
  dt[, c("par_qx", "par_sqx", "par_con") := NULL]
  assert_values(dt[age >= 80 & age != 110], "ax", "not_na", quiet=T)
  return(dt)
}
empir_deaths <- gen_80plus_ax(empir_deaths, ax_from_qx)

# merge on terminal_age_start and mx_term
empir_deaths <- merge(empir_deaths, terminal, by = empir_ids, all.x = T, allow.cartesian = T)

# testing
test1 <- nrow(unique(empir_deaths, by = empir_ids))
print(test1)

# iterate qx to scale to observed terminal mx
scaled <- iterate_qx(empir_deaths, ax_from_qx, id_vars = c(empir_ids, "age"), n_iterations = 30)
empir_deaths <- scaled[[1]]
reset <- scaled[[2]]
ax_scale <- scaled[[3]]
write.csv(reset, paste0(run_output_folder, "/reset_after_terminal_scale.csv"), row.names = F)
write.csv(ax_scale, paste0(run_output_folder, "/ax_scaling.csv"), row.names = F)

# testing
test2 <- nrow(unique(empir_deaths, by = empir_ids))
print(test2)
assertthat::are_equal(test1, test2)

## Apply lifetable smoothing for target location/year combinations ===========================================================

smoothing_by_vars <- c("ihme_loc_id", "source_type", "sex", "age")

## Enforce global smooth here
smoothing_targets <- CJ(ihme_loc_id = unique(empir_deaths$ihme_loc_id), sex = "both")

## Run 3 separate smoothing runs
smoothed_3_year <- smooth_lts(empir_deaths, smoothing_targets, smoothing_by_vars, moving_average_weights = moving_average_weights[3:5])
smoothed_3_year[, smooth_width := 3]

smoothed_5_year <- smooth_lts(empir_deaths, smoothing_targets, smoothing_by_vars, moving_average_weights = moving_average_weights[2:6])
smoothed_5_year[, smooth_width := 5]

smoothed_7_year <- smooth_lts(empir_deaths, smoothing_targets, smoothing_by_vars, moving_average_weights = moving_average_weights)
smoothed_7_year[, smooth_width := 7]

# combine
empir_deaths[, smooth_width := 1]
all_lts <- rbindlist(list(empir_deaths, smoothed_3_year, smoothed_5_year, smoothed_7_year), use.names=T)

# write life tables to .csv
write.csv(all_lts, paste0(master_folder,"/all_lts.csv"), row.names = F)

## END
