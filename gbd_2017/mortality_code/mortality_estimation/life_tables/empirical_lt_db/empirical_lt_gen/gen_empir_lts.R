
## Set run comment
new_run <- T
mark_best <- T
run_comment <- "COMMENT"
apply_outliers <- T

## Setup libraries
library(readr)
library(data.table) 
library(assertable)
library(haven)
library(tidyr)
library(parallel)
library(rhdf5)

library(mortdb)
library(mortcore) 
library(ltcore)

## Source central comp functions
source("get_population.R"))

## Import process-specific functions
empir_lt_func_dir <- paste0("empir_gen_funcs/R")
empir_funcs <- list.files(empir_lt_func_dir)
for(func in empir_funcs) {
  source(paste0(empir_lt_func_dir, "/", func))
}

## Set working folders
main_input_folder <- paste0("empir_lt_db/inputs")

## Generate new run and versioning (if required)
if(new_run == T) {
  empir_run_id <- gen_new_version("life table empirical", "data", comment = run_comment)
  empir_parents <- get_best_versions(model_names = c("ddm estimate", "5q0 estimate", "45q15 estimate", "population estimate"))
}

master_folder <- FILEPATH
run_input_folder <- paste0(master_folder, "/inputs")
run_output_folder <- paste0(master_folder, "/outputs")

## Set unique identifying variables
empir_ids <- c("ihme_loc_id", "source_type", "sex", "year", "deaths_nid", "deaths_underlying_nid")

###############################################################################################################
## Import run metadata, lifetable data, and metadata maps
## Grab best input versions
best_5q0_run <- empir_parents[["5q0 estimate"]]
best_45q15_run <- empir_parents[["45q15 estimate"]]
best_ddm_run <- empir_parents[["ddm estimate"]]

## Import age map and source tables
age_map <- setDT(get_age_map(type="lifetable"))
source_type_map <- setDT(get_mort_ids("source_type"))

## Import location table and country lifetable-type designation
locations <- setDT(get_locations(level = "all", hiv_metadata = T))
group_1_countries <- unique(locations[group %in% c("1A", "1B"), ihme_loc_id])
group_2_countries <- unique(locations[group %in% c("2A", "2B", "2C"), ihme_loc_id])

lt_country_designations <- fread(paste0(run_input_folder, "/mlt_db_categories.csv"))
universal_locations <- unique(lt_country_designations[loc_indic == "universal", location_id])
location_specific_locations <- unique(lt_country_designations[loc_indic %in% c("location-specific", "ZAF") | grepl("ZAF", loc_indic), location_id]) # ZAF uses the location-specific DB
usa_locations <- unique(lt_country_designations[loc_indic == "USA", location_id])

## Import HIV crude death rate form the best version of 45q15 estimate
adult_hiv_covariate <- fread(paste0("data/hiv_covariate.csv"))
adult_hiv_covariate[, year := floor(year)]

## Import HMD old-age qx bounds
hmd_bounds <- fread(paste0(main_input_folder, "/hmd_qx_bounds.csv"))

## Import population age pooling metadata
pop_age_pools <- readRDS(paste0(POPULATION_FILEPATH,"/best_versions.RDS"))
pop_age_pools[, pop_pool_age := as.integer(gsub("v", "", model_version))]
pop_age_pools <- pop_age_pools[, .(ihme_loc_id, pop_pool_age)]

## Import country total population counts
country_pop <- get_mort_outputs("population", "estimate", run_id = "best", age_group_id = 22, sex_id = 3)
country_pop <- country_pop[, .(ihme_loc_id, year = year_id, mean_pop = mean)]


###############################################################################################################
## Import population and mortality, calculate mortality rates from deaths/population

## Import HMD ax and qx extension regression results
hmd_ax_regression <- fread(paste0(run_input_folder, "/hmd_ax_extension.csv"))
hmd_qx_extension_regression <- fread(paste0(run_input_folder, "/hmd_qx_extension.csv"))
hmd_qx_diff_regression <- fread(paste0(run_input_folder, "/hmd_qx_diff_regression.csv"))

## Import DDM deaths
empir_deaths <- tryCatch({
      setDT(haven::read_dta(paste0(DDM_FILEPATH,"/d10_45q15.dta"), encoding = "latin1"))
  }, error = function(e) {
      print("Re-trying file with readstata13")
      setDT(readstata13::read.dta13(paste0(DDM_FILEPATH,"/d10_45q15.dta")))
  })

vr_colnames <- colnames(empir_deaths)[grepl("vr_", colnames(empir_deaths))]
census_colnames <- colnames(empir_deaths)[grepl("c1_", colnames(empir_deaths))]
empir_deaths <- empir_deaths[, .SD, .SDcols = c(empir_ids, "comp_u5", "comp", vr_colnames, census_colnames, "max_age_gap")]

empir_deaths <- empir_deaths[year >= 1950]
empir_deaths <- empir_deaths[max_age_gap <= 5]
empir_deaths[, max_age_gap := NULL]

na_cols <- c("vr_80to84", "vr_85to89", "vr_90to94", "vr_95to99")
empir_deaths[grepl("ZAF", ihme_loc_id), (na_cols) := NA]

empir_deaths[source_type == "CR", source_type := "Civil Registration"]
empir_deaths <- empir_deaths[grepl("VR", source_type) | source_type %in% c("SRS", "DSP", "Civil Registration")] 


###############################################################################################################
## Perform completeness adjustment and outliering based on adult completeness
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

  ## For 5-9 and 10-14, proportionally weight the under-5 and adult completeness
  empir_deaths[, comp_5_9 := (comp_u5 * 2/3) + (comp * 1/3)]
  empir_deaths[, comp_10_14 := (comp_u5 * 1/3) + (comp * 2/3)]

  empir_deaths[, (adult_ages) := lapply(adult_ages, function(x) get(x) / comp)]
  empir_deaths[, (child_ages) := lapply(child_ages, function(x) get(x) / comp_u5)]
  empir_deaths[, vr_5to9 := vr_5to9 / comp_5_9]
  empir_deaths[, vr_10to14 := vr_10to14 / comp_10_14]
  return(list(empir_deaths, comp_outliers, comp_loc_specific))
}

## Run completeness adjustment
keep_countries <- c("SAU", unique(locations[grepl("ZAF", ihme_loc_id), ihme_loc_id])) 
empir_deaths <- completeness_adjustment(empir_deaths, keep_countries)
completeness_outliers <- empir_deaths[[2]]
comp_loc_specific <- empir_deaths[[3]]
empir_deaths <- empir_deaths[[1]]

comp_loc_specific[, comp_under_85 := 1]

empir_deaths[, c("comp_u5", "comp", "comp_5_9", "comp_10_14") := NULL]

###############################################################################################################
## Generate age variable from wide dataset, drop NA values
empir_deaths <- melt(empir_deaths, id.vars = empir_ids)

empir_deaths <- separate(empir_deaths, "variable", into = c("stub", "age_range"))
left_hand_formula <- paste(c(empir_ids, "age_range"), collapse = "+")
empir_deaths <- dcast(empir_deaths, as.formula(paste0(left_hand_formula, "~stub")))
setnames(empir_deaths, c("c1", "vr"), c("population", "deaths"))

empir_deaths <- empir_deaths[!is.na(deaths)]

empir_deaths <- separate(empir_deaths, "age_range", into = c("age", "age_end"), sep = "to")
empir_deaths[, age := gsub("plus", "", age)]
empir_deaths[, age := as.integer(age)]
empir_deaths[, c("age_end") := NULL]

assert_values(empir_deaths, "deaths", "not_na")

## Drop location/ages without corresponding population (e.g. age 95-99)
empir_deaths <- empir_deaths[age < 95]

empir_deaths[, mx := deaths / population]

## Drop lifetables where there is a value of mx = 0 [small numbers not useful]
empir_deaths_0 <- empir_deaths[mx <= 0.00001]
empir_deaths_0 <- unique(empir_deaths_0[, .SD, .SDcols = c(empir_ids)])
empir_deaths_0[, outlier_zeroes := 1]
empir_deaths <- merge(empir_deaths, empir_deaths_0, by = empir_ids, all.x =T)
empir_deaths <- empir_deaths[is.na(outlier_zeroes) | outlier_zeroes != 1]
empir_deaths[, outlier_zeroes := NULL]


###############################################################################################################
## Initialize lifetable variables and calculate lifetable metrics
## Generate initial ax values
empir_deaths <- empir_deaths[, .SD, .SDcols = c(empir_ids, "mx", "age")]

## Generate initial ax values, and drop sources without a 0-1 age group
empir_deaths <- gen_initial_ax(empir_deaths, id_vars = c(empir_ids, "age"))

## Apply ax extension
empir_deaths <- extend_ax(empir_deaths, hmd_ax_regression)
empir_deaths[ax < 0, ax := .01] # This happens mostly in age 90 and above, such as GRL 2004 males

## Apply ax iteration
gen_age_length(empir_deaths, process_terminal = F)
empir_deaths[, qx := mx_ax_to_qx(mx, ax, age_length)]
setkeyv(empir_deaths, c(empir_ids, "age"))
qx_to_lx(empir_deaths)
empir_deaths[, dx := lx * qx] 

empir_deaths <- iterate_ax(empir_deaths, id_vars = c(empir_ids, "age"), n_iterations = 50)

## Apply qx extension
empir_deaths <- extend_qx(empir_deaths, hmd_qx_extension_regression, by_vars = empir_ids)


###############################################################################################################
## Save pre-outliering diagnostic lifetables
write_csv(empir_deaths, paste0(run_output_folder, "/empir_lt_pre_outlier_diagnostic.csv"))

###############################################################################################################
## Perform outliering
  ## Outlier empirical deaths based off of 5q0/45q15 data outliers, as well as empirical LT-specific drops
  outlier_sheet <- fread(paste0(run_input_folder, "/empir_lt_outliers.csv"))

  outlier_sheet[is.na(year_start) & !is.na(year_end), year_start := 1900]
  outlier_sheet[!is.na(year_start) & is.na(year_end), year_end := 2030]

  empir_deaths <- outlier_data(empir_deaths, outlier_sheet, completeness_outliers, 
                               run_id_5q0_estimate = best_5q0_run, run_id_45q15_estimate = best_45q15_run)


###############################################################################################################
## Apply lifetable smoothing for target location/year combinations
smoothing_by_vars <- c("ihme_loc_id", "source_type", "sex", "age")
smoothing_targets <- fread(paste0(run_input_folder, "/smoothing_targets.csv"))

## Enforce global smooth here
smoothing_targets <- CJ(ihme_loc_id = unique(empir_deaths$ihme_loc_id), sex = "both")

## Run 3 separate smoothing runs
smoothed_3_year <- smooth_lts(empir_deaths, smoothing_targets, smoothing_by_vars,
                              moving_average_weights = rep(1, 3))
smoothed_3_year[, smooth_width := 3]

smoothed_5_year <- smooth_lts(empir_deaths, smoothing_targets, smoothing_by_vars,
                          moving_average_weights = rep(1, 5))
smoothed_5_year[, smooth_width := 5]

smoothed_7_year <- smooth_lts(empir_deaths, smoothing_targets, smoothing_by_vars,
                      moving_average_weights = rep(1, 7))
smoothed_7_year[, smooth_width := 7]

## Now, apply cascading selection process
## Keep the least-smoothed lifetable that fits within each classification cutoff
empir_deaths[, smooth_width := 1]

empir_deaths <- cascade_select_lts(list(empir_deaths, smoothed_3_year, smoothed_5_year, smoothed_7_year),
                                   id_vars = c(smoothing_by_vars[smoothing_by_vars != "age"], "year"),
                                   hiv_cdr = adult_hiv_covariate, 
                                   group_1_list = group_1_countries, 
                                   loc_spec_comp_restrictions = comp_loc_specific,
                                   hmd_qx_bounds = hmd_bounds, 
                                   hmd_qx_diff_regression = hmd_qx_diff_regression, 
                                   pop_age_pools = pop_age_pools, 
                                   country_pop = country_pop, 
                                   keep_metadata = F, keep_drops = F)
# Keep exclusion metadata and other metadata on smoothing etc. for vetting in empir_lt_full
empir_lt_drops <- empir_deaths[[2]]
write_csv(empir_lt_drops, paste0(run_output_folder, "/cascade_vetting.csv"))

empir_deaths <- empir_deaths[[1]]

write_csv(empir_deaths, paste0(run_output_folder, "/post_cascade_smoothed.csv"))

empir_deaths <- merge(empir_deaths, empir_lt_drops, by = c(smoothing_by_vars[smoothing_by_vars != "age"], "year", "smooth_width"))
empir_deaths <- empir_deaths[keep == 1]
empir_deaths[, c("var.log.qx", "num_down", "qx_1_violated", "keep", "pred_qx_diff", "qx_log_diff", "pct_qx_diff", "min_5q5", "min_5q10") := NULL]

## Create synthetic ZAF life tables to bridge the gap between HIV-free and with-HIV years
zaf_smooth <- empir_deaths[grepl("ZAF", ihme_loc_id) & (year == 1982 | year == 1993)]
male_zaf_years <- unique(zaf_smooth[sex == "male", year])
female_zaf_years <- unique(zaf_smooth[sex == "female", year])

if(1982 %in% male_zaf_years & 1993 %in% male_zaf_years & 1982 %in% female_zaf_years & 1993 %in% female_zaf_years) {
  zaf_smooth[year == 1982, weights_85 := .8]
  zaf_smooth[year == 1993, weights_85 := .2]

  zaf_smooth[year == 1982, weights_87 := .6]
  zaf_smooth[year == 1993, weights_87 := .4]

  zaf_smooth[year == 1982, weights_89 := .4]
  zaf_smooth[year == 1993, weights_89 := .6]

  zaf_smooth[year == 1982, weights_91 := .2]
  zaf_smooth[year == 1993, weights_91 := .8]

  zaf_1985 <- zaf_smooth[, list(qx = sum(qx * weights_85)), by = c(smoothing_by_vars, "life_table_category")]
  zaf_1985[, year := 1985]
  zaf_1987 <- zaf_smooth[, list(qx = sum(qx * weights_87)), by = c(smoothing_by_vars, "life_table_category")]
  zaf_1987[, year := 1987]
  zaf_1989 <- zaf_smooth[, list(qx = sum(qx * weights_89)), by = c(smoothing_by_vars, "life_table_category")]
  zaf_1989[, year := 1989]
  zaf_1991 <- zaf_smooth[, list(qx = sum(qx * weights_91)), by = c(smoothing_by_vars, "life_table_category")]
  zaf_1991[, year := 1991]
  empir_deaths <- rbindlist(list(empir_deaths, zaf_1985, zaf_1987, zaf_1989, zaf_1991), use.names = T, fill = T)
} else {
  empir_deaths <- NULL # Force stop in case running interactively with a habit of skipping by error messages
  stop("ZAF Smoothing cannot process -- years do not exist")
}

output_empir_ids <- c(empir_ids, "life_table_category")

setkeyv(empir_deaths, c(output_empir_ids, "age"))
qx_to_lx(empir_deaths, assert_na = F)


###############################################################################################################
## Drop countries where qx is over 1 or under 0
over_1 <- empir_deaths[qx > 1 | lx < 0 | qx < 0 | is.na(qx)]
over_1 <- unique(over_1[, .SD, .SDcols = output_empir_ids])
over_1[, drop := 1]
empir_deaths <- merge(empir_deaths, over_1, by = output_empir_ids, all.x = T)
empir_deaths <- empir_deaths[is.na(drop) | drop != 1]
empir_deaths[, drop := NULL]


###############################################################################################################
## Generate summary metrics, format all variables
## Generate 5q0, 45q15, and other summary metrics
empir_deaths[, px := 1 - qx]
sq5 <- empir_deaths[age <= 1, list(mean = 1 - prod(px)), by = output_empir_ids]
sq5[, life_table_parameter_id := 3]
sq5[, age_group_id := 1]

sq45 <- empir_deaths[age >= 15 & age <= 55, list(mean = 1 - prod(px)), by = output_empir_ids]
sq45[, life_table_parameter_id := 3]
sq45[, age_group_id := 199]

## Format to life table parameter IDs etc.
empir_deaths <- empir_deaths[, .SD, .SDcols = c(output_empir_ids, "age", "qx", "lx")]
empir_deaths <- merge(empir_deaths, age_map[, .(age_group_id, age_group_years_start)], by.x = "age", by.y = "age_group_years_start")
empir_deaths[, age := NULL]

empir_deaths <- melt(empir_deaths, id.vars = c(output_empir_ids, "age_group_id"))
empir_deaths[variable == "qx", life_table_parameter_id := 3]
empir_deaths[variable == "lx", life_table_parameter_id := 4]
empir_deaths[, variable := NULL]
setnames(empir_deaths, "value", "mean")

empir_formatted <- rbindlist(list(empir_deaths, sq5, sq45), use.names = T)

setnames(empir_formatted, "year", "year_id")
empir_formatted[sex == "male", sex_id := 1]
empir_formatted[sex == "female", sex_id := 2]

empir_formatted <- merge(empir_formatted, source_type_map[, .(source_type_id, type_short)], by.x = "source_type", by.y = "type_short", all.x = T)

empir_formatted <- empir_formatted[!is.na(source_type_id)]
assert_values(empir_formatted, "source_type_id", "not_na")

empir_formatted <- merge(empir_formatted, locations[, .(location_id, ihme_loc_id)], by = "ihme_loc_id")
assert_values(empir_formatted, "location_id", "not_na", quiet = T)

empir_formatted[life_table_category == "universal", life_table_category_id := 1]
empir_formatted[life_table_category == "location specific", life_table_category_id := 2]
assert_values(empir_formatted, "life_table_category_id", "not_na", quiet = T)


###############################################################################################################
## Create new version, output file, and upload
## Convert NaN values to NA to allow them to be uploaded appropriately
empir_formatted[is.nan(mean), mean := NA]

## Output final dataset
empir_formatted <- empir_formatted[, .(location_id, year_id, sex_id, age_group_id, life_table_parameter_id,
                                       nid = deaths_nid, underlying_nid = deaths_underlying_nid,
                                       source_type_id, life_table_category_id, mean)]

out_filepath <- paste0(run_output_folder, "/empir_lt.csv")
write_csv(empir_formatted, out_filepath)
upload_results(out_filepath, "life table empirical", "data", run_id = empir_run_id)

if(mark_best == T) update_status("life table empirical", "data", run_id = empir_run_id, new_status = "best")
