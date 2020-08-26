## title : select life tables
## purpose : select empirical life tables to include in MLT database

# packages
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
parser$add_argument('--run_mv', type="character", required=TRUE,
                    help='Whether we predicted from machine vision on this run')

args <- parser$parse_args()
empir_run_id <- args$version_id
mark_best <- as.logical(args$mark_best)
apply_outliers <- as.logical(args$apply_outliers)
run_mv <- as.logical(args$run_mv)
user <- Sys.getenv("USER")

## Import process-specific functions
empir_lt_func_dir <- paste0("FILEPATH/empirical_lt_functions")
empir_funcs <- list.files(empir_lt_func_dir)
for(func in empir_funcs) {
  source(paste0(empir_lt_func_dir, "/", func))
}
source(paste0("FILEPATH/shared_empirical_lt_functions"))

## Set working folder
master_folder <- paste0("FILEPATH/empirical_lt_folder/", empir_run_id)
run_input_folder <- paste0(master_folder, "/inputs")
run_output_folder <- paste0(master_folder, "/outputs")

## get empirical deaths from first script
empir_deaths <- fread(paste0(master_folder,"/all_lts.csv"))

## other inputs
completeness_outliers <- fread(paste0(master_folder,"/outputs/completeness_outliers.csv"))
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

  # categorize based on threshold 70% certainty
  mv[predicted > 0.7, outlier := 1]
  mv[predicted < 0.7, outlier := 0]
  mv[is.na(outlier), outlier := 2]
}

## Perform outliering =============================================================================================

if(apply_outliers == T) {
  outlier_sheet <- fread(paste0(run_input_folder, "/empir_lt_outliers.csv"))
  outlier_sheet[is.na(year_start) & !is.na(year_end), year_start := 1900]
  outlier_sheet[!is.na(year_start) & is.na(year_end), year_end := 2030]

} else {
  outlier_sheet <- data.table(ihme_loc_id = "", sex = "male", source_type = "", specific_year = 2050, year_start = as.integer(NA), year_end = as.integer(NA), apply_outliering = 0)

}
## Always apply 5q0 and 45q15 outliers, as well as completeness outliers
## Depending on toggles, either apply or do not apply manual outliers
empir_deaths <- outlier_data(empir_deaths,
                             outlier_sheet = outlier_sheet,
                             completeness_outliers = completeness_outliers,
                             run_id_5q0_estimate = best_5q0_run,
                             run_id_45q15_estimate = best_45q15_run)

outliered_deaths <- empir_deaths$outliered_data
outliered_deaths[, life_table_category := "outlier"]
outliered_deaths <- outliered_deaths[, .SD, .SDcols = c(output_empir_ids, "age", "qx")]

empir_deaths <- empir_deaths$kept_data

smoothing_by_vars <- c("ihme_loc_id", "source_type", "sex", "age")

## Import HIV crude death rate form the best version of 45q15 estimate
adult_hiv_covariate <- fread(paste0("FILEPATH/45q15/", empir_parents[["45q15 estimate"]], "/data/hiv_covariate.csv"))
adult_hiv_covariate[, year := floor(year)]

## Import location table and country lifetable-type designation
locations <- setDT(get_locations(level = "all", hiv_metadata = T))
group_1_countries <- unique(locations[group %in% c("1A", "1B"), ihme_loc_id])
completeness_outliers <- fread(paste0(master_folder,"/outputs/completeness_outliers.csv"))
comp_loc_specific <- fread(paste0(master_folder,"/outputs/comp_loc_specific.csv"))

## Import HMD old-age qx bounds
## Columns: age, sex, pct_95, pct_975, pct_99
hmd_bounds <- fread(paste0(master_folder, "/inputs/hmd_qx_bounds.csv"))
hmd_qx_diff_regression <- fread(paste0(master_folder, "/inputs/hmd_qx_diff_regression.csv"))

## Import population age pooling metadata
pop_age_pools <- fread(paste0("FILEPATH/population/",
                              empir_parents[["population estimate"]],
                              "/versions_best.csv"))
setnames(pop_age_pools, "drop_age", "pop_pool_age")
pop_age_pools <- pop_age_pools[, .(ihme_loc_id, pop_pool_age)]

## Import country total population counts
country_pop <- get_mort_outputs("population", "estimate", run_id = empir_parents[["population estimate"]], age_group_id = 22, sex_id = 3)
country_pop <- country_pop[, .(ihme_loc_id, year = year_id, mean_pop = mean)]

empir_deaths <- cascade_select_lts(list(empir_deaths),
                                   id_vars = c(smoothing_by_vars[smoothing_by_vars != "age"], "year"),
                                   hiv_cdr = adult_hiv_covariate,
                                   group_1_list = group_1_countries,
                                   loc_spec_comp_restrictions = comp_loc_specific,
                                   hmd_qx_bounds = hmd_bounds,
                                   hmd_qx_diff_regression = hmd_qx_diff_regression,
                                   pop_age_pools = pop_age_pools,
                                   country_pop = country_pop,
                                   keep_metadata = F, keep_drops = T)

# Keep exclusion metadata and other metadata on smoothing etc. for vetting in empir_lt_full
empir_lt_drops <- empir_deaths[[2]]
write_csv(empir_lt_drops, paste0(run_output_folder, "/cascade_vetting.csv"))

empir_deaths <- empir_deaths[[1]]

write_csv(empir_deaths, paste0(run_output_folder, "/post_cascade_smoothed.csv"))

empir_deaths <- merge(empir_deaths, empir_lt_drops, by = c(smoothing_by_vars[smoothing_by_vars != "age"], "year", "smooth_width"))
setnames(empir_deaths, "max_outlier_type_id", "outlier_type_id")
empir_deaths <- empir_deaths[, .SD, .SDcols = c(output_empir_ids, "age", "qx")]

## ZAF special case =================================================================================================

## Create synthetic ZAF life tables to bridge the gap between HIV-free and with-HIV years
## Because in the MLT process, we fit ZAF to with-HIV lifetables, we want to make sure that years in the late 1980s have the age pattern pre-HIV
## But we also don't want to cause huge age pattern disconnects, so we ramp up from 1982 to 1993 using a standard interpolation
zaf_smooth <- empir_deaths[grepl("ZAF", ihme_loc_id) & (year == 1982 | year == 1993) & outlier_type_id == 1]
male_zaf_years <- unique(zaf_smooth[sex == "male", year])
female_zaf_years <- unique(zaf_smooth[sex == "female", year])

# If male_zaf_years or female_zaf_years missing 1982 or 1993,
# add back in w/ smooth_width 7
if(! (1982 %in% male_zaf_years)){
  zaf_smooth <- rbind(zaf_smooth, empir_deaths[grepl("ZAF", ihme_loc_id) & year == 1982 & sex == "male" & smooth_width == 7])
}
if(! (1993 %in% male_zaf_years)){
  zaf_smooth <- rbind(zaf_smooth, empir_deaths[grepl("ZAF", ihme_loc_id) & year == 1993 & sex == "male" & smooth_width == 7])
}
if(! (1982 %in% female_zaf_years)){
  zaf_smooth <- rbind(zaf_smooth, empir_deaths[grepl("ZAF", ihme_loc_id) & year == 1982 & sex == "female" & smooth_width == 7])
}
if(! (1993 %in% female_zaf_years)){
  zaf_smooth <- rbind(zaf_smooth, empir_deaths[grepl("ZAF", ihme_loc_id) & year == 1993 & sex == "female" & smooth_width == 7])
}
# recalculate ZAF years
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
  zaf_1985[, smooth_width := 2]
  zaf_1985[, outlier_type_id := 1]
  zaf_1987 <- zaf_smooth[, list(qx = sum(qx * weights_87)), by = c(smoothing_by_vars, "life_table_category")]
  zaf_1987[, year := 1987]
  zaf_1987[, smooth_width := 2]
  zaf_1987[, outlier_type_id := 1]
  zaf_1989 <- zaf_smooth[, list(qx = sum(qx * weights_89)), by = c(smoothing_by_vars, "life_table_category")]
  zaf_1989[, year := 1989]
  zaf_1989[, smooth_width := 2]
  zaf_1989[, outlier_type_id := 1]
  zaf_1991 <- zaf_smooth[, list(qx = sum(qx * weights_91)), by = c(smoothing_by_vars, "life_table_category")]
  zaf_1991[, year := 1991]
  zaf_1991[, smooth_width := 2]
  zaf_1991[, outlier_type_id := 1]
  empir_deaths <- rbindlist(list(empir_deaths, zaf_1985, zaf_1987, zaf_1989, zaf_1991), use.names = T, fill = T)
} else {
  empir_deaths <- NULL # Force stop in case running interactively
  stop("ZAF Smoothing cannot process -- years do not exist")
}

empir_deaths <- rbindlist(list(empir_deaths, outliered_deaths), use.names = T)
setkeyv(empir_deaths, c(output_empir_ids, "age"))
qx_to_lx(empir_deaths, assert_na = F)

## Generate summary metrics ==================================================================================

## Drop countries where qx is over 1 or under 0
over_1 <- empir_deaths[qx > 1 | lx < 0 | qx < 0 | is.na(qx)]
over_1 <- unique(over_1[, .SD, .SDcols = output_empir_ids])
over_1[, drop := 1]
empir_deaths <- merge(empir_deaths, over_1, by = output_empir_ids, all.x = T)
empir_deaths <- empir_deaths[is.na(drop) | drop != 1]
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

## Merge on locations
empir_formatted <- merge(empir_formatted, locations[, .(location_id, ihme_loc_id)], by = "ihme_loc_id")
assert_values(empir_formatted, "location_id", "not_na", quiet = T)

## Partition countries into universal vs. country-specific lifetable database
empir_formatted[life_table_category == "universal", life_table_category_id := 1]
empir_formatted[life_table_category == "location specific", life_table_category_id := 2]
empir_formatted[outlier_type_id == 17, life_table_category_id := 5]
empir_formatted[life_table_category == "outlier", life_table_category_id := 5]
assert_values(empir_formatted, "life_table_category_id", "not_na", quiet = T)

## re-assign based on machine vision, and some other post-hoc testing ===============================================

if(run_mv == T){
  # merge on mv decisions
  empir_formatted <- merge(empir_formatted, mv, by=c("ihme_loc_id","year_id","sex","source_type","smooth_width"), all.x=T)
  empir_formatted[ihme_loc_id %like% "ZAF", `:=` (predicted=0.5, outlier=2)] # ZAF handled separately above
  assert_values(empir_formatted, "predicted","not_na", quiet=T)

  # set to non-outlier if non-outlier from mv
  empir_formatted[outlier == 0 & !(ihme_loc_id %like% "ZAF"), `:=` (outlier_type_id = 1, life_table_category_id = 1)]

  # set to outlier if outlier from mv
  empir_formatted[outlier == 1, `:=` (outlier_type_id = 13, life_table_category_id = 5)]

  # set to non-outlier if only outliered because smaller smooth width selected
  empir_formatted[outlier_type_id == 17, `:=` (outlier_type_id = 1, life_table_category_id = 1)]

  # re-select smallest smooth width
  empir_formatted[outlier_type_id == 1, min_smooth := min(smooth_width), by=c("ihme_loc_id","year_id","sex","source_type","life_table_category_id")]
  empir_formatted[smooth_width > min_smooth & outlier_type_id == 1, `:=` (outlier_type_id = 17, life_table_category_id = 5)]

}

# posthoc outlier adjustments/testing
posthoc_outlier_adjustments <- T
if(posthoc_outlier_adjustments == T){

  review <- read.csv("FILEPATH/empirical-lt/machine-vision/v181_complete_review.csv")
  setnames(review, c("ihme_loc_id","year_id","sex","source_type","smooth_width","life_table_category_id_v181","outlier_type_id_v181"))

  empir_formatted <- merge(empir_formatted, review, by=c("ihme_loc_id","year_id","sex","source_type","smooth_width"), all.x=T)

  empir_formatted[!(life_table_category_id == 2 & life_table_category_id_v181 == 5) & !is.na(life_table_category_id_v181) & outlier_type_id!=18,
                  life_table_category_id := life_table_category_id_v181]

  # adjust outlier_type_id (id 13 = manual outlier)
  empir_formatted[life_table_category_id == 5 & outlier_type_id == 1, outlier_type_id := 13]
  empir_formatted[life_table_category_id %in% c(1,2), outlier_type_id := 1]

  review1 <- fread("FILEPATH/empirical-lt/201/diagnostics/201_review.csv")
  map1 <- fread("FILEPATH/empirical-lt/201/diagnostics/plot_num_map.csv")
  review2 <- fread("FILEPATH/empirical-lt/201/diagnostics/201_review2.csv")
  map2 <- fread("FILEPATH/empirical-lt/201/diagnostics/plot_num_map.csv")

  review1 <- merge(review1, map1, by="plot_num")
  review2 <- merge(review2, map2, by="plot_num")
  setnames(review1, "smooth_width", "decision")
  review <- rbind(review1, review2)
  review[, plot_num := NULL]
  review <- review[!is.na(decision)]
  review <- unique(review, by=c("ihme_loc_id","year_id","sex","source_name"))
  setnames(review, "source_name", "source_type")

  empir_formatted <- merge(empir_formatted, review, by = c("ihme_loc_id", "year_id", "sex", "source_type"), all.x = T)
  empir_formatted[smooth_width == decision & outlier_type_id != 18, `:=` (outlier_type_id = 1, life_table_category_id = 1)]

  review3 <- fread("FILEPATH/empirical-lt/203/diagnostics/review3.csv")
  map3 <- fread("FILEPATH/empirical-lt/203/diagnostics/plot_num_map.csv")
  review3 <- merge(review3, map3, by="plot_num")
  setnames(review3, c("life_table_category_id","source_name"), c("life_table_category_id_review3","source_type"))
  review3[,plot_num := NULL]
  empir_formatted <- merge(empir_formatted, review3, by=c("smooth_width","ihme_loc_id","year_id","sex","source_type"), all.x = T)
  empir_formatted[!is.na(life_table_category_id_review3) & outlier_type_id != 18,
                  `:=` (life_table_category_id = life_table_category_id_review3,
                         outlier_type_id = 1)]

  empir_formatted[ihme_loc_id == "MYS" & year_id %in% c(2010:2013), `:=` (outlier_type_id = 13,
                                                                       life_table_category_id = 5)]
  empir_formatted[ihme_loc_id == "TUR" & year_id == 2014, `:=` (outlier_type_id = 13,
                                                                life_table_category_id = 5)]

  review_sep3 <- fread("FILEPATH/empirical-lt/decisions/usa_review_sep3.csv")
  setnames(review_sep3, "source_name", "source_type")
  review_sep3[,plot_num := NULL]
  empir_formatted <- merge(empir_formatted, review_sep3, by=c("ihme_loc_id","year_id","sex","source_type"), all.x = T)
  empir_formatted[smooth_width == include, `:=` (life_table_category_id = 1,
                                                 outlier_type_id = 1)]
  empir_formatted[loc_specific == 1 & life_table_category_id == 1, life_table_category_id := 2]

  loc_list <- c(163, 531, 130, 16, 17, 18, 141, 122, 128, 125, 69, 132, 149, 161, 181, 35494:35514)

  # re-select smallest smooth_width
  empir_formatted[life_table_category_id == 1,
                  min_smooth := as.integer(min(smooth_width, na.rm=T)),
                  by=c("ihme_loc_id","year_id","sex","source_type")]
  empir_formatted[, min_smooth := as.integer(min(min_smooth, na.rm=T)),
                  by= c("ihme_loc_id","year_id","sex","source_type")]
  empir_formatted[smooth_width > min_smooth & life_table_category_id %in% c(1,2),
                  `:=` (outlier_type_id = 17, life_table_category_id = 5)]
  empir_formatted[, min_smooth := NULL]
  empir_formatted[life_table_category_id == 2,
                  min_smooth := min(smooth_width, na.rm=T),
                  by=c("ihme_loc_id","year_id","sex","source_type")]
  empir_formatted[smooth_width > min_smooth & life_table_category_id == 2,
                  `:=` (outlier_type_id = 17, life_table_category_id = 5)]

  # remove extra columns
  empir_formatted[,c("life_table_category_id_v181", "outlier_type_id_v181", "min_smooth") := NULL]

  # remove location-specific unless in specified locations
  remove_loc_specific <- T
  if(remove_loc_specific == T){
    empir_formatted[life_table_category_id == 2 & !(ihme_loc_id %like% "ZAF") & !(location_id %in% loc_list),
                    `:=` (life_table_category_id = 5, outlier_type_id = 13)]
  }

  # remove duplicates
  empir_formatted <- unique(empir_formatted)

}


## Create new version, output file, and upload ==============================================================

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
