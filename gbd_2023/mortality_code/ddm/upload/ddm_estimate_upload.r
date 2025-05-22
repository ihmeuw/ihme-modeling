################################################################################
## Description: Prep DDM estimates for upload
################################################################################

rm(list=ls())
library(foreign); library(reshape); library(argparse); library(readstata13);
library(data.table); library(readr); library(mortdb); library(haven); library(assertable);

if (Sys.info()[1] == "Linux") {
  root <- "FILEPATH"
  version_id <- as.character(commandArgs(trailingOnly = T)[1])
  gbd_year <- as.numeric(commandArgs(trailingOnly = T)[2])
  mark_best <- as.logical(commandArgs(trailingOnly= T)[3])
} else {
  root <- "FILEPATH"
}
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of DDM')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD round')
parser$add_argument('--mark_best', type="character", required=TRUE,
                    help='True/False mark run as best')
args <- parser$parse_args()

version_id <- args$version_id
gbd_year <- args$gbd_year
mark_best <- args$mark_best

# Set variables and filepaths
main_dir <- paste0("FILEPATH")
d08_data_filepath <- paste0("FILEPATH")
d08_data_filepath_old <- paste0("FILEPATH")

d00_data_filepath <- paste0("FILEPATH")

# Get location metadata
ap_old <- data.table(get_locations(gbd_type = 'ap_old', level = 'estimate', gbd_year = gbd_year))
ap_old <- ap_old[location_name == "Old Andhra Pradesh"]

locations <- data.table(get_locations(gbd_year = gbd_year))
locations <- locations[!(grepl("KEN_", ihme_loc_id) & level == 4)]
locations <- rbind(locations, ap_old)
locations <- locations[, list(ihme_loc_id, location_id, location_name)]


####################
## DDM - Estimates
####################

est <- data.table(read_dta(d08_data_filepath, encoding = "latin1"))

# create age, sex, and source variables
split <- strsplit(est$iso3_sex_source, "&&")
iso3.sex.source <- do.call(rbind, split)
est[, iso3 := iso3.sex.source[,1]]
est[, sex := iso3.sex.source[,2]]
est[, source := iso3.sex.source[,3]]

# keep only the variables of interest, rename
# take the average of the 3 estimates per iso3-sex-source-year
est <- unique(est[, list(iso3 = ihme_loc_id, iso3_sex_source, source, year, sex, u5 = u5_comp_pred, first = pred1, second_med = pred, second_lower = lower, second_upper = upper, trunc_med = trunc_pred, trunc_lower, trunc_upper)])
est <- est[, lapply(.SD, mean, na.rm = T), .SDcols = c('u5', 'first', 'second_med', 'second_lower', 'second_upper', 'trunc_med', 'trunc_lower', 'trunc_upper'), by = c('iso3', 'iso3_sex_source', 'source', 'year', 'sex')]

# create an indicator variable
est[, trunc.or.second := "second.med"]
est[grepl("DSP",source) | source == "SRS" | grepl("VR", source), trunc.or.second := "trunc.med"]

# to plot the final estimates, censuses use second.med and vr/srs/dsp use trunc.med
est[, final_est := 0]
est[trunc.or.second == "second.med", final_est := second_med]
est[trunc.or.second == "trunc.med", final_est := trunc_med]

est[, final_lower := 0]
est[trunc.or.second == "second.med", final_lower := second_lower]
est[trunc.or.second == "trunc.med", final_lower := trunc_lower]

est[,final_upper := 0]
est[trunc.or.second == "second.med", final_upper := second_upper]
est[trunc.or.second == "trunc.med", final_upper := trunc_upper]

# drop unnecessary variables, final formatting
est[, trunc.or.second := NULL]
est[, iso3_sex_source := NULL]
est[, year := year + 0.5]

setnames(est,
         c('u5', 'first', 'second_med', 'second_lower', 'second_upper', 'trunc_med', 'trunc_lower', 'trunc_upper', 'final_est', 'final_lower', 'final_upper'),
         c('mean_u5', 'mean_first', 'mean_pred2', 'lower_pred2', 'upper_pred2', 'mean_trunc', 'lower_trunc', 'upper_trunc', 'mean_final', 'lower_final', 'upper_final'))
est <- unique(est)

# Reshape long
est <- melt(est, id.vars = c("iso3", "sex", "year", "source"),
            variable.name = "method",
            measure.vars = c(grep("mean", names(est), value=T),
                             grep("lower", names(est), value=T),
                             grep("upper", names(est), value=T)))
setDT(est)

# save ddm for 45q15 age-sex split adj
ddm <- copy(est)
setDT(ddm)

est[, value_type :=  gsub("^(.*?)_.*", "\\1", method)]
est[, method := gsub("^.*\\_", "_", method)]
est <- dcast.data.table(est, iso3 + sex + year + source + method ~ value_type, value.var = 'value')


## Set estimate stage id
est[method == "_first", estimate_stage_id := 1]
est[method == "_pred2", estimate_stage_id := 2]
est[method == "_trunc", estimate_stage_id := 9]
est[method == "_final", estimate_stage_id := 10]
est[method == "_u5", estimate_stage_id := 11]

est[, method := NULL]

est[sex == "male", sex_id := 1]
est[sex == "female", sex_id := 2]
est[sex == "both", sex_id := 3]

setnames(est, "year", "viz_year")
est[, year_id := floor(viz_year)]
est[, age_group_id := 199]
est[estimate_stage_id == 11, age_group_id := 1]

## Generate type_id
# Initially categorize main source types
est[source == "VR" | source == "VR-SSA", source_type_id := 1]
est[source == "SRS", source_type_id := 2]
est[grepl("DSP", source), source_type_id := 3]
est[source == "CENSUS", source_type_id := 5]
est[source == "SURVEY", source_type_id := 16]

est[grepl("VR", source) & is.na(source_type_id), source_type_id := 1]

est[source == "MOH survey", source_type_id := 34]

## Split out SSPC-DC
est[source == "SSPC-DC" , source_type_id := 50]

est[source == "FFPS", source_type_id := 38]
est[source == "SUPAS", source_type_id := 39]

est[source == "SUSENAS", source_type_id := 40]
est[source == "HOUSEHOLD", source_type_id := 42]
est[source == "HOUSEHOLD_HHC", source_type_id := 43]
est[source == "MCCD", source_type_id := 55]
est[source == "CR", source_type_id := 56]
est[source == "HOUSEHOLD_DLHS", source_type_id := 41]

assert_values(est, "source_type_id", "not_na")

setnames(est, "iso3", "ihme_loc_id")
est <- merge(est, locations, by = 'ihme_loc_id', all.x = T)

est <- est[, list(year_id, viz_year, location_id, sex_id, age_group_id, estimate_stage_id, source_type_id, mean, lower, upper, source)]
est <- est[order(year_id, location_id, sex_id, age_group_id, estimate_stage_id, source_type_id),]

# Bring in final comp estimates
final_comp <- data.table(read_dta(paste0("FILEPATH")))
final_comp <- final_comp[, list(ihme_loc_id, year, source, sex, mean = final_comp)]
setnames(final_comp, "year", "viz_year")
final_comp[, year_id := floor(viz_year)]

final_ddm <- copy(final_comp)

# Set sex_id
final_comp[sex == "male", sex_id := 1]
final_comp[sex == "female", sex_id := 2]
final_comp[sex == "both", sex_id := 3]
final_comp[, sex := NULL]

# Set age group id variable
final_comp[, age_group_id := 199]

# Set estimate stage id variable
final_comp[, estimate_stage_id := 14]

# Set source type id variable
final_comp[source == "VR" | source == "VR-SSA", source_type_id := 1]
final_comp[source == "SRS", source_type_id := 2]
final_comp[source == "DSP", source_type_id := 3]
final_comp[source == "CENSUS", source_type_id := 5]
final_comp[source == "MOH survey", source_type_id := 34]
final_comp[source == "SSPC-DC" , source_type_id := 50]
final_comp[source == "DC", source_type_id := 37]
final_comp[source == "SSPC", source_type_id := 36]
final_comp[source == "FFPS", source_type_id := 38]
final_comp[source == "SUPAS", source_type_id := 39]
final_comp[source == "SURVEY", source_type_id := 16]
final_comp[source == "SUSENAS", source_type_id := 40]
final_comp[source == "HOUSEHOLD_DLHS", source_type_id := 41]
final_comp[source == "HOUSEHOLD", source_type_id := 42]
final_comp[source == "HOUSEHOLD_HHC", source_type_id := 43]
final_comp[source == "MCCD", source_type_id := 55]
final_comp[source == "CR", source_type_id := 56]

assert_values(final_comp, "source_type_id", "not_na")

## Merge location metadata
final_comp <- merge(final_comp, locations, by = 'ihme_loc_id', all.x = T)
final_comp <- final_comp[, list(year_id, viz_year, location_id, sex_id, age_group_id, estimate_stage_id, source_type_id, mean, lower = NA, upper = NA, source)]

est <- rbind(est, final_comp)

assert_values(est[estimate_stage_id == 9], c("mean", "lower", "upper"), "not_na")
assert_values(est[estimate_stage_id == 10], c("mean", "lower", "upper"), "not_na")
assert_values(est[estimate_stage_id == 2], c("mean", "lower", "upper"), "not_na")

## Save in upload folder
write_csv(est, paste0("FILEPATH"), na = "")

# append age-sex split VR for 2020 and 2023
age_map <- get_age_map(type = "all", gbd_year = gbd_year)[, .(age_group_id, age_start = age_group_years_start, age_end = age_group_years_end)]
age_map <- rbind(
  age_map,
  data.table(age_group_id = 1, age_start = 0 , age_end = 5)
)

adult <- haven::read_dta(paste0("FILEPATH"))
setDT(adult)

split_vr <- fread("FILEPATH")

# aggregate 0-1, and 0-5
split_vr <- merge(split_vr, age_map, by = "age_group_id")

split_vr_u1 <- split_vr[
  age_end <= 1,
  .(age_start = 0, age_end = 1, age_group_id = 28, deaths = sum(deaths)),
  by = c("location_id", "year_id", "sex_id", "nid")
]

split_vr_u5 <- split_vr[
  age_start >= 1 & age_end <= 5,
  .(age_start = 1, age_end = 5, age_group_id = 5, deaths = sum(deaths)),
  by = c("location_id", "year_id", "sex_id", "nid")
]

split_vr <- rbind(
  split_vr[age_end > 5],
  split_vr_u1,
  split_vr_u5
)

# Process for 45q15
pop_data <- get_proc_lineage("ddm", "estimate", run_id = version_id)
pop_data <- get_mort_outputs(
  "population",
  "estimate",
  run_id = pop_data[parent_process_name == "population estimate", parent_run_id],
  year_ids = 2020:2024,
  sex_ids = 1:2,
  age_group_ids = c(1, unique(split_vr$age_group_id))
)
pop_u5 <- pop_data[age_group_id == 1]
pop_data <- pop_data[age_group_id != 1, .(location_id, sex_id, age_group_id, year_id, c1 = mean)]

split_vr <- merge(
  split_vr,
  pop_data,
  by = c("location_id", "sex_id", "year_id", "age_group_id"),
)

split_vr <- split_vr[!(location_id %in% c(191, 163))]

setnames(split_vr, "deaths", "vr")

# append ddm estimates and apply adjustments
ddm <- ddm[
  source == "VR" & method == "mean_u5",
  .(ihme_loc_id = iso3, sex, year_id = floor(year), mean_u5 = value)
  ]
ddm <- merge(ddm, locations[, .(location_id, ihme_loc_id)], by = "ihme_loc_id")

final_ddm <- final_ddm[source == "VR"]
final_ddm <- merge(final_ddm, locations[, .(location_id, ihme_loc_id)], by = "ihme_loc_id")
setnames(final_ddm, "mean", "mean_final")
final_ddm[, c("viz_year", "source") := NULL]

ddm <- merge(ddm, final_ddm, by = c("ihme_loc_id", "location_id", "year_id", "sex"))

ddm_both <- ddm[sex == "both"]
ddm_male <- copy(ddm_both)
ddm_male <- ddm_male[, sex := "male"]
ddm_female <- copy(ddm_both)
ddm_female <- ddm_female[, sex := "female"]

ddm <- rbind(ddm[sex != "both"], ddm_male, ddm_female)
ddm[sex == "male", sex_id := 1]
ddm[sex == "female", sex_id := 2]
ddm[, c("sex", "ihme_loc_id") := NULL]
ddm[mean_u5 > 1, mean_u5 := 1]

split_vr <- merge(split_vr, ddm, by = c("location_id", "sex_id", "year_id"))

split_vr[, obsasmr := vr / c1]
split_vr[age_end <= 5, adjasmr := (vr / mean_u5) / c1]
split_vr[
  age_start == 5 & age_end == 10,
  adjasmr := (vr / ((2 / 3) * mean_u5 + (1 / 3) * mean_final)) / c1
]
split_vr[
  age_start == 10 & age_end == 15,
  adjasmr := (vr / ((1 / 3) * mean_u5 + (2 / 3) * mean_final)) / c1
]
split_vr[age_start >= 15, adjasmr := (vr / mean_final) / c1]

split_vr[age_end == 125, age_end := Inf]

assertable::assert_values(split_vr, "obsasmr", "lte", split_vr$adjasmr)

# make wide VR to merge later
split_vr[, age_group := paste0(age_start, "to", age_end - 1)]
split_vr[, age_group := gsub("toInf", "plus", age_group)]

wide_vr <- copy(split_vr)
wide_vr[, c("age_group_id", "age_start", "age_end") := NULL]

wide_vr <- data.table::dcast(wide_vr, ...~age_group, value.var = c("vr", "c1", "obsasmr", "adjasmr"))

# calc 45q15s
split_vr_45 <- split_vr[age_group_id %in% 8:16, .(vr = sum(vr), c1 = sum(c1), vr_adj = sum(vr / mean_final)), by = c("location_id", "sex_id", "year_id", "nid")]

split_vr_45[, obs45q15 := 1 - exp((-1 * (60 - 15)) * (vr / c1))]
split_vr_45[, adj45q15 := 1 - exp((-1 * (60 - 15)) * (vr_adj / c1))]
split_vr_45 <- split_vr_45[, .(location_id, sex_id, year_id, nid, obs45q15, adj45q15)]

# append 0-5 pop
pop_u5 <- pop_u5[, .(location_id, sex_id, year_id, c1_0to4 = mean)]

wide_vr <- merge(wide_vr, pop_u5, by = c("location_id", "sex_id", "year_id"))

# format all data
wide_vr <- merge(wide_vr, split_vr_45, by = c("location_id", "sex_id", "year_id", "nid"))
setnames(wide_vr, c("mean_u5", "mean_final", "year_id", "nid"), c("comp_u5", "comp", "year", "deaths_nid"))
wide_vr <- merge(wide_vr, locations[, .(location_id, ihme_loc_id)], by = "location_id")
wide_vr[, source_type := "VR"]
wide_vr[sex_id == 1, sex := "male"]
wide_vr[is.na(sex), sex := "female"]
wide_vr[, deaths_source := paste0(ihme_loc_id, "_", source_type)]
wide_vr[, pop_source := ""]
wide_vr[, deaths_underlying_nid := deaths_nid]
wide_vr[, comp_u5_pt_est := comp_u5]
wide_vr[, adjust := 1]
wide_vr[, hh_scaled := 0]
wide_vr[, max_age_gap := 5]
wide_vr[, min_age_gap := 5]

# remove calls
remove_cols <- setdiff(names(wide_vr), names(adult))
wide_vr[, c(remove_cols) := NULL]

setdiff(names(adult), names(wide_vr))

# dedup by VR years
existing_vr <- unique(adult[source_type == "VR", .(ihme_loc_id, sex, year, exists = TRUE)])

wide_vr <- merge(
  wide_vr,
  existing_vr,
  by = c("ihme_loc_id", "sex", "year"),
  all.x = TRUE
)
wide_vr <- wide_vr[is.na(exists)]
wide_vr[, exists := NULL]

sd_data <- adult[source_type == "VR", .(sd = mean(sd, na.rm = TRUE)), by = c("source_type", "ihme_loc_id")]
sd_data[, source_type := NULL]

sd_reg <- adult[source_type == "VR"]
sd_reg <- merge(sd_reg, get_locations(gbd_year = gbd_year)[, .(ihme_loc_id, region_id)])
sd_reg <- sd_reg[, .(sd_reg_max = max(sd, na.rm = TRUE)), by = "region_id"]
sd_reg <- merge(sd_reg, get_locations(gbd_year = gbd_year)[, .(ihme_loc_id, region_id)])
sd_data <- merge(sd_data, sd_reg, by = "ihme_loc_id")
sd_data[is.na(sd), sd := sd_reg_max]
sd_data[, c("sd_reg_max", "region_id") := NULL]

wide_vr <- merge(wide_vr, sd_data, by = "ihme_loc_id", all.x = TRUE)

# append and save
adult <- rbind(adult, wide_vr, fill = TRUE)

haven::write_dta(
  adult,
  paste0("FILEPATH")
)

# Upload ddm estimate file
upload_results(filepath = paste0("FILEPATH"),
               model_name = "ddm",
               model_type = "estimate",
               run_id = version_id, send_slack = T)

if (mark_best){
  update_status(model_name = "ddm",
                model_type = "estimate",
                run_id = version_id,
                new_status = "best",
                assert_parents=F, send_slack = F)
}
