# splice missing data into VRP runs

# HOTFIX: This script adds data that has been deleted from the CoD database and
#         has not been located to add back. It also makes some adjustments to
#         Ukraine per Haidong's request. It can be removed from our pipeline
#         once all old data has been located and the Ukraine changes have been
#         moved to a permanent location.

# This removes changes to GBR 2017 since we have updated this now

# Set up -----------------------------------------------------------------------

library(assertable)
library(data.table)
library(dplyr)
library(haven)

dir_base <- "FILEPATH"

run_id_new <- "RUN_ID"

# Make backups ------------------------------------------------------------

backup_files_from <- c("empirical_deaths.csv", "d00_compiled_deaths.dta", "cod_VR_no_shocks.csv", "noncod_VR_no_shocks.csv")
backup_files_to <- paste0(tools::file_path_sans_ext(backup_files_from), "_original.", tools::file_ext(backup_files_from))

fs::file_copy(
  fs::path(),
  fs::path(),
  overwrite = FALSE
)


# Load -------------------------------------------------------------------------

add_estimate_stage_id <- function(dt){
 dt_22 <- copy(dt)
 dt[, estimate_stage_id := 21]
 dt_22[, estimate_stage_id := 22]
 dt <- rbind(dt, dt_22)
 return(dt)
}

# NOTES: HUN 2016, DYB 2001, DYB 2007, some DYB 2012

missing_nids <- c(325032, 52940, 52840, 237659)

empirical_deaths_297 <- add_estimate_stage_id(fread(fs::path()))
empirical_deaths_308 <- add_estimate_stage_id(fread(fs::path()))
empirical_deaths_new <- fread(fs::path())

d00_compiled_deaths_297 <- add_estimate_stage_id(setDT(read_dta(fs::path())))
d00_compiled_deaths_308 <- add_estimate_stage_id(setDT(read_dta(fs::path())))
d00_compiled_deaths_new <- setDT(read_dta(fs::path()))

noncod_VR_no_shocks_297 <- add_estimate_stage_id(fread(fs::path()))
noncod_VR_no_shocks_308 <- add_estimate_stage_id(fread(fs::path()))
noncod_VR_no_shocks_new <- fread(fs::path())

cod_VR_no_shocks_308 <- add_estimate_stage_id(fread(fs::path()))
cod_VR_no_shocks_new <- fread(fs::path())

# TODO: this is misaligned
use_agge_years_ukr <- c(2008, 2009, 2012, 2015, 2016, 2017, 2018, 2019, 2020, 2021)
ukr_subnats <- c(50559, 44934, 44939)

dt_pop <- fread(fs::path())

dt_pop_postneonatal <- dt_pop[age_group_id %in% c(388, 389)]
dt_pop_postneonatal[, prop := population / sum(population), by = .(location_id, year_id, sex_id)]

dt_pop_1_to_4 <- dt_pop[age_group_id %in% c(238, 34)]
dt_pop_1_to_4[, prop := population / sum(population), by = .(location_id, year_id, sex_id)]

loc_map <- demInternal::get_locations()

# empirical deaths -------------------------------------------------------------

# append
empirical_deaths_IDcols <- setdiff(names(empirical_deaths_new), c("mean","nid","detailed_source","underlying_nid"))
missing_empirical_deaths <- anti_join(empirical_deaths_297, empirical_deaths_new, by = empirical_deaths_IDcols)
missing_empirical_deaths <- missing_empirical_deaths[(nid %in% missing_nids) | (nid==140966 & location_id==13)]

# post neonatal -> 1-5 months + 6-11 months #
missing_empirical_deaths_postneonatal <- missing_empirical_deaths[age_group_id == 4]
missing_empirical_deaths_postneonatal <- merge(
  missing_empirical_deaths_postneonatal,
  dt_pop_postneonatal[, -c("population", "run_id")],
  by = c("location_id", "year_id", "sex_id"),
  all.x = TRUE
)
missing_empirical_deaths_postneonatal[, mean := mean * prop]
missing_empirical_deaths_postneonatal <- missing_empirical_deaths_postneonatal[, -c("age_group_id.x", "prop")]
setnames(missing_empirical_deaths_postneonatal, "age_group_id.y", "age_group_id")
missing_empirical_deaths <- rbind(missing_empirical_deaths[!age_group_id %in% c(388, 389)], missing_empirical_deaths_postneonatal)

# 1 to 4 -> 12 to 23 months + 2 to 4 #
missing_empirical_deaths_1_to_4 <- missing_empirical_deaths[age_group_id == 5]
missing_empirical_deaths_1_to_4 <- merge(
  missing_empirical_deaths_1_to_4,
  dt_pop_1_to_4[, -c("population", "run_id")],
  by = c("location_id", "year_id", "sex_id"),
  all.x = TRUE
)
missing_empirical_deaths_1_to_4[, mean := mean * prop]
missing_empirical_deaths_1_to_4 <- missing_empirical_deaths_1_to_4[, -c("age_group_id.x", "prop")]
setnames(missing_empirical_deaths_1_to_4, "age_group_id.y", "age_group_id")
missing_empirical_deaths <- rbind(missing_empirical_deaths[!age_group_id %in% c(238, 34)], missing_empirical_deaths_1_to_4)

# drop overlapping lower age groups
missing_empirical_deaths <- missing_empirical_deaths[!age_group_id %in% c(4, 5)]

# try not to add duplicate data
missing_empirical_deaths <- anti_join(missing_empirical_deaths, empirical_deaths_new, by = setdiff(names(empirical_deaths_new), "mean"))

empirical_deaths_new <- rbind(empirical_deaths_new, missing_empirical_deaths)

# check for duplicates in VR
empirical_deaths_new_dedup <- copy(empirical_deaths_new)
empirical_deaths_new_dedup[, dup := .N, by = empirical_deaths_IDcols]

# remove dups
empirical_deaths_new_dedup[(dup == 2 & detailed_source == "DYB" & outlier == 0), outlier := 1]
empirical_deaths_new_dedup[, dup := .N, by = empirical_deaths_IDcols]
empirical_deaths_new_dedup[(dup == 2 & detailed_source != "WHO" & outlier == 0), outlier := 1]

# manually drop WHO MNE
empirical_deaths_new_dedup[location_id == 50 & nid == 287600 & year_id %in% c(1971, 1981), outlier := 1]

# check for dups
empirical_deaths_new_dedup[, dup := .N, by = empirical_deaths_IDcols]
assert_values(empirical_deaths_new_dedup[outlier==0], "dup", test="equal", test_val = 1)
empirical_deaths_new_dedup[, dup := NULL]

#save
readr::write_csv(empirical_deaths_new_dedup, fs::path())


# d00_compiled_deaths ----------------------------------------------------------

d00_compiled_deaths_IDcols <- c("ihme_loc_id","country","sex","year",
                                "source_type_id","deaths_footnote", "estimate_stage_id",
                                "outlier")
missing_d00_compiled_deaths <- anti_join(d00_compiled_deaths_297, d00_compiled_deaths_new, by = d00_compiled_deaths_IDcols)
missing_d00_compiled_deaths <- missing_d00_compiled_deaths[deaths_nid %in% missing_nids | (deaths_nid==140966 & ihme_loc_id == "MYS")]

# proportional split to lower age groups from DATUM format
dt_pop_postneonatal_wide <- dcast(
  dt_pop_postneonatal,
  location_id + year_id + sex_id ~ age_group_id,
  value.var = "prop"
)
dt_pop_1_to_4_wide <- dcast(
  dt_pop_1_to_4,
  location_id + year_id + sex_id ~ age_group_id,
  value.var = "prop"
)

dt_pop_props <- merge(
  dt_pop_postneonatal_wide,
  dt_pop_1_to_4_wide,
  by = c("location_id", "year_id", "sex_id"),
  all = TRUE
)
dt_pop_props <- demInternal::ids_names(dt_pop_props, id_cols = "sex_id")
setnames(dt_pop_props, c("year_id", "sex_name", "388", "389", "34", "238"), c("year", "sex", "prop_388", "prop_389", "prop_34", "prop_238"))
dt_pop_props[sex == "all", sex := "both"]

dt_pop_props <- merge(
  loc_map[, c("location_id", "ihme_loc_id")],
  dt_pop_props,
  by = "location_id",
  all.y = TRUE
)

missing_d00_compiled_deaths <- merge(
  missing_d00_compiled_deaths,
  dt_pop_props[, -c("location_id", "sex_id")],
  by = c("ihme_loc_id", "sex", "year"),
  all.x = TRUE
)
missing_d00_compiled_deaths[, DATUMpnatopna := DATUMpostneonatal * prop_388]
missing_d00_compiled_deaths[, DATUMpnbtopnb := DATUMpostneonatal * prop_389]
missing_d00_compiled_deaths[, DATUM12to23months := DATUM1to4 * prop_238]
missing_d00_compiled_deaths[, DATUM2to4 := DATUM1to4 * prop_34]
missing_d00_compiled_deaths[, ':=' (prop_388 = NULL, prop_389 = NULL, prop_34 = NULL, prop_238 = NULL)]

d00_compiled_deaths_new <- rbind(d00_compiled_deaths_new, missing_d00_compiled_deaths, fill = T) #fills in obscure missing datum columns

# check for duplicates in VR
d00_compiled_deaths_new_dedup <- copy(d00_compiled_deaths_new)
d00_compiled_deaths_new_dedup[, dup := .N, by = d00_compiled_deaths_IDcols]

# remove dups
d00_compiled_deaths_new_dedup[(dup == 2 & deaths_source == "DYB" & outlier == 0), outlier := 1]
d00_compiled_deaths_new_dedup[, dup := .N, by = d00_compiled_deaths_IDcols]
d00_compiled_deaths_new_dedup[(dup == 2 & deaths_source != "WHO" & outlier == 0), outlier := 1]

# manually drop WHO MNE
d00_compiled_deaths_new_dedup[ihme_loc_id == "MNE" & deaths_nid == 287600 & year %in% c(1971, 1981), outlier := 1]

# Aggregate GBR 2022 - not needed in cod or noncod no shocks since this is
#         a mix of sources
gbr_subnats <- d00_compiled_deaths_new_dedup[ihme_loc_id %in% c("GBR_434", "GBR_433", "GBR_4749", "GBR_4636") & year == 2022 & outlier == 0]
gbr_subnats[, ':=' (ihme_loc_id = "GBR", deaths_source = "United Kingdom Vital Registration Deaths 2022", deaths_nid = 553082)]
datum_cols <- grep("DATUM", names(gbr_subnats), value = TRUE)
gbr_subnats <- gbr_subnats[, lapply(.SD, sum), by = setdiff(names(gbr_subnats), datum_cols)]
d00_compiled_deaths_new_dedup <- rbind(d00_compiled_deaths_new_dedup, gbr_subnats)

# AGGREGATE UKR AND REPLACE
d00_ukr <- d00_compiled_deaths_new_dedup[ihme_loc_id %in% paste0("UKR_",ukr_subnats) & year %in% use_agge_years_ukr & outlier == 0]
d00_ukr[year == 2015, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 311356, deaths_underlying_nid = "")]
d00_ukr[year == 2016, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 311356, deaths_underlying_nid = "")]
d00_ukr[year == 2017, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 367033, deaths_underlying_nid = "")]
d00_ukr[year == 2018, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 367033, deaths_underlying_nid = "")]
d00_ukr[year == 2019, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 367033, deaths_underlying_nid = "")]
d00_ukr[year == 2020, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 367033, deaths_underlying_nid = "")]
d00_ukr[year == 2021, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 428458, deaths_underlying_nid = "")]
d00_ukr[, ihme_loc_id := "UKR"]
d00_ukr <- d00_ukr[, lapply(.SD, sum),
               by = c("ihme_loc_id", "country", "sex", "year", "deaths_source",
                      "source_type_id", "deaths_footnote", "deaths_nid", "estimate_stage_id",
                      "deaths_underlying_nid", "outlier", "dup")]
assertable::assert_values(
  d00_ukr,
  colnames = "estimate_stage_id",
  test = "in",
  test_val = 21:22
)
d00_ukr[ihme_loc_id == "UKR" & deaths_nid %in% c(333795, 504648, 541896), outlier := 1]
d00_compiled_deaths_new_dedup[ihme_loc_id == "UKR" & year %in% c(2017, use_agge_years_ukr), outlier := 1]
d00_compiled_deaths_new_dedup <- rbind(d00_compiled_deaths_new_dedup, d00_ukr)

# check for dups
d00_compiled_deaths_new_dedup[, dup := .N, by = d00_compiled_deaths_IDcols]
assert_values(d00_compiled_deaths_new_dedup[outlier==0], "dup", test="equal", test_val = 1)
d00_compiled_deaths_new_dedup[, dup := NULL]

write_dta(d00_compiled_deaths_new_dedup, fs::path())


# UKR no shocks ----------------------------------------------------------------

# read in and combine noncod (ukraine (without crimea & sevastopol)) and cod (crimea & sevastopol)
noncod_ukr <- noncod_VR_no_shocks_new[location_id %in% ukr_subnats & year_id %in% use_agge_years_ukr]
cod_ukr <- cod_VR_no_shocks_new[location_id %in% ukr_subnats & year_id %in% use_agge_years_ukr]
no_shocks_ukr <- rbind(cod_ukr, noncod_ukr)

# mismatched nid/source but aligns with agg_vr_national.py
no_shocks_ukr[year_id == 2015, ':=' (source = "WHO_causesofdeath", nid = 311356, underlying_nid = NA)]
no_shocks_ukr[year_id == 2016, ':=' (source = "WHO_causesofdeath", nid = 311356, underlying_nid = NA)]
no_shocks_ukr[year_id == 2017, ':=' (source = "WHO_causesofdeath", nid = 367033, underlying_nid = NA)]
no_shocks_ukr[year_id == 2018, ':=' (source = "WHO_causesofdeath", nid = 367033, underlying_nid = NA)]
no_shocks_ukr[year_id == 2019, ':=' (source = "WHO_causesofdeath", nid = 367033, underlying_nid = NA)]
no_shocks_ukr[year_id == 2020, ':=' (source = "WHO_causesofdeath", nid = 367033, underlying_nid = NA)]
no_shocks_ukr[year_id == 2021, ':=' (source = "WHO_causesofdeath", nid = 428458, underlying_nid = NA)]
no_shocks_ukr <- no_shocks_ukr[, .(deaths = sum(deaths), location_id = 63),
              by = c("age_group_id","sex_id", "source_type_id", "year_id",
                     "source", "nid", "underlying_nid", "estimate_stage_id")]

# remove from cod and noncod
noncod_VR_no_shocks_new <- noncod_VR_no_shocks_new[!(location_id == 63 & year_id %in% c(2017, use_agge_years_ukr))]
cod_VR_no_shocks_new <- cod_VR_no_shocks_new[!(location_id == 63 & year_id %in% c(2017, use_agge_years_ukr))]

# adding to cod since I used those nids, but technically it is both
cod_VR_no_shocks_new <- rbind(cod_VR_no_shocks_new, no_shocks_ukr)

# check each location only has 1 source
cod_VR_no_shocks_new_dedup <- copy(cod_VR_no_shocks_new)
cod_VR_no_shocks_new_dedup[, sources := length(unique(nid)), by = c("location_id", "year_id", "source_type_id", "estimate_stage_id")]
assert_values(cod_VR_no_shocks_new_dedup, "sources", test="equal", test_val = 1)

readr::write_csv(cod_VR_no_shocks_new, fs::path())


# noncod_VR_no_shocks ----------------------------------------------------------

noncod_VR_no_shocks_IDcols <- setdiff(names(noncod_VR_no_shocks_new), c("deaths","nid","source","underlying_nid"))
missing_noncod_VR_no_shocks <- anti_join(noncod_VR_no_shocks_297, noncod_VR_no_shocks_new, by = noncod_VR_no_shocks_IDcols)
missing_noncod_VR_no_shocks <- missing_noncod_VR_no_shocks[nid %in% missing_nids | (nid==140966 & location_id == 13) | (nid==400963 & year_id==2018)]
noncod_VR_no_shocks_new <- rbind(noncod_VR_no_shocks_new, missing_noncod_VR_no_shocks)

# check for duplicates in VR
noncod_VR_no_shocks_new_dedup <- copy(noncod_VR_no_shocks_new)
noncod_VR_no_shocks_new_dedup[, dup := .N, by = noncod_VR_no_shocks_IDcols]

# remove dups
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(dup == 2 & source == "DYB")]
noncod_VR_no_shocks_new_dedup[, dup := .N, by = noncod_VR_no_shocks_IDcols]
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(dup == 2 & source != "WHO")]

# manually drop WHO MNE
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(location_id == 50 & nid == 287600 & year_id %in% c(1971, 1981))]
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(location_id == 50 & nid == 325106 & year_id %in% c(2014, 2016))]

# manually drop GBR_44656 nid 488437 year id 2020
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(location_id == 44656 & nid == 488437 & year_id %in% c(2020))]

# manually replace Eurostat ITA
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(nid %in% c(494014, 494866) & location_id == 86)]
noncod_VR_no_shocks_new_dedup <- rbind(noncod_VR_no_shocks_new_dedup, noncod_VR_no_shocks_308[(nid == 494014 & location_id == 86)], fill = TRUE)

# manually replace LUX 2018
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(nid %in% c(287600) & location_id == 87 & year_id == 2018)]
noncod_VR_no_shocks_new_dedup <- rbind(noncod_VR_no_shocks_new_dedup, noncod_VR_no_shocks_297[(nid == 400963 & year_id == 2018)], fill = TRUE)

# manually replace ITA 2019
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(nid %in% c(477112, 140966) & location_id == 86 & year_id == 2019)]
noncod_VR_no_shocks_new_dedup <- rbind(noncod_VR_no_shocks_new_dedup, noncod_VR_no_shocks_297[nid==477112 & location_id == 86 & year_id == 2019], fill=T)

# manually drop ITA 2019 - using cod
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(location_id == 98 & year_id == 2019)]

# check for dups
noncod_VR_no_shocks_new_dedup[, dup := .N, by = noncod_VR_no_shocks_IDcols]
assert_values(noncod_VR_no_shocks_new_dedup, "dup", test="equal", test_val = 1)
noncod_VR_no_shocks_new_dedup[, dup := NULL]

# check each location only has 1 source
noncod_VR_no_shocks_new_dedup[, sources := length(unique(nid)), by = c("location_id", "year_id", "source_type_id", "estimate_stage_id")]
assert_values(noncod_VR_no_shocks_new_dedup, "sources", test="equal", test_val = 1)

# remove extra sources
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(sources == 2 & source == "DYB")]
noncod_VR_no_shocks_new_dedup[, sources := length(unique(nid)), by = c("location_id", "year_id", "source_type_id", "estimate_stage_id")]
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(sources == 2 & source != "WHO")]

noncod_VR_no_shocks_new_dedup[, sources := length(unique(nid)), by = c("location_id", "year_id", "source_type_id", "estimate_stage_id")]
assert_values(noncod_VR_no_shocks_new_dedup, "sources", test="equal", test_val = 1)
noncod_VR_no_shocks_new_dedup[, sources := NULL]

readr::write_csv(noncod_VR_no_shocks_new_dedup, fs::path())

