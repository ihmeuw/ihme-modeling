# splice missing data into VRP runs

# This removes changes to GBR 2017 since we have updated this now

# Set up -----------------------------------------------------------------------

library(assertable)
library(data.table)
library(dplyr)
library(haven)

dir_base <- 
run_id_new <- 


# Make backups ------------------------------------------------------------

backup_files_from <- c()
backup_files_to <- paste0(tools::file_path_sans_ext(backup_files_from), "", tools::file_ext(backup_files_from))

fs::file_copy(
  fs::path(),
  fs::path(),
  overwrite = FALSE
)


# Load -------------------------------------------------------------------------

missing_nids <- c(325366, 325032, 325025, 52940, 52840, 121922, 237659)

empirical_deaths_297 <- fread(fs::path(dir_base, 297, ""))
empirical_deaths_308 <- fread(fs::path(dir_base, 308, ""))
empirical_deaths_new <- fread(fs::path(dir_base, run_id_new, ""))

d00_compiled_deaths_297 <- setDT(read_dta(fs::path(dir_base, 297, "")))
d00_compiled_deaths_308 <- setDT(read_dta(fs::path(dir_base, 308,"")))
d00_compiled_deaths_new <- setDT(read_dta(fs::path(dir_base, run_id_new, "")))

noncod_VR_no_shocks_297 <- fread(fs::path(dir_base, 297, ""))
noncod_VR_no_shocks_308 <- fread(fs::path(dir_base, 308, ""))
noncod_VR_no_shocks_new <- fread(fs::path(dir_base, run_id_new, ""))

cod_VR_no_shocks_308 <- fread(fs::path(dir_base, 308, ""))
cod_VR_no_shocks_new <- fread(fs::path(dir_base, run_id_new, ""))

use_agge_years_ukr <- c(2008, 2009, 2012, 2015, 2016, 2018, 2019, 2020)
ukr_subnats <- c()

# empirical deaths -------------------------------------------------------------

# append
empirical_deaths_IDcols <- setdiff(names(empirical_deaths_new), c("mean","nid","detailed_source","underlying_nid"))
missing_empirical_deaths <- anti_join(empirical_deaths_297, empirical_deaths_new, by = empirical_deaths_IDcols)
missing_empirical_deaths <- missing_empirical_deaths[(nid %in% missing_nids) | (nid==140966 & location_id==13)]
empirical_deaths_new <- rbind(empirical_deaths_new, missing_empirical_deaths)

# manually outlier WHO LUX 2018
lux_2018 <- empirical_deaths_297[(nid==400963 & year_id==2018)]
empirical_deaths_new <- rbind(empirical_deaths_new, lux_2018)
empirical_deaths_new[nid==287600 & location_id==87 & year_id==2018, outlier := 1]

# manually replace ITA 2019
empirical_deaths_new <- empirical_deaths_new[!(location_id == 86 & year_id == 2019)]
empirical_deaths_new <- rbind(empirical_deaths_new, empirical_deaths_297[location_id == 86 & year_id == 2019])

# manually replace CHL 2019
empirical_deaths_new[(location_id == 98 & year_id == 2019), outlier := 1]
empirical_deaths_new <- rbind(empirical_deaths_new, empirical_deaths_308[location_id == 98 & year_id == 2019])

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
readr::write_csv(empirical_deaths_new_dedup, fs::path(dir_base, run_id_new, ""))


# d00_compiled_deaths ----------------------------------------------------------

d00_compiled_deaths_IDcols <- c("ihme_loc_id","country","sex","year",
                                "source_type_id","deaths_footnote",
                                "outlier")
missing_d00_compiled_deaths <- anti_join(d00_compiled_deaths_297, d00_compiled_deaths_new, by = d00_compiled_deaths_IDcols)
missing_d00_compiled_deaths <- missing_d00_compiled_deaths[deaths_nid %in% missing_nids | (deaths_nid==140966 & ihme_loc_id == "MYS")]
d00_compiled_deaths_new <- rbind(d00_compiled_deaths_new, missing_d00_compiled_deaths, fill = T) #fills in obscure missing datum columns

# manually outlier WHO LUX 2018
lux_2018_2 <- d00_compiled_deaths_297[deaths_nid == 400963 & year == 2018]
d00_compiled_deaths_new <- rbind(d00_compiled_deaths_new, lux_2018_2, fill = T) #fills in obscure missing datum columns
d00_compiled_deaths_new[deaths_nid==287600 & ihme_loc_id=="LUX" & year==2018, outlier := 1]

# manually replace ITA 2019
d00_compiled_deaths_new <- d00_compiled_deaths_new[!(ihme_loc_id == "ITA" & year == 2019)]
d00_compiled_deaths_new <- rbind(d00_compiled_deaths_new, d00_compiled_deaths_297[ihme_loc_id == "ITA" & year == 2019], fill=T)

# manually replace ITA 2019
d00_compiled_deaths_new <- d00_compiled_deaths_new[!(ihme_loc_id == "CHL" & year == 2019)]
d00_compiled_deaths_new <- rbind(d00_compiled_deaths_new, d00_compiled_deaths_308[ihme_loc_id == "CHL" & year == 2019], fill=T)

# check for duplicates in VR
d00_compiled_deaths_new_dedup <- copy(d00_compiled_deaths_new)
d00_compiled_deaths_new_dedup[, dup := .N, by = d00_compiled_deaths_IDcols]

# remove dups
d00_compiled_deaths_new_dedup[(dup == 2 & deaths_source == "DYB" & outlier == 0), outlier := 1]
d00_compiled_deaths_new_dedup[, dup := .N, by = d00_compiled_deaths_IDcols]
d00_compiled_deaths_new_dedup[(dup == 2 & deaths_source != "WHO" & outlier == 0), outlier := 1]

# manually drop WHO MNE
d00_compiled_deaths_new_dedup[ihme_loc_id == "MNE" & deaths_nid == 287600 & year %in% c(1971, 1981), outlier := 1]

# AGGREGATE UKR AND REPLACE
d00_ukr <- d00_compiled_deaths_new_dedup[ihme_loc_id %in% paste0("UKR_",ukr_subnats) & year %in% use_agge_years_ukr]
d00_ukr[year == 2015, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 311356, deaths_underlying_nid = "")]
d00_ukr[year == 2016, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 311356, deaths_underlying_nid = "")]
d00_ukr[year == 2018, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 367033, deaths_underlying_nid = "")]
d00_ukr[year == 2019, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 367033, deaths_underlying_nid = "")]
d00_ukr[year == 2020, ':=' (deaths_source = "WHO_causesofdeath", deaths_nid = 367033, deaths_underlying_nid = "")]
d00_ukr[, ihme_loc_id := "UKR"]
d00_ukr <- d00_ukr[, lapply(.SD, sum),
               by = c("ihme_loc_id", "country", "sex", "year", "deaths_source",
                      "source_type_id", "deaths_footnote", "deaths_nid",
                      "deaths_underlying_nid", "outlier", "dup")]
d00_compiled_deaths_new_dedup[ihme_loc_id == "UKR" & year %in% c(2017, use_agge_years_ukr), outlier := 1]
d00_compiled_deaths_new_dedup <- rbind(d00_compiled_deaths_new_dedup, d00_ukr)

# check for dups
d00_compiled_deaths_new_dedup[, dup := .N, by = d00_compiled_deaths_IDcols]
assert_values(d00_compiled_deaths_new_dedup[outlier==0], "dup", test="equal", test_val = 1)
d00_compiled_deaths_new_dedup[, dup := NULL]

write_dta(d00_compiled_deaths_new_dedup, fs::path(dir_base, run_id_new, ""))


# UKR no shocks ----------------------------------------------------------------

# read in and combine noncod (ukraine (without crimea & sevastopol)) and cod (crimea & sevastopol)
noncod_ukr <- noncod_VR_no_shocks_new[location_id %in% ukr_subnats & year_id %in% use_agge_years_ukr]
cod_ukr <- cod_VR_no_shocks_new[location_id %in% ukr_subnats & year_id %in% use_agge_years_ukr]
no_shocks_ukr <- rbind(cod_ukr, noncod_ukr)

# bad nid/source but aligns with agg_vr_national.py
no_shocks_ukr[year_id == 2015, ':=' (source = "WHO_causesofdeath", nid = 311356, underlying_nid = NA)]
no_shocks_ukr[year_id == 2016, ':=' (source = "WHO_causesofdeath", nid = 311356, underlying_nid = NA)]
no_shocks_ukr[year_id == 2018, ':=' (source = "WHO_causesofdeath", nid = 367033, underlying_nid = NA)]
no_shocks_ukr[year_id == 2019, ':=' (source = "WHO_causesofdeath", nid = 367033, underlying_nid = NA)]
no_shocks_ukr[year_id == 2020, ':=' (source = "WHO_causesofdeath", nid = 367033, underlying_nid = NA)]
no_shocks_ukr <- no_shocks_ukr[, .(deaths = sum(deaths), location_id = 63),
              by = c("age_group_id","sex_id", "source_type_id", "year_id",
                     "source", "nid", "underlying_nid")]

# remove from cod and noncod
noncod_VR_no_shocks_new <- noncod_VR_no_shocks_new[!(location_id == 63 & year_id %in% c(2017, use_agge_years_ukr))]
cod_VR_no_shocks_new <- cod_VR_no_shocks_new[!(location_id == 63 & year_id %in% c(2017, use_agge_years_ukr))]

# adding to cod since I used those nids, but technically it is both
cod_VR_no_shocks_new <- rbind(cod_VR_no_shocks_new, no_shocks_ukr)

# check each location only has 1 source
cod_VR_no_shocks_new_dedup <- copy(cod_VR_no_shocks_new)
cod_VR_no_shocks_new_dedup[, sources := length(unique(nid)), by = c("location_id", "year_id", "source_type_id")]
assert_values(cod_VR_no_shocks_new_dedup, "sources", test="equal", test_val = 1)

readr::write_csv(cod_VR_no_shocks_new, fs::path(dir_base, run_id_new, ""))


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
noncod_VR_no_shocks_new_dedup[, sources := length(unique(nid)), by = c("location_id", "year_id", "source_type_id")]
assert_values(noncod_VR_no_shocks_new_dedup, "sources", test="equal", test_val = 1)

# remove extra sources
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(sources == 2 & source == "DYB")]
noncod_VR_no_shocks_new_dedup[, sources := length(unique(nid)), by = c("location_id", "year_id", "source_type_id")]
noncod_VR_no_shocks_new_dedup <- noncod_VR_no_shocks_new_dedup[!(sources == 2 & source != "WHO")]

noncod_VR_no_shocks_new_dedup[, sources := length(unique(nid)), by = c("location_id", "year_id", "source_type_id")]
assert_values(noncod_VR_no_shocks_new_dedup, "sources", test="equal", test_val = 1)
noncod_VR_no_shocks_new_dedup[, sources := NULL]

readr::write_csv(noncod_VR_no_shocks_new_dedup, fs::path(dir_base, run_id_new, ""))

