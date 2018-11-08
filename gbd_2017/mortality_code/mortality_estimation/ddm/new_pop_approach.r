# Author: 
# Date: 10/27/17
# Purpose: 1) Pull Empirical Population data from mortality database
#          2) Merge in with GBD2016 pop data
#          3) Drop duplicate data points
#          4) Save in /ihme/mortality/ddm/{version_id}/inputs/

rm(list=ls())
library(data.table); library(haven); library(readr); library(readstata13); library(assertable); library(plyr); library(DBI); library(mortdb, lib = "FILEAPTH"); library(mortcore, lib = "FILEPATH")

if (Sys.info()[1] == "Linux") {
  root <- "/home/j" 
  user <- Sys.getenv("USER")
  version_id <- as.character(commandArgs(trailingOnly = T)[1])
} else {
  root <- "J:"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("C:/Users/",user,"FILEPATH")
}
version_id <- 63
save_new_compiled <- T
gbd_year <- 2017
comp_version_id <- 62

if (version_id == 1){
  version_id <- gen_new_version(model_name = "population empirical",
                                model_type = "data",
                                comment = "GBD2017- RUS subnational fixex")
} 


dir.create(file.path("FILEPATH", paste0("v", version_id)), showWarnings = F)

# Pull location metadata
ap_old <- data.table(get_locations(gbd_type = 'ap_old', level = 'estimate', gbd_year = gbd_year))
ap_old <- ap_old[location_name == "Old Andhra Pradesh"]
locations <- data.table(get_locations(gbd_year = gbd_year))
locations <- rbind(locations, ap_old)
locations <- locations[, list(ihme_loc_id, location_name,location_id)]


population <- fread("FILEPATH")
population <- merge(population, locations[, list(ihme_loc_id, location_id)], by = 'location_id', all.x = T)
population[, dupvar := NULL]
population <- population[sex_id != -1]
population <- unique(population)


pop_metadata <- fread("FILEPATH")
pop_metadata[record_type != "registry", source_type := "CENSUS"]
pop_metadata[record_type == "registry", source_type := "pop_registry"]
pop_metadata[is.na(record_type), source_type := "CENSUS"]
population <- merge(population, pop_metadata[, list(ihme_loc_id, year_id, source_type, detailed_source = source, status, age_groups)],
                    by = c('ihme_loc_id', 'year_id', 'source_type', 'detailed_source', 'status'), all.x = T)
population[, outlier := 0]
population[age_groups == 1, outlier := 1]
all_age_only_pop <- copy(population[age_groups == 1])
all_age_only_pop[sex_id == 1, sex := "male"]
all_age_only_pop[sex_id == 2, sex := "female"]
all_age_only_pop[sex_id == 3, sex := "both"]
all_age_only_pop <- all_age_only_pop[, list(ihme_loc_id, year = year_id, source_type, pop_source = detailed_source, sex, outlier, all_age_only = 1)]
population[, age_groups := NULL]


tmp <- (population[grepl("IPUMS", detailed_source) & grepl("plus", age_group_name)])
tmp <- tmp[, list(ihme_loc_id, sex_id, year_id, age_group_name, source_type, detailed_source, status, nid, mean)]
tmp_reshape <- dcast.data.table(tmp, ihme_loc_id + status + year_id + source_type + detailed_source + age_group_name + nid ~ sex_id, value.var = "mean", fill = NA_real_)
setnames(tmp_reshape, c("1", "2", "3"), c("male", "female", "both"))
tmp_reshape <- tmp_reshape[, list(ihme_loc_id, year_id, source_type, detailed_source, age_group_name, nid, status, male, female, both)]
missing_ipums <- tmp_reshape[(male == 0 & female != 0) | (female == 0 & male != 0)]


write_csv(missing_ipums, paste0("FILEPATH", version_id, "FILPATH"))


population <- merge(population, tmp_reshape[(male == 0 & female != 0) | (female == 0 & male != 0), list(ihme_loc_id, year_id, source_type, detailed_source, status, drop = 1)],
                    by = c('ihme_loc_id', 'year_id', 'source_type', 'detailed_source', 'status'),
                    all.x =T)
population[drop == 1, outlier := 1]
population[, drop := NULL]


population[(ihme_loc_id == "DNK" & detailed_source == "STATBANK DENMARK"), source_type := "pop_registry"]


population[(ihme_loc_id == "NGA" & source_type == "CENSUS" & year_id == 1991), outlier := 1]


population[, tmp := seq(.N), by = c('ihme_loc_id', 'year_id', 'detailed_source', 'nid', 'underlying_nid', 'age_group_id', 'outlier')]
population[, max := max(tmp), by = c('ihme_loc_id', 'year_id', 'detailed_source', 'nid', 'underlying_nid', 'age_group_id', 'outlier')]
missing_data <- unique(population[max ==2, list(ihme_loc_id, year_id, detailed_source, outlier, drop = 1)])
population <- merge(population, missing_data, by = c('ihme_loc_id', 'year_id', 'detailed_source', 'outlier'), all.x = T)
population[is.na(drop), drop := 0]
population[drop == 1, outlier := 1]
population[, drop := NULL]


population <- merge(population, locations[, list(location_id, location_name)], by = 'location_id', all.x = T)


population[ihme_loc_id == "CHN_44533" & year_id == 1981, year_id := 1982]
population[grepl("CHN_", ihme_loc_id) & year_id == 1989, year_id := 1990]


population[, precise_year := year_id + .5]


new_pop <- copy(population)


new_pop <- new_pop[ihme_loc_id == "CHN_44533", location_name := "China (without Hong Kong and Macao)"]


ages <- data.table(get_age_map('all'))
ages[, age_group_years_end := age_group_years_end - 1] 
ages[, start := as.character(age_group_years_start)]
ages[, end := as.character(age_group_years_end)]
ages[grepl("\\-", age_group_name), age_group_name := gsub("-", "to", age_group_name)]
ages[grepl("years", age_group_name), age_group_name := gsub("years", "", age_group_name)]

ages[, age_gap := paste0(start, "to", end)]
ages[grepl("plus", age_group_name), age_gap := paste0(start,"plus")]
ages[!grepl("to", age_group_name) & !grepl("plus", age_group_name) & !grepl("\\-", age_group_name), age_gap := paste0(start, "to", start)]
ages[, age_gap := paste0("@", age_gap)]


ages[age_group_id == 283, age_gap := "@UNK"]
ages[age_group_id == 236, age_gap := "@0to4"]
ages[age_group_id == 356, age_gap := "@120plus"]
ages[age_group_id == 26, age_gap := "@70plus"]
ages[age_group_id == 29, age_gap := "@15plus"]
ages[age_group_id == 22, age_gap := "@TOT"]
ages[age_group_id == 28, age_gap := "@0to1"]
ages <-ages[, list(age_group_id, age_gap)]

new_pop <- merge(new_pop, ages, by = 'age_group_id', all.x = T)
new_pop[, c('age_group_id') := NULL]


new_pop <- new_pop[, list(ihme_loc_id, location_id, location_name, status, year_id, sex_id, source_type, detailed_source, nid, underlying_nid, precise_year, age_gap, outlier, mean)]
new_pop_all <- dcast.data.table(new_pop, ihme_loc_id + location_name + status + year_id + sex_id + source_type + detailed_source + nid + underlying_nid + precise_year + outlier ~ age_gap, value.var = "mean", fill = NA_real_)


pop_registry_best <- unique(new_pop_all[source_type == "pop_registry" & status == "best", list(ihme_loc_id, year_id, sex_id, pop_registry_best = 1)])


new_pop_all <- merge(new_pop_all, pop_registry_best,
                     by = c('ihme_loc_id', 'year_id', 'sex_id'),
                     all.x = T)
new_pop_all[is.na(pop_registry_best), pop_registry_best := 0]


new_pop_all[status == "excluded", outlier := 1]


new_pop_all[status == 'duplicated' & pop_registry_best != 1, outlier := 1]
new_pop_all[, pop_registry_best := NULL]


 
new_pop_all[outlier == 0 & status != "best" & (duplicated(new_pop_all[, list(ihme_loc_id, year_id, sex_id, source_type, outlier)]) |
                                                 duplicated(new_pop_all[, list(ihme_loc_id, year_id, sex_id, source_type, outlier)], fromLast = TRUE)), dup := 1]
new_pop_all[is.na(dup), dup := 0]


new_pop_all[(dup == 1 & grepl("IPUMS", detailed_source)), outlier := 1]
new_pop_all[, dup := NULL]


new_pop_all[outlier == 0 & status != "best" & (duplicated(new_pop_all[, list(ihme_loc_id, year_id, sex_id, source_type, outlier)]) |
                                                 duplicated(new_pop_all[, list(ihme_loc_id, year_id, sex_id, source_type, outlier)], fromLast = TRUE)), dup := 1]
new_pop_all[is.na(dup), dup := 0]


new_pop_all[(dup == 1 & detailed_source == "Mitchell"), outlier := 1]
new_pop_all[, dup := NULL]


new_pop_all[outlier == 0 & status != "best" & (duplicated(new_pop_all[, list(ihme_loc_id, year_id, sex_id, source_type, outlier)]) |
                                                 duplicated(new_pop_all[, list(ihme_loc_id, year_id, sex_id, source_type, outlier)], fromLast = TRUE)), dup := 1]
new_pop_all[is.na(dup), dup := 0]


new_pop_all[(dup == 1 & detailed_source == "DYB"), outlier := 1]
new_pop_all[, dup := NULL]


dups <- new_pop_all[outlier == 0 & (duplicated(new_pop_all[, list(ihme_loc_id, year_id, sex_id, source_type, outlier)]) |
                                      duplicated(new_pop_all[, list(ihme_loc_id, year_id, sex_id, source_type, outlier)], fromLast = TRUE))]
if (nrow(dups) != 0) stop("There are duplicates in your data.")


setnames(new_pop_all, grep("@", colnames(new_pop_all)), gsub("@", "DATUM", grep("@", names(new_pop_all), value = T)))
setnames(new_pop_all, c("location_name", "nid", "underlying_nid", "detailed_source", "year_id"), c("country", "pop_nid", "underlying_pop_nid", "pop_source", "year"))


new_pop_all[sex_id == 1, sex := "male"]
new_pop_all[sex_id == 2, sex := "female"]
new_pop_all[sex_id == 3, sex := "both"]
new_pop_all[, sex_id := NULL]
new_pop_all[, pop_nid := as.character(pop_nid)]
new_pop_all[, underlying_pop_nid := as.character(underlying_pop_nid)]
new_pop_all[, pop_footnote := ""]

supas_data <- new_pop_all[ihme_loc_id=="IDN" & source_type == "CENSUS"]
supas_data[, source_type := "SUPAS"]
new_pop_all <- data.table(rbind.fill(new_pop_all, supas_data))


new_pop_all[grepl("CYP", ihme_loc_id) & year < 1974 & source_type == "CENSUS", outlier := 1]


new_pop_all[grepl("KWT", ihme_loc_id), outlier := 1]


pop_census_srs <- new_pop_all[ihme_loc_id %in% c("BGD", "PAK") & grepl("CENSUS", source_type)]
pop_census_srs[, source_type := "SRS"]
new_pop_all <- data.table(rbind.fill(new_pop_all, pop_census_srs))
new_pop_all[, new := TRUE]


vut_pop <- setDT(read_dta("FILEPATH"))
vut_pop <- vut_pop[, c("AREA", "COUNTRY") := NULL]
setnames(vut_pop, c("SEX", "VR_SOURCE", "SUBDIV", "NID", "YEAR"), c("sex_id", "pop_source", "source_type", "pop_nid", "year"))
vut_pop[sex_id == 0, sex := "both"]
vut_pop[sex_id == 1, sex := "male"]
vut_pop[sex_id == 2, sex := "female"]
vut_pop[, sex_id := NULL]
vut_pop[, underlying_pop_nid := 106570]
vut_pop[, status := "best"]
vut_pop[, new := TRUE]
vut_pop[, outlier := 0]
vut_pop[, precise_year := 2009.5]
vut_pop <- merge(vut_pop, locations[, list(ihme_loc_id, country = location_name)], all.x = T)
new_pop_all <- data.table(rbind.fill(new_pop_all, vut_pop))


srs_2016_pop <- setDT(read.dta13("FILEPATH"))
setnames(srs_2016_pop, c("COUNTRY", "nid", "YEAR", "CENSUS_SOURCE", "SEX"), c("ihme_loc_id", "pop_nid", "year", "source_type", "sex_id"))
srs_2016_pop[, AREA := NULL]
srs_2016_pop[, pop_source := "SRS"]
srs_2016_pop[, underlying_pop_nid := 331136]
srs_2016_pop[, status := "best"]
srs_2016_pop[, new := TRUE]
srs_2016_pop[, outlier := 0]
srs_2016_pop[, pop_footnote := ""]
srs_2016_pop[, precise_year := 2016.5]


both_sex_srs_2016_pop <- copy(srs_2016_pop)
both_sex_srs_2016_pop[, sex_id := NULL]
datum_cols_ind_srs_pop_2016 <- grep("^DATUM", names(both_sex_srs_2016_pop), value = T)
both_sex_srs_2016_pop <- both_sex_srs_2016_pop[, lapply(.SD, function(x)replace(x, which(is.na(x)), -999)), .SDcols = datum_cols_ind_srs_pop_2016, by = c('ihme_loc_id', 'year', 'source_type', 'pop_source', 'pop_footnote', 'pop_nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]

both_sex_srs_2016_pop <- both_sex_srs_2016_pop[, lapply(.SD, sum, na.rm = T), .SDcols = grep("^DAT", names(both_sex_srs_2016_pop), value = T), by = c('ihme_loc_id', 'year', 'source_type', 'pop_source', 'pop_footnote', 'pop_nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]
both_sex_srs_2016_pop <- both_sex_srs_2016_pop[, lapply(.SD, function(x) replace(x, which(x<0), NA)), .SDcols = datum_cols_ind_srs_pop_2016, by = c('ihme_loc_id', 'year', 'source_type', 'pop_source', 'pop_footnote', 'pop_nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]
both_sex_srs_2016_pop[, sex_id := 3]

srs_2016_pop <- setDT(rbind(srs_2016_pop, both_sex_srs_2016_pop))
srs_2016_pop[sex_id == 3, sex := "both"]
srs_2016_pop[sex_id == 1, sex := "male"]
srs_2016_pop[sex_id == 2, sex := "female"]
srs_2016_pop[, sex_id := NULL]


old_ap_srs_2016 <- copy(srs_2016_pop)
old_ap_srs_2016 <- old_ap_srs_2016[ihme_loc_id %in% c("IND_4871", "IND_4841")]
old_ap_srs_2016[, ihme_loc_id := NULL]
old_ap_srs_2016_datum_cols <- grep("^DATUM", names(old_ap_srs_2016), value = T)
old_ap_srs_2016 <- old_ap_srs_2016[, lapply(.SD, function(x)replace(x, which(is.na(x)), -999)), .SDcols = old_ap_srs_2016_datum_cols, by = c('sex', 'year', 'source_type', 'pop_source', 'pop_footnote', 'pop_nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]

old_ap_srs_2016 <- old_ap_srs_2016[, lapply(.SD, sum, na.rm = T), .SDcols = grep("^DAT", names(old_ap_srs_2016), value = T), by = c('sex', 'year', 'source_type', 'pop_source', 'pop_footnote', 'pop_nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]
old_ap_srs_2016 <- old_ap_srs_2016[, lapply(.SD, function(x) replace(x, which(x<0), NA)), .SDcols = old_ap_srs_2016_datum_cols, by = c('sex', 'year', 'source_type', 'pop_source', 'pop_footnote', 'pop_nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]
old_ap_srs_2016[, ihme_loc_id := "IND_44849"]
srs_2016_pop <- data.table(rbind(srs_2016_pop, old_ap_srs_2016))
new_pop_all <- data.table(rbind.fill(new_pop_all, srs_2016_pop))
rm(both_sex_srs_2016_pop, old_ap_srs_2016)


new_pop_all[(grepl("IRN_", ihme_loc_id) & source_type == "CENSUS" & year %in% c(1956, 1966, 1976, 1986)), outlier := 1]

if (save_new_compiled) save.dta13(new_pop_all, paste0("FILEPATH"))


tmp_pop <- copy(new_pop_all)
tmp_pop <- tmp_pop[, c('ihme_loc_id', 'country', 'year', 'sex', 'source_type', 'pop_source', 'precise_year', 'pop_nid', 'underlying_pop_nid', 'outlier', grep("DATUM", names(tmp_pop), value = T)), with = F]
comp_pop <- setDT(read_dta(paste0("FILEPATH")))
test <- merge(comp_pop[outlier == 0 & source_type %in% c('CENSUS', 'pop_registry'), list(ihme_loc_id, year, sex, source_type, pop_source, outlier)], 
              tmp_pop[outlier == 0 & source_type %in% c('CENSUS', 'pop_registry'), list(ihme_loc_id, year, sex, source_type, outlier, tmp = 1)],
              by = c('ihme_loc_id', 'year', 'sex', 'source_type', 'outlier'),
              all.x = T)


test <- merge(test[is.na(tmp)], all_age_only_pop, by = c('ihme_loc_id', 'year', 'source_type', 'pop_source', 'sex'), all.x = T)
test[is.na(all_age_only), all_age_only := 0]
test <- test[all_age_only != 1]
if (nrow(test) > 0){
  warning("Not all data from previous upload in the current upload. Please check to make sure this is expected.")

}


pop_metadata <- fread("FILEPATH")
pop_metadata[record_type != "registry", source_type := "CENSUS"]
pop_metadata[record_type == "registry", source_type := "pop_registry"]
pop_metadata[is.na(record_type), source_type := "CENSUS"]
tmp <- merge(test[is.na(tmp) & sex == "both"], 
             pop_metadata[, list(ihme_loc_id, year = year_id, source_type, pop_source = source, status)],
             by = c('ihme_loc_id', 'year', 'source_type', 'pop_source'),
             all.x = T)
tmp <- merge(tmp, locations[, list(ihme_loc_id, location_name)], by = "ihme_loc_id", all.x = T)
write_csv(tmp[, list(ihme_loc_id, location_name, year, source_type, pop_source, status)], paste0("FILEPATH"))



pop_gbd2016 <- data.table(read_dta("FILEPATH"))


pop_gbd2016[, new := FALSE]
pop_gbd2016[, outlier := 0]
pop_gbd2016[ihme_loc_id == "CHN_44533", country := "China (without Hong Kong and Macao)"]


pop_gbd2016[ihme_loc_id == "CHN_44533" & year == 1981 & source_type == "CENSUS", year := 1982]
pop_gbd2016[grepl("CHN_", ihme_loc_id) & year == 1989 & source_type == "CENSUS", year := 1990]


pop_gbd2016[ihme_loc_id== "IRN" & year %in% c(1991, 1994) & source_type == "CENSUS", outlier := 1]
pop_gbd2016 <- pop_gbd2016[!grepl("SAU_", ihme_loc_id)]


pop_gbd2016[grepl("BRA_", ihme_loc_id) & ihme_loc_id != "BRA" & pop_source == "BRA_VR" & year %in% c(1970, 1980, 1991, 2000, 2010), source_type := "CENSUS"]


pop_gbd2016[ihme_loc_id == "BGD" & source_type == "DSP", source_type := "SRS"]


pop_gbd2016[ihme_loc_id == "GRL" & pop_source == "GRL_CENSUS", source_type := "pop_registry"]


pop_gbd2016[ihme_loc_id == "TUR" & source_type == "VR", source_type := "pop_registry"]
pop_gbd2016[ihme_loc_id == "AND" & source_type == "VR", source_type := "pop_registry"]
pop_gbd2016[grepl("SWE", ihme_loc_id) & source_type == "VR", source_type := "pop_registry"]



ind_srs_new <- setDT(read.dta("FILEPATH"))
setnames(ind_srs_new, c("COUNTRY", "SEX", "CENSUS_SOURCE", "YEAR"), c("ihme_loc_id", "sex_id", "source_type", "year"))
ind_srs_new[, c("AREA", "nid") := NULL]
ind_srs_new[, pop_source := source_type]
both_sex_ind_srs <- copy(ind_srs_new)
both_sex_ind_srs[, sex_id := NULL]
datum_cols_ind_srs_pop <- grep("^DATUM", names(both_sex_ind_srs), value = T)
both_sex_ind_srs <- both_sex_ind_srs[, lapply(.SD, function(x)replace(x, which(is.na(x)), -999)), .SDcols = datum_cols_ind_srs_pop, by = c('ihme_loc_id', 'year', 'source_type', 'pop_source')]

both_sex_ind_srs <- both_sex_ind_srs[, lapply(.SD, sum, na.rm = T), .SDcols = grep("^DAT", names(both_sex_ind_srs), value = T), by = c('ihme_loc_id', 'year', 'source_type', 'pop_source')]
both_sex_ind_srs <- both_sex_ind_srs[, lapply(.SD, function(x) replace(x, which(x<0), NA)), .SDcols = datum_cols_ind_srs_pop, by = c('ihme_loc_id', 'year', 'source_type', 'pop_source')]
both_sex_ind_srs[, sex_id := 3]
ind_srs_new <- setDT(rbind(ind_srs_new, both_sex_ind_srs))
ind_srs_new <- merge(ind_srs_new, locations[, list(ihme_loc_id, country = location_name)], by = 'ihme_loc_id', all.x = T)
ind_srs_new[, outlier := 0]
ind_srs_new[sex_id == 3, sex := "both"]
ind_srs_new[sex_id == 1, sex := "male"]
ind_srs_new[sex_id == 2, sex := "female"]
ind_srs_new[, sex_id := NULL]



ind_srs_new <- ind_srs_new[!(ihme_loc_id %in% c("IND_4871", "IND_43872", "IND_43902", "IND_43908", "IND_43938") & !year %in% c(2014, 2015))]
ind_srs_new[ihme_loc_id == "IND_4841" & !year %in% c(2014, 2015), ihme_loc_id := "IND_44849"]


tmp_pop_gbd2016 <- setDT(rbind(pop_gbd2016, ind_srs_new, fill= T))
tmp_pop_gbd2016[, dup:= 0]
tmp_pop_gbd2016[grepl("SRS", source_type), source_type := "SRS"]
tmp_pop_gbd2016[(duplicated(tmp_pop_gbd2016[, list(ihme_loc_id, year, sex, source_type)]) |
                   duplicated(tmp_pop_gbd2016[, list(ihme_loc_id, year, sex, source_type)], fromLast = TRUE)), dup := 1] 
srs_nids <- unique(tmp_pop_gbd2016[dup == 1 & !is.na(pop_nid), list(year, pop_nid, month, day)])
ind_srs_new <- merge(ind_srs_new, srs_nids, by = 'year', all.x = T)


ind_srs_new <- ind_srs_new[ihme_loc_id %in% c("IND_43877", "IND_43885", "IND_43913", "IND_43921", "IND_4846", "IND_4854")]
ind_srs_new[, pop_footnote := ""]
ind_srs_new[, srs_new := 1]


pop_gbd2016 <- setDT(rbind(pop_gbd2016, ind_srs_new, fill = T))
pop_gbd2016[is.na(srs_new), srs_new := 0]
pop_gbd2016[grepl("SRS", source_type), source_type := "SRS"]
pop_gbd2016[(duplicated(pop_gbd2016[, list(ihme_loc_id, year, sex, source_type)]) |
               duplicated(pop_gbd2016[, list(ihme_loc_id, year, sex, source_type)], fromLast = TRUE)), dup := 1] 
pop_gbd2016[is.na(dup), dup := 0]
pop_gbd2016 <- pop_gbd2016[!(dup == 1 & srs_new == 1)]
pop_gbd2016[, c("dup", "srs_new") := NULL]


pop_gbd2016[, precise_year := year + (month/12) + (day/365)]
pop_gbd2016[, precise_year := as.character(precise_year)]
pop_gbd2016[, month := NULL]
pop_gbd2016[, day := NULL]


pop <- data.table(rbind.fill(new_pop_all, pop_gbd2016))


pop[ihme_loc_id == "JOR" & year == 1961 & source_type == "CENSUS", outlier := 1]


pop[ihme_loc_id == "SAU" & year == 2007 & source_type == "CENSUS", outlier := 1]

pop[outlier == 0 & (duplicated(pop[, list(ihme_loc_id, year, sex, source_type, outlier)]) | 
                      duplicated(pop[, list(ihme_loc_id, year, sex, source_type, outlier)], fromLast = TRUE)), dup := 1]
pop[is.na(dup), dup := 0]


pop[new == FALSE & dup == 1, outlier := 1]


dups_pop <- pop[outlier == 0 & (duplicated(pop[, list(ihme_loc_id, year, sex, source_type, outlier)]) | 
                                  duplicated(pop[, list(ihme_loc_id, year, sex, source_type, outlier)], fromLast = TRUE))]
if(nrow(dups_pop) > 0) stop("There are duplicates in your data")


registry_locs <- c("FRA")
for (loc in registry_locs){

  if (loc == "FRA") registry_data <- pop[(ihme_loc_id == loc & source_type == "CENSUS" & year >= 2006)]
  else registry_data <- pop[(ihme_loc_id == loc & source_type == "pop_registry")]
  
  tmp_data_years <- sort(unique(registry_data$year))
  

  min_year <- min(tmp_data_years)
  max_year <- max(tmp_data_years)
  
  use_years <- c()
  next_year <- tmp_data_years[1]
  for (year in tmp_data_years){
    if (year != next_year){
      next 
    }
    i <- match(year, tmp_data_years)
    i_plus <- i + 1
    if (i_plus > length(tmp_data_years)) break
    for (i_plus in i_plus:length(tmp_data_years)){
      current_year <- tmp_data_years[i]
      next_year <- tmp_data_years[i_plus]
      year_diff <- next_year - current_year
      if (year_diff == 5) {
        use_years <- c(use_years, next_year)
        break
      } 
      else if (year_diff != 5 & year_diff < 5){ 
        next
      }
      else if (year_diff != 5 & year_diff > 5) {
        use_years <- c(use_years, next_year)
        break
      }
    }
  }
  use_years <- c(tmp_data_years[1], use_years, max_year)
  if (loc == "FRA") pop[(ihme_loc_id == loc & source_type == "CENSUS" & year >= 2006 & !(year %in% use_years)), outlier := 1]
  else pop[(ihme_loc_id == loc & source_type == "pop_registry" & !(year %in% use_years)), outlier := 1]
}


dups_pop <- pop[outlier == 0 & (duplicated(pop[, list(ihme_loc_id, year, sex, source_type, outlier)]) | 
                                  duplicated(pop[, list(ihme_loc_id, year, sex, source_type, outlier)], fromLast = TRUE))]
if(nrow(dups_pop) > 0) stop("There are duplicates in your data")
pop[, c('new', 'plus_tot', 'dup') := NULL]


pop <- pop[, c('ihme_loc_id', 'country', 'year', 'sex', 'source_type', 'pop_source', 'precise_year', 'pop_nid', 'underlying_pop_nid', 'outlier', grep("DATUM", names(pop), value = T)), with = F]
pop <- melt(pop, id.vars = c('ihme_loc_id', 'country', 'year', 'sex', 'source_type', 'pop_source', 'precise_year', 'pop_nid', 'underlying_pop_nid', 'outlier'), 
            variable.name = "age_gap", 
            measure.vars = c(grep("DATUM", names(pop), value=T)))
setnames(pop, c("pop_source"), c("detailed_source"))
pop[, age_gap := gsub("DATUM", "", age_gap)]
pop <- pop[!is.na(value)]


ages <- data.table(get_age_map('all'))
ages[, age_group_name := gsub("years", "", age_group_name)]
ages[, age_group_name := gsub("-", "to", age_group_name)]
ages[, start := as.character(age_group_years_start)]
ages[grepl(" ", age_group_name), age_gap := gsub(" ", "", age_group_name)]
ages[!grepl(" ", age_group_name), age_gap := paste0(start, "to", start)]


ages[age_group_id == 283, age_gap := "UNK"]
ages[age_group_id == 22, age_gap := "TOT"]
ages[age_group_id == 24, age_gap := "15to49"]
ages[age_group_id == 26, age_gap := "70plus"]
ages[age_group_id == 29, age_gap := "15plus"]
ages[age_group_id == 356, age_gap := "120plus"]
ages <- ages[!age_group_id %in% c(308, 42, 27, 236, 164, 161)]
ages[age_group_id == 1, age_gap := "0to4"]
ages[age_group_id == 28, age_gap := "0to1"]
ages <-ages[, list(age_group_id, age_gap, age_group_name)]


pop <- merge(pop, ages, by = 'age_gap', all.x = T)


if(nrow(pop[is.na(age_group_id) & age_gap != "0to0"]) > 0) stop("There are missing age group id's other than 0to0")
pop[is.na(age_group_id) & age_gap == "0to0", age_group_id := 28]


pop <- merge(pop, locations, by = 'ihme_loc_id', all.x =T)
pop <- pop[, list(ihme_loc_id, year_id = year, precise_year, location_id, sex, age_group_id, source = source_type, detailed_source, nid = pop_nid, underlying_nid = underlying_pop_nid, mean = value, lower = NA, upper = NA, outlier)]


pop[source == "VR", source_type_id := 1]
pop[grepl("SRS", source), source_type_id := 2]
pop[grepl("DSP", source), source_type_id := 3]
pop[grepl("CENSUS", source), source_type_id := 5] 
pop[grepl("VR", source) & is.na(source_type_id), source_type_id := 1]


pop[source == "MOH survey", source_type_id := 34]


pop[source == "SSPC", source_type_id := 36]
pop[source == "DC", source_type_id := 37]
pop[source == "FFPS", source_type_id := 38]
pop[source == "SUPAS", source_type_id := 39]

pop[source == "SUSENAS", source_type_id := 40]
pop[source == "SURVEY", source_type_id := 16]
pop[source == "HOUSEHOLD_DLHS", source_type_id := 41]
pop[source == "HOUSEHOLD_HHC", source_type_id := 43]
pop[source == "HOUSEHOLD", source_type_id := 42]
pop[source == "pop_registry", source_type_id := 52]

pop[source == "SSPC-DC" , source_type_id := 50]

pop[source == "2000_CENS_SURVEY", source_type_id := 5]
pop[source == "NOT_USABLE_MODELED_MEX", source_type_id := 5]


pop[detailed_source == "", detailed_source := source]
pop[underlying_nid == ".", underlying_nid := NA]


pop[sex == "male", sex_id := 1]
pop[sex == "female", sex_id := 2]
pop[sex == "both", sex_id := 3]
pop <- pop[ihme_loc_id != "NRU"]


pop[, detailed_source := strtrim(detailed_source, 100)]

pop <- pop[, list(year_id, precise_year, location_id, sex_id, age_group_id, source_type_id, detailed_source, nid, underlying_nid, mean, lower, upper, outlier, ihme_loc_id)]
pop[, ihme_loc_id := NULL]

assert_values(pop, c('year_id', 'precise_year', 'location_id', 'sex_id', 'age_group_id', 'source_type_id', 'detailed_source', 'mean', 'outlier'), test = "not_na")


write_csv(pop, paste0("FILEPATH", version_id ,".csv"))

upload_results(filepath = paste0("FILEPATH"),
               model_name = "population empirical",
               model_type = "data",
               run_id = version_id, send_slack = T)

update_status(model_name = "population empirical",
              model_type = "data",
              run_id = verison_id,
              new_status = "best",
              new_comment = "GBD2017- using v28, RUS subnational fixes", assert_parents=F, send_slack = F)
