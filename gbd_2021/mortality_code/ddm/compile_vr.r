# Purpose: 1) Prep VR data for graphing purposes

rm(list=ls())
library(data.table); library(haven); library(readr); library(readstata13);
library(assertable); library(plyr); library(DBI);
library(mortdb); library(mortcore)
library(ggplot2)
if (Sys.info()[1] == "Linux") {
  root <- "FILEPATH" 
  user <- Sys.getenv("USER")
  ddm_version_id <- as.character(commandArgs(trailingOnly = T)[1])
} else {
  root <- "FILEPATH"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("FILEPATH")
}
gbd_year <- x
proc_lineage <- setDT(get_proc_lineage(model_name = "ddm", model_type = "estimate", run_id = ddm_version_id,gbd_year=gbd_year))
proc_lineage <- proc_lineage[, list(process_name = parent_process_name, run_id = parent_run_id)]
empirical_deaths_version_id <- proc_lineage[process_name == "death number empirical data", run_id]
main_dir <- paste0("FILEPATH")

dir.create(file.path(main_dir), showWarnings = F)



# Set up location metadata
ap_old <- data.table(get_locations(gbd_type = 'ap_old', level = 'estimate', gbd_year = gbd_year))
ap_old <- ap_old[location_name == "Old Andhra Pradesh"]

locations <- data.table(get_locations(gbd_year = gbd_year))
locations <- locations[!(grepl("KEN_", ihme_loc_id) & level == 4)]
locations <- rbind(locations, ap_old)
locations <- locations[order(ihme_loc_id),]


deaths <- data.table(get_mort_outputs(model_name = "death number empirical", model_type = "data", gbd_year = gbd_year, location_set_id = 82, run_id = empirical_deaths_version_id))
deaths <- deaths[outlier == 0, list(ihme_loc_id, location_id, year = year_id, sex = sex_id, age_group_id, source_type_id, deaths_source = detailed_source, deaths_nid = nid, deaths_underlying_nid = underlying_nid, mean)]
deaths <- unique(deaths)
deaths[, year := as.double(year)]

# Get age group metadata and merge onto deaths data
ages <- data.table(get_age_map('all'))
ages[, age_group_name := gsub("years", "", age_group_name)]
ages[, age_group_name := gsub("-", "to", age_group_name)]
ages[, start := as.character(age_group_years_start)]
ages[grepl(" ", age_group_name), age_gap := gsub(" ", "", age_group_name)]
ages[!grepl(" ", age_group_name), age_gap := paste0(start, "to", start)]
ages[, age_gap := paste0("@", age_gap)]
ages[age_group_id == 283, age_gap := "@UNK"]
ages[age_group_id == 22, age_gap := "@TOT"]
ages[age_group_id == 24, age_gap := "@15to49"]
ages[age_group_id == 161, age_gap := "@0to0"]
ages[age_group_id == 26, age_gap := "@70plus"]
ages[age_group_id == 29, age_gap := "@15plus"]
ages[age_group_id == 1, age_gap := "@0to4"]
ages[age_group_id == 28, age_gap := "@0to0"]
ages <-ages[, list(age_group_id, age_gap, age_group_name)]
deaths <- merge(deaths, ages, by ='age_group_id', all.x = T)
deaths[, age_group_id := NULL]

## Reshape from long to wide and fill NA's if there isn't data for a given age group
deaths <- dcast.data.table(deaths, ihme_loc_id + location_id + year + sex + source_type_id + deaths_source + deaths_nid + deaths_underlying_nid ~ age_gap, value.var = "mean")
setnames(deaths, grep("@", colnames(deaths)), gsub("@", "DATUM", grep("@", names(deaths), value = T)))

## Merge on location metadata
deaths <- merge(deaths, locations[, list(location_id, country = location_name)], by = c('location_id'), all.x = T)
deaths[, location_id := NULL]
deaths[, year := as.double(year)]

## Adjust source type names to match designations in compile code
deaths[source_type_id == 1, source_type := "VR"]
deaths[source_type_id == 2, source_type := "SRS"]
deaths[source_type_id == 3, source_type := "DSP"]
deaths[source_type_id == 5, source_type := "CENSUS"]
deaths[source_type_id == 34, source_type := "MOH survey"]
deaths[source_type_id == 36, source_type := "SSPC"]
deaths[source_type_id == 37, source_type := "DC"]
deaths[source_type_id == 38, source_type := "FFPS"]
deaths[source_type_id == 39, source_type := "SUPAS"]
deaths[source_type_id == 40, source_type := "SUSENAS"]
deaths[source_type_id == 41, source_type := "HOUSEHOLD_DLHS"]
deaths[source_type_id == 42, source_type := "HOUSEHOLD"]
deaths[source_type_id == 43, source_type := "HOUSEHOLD_HHC"]
deaths[source_type_id == 55, source_type := "MCCD"]
deaths[source_type_id == 56, source_type := "CR"]


## Pool DSP together
deaths[source_type_id %in% c(20, 21, 22, 23), source_type := "DSP"]

## Pool all SRS together
deaths[source_type_id %in% c(44, 45, 46, 47, 48, 49), source_type := "SRS"]

## Pool KOR VR together
deaths[source_type_id %in% c(24, 32, 33) & ihme_loc_id == "KOR", source_type := "VR"]

## Pool ZAF VR together
deaths[source_type_id %in% c(30, 31) & grepl("ZAF", ihme_loc_id), source_type := "VR"]

## Pool MEX VR together
deaths[source_type_id %in% c(28, 29) & grepl("MEX", ihme_loc_id), source_type := "VR"]

## South Africa VR-SSA
deaths[source_type_id == 1 & ihme_loc_id == "ZAF" & deaths_source == "stats_south_africa", source_type := "VR-SSA"]

deaths[ihme_loc_id == "CMR" & year == 1976 & source_type_id == 42, source_type := "HOUSEHOLD"]

deaths[source_type_id == 16, source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "BFA" & deaths_source == "OECD" & year == 1960, source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "BGD" & deaths_source == "OECD" & year %in% c(1962, 1963, 1964, 1965), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "HTI" & deaths_source == "DYB" & year == 1972, source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "IDN" & deaths_source == "DYB" & year == 1964, source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "ISR" & deaths_source == "OECD" & year %in% c(1948, 1949), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "KHM" & deaths_source == "DYB" & year %in% c(1959), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "LBR" & deaths_source == "DYB" & year %in% c(1970), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "MWI" & deaths_source == "DYB" & year %in% c(1971), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "PAK" & deaths_source == "OECD" & year %in% c(1962, 1963, 1964, 1965), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "SAU" & year %in% c(2006, 2016), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "TGO" & year %in% c(1961), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "TUR" & year %in% c(1967, 1989), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "TZA" & year %in% c(1973), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "ZAF" & year %in% c(2006), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "GHA" & year %in% c(2010), source_type := "SURVEY"]
deaths[source_type_id == 16 & ihme_loc_id == "SSD" & year %in% c(2008), source_type := "SURVEY"]
deaths[source_type_id == 35 & ihme_loc_id == "BWA", source_type := "VR"]

deaths[source_type_id == 16 & ihme_loc_id == "ZAF", year := 2006.69]
deaths[, source_type_id := NULL]

deaths <- deaths[!(ihme_loc_id == "COG" & year == 2009)]
deaths <- deaths[!(ihme_loc_id == "MMR" & year %in% c(2005, 2010))]
deaths <- deaths[!(ihme_loc_id == "MDV" & year %in% c(1975, 1976))]
deaths <- deaths[!(ihme_loc_id == "OMN" & year %in% c(2001, 2003, 2004))]
deaths <- deaths[!(ihme_loc_id == "TON" & year == 1990)]
deaths <- deaths[!(ihme_loc_id == "NER" & year == 2011)]

former_yugoslavia_location_ids <- c(44, 46, 49, 50, 53, 55)
former_yugoslavia_ihme_loc_ids <- unique(locations[location_id %in% former_yugoslavia_location_ids, ihme_loc_id])
deaths <- deaths[!(deaths_nid == "140201" & year >= 1992 & year <= 1995 & ihme_loc_id %in% former_yugoslavia_ihme_loc_ids)]

deaths <- deaths[!(ihme_loc_id %in% c("EST", "RUS", "LTU", "LVA", "UKR") & source_type == "VR" & year == 1958)]

## Create deaths_footnote column
deaths[, deaths_footnote := ""]

## Change sex values
deaths[sex == 1, sex_name := "male"]
deaths[sex == 2, sex_name := "female"]
deaths[sex == 3, sex_name := "both"]
deaths[, sex := NULL]
setnames(deaths, "sex_name", "sex")

deaths[, deaths_nid := as.character(deaths_nid)]
deaths[, deaths_underlying_nid := as.character(deaths_underlying_nid)]
deaths[is.na(deaths_underlying_nid), deaths_underlying_nid := ""]

## Check N/A's for location and source_type
assert_values(deaths, c('ihme_loc_id', 'country', 'sex', 'year', 'source_type', 'deaths_source'), 'not_na')
assert_values(deaths, c('deaths_nid'), 'not_na', warn_only = T)

deaths[, dup := 0]
deaths[duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)]) |
         duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)], fromLast = T), dup := 1]
deaths <- deaths[dup == 0 | (dup == 1 & !(deaths_source %in% c("WHO_causesofdeath", "DYB")))]
deaths_dups <- deaths[duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)]) |
                        duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)], fromLast = T)]
if (nrow(deaths_dups) != 0) stop("Your deaths data contains duplicates")
deaths[, dup := NULL]

# Create both sex aggregate where missing
tmp <- copy(deaths)
tmp[, n := seq(.N), by = c('ihme_loc_id', 'year', 'source_type', 'deaths_source')]
tmp[, max := max(n), by = c('ihme_loc_id', 'year', 'source_type','deaths_source')]
both_sex_deaths <- unique(tmp[max ==2])
both_sex_deaths[, c("n", "max") := NULL]
both_sex_deaths[, sex := NULL]
datum_cols <- grep("^DATUM", names(both_sex_deaths), value = T)
both_sex_deaths <- both_sex_deaths[, lapply(.SD, function(x)replace(x, which(is.na(x)), -999)), .SDcols = datum_cols, by = c('ihme_loc_id', 'country', 'year', 'source_type', 'deaths_source', 'deaths_footnote', 'deaths_nid', 'deaths_underlying_nid')]

both_sex_deaths <- both_sex_deaths[, lapply(.SD, sum, na.rm = T), .SDcols = grep("^DAT", names(both_sex_deaths), value = T), by = c('ihme_loc_id', 'country', 'year', 'source_type', 'deaths_source', 'deaths_footnote', 'deaths_nid', 'deaths_underlying_nid')]
both_sex_deaths <- both_sex_deaths[, lapply(.SD, function(x) replace(x, which(x<0), NA)), .SDcols = datum_cols, by = c('ihme_loc_id', 'country', 'year', 'source_type', 'deaths_source', 'deaths_footnote', 'deaths_nid', 'deaths_underlying_nid')]
both_sex_deaths[, sex := "both"]
both_sex_deaths <- both_sex_deaths[,c('ihme_loc_id', 'country', 'sex', 'year', 'source_type', 'deaths_source', 'deaths_footnote', grep("^DATUM", names(both_sex_deaths), value = T), 'deaths_nid', 'deaths_underlying_nid'), with = F]

deaths <- data.table(rbind(deaths, both_sex_deaths))

deaths <- deaths[, c('ihme_loc_id', 'country', 'year', 'sex', 'source_type', 'deaths_source', 'deaths_nid', 'deaths_underlying_nid', grep("DATUM", names(deaths), value = T)), with = F]
deaths <- melt(deaths, id.vars = c('ihme_loc_id', 'country', 'year', 'sex', 'source_type', 'deaths_source', 'deaths_nid', 'deaths_underlying_nid'), 
            variable.name = "age_gap", 
            measure.vars = c(grep("DATUM", names(deaths), value=T)))
deaths[, age_gap := gsub("DATUM", "", age_gap)]
deaths <- deaths[!is.na(value)]

# Reshape long
ages <- data.table(get_age_map('all'))
ages[, age_group_name := gsub("years", "", age_group_name)]
ages[, age_group_name := gsub("-", "to", age_group_name)]
ages[, age_group_years_end := as.character(age_group_years_end)]
ages[, start := as.character(age_group_years_start)]
ages[, end := as.character(age_group_years_end)]
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
ages[age_group_id == 28, age_gap := "0to0"]
ages <-ages[, list(age_group_id, age_gap, age_group_name)]
deaths <- merge(deaths, ages, by = 'age_gap', all.x = T)

vr <- deaths[grepl("VR|vr", source_type) & source_type != "VR-SSA"]
vr <- vr[, list(ihme_loc_id, year, sex, age_group_name = age_gap, vr_deaths = value)]
setnames(vr, "year", "year_id")
vr <- vr[!is.na(vr_deaths),]

#aligning vr age groups with gbd age groups
for (cc in unique(vr$ihme_loc_id)){
  country_vr_all <- vr[ihme_loc_id == cc,]
  new_group <- list()
  for(year in unique(country_vr_all$year_id)){
    country_vr <- country_vr_all[year_id == year,]
    if(!"95 plus" %in% unique(country_vr$age_group_name)){
      country_vr <- dcast.data.table(country_vr, ihme_loc_id + sex + year_id ~ age_group_name, value.var = "vr_deaths")
      if ("100 plus" %in% colnames(country_vr) & "95 to 99" %in% colnames(country_vr)){
        country_vr$'95 plus' <- country_vr$'100 plus' + country_vr$'95 to 99'
      }
      country_vr = as.data.table(country_vr)
      if("95 plus" %in% colnames(country_vr)){
        new_group[[year]] <- country_vr[,c("95 plus", "ihme_loc_id", "year_id", "sex"), with = F]
      }
    }
    
  }
  new_group <- rbindlist(new_group, use.names = T)
  new_group <- melt(new_group, id = c("ihme_loc_id", "sex", "year_id"), variable.name = "age_group_name", value.name = "vr_deaths")
  vr <- rbind(vr, new_group, use.names = T)
}

#aligning vr age groups with gbd age groups
for (cc in unique(vr$ihme_loc_id)){
  country_vr_all <- vr[ihme_loc_id == cc,]
  new_group <- list()
  for(year in unique(country_vr_all$year_id)){
    country_vr <- country_vr_all[year_id == year,]
    if(!"80 plus" %in% unique(country_vr$age_group_name)){
      country_vr <- dcast.data.table(country_vr, ihme_loc_id + sex + year_id ~ age_group_name, value.var = "vr_deaths")
      if ("85 plus" %in% colnames(country_vr) & "80 to 84" %in% colnames(country_vr)){
        country_vr$'80 plus' <- country_vr$'85 plus' + country_vr$'80 to 84'
      } else if ("80 to 84" %in% colnames(country_vr) & "85 to 89" %in% colnames(country_vr) & "90 to 94" %in% colnames(country_vr) & "95 plus"%in% colnames(country_vr) ){
        country_vr$'80 plus' <- country_vr$'85 to 89' + country_vr$'80 to 84'+ country_vr$'90 to 94' +  country_vr$'95 plus'
      }  else if ("80 to 84" %in% colnames(country_vr) & "85 to 89" %in% colnames(country_vr) & "90 plus"%in% colnames(country_vr) ){
        country_vr$'80 plus' <- country_vr$'85 to 89' + country_vr$'80 to 84'+ country_vr$'90 plus'
      }
      country_vr = as.data.table(country_vr)
      if("80 plus" %in% colnames(country_vr)){
        new_group[[year]] <- country_vr[,c("80 plus", "ihme_loc_id", "year_id", "sex"), with = F]
      }
    }
    
  }
  new_group <- rbindlist(new_group, use.names = T)
  new_group <- melt(new_group, id = c("ihme_loc_id", "sex", "year_id"), variable.name = "age_group_name", value.name = "vr_deaths")
  vr <- rbind(vr, new_group, use.names = T)
}

vr_2017 <- copy(vr)
write_csv(vr_2017, paste0("FILEPATH"))
rm(vr)

# DONE