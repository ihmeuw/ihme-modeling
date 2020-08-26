
rm(list=ls())
library(data.table); library(slackr); library(argparse); library(haven); library(readstata13); library(assertable); library(DBI); library(readr); library(plyr); library(mortdb, lib = FILEPATH); library(mortcore, lib = FILEPATH)

# Set up location metadata
ap_old <- get_locations()
ap_old <- ap_old[ihme_loc_id %in% c("IND_44849", "XSU", "XYG")]

locations <- get_locations()
locations <- locations[!(grepl("KEN_", ihme_loc_id) & level == 4)]
locations <- rbind(locations, ap_old)
locations <- locations[order(ihme_loc_id),]

##########
# Deaths
##########

deaths <- get_vr()

deaths <- deaths[outlier == 0, list(ihme_loc_id, location_id, year = year_id, sex = sex_id, age_group_id, source_type_id, deaths_source = detailed_source, deaths_nid = nid, deaths_underlying_nid = underlying_nid, mean)]
deaths <- deaths[!location_id %in% c(420, 428)]
deaths <- unique(deaths)
deaths[, year := as.double(year)]


# Get age group metadata and merge onto deaths data
ages <- get_age_map()

deaths <- merge(deaths, ages[, .(age_group_id, age_group_years_end)], by='age_group_id')
deaths[age_group_years_end==125 & mean==0, mean := NA_real_]
deaths[, age_group_years_end := NULL]

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

## 2000_CENS_SURVEY
# deaths[ihme_loc_id == "IDN" & year == 1999 & source_type_id == 5, source_type := "2000_CENS_SURVEY"]
deaths[ihme_loc_id == "CMR" & year == 1976 & source_type_id == 42, source_type := "HOUSEHOLD"]

# Random ones that are "Other survey", categorizing them to "SURVEY"
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

# Small change to put ZAF 2007 community survey in 2006
deaths[source_type_id == 16 & ihme_loc_id == "ZAF", year := 2006.69]
deaths[, source_type_id := NULL]

# Specific drops
deaths <- deaths[!(ihme_loc_id == "COG" & year == 2009)]
deaths <- deaths[!(ihme_loc_id == "MMR" & year %in% c(2005, 2010))]
deaths <- deaths[!(ihme_loc_id == "MDV" & year %in% c(1975, 1976))]
deaths <- deaths[!(ihme_loc_id == "OMN" & year %in% c(2001, 2003, 2004))]
deaths <- deaths[!(ihme_loc_id == "TON" & year == 1990)]
deaths <- deaths[!(ihme_loc_id == "NER" & year == 2011)]

## Drop DYB Yugoslavia split data and specific USSR points
former_yugoslavia_location_ids <- c(44, 46, 49, 50, 53, 55)
former_yugoslavia_ihme_loc_ids <- unique(locations[location_id %in% former_yugoslavia_location_ids, ihme_loc_id])
deaths <- deaths[!(deaths_nid == "140201" & year >= 1992 & year <= 1995 & ihme_loc_id %in% former_yugoslavia_ihme_loc_ids)]

# Drop specific USSR 1958 points
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

# Import scalars
scalars <- fread(FILEPATH)
scalars <- scalars[!grepl("CEN|VR", source_type)]
scalars[, year := as.double(year)]
scalars[ihme_loc_id=="ZAF" & source_type == "SURVEY" & year == 2006, year := 2006.69]
scalars <- scalars[, list(ihme_loc_id, year, sex, source_type, correction_factor)]

# Apply scaling factor to death counts in order to be usable by DDM
deaths <- merge(deaths, scalars, by = c('ihme_loc_id', 'year', 'sex', 'source_type'), all.x = T)
deaths[is.na(correction_factor), hh_scaled := 0]
deaths[!is.na(correction_factor), hh_scaled := 1]
deaths[is.na(correction_factor), correction_factor := 1]
deaths[ihme_loc_id %in% c("BGD", "PAK") & source_type == "SRS", hh_scaled := 1]

deaths[ihme_loc_id == "IND_44849" & source_type=="SRS", hh_scaled := 1]
deaths[source_type %in% c("MCCD", "CR"), hh_scaled := 1]
for (var in grep("DATUM", names(deaths), value =T)){
  deaths[, (var) := get(var) * correction_factor]
}
deaths[, c('correction_factor') := NULL]

# The deaths data should be unique by ihme_loc_id - source - year - sex
deaths_dups <- deaths[duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)])]
if (nrow(deaths_dups) > 0) stop("Deaths data not unique by location-year-sex-source")

save.dta13(deaths, FILEPATH)

