rm(list=ls())
library(data.table); library(slackr); library(argparse); library(haven); library(readstata13)
library(assertable); library(DBI); library(readr); library(plyr)
library(mortdb)
library(mortcore)

username <- Sys.getenv("USER")

# Parse arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version id for this run of DDM')
parser$add_argument('--empirical_deaths_version', type="character", required=TRUE,
                    help='The version id of empirical deaths data')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD round')
parser$add_argument('--with_shock', type="integer", required=TRUE,
                    help='Dummy for with shock')
args <- parser$parse_args()

version_id <- args$version_id
empirical_deaths_version_id <- args$empirical_deaths_version
gbd_year <- args$gbd_year
with_shock <- args$with_shock
gbd_round_id <- get_gbd_round(gbd_year=gbd_year)
prev_gbd_year=get_gbd_year(gbd_round_id-1)

if (!empirical_deaths_version_id %in% c("best", "recent", "recent_completed")){
  empirical_deaths_version_id <- as.numeric(empirical_deaths_version_id)
}

main_dir <- paste0("FILEPATH")
source_outdir <- paste0("FILEPATH")

# Set up location metadata
ap_old <- get_locations(gbd_type = 'ap_old', level = 'estimate', gbd_year = gbd_year)
ap_old <- ap_old[ihme_loc_id %in% c("IND_44849", "XSU", "XYG")]

locations <- get_locations(gbd_year = gbd_year)
locations <- locations[!(grepl("KEN_", ihme_loc_id) & level == 4)]
locations <- rbind(locations, ap_old)
locations <- locations[order(ihme_loc_id),]

age_map <- get_age_map(type = "all")[, .(age_group_id, age_group_years_start, age_group_years_end)]
age_map <- age_map[!age_group_id %in% c(161, 27, 49, 35, 37, 38, 308, 294, 371)]

##########
# Deaths
##########

deaths <- get_mort_outputs(model_name = "death number empirical",
                           model_type = "data",
                           gbd_year = gbd_year,
                           location_set_id = 82,
                           run_id = empirical_deaths_version_id,
                           estimate_stage_ids = ifelse(with_shock == 1, 22, 21),
                           demographic_metadata = T)


deaths <- deaths[outlier == 0, list(ihme_loc_id,
                                    location_id,
                                    year = year_id,
                                    sex = sex_id,
                                    age_group_id, age_group_years_start, age_group_years_end,
                                    source_type_id,
                                    deaths_source = detailed_source,
                                    deaths_nid = nid,
                                    deaths_underlying_nid = underlying_nid, mean)]
deaths <- deaths[!location_id %in% c(420, 428)]
deaths <- unique(deaths)
deaths[, year := as.double(year)]

sspc_data <- fread("FILEPATH")
sspc_data <- merge(
  sspc_data,
  age_map,
  by.x = c("age_start", "age_end"),
  by.y = c("age_group_years_start", "age_group_years_end"),
  all.x = TRUE
)

sspc_data <- sspc_data[, .(ihme_loc_id, location_id,
                           year = year_id, sex = sex_id, age_group_id,
                           age_group_years_start = age_start,
                           age_group_years_end = age_end, source_type_id = 36,
                           deaths_source = "SSPC", deaths_nid = nid,
                           deaths_underlying_nid = NA, new_mean = deaths)]

# combine and remove duplicates in favour of new extractions

# correct source categorization
deaths[deaths_nid %in% sspc_data$deaths_nid, `:=` (deaths_source = "SSPC", source_type_id = 36)]

deaths <- merge(
  deaths,
  sspc_data,
  by = setdiff(names(deaths), "mean"),
  all = TRUE
)

deaths[!is.na(new_mean), mean := new_mean]

deaths[, new_mean := NULL]

# read and format placement srs
ind_srs_dir <- "FILEPATH"
ind_srs <- arrow::read_parquet(fs::path("FILEPATH"))
ind_srs <- setDT(ind_srs)
ind_srs[age_end == Inf, age_end := 125]

ind_srs <- merge(
  ind_srs,
  locations[, .(ihme_loc_id, location_id)],
  by = "ihme_loc_id",
  all.x = TRUE
)

ind_srs <- merge(
  ind_srs,
  age_map,
  by.x = c("age_start", "age_end"),
  by.y = c("age_group_years_start", "age_group_years_end"),
  all.x = TRUE
)

ind_srs <- ind_srs[, .(
  ihme_loc_id,
  location_id,
  year = year_id,
  sex = sex_id,
  age_group_id,
  age_group_years_start = age_start,
  age_group_years_end = age_end,
  source_type_id = 2,
  deaths_source = "SRS",
  deaths_nid = 86967,
  deaths_underlying_nid = 86967,
  mean = deaths
)]

ind_srs <- ind_srs[!is.na(mean)]

# drop IND srs between 1995 and 2020 and replace
deaths <- deaths[!(grepl("IND", ihme_loc_id) & grepl("SRS", deaths_source, ignore.case = TRUE) & year %in% 1995:2020)]

deaths <- rbind(deaths, ind_srs)

deaths <- deaths[age_group_years_start %% 1 == 0 & age_group_years_end %% 1 == 0]
deaths[, c('age_group_years_start', 'age_group_years_end') := NULL]

ages <- mortdb::get_age_map(type = 'all', gbd_year = gbd_year)
deaths <- merge(deaths, ages[, .(age_group_id, age_group_years_end)], by='age_group_id')
deaths[age_group_years_end==125 & mean==0, mean := NA_real_]
deaths[, age_group_years_end := NULL]

deaths[ihme_loc_id %like% "USA" & age_group_id == 235 & is.na(mean), mean := 0]

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
ages[age_group_id == 234, age_gap := "@75plus"]
ages <-ages[, list(age_group_id, age_gap, age_group_name)]
deaths <- merge(deaths, ages, by ='age_group_id', all.x = T)
deaths[, age_group_id := NULL]

## Assert that we don't have any duplicates
deaths[, test := .N, by = c("ihme_loc_id", "year", "sex", "source_type_id", "deaths_source", "deaths_nid", "age_gap")]
if(nrow(deaths[test > 1]) > 0) stop("You have duplicates in your data.")

## Reshape from long to wide and fill NA's if there isn't data for a given age group
deaths <- dcast.data.table(deaths,
                           ihme_loc_id + location_id + year + sex +
                             source_type_id + deaths_source + deaths_nid +
                             deaths_underlying_nid ~ age_gap,
                           value.var = "mean")
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

deaths[source_type_id %in% c(20, 21, 22, 23), source_type := "DSP"]

deaths[source_type_id %in% c(44, 45, 46, 47, 48, 49), source_type := "SRS"]

deaths[source_type_id %in% c(24, 32, 33) & ihme_loc_id == "KOR", source_type := "VR"]

deaths[source_type_id %in% c(30, 31) & grepl("ZAF", ihme_loc_id), source_type := "VR"]

deaths[source_type_id %in% c(28, 29) & grepl("MEX", ihme_loc_id), source_type := "VR"]

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
deaths[ihme_loc_id == "SAU" & year >= 2018, source_type := "VR_2018plus"]
deaths[ihme_loc_id == "TUR" & year >= 2009, source_type := "VR_2009plus"]
deaths[source_type_id == 16 & ihme_loc_id == "ZAF", year := 2006.69]
deaths[, source_type_id := NULL]
deaths <- deaths[!(ihme_loc_id == "COG" & year == 2009)]
deaths <- deaths[!(ihme_loc_id == "MMR" & year %in% c(2005, 2010))]
deaths <- deaths[!(ihme_loc_id == "MDV" & year %in% c(1975, 1976))]
deaths <- deaths[!(ihme_loc_id == "OMN" & year %in% c(2001, 2003, 2004))]
deaths <- deaths[!(ihme_loc_id == "TON" & year == 1990)]
deaths <- deaths[!(ihme_loc_id == "NER" & year == 2011)]
deaths <- deaths[!(ihme_loc_id == "DZA" & year %in% 1987:1997)]
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

# Remove duplicates
deaths[, dup := 0]
deaths[duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)]) |
         duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)], fromLast = T), dup := 1]
deaths <- deaths[dup == 0 | (dup == 1 & !(deaths_source %in% c("WHO_causesofdeath", "DYB")))]
deaths_dups <- deaths[duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)]) |
                        duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)], fromLast = T)]
if (nrow(deaths_dups) != 0) stop("Your deaths data contains duplicates")
deaths[, dup := NULL]

# Import scalars
scalars <- fread(paste0("FILEPATH"))
scalars <- scalars[!grepl("CEN|VR", source_type)]
scalars[, year := as.double(year)]
scalars[ihme_loc_id=="ZAF" & source_type == "SURVEY" & year == 2006, year := 2006.69]
scalars <- scalars[, list(ihme_loc_id, year, sex, source_type, correction_factor)]

# Merge scalars
if (nrow(scalars) > 0) {

  deaths <- merge(deaths, scalars, by = c('ihme_loc_id', 'year', 'sex', 'source_type'), all.x = T)

} else {

  deaths[, correction_factor := NA_real_]

}

# Create hh_scaled indicator
deaths[is.na(correction_factor), hh_scaled := 0]
deaths[!is.na(correction_factor), hh_scaled := 1]

# Manual modifications
deaths[is.na(correction_factor), correction_factor := 1]
deaths[ihme_loc_id %in% c("BGD", "PAK") & source_type == "SRS", hh_scaled := 1]
deaths[ihme_loc_id == "IND_44849" & source_type=="SRS", hh_scaled := 1]
deaths[source_type %in% c("MCCD", "CR"), hh_scaled := 1]

# Apply scaling factor to death counts
for (var in grep("DATUM", names(deaths), value =T)){
  deaths[, (var) := get(var) * correction_factor]
}
deaths[, c('correction_factor') := NULL]

# Assertions
assert_values(deaths, c('ihme_loc_id', 'country', 'sex', 'year', 'source_type', 'deaths_source'), 'not_na')
assert_values(deaths, c('deaths_nid'), 'not_na', warn_only = T)

# The deaths data should be unique by ihme_loc_id - source - year - sex
deaths_dups <- deaths[duplicated(deaths[, list(ihme_loc_id, year, sex, source_type)])]
if (nrow(deaths_dups) > 0) stop("Deaths data not unique by location-year-sex-source")

# Fill 0 DATUMs if NA
deaths[is.na(DATUM0to4) & !is.na(DATUM0to0) & !is.na(DATUM12to23months) & !is.na(DATUM2to4),
       DATUM0to4 := DATUM0to0 + DATUM12to23months + DATUM2to4]
deaths[is.na(DATUM0to4) & !is.na(DATUM0to0) & !is.na(DATUM1to4),
       DATUM0to4 := DATUM0to0 + DATUM1to4]

output_file <- ifelse(
  with_shock == 1,
  paste0("FILEPATH"),
  paste0("FILEPATH")
)
save.dta13(deaths, output_file)

if (with_shock == 0) {

  # append age-sex split VR for 2020 and 2023
  age_map <- get_age_map(type = "all", gbd_year = gbd_year)[, .(age_group_id, age_start = age_group_years_start, age_end = age_group_years_end)]
  age_map <- rbind(
    age_map,
    data.table(age_group_id = 1, age_start = 0 , age_end = 5)
  )

  split_vr <- fread("FILEPATH")

  # aggregate 0-1, and 0-5
  split_vr <- merge(split_vr, age_map, by = "age_group_id")
  split_vr_u1 <- split_vr[age_end <= 1]
  split_vr_u1 <- split_vr_u1[, .(age_start = 0, age_end = 1, age_group_id = 28, deaths = sum(deaths)), by = c("location_id", "year_id", "sex_id", "nid")]

  split_vr_u5 <- split_vr[age_start >= 1 & age_end <= 5]
  split_vr_u5 <- split_vr_u5[, .(age_start = 1, age_end = 5, age_group_id = 5, deaths = sum(deaths)), by = c("location_id", "year_id", "sex_id", "nid")]

  split_vr <- rbind(
    split_vr[age_end > 5],
    split_vr_u1,
    split_vr_u5
  )

  # Process for 5q0
  d00_split <- copy(split_vr)
  d00_split <- merge(
    d00_split,
    locations[, .(ihme_loc_id, location_id, location_name)],
    by = "location_id"
  )

  d00_split[, age_col := paste0("DATUM", age_start, "to", age_end - 1)]
  d00_split[age_col == "DATUM95to124", age_col := "DATUM95plus"]
  d00_split[sex_id == 1, sex := "male"]
  d00_split[sex_id == 2, sex := "female"]
  d00_split[sex_id == 3, sex := "both"]
  d00_split <- d00_split[
    ,
    .(ihme_loc_id, year = year_id, sex, source_type = "VR",
      deaths_source = "VR_age_sex_split", deaths_nid = nid, deaths_underlying_nid = nid,
      country = location_name, deaths_footnote = "Age-sex split VR", hh_scaled = 0,
      age_col, deaths
    )
  ]

  d00_tot <- d00_split[, .(deaths = sum(deaths), age_col = "DATUMTOT"), by = setdiff(names(d00_split), c("deaths", "age_col"))]

  d00_split <- rbind(d00_split, d00_tot)

  d00_split <- dcast(d00_split, ...~age_col, value.var = "deaths")
  d00_split[, DATUM0to4 := DATUM0to0 + DATUM1to4]

  # deduplicate - prioritize VRP
  d00_existing <- unique(deaths[, .(ihme_loc_id, year, sex)])
  d00_existing[, existing := TRUE]

  d00_split <- merge(d00_split, d00_existing, by = c("ihme_loc_id", "year", "sex"), all.x = TRUE)
  d00_split <- d00_split[is.na(existing)]

  d00_split[, existing := NULL]

  haven::write_dta(d00_split, paste0("FILEPATH"))

}
