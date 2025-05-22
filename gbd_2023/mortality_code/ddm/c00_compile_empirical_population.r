rm(list=ls())
library(data.table); library(argparse); library(haven); library(readstata13);
library(assertable); library(DBI); library(readr); library(plyr);
library(mortdb); library(mortcore)

username <- Sys.getenv("USER")

# Parse arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version id for this run of DDM')
parser$add_argument('--empirical_population_version', type="character", required=TRUE,
                    help='The version id of empirical population data')
parser$add_argument('--population_estimate_version', type="character", required=TRUE,
                    help='The version id of population estimates')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD round')
parser$add_argument('--code_dir', type="character", required=TRUE,
                    help='Directory where ddm code is cloned')
args <- parser$parse_args()
version_id <- args$version_id
pop_version_id <- args$empirical_population_version
population_estimate_version_id <- args$population_estimate_version
gbd_year <- args$gbd_year
code_dir <- args$code_dir

gbd_round_id <- get_gbd_round(gbd_year=gbd_year)
prev_gbd_year=get_gbd_year(gbd_round_id-1)

if (!pop_version_id %in% c("best", "recent")){
  pop_version_id <- as.numeric(pop_version_id)
}

main_dir <- paste0("FILEPATH")
source(paste0("FILEPATH"))

# Set up location metadata
ap_old <- data.table(get_locations(gbd_type = 'ap_old', level = 'estimate', gbd_year = gbd_year))
ap_old <- ap_old[ihme_loc_id %in% c("IND_44849", "XSU", "XYG")]

locations <- data.table(get_locations(gbd_year = gbd_year))
locations <- locations[!(grepl("KEN_", ihme_loc_id) & level == 4)]
locations <- rbind(locations, ap_old)
locations <- locations[order(ihme_loc_id),]

age_map <- get_age_map(type = "all")[, .(age_group_id, age_group_years_start, age_group_years_end)]
age_map <- age_map[!age_group_id %in% c(161, 27, 49, 35, 37, 38, 308, 294, 371)]

##############
# Population
##############

# Run population prep
if((gbd_year==2017) & (pop_version_id!="best")){
population <- get_mort_outputs(model_name = "population empirical", model_type = "data", location_set_id = 82, run_id = pop_version_id, gbd_year = prev_gbd_year)
}else{
population <- get_mort_outputs(model_name = "population empirical", model_type = "data", location_set_id = 82, gbd_year = gbd_year, run_id = pop_version_id)
}
population <- population[outlier == 0, list(location_id, year = year_id, precise_year, sex_id, age_group_id, source_type_id, pop_source = detailed_source, pop_nid = nid, underlying_pop_nid = underlying_nid, mean)]

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
  location_id,
  year = year_id,
  precise_year = year_id + 0.5,
  sex_id,
  age_group_id,
  source_type_id = 2,
  pop_source = "SRS",
  pop_nid = 86967,
  underlying_pop_nid = 86967,
  mean = pop
)]

ind_srs <- ind_srs[!is.na(mean)]

population <- population[
  !(location_id %in% locations[grepl("IND", ihme_loc_id), location_id]
    & grepl("SRS", pop_source, ignore.case = TRUE)
    & year %in% 1995:2020)
]

population <- rbind(population, ind_srs)

age_map <- get_age_map(gbd_year = gbd_year, type = "all")[age_group_years_end <= 15]
split_map <- data.table(age_group_id = c(28, 49:62),
                        agg_age = rep(c(1, 6, 7), each = 5))
alt <- get_mort_outputs("census raw", "data", run_id = "recent", location_id = 66, year_ids = 2001)

alt <- alt[, .(location_id, year = year_id, sex_id, age_group_id, mean)]
alt_both <- copy(alt)
alt_both <- alt_both[, mean := sum(mean), by = "age_group_id"]
alt_both[, sex_id := 3]
alt_both <- unique(alt_both)
alt <- rbind(alt, alt_both)

agg <- unique(alt[age_group_id == 39])
agg <- agg[, age_group_id := NULL]

split <- population[location_id == 66 & year == 2011]
split <- merge(split, age_map[, .(age_group_id)], by = "age_group_id")
split[, tot := sum(mean), by = "sex_id"]
split[, prop := mean/tot]
split <- split[, .(age_group_id, sex_id, prop)]
split <- merge(agg, split, by = "sex_id")
split[, mean := mean * prop]
split <- merge(split, split_map, by = "age_group_id")
split[, mean := sum(mean), by = c("sex_id", "agg_age")]
split[, age_group_id := agg_age]
split[, c("agg_age", "prop") := NULL]
split <- unique(split)

alt <- alt[age_group_id != 39 & !age_group_id %in% split_map$age_group_id]
alt <- rbind(alt, split)

old <- population[location_id == 66 & year == 2001]
old[, c("mean", "age_group_id", "sex_id") := NULL]
old <- unique(old)

alt <- merge(alt, old, by = c("location_id", "year"))

population <- population[!(location_id == 66 & year == 2001)]
population <- rbind(population, alt)

population <- population[!(location_id==49 & year==1994)]

population <- merge(population, locations[, list(location_id, ihme_loc_id, country = location_name, region_name)], by = 'location_id', all.x =T)
population <- population[!is.na(ihme_loc_id)]
population[, year := as.double(year)]
population[, precise_year := as.character(precise_year)]

population <- population[!(location_id == 81 & year %in% c(2010, 2021))]

# Set source_type variable
population[source_type_id == 1, source_type := "VR"]
population[source_type_id == 2, source_type := "SRS"]
population[source_type_id == 3, source_type := "DSP"]
population[source_type_id == 5, source_type := "CENSUS"]

population[source_type_id == 34, source_type := "MOH survey"]

population[source_type_id == 36, source_type := "SSPC"]
population[source_type_id == 37, source_type := "DC"]

population[source_type_id == 38, source_type := "FFPS"]
population[source_type_id == 39, source_type := "SUPAS"]
population[source_type_id == 40, source_type := "SUSENAS"]
population[source_type_id == 16, source_type := "SURVEY"]
population[source_type_id == 41, source_type := "HOUSEHOLD_DLHS"]
population[source_type_id == 42, source_type := "HOUSEHOLD"]
population[source_type_id == 43, source_type := "HOUSEHOLD_HHC"]

population[source_type_id == 50, source_type := "SSPC-DC"]
population[source_type_id == 52, source_type := "pop_registry"]
population[source_type_id == 5 & ihme_loc_id== "IDN" & year == 1999 , source_type := "SURVEY"]
population[pop_source == "CONAPO_FROM_KATE_LOFGREN_NOT_USABLE", source_type := "NOT_USABLE_MODELED_MEX"]
population[pop_source == "CHN_PROV_CENSUS_OLD", source_type := "CENSUS_OLD"]
population[pop_source == "NOT_USABALLS", source_type := "CENSUS_NOT_USABLE"]

## Change sex values
population[sex_id == 1, sex_name := "male"]
population[sex_id == 2, sex_name := "female"]
population[sex_id == 3, sex_name := "both"]
setnames(population, "sex_name", "sex")

population[source_type == "SURVEY" & ihme_loc_id== "ZAF" & year == 2007, year := 2006.69]

# Remove misc variables and generate pop_footnote variable
population[, c('sex_id', 'source_type_id') := NULL]
population[, pop_footnote := ""]

# Save population for computing correction factors
pre_scaled_pop <- copy(population)
pre_scaled_pop <- pre_scaled_pop[, list(ihme_loc_id, country, sex, year, source_type, pop_source, pop_footnote, pop_nid, underlying_pop_nid, mean, age_group_id)]
write_csv(population, paste0("FILEPATH"))
check_files("FILEPATH", continual = T)

scalars <- compute_scalars(population, population_estimate_version_id, gbd_year = gbd_year)
write_csv(scalars, paste0("FILEPATH"))

# Pull age group metadata to reshape population data wide
ages <- data.table(get_age_map(gbd_year=gbd_year,type='all'))
ages[, age_group_name := gsub("years", "", age_group_name)]
ages[, age_group_name := gsub("-", "to", age_group_name)]
ages[, start := as.character(age_group_years_start)]
ages[grepl(" ", age_group_name), age_gap := gsub(" ", "", age_group_name)]
ages[!grepl(" ", age_group_name), age_gap := paste0(start, "to", start)]
ages[, age_gap := paste0("@", age_gap)]
ages[age_group_id == 283, age_gap := "@UNK"]
ages[age_group_id == 22, age_gap := "@TOT"]
ages[age_group_id == 24, age_gap := "@15to49"]
ages[age_group_id == 26, age_gap := "@70plus"]
ages[age_group_id == 29, age_gap := "@15plus"]
ages[age_group_id == 238, age_gap := "@1to1"]
ages <- ages[!age_group_id %in% c(308, 42, 27, 236, 164, 161)]
ages[age_group_id == 1, age_gap := "@0to4"]
ages[age_group_id == 28, age_gap := "@0to0"]
ages <-ages[, list(age_group_id, age_gap, age_group_name)]

population <- merge(population, ages, by = 'age_group_id', all.x = T)
population[, age_group_id := NULL]
population[, age_group_name := NULL]

# The population data should be unique by ihme_loc_id - source - year - sex - age group
pop_dups_ages <- population[duplicated(population[, list(ihme_loc_id, year, sex, source_type, age_gap)])]
if (nrow(pop_dups_ages) > 0) stop("Population data not unique by location-year-sex-source-age")

population <- dcast.data.table(population, ihme_loc_id + location_id + country + year +
                                 precise_year + sex + source_type + pop_source + pop_footnote +
                                 pop_nid + underlying_pop_nid ~ age_gap, value.var = "mean", fill = NA_real_)

# Rename some of the fields and create DATUM columns
setnames(population, grep("@", colnames(population)), gsub("@", "DATUM", grep("@", names(population), value = T)))

# Pull in scalars
scalars <- scalars[!grepl("CEN|VR", source_type)]
scalars[, year := as.double(year)]
scalars[ihme_loc_id=="ZAF" & source_type == "SURVEY" & year == 2006, year := 2006.69]
scalars <- scalars[, list(ihme_loc_id, year, sex, source_type, correction_factor)]

# Apply scalars to population counts in order to process them through DDM
population <- merge(population, scalars, by = c('ihme_loc_id', 'year', 'sex', 'source_type'), all.x = T)
if (nrow(population[!is.na(correction_factor)]) != nrow(scalars)){
  stop("Please check your merge. There are either more/less correction factors applied to the empirical population data.")
}

population[is.na(correction_factor), correction_factor := 1]
for (var in grep("DATUM", names(population), value =T)){
  population[, (var) := get(var) * correction_factor]
}

population[, correction_factor := NULL]

# Treat DSP pop data like pop registry data
china_dsp_locs <- unique(population[(source_type == "DSP" & grepl("CHN", ihme_loc_id)), list(ihme_loc_id, source_type)])
loc_source_list <- china_dsp_locs$source_type
names(loc_source_list) <- china_dsp_locs$ihme_loc_id
loc_source_list <- as.list(loc_source_list)

for (i in 1:length(loc_source_list)){
  # Subset data to specific location and if it's registry type and pull all of the years
  source <- loc_source_list[[i]]
  loc <- names(loc_source_list)[i]
  tmp_data <- population[ihme_loc_id == loc & source_type == source]
  tmp_data_years <- sort(unique(tmp_data$year))

  # Find the min/max year of the time interval and increment by 5
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
  population[ihme_loc_id == loc & source_type == source & (year %in% use_years), dsp_use_ddm := 1]
}
population[is.na(dsp_use_ddm), dsp_use_ddm := 0]

# The population data should be unique by ihme_loc_id - source - year - sex
pop_dups <- population[duplicated(population[, list(ihme_loc_id, year, sex, source_type)])]
if (nrow(pop_dups) > 0) stop("Population data not unique by location-year-sex-source")

population <- population[!(ihme_loc_id == "BRA_4776" & year %in% c(1950, 1960, 1970) & source_type == "CENSUS")]
population <- population[!(ihme_loc_id == "JOR" & year == 1961 & source_type == "CENSUS")]
population <- population[!(ihme_loc_id == "NLD" & year%in%c(1950, 1970, 1980, 1990, 2000, 2001, 2010))]
population <- population[!(ihme_loc_id == "HUN" & year > 2011 & year < 2018)]

# remove pop registry
population <- population[source_type!="pop_registry" | ihme_loc_id %in% c("AND", "DEU", "TUR")]

# remove other source_types that are not useable
population <- population[!(source_type %like% "NOT_USABLE")]

population <- population[!(ihme_loc_id == "DZA" & year %in% 1987:1997)]

save.dta13(population, paste0("FILEPATH"))
