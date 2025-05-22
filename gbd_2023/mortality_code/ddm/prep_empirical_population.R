# Description: Compiles empirical population for GBD2020

rm(list=ls())

library(data.table); library(haven); library(lubridate); library(readr);
library(readstata13); library(assertable); library(plyr); library(DBI);
library(foreign); library(mortdb); library(mortcore);

root <- "FILEPATH"
user <- Sys.getenv("USER")

gbd_year <- 2023
gen_new <- T

if (gen_new) {

  run_comment <- "UPDATE RUN COMMENT"
  census_data_version_id <- mortdb::get_proc_version("census processed", "data", "recent", gbd_year = gbd_year)
  version_id <- mortdb::gen_new_version("population empirical", "data", comment = run_comment)
  mortdb::gen_parent_child(child_process = "population empirical data", child_id = version_id, parent_runs = list("census_processed data" = census_data_version_id))

} else {

  version_id <- get_proc_version("population empirical", "data", "recent", gbd_year = gbd_year)
  proc_lineage <- get_proc_lineage("population empirical", "data", run_id = version_id, gbd_year = gbd_year)
  census_data_version_id <- proc_lineage[parent_process_name == "census processed data", parent_run_id]

}

locations <- data.table(get_locations(gbd_year = gbd_year, level = "estimate"))

empirical_pop_gbd2017 <- fread("FILEPATH")

empirical_pop_gbd2017[nid == 12585, nid := 2585]
empirical_pop_gbd2017[nid == 259185, nid := 93410]

# Pull census data
new_census_data <- get_mort_outputs(
  model_name = "census_processed",
  model_type = "data",
  gbd_year = gbd_year,
  run_id = census_data_version_id
)
new_census_data <- new_census_data[
  data_stage_id == 6,
  list(
    year_id, ihme_loc_id, location_id, sex_id, outlier_type_id, record_type_id,
    method_id, age_group_id, census_midpoint_date, detailed_source = source_name,
    nid, underlying_nid, sub_agg, mean
  )
]
new_census_data <- unique(new_census_data)

# Mark outliers
new_census_data[(outlier_type_id == 1), outlier := 0]
new_census_data[is.na(outlier), outlier := 1]

# Truncate detailed source value
new_census_data[, detailed_source := strtrim(detailed_source, 100)]

# Create source_type_ids
new_census_data[record_type_id == 1, source_type_id := 52]
new_census_data[record_type_id %in% c(4,5,7), source_type_id := 5]

if (nrow(new_census_data[is.na(source_type_id)]) > 0) stop("Missing source_type_id")

# Create precise_year
new_census_data[, precise_year := round(decimal_date(as.POSIXlt(census_midpoint_date)), 1)]
new_census_data[, census_midpoint_date := NULL]

# Merge age metadata onto pop with standard age_group_id's
ages <- data.table(get_age_map(type = 'all'))
ages[, age_group_years_end := age_group_years_end - 1]
ages[, start := as.character(age_group_years_start)]
ages[, end := as.character(age_group_years_end)]
ages[grepl("\\-", age_group_name), age_group_name := gsub("-", "to", age_group_name)]
ages[grepl("years", age_group_name), age_group_name := gsub("years", "", age_group_name)]

ages[, age_gap := paste0(start, "to", end)]
ages[grepl("plus", age_group_name), age_gap := paste0(start,"plus")]
ages[!grepl("to", age_group_name) & !grepl("plus", age_group_name) & !grepl("\\-", age_group_name) & !grepl("Under|under", age_group_name), age_gap := paste0(start, "to", start)]
ages[, age_gap := paste0("@", age_gap)]

# Manual changes to age gaps
ages[age_group_id == 283, age_gap := "@UNK"]
ages[age_group_id == 236, age_gap := "@0to4"]
ages[age_group_id == 356, age_gap := "@120plus"]
ages[age_group_id == 26, age_gap := "@70plus"]
ages[age_group_id == 29, age_gap := "@15plus"]
ages[age_group_id == 22, age_gap := "@TOT"]
ages[age_group_id == 28, age_gap := "@0to1"]
ages <- ages[, list(age_group_id, age_gap, age_group_name, age_group_years_start, age_group_years_end)]

new_census_data <- merge(new_census_data, ages, by = 'age_group_id', all.x = T)
new_census_data[, c('age_group_id') := NULL]

new_census_data <- new_census_data[, list(location_id, year_id, sex_id, method_id, outlier_type_id, record_type_id, sub_agg, source_type_id, detailed_source, nid, underlying_nid, precise_year, age_gap, outlier, mean)]
new_census_data <- dcast.data.table(
  new_census_data,
  location_id + year_id + sex_id + method_id + outlier_type_id + record_type_id + sub_agg + source_type_id + detailed_source + nid + underlying_nid + precise_year + outlier ~ age_gap,
  value.var = "mean",
  fill = NA_real_
)

# Create a data set containing location-year-sex combinations where pop registry data is marked best
pop_registry_best <- new_census_data[source_type_id == 52 & outlier == 0, list(location_id, year_id, sex_id, pop_registry_best = 1)]

# Merge the pop registry metadata onto the population data
new_census_data <- merge(
  new_census_data,
  pop_registry_best,
  by = c('location_id', 'year_id', 'sex_id'),
  all.x = TRUE
)
new_census_data[is.na(pop_registry_best), pop_registry_best := 0]

# For countries where pop registry is marked best, we want to see if there is census data that we can use
new_census_data[, dup := 0]
new_census_data[
  outlier == 1 & source_type_id == 5 & pop_registry_best == 1 &
    (duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)]) |
       duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)], fromLast = TRUE)),
  dup := 1
]

# Of the census data that is in a year where pop registry is marked best, we un-outlier it so that we can apply the source hierarchy rules
new_census_data[dup == 1, outlier := 0]

# Out of the duplicates, we want to apply our source selection hierarchy to only take one
new_census_data[(dup == 1 & grepl("IPUMS", detailed_source)), outlier := 1]
new_census_data[, dup := NULL]

new_census_data[, dup := 0]
new_census_data[
  outlier == 0 & source_type_id == 5 & pop_registry_best == 1 &
    (duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)]) |
       duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)], fromLast = TRUE)),
  dup := 1
]

new_census_data[dup == 1, outlier := 0]

nrow(new_census_data[outlier == 0 & (duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)]))])

new_census_data[(dup == 1 & grepl("Mitchell", detailed_source)), outlier := 1]
new_census_data[, dup := NULL]

new_census_data[
  outlier == 0 & source_type_id == 5 & pop_registry_best == 1 &
    (duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)]) |
       duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)], fromLast = TRUE)),
  dup := 1
]
new_census_data[is.na(dup), dup := 0]

new_census_data[(dup == 1 & grepl("DYB", detailed_source)), outlier := 1]
new_census_data[, dup := NULL]

# Check for duplicates of data that comes from the same source but one is a custom subnational aggregation
new_census_data[
  outlier == 0 & source_type_id == 5 & pop_registry_best == 1 &
    (duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)]) |
       duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)], fromLast = TRUE)),
  dup := 1
]
new_census_data[is.na(dup), dup := 0]

# We prefer data that is not aggregated from the subnational
new_census_data[(dup == 1 & sub_agg == 1), outlier := 1]
new_census_data[, dup := NULL]

# Check for duplicates based on de jure vs de facto
new_census_data[
  outlier == 0 & source_type_id == 5 & pop_registry_best == 1 &
    (duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)]) |
       duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)], fromLast = TRUE)),
  dup := 1
]
new_census_data[is.na(dup), dup := 0]

# We prefer de facto
new_census_data[(dup == 1 & method_id == 14), outlier := 1]
new_census_data[, dup := NULL]

nrow(new_census_data[outlier == 0 & (duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)]))])

if (nrow(new_census_data[outlier == 0 & (duplicated(new_census_data[, list(location_id, year_id, sex_id, source_type_id, outlier)]))]) > 0) stop("There are duplicates in your data")

# Manual changes based on previous GBD Rounds

supas_data <- new_census_data[location_id == 11 & source_type_id == 5]
supas_data[, source_type_id := 39]
new_census_data <- data.table(rbind.fill(new_census_data, supas_data))
new_census_data[location_id == 77 & year_id < 1974 & source_type_id == 5, outlier := 1]
new_census_data[location_id == 145, outlier := 1]
pop_census_srs <- new_census_data[location_id %in% c(161, 165) & source_type_id == 5]
pop_census_srs[, source_type_id := 2]
new_census_data <- data.table(rbind.fill(new_census_data, pop_census_srs))

#### Bring in new SRS
for (srs_year in 2017:2020) {

  if (srs_year == 2017) {
    srs_pop <- setDT(read.dta13("FILEPATH"))
    srs_pop[, underlying_pop_nid := 331136]
  } else if(srs_year == 2018) {
    srs_pop <- setDT(read.dta13("FILEPATH"))
    srs_pop[, underlying_pop_nid := 449948]
  } else if(srs_year == 2019) {
    srs_pop <- setDT(read.dta13("FILEPATH"))
    srs_pop[, underlying_pop_nid := 498546]
  } else if(srs_year == 2020) {
    srs_pop <- setDT(read.dta13("FILEPATH"))
    srs_pop[, underlying_pop_nid := 522751]
  }

  setnames(srs_pop, c("COUNTRY", "NID", "CENSUS_SOURCE", "SEX"), c("ihme_loc_id", "nid", "detailed_source", "sex_id"))

  srs_pop[, AREA := NULL]
  srs_pop[, VR_SOURCE := NULL]
  srs_pop[, SUBDIV := NULL]

  srs_pop[, source_type := "SRS"]
  srs_pop[, status := "best"]
  srs_pop[, new := TRUE]
  srs_pop[, outlier := 0]
  srs_pop[, pop_footnote := ""]
  srs_pop[, year_id := srs_year]
  srs_pop[, precise_year := srs_year + 0.5]

  # Create both sexes data
  srs_pop <- srs_pop[sex_id != 3]
  both_sex_srs_pop <- copy(srs_pop)
  both_sex_srs_pop[, sex_id := NULL]
  datum_cols_ind_srs_pop <- grep("^DATUM", names(both_sex_srs_pop), value = T)
  both_sex_srs_pop <- both_sex_srs_pop[, lapply(.SD, function(x)replace(x, which(is.na(x)), -999)), .SDcols = datum_cols_ind_srs_pop, by = c('ihme_loc_id', 'year_id', 'source_type','detailed_source', 'pop_footnote', 'nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]

  both_sex_srs_pop <- both_sex_srs_pop[, lapply(.SD, sum, na.rm = T), .SDcols = grep("^DAT", names(both_sex_srs_pop), value = T), by = c('ihme_loc_id', 'year_id', 'source_type', 'detailed_source', 'pop_footnote', 'nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]
  both_sex_srs_pop <- both_sex_srs_pop[, lapply(.SD, function(x) replace(x, which(x<0), NA)), .SDcols = datum_cols_ind_srs_pop, by = c('ihme_loc_id', 'year_id', 'source_type','detailed_source', 'pop_footnote', 'nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]
  both_sex_srs_pop[, sex_id := 3]

  srs_pop <- setDT(rbind(srs_pop, both_sex_srs_pop))

  # Create Old AP
  old_ap_srs <- copy(srs_pop)
  old_ap_srs <- old_ap_srs[ihme_loc_id %in% c("IND_4871", "IND_4841")]
  old_ap_srs[, ihme_loc_id := NULL]
  old_ap_srs_datum_cols <- grep("^DATUM", names(old_ap_srs), value = T)
  old_ap_srs <- old_ap_srs[, lapply(.SD, function(x)replace(x, which(is.na(x)), -999)), .SDcols = old_ap_srs_datum_cols, by = c('sex_id', 'year_id', 'source_type','detailed_source', 'pop_footnote', 'nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]

  old_ap_srs <- old_ap_srs[, lapply(.SD, sum, na.rm = T), .SDcols = grep("^DAT", names(old_ap_srs), value = T), by = c('sex_id', 'year_id', 'source_type','detailed_source', 'pop_footnote', 'nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]
  old_ap_srs <- old_ap_srs[, lapply(.SD, function(x) replace(x, which(x<0), NA)), .SDcols = old_ap_srs_datum_cols, by = c('sex_id', 'year_id', 'source_type', 'detailed_source','pop_footnote', 'nid', 'underlying_pop_nid', 'new', 'outlier', 'status', 'precise_year')]
  old_ap_srs[, ihme_loc_id := "IND_44849"]
  srs_pop <- data.table(rbind(srs_pop, old_ap_srs))

  setnames(srs_pop, 'underlying_pop_nid', 'underlying_nid')

  names(srs_pop) <- gsub("DATUM", "@", names(srs_pop))

  # Merge on location id
  srs_pop <- merge(
    srs_pop,
    locations[, list(ihme_loc_id, location_id)],
    by = 'ihme_loc_id'
  )
  srs_pop[, ihme_loc_id := NULL]
  srs_pop[, source_type_id := 2]

  setdiff(names(srs_pop), names(new_census_data))
  setdiff(names(new_census_data), names(srs_pop))

  new_census_data <- data.table(rbind.fill(new_census_data, srs_pop))

}

#### Bring in new CHN DSP
for (dsp_year in 2016:2021) {

  dsp_pop <- fread(paste0("FILEPATH"))

  dsp_pop[, source_type := "DSP"]
  dsp_pop[, status := "best"]
  dsp_pop[, new := TRUE]
  dsp_pop[, outlier := 0]
  dsp_pop[, pop_footnote := ""]
  dsp_pop[, precise_year := dsp_year + 0.5]

  dsp_pop[, ihme_loc_id := NULL]

  dsp_pop <- merge(
    dsp_pop[, !c("age_start", "age_end")],
    ages,
    by = "age_group_id",
    all.x = TRUE
  )

  dsp_pop <- dsp_pop[, list(location_id, year_id, sex_id, source_type_id, detailed_source, nid, precise_year, age_gap, outlier, mean)]
  dsp_pop <- dcast.data.table(
    dsp_pop,
    location_id + year_id + sex_id + source_type_id + detailed_source + nid + precise_year + outlier ~ age_gap,
    value.var = "mean",
    fill = NA_real_
  )

  setdiff(names(dsp_pop), names(new_census_data))
  setdiff(names(new_census_data), names(dsp_pop))

  new_census_data <- data.table(rbind.fill(new_census_data, dsp_pop))

}

#### Bring in new CHN SPCC
sspc_pop <- fread("FILEPATH")

sspc_pop[, detailed_source := "CHN_SPCC"]
sspc_pop[, source_type := "SPCC"]
sspc_pop[, source_type_id := 36]

sspc_pop[, status := "best"]
sspc_pop[, new := TRUE]
sspc_pop[, outlier := 0]
sspc_pop[, pop_footnote := ""]
sspc_pop[, precise_year := year_id + 0.5]

sspc_pop[, ihme_loc_id := NULL]

sspc_pop[age_end == 125, age_end := 124]

sspc_pop <- merge(
  sspc_pop[, !c("deaths")],
  ages,
  by.x = c("age_start", "age_end"),
  by.y = c("age_group_years_start", "age_group_years_end"),
  all.x = TRUE
)
setnames(sspc_pop, "pop", "mean")

sspc_pop <- sspc_pop[, list(location_id, year_id, sex_id, source_type_id, detailed_source, nid, precise_year, age_gap, outlier, mean)]
sspc_pop <- dcast.data.table(
  sspc_pop,
  location_id + year_id + sex_id + source_type_id + detailed_source + nid + precise_year + outlier ~ age_gap,
  value.var = "mean",
  fill = NA_real_
)

setdiff(names(sspc_pop), names(new_census_data))
setdiff(names(new_census_data), names(sspc_pop))

new_census_data <- data.table(rbind.fill(new_census_data, sspc_pop[, !c("@0to124")]))

new_census_data[, new := TRUE]
new_census_data[, c("pop_registry_best", "outlier_type_id", "method_id", "record_type_id", "sub_agg") := NULL]
empirical_pop_gbd2017[, new := FALSE]

empirical_pop_gbd2017 <- merge(empirical_pop_gbd2017, ages, by = "age_group_id", all.x = TRUE)
empirical_pop_gbd2017[, "age_group_id" := NULL]

empirical_pop_gbd2017 <- empirical_pop_gbd2017[, list(location_id, year_id, sex_id, source_type_id, detailed_source, nid, underlying_nid, precise_year, age_gap, outlier, mean)]

# Drop duplicates
empirical_pop_gbd2017 <- empirical_pop_gbd2017[!duplicated(empirical_pop_gbd2017)]
empirical_pop_gbd2017 <- dcast.data.table(
  empirical_pop_gbd2017,
  location_id + year_id + sex_id + source_type_id + detailed_source + nid + underlying_nid + precise_year + outlier ~ age_gap,
  value.var = "mean",
  fill = NA_real_
)
empirical_pop_gbd2017[, new := FALSE]

empirical_pop_gbd2017[nid %in% sspc_pop$nid, outlier := 1]

empirical_pop <- rbind(empirical_pop_gbd2017, new_census_data, fill = TRUE)

# Find overlapping data
empirical_pop[
  outlier == 0 &
    (duplicated(empirical_pop[, list(location_id, year_id, sex_id, source_type_id, outlier)]) |
       duplicated(empirical_pop[, list(location_id, year_id, sex_id, source_type_id, outlier)], fromLast = TRUE)),
  dup := 1
]
empirical_pop[is.na(dup), dup := 0]

empirical_pop[new == FALSE & dup == 1, outlier := 1]

# Check if there are duplicates
dups_pop <- empirical_pop[
  outlier == 0 &
    (duplicated(empirical_pop[, list(location_id, year_id, sex_id, source_type_id, outlier)]) |
       duplicated(empirical_pop[, list(location_id, year_id, sex_id, source_type_id, outlier)], fromLast = TRUE))
]
if(nrow(dups_pop) > 0) stop("There are duplicates in your data")

# Split out population registry data into 5 year increments.
registry_locs <- c(80)
for (loc in registry_locs) {

  # Subset data to specific location and if it's registry type and pull all of the years
  if (loc == "FRA") registry_data <- empirical_pop[(location_id  == loc & source_type_id == 5 & year_id >= 2006)]
  else registry_data <- empirical_pop[(location_id == loc & source_type_id == 5)]

  tmp_data_years <- sort(unique(registry_data$year_id))

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
  print(use_years)
  if (loc == 80) empirical_pop[(location_id == loc & source_type_id == 5 & year_id >= 2006 & !(year_id %in% use_years)), outlier := 1]
  else empirical_pop[(location_id == loc & source_type_id == 52 & !(year_id %in% use_years)), outlier := 1]

}

# Create DATUM Variables
setnames(empirical_pop, grep("@", colnames(empirical_pop)), gsub("@", "DATUM", grep("@", names(empirical_pop), value = T)))

# Reshape long
empirical_pop <- empirical_pop[, c('location_id', 'year_id', 'sex_id', 'source_type_id', 'detailed_source', 'precise_year', 'nid', 'underlying_nid', 'outlier', grep("DATUM", names(empirical_pop), value = T)), with = F]
empirical_pop <- melt(
  empirical_pop,
  id.vars = c('location_id', 'year_id', 'sex_id', 'source_type_id', 'detailed_source', 'precise_year', 'nid', 'underlying_nid', 'outlier'),
  variable.name = "age_gap",
  measure.vars = c(grep("DATUM", names(empirical_pop), value = T))
)

empirical_pop[, age_gap := gsub("DATUM", "", age_gap)]
empirical_pop <- empirical_pop[!is.na(value)]

# Get age group_id
ages <- data.table(get_age_map(gbd_year = gbd_year, type = 'all'))
ages[, age_group_name := gsub("years", "", age_group_name)]
ages[, age_group_name := gsub("-", "to", age_group_name)]
ages[, start := as.character(age_group_years_start)]
ages[grepl(" ", age_group_name), age_gap := gsub(" ", "", age_group_name)]
ages[!grepl(" ", age_group_name), age_gap := paste0(start, "to", start)]

# Manual fixes
ages[age_group_id == 283, age_gap := "UNK"]
ages[age_group_id == 22, age_gap := "TOT"]
ages[age_group_id == 24, age_gap := "15to49"]
ages[age_group_id == 26, age_gap := "70plus"]
ages[age_group_id == 29, age_gap := "15plus"]
ages[age_group_id == 356, age_gap := "120plus"]
ages <- ages[!age_group_id %in% c(308, 42, 27, 236, 164, 161)]
ages[age_group_id == 1, age_gap := "0to4"]
ages[age_group_id == 28, age_gap := "0to1"]
ages <- ages[, list(age_group_id, age_gap, age_group_name)]

# Merge age metadata onto population data
empirical_pop <- merge(empirical_pop, ages, by = "age_gap", all.x = TRUE)

# These would be the remaining data points that were DATUM0to0
if(nrow(empirical_pop[is.na(age_group_id) & age_gap != "0to0"]) > 0) stop("There are missing age group id's other than 0to0")
empirical_pop[is.na(age_group_id) & age_gap == "0to0", age_group_id := 28]

empirical_pop[, c("age_gap", "age_group_name") := NULL]

assert_values(empirical_pop, c('year_id', 'precise_year', 'location_id', 'sex_id', 'age_group_id', 'source_type_id', 'detailed_source', 'mean', 'outlier'), test = "not_na")
assert_values(empirical_pop, c('mean'), "gte", 0)
names(empirical_pop)

setnames(empirical_pop, "value", "mean")

dlhs <- empirical_pop[sex_id %in% c(1, 2) & detailed_source %like% "DLHS"]
empirical_pop <- empirical_pop[!(sex_id == 3 & detailed_source %like% "DLHS")]

by_cols <- grep("mean|sex", names(dlhs), invert = T, value = T)
dlhs <- dlhs[, .(mean = sum(mean)), by = by_cols]
dlhs[, sex_id := 3]

empirical_pop <- rbind(empirical_pop, dlhs)

# Write to csv
output_dir <- paste0("FILEPATH")
if(!dir.exists(output_dir)) {
  dir.create(output_dir)
}
fwrite(empirical_pop, paste0("FILEPATH"))

upload_results(
  filepath = paste0("FILEPATH"),
  model_name = "population empirical",
  model_type = "data",
  run_id = version_id,
  send_slack = TRUE
)

update_status(
  model_name = "population empirical",
  model_type = "data",
  run_id = version_id,
  new_status = "best"
)
