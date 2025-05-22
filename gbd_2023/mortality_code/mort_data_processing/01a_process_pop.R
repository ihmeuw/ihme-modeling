
## Meta ------------------------------------------------------------------------

# Description: Download and process population data

# Steps:
#   1.  Initial setup
#   2.  Read in internal inputs
#   3.  Create mapping of common population splits
#   4.  Get baseline pop
#   5.  Read in population
#   6.  Create splitting proportions
#   7.  Read in empirical populations
#   8.  Perform splitting
#   9.  Perform aggregations
#   10. Save
#   11. Generate VR-specific empirical pop

# Inputs:
#   *    "FILEPATH": detailed configuration file.
#   *    Empirical population
#   *    "FILEPATH": prepped IND SRS populations from setup
# Outputs:
#   *    "FILEPATH"

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)
library(demInternal)
library(mortdb)

# Setup ------------------------------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir",
  type = "character",
  required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# Define the main directory here
if (interactive()) {
  main_dir <- "INSERT_PATH_HERE"
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
}

# get config
config <- config::get(
  file = paste0("FILEPATH"),
  use_parent = FALSE
)
list2env(config$default, .GlobalEnv)

# Read in internal inputs ------------------------------------------------------

age_map_gbd <- fread(fs::path("FILEPATH"))
age_map_extended <- fread(fs::path("FILEPATH"))

all_ages <- mortdb::get_age_map(type = "all", gbd_year = gbd_year)
all_ages <- all_ages[,
  .(age_group_id, age_start = age_group_years_start, age_end = age_group_years_end)
]

source_map <- mortdb::get_mort_ids("source_type")

locs <- fread(fs::path("FILEPATH"))

# Create mapping of common population splits -----------------------------------

split_map <- rbind(

  data.table(
    age_start_split = age_map_gbd[age_end <= 1, age_start],
    age_end_split = age_map_gbd[age_end <= 1, age_end],
    age_start_agg = 0,
    age_end_agg = 1
  ),

  data.table(
    age_start_split = age_map_gbd[age_start >= 1 & age_end <= 5, age_start],
    age_end_split = age_map_gbd[age_start >= 1 & age_end <= 5, age_end],
    age_start_agg = 1,
    age_end_agg = 5
  ),

  data.table(
    age_start_split = age_map_gbd[age_end <= 5, age_start],
    age_end_split = age_map_gbd[age_end <= 5, age_end],
    age_start_agg = 0,
    age_end_agg = 5
  ),

  data.table(
    age_start_split = c(0, 1),
    age_end_split = c(1, 5),
    age_start_agg = 0,
    age_end_agg = 5
  ),

  data.table(
    age_start_split = age_map_gbd[age_end <= 10, age_start],
    age_end_split = age_map_gbd[age_end <= 10, age_end],
    age_start_agg = 0,
    age_end_agg = 10
  ),

  data.table(
    age_start_split = age_map_gbd[, age_start],
    age_end_split = age_map_gbd[, age_end],
    age_start_agg = 0,
    age_end_agg = 125
  )

)

# append the splitting of all 10 year age groups between 10-90 to the split map
for (a in seq(10, 80, 10)) {

  temp <- data.table(
    age_start_split = age_map_gbd[age_start >= a & age_end <= (a + 10), age_start],
    age_end_split = age_map_gbd[age_start >= a & age_end <= (a + 10), age_end],
    age_start_agg = a,
    age_end_agg = a + 10
  )

  split_map <- rbind(split_map, temp)

}

# append splitting of all open age intervals beginning prior to age 95 to split map
for (a in seq(60, 90, 5)) {

  temp <- data.table(
    age_start_split = age_map_gbd[age_start >= a, age_start],
    age_end_split = age_map_gbd[age_start >= a, age_end],
    age_start_agg = a,
    age_end_agg = 125
  )

  split_map <- rbind(split_map, temp)

}

# Read in population -----------------------------------------------------------

# population estimates
pop <- mortdb::get_mort_outputs(
  model_name = "population",
  model_type = "estimate",
  run_id = pop_version
)[!is.na(ihme_loc_id)]
pop <- merge(pop, all_ages, by = "age_group_id")

pop <- pop[, .(ihme_loc_id, year_id, sex_id, age_start, age_end, population = mean)]

# create aggregates in split map
agg_map <- unique(split_map[, . (age_start = age_start_agg, age_end = age_end_agg)])

pop_age_map <- unique(pop[, .(age_start, age_end, pop = TRUE)])

# exclude existing aggregates
agg_map <- merge(
  agg_map,
  pop_age_map,
  by = c("age_start", "age_end"),
  all.x = TRUE
)
agg_map <- agg_map[is.na(pop)]
agg_map[, pop := NULL]

pop_aggs <- hierarchyUtils::agg(
  dt = pop[age_start < age_end],
  id_cols = setdiff(names(pop), "population"),
  col_stem = "age",
  col_type = "interval",
  value_cols = "population",
  mapping = agg_map,
  overlapping_dt_severity = "warning"
)

pop <- rbind(pop, pop_aggs)

demUtils::assert_is_unique_dt(pop, c("ihme_loc_id", "year_id", "sex_id", "age_start", "age_end"))

# round pop to match ages in split map
pop <- pop[, age_start := round(age_start, 7)]
pop <- pop[, age_end := round(age_end, 7)]

# Create splitting proportions -------------------------------------------------

split_map <- merge(
  split_map,
  pop,
  by.x = c("age_start_agg", "age_end_agg"),
  by.y = c("age_start", "age_end"),
  allow.cartesian = TRUE
)

setnames(split_map, "population", "agg_population")

split_map <- merge(
  split_map,
  pop,
  by.x = c("ihme_loc_id", "year_id", "sex_id", "age_start_split", "age_end_split"),
  by.y = c("ihme_loc_id", "year_id", "sex_id", "age_start", "age_end")
)

split_map[, prop := population / agg_population]

assertable::assert_values(split_map, "prop", "lt", 1)
assertable::assert_values(split_map, "prop", "gt", 0)

split_map[, c("agg_population", "population") := NULL]

# Read in empirical population -------------------------------------------------

emp_pop <- mortdb::get_mort_outputs(
  model_name = "population empirical",
  model_type = "data",
  run_id = data_emp_pop_version,
  gbd_year = gbd_year
)
emp_pop <- emp_pop[outlier == 0]

# add comsa data to be split
comsa_additions_moz <- data.table(
  year_id = 2018:2022,
  location_id = 184,
  ihme_loc_id = "MOZ",
  sex_id = 3,
  age_group_id = 22,
  source_type_id = 2,
  source_name = "SRS",
  detailed_source = "COMSA",
  nid = 459445,
  underlying_nid = NA,
  mean = 865349
)
comsa_additions_moz[, precise_year := year_id + 0.5]

comsa_additions_sle <- data.table(
  year_id = 2016:2020,
  location_id = 217,
  ihme_loc_id = "SLE",
  sex_id = 3,
  age_group_id = 22,
  source_type_id = 2,
  source_name = "SRS",
  detailed_source = "COMSA",
  nid = 524714,
  underlying_nid = NA,
  mean = 343123
)
comsa_additions_sle[, precise_year := year_id + 0.5]

# sex split comsa data by gbd proportion
comsa_additions <- rbind(comsa_additions_moz, comsa_additions_sle)
pop_comsa <- pop[
  age_start == 0 & age_end == 125 &
    ((ihme_loc_id == "MOZ" & year_id %in% 2018:2022) | (ihme_loc_id == "SLE" & year_id %in% 2016:2020))
]
pop_comsa <- dcast(
  pop_comsa,
  ihme_loc_id + year_id + age_start + age_end ~ sex_id,
  value.var = "population"
)
pop_comsa[, female_prop := `2` / `3`]
pop_comsa[, male_prop := `1` / `3`]
pop_comsa[, ':=' (`1` = NULL, `2` = NULL, `3` = NULL)]
comsa_additions <- merge(
  comsa_additions,
  pop_comsa,
  by = c("ihme_loc_id", "year_id"),
  all.x = TRUE
)
comsa_additions[, `1` := mean * male_prop]
comsa_additions[, `2` := mean * female_prop]
comsa_additions[, ':=' (male_prop = NULL, female_prop = NULL, mean = NULL, sex_id = NULL, age_start = NULL, age_end = NULL)]
comsa_additions <- melt(
  comsa_additions,
  id.vars = c(
    "location_id", "ihme_loc_id", "year_id", "source_type_id", "source_name",
    "detailed_source", "age_group_id", "nid", "underlying_nid", "precise_year"
  ),
  variable.name = "sex_id",
  value.name = "mean"
)
comsa_additions[, sex_id := as.numeric(as.character(sex_id))]

emp_pop <- rbind(emp_pop, comsa_additions, fill = TRUE)

emp_pop <- merge(
  emp_pop,
  all_ages,
  by = "age_group_id"
)

# Standardize columns
emp_pop <- emp_pop[,
  .(ihme_loc_id, location_id, year_id, precise_year,
    sex_id, source_type = source_name,
    age_start = round(age_start, 7), age_end = round(age_end, 7),
    pop_nid = nid, underlying_pop_nid = underlying_nid,
    census_pop = mean)
]

# Drop old round locations
emp_pop <- emp_pop[!is.na(ihme_loc_id)]

# append and prioritize latest IND SRS pop extractions
srs_pop <- fread(fs::path("FILEPATH"))
setnames(srs_pop, "nid", "pop_nid")
srs_pop[, underlying_pop_nid := NA_real_][, precise_year := year_id][, source_type := "SRS"]
srs_pop <- merge(
  srs_pop,
  locs[, .(ihme_loc_id, location_id)],
  by = "ihme_loc_id",
  all.x = TRUE
)

srs_pop[, new_srs := TRUE]

emp_pop <- rbind(emp_pop, srs_pop, fill = TRUE)
emp_pop[,
  dup := .N,
  by = c("ihme_loc_id", "year_id", "sex_id", "age_start", "age_end", "source_type")
]
emp_pop <- emp_pop[dup == 1 | (dup == 2 & !is.na(new_srs))]
emp_pop[, c("dup", "new_srs") := NULL]

demUtils::assert_is_unique_dt(
  emp_pop,
  c("ihme_loc_id", "year_id", "sex_id", "age_start", "age_end", "source_type")
)

# Make sure all source types exist in the data before merging simplest
# source_type_id
if (!all(unique(emp_pop$source_type) %in% source_map$type_short)) {
  stop ("Review source types in empirical pop")
}

emp_pop <- merge(
  emp_pop,
  source_map[, .(source_type_id, type_short)],
  by.x = "source_type",
  by.y = "type_short"
)

# Perform splitting ------------------------------------------------------------

# NOTE: not splitting unknowns
split_emp_pop <- merge(
  emp_pop,
  split_map,
  by.x = c("ihme_loc_id", "year_id", "sex_id", "age_start", "age_end"),
  by.y = c("ihme_loc_id", "year_id", "sex_id", "age_start_agg", "age_end_agg")
)

# prioritize split from most granular age group
split_emp_pop[, agg_age_length := age_end - age_start]
split_emp_pop[,
  min_agg_age_length := min(agg_age_length),
  by = c("source_type", "ihme_loc_id", "year_id", "sex_id", "age_start_split", "age_end_split")
]
split_emp_pop <- split_emp_pop[agg_age_length == min_agg_age_length]

split_emp_pop[, census_pop := census_pop * prop]
split_emp_pop[, age_start := age_start_split][, age_end := age_end_split]
split_emp_pop[,
  c("age_start_split", "age_end_split", "prop", "agg_age_length", "min_agg_age_length") := NULL
]

split_emp_pop[, process_type := "split"]

# combine data and prioritize non-split values
emp_pop <- rbind(emp_pop, split_emp_pop, fill = TRUE)

emp_pop[,
  dup := .N,
  by = c("source_type", "ihme_loc_id", "year_id", "sex_id", "age_start", "age_end")
]

assertable::assert_values(emp_pop, "dup", "lte", 2)

emp_pop <- emp_pop[dup == 1 | (is.na(process_type) & dup == 2)]

emp_pop[, dup := NULL]
emp_pop[is.na(process_type), process_type := "raw"]

demUtils::assert_is_unique_dt(
  emp_pop,
  c("ihme_loc_id", "year_id", "sex_id", "age_start", "age_end", "source_type")
)

# Perform aggregation ----------------------------------------------------------

# aggregate to all GBD age groups and prioritize existing data
agg_emp_pop <- copy(emp_pop)
agg_emp_pop[, process_type := NULL]
agg_emp_pop <- agg_emp_pop[!((age_end - age_start) == 0)]
agg_emp_pop <- agg_emp_pop[!(age_end == 125 & age_start < 95)]
agg_emp_pop <- agg_emp_pop[!((age_end - age_start) > 5 & age_end != 125)]

agg_emp_pop <- hierarchyUtils::agg(
  dt = agg_emp_pop,
  id_cols = setdiff(names(agg_emp_pop), "census_pop"),
  value_cols = "census_pop",
  col_stem = "age",
  col_type = "interval",
  mapping = age_map_gbd[, .(age_start, age_end)],
  missing_dt_severity = "warning",
  present_agg_severity = "warning",
  overlapping_dt_severity = "warning",
  na_value_severity = "warning"
)

agg_emp_pop[, process_type := "agg"]

emp_pop <- rbind(emp_pop, agg_emp_pop)

emp_pop[,
  dup := .N,
  by = c("source_type", "ihme_loc_id", "year_id", "sex_id", "age_start", "age_end")
]

assertable::assert_values(emp_pop, "dup", "lte", 2)

emp_pop <- emp_pop[dup == 1 | (process_type != "agg" & dup == 2)]

emp_pop[, dup := NULL]

demUtils::assert_is_unique_dt(
  emp_pop,
  c("ihme_loc_id", "year_id", "sex_id", "age_start", "age_end", "source_type")
)

# create non-standard aggregates
agg_map <- data.table(
  age_start = c(0, 1, 5, 15, 45, 60),
  age_end = c(1, 5, 15, 45, 60, 125)
)
incomp_agg_map <- data.table(
  age_start = c(0, 0, 0.0767123, 0.0767123, 0, 50, 60, 15, 75, 80, 85),
  age_end = c(5, 0.0767123, 1, 15, 125, 60, 70, 125, 125, 125, 125)
)

agg_map <- rbind(agg_map, incomp_agg_map)

agg_emp_pop <- copy(emp_pop)
agg_emp_pop[, process_type := NULL]
agg_emp_pop <- agg_emp_pop[!((age_end - age_start) == 0)]

agg_emp_pop <- hierarchyUtils::agg(
  dt = agg_emp_pop,
  id_cols = setdiff(names(agg_emp_pop), "census_pop"),
  value_cols = "census_pop",
  col_stem = "age",
  col_type = "interval",
  mapping = agg_map,
  missing_dt_severity = "warning",
  present_agg_severity = "warning",
  overlapping_dt_severity = "warning",
  na_value_severity = "warning"
)
agg_emp_pop[, process_type := "agg"]

agg_emp_pop[,
  dup := .N,
  by = c("source_type", "ihme_loc_id", "year_id", "sex_id", "age_start", "age_end")
]
agg_emp_pop <- agg_emp_pop[!(dup == 2 & year_id != precise_year)]
agg_emp_pop[, dup := NULL]

emp_pop <- rbind(emp_pop, agg_emp_pop)

emp_pop[,
  dup := .N,
  by = c("source_type", "ihme_loc_id", "year_id", "sex_id", "age_start", "age_end")
]

assertable::assert_values(emp_pop, "dup", "lte", 2)

emp_pop <- emp_pop[dup == 1 | (process_type != "agg" & dup == 2)]

emp_pop[, dup := NULL]

demUtils::assert_is_unique_dt(
  emp_pop,
  c("ihme_loc_id", "year_id", "sex_id", "age_start", "age_end", "source_type")
)

# Save -------------------------------------------------------------------------

readr::write_csv(
  emp_pop,
  fs::path("FILEPATH")
)

## Generate VR-specific empirical pop ------------------------------------------

emp_pop_split  <- copy(emp_pop)

# use total population for unknown ages
emp_pop_split[age_map_extended[!age_group_id == 27], age_group_id := i.age_group_id, on = c("age_start", "age_end")]
emp_pop_unknown_age <- emp_pop_split[age_group_id == 22]
emp_pop_unknown_age[, ':=' (age_group_id = 283, age_start = 999, age_end = 999, process_type = "copy")]
emp_pop_split <- rbind(emp_pop_split, emp_pop_unknown_age)
# append on comsa sex id 3 pop data
emp_pop_comsa_allsexes <- emp_pop_split[pop_nid %in% c(459445, 524714)]
emp_pop_comsa_allsexes <- emp_pop_comsa_allsexes[, .(census_pop = sum(census_pop)), by = setdiff(names(emp_pop_comsa_allsexes), c("census_pop", "sex_id"))]
emp_pop_comsa_allsexes[, sex_id := 3]
emp_pop_split <- rbind(emp_pop_split, emp_pop_comsa_allsexes)
# use total population for unknown sexes
emp_pop_unknown_sex <- emp_pop_split[sex_id == 3]
emp_pop_unknown_sex[, ':=' (sex_id = 4, process_type = "copy")]
emp_pop_split <- rbind(emp_pop_split, emp_pop_unknown_sex)
emp_pop_split[
  ,
  dup := .N,
  by = c("source_type", "ihme_loc_id", "year_id", "sex_id", "age_start", "age_end", "pop_nid")
]
assertable::assert_values(emp_pop_split, "dup", "lte", 2)
emp_pop_split <- emp_pop_split[dup == 1 | (process_type != "copy" & dup == 2)]
emp_pop_split_no_vr <- emp_pop_split[!source_type_id == 1]

# save
readr::write_csv(emp_pop_split_no_vr, fs::path("FILEPATH"))
