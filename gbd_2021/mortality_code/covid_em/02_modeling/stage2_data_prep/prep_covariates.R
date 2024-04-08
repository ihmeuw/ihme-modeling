
# Meta --------------------------------------------------------------------

## DESCRIPTION
#  Prep covariates for COVID-em modeling

# Load libraries ----------------------------------------------------------

library(data.table)
library(dplyr)
library(ggplot2)
library(readr)
library(zoo)

source("")
source("")
source("")

# Set parameters ----------------------------------------------------------

out_dir <- fs::path("")
out_dir_gbd <- fs::path(out_dir, "gbd_covariates")

date <- format(Sys.time(), "%Y-%m-%d-%H-%M")

# get config
data_run_id <- ""
main_dir <- paste0()
config <- config::get(
  file = paste0(),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# locations
locs <- demInternal::get_locations(gbd_year = 2020)
covid_loc_map <- demInternal::get_locations(gbd_year = gbd_year, location_set_name = "COVID-19 modeling")
covid_only_locs <- covid_loc_map[!location_id %in% unique(locs$location_id)]
locs <- rbind(locs, covid_only_locs)
nor_locs_old <- demInternal::get_locations(gbd_year = 2019)
nor_locs_old <- nor_locs_old[(!location_id %in% unique(locs$location_id)) & (ihme_loc_id %like% "NOR_")]
locs_w_nor <- rbind(locs, nor_locs_old)

# location week types
location_week_types <- fread("")
locs_with_isoweek <- location_week_types[week_type == "isoweek", ihme_loc_id]
locs_with_epiweek <- location_week_types[week_type == "epiweek", ihme_loc_id]
locs_with_ukweek <- location_week_types[week_type == "ukweek", ihme_loc_id]

# Get data ---------------------------------------------------------------------

dt_sy_pop <- demInternal::get_dem_outputs(
  "population single year estimate",
  gbd_year = 2019,
  run_id = "best",
  year_ids = 2019,
  sex_ids = 3,
  name_cols = TRUE
)

dt_total_death_number <- demInternal::get_dem_outputs(
  "no shock death number estimate",
  gbd_year = 2019,
  run_id = "best",
  year_ids = 1990:2019,
  sex_ids = 3,
  age_group_ids = 22,
  estimate_stage_ids = 5,
  name_cols = TRUE
)

dt_pop <- demInternal::get_dem_outputs(
  "population estimate",
  gbd_year = 2019,
  run_id = "best",
  year_ids = 1990:2019
)
dt_total_pop <- dt_pop[age_group_id == 22 & sex_id == 3]

dt_pop_2020 <- demInternal::get_dem_outputs(
  "population estimate",
  gbd_year = 2020,
  run_id = "best",
  year_ids = 1990:2020
)
dt_total_pop_2020 <- dt_pop_2020[age_group_id == 22 & sex_id == 3]

dt_pop_2020_chn <- demInternal::get_dem_outputs(
  "population estimate",
  gbd_year = 2020,
  run_id = 300,
  location_ids = 6,
  year_ids = 1990:2020
)
dt_total_pop_2020 <- dt_pop_2020[age_group_id == 22 & sex_id == 3]


# Prep demographics covariates -------------------------------------------------

# Mean population age
dt_mean_pop_age <- dt_sy_pop[
  ,
  .(mean_pop_age = weighted.mean(age_start, mean / sum(mean))),
  by = .(location_id, ihme_loc_id, location_name)
]

# Crude all-age death rate
dt_crude_death_rate <- dt_total_death_number[
  dt_total_pop,
  .(location_id, ihme_loc_id, location_name, year_id, crude_death_rate = mean / i.mean),
  on = .(location_id, year_id),
  nomatch = NULL
]

dt_crude_death_rate_2019 <- dt_crude_death_rate[year_id == 2019, -"year_id"]

# Standard deviation of crude death rate over time
dt_crude_death_rate_sd_1990 <- dt_crude_death_rate[
  year_id >= 1990,
  .(crude_death_rate_sd = sd(crude_death_rate)),
  by = .(location_id, ihme_loc_id, location_name)
]

dt_crude_death_rate_sd_2000 <- dt_crude_death_rate[
  year_id >= 2000,
  .(crude_death_rate_sd = sd(crude_death_rate)),
  by = .(location_id, ihme_loc_id, location_name)
]

# Proportion of population at age X or older
setorderv(dt_sy_pop, c("location_id", "age_end", "age_start"))
dt_prop_pop <- dt_sy_pop[
  ,
  .(age_start, prop_pop_above = rev(cumsum(rev(mean))) / sum(mean)),
  by = .(location_id)
]


# Prep GBD covariates ----------------------------------------------------------

## Age/Sex split ##

# hypertension
dt_hypertension_raw <- fread(fs::path())

dt_hypertension <- merge(
  dt_hypertension_raw,
  dt_pop[, .(location_id, year_id, age_group_id, sex_id, population = mean)],
  by = c("location_id", "year_id", "age_group_id", "sex_id"),
  all.x = TRUE
)

dt_hypertension_total_2019 <- dt_hypertension[
  year_id == 2019,
  .(hypertension_prevalence = sum(prev_140 * population) / sum(population)),
  by = .(location_id)
]

# GBD Inpatient admissions
dt_inpatient_admis <- get_covariate_estimates(covariate_id = 2331,
                                              year_id = 2020,
                                              decomp_step = "iterative",
                                              gbd_round_id = mortdb::get_gbd_round(2020))

dt_inpatient_admis <- merge(
  dt_inpatient_admis,
  dt_pop_2020[, .(location_id, age_group_id, sex_id, population = mean)],
  by = c("location_id", "age_group_id", "sex_id"),
  all.x = TRUE
)

dt_inpatient_admis <- dt_inpatient_admis[,
  .(gbd_inpatient_admis = sum(mean_value * population) / sum(population)),
  by = .(location_id)
  ]

# GBD diabetes
dt_diabetes <- get_covariate_estimates(covariate_id = 30,
                                       year_id = 2000:2020,
                                       decomp_step = "iterative",
                                       gbd_round_id = mortdb::get_gbd_round(2019))

dt_diabetes <- merge(
  dt_diabetes,
  dt_pop[, .(location_id, year_id, age_group_id, sex_id, population = mean)],
  by = c("location_id", "year_id", "age_group_id", "sex_id"),
  all.x = TRUE
)

dt_diabetes <- dt_diabetes[
  year_id == 2019,
  .(gbd_diabetes = sum(mean_value * population) / sum(population)),
  by = .(location_id)
  ]

## All ages/sexes covariates ##

# smoking
dt_smoking <- fread(fs::path())
dt_smoking <- merge(
  dt_smoking,
  dt_total_pop[, c("location_id","year_id","mean")],
  by = c("location_id","year_id"),
  all.x = TRUE
)
dt_smoking[, ':=' (mean_pop_smoke_num = mean_pop_smoke_num / mean,
                   lower_pop_smoke_num = lower_pop_smoke_num / mean,
                   upper_pop_smoke_num = upper_pop_smoke_num /mean)]
setnames(dt_smoking, c("mean_pop_smoke_num","lower_pop_smoke_num","upper_pop_smoke_num"),
                       c("mean_value","lower_value","upper_value"))
dt_smoking[, mean := NULL]

# universal health coverage
dt_uhc <- get_covariate_estimates(
  covariate_id = 1097,
  year_id = 2019,
  gbd_round_id = 6,
  decomp_step = "iterative"
)

# absolute average latitude
dt_lat <- get_covariate_estimates(
  covariate_id = 3,
  year_id = 2019,
  gbd_round_id = 6,
  decomp_step = "iterative"
)

# HAQI
dt_haqi <- get_covariate_estimates(
  covariate_id = 1099,
  year_id = 2019,
  gbd_round_id = 6,
  decomp_step = "iterative"
)

# GBD Obesity
dt_obesity <- fread(fs::path())
dt_obesity <- dt_obesity[year_id == 2019]

## GBD causes ##

cause_list <- list(
  cvd = 491,
  ncd = 409,
  cong_downs = 645,
  diseaseckd = 589,
  resp_copd = 509,
  hemog_sickle = 615,
  subs = 973,
  resp_asthma = 515,
  cvd_pah = 1004,
  cirrhosis = 521,
  hemog_thalass = 614,
  neuro = 542,
  hiv = 298,
  neo = 410,
  cvd_stroke_cerhem = 496,
  endo = 619,
  diabetes = 587
)

dt_gbd_causes <- get_outputs(
  topic = "cause",
  cause_id = as.vector(cause_list),
  year_id = 2019,
  age_group_id = 22,
  location_id = "all",
  gbd_round_id = 6,
  decomp_step = "step5",
  metric_id = 3,
  measure_id = 1
)

dt_gbd_causes[, acause := gsub("^_", "", acause)]
setnames(dt_gbd_causes, c("val","upper","lower"), c("mean_value","upper_value","lower_value"))

dt_gbd_causes_list <- split(
  dt_gbd_causes[, .(location_id, year_id, acause, mean_value, upper_value, lower_value)],
  by = "acause",
  keep.by = FALSE
)


# Person years -----------------------------------------------------------------

# -------------------- gbd pop -------------------- #

population <- demInternal::get_dem_outputs(
  process_name = "population estimate",
  run_id = "best",
  gbd_year = gbd_year,
  year_ids = 1999:2022,
  location_ids = locs[!(ihme_loc_id %like% "CHN"), location_id],
  sex_ids = c(1,2,3),
  age_group_ids = c(1,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235,22),
  name_cols = TRUE,
  odbc_section = "HOST",
  odbc_dir = "~"
)

population_chn <- demInternal::get_dem_outputs(
  process_name = "population estimate",
  run_id = "best",
  gbd_year = gbd_year,
  year_ids = 1999:2022,
  location_ids = locs[ihme_loc_id %like% "CHN", location_id],
  sex_ids = c(1,2,3),
  age_group_ids = c(1,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235,22),
  name_cols = TRUE,
  odbc_section = "HOST",
  odbc_dir = "~"
)

population <- rbind(population, population_chn)

nor_pop_old <- demInternal::get_dem_outputs(
  process_name = "population estimate",
  run_id = 192,
  gbd_year = 2019,
  year_ids = 2000:2019,
  location_ids = nor_locs_old[, location_id],
  sex_ids = c(1,2,3),
  age_group_ids = c(1,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235,22),
  name_cols = TRUE,
  odbc_section = "HOST",
  odbc_dir =  "~"
)

# replicate 2019 for 2020 and 2021
nor_pop_old_2020 <- nor_pop_old[year_id == 2019]
nor_pop_old_2020[, ':=' (year_id = 2020, year_start = 2020)]
nor_pop_old_2021 <- nor_pop_old[year_id == 2019]
nor_pop_old_2021[, ':=' (year_id = 2021, year_start = 2021)]
nor_pop_old_2022 <- nor_pop_old[year_id == 2019]
nor_pop_old_2022[, ':=' (year_id = 2022, year_start = 2022)]

population <- rbind(population, nor_pop_old, nor_pop_old_2020, nor_pop_old_2021, nor_pop_old_2022)

# subset columns
setnames(population, "mean", "population")
cols <- c("location_id", "year_start", "sex", "age_start", "age_end", "population")
population <- population[, .SD, .SDcols = cols]
data.table::setkeyv(population, setdiff(cols, "population"))

population[, `:=` (type = "gbd", date_prepped = NA)]

population[is.infinite(age_end), age_end := 125]

# Combine ITA 35498 & 35499
ita99999 <- population[location_id %in% c(35498, 35499)]
ita99999[, location_id := 99999]
ita99999 <- ita99999[,
                     list(population = sum(population)),
                     by = c("location_id", "year_start", "sex", "age_start", "age_end",
                            "type", "date_prepped")
                     ]
population <- rbind(population, ita99999)

# add age name
population <- hierarchyUtils::gen_name(population, col_stem = "age")

population <- population[location_id %in% unique(c(locs_w_nor[, location_id], 99999)) &
                           sex == "all" & age_name == "0 to 125"]

# add ihme loc id
population <- merge(
  locs_w_nor[, c("location_id", "ihme_loc_id")],
  population,
  by = "location_id",
  all.y = TRUE
)
population[location_id == 99999, ihme_loc_id := "ITA_99999"]

# setup
pop_id_cols <- c("location_id", "ihme_loc_id", "sex", "age_name", "year_start", "type")
population <- population[, .SD, .SDcols = c(pop_id_cols, "population")]

gbd_population <- copy(population)

# -------------------- covid pop -------------------- #

covid_team_pop <- fread("")
covid_team_pop <- covid_team_pop[, list(population = sum(population)), by = "location_id"]

# set week to 26 for mid-year, plus extra variables
covid_team_pop[, `:=` (type = "covid_team", age_name = "0 to 125", sex = "all")]

# Use same values for 2020 and 2021
covid_team_pop <- setDT(cbind(covid_team_pop, year_start = rep(2018:2023, each = nrow(covid_team_pop))))

# subset to only missing locations
covid_team_pop_sub <- covid_team_pop[!location_id %in% unique(gbd_population$location_id)]

# add ihme loc id
covid_team_pop_sub <- merge(
  locs[, c("location_id", "ihme_loc_id")],
  covid_team_pop_sub,
  by = "location_id",
  all.y = TRUE
)

# only keep where ihme loc id is present
covid_team_pop_sub <- covid_team_pop_sub[!is.na(ihme_loc_id)]

covid_population <- copy(covid_team_pop_sub)

# -------------------- combine and run -------------------- #

all_population <- rbind(gbd_population, covid_population)

array_locs <- unique(all_population[, "ihme_loc_id"])
array_locs[, task_id := .I]

njobs <- nrow(array_locs)

date <- ""
dir.create(paste0(), recursive = T)
dir.create(paste0(), recursive = T)

readr::write_csv(array_locs, paste0())
readr::write_csv(all_population, paste0())

# submit qsub
image_dir <- ""
image_gbd_year_release <- ""

mortcore::array_qsub(
  jobname = "interpolate_pop",
  shell = glue::glue(""),
  code = "",
  cores = 1, mem = 2, wallclock = "00:10:00", archive_node = F,
  pass_shell = list(y = image_gbd_year_release),
  pass_argparse = list(task_map_path = paste0(),
                       date = date),
  num_tasks = njobs,
  queue = "all",
  proj = "proj_mortenvelope",
  submit = TRUE
)

pop_files <- list.files(paste0(), pattern = ".csv", full.names = T, recursive = T)
pop_all <- lapply(pop_files, fread) %>% rbindlist(use.names = T, fill = T)

readr::write_csv(pop_all, paste0())

# save final version
readr::write_csv(pop_all, paste0())
readr::write_csv(pop_all, fs::path())

old <- fread()
new <- fread()

test <- merge(
  old[, c("ihme_loc_id", "year_start", "time_unit", "time_start", "person_years")],
  new[, c("ihme_loc_id", "year_start", "time_unit", "time_start", "person_years")],
  by = c("ihme_loc_id", "year_start", "time_unit", "time_start"),
  all = TRUE
)

test[, diff := person_years.y - person_years.x]

dropped <- test[!is.na(person_years.x) & is.na(person_years.y)]
added <- test[is.na(person_years.x) & !is.na(person_years.y)]


# Save covariates --------------------------------------------------------------

Sys.umask("0113")

readr::write_csv(dt_mean_pop_age, fs::path())
readr::write_csv(dt_crude_death_rate_2019, fs::path())
readr::write_csv(dt_crude_death_rate_sd_1990, fs::path())
readr::write_csv(dt_crude_death_rate_sd_2000, fs::path())


for (age in c(60, 70, 75, 80, 85)) {

  readr::write_csv(
    dt_prop_pop[age_start == age],
    fs::path(out_dir, paste0())
  )

}

purrr::iwalk(
  dt_gbd_causes_list,
  ~readr::write_csv(.x, fs::path(, paste0()))
)

readr::write_csv(dt_hypertension_total_2019, fs::path())
readr::write_csv(dt_inpatient_admis, fs::path())
readr::write_csv(dt_diabetes, fs::path())
readr::write_csv(dt_smoking, fs::path())
readr::write_csv(dt_uhc, fs::path())
readr::write_csv(dt_lat, fs::path())
readr::write_csv(dt_haqi, fs::path())
readr::write_csv(dt_obesity, fs::path())
