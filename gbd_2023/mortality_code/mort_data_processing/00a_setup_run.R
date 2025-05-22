
## Meta ------------------------------------------------------------------------

# Description: Sets up a new gbd mortality data processing run

# Steps:
#   1. Read in and check config files
#   2. Create a version number for new run
#   3. Create folder structure for new run
#   4. Format and save input files to be used across scripts
#        - Location and age inputs
#        - Completeness estimates
#        - Population
#        - IND SRS population
#        - Early SRS correction
#        - Empirical pop
#        - Shocks
#        - Reference 5q0
#        - HDSS
#        - Google sheets
#        - WPP mx
#   5. Send initial slack launch message
#   6. Output detailed config file
#   7. Qsub the submission script `00b_submit_run.R`

# Inputs:
#   * "FILEPATH": configuration file for all demographic processes
#   * "FILEPATH": configuration file for gbd mortality
#     data processing

# Outputs:
#   * "FILEPATH": detailed configuration file
#     that is run-specific
#     - Newly created variables like {run_id_processing_template},
#       {main_dir}, etc. are included

repo_name <- "processing_gbd_mortality"
process_config_file <- paste0(repo_name, ".yml")
process_detailed_config_file <- paste0(repo_name, "_detailed.yml")

user <- Sys.getenv("USER")

source("FILEPATH")
source("FILEPATH")

# Load libraries ---------------------------------------------------------------

library(argparse)
library(assertable)
library(data.table)
library(demInternal)
library(forcats)
library(tidyverse)

# Command line arguments --------------------------------------------------

# Define the repo directory here
if (interactive()) {
  repo_dir_default <- fs::path("FILEPATH")
}

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--code_dir",
  type = "character",
  required = !interactive(),
  default = fs::path("FILEPATH"),
  help = "path to directory containing code files"
)
parser$add_argument(
  "--config_dir",
  type = "character",
  required = !interactive(),
  default = fs::path("FILEPATH"),
  help = paste0("path to directory with `all.yml` and `", process_config_file, "` config files")
)
parser$add_argument(
  "--configuration_name_all",
  type = "character",
  required = !interactive(),
  default = "test_production",
  help = "configuration from `all.yml` to use for this process run"
)
parser$add_argument(
  "--configuration_name_process",
  type = "character",
  required = !interactive(),
  default = "test_production",
  help = paste0("configuration from `", process_config_file, "` to use for this process run")
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# assertions for command line arguments
assertthat::assert_that(fs::dir_exists(code_dir))
assertthat::assert_that(fs::dir_exists(config_dir))

# Load configuration files -----------------------------------------------------

# load all demographics config file
config_all <- config::get(
  file = fs::path("FILEPATH"),
  config = configuration_name_all,
  use_parent = FALSE
)
list2env(config_all, .GlobalEnv)

# load process specific config file
config_process <- config::get(
  file = fs::path("FILEPATH"),
  config = configuration_name_process,
  use_parent = FALSE
)
list2env(config_process, .GlobalEnv)


# Create version name ----------------------------------------------------------

if (test) {
  # get current time in "Year-Month-Day-Hour-Minute" format
  time_now <- format(Sys.time(), "%Y-%m-%d-%H-%M")

  test_name <- paste0("test_", time_now, "_", Sys.getenv("USER"))
  run_id_process <- test_name
} else {
  # make a new process version which we will base the folder structure off of
  run_id_process <- demInternal::gen_new_version(
    process_name = process_name,
    comment = run_comment,
    gbd_year = gbd_year,
    odbc_dir = odbc_dir,
    odbc_section = odbc_section
  )
}

# Create folder structure ------------------------------------------------------

main_dir <- fs::path(
  "FILEPATH"
)
previous_main_dir <- fs::path(
 "FILEPATH"
)

fs::dir_create("FILEPATH", mode = "u=rwx,go=rwx")
fs::dir_create(fs::path("FILEPATH"), mode = "u=rwx,go=rwx")
fs::dir_create(fs::path("FILEPATH"), mode = "u=rwx,go=rwx")
fs::dir_create(fs::path("FILEPATH"), mode = "u=rwx,go=rwx")
fs::dir_create(fs::path("FILEPATH"), mode = "u=rwx,go=rwx")
fs::dir_create(fs::path("FILEPATH"), mode = "u=rwx,go=rwx")
fs::dir_create(fs::path("FILEPATH"), mode = "u=rwx,go=rwx")
fs::dir_create(fs::path("FILEPATH"), mode = "u=rwx,go=rwx")

# save original config files
fs::file_copy(
  path = fs::path("FILEPATH"),
  new_path = fs::path("FILEPATH")
)
fs::file_copy(
  path = fs::path("FILEPATH"),
  new_path = fs::path("FILEPATH")
)

# merge together config objects
config_detailed <- data.table::copy(config_all)
config_detailed <- config::merge(config_detailed, config_process)

# save detailed config file
config_detailed <- list(default = config_detailed)

yaml::write_yaml(
  config_detailed,
  fs::path("FILEPATH")
)

# Format and save input files to be used across scripts ------------------------

## Location and age inputs -----------------------------------------------------

# source map
source_map <- demInternal::get_dem_ids("source_type")

# location hierarchies
loc_hierarchy <- demInternal::get_locations(
  gbd_year = gbd_year,
  location_set_name = "mortality computation"
)
loc_hierarchy_countryplus <- demInternal::get_locations(
  gbd_year = gbd_year,
  level = "countryplus",
  location_set_name = "mortality computation"
)

readr::write_csv(
  loc_hierarchy,
  fs::path("FILEPATH")
)
readr::write_csv(
  loc_hierarchy_countryplus,
  fs::path("FILEPATH")
)

# age maps
age_map <- demInternal::get_age_map(
  gbd_year = gbd_year,
  type = "mort"
)
age_map_sy <- demInternal::get_age_map(
  gbd_year = gbd_year,
  type = "single_year"
)
age_map <- rbind(age_map, age_map_sy)

age_map_extended <- mortdb::get_age_map(
  gbd_year = gbd_year,
  type = "all"
)[, c("age_group_id", "age_group_years_start", "age_group_years_end")]
setnames(
  age_map_extended,
  c("age_group_years_start", "age_group_years_end"),
  c("age_start", "age_end")
)
age_map_extended[, age_start := round(age_start, 7)][, age_end := round(age_end, 7)]
age_map_extended <- age_map_extended[!(age_group_id %in% c(161, 308, 49, 27, 36, 38, 294, 371))] # drop duplicates

age_map_gbd <- mortdb::get_age_map(type = "gbd")[, c("age_group_id", "age_group_years_start", "age_group_years_end")]
setnames(
  age_map_gbd,
  c("age_group_years_start", "age_group_years_end"),
  c("age_start", "age_end")
)
age_map_gbd[, age_start := round(age_start, 7)][, age_end := round(age_end, 7)]

readr::write_csv(age_map, fs::path("FILEPATH"))
readr::write_csv(age_map_extended, fs::path("FILEPATH"))
readr::write_csv(age_map_gbd, fs::path("FILEPATH"))

## Completeness estimates ------------------------------------------------------

# read in output of DDM
ddm_final_comp_wshock <- setDT(
  haven::read_dta(
    fs::path(
      "FILEPATH"
    )
  )
)

# delta transform from log base 10 to normal space for variance
ddm_final_comp_wshock[, var_comp := (final_comp * log(10))^2 * (sd^2)]

# standardize source type
ddm_final_comp_wshock[loc_hierarchy, location_id := i.location_id, on = "ihme_loc_id"]
ddm_final_comp_wshock[ihme_loc_id == "IND_44849", location_id := 44849]
ddm_final_comp_wshock[source %in% c("CENSUS", "HOUSEHOLD", "SURVEY"), source := str_to_title(source)]
ddm_final_comp_wshock[source %in% c("HOUSEHOLD_DLHS", "HOUSEHOLD_HHC"), source := gsub("HOUSEHOLD_", "", source)]
ddm_final_comp_wshock[source == "CR", source := "Civil Registration"]
ddm_final_comp_wshock[source == "MOH survey", source := "MOH Survey"]
ddm_final_comp_wshock[, source_type_name := source]
ddm_final_comp_wshock[source_map, source_type_id := i.source_type_id, on = "source_type_name"]
ddm_final_comp_wshock <- source_type_id_correction(ddm_final_comp_wshock)

# standardize id columns
setnames(ddm_final_comp_wshock, "year", "year_id")
ddm_final_comp_wshock[, sex_id := dplyr::case_when(
  sex == "male" ~ 1,
  sex == "female" ~ 2,
  sex == "both" ~ 3
)]
ddm_final_comp_wshock <- ddm_final_comp_wshock[,
  c("location_id", "year_id", "sex_id", "source_type_id", "source_type", "var_comp")
]

readr::write_csv(
  ddm_final_comp_wshock,
  fs::path("FILEPATH")
)

# format completeness estimates for data adjustment
dt_ddm_est <- mortdb::get_mort_outputs(
  model_name = "ddm",
  model_type = "estimate",
  run_id = ddm_version,
  estimate_stage_ids = c(11, 14),
  demographic_metadata = TRUE
)

dt_comp_est <-
  dt_ddm_est[,
    .(location_id,
      ihme_loc_id,
      year_id,
      sex_id,
      source_type_id,
      variable = fifelse(age_group_id == 1, "comp_u5", "comp"),
      value = pmin(mean, 1)
    )
  ] |>
  dcast(... ~ variable + sex_id, value.var = "value", sep = ".")

# add sex id 9 (old unknown) for raw vr
dt_comp_est[, ':=' (comp.9 = comp.3, comp_u5.9 = comp_u5.3)]
dt_comp_est[, comp.3 := NULL]

# replace NA m/f with value for both sexes
dt_comp_est[is.na(comp_u5.1), comp_u5.1 := comp_u5.3]
dt_comp_est[is.na(comp_u5.2), comp_u5.2 := comp_u5.3]
dt_comp_est[, comp_u5.3 := NULL]

dt_comp_est <- melt(
  dt_comp_est,
  measure.vars = patterns("^comp_u5\\.", "^comp\\."),
  value.name = c("comp_u5", "comp_adult"),
  variable.name = "sex_id"
)
dt_comp_est[,
  sex_id := forcats::lvls_revalue(
    sex_id,
    c("1", "2", "9")
  )
]

dt_comp_est[, sex_id := as.integer(as.character(sex_id))]

# use SSPC-DC for SSPC/DC child estimates
dt_comp_est_sspcdc_child <- dt_comp_est[source_type_id == 50]
dt_comp_est_sspcdc_child[, ":=" (comp_u5_sspcdc = comp_u5, comp_adult = NULL, comp_u5 = NULL, source_type_id = NULL)]
dt_comp_est <- merge(
  dt_comp_est,
  dt_comp_est_sspcdc_child,
  by = setdiff(names(dt_comp_est_sspcdc_child), c("comp_u5_sspcdc")),
  all.x = TRUE
)
dt_comp_est[is.na(comp_u5) & source_type_id %in% 36:37, comp_u5 := comp_u5_sspcdc]
dt_comp_est[, comp_u5_sspcdc := NULL]

# create estimate for older child and teen age groups
dt_comp_est[, `:=`(
  comp_5_9 = (comp_u5 * 2/3) + (comp_adult * 1/3),
  comp_10_14 = (comp_u5 * 1/3) + (comp_adult * 2/3)
)]

# manual updates
dt_comp_est <- source_type_id_correction(dt_comp_est)

# save
readr::write_csv(
  dt_comp_est,
  fs::path("FILEPATH")
)

## Population ------------------------------------------------------------------

# population estimates
pop <- mortdb::get_mort_outputs(
  model_name = "population",
  model_type = "estimate",
  run_id = pop_version
)[!is.na(ihme_loc_id)]
setnames(pop, "mean", "population")

# use total population for unknown
pop_unknown_sex <- pop[sex_id == 3]
pop_unknown_sex[, sex_id := 4]
pop <- rbind(pop, pop_unknown_sex)
pop_unknown_age <- pop[age_group_id == 22]
pop_unknown_age[, age_group_id := 283]
pop <- rbind(pop, pop_unknown_age)

readr::write_csv(pop, fs::path("FILEPATH"))

# population single year estimates
pop_sy <- mortdb::get_mort_outputs(
  model_name = "population single_year",
  model_type = "estimate",
  run_id = pop_sy_version,
  demographic_metadata = TRUE
)[!is.na(ihme_loc_id)]

# use total population for unknown
pop_unknown_sex <- pop_sy[sex_id == 3]
pop_unknown_sex[, sex_id := 4]
pop_sy <- rbind(pop_sy, pop_unknown_sex)

readr::write_csv(pop_sy, fs::path("FILEPATH"))

# create pop for pydisagg merging with age_start and age_end
pop_merge_pydisagg <- merge(pop, age_map_extended, by = "age_group_id")

readr::write_csv(pop_merge_pydisagg, fs::path("FILEPATH"))

## IND SRS population ----------------------------------------------------------

srs_pop_files <- c(
  fs::path("FILEPATH"),
  fs::path("FILEPATH"),
  fs::path("FILEPATH"),
  paste0("FILEPATH")
)

srs_pop <- import_files(
  srs_pop_files,
  FUN = haven::read_dta
)
srs_pop[is.na(YEAR), YEAR := as.numeric(stringr::str_sub(CENSUS_SOURCE, -4))]
srs_pop <- srs_pop[, -c("CENSUS_SOURCE", "AREA", "VR_SOURCE", "SUBDIV")]
setnames(srs_pop, c("YEAR", "NID", "COUNTRY", "SEX"), c("year_id", "nid", "ihme_loc_id", "sex_id"))
srs_pop <- melt(
  srs_pop,
  id.vars = c("year_id", "nid", "ihme_loc_id", "sex_id"),
  variable.name = "age_group",
  value.name = "census_pop"
)
srs_pop[, age_group := gsub("DATUM", "", age_group)]
srs_pop <- srs_pop |>
  separate(age_group, into = c("age_start", "age_end"), sep = "to", remove = FALSE) |>
  mutate(age_end = as.numeric(age_end) + 1) |>
  mutate(age_start = as.numeric(age_start))
srs_pop <- srs_pop |>
  mutate(age_start = case_when(
    age_group == "TOT" ~ 0,
    age_group == "85plus" ~ 85,
    TRUE ~ age_start)) |>
  mutate(age_end = case_when(
    age_group == "TOT" ~ 125,
    age_group == "85plus" ~ 125,
    TRUE ~ age_end)) |>
  setDT()
srs_pop[, age_group := NULL]

pop_emp_srs <- mortdb::get_mort_outputs(
  model_name = "population empirical",
  model_type = "data",
  run_id = data_emp_pop_version,
  year_ids = 1993:1994,
  location_ids = loc_hierarchy[ihme_loc_id %like% "IND", location_id]
)
pop_emp_srs <- merge(pop_emp_srs, age_map_extended, by = "age_group_id", all.x = TRUE)
pop_emp_srs <- pop_emp_srs[, c("ihme_loc_id", "year_id", "nid", "sex_id", "age_start", "age_end", "mean")]
setnames(pop_emp_srs, "mean", "census_pop")
pop_emp_srs[age_start < 5 & age_end != 125, ':=' (age_start = 0, age_end = 5)]
pop_emp_srs <- pop_emp_srs[, .(census_pop = sum(census_pop)), by = setdiff(names(pop_emp_srs), "census_pop")]

srs_pop <- rbind(pop_emp_srs, srs_pop)

# save
readr::write_csv("FILEPATH")

## Early SRS correction --------------------------------------------------------

# Read in the needed ddm_correction_factors
ddm_correction_factors <- fread(
  fs::path(
    "FILEPATH"
  )
)

srs_pop_scalars <- ddm_correction_factors[source_type %in% c("SRS", "SRS_LIBRARY") & ihme_loc_id %like% "IND"]
srs_pop_scalars <- srs_pop_scalars[year >= 1993, .(srs_scale = mean(correction_factor)), by = c("ihme_loc_id", "sex")]
setnames(srs_pop_scalars, "sex", "sex_id")
srs_pop_scalars <- srs_pop_scalars %>%
  mutate(sex_id = case_when(
    sex_id == "male" ~ 1,
    sex_id == "female" ~ 2,
    sex_id == "both" ~ 3,
    TRUE ~ as.numeric(sex_id)
  )
  )

# save
readr::write_csv("FILEPATH")

## Shocks ----------------------------------------------------------------------

# aggregated shocks from VRP
shocks <- setDT(
  haven::read_dta(
    fs::path("FILEPATH")
  )
)
setnames(shocks, c("year", "sex"), c("year_id", "sex_id"))

readr::write_csv(
  shocks,
  fs::path("FILEPATH")
)

shocks_summary <- shocks[, .(shocks_deaths = sum(numkilled)), by = c("location_id", "year_id")]
shocks_summary <- merge(
  loc_hierarchy[, c("location_id", "ihme_loc_id", "location_name")],
  shocks_summary,
  by = "location_id",
  all.y = TRUE
)

readr::write_csv(
  shocks_summary,
  fs::path("FILEPATH")
)

# shocks aggregator
if (!file.exists(fs::path("FILEPATH"))) {
  aggregate_shocks(
    base_dir = base_dir,
    process_name = process_name,
    shocks_version = shocks_version,
    shock_years = estimation_year_start:estimation_year_end,
    locs = loc_hierarchy
  )
}

file.copy(
  fs::path("FILEPATH"),
  fs::path("FILEPATH")
)

## Reference 5q0 ---------------------------------------------------------------

data_5q0 <- mortdb::get_mort_outputs(
  model_name = "5q0",
  model_type = "data",
  run_id = data_5q0_version,
  gbd_year = gbd_year,
  outlier_run_id = "active",
  bias_adj_run_id = data_5q0_bias_adj_version,
  demographic_metadata = TRUE
)

reference_5q0 <- data_5q0[reference == 1]

readr::write_csv(
  reference_5q0,
  fs::path("FILEPATH")
)

## HDSS data -------------------------------------------------------------------

hdss_data_1 <- fread(fs::path("FILEPATH"))
hdss_data_2 <- fread(fs::path("FILEPATH"))
hdss_data <- rbind(hdss_data_1, hdss_data_2, fill = TRUE)

hdss_data[!is.na(location_id_sub), location_id := location_id_sub]
hdss_data[, location_id_sub := NULL]

map_loc_center <- unique(hdss_data[, .(location_id, CentreId)])
map_loc_center[, id_new := seq_len(.N), by = .(location_id)]
hdss_data[map_loc_center, location_id_new := paste0(location_id, "000", i.id_new), on = .(location_id, CentreId)]

hdss_data[, year_id := year_start]
hdss_data[, `:=` (population = pyears, sample_size = pyears, sample_size_orig = pyears)]
hdss_data[, mx_se := sqrt((ifelse(mx < 1, mx, 0.99) * (1 - ifelse(mx < 1, mx, 0.99))) / sample_size)]
hdss_data[, source_type_name := "HDSS"]
hdss_data[, onemod_process_type := "standard gbd age group data"]

hdss_data[, c("CentreId", "pyears", "year_start", "year_end") := NULL]

hdss_data <- hdss_data[sex_id %in% 1:2]

hdss_outliers <- fread(fs::path("FILEPATH"))
hdss_outliers[, outlier := 1]

hdss_data <- merge(
  hdss_data,
  hdss_outliers,
  by = c("nid", "location_id", "year_id", "sex_id"),
  all.x = TRUE
)
hdss_data[is.na(outlier), outlier := 0]

readr::write_csv(
  hdss_data,
  fs::path("FILEPATH")
)

## Google sheets ---------------------------------------------------------------

# enn/lnn prep tracker
enn_lnn_prep_tracker <- setDT(googlesheets4::read_sheet(enn_lnn_review_url))

readr::write_csv(
  enn_lnn_prep_tracker,
  fs::path("FILEPATH")
)

# vr reliability indicator
vr_reliability_adj_sheet <- setDT(googlesheets4::read_sheet(vr_reliability_url))

vr_reliability_adj_sheet <- merge(
  vr_reliability_adj_sheet,
  loc_hierarchy[, c("ihme_loc_id", "location_id")],
  by = "ihme_loc_id",
  all.x = TRUE
)

vr_reliability_adj_sheet <- rbind(
  vr_reliability_adj_sheet,
  data.table(
    location_id = loc_hierarchy[parent_id %in% vr_reliability_adj_sheet$location_id]$location_id,
    ihme_loc_id = loc_hierarchy[parent_id %in% vr_reliability_adj_sheet$location_id]$ihme_loc_id,
    vr_reliability_adj = 1
  )
)

readr::write_csv(
  vr_reliability_adj_sheet,
  fs::path("FILEPATH")
)

# ssa data inclusion
ssa_data_inclusion_sheet <- setDT(googlesheets4::read_sheet(ssa_data_inclusion_url))
ssa_data_inclusion_sheet[, underlying_nid := as.character(underlying_nid)]
ssa_data_inclusion_sheet[underlying_nid == "NA", underlying_nid := NA]
ssa_data_inclusion_sheet[, underlying_nid := as.numeric(underlying_nid)]

readr::write_csv(
  ssa_data_inclusion_sheet,
  fs::path("FILEPATH")
)

## WPP mx ----------------------------------------------------------------------

wpp_mx <- fread(fs::path("FILEPATH"))

wpp_mx <- wpp_mx[, c("location", "date_start", "sex_id", "age_start", "age_end", "value")]
setnames(
  wpp_mx,
  c("location", "date_start", "value"),
  c("ihme_loc_id", "year_id", "value_wpp")
)

wpp_mx[age_end == Inf, age_end := 125]

wpp_mx <- merge(
  mortdb::get_age_map(type = "gbd")[, c("age_group_years_start", "age_group_years_end", "age_group_name", "age_group_id")],
  wpp_mx,
  by.x = c("age_group_years_start", "age_group_years_end"),
  by.y = c("age_start", "age_end"),
  all.y = TRUE
)

wpp_mx[age_group_years_start == 0 & age_group_years_end == 1, ':=' (age_group_name = "<1 year", age_group_id = 28)]

wpp_mx[loc_hierarchy, ':=' (location_id = i.location_id), on = "ihme_loc_id"]
wpp_mx[, measure := "mx"]
wpp_mx[, series_name := "wpp"]

readr::write_csv(
  wpp_mx,
  fs::path("FILEPATH")
)

# Send initial slack launch message --------------------------------------------

slackr::slackr_setup(config_file = slack_config_file)

launch_message <- glue::glue(
  ""
)

slack_thread_ts <- slackr::slackr_msg(
  txt = launch_message,
  channel = paste0(""),
)
slack_thread_ts <- slack_thread_ts$ts

# Output detailed config file --------------------------------------------------

# add on other important created variables
add_config_vars <- c(
  "user",
  "run_id_process",
  "main_dir",
  "previous_main_dir",
  "code_dir",
  "config_dir",
  "configuration_name_all",
  "configuration_name_process",
  "slack_thread_ts"
)

for (var in add_config_vars) {
  config_detailed$default[[var]] <- get(var)
}

# save detailed config file
config_detailed <- list(default = config_detailed)

yaml::write_yaml(
  config_detailed,
  fs::path("FILEPATH")
)

# Qsub the submission script ---------------------------------------------------

mortcore::qsub(
  jobname = paste0("submit_processing_gbd_mortality_data_", run_id_process),
  code = fs::path("FILEPATH"),
  shell = image_shell_path_r,
  cores = 1,
  mem = 10,
  wallclock = "04:00:00",
  archive = FALSE,
  pass_shell = list(i = image_path),
  pass_argparse = list(main_dir = main_dir),
  queue = gsub(".q", "", queue),
  proj = submission_project_name,
  submit = TRUE
)
