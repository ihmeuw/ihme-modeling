# Meta -------------------------------------------------------------------------

# Description: Creates covariate dataset for testing regressions

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)
library(demInternal)
library(dplyr)
library(tidyr)
library(zoo)

# Helper -----------------------------------------------------------------------

validate_data <- function(dt,
                          process_locations,
                          not_na) {

  dt <- copy(dt)

  # check missing locations
  missing_locations <- setdiff(
    process_locations[, location_id],
    unique(dt$location_id)
  )
  missing_locations <- merge(
    data.table(location_id = missing_locations),
    process_locations[, c("location_id","ihme_loc_id")],
    by = "location_id",
    all.x = TRUE
  )
  if (length(missing_locations) > 0) {
    warning(paste0("Missing data or import error for these locations (", length(missing_locations), "): ",
                   paste(unique(missing_locations[,ihme_loc_id]), collapse = ", ")))
  }

  # check not_na columns do not have na values
  assertable::assert_values(
    dt, not_na, test = "not_na", quiet = TRUE
  )

  return(invisible(dt))

}

# change ihme_loc_id to national so national sum generated when aggregating over ihme_loc_id
change_subnat_to_nat <- function(national_ihme_loc, input_file) {

  national <- copy(input_file)

  if (national_ihme_loc == "CHN") {

    national_loc_id <- 6

  } else {

    national_loc_id <- locs[ihme_loc_id == national_ihme_loc, location_id]
    national_loc_id <- as.numeric(national_loc_id)

  }

  assertthat::assert_that(!national_loc_id %in% unique(national$location_id))

  national <- national[location_id %in% get(paste0(tolower(national_ihme_loc), "_loc_ids"))]
  national[, location_id := national_loc_id]

  return(national)
}

# Fill missing subnational locations with national values
fill_loc_w_parent <- function(missing_ihme_loc, input_file) {

  national <- copy(input_file)
  assertthat::assert_that(!missing_ihme_loc %in% unique(national$ihme_loc_id))

  if(missing_ihme_loc %in% gbr_utla_locs){
    parent_id = 4749
  } else {
    parent_id <- locs[ihme_loc_id == missing_ihme_loc, parent_id]
  }

  # try to use national value if subnational parent missing
  if(!parent_id %in% unique(national$location_id)){
    parent_id <- locs[ihme_loc_id == gsub("_.*", "", missing_ihme_loc), location_id]
  }

  new_subnat <- national[location_id == parent_id]
  new_subnat[, location_id := locs[ihme_loc_id == missing_ihme_loc, location_id]]
  new_subnat[, ihme_loc_id := missing_ihme_loc]

  return(new_subnat)
}


# Setup ------------------------------------------------------------------------

data_run_id <- ""

date <- format(Sys.time(), "%Y-%m-%d-%H-%M")

GBD <- T

if (GBD) {
  dataset_types <- c("fit","all", "only2020", "only2021")
} else {
  dataset_types <- c("all", "custom") 
}

gbd_year = 2020

main_dir <- paste0()

## get config ##
config <- config::get(
  file = paste0(),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

covid_years <- c(2020, 2021)

code_dir <- ""
data_code_dir <- ""
base_dir <- ""

out_dir <- paste0()
dir.create(out_dir)

source(paste0("<FILEPATH>/load_summaries_and_data.R"))
source(paste0("<FILEPATH>/get_covariate_estimates.R"))
source(paste0("<FILEPATH>/em_data_validation.R"))
source(paste0("<FILEPATH>/pop_to_person_years.R"))
source(paste0("<FILEPATH>/interpolate_pop.R"))
source(paste0("<FILEPATH>/add_weeks_months.R"))
source(paste0("<FILEPATH>/adj_weeks_for_agg.R"))

## location mapping ##
gbd_loc_map <- demInternal::get_locations(gbd_year = 2020)

covid_loc_map <- demInternal::get_locations(gbd_year = gbd_year, location_set_name = "COVID-19 modeling")
covid_only_locs <- covid_loc_map[!location_id %in% unique(gbd_loc_map$location_id)]

gbd_loc_map_old <- demInternal::get_locations(gbd_year = 2019)
nor_locs_old <- gbd_loc_map_old[!(location_id %in% gbd_loc_map$location_id) & ihme_loc_id %like% "NOR_"]

locs <- rbind(gbd_loc_map, covid_only_locs, nor_locs_old)

loc_week_types <- fread(fs::path())

# get subnats
locs_w_subs <- sort(unique(substr(locs[ihme_loc_id %like% "_"]$ihme_loc_id, 1, 3)))

gbr_locs <- gbd_loc_map[ihme_loc_id %like% "GBR_" & level == 5]$ihme_loc_id
gbr_utla_locs <- gbd_loc_map[ihme_loc_id %like% "GBR_" & level == 6]$ihme_loc_id
gbr_utla_loc_ids <- gbd_loc_map[ihme_loc_id %like% "GBR_" & level == 6]$location_id
ind_locs <- gbd_loc_map[ihme_loc_id %like% "IND_" & level == 4]$ihme_loc_id
ken_locs <- gbd_loc_map[ihme_loc_id %like% "KEN_" & level == 4]$ihme_loc_id
reg_locs <- gbd_loc_map[level == 2]$ihme_loc_id

locs_w_subs <- sort(c(locs_w_subs, gbr_locs, "GBR_4749", "CHN_44533", ind_locs, ken_locs, reg_locs))

for (loc in locs_w_subs) {
  loc_name <- paste(loc, "_loc_ids", sep = "")
  loc_id <- locs[ihme_loc_id == loc, location_id]
  assign(tolower(loc_name), unique(locs[parent_id == loc_id, location_id]))

  if (loc == "CHN") assign(tolower(loc_name), unique(c(locs[parent_id == 44533, location_id], 354, 361)))
  if (loc == "IND") assign(tolower(loc_name), unique(covid_loc_map[parent_id == loc_id, location_id]))

  if (loc == "NOR") assign(tolower(loc_name), unique(gbd_loc_map[parent_id == loc_id, location_id]))

  if (loc == "USA") {
    wa_state_loc_ids <- c(3539, 60886, 60887)
    usa_loc_ids <- c(usa_loc_ids, wa_state_loc_ids)
  }

}

# norway remapping
nor_map <- fread("")

# location week types
location_week_types <- fread("")
locs_with_isoweek <- location_week_types[week_type == "isoweek", ihme_loc_id]
locs_with_epiweek <- location_week_types[week_type == "epiweek", ihme_loc_id]
locs_with_ukweek <- location_week_types[week_type == "ukweek", ihme_loc_id]


# Read in input files ----------------------------------------------------------

dt <- setDT(readRDS(""))
setnames(dt, "year_id", "year_start")
dt[, ':='(time_unit = "year", time_start = year_start,
          deaths_observed = NULL, deaths_expected = NULL)]

if(GBD){
  ind_custom <- fread(paste0())
} else {
  ind_custom <- fread(paste0())
}

pop <- fread("")
pop[time_unit == "day", origin := as.Date(paste0(year_start, "-01-01"),tz = "UTC") - lubridate::days(1)]
pop[time_unit == "day", date := as.Date(time_start, origin = origin, tz = "UTC")]
pop[, origin := NULL]
  
stars <- fread("")

if(GBD){

  covid_raw_file <- ""
  covid_raw <- fread(covid_raw_file)
  idr <- fread("")
  mobility <- fread("")
  sero <- fread("")
  sero_run_id <- ""

  # Read in russian covid deaths separately
  rus_covid <- fread("")

} else {

  covid_raw_file <- ""
  covid_raw <- fread(covid_raw_file)
  idr <- setDT(arrow::read_parquet(""))
  mobility <- fread("")
  sero_dir <- ""
  sero <- fread(fs::path())
  sero_run_id <- sub("","\\1", sero_dir)
}

## set all time cutoff ##
all_time_cutoff <- as.IDate("2021-12-31")

crude_death_rate <- fread("")
crude_death_rate_sd_1990 <- fread("")
crude_death_rate_sd_2000 <- fread("")
mean_pop_age <- fread("")
prop_pop_60plus <- fread("")
prop_pop_70plus <- fread("")
prop_pop_75plus <- fread("")
prop_pop_80plus <- fread("")
prop_pop_85plus <- fread("")

setnames(crude_death_rate_sd_1990, "crude_death_rate_sd", "crude_death_rate_sd_1990")
setnames(crude_death_rate_sd_2000, "crude_death_rate_sd", "crude_death_rate_sd_2000")
setnames(prop_pop_60plus, "prop_pop_above", "prop_pop_60plus")
setnames(prop_pop_70plus, "prop_pop_above", "prop_pop_70plus")
setnames(prop_pop_75plus, "prop_pop_above", "prop_pop_75plus")
setnames(prop_pop_80plus, "prop_pop_above", "prop_pop_80plus")
setnames(prop_pop_85plus, "prop_pop_above", "prop_pop_85plus")

# location ids for small islands with pandemic start dates in 2021
late_pan_islands <- c(23,25,29,380)

# Use new file for UTLAs
covid_raw_UTLA <- fread("")
covid_raw_UTLA <- covid_raw_UTLA[location_id %in% c(gbr_utla_loc_ids, 4749)]
covid_raw <- rbind(covid_raw[!location_id %in% gbr_utla_loc_ids], covid_raw_UTLA)


# Get external em data ---------------------------------------------------------

ext_dir <- fs::path()
ext <- assertable::import_files(
  filenames = list.files(ext_dir, pattern = ".csv"),
  folder = ext_dir
)

ext <- demInternal::ids_names(
  ext,
  id_cols = "location_id",
  extra_output_cols = "ihme_loc_id",
  gbd_year = 2020,
  warn_only = TRUE
)

ext <- ext[!is.na(deaths_excess) & !is.na(deaths_covid)]
ext <- ext[ihme_loc_id == "IRN" | ihme_loc_id %like% "ZAF"]

# drop last weeks for ZAF
zaf <- ext[ihme_loc_id == "ZAF"]
zaf_last_yr <- max(zaf$year_start)
zaf_last_wk <- max(zaf$time_start)
ext <- ext[
  !(ihme_loc_id %like% "ZAF") |
    (time_unit == "week" & (year_start == zaf_last_yr & time_start < zaf_last_wk)) |
    (time_unit == "week" & year_start < zaf_last_yr)
  ]

ext_prepped <- ext[, .(location_id, year_start, time_unit, time_start, person_years, deaths_excess)]
ext_prepped[, death_rate_excess := deaths_excess / person_years]
ext_prepped <- ext_prepped[, -c("deaths_excess", "person_years")]
ext_prepped <- ext_prepped[rep(seq_len(.N), 100)]
ext_prepped <- ext_prepped[order(location_id, year_start, time_start)]
replicates <- nrow(unique(ext_prepped[, c("location_id", "year_start", "time_start")]))
ext_prepped[, draw := rep(1:100, replicates)]

# fills dt death rate excess
dt <- rbind(dt, ext_prepped, fill = TRUE)

dt <- merge(
  locs[,c("location_id","ihme_loc_id","location_name")], 
  dt, 
  by = "location_id", 
  all.y = T
)
dt[location_id == 99999, ':='(ihme_loc_id = "ITA_99999",
                              location_name = "Provincia autonoma di Bolzano + Provincia autonoma di Trento")]


# Specify times to keep --------------------------------------------------------

locs_w_2020 <- dt[year_start == 2020, unique(location_id)]
locs_w_2021 <- dt[year_start == 2021, unique(location_id)]
locs_w_all <- locs_w_2020[locs_w_2020 %in% locs_w_2021]
locs_w_only2020 <- locs_w_2020[!locs_w_2020 %in% locs_w_2021]
locs_w_only2021 <- locs_w_2021[!locs_w_2021 %in% locs_w_2020]

keep_years <- data.table(location_id = rep(c(locs[, location_id], 99999), 2), 
                         year_start = c(rep(2020,1154), rep(2021,1154)),
                         keep = 0
                         )
keep_years[location_id %in% locs_w_all, keep := 1]
keep_years[location_id %in% locs_w_2020 & year_start == 2020, keep := 1]
keep_years[location_id %in% locs_w_2021 & year_start == 2021, keep := 1]


# Prep covid deaths ------------------------------------------------------------

covid <- copy(covid_raw)

covid[location_id == 53577 & Date == "2020-01-30", Deaths := 0]
covid[location_id == 53579 & Date == "2020-02-05", Deaths := 0]

# Format russia covid deaths to replace values in full_data file
setnames(rus_covid, c("date", "imputed_deaths"), c("Date", "Deaths"))
rus_covid <- rus_covid[, c("location_id", "location_name", "Date", "Deaths")]
rus_covid[, Date := as.IDate(Date)]
covid <- rbind(covid[!location_id %in% unique(rus_covid$location_id)], rus_covid, fill = TRUE)

# Make dataset square
covid <- covid[!is.na(Deaths)]
setnames(covid, "Date", "date")
covid <- fill_missing_dates(covid)

covid <- check_square(covid) 

covid <- merge(
  covid,
  locs[, c("location_id","ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)
unique(covid[is.na(ihme_loc_id), "location_name"])

covid <- covid[!location_id %in% c(81, 130, 95)]

# deaths are cumulative -- find weekly
covid <- covid[order(date)]
covid <- covid %>%
  dplyr::group_by(location_id) %>%
  tidyr::fill(Deaths, .direction = "down") %>%
  dplyr::ungroup() %>% setDT

# get daily deaths then sum to weekly
covid[, daily_deaths := Deaths - shift(Deaths), by = "location_id"]

assertthat::assert_that(!102 %in% unique(covid$location_id))
assertthat::assert_that(!570 %in% unique(covid$location_id))
covid_usa_agg <- covid[location_id %in% usa_loc_ids]
covid_usa_agg[, ":="( location_id = 102, ihme_loc_id = "USA")]
covid_wa_agg <- covid[location_id %in% wa_state_loc_ids]
covid_wa_agg[, ":="( location_id = 570, ihme_loc_id = "USA_570")]
covid <- rbindlist(list(covid, covid_usa_agg, covid_wa_agg), use.names = TRUE)

# aggregate admin 2 to admin 1
covid_locs_1 <- sort(locs[(ihme_loc_id %like% "KEN" & level == 4) | (ihme_loc_id %like% "GBR" & level == 5)]$ihme_loc_id)
covid_locs_1 <- covid_locs_1[covid_locs_1 != "IND_60896"]
covid_subs <- rbindlist(lapply(
  covid_locs_1,
  change_subnat_to_nat,
  input_file = covid
))
covid <- rbind(covid, covid_subs)

# aggregate to national
covid_nats_list <- sort(unique(substr(covid_loc_map[ihme_loc_id %like% "_"]$ihme_loc_id, 1, 3)))
covid_nats_list <- covid_nats_list[!(covid_nats_list %in% c("GBR", "USA", "CHN"))]

covid_nats <- rbindlist(lapply(
  covid_nats_list,
  change_subnat_to_nat,
  input_file = covid
))

covid <- rbind(covid, covid_nats)

# special process for GBR
gbr_national <- copy(covid)
gbr_national <- gbr_national[location_id %in% c(4749, 4636)]
gbr_national[location_id %in% c(4749, 4636), location_id := 432]
covid <- rbind(covid, gbr_national)

uk_national <- copy(covid)
uk_national <- uk_national[location_id %in% c(4749, 4636, 433, 434)]
uk_national[location_id %in% c(4749, 4636, 433, 434), location_id := 95]
covid <- rbind(covid, uk_national)

# ITA_99999 is made from ITA_35498 and ITA_35499 in allcause data
ita_combine <- copy(covid)
ita_combine <- ita_combine[location_id %in% c(35498, 35499)]
ita_combine[, location_id := 99999]
covid <- rbind(covid, ita_combine)

# aggregate to missing nationals
covid <- covid[,
               list(deaths_covid = sum(daily_deaths, na.rm = TRUE)),
               by = c("location_id","date")
               ]
covid <- covid[order(location_id, date)]

# create version of covid before loop
covid_initial <- copy(covid)


# Prep Covid cases -------------------------------------------------------------

covid_cases <- copy(covid_raw)

# Add beginning of time period for GBR UTLAs and PHL
utla_adds <- data.table(location_id = gbr_utla_loc_ids,
                        Date = as.IDate("2020-01-31"),
                        Confirmed = 0)
phl_adds <- data.table(location_id = phl_loc_ids,
                        Date = as.IDate("2020-01-30"),
                        Confirmed = 0)
covid_cases <- rbind(covid_cases, utla_adds, phl_adds, fill = T)

# Make dataset square
setnames(covid_cases, "Date", "date")
covid_cases <- fill_missing_dates(covid_cases)
covid_cases <- covid_cases[order(location_id, date)]

# check square before shift and aggregation
covid_cases <- check_square(covid_cases)

# linear interpolate missing data
covid_cases <- covid_cases[, c("location_id","date","Confirmed")]
covid_cases <- covid_cases[order(location_id, date)]
covid_cases <- do.call("rbind", by(covid_cases, covid_cases$location_id, na.trim, sides = "both"))
covid_cases <- covid_cases %>%
  group_by(location_id) %>%
  mutate(fill = na.approx(Confirmed)) %>%
  setDT()
covid_cases[is.na(Confirmed), Confirmed := fill]

# calculate daily cases
covid_cases[, daily_cases := Confirmed - shift(Confirmed, type = "lag"),
            by = "location_id"]
covid_cases[daily_cases < 0, daily_cases := 0]
covid_cases[is.na(daily_cases), daily_cases := 0]

covid_cases <- covid_cases[!location_id %in% c(81, 130, 95)]

assertthat::assert_that(!102 %in% unique(covid_cases$location_id))
covid_usa_agg <- covid_cases[location_id %in% usa_loc_ids]
covid_usa_agg[, location_id := 102]
covid_wa_agg <- covid_cases[location_id %in% wa_state_loc_ids]
covid_wa_agg[, location_id := 570]
covid_cases <- rbindlist(list(covid_cases, covid_usa_agg, covid_wa_agg), use.names = TRUE)

# aggregate admin 2 to admin 1
covid_subs <- rbindlist(lapply(
  covid_locs_1,
  change_subnat_to_nat,
  input_file = covid_cases
))
covid_cases <- rbind(covid_cases, covid_subs)

# aggregate to national
covid_nats <- rbindlist(lapply(
  covid_nats_list,
  change_subnat_to_nat,
  input_file = covid_cases
))

covid_cases <- rbind(covid_cases, covid_nats)

# special process for GBR:
gbr_national <- copy(covid_cases)
gbr_national <- gbr_national[location_id %in% c(4749, 4636)]
gbr_national[location_id %in% c(4749, 4636), location_id := 432]
covid_cases <- rbind(covid_cases, gbr_national)

uk_national <- copy(covid_cases)
uk_national <- uk_national[location_id %in% c(4749, 4636, 433, 434)]
uk_national[location_id %in% c(4749, 4636, 433, 434), location_id := 95]
covid_cases <- rbind(covid_cases, uk_national)

# ITA_99999 is made from ITA_35498 and ITA_35499 in allcause data
ita_combine <- copy(covid_cases)
ita_combine <- ita_combine[location_id %in% c(35498, 35499)]
ita_combine[, location_id := 99999]
covid_cases <- rbind(covid_cases, ita_combine)

# aggregate to missing nationals
covid_cases <- covid_cases[,
                           list(daily_cases = sum(daily_cases, na.rm = TRUE)),
                           by = c("location_id","date")
                           ]
covid_cases <- covid_cases[order(location_id, date)]

# Rake GBR UTLA daily cases to England
gbr_4749_cases <- covid_cases[location_id %in% c(4749, gbr_utla_loc_ids)]
eng_map <- locs[ihme_loc_id %in% gbr_utla_locs, c("location_id")]
eng_map[, parent := 4749]
setnames(eng_map, "location_id", "child")
scaled_eng <- hierarchyUtils::scale(dt = gbr_4749_cases,
                                    id_cols = c("location_id", "date"),
                                    value_cols = c("daily_cases"),
                                    col_stem = "location_id",
                                    col_type = "categorical",
                                    mapping = eng_map,
                                    missing_dt_severity = "warning",
                                    collapse_missing = TRUE)
scaled_eng[is.na(daily_cases), daily_cases := 0]
covid_cases <- rbind(covid_cases[!location_id %in% unique(scaled_eng$location_id)], scaled_eng)

# Rake PHL subnational daily cases to PHL
phl_cases <- covid_cases[location_id %in% c(16, phl_loc_ids)]
phl_map <- locs[location_id %in% phl_loc_ids, c("location_id", "parent_id")]
colnames(phl_map) <- c("child", "parent")
scaled_phl <- hierarchyUtils::scale(dt = phl_cases,
                                    id_cols = c("location_id", "date"),
                                    value_cols = c("daily_cases"),
                                    col_stem = "location_id",
                                    col_type = "categorical",
                                    mapping = phl_map,
                                    missing_dt_severity = "warning",
                                    collapse_missing = TRUE)
scaled_phl[is.na(daily_cases), daily_cases := 0]
covid_cases <- rbind(covid_cases[!location_id %in% unique(scaled_phl$location_id)], scaled_phl)

daily_covid <- copy(covid_cases)

covid_run_id <- sub("","\\1", covid_raw_file)
readr::write_csv(daily_covid, paste0( ))
if(GBD){
  readr::write_csv(daily_covid, paste0())
} else {
  readr::write_csv(daily_covid, paste0())
}


# Prep IDR ---------------------------------------------------------------------

# format wide draws
idr <- melt(idr, measure.vars = patterns("draw"), variable.name = "draw")
idr[, draw := gsub("draw_", "", draw)]
setnames(idr, "value", "idr_reference")

idr <- idr[!is.na(idr_reference)]
idr <- idr[, .(idr_reference = mean(idr_reference, na.rm = TRUE)), by = c("location_id", "date")]

idr$date <- as.IDate(idr$date)

# aggregate usa and wa
idr_usa_agg <- idr[location_id %in% usa_loc_ids]
idr_usa_agg[, location_id := 102]
idr_wa_agg <- idr[location_id %in% wa_state_loc_ids]
idr_wa_agg[, location_id := 570]
idr <- rbindlist(list(idr, idr_usa_agg, idr_wa_agg), use.names = TRUE)

idr <- merge(
  idr,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

# get national from admin 1
idr_summary_sub_locs_2 <- sort(unique(substr(idr[ihme_loc_id %like% "_"]$ihme_loc_id, 1, 3)))
idr_summary_sub_locs_2 <- idr_summary_sub_locs_2[!(idr_summary_sub_locs_2 %in% idr$ihme_loc_id)]

idr_nats <- rbindlist(lapply(
  idr_summary_sub_locs_2,
  change_subnat_to_nat,
  input_file = idr
))
idr <- rbind(idr, idr_nats)

# get regions from national
ihme_regions <- locs[level == 2, ihme_loc_id]
idr_regs <- rbindlist(lapply(
  ihme_regions,
  change_subnat_to_nat,
  input_file = idr
))
idr <- rbind(idr, idr_regs)

# aggregate to national
idr <- idr[, .(idr_reference = mean(idr_reference)), by = c("location_id", "date")]

# add ihme loc id
idr <- merge(
  idr,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

# fill missing locations with parent values
missing_locs <- locs[!location_id %in% unique(idr$location_id) & level >= 3, ihme_loc_id]
idr_missing_locs <- rbindlist(lapply(
  missing_locs,
  fill_loc_w_parent,
  input_file = idr
))
idr <- rbind(idr, idr_missing_locs)

# calc covid incidence
idr <- merge(
  idr,
  daily_covid,
  by = c("location_id", "date"),
  all.x = TRUE
)

idr[, covid_incidence := (1 / idr_reference) * daily_cases]
idr[covid_incidence == 0, covid_incidence := 0.01]

# Make sure that ETH_ uses national covid incidence
idr <- idr[order(location_id, date)]
eth_loc_ids <- locs[ihme_loc_id %like% "ETH_", location_id]
idr_eth_nat <- idr[location_id == 179]

setnames(idr_eth_nat, "covid_incidence", "covid_incidence_nat")

idr_eth_sub <- idr[location_id %in% eth_loc_ids]
idr_eth_sub <- merge(
  idr_eth_sub,
  idr_eth_nat[, c("covid_incidence_nat", "date")],
  by = "date",
  all.x = TRUE
)

idr_eth_sub[, covid_incidence := covid_incidence_nat]
idr_eth_sub <- idr_eth_sub[, -c("covid_incidence_nat")]
idr <- rbind(idr[!location_id %in% eth_loc_ids], idr_eth_sub)


idr[is.na(covid_incidence), covid_incidence := 0.01]

# lag idr
idr <- idr[order(location_id, date)]
idr[, idr_lagged := shift(idr_reference, 19), by = "location_id"]
idr[is.na(idr_lagged), idr_lagged := 0]

# create version of idr before loop
idr_initial <- copy(idr)

# Prep mobility ----------------------------------------------------------------

# fill in end of 2021 with mobility forecast
mobility[, date := as.IDate(date)]
mobility[date <= "2021-12-31" & is.na(mean), mean := mobility_forecast]

mobility <- mobility[, c("location_id", "date", "mean")]
mobility <- mobility[!is.na(mean)]
setnames(mobility, "mean", "mobility_reference")

mobility <- merge(
  mobility,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

# lag mobility
mobility <- mobility[order(date)]
mobility[, mobility_lagged := shift(mobility_reference, 19), by = "location_id"]
mobility[is.na(mobility_lagged), mobility_lagged := 0]

# create version of mobility before loop
mobility_initial <- copy(mobility)

# Prep seroprevalence ----------------------------------------------------------

# make long not wide
sero <- melt(
  sero,
  measure.vars = patterns("draw"),
  variable.name = "draw"
)

# date and draw
sero[, date := as.Date(date)]
sero[, draw_num := gsub("draw_","",draw)]

# Find first and last day of sero data across all draws
sero_keep <- sero[!is.na(value), .(min_date = min(date), max_date = max(date)), by = c("location_id", "draw")][, .(keep_date_start = max(min_date), keep_date_end = min(max_date)), by = "location_id"]

# merge on various start and end times
sero <- merge(
  sero,
  sero_keep,
  by = "location_id",
  all.x = TRUE
)

# subset to only locations in the infections time period
sero <- sero[date >= keep_date_start & date <= keep_date_end]

# collapse over draws
sero_summary <- sero[, .(mean = mean(value, na.rm = TRUE)), by = c("location_id", "date")]
colnames(sero_summary) <- c("location_id", "date", "daily_infections")

# aggregate USA and WA
assertthat::assert_that(!102 %in% unique(sero_summary$location_id))
sero_usa_agg <- sero_summary[location_id %in% usa_loc_ids]
sero_usa_agg[, location_id := 102]
sero_wa_agg <- sero_summary[location_id %in% wa_state_loc_ids]
sero_wa_agg[, location_id := 570]
sero_summary <- rbindlist(list(sero_summary, sero_usa_agg, sero_wa_agg), use.names = TRUE)

sero_summary <- merge(
  sero_summary,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

# aggregate admin2 to admin1
sero_summary_sub_locs_1 <- sort(locs[(ihme_loc_id %like% "KEN" & level == 4) | (ihme_loc_id %like% "GBR" & level == 5)]$ihme_loc_id)
sero_summary_sub_locs_1 <- sero_summary_sub_locs_1[sero_summary_sub_locs_1 != "IND_60896"]
sero_subs <- rbindlist(lapply(
  sero_summary_sub_locs_1,
  change_subnat_to_nat,
  input_file = sero_summary
))
sero_summary <- rbind(sero_summary, sero_subs)

# aggregate admin1 to national
sero_summary_sub_locs_2 <- sort(unique(substr(sero_summary[ihme_loc_id %like% "_"]$ihme_loc_id, 1, 3)))
sero_summary_sub_locs_2 <- sero_summary_sub_locs_2[!(sero_summary_sub_locs_2 %in% sero_summary$ihme_loc_id)]
sero_nats <- rbindlist(lapply(
  sero_summary_sub_locs_2,
  change_subnat_to_nat,
  input_file = sero_summary
))
sero_summary <- rbind(sero_summary, sero_nats)

# aggregate by new locations
sero_summary <- sero_summary[, .(daily_infections = sum(daily_infections)), by = c("location_id", "date")]

# add ihme_loc_id
sero <- merge(
  locs[, c("location_id","ihme_loc_id","location_name","parent_id")],
  sero_summary,
  by = "location_id",
  all.y = TRUE
)
sero$date <- as.IDate(sero$date)

# lag seroprevalence by 25 days
sero <- sero[order(location_id, date)]
sero[, infections_lagged := shift(daily_infections, 25), by = "location_id"]
sero[is.na(infections_lagged), infections_lagged := 0]

# create version of sero before loop
sero_initial <- copy(sero)

# Start dates ------------------------------------------------------------------

start_sero_dt <- copy(sero_initial)
start_deaths_dt <- copy(covid_initial)
start_cases_dt <- copy(daily_covid)

start_sero_dt <- start_sero_dt[daily_infections > 0, .(sero_start_date = min(date)), by = c("location_id")]
start_deaths_dt <- start_deaths_dt[deaths_covid > 0, .(deaths_start_date = min(date)), by = c("location_id")]
start_cases_dt <- start_cases_dt[daily_cases > 0, .(cases_start_date = min(date)), by = c("location_id")]

start_dt <- Reduce(
  function(x,y) merge(x, y, by = "location_id", all = TRUE),
  list(start_sero_dt, start_deaths_dt, start_cases_dt)
)

start_dt[, start_date := apply(start_dt[,-1], 1, FUN = min, na.rm = TRUE)]
start_dt[start_date < "2020-01-01", start_date := "2020-01-01"]

# add old norway locations using new location start dates
nor_start <- nor_map[location_id %in% nor_locs_old$location_id]
nor_start <- merge(
  nor_start[, c("location_id", "parent_id")],
  start_dt,
  by.x = "parent_id",
  by.y = "location_id",
  all.x = TRUE
)
nor_start[, parent_id := NULL]
start_dt <- rbind(start_dt, nor_start)

assertable::assert_values(start_dt, "start_date", test = "lte", test_val = "deaths_start_date")

start_dt <- merge(
  locs[, c("location_id","ihme_loc_id","location_name","region_name")],
  start_dt,
  by = "location_id",
  all.y = TRUE
)

# set IRN to 2020-01-01 since we have external estimated mort from week 1-38
start_dt[ihme_loc_id == "IRN", start_date := "2020-01-01"]

# set all china subnationals to consistent 2020-01-01
start_dt[ihme_loc_id %like% "CHN", start_date := "2020-01-01"]

readr::write_csv(start_dt, paste0())
start_dt <- start_dt[, c("location_id", "start_date")]
start_dt[, start_date := as.Date(start_date)]

# save to use for draws
readr::write_csv(start_dt, paste0())
if(GBD){
  readr::write_csv(start_dt, paste0())
} else {
  readr::write_csv(start_dt, paste0())
}


# Population yearly for excess -------------------------------------------------

pop_year <- merge(
  pop,
  start_dt,
  by = "location_id",
  all.x = T
)
pop_year <- pop_year[(time_unit == "day") & (date >= start_date) & (date <= all_time_cutoff), 
                .(person_years = sum(person_years)),
                by = c("ihme_loc_id","year_start","location_id","sex","age_name",
                       "type")]
pop_year[, ':=' (time_unit = "year", time_start = year_start)]
pop <- rbind(pop, pop_year, fill = TRUE)


# GBD population 2021 ----------------------------------------------------------

pop_2021 <- demInternal::get_dem_outputs(
  process_name = "population estimate",
  run_id = 298,
  gbd_year = gbd_year,
  year_ids = 2021,
  location_ids = locs[!ihme_loc_id %like% "CHN", location_id],
  sex_ids = c(3),
  age_group_ids = c(22),
  name_cols = TRUE,
  odbc_section = "HOST",
  odbc_dir = "~"
)

chn_pop_2021 <- demInternal::get_dem_outputs(
  process_name = "population estimate",
  run_id = 300,
  gbd_year = gbd_year,
  year_ids = 2021,
  location_ids = locs[ihme_loc_id %like% "CHN", location_id],
  sex_ids = c(3),
  age_group_ids = c(22),
  name_cols = TRUE,
  odbc_section = "HOST",
  odbc_dir = "~"
)

pop_2021 <- rbind(pop_2021, chn_pop_2021)

# old norway
nor_pop_old <- demInternal::get_dem_outputs(
  process_name = "population estimate",
  run_id = 192,
  gbd_year = 2019,
  year_ids = 2019,
  location_ids = nor_locs_old[, location_id],
  sex_ids = c(3),
  age_group_ids = c(22),
  name_cols = TRUE,
  odbc_section = "HOST",
  odbc_dir =  "~"
)
nor_pop_old[, ':=' (year_id = 2021, year_start = 2021)]
pop_2021 <- rbind(pop_2021,nor_pop_old)

# re-format
setnames(pop_2021, "mean", "population")
cols <- c("location_id", "year_start", "sex_name", "age_start", "age_end", "population")
pop_2021 <- pop_2021[, .SD, .SDcols = cols]
data.table::setkeyv(pop_2021, setdiff(cols, "population"))
pop_2021[, `:=` (pop_source = "gbd")]
pop_2021[is.infinite(age_end), age_end := 125]
pop_2021 <- hierarchyUtils::gen_name(pop_2021, col_stem = "age")
pop_2021 <- pop_2021[, -c("age_start","age_end")]

# combine ITA 35498 & 35499
ita99999 <- pop_2021[location_id %in% c(35498, 35499)]
ita99999[, location_id := 99999]
ita99999 <- ita99999[,
                     list(population = sum(population)),
                     by = c("location_id", "year_start", "sex_name",
                            "pop_source", "age_name")
                     ]
pop_2021 <- rbind(pop_2021, ita99999)

covid_team_pop <- fread("")
covid_team_pop <- covid_team_pop[, list(population = sum(population)), by = "location_id"]

# re-format
covid_team_pop[, `:=` (pop_source = "covid_team", age_name = "0 to 125", sex_name = "all",
                       year_start = 2021)]

missing_pop <- unique(locs[!location_id %in% unique(pop_2021$location_id), location_id])
pop_2021 <- rbind(pop_2021, covid_team_pop[location_id %in% missing_pop])

pop_2021 <- merge(
  locs[, c("location_id", "ihme_loc_id")],
  pop_2021,
  by = "location_id",
  all.y = TRUE
)
pop_2021[location_id == 99999, ihme_loc_id := "ITA_99999"]

pop_2021 <- pop_2021[, ':=' (person_years = population, time_unit = "year",
                             time_start = year_start)]

readr::write_csv(pop_2021, paste0())

# Prep covariates with dataset specifics ---------------------------------------

# create version of dt before loop
dt_initial <- copy(dt)

for (dataset_type in dataset_types) {

  message(paste0("Working on the ", toupper(dataset_type), " dataset"))

  dt <- copy(dt_initial)

  ## set time frames for prediction dataset ##
  year_cutoff <- lubridate::year(all_time_cutoff)
  if(all_time_cutoff == "2021-12-31"){
    week_cutoff = 52
  } else if (all_time_cutoff == "2020-12-31") {
    week_cutoff = 53
  } else {
    week_cutoff <- lubridate::week(all_time_cutoff)
  }
  month_cutoff <- lubridate::month(all_time_cutoff)
  

  # Prep deaths excess ---------------------------------------------------------

  # merge on pop for weighted mean and aggregation - has 2020 and 2021 separate 
  dt <- merge(
    dt,
    pop[, c("location_id","person_years","year_start","time_start","time_unit")],
    by = c("location_id","year_start","time_start","time_unit"),
    all.x = TRUE
  )
  dt[is.na(death_rate_excess), death_rate_excess := deaths_excess / person_years]

  if (dataset_type == "all") {

    dt <- dt[year_start %in% covid_years]
    
  } else if (dataset_type == "only2020") {

    dt <- dt[year_start == 2020]
    
  } else if (dataset_type == "only2021") {

    dt <- dt[year_start == 2021]

  } else if (dataset_type == "fit") {
    
    dt <- dt[year_start %in% covid_years]
    
  } else if (dataset_type %in% c("custom", "custom2020", "custom2021")) {

    # outlier by week and month
    dt <- merge(
      dt,
      keep_times,
      by = c("location_id","ihme_loc_id","year_start","time_start","time_unit"),
      all.x = TRUE
    )
    dt <- dt[(keep == 1)]
    dt <- dt[, -c("keep")]

  }

  # IRN is actually weeks 9-39
  irn_pop <- pop[location_id == 142 & time_unit == "week" & year_start == 2020 & time_start %in% 1:38]
  irn_py <- irn_pop[, .(person_years = sum(person_years))]
  dt[location_id == 142, person_years := irn_py]
  
  # aggregate over draws 
  dt <- dt[,.(death_rate_excess = mean(death_rate_excess)),
           by = c("location_id","year_start","time_start","time_unit",
                  "ihme_loc_id","location_name","person_years")]

  # aggregate over time
  dt <- dt[,.(death_rate_excess = sum(death_rate_excess * person_years, na.rm = T) / sum(person_years),
             death_rate_person_years = sum(person_years)),
           by = c("location_id","ihme_loc_id","location_name")]

  # make sure gbd subnats are present
  if (!dataset_type %in% c("custom", "custom2020", "custom2021", "fit")) {
    gbd_locs <- locs[!location_id %in% unique(dt$location_id), c("location_id","ihme_loc_id","location_name")]
    if (!99999 %in% unique(dt$location_id)){
      gbd_locs <- rbind(gbd_locs, data.table(location_id = 99999, ihme_loc_id = "ITA_99999", location_name = "Provincia autonoma di Bolzano + Provincia autonoma di Trento"))
    }
    dt <- rbind(dt, gbd_locs, fill = TRUE)
  }
  
  
  # Prep person-years ----------------------------------------------------------
  
  pop_sub <- copy(pop)
  
  pop_sub <- merge(
    pop_sub,
    start_dt,
    by = "location_id",
    all.x = TRUE
  )
  
  if (dataset_type == "all") {
    
    pop_sub <- pop_sub[(time_unit == "day") & (date >= start_date) & (date <= all_time_cutoff)]
    
  } else if (dataset_type == "only2020") {
    
    pop_sub <- pop_sub[(time_unit == "day") & (date >= start_date) & (year_start == 2020)]
    
  } else if (dataset_type == "only2021") {
    
    pop_sub <- pop_sub[(time_unit == "day") & (year_start == 2021)]
    
  } else if (dataset_type == "fit") {
    
    pop_sub <- merge(
      pop_sub,
      keep_years,
      by = c("location_id", "year_start"),
      all.x = TRUE
    )
    pop_sub <- pop_sub[(keep == 1) & (time_unit == "day") &
                         (date >= start_date) & (date <= all_time_cutoff)]
    
    
  } else if (dataset_type %in% c("custom", "custom2020", "custom2021")) {
    
    pop_sub <- merge(
      pop_sub,
      keep_times,
      by = c("location_id", "ihme_loc_id", "year_start", "time_start", "time_unit"),
      all.x = TRUE
    )
    pop_sub <- pop_sub[(keep == 1)]
    
  }
  
  if (dataset_type == "only2021"){
    pop_agg <- pop_2021[, c("location_id","person_years")]
  } else {
    # aggregate
    pop_agg <- pop_sub[, .(person_years = sum(person_years)), by = "location_id"]
  }
  
  dt <- merge(
    dt,
    pop_agg,
    by = "location_id",
    all.x = TRUE
  )


  # Prep gbd covariates --------------------------------------------------------

  gbd_cov_folder <- ""
  gbd_cov_files <- list.files(gbd_cov_folder, full.names = TRUE, pattern = "")
  gbd_cov_names <- gsub("","", gbd_cov_files)

  # read in and rename by filename
  gbd_covs <- fread(gbd_cov_files[1])[, c("location_id", "mean_value")]
  setnames(gbd_covs, "mean_value", gbd_cov_names[1])

  for (file in gbd_cov_files[-1]) {
    cov <- fread(file)
    cov_name <- gsub("","", file)
    setnames(cov, "mean_value",
             cov_name,
             skip_absent = TRUE)
    keep <- c("location_id", cov_name)
    gbd_covs <- merge(
      gbd_covs,
      cov[, ..keep],
      by = "location_id",
      all = TRUE
    )
  }

  # create ITA_99999 from ITA_35498 and ITA_35499
  ita_combine <- copy(gbd_covs)
  ita_combine <- ita_combine[location_id %in% c(35498, 35499)]
  ita_combine[, location_id := 99999]
  ita_combine <- ita_combine[, .(cirrhosis_death_rate = mean(cirrhosis_death_rate),
                                 ckd_death_rate = mean(ckd_death_rate),
                                 cong_downs_death_rate = mean(cong_downs_death_rate),
                                 cvd_death_rate = mean(cvd_death_rate),
                                 cvd_pah_death_rate = mean(cvd_pah_death_rate),
                                 cvd_stroke_cerhem_death_rate = mean(cvd_stroke_cerhem_death_rate),
                                 endo_death_rate = mean(endo_death_rate),
                                 diabetes_death_rate = mean(diabetes_death_rate),
                                 gbd_obesity = mean(gbd_obesity),
                                 gbd_inpatient_admis = mean(gbd_inpatient_admis),
                                 gbd_diabetes = mean(gbd_diabetes),
                                 HAQI = mean(HAQI),
                                 hemog_sickle_death_rate = mean(hemog_sickle_death_rate),
                                 hemog_thalass_death_rate = mean(hemog_thalass_death_rate),
                                 hiv_death_rate = mean(hiv_death_rate),
                                 hypertension_prevalence = mean(hypertension_prevalence),
                                 avg_abs_latitude = mean(avg_abs_latitude),
                                 ncd_death_rate = mean(ncd_death_rate),
                                 neo_death_rate = mean(neo_death_rate),
                                 neuro_death_rate = mean(neuro_death_rate),
                                 resp_asthma_death_rate = mean(resp_asthma_death_rate),
                                 resp_copd_death_rate = mean(resp_copd_death_rate),
                                 smoking_prevalence = mean(smoking_prevalence),
                                 subs_death_rate = mean(subs_death_rate),
                                 universal_health_coverage = mean(universal_health_coverage)
  ), by = "location_id"]

  # update Norway subnationals for 2019 covs
  nor_subs <- gbd_covs[location_id %in% nor_locs_old$location_id]
  nor_subs <- merge(
    nor_subs,
    pop_agg,
    by = "location_id",
    all.x = TRUE
  )
  nor_subs_old <- merge(
    nor_subs,
    nor_map[!parent_id == 90 & !parent_id == 73, -c("level","location_name")],
    by = "location_id",
    all.x = TRUE
  )
  nor_subs_old[, location_id := parent_id]
  nor_subs_new <- nor_subs_old[, .(avg_abs_latitude = mean(avg_abs_latitude),
                                   cirrhosis_death_rate = weighted.mean(cirrhosis_death_rate, person_years),
                                   ckd_death_rate = weighted.mean(ckd_death_rate, person_years),
                                   cong_downs_death_rate = weighted.mean(cong_downs_death_rate, person_years),
                                   cvd_death_rate = weighted.mean(cvd_death_rate, person_years),
                                   cvd_pah_death_rate = weighted.mean(cvd_pah_death_rate, person_years),
                                   cvd_stroke_cerhem_death_rate = weighted.mean(cvd_stroke_cerhem_death_rate, person_years),
                                   diabetes_death_rate = weighted.mean(diabetes_death_rate, person_years),
                                   endo_death_rate = weighted.mean(endo_death_rate, person_years),
                                   gbd_diabetes = weighted.mean(gbd_diabetes, person_years),
                                   gbd_obesity = weighted.mean(gbd_obesity, person_years),
                                   HAQI = weighted.mean(HAQI, person_years),
                                   hemog_sickle_death_rate = weighted.mean(hemog_sickle_death_rate, person_years),
                                   hemog_thalass_death_rate = weighted.mean(hemog_thalass_death_rate),
                                   hiv_death_rate = weighted.mean(hiv_death_rate, person_years),
                                   hypertension_prevalence = weighted.mean(hypertension_prevalence, person_years),
                                   ncd_death_rate = weighted.mean(ncd_death_rate, person_years),
                                   neo_death_rate = weighted.mean(neo_death_rate, person_years),
                                   neuro_death_rate = weighted.mean(neuro_death_rate, person_years),
                                   resp_asthma_death_rate = weighted.mean(resp_asthma_death_rate, person_years),
                                   resp_copd_death_rate = weighted.mean(resp_copd_death_rate, person_years),
                                   smoking_prevalence = weighted.mean(smoking_prevalence, person_years),
                                   subs_death_rate = weighted.mean(subs_death_rate, person_years),
                                   universal_health_coverage = weighted.mean(universal_health_coverage, person_years)
  ), by = c("location_id")]

  nor_subs_new <- merge(
    nor_subs_new,
    gbd_covs[, c("location_id","gbd_inpatient_admis")],
    by = "location_id",
    all.x = TRUE
  )

  gbd_covs <- gbd_covs[!location_id %in% nor_subs_new$location_id]

  gbd_covs <- rbind(gbd_covs, ita_combine, nor_subs_new)

  assertable::assert_values(
    gbd_covs[!location_id %in% locs[level < 3, location_id]], gbd_cov_names, test = "gte", test_val = 0, warn_only = T
  )

  # Finalize and add covid deaths ----------------------------------------------

  covid <- copy(covid_initial)

  covid <- merge(
    covid,
    start_dt,
    by = "location_id",
    all.x = T
  )

  if (dataset_type == "all") {

    covid <- covid[(date >= start_date) & (date <= all_time_cutoff)]

  } else if (dataset_type == "only2020") {

    covid <- covid[(date >= start_date) & (date <= as.Date("2020-12-31"))]

  } else if (dataset_type == "only2021") {

    covid <- covid[(date >= as.Date("2021-01-01")) & (date <= as.Date("2021-12-31"))]

  } else if (dataset_type == "fit") {
    
    covid[, year_start := lubridate::year(date)]
    covid <- merge(
      covid,
      keep_years,
      by = c("location_id", "year_start"),
      all.x = TRUE
    )
    covid <- covid[(keep == 1) & (date >= start_date) & (date <= all_time_cutoff)]
    
    
  } else if (dataset_type %in% c("custom", "custom2020", "custom2021")) {

    # get week/year from date based on country week type
    covid <- add_weeks_months(dt = covid, gbd_year = gbd_year, data_code_dir = data_code_dir)

    # outlier by week and month
    covid <- merge(
      covid,
      keep_times,
      by = c("location_id", "year_start", "time_start", "time_unit"),
      all.x = TRUE
    )
    covid <- covid[(keep == 1)]

  }

  if(dataset_type %in% c("custom", "custom2020", "custom2021")) {
    covid <- covid[, list(deaths_covid = sum(deaths_covid, na.rm = TRUE)),
                   by = c("location_id","year_start","time_start","time_unit")]

    covid <- merge(
      covid,
      pop[, c("location_id","person_years","year_start","time_start","time_unit")],
      by = c("location_id","year_start","time_start","time_unit"),
      all.x = TRUE
    )
  } else {
    # merge on person-years
    pop_sub$date <- as.IDate(pop_sub$date)
    covid <- merge(
      covid,
      pop_sub[time_unit == "day", c("location_id","person_years","date")],
      by = c("location_id","date"),
      all.x = TRUE 
    )
  }

  # aggregate over time unit
  covid <- covid[, .(deaths_covid = sum(deaths_covid, na.rm = TRUE),
                     person_years = sum(person_years, na.rm = TRUE)), by = c("location_id")]

  # Rake GBR UTLA covid deaths to England
  gbr_4749_covid <- covid[location_id %in% c(4749, gbr_utla_loc_ids), c("location_id","deaths_covid")]
  gbr_4749_covid[, index := 1]
  scaled_eng <- hierarchyUtils::scale(dt = gbr_4749_covid,
                                      id_cols = c("location_id", "index"),
                                      value_cols = c("deaths_covid"),
                                      col_stem = "location_id",
                                      col_type = "categorical",
                                      mapping = eng_map,
                                      missing_dt_severity = "warning",
                                      na_value_severity = "warning",
                                      collapse_missing = TRUE)
  setnames(scaled_eng, "deaths_covid", "deaths_covid_scaled")
  covid <- merge(
    covid,
    scaled_eng[, c("location_id", "deaths_covid_scaled")],
    by = "location_id",
    all.x = T
  )
  covid[!is.na(deaths_covid_scaled), deaths_covid := deaths_covid_scaled]
  covid[, deaths_covid_scaled := NULL]

  # get death rate covid
  covid[, death_rate_covid := deaths_covid / person_years]

  setnames(covid, "person_years", "covid_person_years")
  dt <- merge(
    dt,
    covid[, c("location_id","deaths_covid","death_rate_covid","covid_person_years")],
    by = c("location_id"),
    all.x = TRUE
  )

  # KNA has 0 covid deaths before cutoff
  dt[location_id == 393 & deaths_covid == 0, death_rate_covid := 0]


  # Finalize and add IDR -------------------------------------------------------

  idr <- copy(idr_initial)

  idr <- merge(
    idr,
    start_dt,
    by = "location_id",
    all.x = T
  )

  if (dataset_type == "all") {

    idr <- idr[(date >= start_date) & (date <= all_time_cutoff)]

  } else if (dataset_type == "only2020") {

    idr <- idr[(date >= start_date) & (date <= as.Date("2020-12-31"))]

  } else if (dataset_type == "only2021") {

    idr <- idr[(date >= as.Date("2021-01-01")) & (date <= as.Date("2021-12-31"))]

  } else if (dataset_type == "fit") {
    
    idr[, year_start := lubridate::year(date)]
    idr <- merge(
      idr,
      keep_years,
      by = c("location_id", "year_start"),
      all.x = TRUE
    )
    idr <- idr[(keep == 1) & (date >= start_date) & (date <= all_time_cutoff)]
    
  } else if (dataset_type %in% c("custom", "custom2020", "custom2021")) {

    # get week/year from date based on country week type
    idr <- add_weeks_months(dt = idr, gbd_year = gbd_year, data_code_dir = data_code_dir)

    # outlier by week and month
    idr <- merge(
      idr,
      keep_times,
      by = c("location_id", "ihme_loc_id", "year_start", "time_start", "time_unit"),
      all.x = TRUE
    )
    idr <- idr[(keep == 1)]

  }

  # ITA_99999 is made from ITA_35498 and ITA_35499 in allcause data
  ita_combine <- copy(idr)
  ita_combine <- ita_combine[location_id %in% c(35498, 35499)]
  ita_combine[, location_id := 99999]
  idr <- rbind(idr, ita_combine)

  # do not weight TZA
  idr_tza <- idr[ihme_loc_id == "TZA",
                 list(idr_reference = mean(idr_reference, na.rm = TRUE),
                      idr_lagged = mean(idr_lagged, na.rm = TRUE),
                      confirmed_cases = sum(daily_cases, na.rm = TRUE),
                      covid_incidence = sum(covid_incidence, na.rm = TRUE)
                 ),
                 by = c("location_id")
                 ]

  idr <- idr[!ihme_loc_id == "TZA",
             list(idr_reference = weighted.mean(idr_reference, covid_incidence, na.rm = TRUE),
                  idr_lagged = weighted.mean(idr_lagged, covid_incidence, na.rm = TRUE),
                  confirmed_cases = sum(daily_cases, na.rm = TRUE),
                  covid_incidence = sum(covid_incidence, na.rm = TRUE)
             ),
             by = c("location_id")
             ]
  idr <- rbind(idr, idr_tza)

  if (GBD) {

    if (dataset_type == "all") {

      idr_all <- copy(idr)

    } else if (dataset_type == "only2020") {

      idr_only2020 <- copy(idr)

    } else if (dataset_type == "only2021") {

      idr_only2021 <- copy(idr)

    }

    if (exists("idr_all") & exists("idr_only2020") & exists("idr_only2021")) {

      idr_all_usa <- idr_all[location_id == 102]
      idr_only2020_usa <- idr_only2020[location_id == 102]
      idr_only2021_usa <- idr_only2021[location_id == 102]

      idr_usa <- rbind(idr_only2020_usa, idr_only2021_usa)

      test <- idr_usa[, list(idr_reference = weighted.mean(idr_reference, covid_incidence, na.rm = T),
                             idr_lagged = weighted.mean(idr_lagged, covid_incidence, na.rm = T),
                             confirmed_cases = sum(confirmed_cases, na.rm = TRUE),
                             covid_incidence = sum(covid_incidence, na.rm = TRUE))
                      ]

      idr_ref_min <- min(idr_only2020_usa$idr_reference, idr_only2021_usa$idr_reference)
      idr_ref_max <- max(idr_only2020_usa$idr_reference, idr_only2021_usa$idr_reference)

      idr_lag_min <- min(idr_only2020_usa$idr_lagged, idr_only2021_usa$idr_lagged)
      idr_lag_max <- max(idr_only2020_usa$idr_lagged, idr_only2021_usa$idr_lagged)

      idr_ref_test <- idr_all_usa[round(idr_reference, 6) == round(test$idr_reference, 6) & idr_reference >= idr_ref_min & idr_reference <= idr_ref_max]
      idr_lag_test <- idr_all_usa[round(idr_lagged, 6) == round(test$idr_lagged, 6) & idr_lagged >= idr_lag_min & idr_lagged <= idr_lag_max]

      assertable::assert_nrows(idr_ref_test, 1)
      assertable::assert_nrows(idr_lag_test, 1)

      idr_check_passed <- TRUE

    }

  }

  dt <- merge(
    dt,
    idr[, c("location_id", "idr_reference", "idr_lagged")],
    by = "location_id",
    all.x = TRUE
  )

  # Finalize and add mobility --------------------------------------------------

  mobility <- copy(mobility_initial)

  mobility <- merge(
    mobility,
    start_dt,
    by = "location_id",
    all.x = TRUE
  )

  if (dataset_type == "all") {

    mobility <- mobility[(date >= start_date) & (date <= all_time_cutoff)]

  } else if (dataset_type == "only2020") {

    mobility <- mobility[(date >= start_date) & (date <= as.Date("2020-12-31"))]

  } else if (dataset_type == "only2021") {

    mobility <- mobility[(date >= as.Date("2021-01-01")) & (date <= as.Date("2021-12-31"))]

  } else if (dataset_type == "fit") {
    
    mobility[, year_start := lubridate::year(date)]
    # outlier by week and month
    mobility <- merge(
      mobility,
      keep_years,
      by = c("location_id","year_start"),
      all.x = TRUE
    )
    mobility <- mobility[(keep == 1) & (date >= start_date) & (date <= all_time_cutoff)]
    
  } else if (dataset_type %in% c("custom", "custom2020", "custom2021")) {

    # add time unit
    mobility <- add_weeks_months(dt = mobility, gbd_year = gbd_year, data_code_dir = data_code_dir)

    # outlier by week and month
    mobility <- merge(
      mobility,
      keep_times,
      by = c("location_id","year_start","time_start","time_unit"),
      all.x = TRUE
    )
    mobility <- mobility[(keep == 1)]

  }

  # ITA_99999 is made from ITA_35498 and ITA_35499 in allcause data
  ita_combine <- copy(mobility)
  ita_combine <- ita_combine[location_id %in% c(35498, 35499)]
  ita_combine[, location_id := 99999]
  mobility <- rbind(mobility, ita_combine)

  mobility <- mobility[, .(mobility_reference = mean(mobility_reference),
                           mobility_lagged = mean(mobility_lagged)),
                       by = "location_id"]

  dt <- merge(
    dt,
    mobility,
    by = "location_id",
    all.x = TRUE
  )

  # Finalize and add seroprevalence --------------------------------------------

  sero <- copy(sero_initial)

  sero <- merge(
    sero,
    start_dt,
    by = "location_id",
    all.x = TRUE
  )

  if (dataset_type == "all") {

    sero <- sero[(date >= start_date) & (date <= all_time_cutoff)]

  } else if (dataset_type == "only2020") {

    sero <- sero[(date >= start_date) & (date <= as.Date("2020-12-31"))]

  } else if (dataset_type == "only2021") {

    sero <- sero[(date >= as.Date("2021-01-01")) & (date <= as.Date("2021-12-31"))]

  } else if (dataset_type == "fit") {
    
    sero[, year_start := lubridate::year(date)]
    sero <- merge(
      sero,
      keep_years,
      by = c("location_id","year_start"),
      all.x = TRUE
    )
    sero <- sero[(keep == 1) & (date >= start_date) & (date <= all_time_cutoff)]
    
  } else if (dataset_type %in% c("custom", "custom2020", "custom2021")) {

    # add week and month
    sero <- add_weeks_months(dt = sero, gbd_year = gbd_year, data_code_dir = data_code_dir)

    # outlier by week and month
    sero <- merge(
      sero,
      keep_times[, -c("ihme_loc_id")],
      by = c("location_id","year_start","time_start","time_unit"),
      all.x = TRUE
    )
    sero <- sero[(keep == 1)]

  }

  # match population based on time unit or date
  if (dataset_type %in% c("custom", "custom2020", "custom2021")){
    # aggregate to week to match with population
    sero <- sero[, .(daily_infections = sum(daily_infections, na.rm = TRUE),
                     daily_infections_lagged = sum(infections_lagged, na.rm = TRUE)),
                 by = c("location_id","year_start","time_start","time_unit")]

    # merge on person-years
    sero <- merge(
      sero,
      pop[, c("location_id","person_years","year_start","time_start","time_unit")],
      by = c("location_id","year_start","time_start","time_unit"),
      all.x = TRUE
    )
  } else {

    sero <- sero[, .(daily_infections = sum(daily_infections, na.rm = TRUE),
                     daily_infections_lagged = sum(infections_lagged, na.rm = TRUE)),
                 by = c("location_id","date")]

    # merge on person-years
    sero <- merge(
      sero,
      pop_sub[time_unit == "day", c("location_id","person_years","date")],
      by = c("location_id","date"),
      all.x = TRUE # keeps same number of person years
    )
  }

  # aggregate
  sero <- sero[, .(cumulative_infections = sum(daily_infections, na.rm = TRUE),
                   cumulative_infections_lagged = sum(daily_infections_lagged, na.rm = TRUE),
                   person_years = sum(person_years, na.rm = TRUE)), by = "location_id"]

  # convert to rate
  sero[, cumulative_infections := cumulative_infections / person_years]
  sero[, cumulative_infections_lagged := cumulative_infections_lagged / person_years]

  if (dataset_type == "only2020") {

    sero[location_id == 43870 & is.na(cumulative_infections), cumulative_infections := 0] 
    sero[location_id == 43870 & is.na(cumulative_infections_lagged), cumulative_infections_lagged := 0] 

  }

  assertable::assert_values(
    sero[!location_id == 6],
    c("cumulative_infections", "cumulative_infections_lagged"),
    test = "gte", test_val = 0, warn_only = TRUE
  )

  setnames(sero, "person_years", "ci_person_years")
  # combine
  dt <- merge(
    dt,
    sero[, c("location_id", "cumulative_infections", "cumulative_infections_lagged", "ci_person_years")],
    by = "location_id",
    all.x = TRUE
  )

  # Loop to add covariates with flat files -------------------------------------

  for (cov in c("crude_death_rate","crude_death_rate_sd_1990","crude_death_rate_sd_2000",
                "mean_pop_age","stars","prop_pop_60plus","prop_pop_70plus",
                "prop_pop_75plus","prop_pop_80plus","prop_pop_85plus")) {

    temp <- get(cov)

    temp <- validate_data(
      dt = temp,
      process_locations = dt_initial,
      not_na =  c("location_id", cov)
    )

    assertable::assert_values(temp, c(cov), test = "gte", test_val = 0, warn_only = TRUE)

    temp <- temp[, .(location_id, get(cov))]
    colnames(temp) <- c("location_id", cov)

    dt <- merge(
      dt,
      temp,
      by = "location_id",
      all.x = TRUE
    )

  }

  # Final dataset tweaks -------------------------------------------------------

  ## combine ITA 35498 & 35499 to make ITA 99999 ##
  dt_ita <- dt[location_id %in% c(35498, 35499)]

  dt_ita[, ':=' (location_id = 99999,
                 ihme_loc_id = "ITA_99999",
                 location_name = "Provincia autonoma di Bolzano + Provincia autonoma di Trento")]

  dt_ita <- dt_ita[, .(crude_death_rate = mean(crude_death_rate),
                       crude_death_rate_sd_1990 = mean(crude_death_rate_sd_1990),
                       crude_death_rate_sd_2000 = mean(crude_death_rate_sd_2000),
                       mean_pop_age = mean(mean_pop_age),
                       cumulative_infections = mean(cumulative_infections),
                       cumulative_infections_lagged = mean(cumulative_infections_lagged),
                       ci_person_years = sum(ci_person_years),
                       prop_pop_60plus = mean(prop_pop_60plus),
                       prop_pop_70plus = mean(prop_pop_70plus),
                       prop_pop_75plus = mean(prop_pop_75plus),
                       prop_pop_80plus = mean(prop_pop_80plus),
                       prop_pop_85plus = mean(prop_pop_85plus)),
                   by = c("location_id","ihme_loc_id")
  ]

  dt[location_id == 99999, ':=' (
    crude_death_rate = dt_ita[, crude_death_rate],
    crude_death_rate_sd_1990 = dt_ita[, crude_death_rate_sd_1990],
    crude_death_rate_sd_2000 = dt_ita[, crude_death_rate_sd_2000],
    mean_pop_age = dt_ita[, mean_pop_age],
    cumulative_infections = dt_ita[, cumulative_infections],
    cumulative_infections_lagged = dt_ita[, cumulative_infections_lagged],
    ci_person_years = dt_ita[, ci_person_years],
    prop_pop_60plus = dt_ita[, prop_pop_60plus],
    prop_pop_70plus = dt_ita[, prop_pop_70plus],
    prop_pop_75plus = dt_ita[, prop_pop_75plus],
    prop_pop_80plus = dt_ita[, prop_pop_80plus],
    prop_pop_85plus = dt_ita[, prop_pop_85plus],
    stars = 5
  )]

  if (dataset_type %in% c("custom", "custom2020", "custom2021", "fit")) dt <- dt[!location_id %in% c(35498, 35499)]

  ## add gbd covs ##

  dt <- merge(
    dt,
    gbd_covs,
    by = "location_id",
    all.x = TRUE
  )

  ## add custom india values ##

  ind_custom <- ind_custom[!is.na(death_rate_excess)] 

  if (dataset_type %in% c("custom", "custom2020", "custom2021", "fit")) {

    dt <- dt[!location_id %in% unique(ind_custom$location_id)]
    dt <- rbind(dt, ind_custom)

  } else {

    dt[location_id %in% unique(ind_custom$location_id), death_rate_excess := ind_custom[, death_rate_excess]]

  }

  ## fill subnats in only covid hierarchy with higher admin level for datasets except "custom" ##

  if (dataset_type %in% c("all", "only2020", "only2021")) {

    fill_subnat_with_nat <- function(national_ihme_loc, dt) {
      national <- dt[ihme_loc_id == national_ihme_loc]

      if (national_ihme_loc == "USA_570") {
        national_ihme_loc <- "wa_state"
      }

      dt[location_id %in% get(paste0(tolower(national_ihme_loc), "_loc_ids")),
         ':=' (stars = national$stars,
               crude_death_rate = national$crude_death_rate,
               crude_death_rate_sd_1990 = national$crude_death_rate_sd_1990,
               crude_death_rate_sd_2000 = national$crude_death_rate_sd_2000,
               mean_pop_age = national$mean_pop_age,
               gbd_inpatient_admis = national$gbd_inpatient_admis,
               gbd_diabetes = national$gbd_diabetes,
               prop_pop_60plus = national$prop_pop_60plus,
               prop_pop_70plus = national$prop_pop_70plus,
               prop_pop_75plus = national$prop_pop_75plus,
               prop_pop_80plus = national$prop_pop_80plus,
               prop_pop_85plus = national$prop_pop_85plus,
               avg_abs_latitude = national$avg_abs_latitude,
               cirrhosis_death_rate = national$cirrhosis_death_rate,
               ckd_death_rate = national$ckd_death_rate,
               cong_downs_death_rate = national$cong_downs_death_rate,
               cvd_death_rate = national$cvd_death_rate,
               cvd_pah_death_rate = national$cvd_pah_death_rate,
               cvd_stroke_cerhem_death_rate = national$cvd_stroke_cerhem_death_rate,
               diabetes_death_rate = national$diabetes_death_rate,
               endo_death_rate = national$endo_death_rate,
               gbd_obesity = national$gbd_obesity,
               HAQI = national$HAQI,
               hemog_sickle_death_rate = national$hemog_sickle_death_rate,
               hemog_thalass_death_rate = national$hemog_thalass_death_rate,
               hiv_death_rate = national$hiv_death_rate,
               hypertension_prevalence = national$hypertension_prevalence,
               ncd_death_rate = national$ncd_death_rate,
               neo_death_rate = national$neo_death_rate,
               neuro_death_rate = national$neuro_death_rate,
               resp_asthma_death_rate = national$resp_asthma_death_rate,
               resp_copd_death_rate = national$resp_copd_death_rate,
               smoking_prevalence = national$smoking_prevalence,
               subs_death_rate = national$subs_death_rate,
               universal_health_coverage = national$universal_health_coverage)]
    } 

    for (national_ihme_loc in c("CAN","DEU","ESP","USA_570")) {

      fill_subnat_with_nat(national_ihme_loc = national_ihme_loc, dt = dt)

    }

  }

  #remove china mainland and china without macao and hong kong 
  dt <- dt[!location_id %in% c(6, 44533)]

  # make sure 0s are small positive values for CI since it is logged
  dt[cumulative_infections == 0, cumulative_infections := .00001]
  dt[cumulative_infections_lagged == 0, cumulative_infections_lagged := .00001]
  dt[is.na(cumulative_infections) & location_id %in% locs[level>2, location_id], cumulative_infections := .00001]
  dt[is.na(cumulative_infections_lagged) & location_id %in% locs[level>2, location_id], cumulative_infections_lagged := .00001]

  # give 0 idr in 2020 where values are missing
  dt[location_id %in% late_pan_islands & is.na(idr_reference), idr_reference := 0]
  dt[location_id %in% late_pan_islands & is.na(idr_lagged), idr_lagged := 0]
  
  # Use median IND covariate values for 4857, 4850, 4853, & 4869
  ind_state_replace <- c(4857, 4850, 4853, 4869)
  dt_ind <- dt[location_id %in% locs[ihme_loc_id %like% "IND" & level > 3, location_id]]
  dt_ind[, group := "admin1"]
  dt_ind[location_name %like% "Urban", group := "urban"]
  dt_ind[location_name %like% "Rural", group := "rural"]
  median_covs <- setdiff(colnames(dt_ind), c("location_id", "ihme_loc_id", 
                                             "location_name", "person_years",
                                             "death_rate_excess", "death_rate_person_years", 
                                             "death_rate_covid", "deaths_covid", "covid_person_years",
                                             "avg_abs_latitude", "group"))
  for (col in median_covs){
    dt_ind[, get("col") := median(get(col), na.rm = T), by = "group"]
  }
  dt_ind[, group := NULL]
  dt_ind <- dt_ind[location_id %in% locs[location_id %in% ind_state_replace | parent_id %in% ind_state_replace, location_id]]
  dt <- dt[!location_id %in% dt_ind[, location_id]]
  dt <- rbind(dt, dt_ind)
  

  # Run checks -----------------------------------------------------------------

  # break if 99999 not present
  if (!99999 %in% unique(dt$location_id)) {
    stop("Bring back ITA 99999")
  }

  # confirm columns
  data_cols <- c("location_id","ihme_loc_id","location_name","person_years","death_rate_person_years",
                 "death_rate_excess","death_rate_covid","deaths_covid","covid_person_years","avg_abs_latitude",
                 "stars","universal_health_coverage","HAQI",
                 "idr_reference","idr_lagged","mobility_reference","mobility_lagged",
                 "cumulative_infections","cumulative_infections_lagged", "ci_person_years",
                 "crude_death_rate","crude_death_rate_sd_1990","crude_death_rate_sd_2000",
                 "mean_pop_age","prop_pop_60plus","prop_pop_70plus",
                 "prop_pop_75plus","prop_pop_80plus","prop_pop_85plus",
                 gbd_cov_names)

  assert_colnames(dt, data_cols, only_colnames = TRUE)

  assert_values(dt[!is.na(death_rate_person_years)], c("death_rate_person_years"), test = "lte", test_val = "person_years")
  assert_values(dt[!is.na(covid_person_years)], c("covid_person_years"), test = "lte", test_val = "person_years")
  assert_values(dt[!is.na(ci_person_years)], c("ci_person_years"), test = "lte", test_val = "person_years")

  assert_values(dt[location_id %in% locs[level > 2, location_id]], c("cumulative_infections","cumulative_infections_lagged"), test = "gte", test_val = 0)
  assert_values(dt[location_id %in% locs[level > 2, location_id] & !is.na(deaths_covid)], c("deaths_covid","death_rate_covid"), test = "gte", test_val = 0)

  # Save -----------------------------------------------------------------------

  if (GBD) {

    if (dataset_type == "all") {

      readr::write_csv(dt, paste0())

    } else if (dataset_type == "only2020") {

      readr::write_csv(dt, paste0())

    } else if (dataset_type == "only2021") {

      readr::write_csv(dt, paste0())

    } else if (dataset_type == "fit") {
      
      readr::write_csv(dt, paste0())
      
    } else if (dataset_type == "custom2020") {

      dt[, year_start := 2020]
      readr::write_csv(dt, paste0())

    } else if (dataset_type == "custom2021") {

      dt[, year_start := 2021]
      readr::write_csv(dt, paste0())

    } else if (dataset_type == "custom") {

      readr::write_csv(dt, paste0())
    }

  } else {

    if (dataset_type == "all") {

      readr::write_csv(dt, paste0())

    } else if (dataset_type == "only2020") {

      readr::write_csv(dt, paste0())

    } else if (dataset_type == "only2021") {

      readr::write_csv(dt, paste0())

    }  else if (dataset_type == "custom") {

      readr::write_csv(dt, paste0())

    }

  }

}

# if both custom datasets exist then combine into one
custom2020_exists <- file.exists(paste0())
custom2021_exists <- file.exists(paste0())
if(custom2020_exists &  custom2021_exists){
  custom2020 <- fread(paste0())
  custom2021 <- fread(paste0())
  custom2021 <- custom2021[!ihme_loc_id %like% "IND"]
  custom <- rbind(custom2020, custom2021)
  readr::write_csv(custom, paste0())
}

if (GBD) {

  if (idr_check_passed != TRUE) stop ("IDR check was not performed.")

}
