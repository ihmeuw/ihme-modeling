# Meta -------------------------------------------------------------------------

# Description: Compute draw level estimates of covariates

# Load libraries ---------------------------------------------------------------

library(arrow)
library(data.table)

# Helper -----------------------------------------------------------------------

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

set.seed(777)
num_draws <- 100

data_run_id <- ""

date <- format(Sys.time(), "%Y-%m-%d-%H-%M")

GBD <- T

if (GBD) {

  dataset_types <- c("fit", "all", "only2020", "only2021") 

} else {
  dataset_types <- c("all", "custom") 
}

covid_years <- c(2020, 2021)
gbd_year = 2020

main_dir <- paste0()

out_dir <- paste0()
dir.create(out_dir)

data_code_dir <- ""

source(paste0("<FILEPATH>/add_weeks_months.R"))
source(paste0("<FILEPATH>/em_data_validation.R"))
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
nor_map <- fread()

# Read in input files ----------------------------------------------------------

dt <- setDT(readRDS(""))
setnames(dt, "year_id", "year_start")
dt[, ':='(time_unit = "year", time_start = year_start,
          deaths_observed = NULL, deaths_expected = NULL)]

india_raw_file_dir <- ""
if(GBD) {
  ind_custom_orig <- fread(paste0())
} else {
  ind_custom_orig <- fread(paste0())
}

if(GBD){

  cov_dt_id <- ""

} else {
  cov_dt_id <- ""
}

pop <- fread("")
pop[time_unit == "day", origin := as.Date(paste0(year_start, "-01-01"), tz = "UTC") - lubridate::days(1)]
pop[time_unit == "day", date := as.Date(time_start, origin = origin, tz = "UTC")]
pop[, origin := NULL]

pop_2021 <- fread("")

if (GBD) {

  idr <- fread("")
  mobility <- fread("")
  sero <- fread("")

  daily_covid <- fread("")
  start_dt <- fread( "")

} else {

  idr <- setDT(read_parquet(""))
  mobility <- fread("")
  sero_dir <- ""
  sero <- fread(fs::path())

  daily_covid <- fread("")
  start_dt <- fread("")
}

# location ids for small islands with pandemic start dates in 2021
late_pan_islands <- c(23,25,29,380)

# Pull estimates from database -------------------------------------------------

gbd_2019_pop <- demInternal::get_dem_outputs(
  process_name = "population estimate",
  gbd_year = 2019,
  run_id = "best",
  year_ids = 1990:2019,
  age_group_id = 22,
  sex_id = 3
)

gbd_2019_pop_2019 <- gbd_2019_pop[year_id == 2019]

gbd_2019_death_number <- demInternal::get_dem_outputs(
  process_name = "no shock death number estimate",
  gbd_year = 2019,
  run_id = "best",
  year_ids = 1990:2019,
  sex_ids = 3,
  age_group_ids = 22,
  estimate_stage_ids = 5,
  name_cols = TRUE
)

# Population yearly for excess -------------------------------------------------

pop_year <- merge(
  pop,
  start_dt,
  by = "location_id",
  all.x = T
)
pop_year <- pop_year[(time_unit == "day") & (date >= start_date) & (date <= as.IDate("2021-12-31")), 
                     .(person_years = sum(person_years)),
                     by = c("ihme_loc_id","year_start","location_id","sex","age_name",
                            "type")]
pop_year[, ':=' (time_unit = "year", time_start = year_start)]
pop <- rbind(pop, pop_year, fill = TRUE) #fills date

# Get external em data ---------------------------------------------------------

ext_dir <- fs::path()
ext <- assertable::import_files(
  filenames = list.files(ext_dir, pattern = ""),
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
ext_prepped <- ext_prepped[rep(seq_len(.N), num_draws)]
ext_prepped <- ext_prepped[order(location_id,year_start,time_start)]

replicates <- nrow(unique(ext_prepped[,c("location_id","year_start","time_start")]))

ext_prepped[, draw := rep(1:num_draws, replicates)]

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


# Prep gbd covariate draws -----------------------------------------------------

gbd_cov_folder <- ""
gbd_cov_files <- list.files(gbd_cov_folder, full.names = TRUE, pattern = "")

gbd_cov_files <- gbd_cov_files[!(gbd_cov_files %like% "hypertension|gbd_inpatient_admis|gbd_diabetes")]

gbd_cov_names <- gsub("", "", gbd_cov_files)
gbd_locs <- fread(gbd_cov_files[1])[, "location_id"]

rep_gbd_locs <- purrr::map_dfr(seq_len(num_draws), function(x) gbd_locs)

draw_level_covs <- data.table(location_id = rep_gbd_locs[order(location_id), location_id],
                              draw = 1:num_draws)

for (i in 1:length(gbd_cov_files)) {

  full_file_name = gbd_cov_files[i]
  file_name = gbd_cov_names[i]

  message(paste0("Working on ", file_name))

  cov <- fread(full_file_name)

  # get standard deviation
  cov[, std_dev := min(upper_value - mean_value, mean_value - lower_value) / 1.96]

  # sample a normal distribution using those mean and standard deviation 100x
  draw_level <- data.table()

  for (loc in unique(cov$location_id)) {

    mean = cov[location_id == loc, mean_value]
    std_dev = cov[location_id == loc, std_dev]
    draws <- data.table(location_id = loc,
                        draw = 1:num_draws,
                        val = rnorm(num_draws, mean = mean, sd = std_dev)
    )
    draw_level <- rbind(draw_level,draws)
  }

  setnames(draw_level, "val", file_name)

  draw_level_covs <- merge(
    draw_level_covs,
    draw_level,
    by = c("location_id", "draw"),
    all = TRUE
  )

}

# create ITA_99999 from ITA_35498 and ITA_35499
ita_combine <- copy(draw_level_covs)
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
                               HAQI = mean(HAQI),
                               hemog_sickle_death_rate = mean(hemog_sickle_death_rate),
                               hemog_thalass_death_rate = mean(hemog_thalass_death_rate),
                               hiv_death_rate = mean(hiv_death_rate),
                               avg_abs_latitude = mean(avg_abs_latitude),
                               ncd_death_rate = mean(ncd_death_rate),
                               neo_death_rate = mean(neo_death_rate),
                               neuro_death_rate = mean(neuro_death_rate),
                               resp_asthma_death_rate = mean(resp_asthma_death_rate),
                               resp_copd_death_rate = mean(resp_copd_death_rate),
                               smoking_prevalence = mean(smoking_prevalence),
                               subs_death_rate = mean(subs_death_rate),
                               universal_health_coverage = mean(universal_health_coverage)
), by = c("location_id","draw")]
draw_level_covs <- rbind(draw_level_covs, ita_combine)

# update Norway subnationals for 2019 covs
setnames(gbd_2019_pop_2019, "mean", "person_years")
nor_subs <- draw_level_covs[location_id %in% nor_locs_old$location_id]
nor_subs <- merge(
  nor_subs,
  gbd_2019_pop_2019[, c("location_id","person_years")],
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
                                 gbd_obesity = weighted.mean(gbd_obesity, person_years),
                                 HAQI = weighted.mean(HAQI, person_years),
                                 hemog_sickle_death_rate = weighted.mean(hemog_sickle_death_rate, person_years),
                                 hemog_thalass_death_rate = weighted.mean(hemog_thalass_death_rate),
                                 hiv_death_rate = weighted.mean(hiv_death_rate, person_years),
                                 ncd_death_rate = weighted.mean(ncd_death_rate, person_years),
                                 neo_death_rate = weighted.mean(neo_death_rate, person_years),
                                 neuro_death_rate = weighted.mean(neuro_death_rate, person_years),
                                 resp_asthma_death_rate = weighted.mean(resp_asthma_death_rate, person_years),
                                 resp_copd_death_rate = weighted.mean(resp_copd_death_rate, person_years),
                                 smoking_prevalence = weighted.mean(smoking_prevalence, person_years),
                                 subs_death_rate = weighted.mean(subs_death_rate, person_years),
                                 universal_health_coverage = weighted.mean(universal_health_coverage, person_years)
), by = c("location_id","draw")]
draw_level_covs <- draw_level_covs[!location_id %in% nor_subs_new$location_id]
draw_level_covs <- rbind(draw_level_covs, nor_subs_new)

# Prep and add demographics covariates with draws-------------------------------

# crude death rate
dt_crude_death_rate <- gbd_2019_death_number[
  gbd_2019_pop,
  .(location_id, ihme_loc_id, location_name, year_id,
    mean_value = mean / i.mean, lower_value = lower / i.mean, upper_value = upper / i.mean),
  on = .(location_id, year_id),
  nomatch = NULL
  ]

dt_crude_death_rate_2019 <- dt_crude_death_rate[year_id == 2019, -"year_id"]

dt_crude_death_rate_2019[, std_dev := min(upper_value - mean_value, mean_value - lower_value) / 1.96]

# sample a normal distribution using those mean and standard deviation
draw_level <- data.table()

for (loc in unique(dt_crude_death_rate_2019$location_id)) {

  mean <- dt_crude_death_rate_2019[location_id == loc, mean_value]
  std_dev <- dt_crude_death_rate_2019[location_id == loc, std_dev]

  draws <- data.table(location_id = loc,
                      draw = 1:num_draws,
                      val = rnorm(num_draws, mean = mean, sd = std_dev)
  )

  draw_level <- rbind(draw_level, draws)

}

setnames(draw_level, "val", "crude_death_rate")

draw_level_covs <- merge(
  draw_level_covs,
  draw_level,
  by = c("location_id", "draw"),
  all = TRUE
)

# standard deviation of crude death rate over time
dt_crude_death_rate_sd_1990 <- dt_crude_death_rate[
  year_id >= 1990,
  .(crude_death_rate_sd_1990 = sd(mean_value)),
  by = .(location_id, ihme_loc_id, location_name)
  ]

dt_crude_death_rate_sd_2000 <- dt_crude_death_rate[
  year_id >= 2000,
  .(crude_death_rate_sd_2000 = sd(mean_value)),
  by = .(location_id, ihme_loc_id, location_name)
  ]

draw_level_covs <- merge(
  draw_level_covs,
  dt_crude_death_rate_sd_1990[, !c("ihme_loc_id", "location_name")],
  by = "location_id",
  all = TRUE
)

draw_level_covs <- merge(
  draw_level_covs,
  dt_crude_death_rate_sd_2000[, !c("ihme_loc_id", "location_name")],
  by = "location_id",
  all = TRUE
)

# Prep IDR ---------------------------------------------------------------------

# format wide draws
idr <- melt(idr, measure.vars = patterns("draw"), variable.name = "draw")
idr[, draw := as.numeric(gsub("draw_", "", draw))]
setnames(idr, "value", "idr_reference")
idr[, draw := draw + 1]
idr$date <- as.IDate(idr$date, format = "%Y-%m-%d")

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
idr <- idr[, .(idr_reference = mean(idr_reference)), by = c("location_id", "date", "draw")]

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

# make sure that ETH_ uses national covid incidence
idr <- idr[order(location_id, draw, date)]
eth_loc_ids <- locs[ihme_loc_id %like% "ETH_", location_id]
idr_eth_nat <- idr[location_id == 179]

setnames(idr_eth_nat, "covid_incidence", "covid_incidence_nat")

idr_eth_sub <- idr[location_id %in% eth_loc_ids]
idr_eth_sub <- merge(
  idr_eth_sub,
  idr_eth_nat[, c("covid_incidence_nat", "date", "draw")],
  by = c("date", "draw"),
  all.x = TRUE
)

idr_eth_sub[, covid_incidence := covid_incidence_nat]
idr_eth_sub <- idr_eth_sub[, -c("covid_incidence_nat")]
idr <- rbind(idr[!location_id %in% eth_loc_ids], idr_eth_sub)

# fill locations with 0.01 incidence
idr[is.na(covid_incidence), covid_incidence := 0.01]

# lag idr
idr <- idr[order(location_id, draw, date)]
idr[, idr_lagged := shift(idr_reference, 19), by = c("location_id","draw")]
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

# create version of mobility before loop
mobility_initial <- copy(mobility)

# Prep seroprevalence ----------------------------------------------------------

# make from wide to long
sero <- melt(
  sero,
  measure.vars = patterns("draw"),
  variable.name = "draw"
)

# format date and draws
sero[, date := as.Date(date)]
sero[, draw := as.numeric(gsub("draw_", "", draw)) + 1]

# Find first and last day of sero data across all draws
sero_keep <- sero[!is.na(value), .(min_date = min(date), max_date = max(date)), by = c("location_id", "draw")][, .(keep_date_start = max(min_date), keep_date_end = min(max_date)), by = "location_id"]

# merge on various start and end times
sero <- merge(
  sero,
  sero_keep,
  by = "location_id",
  all.x = T
)

# subset to only locations in the infections time period
sero <- sero[date >= keep_date_start & date <= keep_date_end]

# reformat
setnames(sero, "value", "daily_infections")

# aggregate USA and WA
assertthat::assert_that(!102 %in% unique(sero$location_id))
sero_usa_agg <- sero[location_id %in% usa_loc_ids]
sero_usa_agg[, location_id := 102]
sero_wa_agg <- sero[location_id %in% wa_state_loc_ids]
sero_wa_agg[, location_id := 570]
sero <- rbindlist(list(sero, sero_usa_agg, sero_wa_agg), use.names = TRUE)

sero <- merge(
  sero,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = T
)

# aggregate admin2 to admin1
sero_sub_locs_1 <- sort(locs[(ihme_loc_id %like% "KEN" & level == 4) | (ihme_loc_id %like% "GBR" & level == 5)]$ihme_loc_id)
sero_sub_locs_1 <- sero_sub_locs_1[sero_sub_locs_1 != "IND_60896"]
sero_subs <- rbindlist(lapply(
  sero_sub_locs_1,
  change_subnat_to_nat,
  input_file = sero
))
sero <- rbind(sero, sero_subs)

# aggregate admin1 to national
sero_sub_locs_2 <- sort(unique(substr(sero[ihme_loc_id %like% "_"]$ihme_loc_id, 1, 3)))
sero_sub_locs_2 <- sero_sub_locs_2[!(sero_sub_locs_2 %in% sero$ihme_loc_id)]
sero_nats <- rbindlist(lapply(
  sero_sub_locs_2,
  change_subnat_to_nat,
  input_file = sero
))
sero <- rbind(sero, sero_nats)

# aggregate by new locations
sero <- sero[, .(daily_infections = sum(daily_infections)), by = c("location_id", "date", "draw")]

# add ihme_loc_id
sero <- merge(
  locs[, c("location_id", "ihme_loc_id")],
  sero,
  by = "location_id",
  all.y = TRUE)
sero$date <- as.IDate(sero$date)

# lag seroprevalence by 25 days
sero <- sero[order(location_id, draw, date)]
sero[, infections_lagged := shift(daily_infections, 25), by =  c("location_id", "draw")]
sero[is.na(infections_lagged), infections_lagged := 0]

# create version of sero before loop
sero_initial <- copy(sero)


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

# get custom india time periods
ind <- fread(india_raw_file_dir)
ind <- ind[!is.na(excess_deaths)]

ind <- ind[!(ihme_loc_id == "IND_4870" & year_start == 2021)]
ind <- ind[!(ihme_loc_id == "IND_4857" & year_start == 2021)]

ind[, ':=' (date_start = as.Date(paste0(month_start, '-', day_start,'-', year_start), format = "%m-%d-%Y"),
            date_end = as.Date(paste0(month_end, '-', day_end, '-', year_end), format = "%m-%d-%Y"))]

ind <- merge(
  locs[, c("ihme_loc_id","location_id")],
  ind,
  by = "ihme_loc_id",
  all.y = TRUE
)

keep_ind_sep <- unique(ind[!is.na(excess_deaths), c("location_id", "date_start", "date_end")])

keep_ind <- data.table()

for (loc_time in 1:nrow(keep_ind_sep)) {
  
  temp <- keep_ind_sep[loc_time]
  temp <- as.data.table(melt(
    temp,
    id.vars = "location_id",
    value.name = "date"
  ))
  temp <- temp[, -c("variable")]
  temp[, date := as.IDate(temp$date)]
  temp <- fill_missing_dates(temp)
  temp[, ':=' (month_start = month(date),
               year_start = year(date),
               time_unit = "month",
               ihme_loc_id = paste0("IND_",location_id),
               keep_ind = 1)]
  temp <- unique(temp[, c("location_id","month_start","year_start","keep_ind")])
  keep_ind <- rbind(keep_ind, temp)
  
}


# Prep covariates with dataset specifics ---------------------------------------

# create version of dt before loop
draw_level_covs_initial <- copy(draw_level_covs)

for (dataset_type in dataset_types) {

  message(paste0("Working on the ", toupper(dataset_type), " dataset"))

  draw_level_covs <- copy(draw_level_covs_initial)

  # Specify times to keep ------------------------------------------------------

  if (dataset_type %in% c("fit", "all")) {

    all_time_cutoff <- as.IDate("2021-12-31")

  } else if (dataset_type == "only2020") {

    all_time_cutoff <- as.IDate("2020-12-31")

  } else if (dataset_type == "only2021") {

    all_time_cutoff <- as.IDate("2021-12-31")

  }

  # Prep and add death rate excess ---------------------------------------------

  # add person-years
  death_rate_excess <- merge(
    dt,
    pop[, c("location_id","year_start","time_start","time_unit","person_years")],
    by = c("location_id","year_start","time_start","time_unit"),
    all.x = TRUE
  )
  death_rate_excess[is.na(death_rate_excess), death_rate_excess := deaths_excess / person_years]

  if (dataset_type == "all") {

    death_rate_excess <- death_rate_excess[year_start %in% covid_years]
    
  } else if (dataset_type == "only2020") {

    death_rate_excess <- death_rate_excess[year_start == 2020]

  } else if (dataset_type == "only2021") {

    death_rate_excess <- death_rate_excess[year_start == 2021]

  }  else if (dataset_type == "fit") {
    
    death_rate_excess <- death_rate_excess[year_start %in% covid_years]
    
  } else if (dataset_type %in% c("custom", "custom2020", "custom2021")) {

    death_rate_excess <- merge(
      death_rate_excess,
      keep_times,
      by = c("location_id","year_start","time_start","time_unit"),
      all.x = TRUE
    )
    death_rate_excess <- death_rate_excess[(keep == 1)]
    death_rate_excess <- death_rate_excess[, -c("keep")]

  }
  
  # aggregate values and add india
  death_rate_excess <- death_rate_excess[, .(death_rate_excess = sum(death_rate_excess * person_years, na.rm = T) / sum(person_years),
                                           death_rate_person_years = sum(person_years)),
                                         by = c("location_id", "draw")]

  # add india
  ind_custom <- copy(ind_custom_orig)

  ind_custom <- ind_custom[, c("location_id", "death_rate_excess","death_rate_person_years")]
  ind_custom <- ind_custom[rep(seq_len(.N), num_draws)]
  ind_custom <- ind_custom[order(location_id)]

  replicates <- nrow(unique(ind_custom[, c("location_id")]))

  ind_custom[, draw := rep(1:num_draws, replicates)]

  # combine
  death_rate_excess <- rbind(death_rate_excess, ind_custom)

  # merge
  draw_level_covs <- merge(
    draw_level_covs,
    death_rate_excess,
    by = c("location_id","draw"),
    all = TRUE
  )

  # Finalize and add IDR -------------------------------------------------------
  message("Starting IDR")

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

    idr <- idr[date >= as.Date("2021-01-01") & date <= as.Date("2021-12-31")]

  } else if (dataset_type == "fit") {
    
    idr[, year_start := lubridate::year(date)]
    idr[, month_start := lubridate::month(date)]
    idr <- merge(
      idr,
      keep_years,
      by = c("location_id","year_start"),
      all.x = TRUE
    )
    idr <- merge(
      idr,
      keep_ind,
      by = c("location_id", "year_start", "month_start"),
      all.x = TRUE
    )
    idr[!is.na(keep_ind), keep := keep_ind]
    idr[, keep_ind := NULL]
    idr <- idr[(keep == 1) & (date >= start_date) & (date <= all_time_cutoff)]
    
  } else if (dataset_type %in% c("custom", "custom2020", "custom2021")) {

    # get week/year from date based on country week type
    idr <- add_weeks_months(dt = idr, gbd_year = gbd_year, data_code_dir = data_code_dir)

    # outlier by week and month
    idr <- merge(
      idr,
      keep_times,
      by = c("location_id","year_start","time_start","time_unit"),
      all.x = TRUE
    )
    idr <- idr[(keep == 1)]

  }

  # do not weight TZA since daily cases/incidence mostly 0
  idr_tza <- idr[location_id == 189,
                 list(idr_reference = mean(idr_reference, na.rm = T),
                      idr_lagged = mean(idr_lagged, na.rm = T),
                      confirmed_cases = sum(daily_cases, na.rm = T),
                      covid_incidence = sum(covid_incidence, na.rm = T)),
                 by = c("location_id", "draw")
                 ]

  idr <- idr[location_id != 189,
             list(idr_reference = weighted.mean(idr_reference, covid_incidence, na.rm = T),
                  idr_lagged = weighted.mean(idr_lagged, covid_incidence, na.rm = T),
                  confirmed_cases = sum(daily_cases, na.rm = TRUE),
                  covid_incidence = sum(covid_incidence, na.rm = TRUE)),
             by = c("location_id", "draw")
             ]
  idr <- rbind(idr, idr_tza)

  if (num_draws == 1000) {

    # duplicate 10 times to get 1000 draws
    reps <- length(unique(idr$location_id))

    for (num in seq(101, 901, 100)) {

      idr_temp <- copy(idr[draw %in% 1:100])
      idr_temp[, draw := rep(seq(num, num + 99, 1), reps)]
      idr <- rbind(idr, idr_temp)

    }

  }

  # merge
  draw_level_covs <- merge(
    draw_level_covs,
    idr[, c("location_id","draw","idr_reference","idr_lagged")],
    by = c("location_id","draw"),
    all = TRUE
  )

  # Finalize and add mobility --------------------------------------------------
  message("Starting mobility")

  mobility <- copy(mobility_initial)

  mobility <- merge(
    mobility,
    start_dt,
    by = "location_id",
    all.x = T
  )

  if (dataset_type == "all") {

    mobility <- mobility[(date >= start_date) & (date <= all_time_cutoff)]

  } else if (dataset_type == "only2020") {

    mobility <- mobility[(date >= start_date) & (date <= as.Date("2020-12-31"))]

  } else if (dataset_type == "only2021") {

    mobility <- mobility[date >= as.Date("2021-01-01") & date <= as.Date("2021-12-31")]

  } else if (dataset_type == "fit") {
    
    mobility[, year_start := lubridate::year(date)]
    mobility[, month_start := lubridate::month(date)]
    # outlier by week and month
    mobility <- merge(
      mobility,
      keep_years,
      by = c("location_id","year_start"),
      all.x = TRUE
    )
    mobility <- merge(
      mobility,
      keep_ind,
      by = c("location_id", "year_start", "month_start"),
      all.x = TRUE
    )
    mobility[!is.na(keep_ind), keep := keep_ind]
    mobility[, keep_ind := NULL]
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

  # lag mobility
  mobility <- mobility[order(date)]
  mobility[, mobility_lagged := shift(mobility_reference, 19), by = "location_id"]
  mobility[is.na(mobility_lagged), mobility_lagged := 0]

  # aggregate
  mobility <- mobility[, .(mobility_reference = mean(mobility_reference),
                           mobility_lagged = mean(mobility_lagged)),
                       by = "location_id"]

  # replicate by 100 so covid-only locs have the correct number of draws
  mobility <- mobility[rep(seq_len(.N), num_draws)]
  mobility <- mobility[order(location_id)]
  mobility[, draw := rep(1:num_draws, length(unique(mobility[, location_id])))]

  # merge
  draw_level_covs <- merge(
    draw_level_covs,
    mobility[, c("location_id","draw","mobility_reference","mobility_lagged")],
    by = c("location_id","draw"),
    all = TRUE
  )


  # Prep person-years ----------------------------------------------------------
  message("Starting person years")
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
    
    pop_sub[, month_start := lubridate::month(date)]
    pop_sub <- merge(
      pop_sub,
      keep_years,
      by = c("location_id", "year_start"),
      all.x = TRUE
    )
    pop_sub <- merge(
      pop_sub,
      keep_ind,
      by = c("location_id", "year_start", "month_start"),
      all.x = TRUE
    )
    pop_sub[!is.na(keep_ind), keep := keep_ind]
    pop_sub[, keep_ind := NULL]
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
  pop_sub$date <- as.IDate(pop_sub$date)

  if (dataset_type == "only2021"){
    pop_agg <- pop_2021[, c("location_id","person_years")]
  } else {
    # aggregate
    pop_agg <- pop_sub[, .(person_years = sum(person_years)), by = "location_id"]
  }

  draw_level_covs <- merge(
    draw_level_covs,
    pop_agg,
    by = "location_id",
    all.x = TRUE
  )

  
  # Finalize and add seroprevalence --------------------------------------------
  message("Starting seroprevalence")
  sero <- copy(sero_initial)
  
  sero <- merge(
    sero,
    start_dt,
    by = "location_id",
    all.x = T
  )
  
  sero <- merge(
    sero,
    sero_keep,
    by = "location_id",
    all.x = TRUE
  )
  
  if (dataset_type == "all") {
    
    sero <- sero[(date >= start_date) & (date <= all_time_cutoff)]
    
  } else if (dataset_type == "only2020") {
    
    sero <- sero[(date >= start_date) & (date <= as.Date("2020-12-31"))]
    
  } else if (dataset_type == "only2021") {
    
    sero <- sero[date >= as.Date("2021-01-01") & date <= as.Date("2021-12-31")]
    
  } else if (dataset_type == "fit") {
    
    sero[, year_start := lubridate::year(date)]
    sero[, month_start := lubridate::month(date)]
    sero <- merge(
      sero,
      keep_years,
      by = c("location_id","year_start"),
      all.x = TRUE
    )
    sero <- merge(
      sero,
      keep_ind,
      by = c("location_id", "year_start", "month_start"),
      all.x = TRUE
    )
    sero[!is.na(keep_ind), keep := keep_ind]
    sero[, keep_ind := NULL]
    sero <- sero[(keep == 1) & (date >= start_date) & (date <= all_time_cutoff)]
    
  } else if (dataset_type %in% c("custom", "custom2020", "custom2021")) {
    # add time unit
    sero <- add_weeks_months(dt = sero, gbd_year = gbd_year, data_code_dir = data_code_dir)
    
    # outlier by week and month
    sero <- merge(
      sero,
      keep_times,
      by = c("location_id","year_start","time_start","time_unit"),
      all.x = TRUE
    )
    sero <- sero[(keep == 1)]
    
  }
  
  # match population based on time unit or date
  if (dataset_type %in% c("custom", "custom2020", "custom2021")) {
    
    # aggregate to week to match with population
    sero <- sero[, .(daily_infections = sum(daily_infections, na.rm = TRUE),
                     daily_infections_lagged = sum(infections_lagged, na.rm = TRUE)),
                 by = c("location_id","draw","year_start","time_start","time_unit")]
    
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
                 by = c("location_id","date","draw")]
    
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
                   person_years = sum(person_years, na.rm = TRUE)), by = c("location_id", "draw")]
  
  # convert to rate
  sero[, cumulative_infections := cumulative_infections / person_years]
  sero[, cumulative_infections_lagged := cumulative_infections_lagged / person_years]
  
  sero[is.infinite(cumulative_infections), cumulative_infections := NA]
  sero[is.infinite(cumulative_infections_lagged), cumulative_infections_lagged := NA]
  
  # duplicate 10 times to get 1000 draws if needed
  if (num_draws == 1000) {
    
    reps <- length(unique(sero$location_id))
    
    for(num in seq(101, 901, 100)) {
      
      sero_temp <- copy(sero[draw %in% 1:100])
      sero_temp[, draw := rep(seq(num, num + 99, 1), reps)]
      sero <- rbind(sero, sero_temp)
      
    }
    
  }
  
  assertable::assert_values(sero, c("cumulative_infections","cumulative_infections_lagged"))
  setnames(sero, "person_years", "ci_person_years")
  
  # merge
  draw_level_covs <- merge(
    draw_level_covs,
    sero[, c("location_id","draw","cumulative_infections","cumulative_infections_lagged","ci_person_years")],
    by = c("location_id","draw"),
    all = TRUE
  )
  

  # Prep and add demographics covariates without draws -------------------------
  message("Starting demo covs")
  if (dataset_type == "custom") {

    dt_em <- fread(paste0())

  } else {

    dt_em <- fread(paste0())

  }

  dt_em <- dt_em[!location_id == 999]

  draw_level_covs <- merge(
    draw_level_covs,
    dt_em[, c("location_id","death_rate_covid","deaths_covid","covid_person_years","stars",
              "hypertension_prevalence","gbd_diabetes","gbd_inpatient_admis",
              "mean_pop_age","prop_pop_60plus","prop_pop_70plus",
              "prop_pop_75plus","prop_pop_80plus","prop_pop_85plus")],
    by = "location_id",
    all = TRUE
  )

  # Final dataset tweaks -------------------------------------------------------
  message("Starting final dataset tweaks")
  
  ## drop locations without data for fit dataset 
  if(dataset_type == "fit"){
    draw_level_covs <- draw_level_covs[!is.na(death_rate_excess)]
  }
  
  ## combine ITA 35498 & 35499 to make ITA 99999 ##
  dt_ita <- draw_level_covs[location_id %in% c(35498, 35499)]
  
  dt_ita[, ':=' (location_id = 99999,
                 ihme_loc_id = "ITA_99999",
                 location_name = "Provincia autonoma di Bolzano + Provincia autonoma di Trento")]
  
  dt_ita <- dt_ita[, .(crude_death_rate = mean(crude_death_rate),
                       crude_death_rate_sd_1990 = mean(crude_death_rate_sd_1990),
                       crude_death_rate_sd_2000 = mean(crude_death_rate_sd_2000),
                       mobility_reference = mean(mobility_reference),
                       mobility_lagged = mean(mobility_lagged),
                       idr_reference = mean(idr_reference),
                       idr_lagged = mean(idr_lagged),
                       cumulative_infections = mean(cumulative_infections),
                       cumulative_infections_lagged = mean(cumulative_infections_lagged),
                       ci_person_years = sum(ci_person_years)),
                   by = c("location_id","draw","ihme_loc_id")
  ]
  
  draw_level_covs[location_id == 99999, ':=' (
    crude_death_rate = dt_ita[, crude_death_rate],
    crude_death_rate_sd_1990 = dt_ita[, crude_death_rate_sd_1990],
    crude_death_rate_sd_2000 = dt_ita[, crude_death_rate_sd_2000],
    mobility_reference = dt_ita[, mobility_reference],
    mobility_lagged = dt_ita[, mobility_lagged],
    idr_reference = dt_ita[, idr_reference],
    idr_lagged = dt_ita[, idr_lagged],
    cumulative_infections = dt_ita[, cumulative_infections],
    cumulative_infections_lagged = dt_ita[, cumulative_infections_lagged],
    ci_person_years = dt_ita[, ci_person_years]
  )]
  
  ## fill subnats in only covid hierarchy with higher admin level for datasets except "custom" ##
  draw_level_covs <- merge(
    draw_level_covs,
    locs[, c("location_id","ihme_loc_id")],
    by = "location_id",
    all.x = TRUE
  )
  
  # fake draws here since there is no data except covid for these locations
  if (dataset_type %in% c("custom", "custom2020", "custom2021", "fit")) {
    
    temp <- draw_level_covs[location_id %in% c(deu_loc_ids,wa_state_loc_ids,can_loc_ids,esp_loc_ids)]
    
    temp <- temp[rep(seq_len(.N), num_draws)]
    temp <- temp[order(location_id)]
    
    temp[, draw := rep(1:num_draws, 51)]
    
    draw_level_covs <- draw_level_covs[!location_id %in% c(deu_loc_ids,wa_state_loc_ids,can_loc_ids,esp_loc_ids)]
    
    draw_level_covs <- rbind(draw_level_covs,temp)
    
  }
  
  for (national_ihme_loc in c("DEU","USA_570","CAN","ESP")) {
    
    print(national_ihme_loc)
    
    national <- draw_level_covs[ihme_loc_id == national_ihme_loc]
    
    if (national_ihme_loc == "USA_570"){
      national_ihme_loc <- "wa_state"
    }
    
    num_subnats <- length(get(paste0(tolower(national_ihme_loc), "_loc_ids")))
    
    draw_level_covs <- draw_level_covs[order(location_id, draw)]
    
    draw_level_covs[location_id %in% get(paste0(tolower(national_ihme_loc), "_loc_ids")),
                    ':=' (crude_death_rate = rep(national$crude_death_rate, num_subnats),
                          crude_death_rate_sd_1990 = rep(national$crude_death_rate_sd_1990, num_subnats),
                          crude_death_rate_sd_2000 = rep(national$crude_death_rate_sd_2000, num_subnats),
                          gbd_inpatient_admis = rep(national$gbd_inpatient_admis, num_subnats),
                          gbd_diabetes = rep(national$gbd_diabetes, num_subnats),
                          avg_abs_latitude = rep(national$avg_abs_latitude, num_subnats),
                          cirrhosis_death_rate = rep(national$cirrhosis_death_rate, num_subnats),
                          ckd_death_rate = rep(national$ckd_death_rate, num_subnats),
                          cong_downs_death_rate = rep(national$cong_downs_death_rate, num_subnats),
                          cvd_death_rate = rep(national$cvd_death_rate, num_subnats),
                          cvd_pah_death_rate = rep(national$cvd_pah_death_rate, num_subnats),
                          cvd_stroke_cerhem_death_rate = rep(national$cvd_stroke_cerhem_death_rate, num_subnats),
                          diabetes_death_rate = rep(national$diabetes_death_rate, num_subnats),
                          endo_death_rate = rep(national$endo_death_rate, num_subnats),
                          gbd_obesity = rep(national$gbd_obesity, num_subnats),
                          HAQI = rep(national$HAQI, num_subnats),
                          hemog_sickle_death_rate = rep(national$hemog_sickle_death_rate, num_subnats),
                          hemog_thalass_death_rate = rep(national$hemog_thalass_death_rate, num_subnats),
                          hiv_death_rate = rep(national$hiv_death_rate, num_subnats),
                          ncd_death_rate = rep(national$ncd_death_rate, num_subnats),
                          neo_death_rate = rep(national$neo_death_rate, num_subnats),
                          neuro_death_rate = rep(national$neuro_death_rate, num_subnats),
                          resp_asthma_death_rate = rep(national$resp_asthma_death_rate, num_subnats),
                          resp_copd_death_rate = rep(national$resp_copd_death_rate, num_subnats),
                          smoking_prevalence = rep(national$smoking_prevalence, num_subnats),
                          subs_death_rate = rep(national$subs_death_rate, num_subnats),
                          universal_health_coverage = rep(national$universal_health_coverage, num_subnats))]
  }
  
  draw_level_covs <- draw_level_covs[, -c("ihme_loc_id")]
  
  # Remove china mainland and china w/o macao and hong kong
  draw_level_covs <- draw_level_covs[!location_id %in% c(6, 44533)]
  
  # make sure 0s are small positive values for CI since it is logged
  draw_level_covs[cumulative_infections == 0, cumulative_infections := .00001]
  draw_level_covs[cumulative_infections_lagged == 0, cumulative_infections_lagged := .00001]
  draw_level_covs[is.na(cumulative_infections) & location_id %in% locs[level>2, location_id], cumulative_infections := .00001]
  draw_level_covs[is.na(cumulative_infections_lagged) & location_id %in% locs[level>2, location_id], cumulative_infections_lagged := .00001]
  
  # give 0 idr in 2020 where values are missing
  draw_level_covs[location_id %in% late_pan_islands & is.na(idr_reference), idr_reference := 0]
  draw_level_covs[location_id %in% late_pan_islands & is.na(idr_lagged), idr_lagged := 0]
  
  ## subset to specific locations if custom ##
  if (dataset_type %in% c("custom", "custom2020", "custom2021", "fit")) {

    keep_years <- keep_years[!location_id %in% c(35498,35499)]

    # subset to only locations needed
    draw_level_covs <- draw_level_covs[location_id %in% unique(keep_years$location_id)]

  }
  
  # Use median IND covariate values for 4857, 4850, 4853, & 4869
  ind_state_replace <- c(4857, 4850, 4853, 4869)
  dt_ind <- draw_level_covs[location_id %in% locs[ihme_loc_id %like% "IND" & level > 3, location_id]]
  dt_ind[, group := "admin1"]
  dt_ind[location_id %in% locs[parent_id %in% ind_state_replace &
                                 location_name %like% "Urban", location_id], group := "urban"]
  dt_ind[location_id %in% locs[parent_id %in% ind_state_replace & 
                                 location_name %like% "Rural", location_id], group := "rural"]
  median_covs <- setdiff(colnames(dt_ind), c("location_id", "ihme_loc_id", 
                                             "location_name", "person_years",
                                             "death_rate_excess", "death_rate_person_years", 
                                             "death_rate_covid", "deaths_covid", "covid_person_years",
                                             "avg_abs_latitude", "group"))
  for (col in median_covs){
    dt_ind[, get("col") := median(get(col), na.rm = T), by = c("group", "draw")]
  }
  dt_ind[, group := NULL]
  dt_ind <- dt_ind[location_id %in% locs[location_id %in% ind_state_replace | parent_id %in% ind_state_replace, location_id]]
  draw_level_covs <- draw_level_covs[!location_id %in% dt_ind[, location_id]]
  draw_level_covs <- rbind(draw_level_covs, dt_ind)

  # Run checks -----------------------------------------------------------------
  
  # break if 99999 not present
  if (!99999 %in% unique(dt$location_id)) {
    stop("Bring back ITA 99999")
  }
  
  # drop ken admin 4 subnats
  drop_locs <- c(gbd_loc_map[ihme_loc_id %like% "KEN_" & level == 4]$location_id)
  draw_level_covs <- draw_level_covs[!location_id %in% drop_locs]

  # drop global and region b/c missing from some gbd covs
  drop_locs <- gbd_loc_map[level <= 2]$location_id
  draw_level_covs <- draw_level_covs[!location_id %in% drop_locs]

  # check columns present
  data_cols <- c("location_id","draw","person_years","death_rate_person_years",
                 "death_rate_excess","death_rate_covid","deaths_covid","covid_person_years","stars",
                 "idr_reference","idr_lagged","mobility_reference","mobility_lagged",
                 "cumulative_infections","cumulative_infections_lagged","ci_person_years",
                 "crude_death_rate","crude_death_rate_sd_1990","crude_death_rate_sd_2000",
                 "mean_pop_age","prop_pop_60plus","prop_pop_70plus",
                 "prop_pop_75plus","prop_pop_80plus","prop_pop_85plus",
                 "gbd_inpatient_admis","gbd_diabetes","hypertension_prevalence",
                 gbd_cov_names)

  assertable::assert_colnames(draw_level_covs, data_cols, only_colnames = TRUE)

  assertable::assert_values(
    draw_level_covs[hemog_sickle_death_rate > 0 & !is.na(person_years) & !is.na(cumulative_infections)],
    setdiff(data_cols, c("death_rate_covid","death_rate_excess","stars","mobility_reference","mobility_lagged")),
    test = "gte",
    test_val = 0,
    warn_only = TRUE
  )

  assertable::assert_values(draw_level_covs[location_id %in% locs[level > 2, location_id]], c("cumulative_infections","cumulative_infections_lagged"), test = "gte", test_val = 0)

  # Save -------------------------------------------------------------------------

  if (GBD) {
    if (dataset_type == "all") {

      readr::write_csv(draw_level_covs, paste0())

    } else if (dataset_type == "only2020") {

      readr::write_csv(draw_level_covs, paste0())

    } else if (dataset_type == "only2021") {

      readr::write_csv(draw_level_covs, paste0())

    } else if (dataset_type == "fit") {
      
      readr::write_csv(draw_level_covs, paste0())
      
    } else if (dataset_type == "custom2020") {
      
      draw_level_covs[, year_start := 2020]
      readr::write_csv(draw_level_covs, paste0())
      
    } else if (dataset_type == "custom2021") {
      
      draw_level_covs[, year_start := 2021]
      readr::write_csv(draw_level_covs, paste0())
      
    }else if (dataset_type == "custom") {

      readr::write_csv(draw_level_covs, paste0())

    }
    print(paste0())

  } else {
    if (dataset_type == "all") {

      readr::write_csv(draw_level_covs, paste0())

    } else if (dataset_type == "only2020") {

      readr::write_csv(draw_level_covs, paste0())

    } else if (dataset_type == "only2021") {

      readr::write_csv(draw_level_covs, paste0())

    } else if (dataset_type == "custom") {

      readr::write_csv(draw_level_covs, paste0())

    }
    print(paste0())

  }
  pryr::mem_used()

}


# if both custom datasets exist then combine into one 
custom2020_exists <- file.exists(paste0())
custom2021_exists <- file.exists(paste0())
if(custom2020_exists &  custom2021_exists){
  custom2020 <- fread(paste0())
  custom2021 <- fread(paste0())
  custom <- rbind(custom2020, custom2021)
  readr::write_csv(custom, paste0())
}

