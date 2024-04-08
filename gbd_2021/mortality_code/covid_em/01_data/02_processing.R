
# Meta --------------------------------------------------------------------

# Description: Processes one location data for COVID-19 excess mortality model
# Steps:
#   1. Download config and all-cause and person_years inputs
#   2. Prep date for all-cause data
#   3. Aggregate all-cause data to get all-age both-sex
# Inputs:
#   * All-cause mortality data
#   * person_years
# Outputs:
#   * 


# Load libraries ----------------------------------------------------------

message(Sys.time(), " | Setup")

library(aweek)
library(argparse)
library(arrow)
library(assertable)
library(data.table)
library(dplyr)
library(demInternal)
library(demUtils)
library(fs)
library(hierarchyUtils)
library(lubridate)


# Command line arguments --------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", type = "character", required = !interactive(),
  help = "base directory for this version run"
)
parser$add_argument(
  "--loc_id", type = "character", required = !interactive(),
  help = "location ID to fit and predict model for"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)


# Setup -------------------------------------------------------------------

# get config
config <- config::get(
  file = paste0(),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# load in custom functions
source(paste0(<FILEPATH>/agg_month_proportional.R))
source(paste0(<FILEPATH>/interpolate_pop.R))
source(paste0(<FILEPATH>/em_data_validation.R))
source(paste0(<FILEPATH>/pop_to_person_years.R))

# location week types
location_week_types <- fread(fs::path())
locs_with_isoweek <- location_week_types[week_type == "isoweek", ihme_loc_id]
locs_with_epiweek <- location_week_types[week_type == "epiweek", ihme_loc_id]
locs_with_ukweek <- location_week_types[week_type == "ukweek", ihme_loc_id]


# Inputs ------------------------------------------------------------------

message(Sys.time(), " | Inputs")

# get mappings
#locations
process_locations <- fread(paste0())
process_locations <- process_locations[ihme_loc_id == "Mumbai", location_id := 999]
process_locations$location_id <- as.numeric(process_locations$location_id)
#sexes
process_sexes <- fread(paste0())
#ages
process_ages <- fread(paste0())
process_ages <- hierarchyUtils::gen_name(process_ages, col_stem = "age")

# get ihme_loc_id for current location
if(loc_id == "Mumbai"){
  ihme_loc <- "Mumbai"
  loc_id <- 4860
} else {
  ihme_loc <- process_locations[location_id == loc_id, ihme_loc_id]
}
loc_id <- as.numeric(loc_id)

# all-cause
process_allcause <- (ihme_loc %in% process_locations[(is_estimate_1), ihme_loc_id])
if (process_allcause) {
  allcause <- fread(paste0())
  allcause <- allcause[ihme_loc_id == ihme_loc]
  allcause[, location_id := loc_id]
  wmd <- fread(paste0())
  wmd <- wmd[location_id == loc_id]
}

# completeness estimates 
completeness <- fread(fs::path())
completeness <- completeness[location_id == loc_id]
total_completeness <- fread(fs::path())
total_completeness <- total_completeness[location_id == loc_id]

# all-cause from WMD only 
wmd_locs <- fread(paste0())
process_wmd <- (ihme_loc %in% wmd_locs[, ihme_loc_id])
if (process_wmd) {
  wmd <- fread(paste0())
  wmd <- wmd[location_id == loc_id]
}

# external mortality
process_em <- (ihme_loc %in% external_em_locs)
if(process_em){
  ext_em <- fread(paste0())
  ext_em <- ext_em[ihme_loc_id == ihme_loc]
  if (ihme_loc == "Mumbai") {
    ext_em[, location_id := loc_id]
  }
}

# person years
person_years <- fread(paste0())
person_years <- person_years[location_id == loc_id]

# covid
covid <- fread(paste0())
covid <- covid[location_id == loc_id]
covid <- covid[year_start %in% covid_years]

# idr cov
idr <- fread(paste0())
if (loc_id == 99999) {
  idr <- idr[location_id %in% c(35498, 35499)] # italy
  idr[, location_id := 99999]
  # make testing reference the mean
  idr <- idr[,
             list(idr = mean(idr)),
             by = c("location_id", "date")
             ]
}
if (ihme_loc %like% "ETH_") {
  idr_nat <- idr[location_id == 179]
}
idr <- idr[location_id == loc_id]

# using raw data #
if (ihme_loc %like% "ETH_") {
  daily_covid <- fread(paste0())
  daily_covid <- daily_covid[location_id == 179]
} else {
  daily_covid <- fread(paste0())
  daily_covid <- daily_covid[location_id == loc_id]
}

# seir covs
# testing cov
testing <- fread(paste0())
if (loc_id == 99999) {
  testing <- testing[location_id %in% c(35498, 35499)] # italy
  testing[, location_id := 99999]
  # make testing reference the mean
  testing <- testing[,
    list(mean = mean(mean)),
    by = c("location_id", "date", "observed")
  ]
}
testing <- testing[location_id == loc_id]

# mobility cov
mobility <- fread(paste0())
if (loc_id == 99999) {
  mobility <- mobility[location_id %in% c(35498, 35499)] # italy
  mobility[, location_id := 99999]
  # make testing reference the mean
  mobility <- mobility[,
    list(mean = mean(mean)),
    by = c("location_id", "date", "observed")
  ]
}
mobility <- mobility[location_id == loc_id]

# gbd inpatient admission
gbd_inpatient <- fread(paste0())
if (loc_id == 99999) {
  gbd_inpatient <- gbd_inpatient[location_id %in% c(35498, 35499)] # italy
  gbd_inpatient[, location_id := 99999]
  # make testing reference the mean
  gbd_inpatient <- gbd_inpatient[,
                           list(mean_value = mean(mean_value)),
                           by = c("location_id", "year_id","age_group_id","sex")
                           ]
}
gbd_inpatient <- gbd_inpatient[location_id == loc_id]

# vr star rating
stars <- fread(paste0())
if (loc_id == 99999) {
  stars <- stars[location_id %in% c(35498, 35499)] # italy
  stars[, location_id := 99999]
  # make testing reference the mean
  stars <- stars[,
                 list(stars = mean(stars)),
                 by = c("location_id")
                 ]
}
stars <- stars[location_id == loc_id]


# Standardize ----------------------------------------------------------------

if (process_allcause) {

  # SWE has "UNK" 'week_start' entries
  allcause[, week_start := as.integer(week_start)]
  allcause <- allcause[!(is.na(week_start) & is.na(month_start))]

  # USA state-level 2020 data is all-age both-sex
  if ((ihme_loc %like% "USA_" | ihme_loc == "PRI")) {
    allcause[, `:=` (age_start = 0, age_end = 125, sex = "all")]

  }

  # PRT HMD missing 95+ age group: combine w/ 90-95.
  if (ihme_loc == "PRT") {
    allcause[age_start >= 90, `:=` (age_start = 90, age_end = 125)]
  }

  # NZL includes week 52 of 2010 that needs to be dropped
  if (ihme_loc == 'NZL') {
    allcause <- allcause[year_start >= 2011]
  }

  # USA use all sex combined only (pre-2020 is only all-sex)
  if (ihme_loc == "USA") {
    allcause <- allcause[sex == "all"]
  }

  # BRA_4771 has deaths > pop for 95+: collapse 90-94 and 95+
  if (ihme_loc == "BRA_4771") {
    allcause[age_start >= 90, `:=` (age_start = 90, age_end = 125)]
  }

  # Sex and age specific Eurostat data
  if(allcause$source[1] == "EUROSTAT"){
    allcause <- allcause[!sex=="all" & (age_start==0 & age_end==125)]
  }

  # sex and age specific THA data
  if(ihme_loc == "THA"){
    allcause <- allcause[sex=="all" & (age_start==0 & age_end==125)]
  }
  
  # sex and age specific JPN data
  if(ihme_loc %like% "JPN"){
    allcause <- allcause[(age_start==0 & age_end==125)]
  }

  # combine weeks 52 & 53 in VR
  if(!ihme_loc %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek)){
    allcause[week_start == 53, ':=' (week_start = 52, week_end = 52)]
    allcause <- allcause[, .(deaths=sum(deaths)), by=setdiff(names(allcause),"deaths")]
  }

}


# WMD Adjustment ---------------------------------------------------------------

if ((process_allcause) | (process_wmd)) {
  # VR adjustment for countries with low estimated vr completeness 
  # Adjusting all of WMD so any allcause replacements are already adjusted
  wmd <- merge(
    wmd, completeness,
    by = c("year_start", "location_id", "sex"),
    all.x = T
  )
  wmd <- merge(
    wmd, total_completeness,
    by = c("year_start", "location_id"),
    all.x = T
  )
  # first fill down so all gaps and end of series filled with previous year
  wmd <- wmd %>%
    dplyr::group_by(location_id) %>%
    tidyr::fill(total_comp, .direction = "down") %>%
    dplyr::ungroup() %>% setDT
  # then fill up so gaps at beginning of time series filled with next year
  wmd <- wmd %>%
    dplyr::group_by(location_id) %>%
    tidyr::fill(total_comp, .direction = "up") %>%
    dplyr::ungroup() %>% setDT
  
  # adjust VR death counts based on completeness
  wmd[age_end <= 5, deaths := deaths / child]
  wmd[age_start == 5 & age_end == 10, deaths := deaths / ((child * 2/3) + (adult * 1/3))]
  wmd[age_start == 10 & age_end == 15, deaths := deaths / ((child * 1/3) + (adult * 2/3))]
  wmd[!age_start == 0 & age_end > 15, deaths := deaths / adult]
  wmd[age_start == 0 & age_end == 125, deaths := deaths / total_comp]
  wmd[, c("adult", "child", "total_comp") := NULL]
}


# Data processing: all-cause ---------------------------------------------------

if (process_allcause) {

  message(Sys.time(), " | Data processing: all-cause")

  # aggregate age groups over 95+
  allcause[age_end > 125, age_end := 125]
  allcause[age_start >= 95, `:=` (age_start = 95, age_end = 125)]
  allcause <- allcause[,
    list(deaths = sum(deaths)),
    by = c(setdiff(id_cols, c("time_start", "time_unit")),
          "week_start", "month_start", "day_start", "source")
  ]

  # format to time_start and time_unit based on presence of week info
  if (nrow(allcause[is.na(week_start)]) > 0 & !nrow(allcause[is.na(month_start)]) > 0) {
    allcause[, `:=` (time_unit = "month", time_start = month_start)]
  } else if (!nrow(allcause[is.na(week_start)]) > 0 & nrow(allcause[is.na(month_start)]) > 0){
    allcause[, `:=` (time_unit = "week", time_start = week_start)]
  } else {

    # create temporary id_cols with month & week
    id_cols_2 <- c(
      "month_start", "week_start", "source",
      setdiff(id_cols, c("time_start", "time_unit"))
    )

    # create weekly totals
    allcause_w <- allcause[!is.na(week_start),
      list(deaths = sum(deaths),
           time_unit = "week",
           time_start = week_start),
      by = setdiff(id_cols_2, "month_start")
    ]
    allcause_w <- allcause_w[, -c("week_start")]
    # create monthly totals
    allcause_m <- allcause[!is.na(month_start),
      list(deaths = sum(deaths),
           time_unit = "month",
           time_start = month_start),
      by = setdiff(id_cols_2, "week_start")
    ]
    allcause_m <- allcause_m[, -c("month_start")]

    # combine
    allcause <- rbind(allcause_w, allcause_m)

  }

  # format year/month/day to week if possible
  if (all(c("day_start","month_start") %in% names(allcause)) &
          all(!is.na(allcause$day_start)) & all(is.na(allcause$week_start))) {

    allcause_w <- copy(allcause)
    allcause_w$week_start <- as.numeric(allcause_w$week_start)

    if (ihme_loc %in% locs_with_isoweek){
      allcause_w[is.na(week_start),
                 ':='(
                   week_start = lubridate::isoweek(
                     lubridate::ymd(
                       paste(year_start, month_start, day_start, sep = "/")
                     )
                   ),
                   year_start = lubridate::isoyear(
                     lubridate::ymd(
                       paste(year_start, month_start, day_start, sep = "/")
                     )
                   )
                 )
               ]
      if(all(allcause_w$source == "ITA_ISTAT")){
        allcause_w <- allcause_w[!(is.na(week_start) & month_start == 2 & day_start == 29 & deaths == 0)]
      }
    } else if (ihme_loc %in% locs_with_epiweek){
      allcause_w[is.na(week_start),
                 ':='(
                   week_start = lubridate::epiweek(
                     lubridate::ymd(
                       paste(year_start, month_start, day_start, sep = "/")
                     )
                   ),
                   year_start = lubridate::epiyear(
                     lubridate::ymd(
                       paste(year_start, month_start, day_start, sep = "/")
                     )
                   )
                 )
               ]
    } else if (ihme_loc %in% locs_with_ukweek) {
      flag <- allcause_w[is.na(week_start) & !is.na(month_start) & !is.na(day_start)]
      if(nrow(flag) > 0) {
        error("New UK data has been added and needs converted to week")
      }
    } else {
      allcause_w[is.na(week_start),
                 ':='(
                   week_start = lubridate::week(
                     lubridate::ymd(
                       paste(year_start, month_start, day_start, sep = "/")
                     )
                   ),
                   year_start = lubridate::week(
                     lubridate::ymd(
                       paste(year_start, month_start, day_start, sep = "/")
                     )
                   )
                 )
               ]
    }
    allcause_w[, ':='(time_start = week_start, time_unit = "week")]

    # Combine weeks 52 & 53 in VR
    if(!ihme_loc %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek)){
      allcause_w[week_start == 53, ':=' (week_start = 52, week_end = 52)]
      allcause_w <- allcause_w[, .(deaths=sum(deaths)), by=setdiff(names(allcause_w),"deaths")]
    }

    allcause <- rbind(allcause, allcause_w)
  }

  # aggregate
  allcause <- allcause[,
    list(deaths = sum(deaths)),
    by = c(id_cols, "source")
  ]

  # Dropping locations missing terminal age groups in later weeks
  allcause[, year_time := paste(year_start, time_start, time_unit, sep = "_")]
  times_with_95plus <- unique(allcause[age_start == 95, year_time])
  if (length(times_with_95plus) > 0) {
    dropped <- unique(allcause[!year_time %in% times_with_95plus, year_time])
    message(paste0("Dropping ",length(dropped)," location time units without 95+"))
    if (length(dropped) > 0){
      print(dropped)
    }
    allcause_aa <- allcause[!year_time %in% times_with_95plus]
    allcause <- allcause[(year_time %in% times_with_95plus)]
  }

  # VR adjustment for countries with low estimated vr completeness 
  # combine with processed VR data
    allcause <- merge(
      allcause, completeness,
      by = c("year_start", "location_id", "sex"),
      all.x = T
    )
    allcause <- merge(
      allcause, total_completeness,
      by = c("year_start", "location_id"),
      all.x = T
    )
    # first fill down so all gaps and end of series filled with previous year
    allcause <- allcause %>%
      dplyr::group_by(location_id) %>%
      tidyr::fill(total_comp, .direction = "down") %>%
      dplyr::ungroup() %>% setDT
    # then fill up so gaps at beginning of time series filled with next year
    allcause <- allcause %>%
      dplyr::group_by(location_id) %>%
      tidyr::fill(total_comp, .direction = "up") %>%
      dplyr::ungroup() %>% setDT
    
    # adjust VR death counts based on completeness
    allcause[age_end <= 5, deaths := deaths / child]
    allcause[age_start == 5 & age_end == 10, deaths := deaths / ((child * 2/3) + (adult * 1/3))]
    allcause[age_start == 10 & age_end == 15, deaths := deaths / ((child * 1/3) + (adult * 2/3))]
    allcause[!age_start == 0 & age_end > 15, deaths := deaths / adult]
    allcause[age_start == 0 & age_end == 125, deaths := deaths / total_comp]
    allcause[, c("adult", "child", "total_comp") := NULL]

  age_mapping <- rbind(
    data.table(age_start=0, age_end=125),
    process_ages[(is_estimate) & !(age_start==0 & age_end==125),list(age_start, age_end)]
    )

  # aggregate to all-age and collapse to common age intervals - without dropping 95+
  allcause <- hierarchyUtils::agg(
    allcause,
    id_cols = c(id_cols, "source","year_time"),
    value_cols = "deaths",
    col_stem = "age",
    col_type = "interval",
    mapping = age_mapping,
    present_agg_severity  = 'none',
    missing_dt_severity = 'warning'
  )

  if (length(times_with_95plus) > 0) {
    if(length(dropped > 1)){
      # aggregate to all-age and collapse to common age intervals - all ages
      allcause_aa <- hierarchyUtils::agg(
        allcause_aa,
        id_cols = c(id_cols, "source","year_time"),
        value_cols = "deaths",
        col_stem = "age",
        col_type = "interval",
        mapping = age_mapping,
        present_agg_severity  = 'none',
        missing_dt_severity = 'warning'
      )
      allcause_aa <- allcause_aa[age_start == 0 & age_end == 125]

      # combine
      check <- rbind(allcause[year_time %in% dropped], allcause_aa[!year_time %in% dropped])
      if (nrow(check) > 1){
        stop("Age groups not dropping different terminal age groups properly")
      }
      allcause <- rbind(allcause, allcause_aa)
    }
  }
  allcause[, "year_time" := NULL]

  # aggregate to all-sex
  if (all(c("male", "female") %in% unique(allcause$sex)) &
      (!nrow(allcause[sex == "male"]) == nrow(allcause[sex == "all"]))) {
    allcause_all_sex <- hierarchyUtils::agg(
      allcause[sex != "all"],
      id_cols = c(id_cols, "source"),
      value_cols = "deaths",
      col_stem = "sex",
      col_type = "categorical",
      mapping = process_sexes[is_estimate == T & sex != "all",
                              list(child = sex, parent = parent_sex)],
      missing_dt_severity = 'warning'
    )
    allcause <- rbind(allcause, allcause_all_sex)
    allcause <- unique(allcause, by = c(id_cols, "source"))
  }

  # aggregate to monthly and tack onto weekly dataset
  if (("week" %in% unique(allcause$time_unit)) &
      !("month" %in% unique(allcause$time_unit)) | ihme_loc %in% c("GEO")) {
    allcause$time_start <- as.numeric(allcause$time_start)
    allcause_monthly <- agg_month_proportional(
      dt = allcause[time_unit == "week"],
      measure = "deaths",
      id_cols = c(id_cols, "source")
    )
    allcause <- rbind(allcause, allcause_monthly)
  }

  # Identify if WMD is is more recent and if so, replace
  if (nrow(wmd) > 0) {

    if (nrow(wmd[time_unit == "week"]) > 0) {

      wmd_monthly <- agg_month_proportional(
        dt = wmd[time_unit == "week"],
        measure = "deaths",
        id_cols = c(id_cols, "source")
      )

      wmd <- rbind(wmd, wmd_monthly)

    }

    temp_allcause <- copy(allcause)
    temp_wmd <- copy(wmd)

    # Drop incomplete last row in favor of WMD (Ireland and Philippines for now)
    if (loc_id == 16 | loc_id == 84) temp_allcause <- temp_allcause[-(nrow(temp_allcause)), ]

    setnames(temp_allcause, "deaths", "deaths_allcause")
    setnames(temp_wmd, "deaths", "deaths_wmd")

    temp <- merge(
      temp_allcause[age_start == 0 & age_end == 125 & sex == "all", !c("source")],
      temp_wmd[age_start == 0 & age_end == 125 & sex == "all", !c("source")],
      by = c("location_id", "year_start", "time_start", "time_unit",
             "age_start", "age_end", "sex"),
      all = TRUE
    )

    allcause_units <- unique(temp_allcause$time_unit)

    temp <- temp[!is.na(deaths_wmd) & time_unit %in% allcause_units]

    if (loc_id %in% c(71,93,90,83,78,79)) {

      keep <- temp[!is.na(deaths_allcause)]

    } else {

      keep <- temp[year_start >= 2020 & is.na(deaths_allcause)]

    }

    if (nrow(keep) > 0) {

      # save
      readr::write_csv(
        keep, fs::path())
      )

      keep[, ':=' (deaths = deaths_wmd, deaths_allcause = NULL, deaths_wmd = NULL)]

      keep[, source := "WMD"]

      allcause[, index := paste0(year_start, "_", time_start, "_", time_unit, "_", sex, "_", age_start, "_", age_end)]
      keep[, index := paste0(year_start, "_", time_start, "_", time_unit, "_", sex, "_", age_start, "_", age_end)]

      allcause <- rbind(allcause[!(index %in% keep$index)], keep)

      allcause[, index := NULL]
      
      if (loc_id %in% c(93,90,83,78,79)) {
        allcause <- allcause[!source == "HMD"]
      }

    }

  }

}


# Data processing: external mortality ------------------------------------------

if (process_em){
  message(Sys.time(), " | Data processing: external em")

  # subset
  ext_em <- ext_em[, c(..id_cols,"model_type","deaths_excess","deaths_covid",
                       "population","source","covid_source","pop_source")]
  ext_em$deaths_covid <- as.numeric(ext_em$deaths_covid)
  ext_em$population <- as.numeric(ext_em$population)

  # find common age intervals between all-cause and population
  age_mapping <- rbind(
    data.table(age_start=0, age_end=125),
    process_ages[(is_estimate) & !(age_start==0 & age_end==125),list(age_start, age_end)]
  )

  # aggregate to all-age and collapse to common age intervals
  ext_em <- hierarchyUtils::agg(
    ext_em,
    id_cols = c(id_cols,'model_type','source','covid_source','pop_source'),
    value_cols = c("deaths_excess",'deaths_covid','population'),
    col_stem = "age",
    col_type = "interval",
    mapping = age_mapping,
    present_agg_severity  = 'none',
    missing_dt_severity = 'warning',
    na_value_severity = 'skip'
  )

  # aggregate to all-sex
  if (all(c("male", "female") %in% unique(ext_em$sex)) &
      (!nrow(ext_em[sex == "male"]) == nrow(ext_em[sex == "all"]))) {
    ext_em_sex <- hierarchyUtils::agg(
      ext_em[sex != "all"],
      id_cols = c(id_cols,'model_type','source','covid_source','pop_source'),
      value_cols = c("deaths_excess",'deaths_covid','population'),
      col_stem = "sex",
      col_type = "categorical",
      mapping = process_sexes[is_estimate == T & sex != "all",
                              list(child = sex, parent = parent_sex)],
      missing_dt_severity = 'skip',
      na_value_severity = 'skip'
    )
    ext_em <- rbind(ext_em, ext_em_sex)
    ext_em <- unique(ext_em, by = c(id_cols, "model_type","source"))
  }

  # add `age_name` variable
  ext_em <- hierarchyUtils::gen_name(ext_em, "age")
}


# Combine Person Years ---------------------------------------------------------

message(Sys.time(), " | Combine all-cause and person years")

# combine deaths and person years
if (process_allcause) {
  data <- merge(
    allcause,
    person_years,
    by = c(id_cols),
    all.y = T
  )
  data <- data[year_start >= min(allcause$year_start)]
  assertthat::assert_that(
    all(data[!is.na(deaths)]$deaths <= data[!is.na(deaths)]$person_years),
    msg = "Check for cases where deaths > person years"
  )
  # add `age_name` variable
  data <- hierarchyUtils::gen_name(data, "age")
}
if (process_em) {
  if(nrow(ext_em[is.na(population)]) > 0){
    ext_em <- ext_em[,-c("population", "pop_source")]
    if(ihme_loc %like% "IRN"){
      pop_IRN <- copy(person_years)
      pop_IRN <- pop_IRN[year_start == 2020 & time_start <= 38 & time_unit == "week",
                         list(person_years = sum(person_years)
                         ),
                         by = "location_id"
                         ]
      pop_IRN[, ':='(time_start = 38, time_unit = "week", year_start = 2020,
                      sex = "all", age_start = 0, age_end = 125, pop_source = "gbd")]
      data_ext <- merge(ext_em, pop_IRN, by = id_cols, all.x = T)
    } else if (ihme_loc %like% "RUS") {
      pop_RUS <- copy(person_years)
      pop_RUS <- pop_RUS[year_start == 2020 & time_start <= 11 & time_unit == "month",
                         list(person_years = sum(person_years)
                         ),
                         by = "location_id"
                         ]
      pop_RUS[, ':='(time_start = 11, time_unit = "month", year_start = 2020,
                     sex = "all", age_start = 0, age_end = 125, pop_source = "gbd")]
      data_ext <- merge(ext_em, pop_RUS, by = id_cols, all.x = T)
    } else {
      data_ext <- merge(ext_em, person_years, by = id_cols, all.x = T)
    }
  } else {
    data_ext <- copy(ext_em)
  }
}
if (process_wmd) {
  data <- merge(
    wmd, 
    person_years,
    by = c(id_cols),
    all.y = T
  )
  data <- data[year_start >= min(wmd$year_start)]
  assertthat::assert_that(
    all(data[!is.na(deaths)]$deaths <= data[!is.na(deaths)]$person_years),
    msg = "Check for cases where deaths > person years"
  )
  # add `age_name` variable
  data <- hierarchyUtils::gen_name(data, "age")
}
if (!process_allcause & !process_wmd & nrow(person_years) > 0) {
  data <- copy(person_years)
  data[, `:=` (deaths = NA, source = NA)]
  data <- data[year_start %in% covid_years]
  # add `age_name` variable
  data <- hierarchyUtils::gen_name(data, "age")
}
if (nrow(person_years) == 0){
  message("All cause data is missing person years for this location")
}


# EM Person Years --------------------------------------------------------------

# needs to be separate from person_years to capture non-GBD person_years - this section
# finds person years for external em data that may be only a single time unit
if (process_em & ihme_loc == "Mumbai"){
  # other locations are for periods of time and need specific n_days
  # making num_units inclusive of specified time unit
  num_units <- unique(data_ext[,time_start])
  if(length(num_units) > 1){
    stop("Error in calculating person years over period")
  }
  if (length(num_units) == 1){
    if(num_units == 0){
      data_ext[, person_years := population]
    } else {
      data_ext <- pop_to_py(data_ext, num_units)
    }
  } else {
    data_ext <- pop_to_py(data_ext, num_units)
  }
  data_ext[, population := NULL]
}


# COVID -------------------------------------------------------------------

message(Sys.time(), " | COVID data")

if (nrow(covid) == 0) {
  warning("No JHU COVID data for this location")

  data[, c("covid_source", "deaths_covid") := NA]

} else {

  # check that input is weekly
  assertable::assert_colnames(covid[time_unit=="week"], "time_start", only_colnames = F, quiet = T)
  assertable::assert_values(covid[time_unit=="week"], "time_start", "not_na", quiet = T)
  setorderv(covid, cols = c("year_start", "time_unit", "time_start"))

  # check
  covid[time_unit=="week", diff_weeks := time_start - shift(time_start), by = "year_start"]
  assertthat::assert_that(
    max(covid$diff_weeks, na.rm = T) == 1,
    msg = "Weeks are not consecutive -- does this data represent periods of 1 week?"
  )
  covid[, diff_weeks := NULL]

  # format
  setnames(covid, "type", "covid_source")
  covid <- covid[, .SD, .SDcols = c(id_cols, "covid_source", "deaths_covid")]

  if(nrow(person_years) > 0) {
    data <- merge(data, covid, by = id_cols, all.x = T)
  } else {
    data <- copy(covid)
    data <- data[year_start %in% covid_years]

    fill_gaps <- function(dt, mapping) {
      missing <- dplyr::anti_join(mapping, unique(dt[,c("age_start","age_end","sex")]), by = c("age_start","age_end","sex"))

      if(nrow(missing) > 0) {
        new_rows <- data.table(age_start = missing$age_start, location_id = dt$location_id[1], time_start = dt$time_start[1],
                               sex = missing$sex, year_start = dt$year_start[1], time_unit = dt$time_unit[1],
                               age_end = missing$age_end)

        dt <- rbindlist(list(dt,new_rows), fill = TRUE, use.names = TRUE)

      }

      return(dt)

    }

    mapping <- rbind(
      data.table(age_start=0, age_end=125),
      process_ages[(is_estimate) & !(age_start==0 & age_end==125),list(age_start, age_end)]
    )
    mapping <- mapping[rep(seq_len(nrow(mapping)), each = 3), ]
    mapping$sex <- rep(c("all", "male", "female"), times = 21)
    data <- data[, fill_gaps(.SD,mapping), by = c("time_start", "time_unit", "location_id", "year_start"),
                 .SDcols = names(data)]
    data <- subset(data, select=which(!duplicated(names(data))))

    # add `age_name` variable and others
    data[, `:=` (person_years = NA, pop_source = NA, deaths = NA, source = NA,
                 person_years = NA)]
    data <- hierarchyUtils::gen_name(data, "age")

  }

  if (process_em) {
    if(!ihme_loc %like% "ZAF"){
      covid <- covid[,deaths_covid := cumsum(deaths_covid), by = "time_unit"]
    }
    if (nrow(data_ext[is.na(deaths_covid)]) > 0) {
      data_ext <- data_ext[,-c('deaths_covid','covid_source')]
      data_ext <- merge(data_ext, covid, by = id_cols, all.x = T)
    }
  }
}


# IDR covariate ----------------------------------------------------------------

message(Sys.time(), " | IDR")
setnames(idr, "idr", "idr_reference")

# add ihme_loc_id
idr <- merge(idr, process_locations[,c("location_id","ihme_loc_id")], by = "location_id")

# using raw data #
if (ihme_loc %like% "ETH_"){
  # use national covid_incidence to weight ETH subnationals
    setnames(idr_nat, "idr", "idr_reference")
    idr_nat <- merge(
      idr_nat,
      daily_covid[,c("location_id","date","daily_cases")],
      by = c("location_id", "date"),
      all.x = T
      )
    idr_nat[, covid_incidence := (1 / idr_reference) * daily_cases]
    idr_nat[covid_incidence == 0, covid_incidence := 0.01]
    idr <- merge(idr,
                 idr_nat[,c("date","covid_incidence","daily_cases")],
                 by = "date", all.x = T
                 )
} else {
  idr <- merge(idr, daily_covid, by = c("location_id", "date"), all.x = T)
  idr[, covid_incidence := (1 / idr_reference) * daily_cases]
  idr[covid_incidence == 0, covid_incidence := 0.01]
}

# Filling in with 0.01 incidence
idr[is.na(covid_incidence), covid_incidence := 0.1]

# lag idr
idr <- idr[order(location_id, date)]
idr[, idr_lagged := shift(idr_reference, covariate_lag_days), by = "location_id"]
idr[is.na(idr_lagged), idr_lagged := 0]

# weeks
idr_w <- copy(idr)
idr_w[!ihme_loc_id %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek), ':=' (time_start = lubridate::week(date),
                                                                       year_start = lubridate::year(date))]
# combine week 53 with 52 otherwise
idr_w[!ihme_loc_id %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek) & time_start == 53, time_start := 52]
idr_w[ihme_loc_id %in% locs_with_isoweek, ':=' (time_start = lubridate::isoweek(date),
                                                year_start = lubridate::isoyear(date))]
idr_w[ihme_loc_id %in% locs_with_epiweek, ':=' (time_start = lubridate::epiweek(date),
                                                year_start = lubridate::epiyear(date))]
idr_w[ihme_loc_id %in% locs_with_ukweek, ':=' (week_year = aweek::date2week(date, week_start = "sat", floor_day = T))]
idr_w[ihme_loc_id %in% locs_with_ukweek, ':=' (week_start = substr(week_year,7,8),
                                               year_start = substr(week_year,1,4))]
idr_w[ihme_loc_id %in% locs_with_ukweek, time_start := week_start]
idr_w[ihme_loc_id %in% locs_with_ukweek & year_start == 2020, ':=' (time_start = time_start + 1)]
idr_w[ihme_loc_id %in% locs_with_ukweek & year_start == 2019 & time_start == 53, ':=' (time_start = 1,
                                                                                         year_start = 2020)]
idr_w[, c("time_start", "year_start") := lapply(.SD, as.numeric), .SDcols = c("time_start", "year_start")]
idr_w[, week_year := NULL]
idr_w[, ":=" (time_unit = "week")]

# Do not weight TZA
if (loc_id == 189){
  idr_w <- idr_w[,
                 list(idr_reference = mean(idr_reference),
                      idr_lagged = mean(idr_lagged),
                      confirmed_cases = sum(daily_cases),
                      covid_incidence = sum(covid_incidence)),
                 by = setdiff(id_cols, c("age_start", "age_end", "sex"))
                 ]
} else {
  idr_w <- idr_w[,
                 list(idr_reference = weighted.mean(idr_reference, covid_incidence),
                      idr_lagged = weighted.mean(idr_lagged, covid_incidence),
                      confirmed_cases = sum(daily_cases),
                      covid_incidence = sum(covid_incidence)),
                 by = setdiff(id_cols, c("age_start", "age_end", "sex"))
                 ]
}

# months
idr_m <- copy(idr)
idr_m[, ":=" (year_start = lubridate::year(date),
              time_start = lubridate::month(date),
              time_unit = 'month')]
# Do not weight TZA
if (loc_id == 189) {
  idr_m <- idr_m[,
                 list(idr_reference = mean(idr_reference),
                      idr_lagged = mean(idr_lagged),
                      confirmed_cases = sum(daily_cases),
                      covid_incidence = sum(covid_incidence)
                      ),
                 by = setdiff(id_cols, c("age_start", "age_end", "sex"))
                 ]
} else {
  idr_m <- idr_m[,
                 list(idr_reference = weighted.mean(idr_reference, covid_incidence),
                      idr_lagged = weighted.mean(idr_lagged, covid_incidence),
                      confirmed_cases = sum(daily_cases),
                      covid_incidence = sum(covid_incidence)
                      ),
                 by = setdiff(id_cols, c("age_start", "age_end", "sex"))
                 ]
}

# locations with a time period
if (ihme_loc %like% "IRN") {
  idr_IRN <- copy(idr)
  idr_IRN[, ":=" (year_start = lubridate::year(date),
                  time_start = lubridate::week(date),
                  time_unit = 'week')]
  idr_IRN <- idr_IRN[year_start == 2020 & time_start <= 38 & time_unit == "week",
                           list(idr_reference = weighted.mean(idr_reference, covid_incidence),
                                idr_lagged = weighted.mean(idr_lagged, covid_incidence),
                                confirmed_cases = sum(daily_cases, na.rm=T),
                                covid_incidence = sum(covid_incidence)
                           ),
                           by = setdiff(id_cols, c("age_start", "age_end", "sex","time_start"))
                           ]
  idr_IRN[, time_start := 38]
}
if(ihme_loc == "Mumbai"){
  idr_mumbai <- copy(idr)
  idr_mumbai[, ":=" (year_start = lubridate::year(date),
                     time_start = lubridate::month(date),
                     time_unit = 'month')]
  idr_mumbai <- idr_mumbai[year_start == 2020 & time_start >=2 & time_start <= 7 & time_unit == "month",
                           list(idr_reference = weighted.mean(idr_reference, covid_incidence),
                                idr_lagged = weighted.mean(idr_lagged, covid_incidence),
                                confirmed_cases = sum(daily_cases, na.rm=T),
                                covid_incidence = sum(covid_incidence)
                 ),
                 by = setdiff(id_cols, c("age_start", "age_end", "sex","time_start"))
                 ]
  idr_mumbai[, time_start := 7]
}

idr <- rbind(idr_m, idr_w)

if (process_em){
  if (ihme_loc %like% "IRN") {
    data_ext <- merge(
      data_ext, idr_IRN,
      by = setdiff(id_cols, c("sex", "age_start", "age_end")),
      all.x = T
    )
  } else if (ihme_loc == "Mumbai") {
    data_ext <- merge(
      data_ext, idr_mumbai,
      by = setdiff(id_cols, c("sex", "age_start", "age_end")),
      all.x = T
    )
  } else {
    data_ext <- merge(
      data_ext, idr,
      by = setdiff(id_cols, c("sex", "age_start", "age_end")),
      all.x = T
    )
  }
}

data <- merge(
  data, idr,
  by = setdiff(id_cols, c("sex", "age_start", "age_end")),
  all.x = T
)


# SEIR Covariates --------------------------------------------------------------

message(Sys.time(), " | SEIR")

# testing
setnames(testing, "mean", "testing_reference")

# replace ihme_loc_id
testing[, ihme_loc_id := NULL]
testing <- merge(testing, process_locations[,c("location_id","ihme_loc_id")], by = "location_id")

# lag testing
testing <- testing[order(date)]
testing[, testing_lagged := shift(testing_reference, covariate_lag_days), by = "location_id"]
testing[is.na(testing_lagged), testing_lagged := 0]

# add time unit
# weeks
testing_w <- copy(testing)
testing_w[!ihme_loc_id %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek), ':=' (time_start = lubridate::week(date),
                                                                           year_start = lubridate::year(date))]
# combine week 53 with 52 otherwise
testing_w[!ihme_loc_id %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek) & time_start == 53, time_start := 52]
testing_w[ihme_loc_id %in% locs_with_isoweek, ':=' (time_start = lubridate::isoweek(date),
                                                    year_start = lubridate::isoyear(date))]
testing_w[ihme_loc_id %in% locs_with_epiweek, ':=' (time_start = lubridate::epiweek(date),
                                                    year_start = lubridate::epiyear(date))]
testing_w[ihme_loc_id %in% locs_with_ukweek, ':=' (week_year = aweek::date2week(date, week_start = "sat", floor_day = T))]
testing_w[ihme_loc_id %in% locs_with_ukweek, ':=' (time_start = substr(week_year,7,8),
                                               year_start = substr(week_year,1,4))]
testing_w[ihme_loc_id %in% locs_with_ukweek & year_start == 2020, ':=' (time_start = time_start + 1)]
testing_w[ihme_loc_id %in% locs_with_ukweek & year_start == 2019 & time_start == 53, ':=' (time_start = 1,
                                                                                         year_start = 2020)]
testing_w[, c("time_start", "year_start") := lapply(.SD, as.numeric), .SDcols = c("time_start", "year_start")]
testing_w[, week_year := NULL]
testing_w[, ":=" (time_unit = "week")]
testing_w <- testing_w[,
  list(testing_reference = mean(testing_reference),
       testing_lagged = mean(testing_lagged)),
  by = setdiff(id_cols, c("age_start", "age_end", "sex"))
]

# months
testing_m <- copy(testing)
testing_m[, ":=" (year_start = lubridate::year(date),
                  time_start = lubridate::month(date),
                  time_unit = "month")]
testing_m <- testing_m[,
  list(testing_reference = mean(testing_reference),
      testing_lagged = mean(testing_lagged)),
  by = setdiff(id_cols, c("age_start", "age_end", "sex"))
]
testing <- rbind(testing_m, testing_w)

# mobility
setnames(mobility, "mean", "mobility_reference")

# replace ihme_loc_id
mobility[, ihme_loc_id := NULL]
mobility <- merge(mobility, process_locations[,c("location_id","ihme_loc_id")], by = "location_id")

# lag mobility
mobility <- mobility[order(date)]
mobility[, mobility_lagged := shift(mobility_reference, covariate_lag_days), by = "location_id"]
mobility[is.na(mobility_lagged), mobility_lagged := 0]

# add time unit
# weeks
mobility_w <- copy(mobility)
mobility_w[!ihme_loc_id %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek), ':=' (time_start = lubridate::week(date),
                                                                            year_start = lubridate::year(date))]
# combine week 53 with 52 otherwise
mobility_w[!ihme_loc_id %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek) & time_start == 53, time_start := 52]
mobility_w[ihme_loc_id %in% locs_with_isoweek, ':=' (time_start = lubridate::isoweek(date),
                                                     year_start = lubridate::isoyear(date))]
mobility_w[ihme_loc_id %in% locs_with_epiweek, ':=' (time_start = lubridate::epiweek(date),
                                                     year_start = lubridate::epiyear(date))]
mobility_w[ihme_loc_id %in% locs_with_ukweek, ':=' (week_year = aweek::date2week(date, week_start = "sat", floor_day = T))]
mobility_w[ihme_loc_id %in% locs_with_ukweek, ':=' (time_start = substr(week_year,7,8),
                                                   year_start = substr(week_year,1,4))]
mobility_w[ihme_loc_id %in% locs_with_ukweek & year_start == 2020, ':=' (time_start = time_start + 1)]
mobility_w[ihme_loc_id %in% locs_with_ukweek & year_start == 2019 & time_start == 53, ':=' (time_start = 1,
                                                                                         year_start = 2020)]
mobility_w[, c("time_start", "year_start") := lapply(.SD, as.numeric), .SDcols = c("time_start", "year_start")]
mobility_w[, week_year := NULL]
mobility_w[, ":=" (time_unit = "week")]
mobility_w <- mobility_w[,
  list(mobility_reference = mean(mobility_reference),
       mobility_lagged = mean(mobility_lagged)),
  by = setdiff(id_cols, c("age_start", "age_end", "sex"))
]

# months
mobility_m <- copy(mobility)
mobility_m[, ":=" (year_start = lubridate::year(date),
                   time_start = lubridate::month(date),
                   time_unit = 'month')]
mobility_m <- mobility_m[,
  list(mobility_reference = mean(mobility_reference),
       mobility_lagged = mean(mobility_lagged)),
  by = setdiff(id_cols, c("age_start", "age_end", "sex"))
]
mobility <- rbind(mobility_m,mobility_w)

# combine
if (process_em){

  # Aggregate for periods
  if (ihme_loc %like% "IRN") {
    testing <- testing[year_start == 2020 & time_unit == "week" & time_start <= 38,
                       list(testing_reference = mean(testing_reference),
                            testing_lagged = mean(testing_lagged)),
                       by = setdiff(id_cols, c("age_start", "age_end", "sex", "time_start"))
                       ]
    testing[, time_start := 38]
    mobility <- mobility[year_start == 2020 & time_unit == "week" & time_start <= 38,
                         list(mobility_reference = mean(mobility_reference),
                              mobility_lagged = mean(mobility_lagged)),
                         by = setdiff(id_cols, c("age_start", "age_end", "sex", "time_start"))
                         ]
    mobility[, time_start := 38]
  }
  if (ihme_loc == "Mumbai") {
    testing <- testing[year_start == 2020 & time_unit == "month" & time_start >= 2 & time_start <= 7,
                       list(testing_reference = mean(testing_reference),
                            testing_lagged = mean(testing_lagged)),
                            by = setdiff(id_cols, c("age_start", "age_end", "sex", "time_start"))
                       ]
    testing[, time_start := 7]
    mobility <- mobility[year_start == 2020 & time_unit == "month" & time_start >= 2 & time_start <= 7,
                       list(mobility_reference = mean(mobility_reference),
                            mobility_lagged = mean(mobility_lagged)),
                       by = setdiff(id_cols, c("age_start", "age_end", "sex", "time_start"))
                       ]
    mobility[, time_start := 7]
  }

  data_ext <- Reduce(
    function(x,y) merge(x, y, by = setdiff(id_cols, c("sex", "age_start", "age_end")), all.x = T),
    list(data_ext, testing, mobility)
  )
}

data <- Reduce(
  function(x,y) merge(x, y, by = setdiff(id_cols, c("sex", "age_start", "age_end")), all.x = T),
  list(data, testing, mobility)
)


# Hospitalization covariates ---------------------------------------------------

## GBD inpatient admissions ##
setnames(gbd_inpatient, c("mean_value","year_id"), c("gbd_inpatient_admission","year_start"))

# subset
gbd_inpatient <- gbd_inpatient[, c("location_id","year_start","age_group_id","sex",'gbd_inpatient_admission')]

# aggregate age and sex
gbd_inpatient <- gbd_inpatient[, .(gbd_inpatient_admission = mean(gbd_inpatient_admission)), by = c("location_id","year_start")]

# combine
# capacity #
if(process_em) {
  data_ext <- merge(
    data_ext,
    gbd_inpatient,
    by = c("location_id","year_start"),
    all.x = T
  )
}
data <- merge(
  data,
  gbd_inpatient,
  by = c("location_id","year_start"),
  all.x = T
)


# Star rating ------------------------------------------------------------------

# combine
if(process_em) {
  data_ext <- merge(
    data_ext,
    stars[, c("location_id","stars")],
    by = "location_id",
    all.x = T
  )
}
data <- merge(
  data,
  stars[, c("location_id","stars")],
  by = "location_id",
  all.x = T
)


# Check and save ---------------------------------------------------------------

message(Sys.time(), " | Check and save")

# Remove location id from Mumbai until it has its own
if(ihme_loc=='Mumbai'){
  data_ext[,location_id:=as.integer(NA)]
}

# Want to use midpoint of period from here
if(process_em){
  # Sources with for a time period should be set to the middle of that
  # time period rather than the end
  if(ihme_loc %like% "IRN"){
    data_ext[, time_start := round((time_start - 8) / 2)]
  }
  if(ihme_loc == "Mumbai"){
    data_ext[, time_start := round((time_start - 1) / 2)]
  }
}

# Add tag for wmd only data 
if (process_wmd) {
  data[, wmd_only_loc := T]
} else { 
  data[, wmd_only_loc := F] 
}
if (process_em){
  data_ext[, wmd_only_loc := F]
}

# check values
data_cols <- c(id_cols, "age_name", "source", "pop_source", "covid_source",
               "deaths_covid", "person_years", "testing_reference",
               "mobility_reference","mobility_lagged", "idr_reference",
               "idr_lagged", "testing_lagged",
               "confirmed_cases", "covid_incidence",
               "gbd_inpatient_admission", "stars", "wmd_only_loc")
if (process_allcause & (!loc_id == 60412)) { 
  # check years
  assertthat::assert_that(2020 %in% unique(data$year_start))
  if (process_allcause) {
    assertthat::assert_that(
      length(unique(data[year_start < 2020, year_start])) > 0,
      msg = "Need at least one year prior to 2020."
    )
  }
  assertable::assert_values(
    data[!is.na(deaths)], "deaths", "gte", 0, quiet = T
  )
  assertthat::assert_that(
    all(data[!is.na(deaths) & !is.na(person_years)]$deaths <
          data[!is.na(deaths) & !is.na(person_years)]$person_years)
  )
}
if (process_em) {
  data_ext[, ihme_loc_id := NULL]
  data_ext <- data_ext[!(year_start > 2022)]
  assertthat::assert_that(
    all(data_ext[!is.na(deaths_excess) & !is.na(person_years)]$deaths_excess < data_ext[!is.na(deaths_excess) & !is.na(person_years)]$person_years)
  )
  data_cols_ext <- c(data_cols, "deaths_excess","model_type")
  data_ext <- validate_em_data(
    data_ext,
    id_cols = id_cols,
    data_cols = data_cols_ext
  )
}
# run the following checks on any data
data_cols <- c(data_cols, "deaths")
data[, ihme_loc_id := NULL]

check_dt <- data[!year_start %in% c(min(data[,year_start]),2022)]
if (nrow(check_dt > 0)) {
  assertable::assert_ids(
    data = data[time_unit=="week"& !year_start == min(data[,year_start])],
    id_vars = list(
      "year_start" = (min(data[,year_start])+1):max(data[,year_start]),
      "time_start" = 1:52,
      "sex" = "all",
      "age_name" = "0 to 125"),
    warn_only = T
  )
  assertable::assert_ids(
    data = data[time_unit=="month" & !year_start == min(data[,year_start])],
    id_vars = list(
      "year_start" = (min(data[,year_start])+1):max(data[,year_start]),
      "time_start" = 1:12,
      "sex" = "all",
      "age_name" = "0 to 125")
  )
}

# save
if(process_em){
  readr::write_csv(
    data_ext, fs::path()
  )
}

if(ihme_loc != "Mumbai"){
  readr::write_csv(
    data, fs::path()
  )
}
