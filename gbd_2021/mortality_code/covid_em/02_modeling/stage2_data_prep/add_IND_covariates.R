# Meta -------------------------------------------------------------------------

# Description: Prep India from tabulated data
# Steps:
#   1. Loads India tabulated file
#   2. Saves so values can be appended manually
#   3. Loads new file so covariates can be aggregated and added
#   4. Saves formatted data
# Inputs:
#   * 
#   * 
# Outputs:
#   * 
# Notes:

# Load -------------------------------------------------------------------------

data_code_dir <- ""

library(assertable)
library(arrow)
library(data.table)
library(demInternal)
library(dplyr)

source(paste0("<FILEPATH>/get_covariate_estimates.R"))
source(paste0("<FILEPATH>/em_data_validation.R"))
source(paste0("<FILEPATH>/interpolate_pop.R"))
source(paste0("<FILEPATH>/pop_to_person_years.R"))


# Set up -----------------------------------------------------------------------

data_run_id <- ""

current_date <- ""

GBD <- T

output_dir <- ""
daily_dir <- paste0()
main_dir <- paste0()
base_dir <- ""

# get config
config <- config::get(
  file = paste0(),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# get stage 1 output locations
process_locations <- fread(paste0())
process_locations <- process_locations[!ihme_loc_id == "Mumbai"]
process_locations$location_id <- as.numeric(process_locations$location_id)

# location mapping
gbd_year <- 2020
locs <- demInternal::get_locations(gbd_year = gbd_year)
covid_loc_map <- demInternal::get_locations(gbd_year = gbd_year, location_set_name = "COVID-19 modeling")
covid_only_locs <- covid_loc_map[!location_id %in% unique(locs$location_id)]
locs <- rbind(locs, covid_only_locs)

ind_4841_loc_ids <- unique(locs[parent_id==4841, location_id])
ind_4842_loc_ids <- unique(locs[parent_id==4842, location_id])
ind_4843_loc_ids <- unique(locs[parent_id==4843, location_id])
ind_4844_loc_ids <- unique(locs[parent_id==4844, location_id])
ind_4846_loc_ids <- unique(locs[parent_id==4846, location_id])
ind_4849_loc_ids <- unique(locs[parent_id==4849, location_id])
ind_4850_loc_ids <- unique(locs[parent_id==4850, location_id])
ind_4851_loc_ids <- unique(locs[parent_id==4851, location_id])
ind_4852_loc_ids <- unique(locs[parent_id==4852, location_id])
ind_4853_loc_ids <- unique(locs[parent_id==4853, location_id])
ind_4854_loc_ids <- unique(locs[parent_id==4854, location_id])
ind_4855_loc_ids <- unique(locs[parent_id==4855, location_id])
ind_4856_loc_ids <- unique(locs[parent_id==4856, location_id])
ind_4857_loc_ids <- unique(locs[parent_id==4857, location_id])
ind_4859_loc_ids <- unique(locs[parent_id==4859, location_id])
ind_4860_loc_ids <- unique(locs[parent_id==4860, location_id])
ind_4861_loc_ids <- unique(locs[parent_id==4861, location_id])
ind_4862_loc_ids <- unique(locs[parent_id==4862, location_id])
ind_4863_loc_ids <- unique(locs[parent_id==4863, location_id])
ind_4864_loc_ids <- unique(locs[parent_id==4864, location_id])
ind_4865_loc_ids <- unique(locs[parent_id==4865, location_id])
ind_4867_loc_ids <- unique(locs[parent_id==4867, location_id])
ind_4868_loc_ids <- unique(locs[parent_id==4868, location_id])
ind_4869_loc_ids <- unique(locs[parent_id==4869, location_id])
ind_4870_loc_ids <- unique(locs[parent_id==4870, location_id])
ind_4871_loc_ids <- unique(locs[parent_id==4871, location_id])
ind_4872_loc_ids <- unique(locs[parent_id==4872, location_id])
ind_4873_loc_ids <- unique(locs[parent_id==4873, location_id])
ind_4874_loc_ids <- unique(locs[parent_id==4874, location_id])
ind_4875_loc_ids <- unique(locs[parent_id==4875, location_id])
ind_44538_loc_ids <- unique(locs[parent_id==44538, location_id])


# Add Covariates ---------------------------------------------------------------

ind <- fread("")

# only need rows with deaths excess
ind <- ind[!is.na(excess_deaths)]

# drop cities and old regions
ind <- ind[!(location_name %in% c("Mumbai","Hyderabad","Kolkata"))]
ind <- ind[!(ihme_loc_id == "IND_4870" & year_start == 2021)]
ind <- ind[!(ihme_loc_id == "IND_4857" & year_start == 2021)]

# VR completeness
ind[completeness_avg > 1, completeness_avg := 1]
ind[, excess_deaths := excess_deaths / completeness_avg]

# convert to dates
ind[, ':=' (date_start = as.Date(paste0(month_start,'-',day_start,'-',year_start), format = "%m-%d-%Y"),
            date_end = as.Date(paste0(month_end,'-',day_end,'-',year_end), format = "%m-%d-%Y"))]

# add location id
ind <- merge(
  process_locations[, c("ihme_loc_id","location_id")],
  ind,
  by = "ihme_loc_id",
  all.y = T
)

ind$month_start <- as.numeric(ind$month_start)
ind$month_end <- as.numeric(ind$month_end)

# get list of dates by location
keep_dates_sep <- unique(ind[, c("location_id", "date_start","date_end")])
keep_dates <- data.table()
for(loc_time in 1:nrow(keep_dates_sep)){
  temp <- keep_dates_sep[loc_time]
  temp <- as.data.table(melt(
    temp,
    id.vars = "location_id",
    value.name = "date"
  ))
  temp <- temp[, -c("variable")]
  temp$date <- as.IDate(temp$date)
  temp <- fill_missing_dates(temp)
  temp[, ':=' (time_start = month(date), year_start = year(date))]
  temp <- unique(temp[, c("location_id","time_start","year_start")])
  keep_dates <- rbind(keep_dates, temp)
}

keep_dates$location_id <- as.numeric(keep_dates$location_id)
keep_dates[, time_unit := "month"]

# combine deaths excess time period
ind <- ind[, .(excess_deaths = sum(excess_deaths),
               date_start_1 = min(date_start),
               date_start_2 = max(date_start),
               date_end_1 = min(date_end),
               date_end_2 = max(date_end)), by = c("ihme_loc_id","location_id","location_name")]
ind$location_id <- as.numeric(ind$location_id)


## Person Years ## -------------------------------------------------------------

person_years <- fread("")

# subset
person_years <- merge(
  person_years,
  keep_dates,
  by = c("location_id","time_start","year_start","time_unit"),
  all.y = T
)

# aggregate
person_years <- person_years[, .(person_years = sum(person_years)), by = "location_id"]

# combine
ind <- merge(
  ind,
  person_years[, c("location_id","person_years")],
  by = "location_id",
  all.x = T
)


## Crude Death Rate ## ---------------------------------------------------------

cdr <- fread("")

# combine
ind <- merge(
  ind,
  cdr[, c("location_id","crude_death_rate")],
  by = "location_id",
  all.x = T
)


## Crude Death Rate SD ## ------------------------------------------------------

## 1990 ##
cdr_sd_1990 <- fread("")
setnames(cdr_sd_1990, "crude_death_rate_sd", "crude_death_rate_sd_1990")

# combine
ind <- merge(
  ind,
  cdr_sd_1990[, c("location_id","crude_death_rate_sd_1990")],
  by = "location_id",
  all.x = T
)

## 2000 ##

cdr_sd_2000 <- fread("")
setnames(cdr_sd_2000, "crude_death_rate_sd", "crude_death_rate_sd_2000")

# combine
ind <- merge(
  ind,
  cdr_sd_2000[, c("location_id","crude_death_rate_sd_2000")],
  by = "location_id",
  all.x = T
)


## Mean Population Age ## ------------------------------------------------------

mean_pop_age <- fread("")

# combine
ind <- merge(
  ind,
  mean_pop_age[, c("location_id","mean_pop_age")],
  by = "location_id",
  all.x = T
)


# Seroprevalence ##-------------------------------------------------------------

sero <- fread("")

sero <- melt(
  sero,
  measure.vars = patterns("draw"),
  variable.name = "draw"
)

sero_summary <- sero[, .(mean = mean(value, na.rm = T),
                         lower = quantile(value, .025, na.rm = T),
                         upper = quantile(value, .975, na.rm = T)),
                     by = c("location_id","date")]

colnames(sero_summary) <- c("location_id", "date", "mean_infections", "lower_infections", "upper_infections")
sero_summary[, daily_infections := mean_infections]

# we get national sum
change_subnat_to_nat <- function(national_ihme_loc, sero_summary) {
  national <- copy(sero_summary)
  if(national_ihme_loc == "CHN"){
    national_loc_id <- 6
  } else {
    national_loc_id <- locs[ihme_loc_id == national_ihme_loc, location_id]
    national_loc_id <- as.numeric(national_loc_id)
  }
  subnational_locs <- get(paste0(tolower(national_ihme_loc), "_loc_ids"))
  assertthat::assert_that(!national_loc_id %in% unique(sero_summary$location_id))
  missing_subnationals <- subnational_locs[!subnational_locs %in% unique(sero_summary$location_id)]
  if(length(missing_subnationals) > 0 & !national_ihme_loc=="IND"){
    stop(paste0("Error: missing subnational location for national aggregation in seroprevalence. Location:",national_ihme_loc))
  }
  national <- national[location_id %in% subnational_locs]
  national[, location_id := national_loc_id]
  return(national)
}

# add ihme_loc_id
sero <- merge(
  locs[, c("location_id", "ihme_loc_id")],
  sero_summary,
  by = "location_id",
  all.y = TRUE)
sero$date <- as.IDate(sero$date)

# lag seroprevalence by 25 days
sero <- sero[order(location_id, date)]

sero[, infections_lagged := shift(daily_infections, 25), by = "location_id"]
sero[is.na(infections_lagged), infections_lagged := 0]

# add start and end dates
sero <- merge(
  sero,
  ind[, c("location_id","date_start_1","date_start_2","date_end_1","date_end_2")],
  by = "location_id",
  all.y = T
)

# subset to value
sero <- sero[(date >= date_start_1 & date <= date_end_1) | (date >= date_start_2 & date <= date_end_2)]

# assert each location only has one value per date
# GBD location set has rural/urban that need to be aggregated by date
if (!GBD) {
  test <- as.data.table(duplicated(sero[, c("location_id", "date")]))
  if (nrow(test[V1 == TRUE]) > 0) stop("There are duplicates in the location file.")
}

# aggregate
sero <- sero[, .(cumulative_infections = sum(daily_infections),
                 cumulative_infections_lagged = sum(infections_lagged)),
             by = "location_id"]

# combine
ind <- merge(
  ind,
  sero[, c("location_id","cumulative_infections", "cumulative_infections_lagged")],
  by = "location_id",
  all.x = T
)


ind[, ci_person_years := person_years]
ind[, cumulative_infections := cumulative_infections / ci_person_years]
ind[, cumulative_infections_lagged := cumulative_infections_lagged / ci_person_years]


# Prop Pop ---------------------------------------------------------------------

prop_pop_60plus <- fread("")
setnames(prop_pop_60plus, "prop_pop_above", "prop_pop_60plus")

# combine
ind <- merge(
  ind,
  prop_pop_60plus[, c("location_id","prop_pop_60plus")],
  by = "location_id",
  all.x = T
)

prop_pop_70plus <- fread("")
setnames(prop_pop_70plus, "prop_pop_above", "prop_pop_70plus")

# combine
ind <- merge(
  ind,
  prop_pop_70plus[, c("location_id","prop_pop_70plus")],
  by = "location_id",
  all.x = T
)

prop_pop_75plus <- fread("")
setnames(prop_pop_75plus, "prop_pop_above", "prop_pop_75plus")

# combine
ind <- merge(
  ind,
  prop_pop_75plus[, c("location_id","prop_pop_75plus")],
  by = "location_id",
  all.x = T
)

prop_pop_80plus <- fread("")
setnames(prop_pop_80plus, "prop_pop_above", "prop_pop_80plus")

# combine
ind <- merge(
  ind,
  prop_pop_80plus[, c("location_id","prop_pop_80plus")],
  by = "location_id",
  all.x = T
)

prop_pop_85plus <- fread("")
setnames(prop_pop_85plus, "prop_pop_above", "prop_pop_85plus")

# combine
ind <- merge(
  ind,
  prop_pop_85plus[, c("location_id","prop_pop_85plus")],
  by = "location_id",
  all.x = T
)


# Other gbd covariates ---------------------------------------------------------

gbd_cov_folder <- ""
gbd_cov_files <- list.files(gbd_cov_folder, full.names = TRUE, pattern = "")
gbd_cov_names <- gsub("","",gbd_cov_files)

# read in and rename by filename
gbd_covs <- fread(gbd_cov_files[1])[, c("location_id","mean_value")]
setnames(gbd_covs, "mean_value", gbd_cov_names[1])
for (file in gbd_cov_files[-1]){
  cov <- fread(file)
  # re-finding cov name so they don't get misaligned
  cov_name <- gsub("","",file)
  setnames(cov, "mean_value",
           cov_name,
           skip_absent = TRUE)
  keep <- c("location_id", cov_name)
  gbd_covs <- merge(
    gbd_covs,
    cov[, ..keep],
    by = "location_id",
    all = T
  )
}

# combine
ind <- merge(
  ind,
  gbd_covs,
  by = "location_id",
  all.x = T
)


# Mobility ---------------------------------------------------------------------

# Using Pre-SEIR mobility
mobility <- fread("")
mobility <- mobility[, c("location_id", "date", "mean")]

# drop forcasting rows
mobility <- mobility[!is.na(mean)]
mobility <- mobility[location_id %in% unique(ind$location_id)]

# mobility
setnames(mobility, "mean", "mobility_reference")

# replace ihme_loc_id
mobility <- merge(
  mobility,
  process_locations[,c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

# lag mobility
mobility <- mobility[order(date)]
mobility[, mobility_lagged := shift(mobility_reference, covariate_lag_days), by = "location_id"]
mobility[is.na(mobility_lagged), mobility_lagged := 0]

# months
mobility_m <- copy(mobility)
mobility_m[, ":=" (year_start = lubridate::year(date),
                   time_start = lubridate::month(date),
                   time_unit = 'month')]
mobility_m <- mobility_m[,
                         list(mobility_reference = mean(mobility_reference, na.rm = T),
                              mobility_lagged = mean(mobility_lagged, na.rm = T)),
                         by = setdiff(id_cols, c("age_start", "age_end", "sex"))
                         ]
mobility <- copy(mobility_m)

# outlier by week and month
mobility <- merge(
  mobility,
  keep_dates,
  by = c("location_id","year_start","time_start"),
  all.y = T
)

mobility <- mobility[, .(mobility_reference = mean(mobility_reference, na.rm = T),
                         mobility_lagged = mean(mobility_lagged, na.rm = T)),
                     by = "location_id"]

# combine
ind <- merge(
  ind,
  mobility,
  by = "location_id",
  all.x = T
)


# IDR --------------------------------------------------------------------------

daily_covid <- fread(paste0(main_dir, ""))

if (GBD) {
  idr <- fread("")
} else {
  idr <- setDT(read_parquet(""))
}
# format long by draw rather than wide
idr <- melt(idr, measure.vars = patterns("draw"), variable.name = "draw")
idr[, draw := gsub("draw_", "", draw)]
setnames(idr, "value", "idr_reference")
idr <- idr[!is.na(idr_reference)]

# add start and end dates
idr <- merge(
  idr,
  ind[, c("location_id","date_start_1","date_start_2","date_end_1","date_end_2")],
  by = "location_id",
  all.y = T
)
# subset to dates
idr <- idr[(date >= date_start_1 & date <= date_end_1) | (date >= date_start_2 & date <= date_end_2)]

idr <- idr[, .(idr_reference = mean(idr_reference, na.rm = T)),
           by = c("location_id","date")]
idr$date <- as.IDate(idr$date)

# add ihme_loc_id
idr <- merge(idr, locs[, c("location_id","ihme_loc_id")], by = "location_id")

# calc covid incidence
idr <- merge(idr,
             daily_covid,
             by = c("location_id", "date"),
             all.x = TRUE
)
idr[, covid_incidence := (1 / idr_reference) * daily_cases]
idr[covid_incidence == 0, covid_incidence := 0.01]

# subset to ind
idr <- idr[location_id %in% unique(ind$location_id)]

# Filling in with 0.01 incidence
idr[is.na(covid_incidence), covid_incidence := 0.1]

# lag idr
idr <- idr[order(location_id, date)]
idr[, idr_lagged := shift(idr_reference, covariate_lag_days), by = "location_id"]
idr[is.na(idr_lagged), idr_lagged := 0]

# get month/year from date
idr[, ':=' (time_start = lubridate::month(date), year_start = lubridate::year(date), time_unit = "month")]
idr[, c("time_start", "year_start") := lapply(.SD, as.numeric), .SDcols = c("time_start", "year_start")]

idr <- idr[, .(idr_reference = mean(idr_reference, na.rm = T),
               idr_lagged = mean(idr_lagged, na.rm = T)),
           by = "location_id"]

# combine
ind <- merge(
  ind,
  idr,
  by = "location_id",
  all.x = T
)


# Covid ------------------------------------------------------------------------

# load data
covid_raw <- fread("")
covid_raw <- covid_raw[location_id %in% unique(ind$location_id)]

# Fill dates between min and max for each location
covid_raw <- covid_raw[!is.na(Deaths)]
setnames(covid_raw, "Date", "date")
covid <- fill_missing_dates(covid_raw)

# check squareness
covid <- check_square(covid)

# add start and end dates
covid <- merge(
  covid,
  ind[, c("location_id","date_start_1","date_start_2","date_end_1","date_end_2")],
  by = "location_id",
  all.y = T
)
# subset to dates
covid <- covid[(date >= date_start_1 & date <= date_end_1) | (date >= date_start_2 & date <= date_end_2)]

# add ihme loc id
covid <- merge(
  covid,
  locs[,c("location_id","ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)
unique(covid[is.na(ihme_loc_id), "location_name"])

# deaths are cumulative -- find weekly
covid <- covid[order(date)]
covid <- covid %>%
  dplyr::group_by(location_id) %>%
  tidyr::fill(Deaths, .direction = "down") %>%
  dplyr::ungroup() %>% setDT

# get daily deaths then sum to weekly
covid[, daily_deaths := Deaths - shift(Deaths), by = "location_id"]

# aggregate over time unit
covid <- covid[,
               list(deaths_covid = sum(daily_deaths, na.rm = TRUE)),
               by = c("location_id")
               ]

# combine
ind <- merge(
  ind,
  covid,
  by = "location_id",
  all.x = T
)


# Others -----------------------------------------------------------------------

keep_dates[, time_unit := "month"]

other_raw <- assertable::import_files(
  filenames = list.files(paste0(), pattern = ""),
  folder = paste0()
)

other <- other_raw[location_id %in% unique(ind$location_id) &
                     time_unit == "month" &
                     sex == "all" &
                     age_name == "0 to 125"]

other <- merge(
  other,
  keep_dates,
  by = c("location_id","time_start","year_start","time_unit"),
  all.y = TRUE
)

other <- other[, .(stars = mean(stars)),
               by = "location_id"
               ]

ind <- merge(
  ind,
  other,
  by = "location_id",
  all.x = T
)


# Rates ------------------------------------------------------------------------

ind[, covid_person_years := person_years]
ind[, death_rate_person_years := person_years]
ind[, death_rate_covid := deaths_covid / person_years]
ind[, death_rate_excess := excess_deaths / person_years]


# check and save ---------------------------------------------------------------

ind[, ':=' (age_name = "0 to 125", sex = "all")]

# check
data_cols <- c("location_id","ihme_loc_id","location_name","death_rate_person_years",
               "death_rate_excess", "person_years","death_rate_covid","deaths_covid","covid_person_years","stars",
               "idr_lagged","idr_reference","mobility_reference","mobility_lagged",
               "crude_death_rate","crude_death_rate_sd_1990","crude_death_rate_sd_2000",
               "mean_pop_age","ci_person_years",
               "prop_pop_60plus","prop_pop_70plus","prop_pop_75plus",
               "prop_pop_80plus","prop_pop_85plus",
               "cumulative_infections","cumulative_infections_lagged",
               gbd_cov_names)

ind <- ind[, ..data_cols]

assert_colnames(ind, data_cols, only_colnames = TRUE)

if(GBD) {
  readr::write_csv(ind, paste0())
} else {
  readr::write_csv(ind, paste0())
}

