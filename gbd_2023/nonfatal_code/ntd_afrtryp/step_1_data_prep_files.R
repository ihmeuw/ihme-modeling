########################################################################
# Description: Process cases, pop at risk, and coverage data for HAT.  #
#              Merge together to create model input file.              #
#                                                                      #
########################################################################

### ========================= BOILERPLATE ========================= ###

rm(list=ls())
data_root <- "FILEPATH"
cause <- "ntd_afrtryp"

## Define paths 
interms_dir <- "FILEPATH"

##	Source relevant libraries
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_location_metadata.R")

## Define Constants
release_id = ADDRESS


### ========================= MAIN EXECUTION ========================= ###

# Bundle IDs
# ADDRESS1 - Cases of HAT
# ADDRESS2 - Population at risk
# ADDRESS3 - Screening coverage


## (1) Prep case data

# Import case data
case_dt <- get_bundle_data(bundle_id = ADDRESS1 , release_id=release_id)

# Clean
case_dt <- case_dt[, .(cause_id, location_id, year_id = year_end, value_detail, cases)]
case_dt <- case_dt[value_detail != ""]

# Uganda: combine species case counts so we have just one row per location-year
case_dt <- case_dt[order(location_id, year_id, value_detail)]
case_dt[, lagged_cases := shift(cases, n=1, type="lag")] 
case_dt[location_id==190, new_total := sum(cases), by=.(location_id, year_id)] 
case_dt[location_id!=190, new_total := cases] 
case_dt <- case_dt[, .SD[.N], by=.(location_id, year_id)]

# For Uganda only:
# lagged_cases = reported gambiense cases
# cases = reported rhodesiense cases
# new_total = gambiense + rhodesiense

# For all other locations:
# new_total = cases


## (2) Prep pop at risk data

# Import risk data
risk_dt <- get_bundle_version(bundle_version = ADDRESS2)

# Clean
risk_dt <- risk_dt[, .(location_id, year_id = year_end, value_ppl_risk)][order(location_id, year_id)]


## (3) Merge cases and pop at risk by location-year
case_risk_dt <- merge(case_dt, risk_dt, by = c("location_id", "year_id"),  all = TRUE)


## (4) Impute pop at risk data for 2016-2022
# For locations with pop at risk reported in 2015, extrapolate for 2016-2022 by applying
# a 2% annual growth rate 

# Add placeholder rows for year 2019 
# Set the number of cases in 2019 to NA (data extracted thru 2018)
for (i in 1:2) {
  start_year = 2022
  case_risk_dt <- rbind(setDT(case_risk_dt),
                        case_risk_dt[year_id==2022,][, `:=` (year_id = start_year + i,
                                                             new_total=NA,
                                                             cases=NA,
                                                             lagged_cases=NA)])[order(location_id, year_id)]
}

# Create pop at risk data for 2016-2022 by applying 2% annual growth rate
for (i in 1:9) {
  lag_column_name <- paste0("lag", i)
  year_value <- 2015 + i
  case_risk_dt[, (lag_column_name) := shift(value_ppl_risk, n=1)][year_id==year_value, value_ppl_risk := 1.02*get(lag_column_name)]
  case_risk_dt[, c(lag_column_name) := NULL]
}

## (5) Prep screening coverage data

# Import coverage data
coverage_dt <- get_bundle_version(bundle_version_id = ADDRESS)

# Clean
coverage_dt <- coverage_dt[, .(location_id, year_id = year_end, value_coverage, value_ln_coverage)]


## (6) Merge coverage data onto case/risk data by location_id, year_id
dt <- merge(case_risk_dt, coverage_dt, by = c("location_id", "year_id"), all.x = TRUE)


## (7) Mozambique only has data til 2015. Need to expand rows to enable predictions
for (year in 1:9) {
  start_year = 2015
  dt <- rbind(setDT(dt), dt[location_id==184 & year_id==start_year,][,year_id := year_id + year])
}
dt <- dt[order(location_id, year_id)]

## (8) Create vars for # of reported cases of each type (gambiense, rhodesiense, total)
dt$reported_tgb <- ifelse(dt$location_id!=190,
  ifelse(dt$value_detail=="reported_gambiense",
         dt$new_total, 0),
  dt$lagged_cases)

dt$reported_tgr <- ifelse(dt$location_id!=190,
  ifelse(dt$value_detail=="reported_rhodesiense",
         dt$new_total, 0),
  dt$cases)

# year_id 2019-2022: all case totals should be set to NA (data extracted thru 2018)
dt[year_id %in% 2023:2024, `:=` (reported_tgb = NA, reported_tgr = NA)]

# year_id < 2023: set missing case totals to 0
dt[(year_id < 2023 & is.na(reported_tgb)), reported_tgb:=0]
dt[(year_id < 2023 & is.na(reported_tgr)), reported_tgr:=0]

# total reported
dt[, total_reported := reported_tgb + reported_tgr]

# drop vars
dt[, `:=`  (value_detail = NULL,
            cases = NULL,
            lagged_cases = NULL,
            new_total = NULL
            )]


## (9) Get location metadata 
loc_metadata <- get_location_metadata(location_set_id = 35,
                                      release_id=release_id)[,.(location_id, region_id, location_name)]
dt <- merge(dt, loc_metadata, by = "location_id", all.x = TRUE)


## (10) Subset to locations that will be modeled

# drop Sierra Leone:
dt <- dt[location_id != 217]

# Replace Nigeria with Delta State
dt[location_id==214, `:=` (location_id = 25327, location_name="Delta")]

# Replace Kenya with Busia
dt[location_id==180, `:=` (location_id = 35620, location_name="Busia")]


## (11) Save intermediate file
Data2Model <- dt
saveRDS(Data2Model, file = paste0("FILEPATH"))
