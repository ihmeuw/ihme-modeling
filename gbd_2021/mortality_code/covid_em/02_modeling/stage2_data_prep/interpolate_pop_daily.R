
library(argparse)
library(data.table)
library(zoo)

source("")

parser <- ArgumentParser()
parser$add_argument("--task_map_path", type = "character", required = TRUE,
                    help = "Path to array job task map file")
parser$add_argument("--date", type = "character", required = TRUE,
                    help = "Date for specifying folder")
args <- parser$parse_args()

task_map_path <- args$task_map_path
date <- args$date

SGE_TASK_ID <- Sys.getenv("SLURM_ARRAY_TASK_ID")

loc <- fread(task_map_path)[task_id == SGE_TASK_ID, ihme_loc_id]

message(paste0("Working on: ", loc))

pop_input <- fread(paste0())
id_cols <- c("location_id", "ihme_loc_id", "sex", "age_name", "year_start", "type")

gbd_year_start <- 2000
gbd_year_end <- 2022
covid_team_year_start <- 2019
covid_team_year_end <- 2022

dt <- copy(pop_input[ihme_loc_id == loc])
dt_orig <- copy(pop_input[ihme_loc_id == loc])

dt[, time_units := ifelse(year_start %in% c(2000, 2004, 2008, 2012, 2016, 2020), 366, 365)]
dt[, time_start := ifelse(year_start %in% c(2000, 2004, 2008, 2012, 2016, 2020), 183, 182)]

dt2 <- data.table()

for (loc in unique(dt$ihme_loc_id)) {

  dt_temp <- dt[ihme_loc_id == loc]

  # create 'time_id' which is continuous across years
  dt_temp[, time_id := time_start]
  for (i in 2:nrow(dt_temp)) {
    time_id_prev <- dt_temp[i - 1,]$time_id
    dt_temp[i, time_id := time_id_prev + time_units]
  }

  dt2 <- rbind(dt2, dt_temp)

}

dt <- copy(dt2)

n_time_units_total <- max(dt$time_id) + 183

# save july 1 time ids
july_1_time_ids_dt <- unique(dt[, c("time_id", "time_start", "year_start")])
july_1_time_ids <- unique(dt$time_id)

# modified 'id_cols'
new_id_cols <- c("time_id", setdiff(id_cols, c("time_start", "year_start")))

# interpolate to get detailed time units
dt[, population := log(population)]
setkeyv(dt, new_id_cols)
dt <- demUtils::interpolate(
  dt = dt,
  id_cols = new_id_cols,
  interpolate_col = "time_id",
  value_col = "population",
  interpolate_vals = 1:n_time_units_total,
  rule = 1 
)
dt[, population := exp(population)]
dt <- dt[!is.na(population)]

# extrapolate using rate of change to get population through end of time period
if (n_time_units_total > max(dt$time_id)) {
  dt <- demUtils::extrapolate(
    dt = dt,
    id_cols = new_id_cols,
    extrapolate_col = "time_id",
    value_col = "population",
    extrapolate_vals = min(dt$time_id):n_time_units_total,
    method = "rate_of_change",
    n_groups_fit = 159
  )
}

# format back to year_start and time_start
dt <- merge(
  dt,
  july_1_time_ids_dt,
  by = "time_id",
  all.x = TRUE
)

dt2 <- data.table()

for (loc in unique(dt$ihme_loc_id)) {

  dt_temp <- dt[ihme_loc_id == loc]

  for (time_id_july_1 in july_1_time_ids) {

    year <- dt_temp[time_id == time_id_july_1]$year_start
    time <- dt_temp[time_id == time_id_july_1]$time_id

    num_days <- ifelse(year %in% c(2000, 2004, 2008, 2012, 2016, 2020), 366, 365)
    midpoint <- ifelse(year %in% c(2000, 2004, 2008, 2012, 2016, 2020), 183, 182)

    for (n in 1:(midpoint - 1)) {

      dt_temp[time_id == time_id_july_1 - n, year_start := year]
      dt_temp[time_id == time_id_july_1 - n, time_start := midpoint - n]

    }

    for (n in (midpoint + 1):num_days) {

      dt_temp[time_id == time_id_july_1 + (n - midpoint), year_start := year]
      dt_temp[time_id == time_id_july_1 + (n - midpoint), time_start := n]

    }

  }

  dt2 <- rbind(dt2, dt_temp)

}

# convert to dates
dt2[, date := as.Date(time_start, origin = paste0(year_start - 1, "-12-31"))]

# get person-years
dt2[, person_years := ifelse(year_start %in% c(2000, 2004, 2008, 2012, 2016, 2020), population / 366, population / 365)]

# get total person-years by year
dt2[, person_years_annual := sum(person_years), by = list(location_id, ihme_loc_id, sex, age_name, year_start)]

# add gbd population
dt_orig <- dt_orig[, c("ihme_loc_id", "year_start", "population")]
setnames(dt_orig, "population", "gbd_population")

dt2 <- merge(
  dt2,
  dt_orig,
  by = c("ihme_loc_id", "year_start"),
  all.x = TRUE
)

dt2[year_start == min(year_start), gbd_population := gbd_population / 2]

# scale
dt2[, person_years_new := (gbd_population * person_years) / person_years_annual]
dt2[, `:=` (population = NULL, gbd_population = NULL, person_years = NULL, person_years_annual = NULL)]
setnames(dt2, "person_years_new", "person_years")

# clean days
dt_days <- copy(dt2)
dt_days[, `:=` (time_unit = "day", date = NULL, time_id = NULL)]

# convert
dt_add_weeks_months <- add_weeks_months(
  dt = dt2,
  gbd_year = 2020,
  data_code_dir = "",
  type = "pop"
)

# convert to weeks
dt_weeks <- dt_add_weeks_months[time_unit == "week"]
dt_weeks_agg <- dt_weeks[, .(person_years = sum(person_years)), by = list(location_id, ihme_loc_id, sex, age_name, time_unit, type, time_start, year_start)]

# convert to months
dt_months <- dt_add_weeks_months[time_unit == "month"]
dt_months_agg <- dt_months[, .(person_years = sum(person_years)), by = list(location_id, ihme_loc_id, sex, age_name, time_unit, type, time_start, year_start)]

# combine
final <- rbind(dt_days, dt_weeks_agg, dt_months_agg)
if (unique(final$type) == "covid_team") final <- final[year_start %in% covid_team_year_start:covid_team_year_end]
if (unique(final$type) == "gbd") final <- final[year_start %in% gbd_year_start:gbd_year_end]

readr::write_csv(final, paste0())
