
# Load libraries ----------------------------------------------------------

library(data.table)


# Set parameters ----------------------------------------------------------

set.seed(9876)

current_date <- Sys.Date()

base_dir <- fs::path("FILEPATH")
dir_covid_plots <- fs::path("FILEPATH")

run_id_stage_1 <- "VERSION"

path_lags <- fs::path(dir_covid_plots, "DATE", "download_date_by_location_prepped.csv")

covid_years <- 2020:2022
month_cutoff <- 3
week_cutoff <- 9

n_draws <- 100
draw_samples <- sample(1:1000, n_draws)


# Load maps ---------------------------------------------------------------

loc_map_covid <- demInternal::get_locations(gbd_year = 2020, location_set_name = "COVID-19 modeling")

loc_map_covid <- rbind(
  loc_map_covid,
  data.table(
    location_id = 99999,
    ihme_loc_id = "ITA_99999",
    location_name = "Provincia autonoma di Bolzano + Provincia autonoma di Trento",
    region_name = "Western Europe",
    super_region_name = "High-income",
    level = 4,
    parent_id = 86
  ),
  fill = TRUE
)

loc_map_gbd <- demInternal::get_locations(gbd_year = 2020)
loc_map_all <- unique(rbind(loc_map_covid, loc_map_gbd), by = "location_id")


# Load data ---------------------------------------------------------------

dt_lags <- fread(path_lags)
ds_s1_draws <- arrow::open_dataset(fs::path(base_dir, run_id_stage_1, "outputs/draws"))


# Prep draws --------------------------------------------------------------

dt_draws <- ds_s1_draws |>
  dplyr::filter(
    model_type == "ensemble_long",
    process_stage == "final",
    sex == "all",
    age_start == 0,
    age_end == 125,
  ) |>
  dplyr::filter(draw %in% draw_samples) |>
  dplyr::filter(
    (year_start %in% covid_years) &
      !(year_start == min(covid_years) & time_unit == "month" & time_start < month_cutoff) &
      !(year_start == min(covid_years) & time_unit == "week" & time_start < week_cutoff)
  ) |>
  dplyr::select(location_id, year_start, time_start, time_unit, draw, death_rate_excess = value) |>
  dplyr::arrange(location_id, time_unit, year_start, time_start, draw) |>
  dplyr::collect() |>
  setDT()

# Renumber draws
dt_draws[, draw := seq_along(draw), by = .(location_id, year_start, time_start, time_unit)]


# Prep lags ---------------------------------------------------------------

dt_lags_prepped <- dt_lags[,.(
  location_id,
  ihme_loc_id,
  is_hmd = !is.na(HMD_lag),
  lag = fifelse(is.na(non_HMD_lag), HMD_lag, non_HMD_lag),
  time_unit = week_month,
  year_start = download_year,
  month = download_month,
  week = download_week
)]

dt_lags_prepped[
  ihme_loc_id == "UKR",
  `:=`(location_id = 50559, ihme_loc_id = "UKR_50559")
]


# Set time unit

dt_lags_prepped[
  dt_draws[draw == 1],
  time_unit_s1 := i.time_unit,
  on = "location_id"
]

dt_lags_prepped[time_unit == "", time_unit := time_unit_s1]
dt_lags_prepped[
  time_unit != time_unit_s1,
  `:=`(
    lag = fifelse(time_unit_s1 == "month", floor(lag / 4), lag * 4),
    time_unit = time_unit_s1
  )
]

dt_lags_prepped[, `:=`(
  time_start = fifelse(time_unit == "week", week, month),
  time_unit_s1 = NULL
)]

# Get max time unit (determine 52 or 53 weeks in a year)

dt_max_times <- dt_draws[
  ,
  .(max_time = max(time_start)),
  by = .(location_id, time_unit, year_start)
]

dt_max_times[
  year_start == 2021 & time_unit == "week" & max_time < 52,
  max_time := 52
]

dt_max_times[time_unit == "month", max_time := 12]

dt_max_times[, next_year := year_start + 1]

dt_lags_prepped[
  dt_max_times,
  max_time := i.max_time,
  on = .(location_id, time_unit, year_start = next_year)
]


# Don't let lags cross into previous year

dt_lags_prepped[, last_complete := time_start - lag]

dt_lags_prepped[
  last_complete < 1,
  `:=`(
    year_start = year_start - 1,
    last_complete = max_time - (lag - time_start)
  )
]


# Apply lags --------------------------------------------------------------

dt_draws[
  dt_lags_prepped,
  outlier_lags := ((year_start == i.year_start) & (time_start > i.last_complete)) | (year_start > i.year_start),
  on = .(location_id)
]


# Apply outliering --------------------------------------------------------

dt_draws[, outlier := outlier_lags]


# Outlier Western Europe heatwave
we_loc_ids <- loc_map_all[region_name == "Western Europe", location_id]

# Outlier ARM
dt_draws[
  location_id == 33 & year_start == 2020 & time_start %between% c(35, 45),
  outlier := TRUE
]

# Outlier THA prior to May 2021 (overwrite lag-based outliering)
dt_draws[
  location_id == 18,
  outlier := (year_start == 2020 | (year_start == 2021 & time_start < 5))
]

# Outlier MNG prior to Jan 2021 (overwrite lag-based outliering)
dt_draws[
  location_id == 38,
  outlier := year_start == 2020
]


# Summarize outliering ----------------------------------------------------

dt_outlier_summary <- dt_draws[draw == 1, -c("death_rate_excess", "draw")]

dt_outlier_summary[
  loc_map_all,
  `:=`(
    ihme_loc_id = i.ihme_loc_id,
    location_name = i.location_name
  ),
  on = "location_id"
]

setcolorder(dt_outlier_summary, c("location_id", "ihme_loc_id", "location_name"))
setorderv(dt_outlier_summary, c("location_id", "time_unit", "year_start", "time_start"))


# Save outputs ------------------------------------------------------------

dt_draws[, time_unit := as.factor(time_unit)]

postfix <- ""
file_name_base <- paste0("s1-", run_id_stage_1, "-prepped_outliers-")
file_name_draws <- paste0(file_name_base, n_draws, "_draws", postfix)
file_name_summary <- paste0(file_name_base, "summary", postfix)

saveRDS(
  dt_draws,
  fs::path(dir_covid_plots, current_date, file_name_draws, ext = "RDS")
)

readr::write_csv(
  dt_outlier_summary,
  fs::path(dir_covid_plots, current_date, file_name_summary, ext = "csv")
)
