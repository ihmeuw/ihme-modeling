
# Meta --------------------------------------------------------------------

# Prepare excess mortality using observed deaths from the COVID-em pipeline
# and expected mortality from the "no COVID-era VR" no-shock GBD envelope

# excess = observed_covidem - expected_gbd


# Load packages -----------------------------------------------------------

library(data.table)
library(furrr)


# Set parameters ----------------------------------------------------------

plan("multisession")

run_id_data <- "VERSION"
run_id_s1 <- "VERSION"
run_id_s3 <- "VERSION"
run_id_nsdn <- 612

covid_years <- 2020:2021
locs_non_gbd <- 99999
n_draws <- 100

USER <- Sys.getenv("USER")
current_date <- Sys.Date()

dir_code_model <- here::here()
dir_code_processing <- fs::path("FILEPATH")
dir_covid_plots <- "FILEPATH"
dir_covid_processing <- "FILEPATH"
dir_covid_estimates <- "FILEPATH"
dir_finalizer <- "FILEPATH"

date_method_choices <- "DATE"

run_id_finalizer <- demInternal::get_parent_child(
  "no shock death number estimate",
  run_id = run_id_nsdn,
  lineage_type = "child"
)[child_process_name == "with shock death number estimate", child_run_id]

draw_samples <- withr::with_seed(9876, sample(0:999, n_draws))
draw_samples_p1 <- draw_samples + 1

id_cols_s1 <- c("location_id", "ihme_loc_id", "source", "year_start", "time_start", "time_unit")


# Load helper functions ---------------------------------------------------

source(fs::path(dir_code_model, "functions/R/load_summaries_and_data.R"))
source(fs::path(dir_code_processing, "functions/R/adj_weeks_for_agg.R"))


# load maps ---------------------------------------------------------------

map_locs_gbd <- demInternal::get_locations(gbd_year = 2021)
map_locs_covid <- demInternal::get_locations(gbd_year = 2020, location_set_name = "COVID-19 modeling")

map_loc_ita_99999 <- data.table(
  location_id = 99999,
  ihme_loc_id = "ITA_99999",
  location_name = "Provincia autonoma di Bolzano + Provincia autonoma di Trento",
  region_name = "Western Europe",
  super_region_name = "High-income",
  level = 4,
  parent_id = 86,
  is_estimate = 1,
  path_to_top_parent = "1,64,73,86,99999"
)

map_locs <- rbind(
  map_locs_gbd,
  map_locs_covid[ihme_loc_id %like% "CAN_|ESP_|DEU_"],
  map_loc_ita_99999,
  fill = TRUE
)


# Load data ---------------------------------------------------------------

dt_vr <- fread(fs::path("FILEPATH"))
dt_loc_week_types <- fread(fs::path(dir_code_processing, "location_week_types.csv"))
dt_method_choices <- fread(fs::path(dir_covid_plots, date_method_choices, paste0("em_method_choices-", date_method_choices, ".csv")))
dt_lka_observed <- fread("FILEPATH")

dt_results_s3 <- fread(fs::path(
  dir_covid_estimates, run_id_s3, "outputs", paste0("model_prediction-ratios-draws-", run_id_s3),
  ext = "csv"
))

ds_s1_draws <- arrow::open_dataset(fs::path(dir_covid_estimates, run_id_s1, "outputs/draws"))

# Need an additional year of data to do year start/end adjustments
s1_load_years <- c(min(covid_years) - 1, covid_years, max(covid_years) + 1)

dt_s1_draws <- ds_s1_draws |>
  dplyr::filter(
    model_type == "ensemble_long",
    process_stage == "final",
    sex == "all",
    age_start == 0,
    age_end == 125,
    draw %in% draw_samples_p1,
    year_start %in% s1_load_years
  ) |>
  dplyr::select(location_id, year_start, time_start, time_unit, draw, death_rate_excess = value) |>
  dplyr::arrange(location_id, time_unit, year_start, time_start, draw) |>
  dplyr::collect() |>
  setDT()

dt_s1_summary <- load_summaries_and_data(
  run_ids = run_id_s1,
  gbd_year = 2021,
  load_model_type = "ensemble_long",
  base_dir = dir_covid_estimates
)

dt_loc_years <- purrr::reduce(
  list(
    unique(dt_vr[year_id %in% covid_years, .(yr = year_id, loc = location_id)]),
    unique(dt_s1_summary[year_start %in% covid_years, .(yr = year_start, loc = location_id)]),
    dt_lka_observed[year_start %in% covid_years, .(yr = year_start, loc = location_id)]
  ),
  funion
)

dt_nsdn <-
  dt_loc_years[!loc %in% locs_non_gbd] |>
  future_pmap_dfr(function(yr, loc) {
    file_name <- paste0("combined_env_aggregated_", yr, ".h5")
    file_path <- fs::path(dir_finalizer, run_id_finalizer, "env_no_shock", file_name)
    dt <- setDT(mortcore::load_hdf(file_path, by_val = loc))
    dt[sex_id == 3 & age_group_id == 22 & draw %in% draw_samples]
  })


# Prep data ---------------------------------------------------------------

## Stage 1 excess ----

dt_s1_draws[
  dt_s1_summary,
  `:=`(
    deaths_observed = i.deaths_observed,
    deaths_expected = i.deaths_observed - (x.death_rate_excess * i.person_years),
    person_years = i.person_years,
    source = i.source
  ),
  on = .(location_id, year_start, time_start, time_unit)
]

dt_s1_draws[, draw := match(draw, draw_samples_p1)]
dt_s1_draws[, death_rate_excess := NULL]
dt_s1_draws[map_locs, ihme_loc_id := i.ihme_loc_id, on = "location_id"]

dt_s1_draws_adj <-
  CJ(
    yr = covid_years,
    measure = c("deaths_observed", "deaths_expected")
  ) |>
  purrr::pmap_dfr(function(yr, measure) {

    dt_adj <- adj_weeks_for_agg(
      dt = dt_s1_draws[, c(..id_cols_s1, "draw", "person_years", ..measure)],
      id_cols = c(setdiff(id_cols_s1, c("year_start", "time_start")), "draw"),
      measure = measure,
      year = yr,
      location_week_types = dt_loc_week_types
    )[year_start == yr, -"person_years"]

    setnames(dt_adj, measure, "value")
    dt_adj[, variable := measure]

  })

stopifnot(dt_s1_draws_adj[, .N, by = .(location_id, year_start, time_start, time_unit, variable)][N != n_draws, .N] == 0)

dt_s1_draws_annual <-
  dt_s1_draws_adj[
    j = .(
      value = sum(value),
      n_points = uniqueN(time_start),
      source = paste(unique(source), collapse = "; ")
    ),
    by = .(location_id, ihme_loc_id, year_id = year_start, draw, time_unit, variable)
  ] |>
  dcast(...~variable, value.var = "value")

unique(dt_s1_draws_annual[source %like% ";", -c("draw", "deaths_expected")])


# Get incomplete Stage 1 timeseries ---------------------------------------

dt_incomplete <- dt_s1_draws_annual[
  (time_unit == "month" & n_points != 12) | (time_unit == "week" & !n_points %in% 52:53),
  .(ihme_loc_id, year_id, time_unit, n_points)
] |> unique()

dt_incomplete <- rbind(
  dt_incomplete,
  data.table(
    ihme_loc_id = c("MNG", "ARM", "THA", "THA"),
    year_id =     c(2020,  2020,  2020,  2021)
  ),
  fill = TRUE,
  use.names = TRUE
)
setorderv(dt_incomplete, c("ihme_loc_id", "year_id"))

dt_incomplete[map_locs, location_id := i.location_id, on = "ihme_loc_id"]

## Calculate extension adjustments for some incomplete locations ----

ihme_locs_extend <- c("JAM", "PAN")

dt_incomplete_extend <- dt_incomplete[ihme_loc_id %in% ihme_locs_extend]
dt_incomplete <- dt_incomplete[!ihme_loc_id %in% ihme_locs_extend]

dt_person_years_extend <-
  unique(dt_incomplete_extend$ihme_loc_id) |>
  rlang::set_names() |>
  lapply(\(x) fread(fs::path(dir_covid_processing, run_id_data, "outputs", x, ext = "csv"))) |>
  rbindlist(idcol = "ihme_loc_id") |>
  (\(dt) dt[
    dt_incomplete_extend,
    .(location_id, ihme_loc_id, year_id, time_unit, time_start, n_points, person_years),
    on = .(ihme_loc_id, year_start = year_id, time_unit)
  ])() |>
  (\(dt) dt[
    j = .(
      person_years_available = sum(person_years * (time_start <= n_points)),
      person_years_full = sum(person_years)
    ),
    by = .(location_id, ihme_loc_id, year_id, time_unit)
  ])()

dt_person_years_extend[, adjust_ratio := person_years_full / person_years_available]


# Combine data --------------------------------------------------------------

dt_excess <- dt_nsdn[j = .(
  location_id,
  year_id,
  draw = match(draw, draw_samples),
  deaths_expected_gbd = deaths
)]

dt_excess[
  dt_vr,
  `:=`(
    deaths_observed_gbd = i.deaths_adj,
    source_gbd = paste0("(nid:", i.nid, ") ", i.detailed_source)
  ),
  on = .(location_id, year_id)
]

fsetdiff(
  unique(dt_s1_draws_annual[, .(location_id, year_id)]),
  unique(dt_excess[, .(location_id, year_id)])
)

dt_excess[
  dt_s1_draws_annual,
  `:=`(
    deaths_expected_covidem = i.deaths_expected,
    deaths_observed_covidem = i.deaths_observed,
    source_covidem = i.source
  ),
  on = .(location_id, year_id, draw)
]

dt_excess_non_gbd <-
  dt_s1_draws_annual[
    location_id %in% locs_non_gbd,
    .(
      location_id,
      year_id,
      draw,
      deaths_expected_covidem = deaths_expected,
      deaths_observed_covidem = deaths_observed,
      source_covidem = source
    )
  ]

dt_excess <- rbind(
  dt_excess,
  dt_excess_non_gbd,
  use.names = TRUE,
  fill = TRUE
)

setorderv(dt_excess, c("location_id", "year_id", "draw"))

dt_excess[, deaths_expected_avg := (deaths_expected_gbd + deaths_expected_covidem) / 2]

dt_excess2 <- copy(dt_excess)

dt_excess2[, `:=`(
  deaths_excess_covidem = deaths_observed_covidem - deaths_expected_covidem,
  deaths_excess_gbd = deaths_observed_gbd - deaths_expected_gbd
)]


# Calculate excess --------------------------------------------------------

dt_excess[
  dt_method_choices,
  `:=`(
    method_expected = i.expected,
    method_observed = i.observed
  ),
  on = "location_id"
]

dt_excess[
  (location_id == 35 & year_id == 2021) | (location_id %in% 35498:35499 & year_id == 2020),
  `:=`(
    method_expected = "GBD",
    method_observed = "GBD"
  )
]

dt_excess[method_expected == "EM",  deaths_expected_final := deaths_expected_covidem]
dt_excess[method_expected == "GBD", deaths_expected_final := deaths_expected_gbd]
dt_excess[method_expected == "AVG", deaths_expected_final := deaths_expected_avg]
dt_excess[
  is.na(deaths_expected_covidem),
  `:=`(
    method_expected = "GBD",
    deaths_expected_final = deaths_expected_gbd
  )
]

dt_excess[
  method_observed == "EM",
  `:=`(
    deaths_observed_final = deaths_observed_covidem,
    source_observed_final = source_covidem
  )
]
dt_excess[
  method_observed == "GBD",
  `:=`(
    deaths_observed_final = deaths_observed_gbd,
    source_observed_final = source_gbd
  )
]
dt_excess[
  method_observed == "GBD" & is.na(deaths_observed_gbd),
  `:=`(
    deaths_observed_final = deaths_observed_covidem,
    source_observed_final = source_covidem,
    method_observed = "EM"
  )
]

# Need to adjust 2021 COVID-em observed deaths upward by the ratio of
# gbd_2020/em_2020. This will be a single value because observed deaths don't
# have draws

can_2021_em_adj <- dt_excess[
  location_id == 101 & year_id == 2020,
  unique(deaths_observed_gbd / deaths_observed_covidem)
]

dt_excess[
  location_id == 101 & year_id == 2021,
  deaths_observed_final := deaths_observed_covidem * can_2021_em_adj
]

## Special locations ----

# LKA
dt_excess[
  dt_lka_observed,
  `:=`(
    deaths_observed_final = i.deaths,
    deaths_expected_final = x.deaths_expected_gbd
  ),
  on = .(location_id, year_id = year_start)
]

## Use GBD when EM excess is incomplete ----

dt_complete_gbd <- dt_excess[dt_incomplete, on = .(location_id, year_id)][
  !is.na(deaths_observed_gbd) & draw == 1, .(location_id, year_id)
]

dt_excess[
  dt_complete_gbd,
  `:=`(
    method_expected = "GBD",
    method_observed = "GBD",
    deaths_expected_final = deaths_expected_gbd,
    deaths_observed_final = deaths_observed_gbd,
    source_observed_final = source_gbd
  ),
  on = .(location_id, year_id)
]

dt_incomplete <- dt_incomplete[!dt_complete_gbd, on = .(location_id, year_id)]


## Final results ----

dt_excess_final <- dt_excess[
  j = .(
    location_id, year_id, draw,
    deaths_observed = deaths_observed_final,
    deaths_expected = deaths_expected_final,
    deaths_excess = deaths_observed_final - deaths_expected_final
  )
]


# Extend some incomplete location-years -----------------------------------

dt_excess_final[
  dt_person_years_extend,
  `:=`(
    deaths_observed = deaths_observed * i.adjust_ratio,
    deaths_excess = deaths_excess * i.adjust_ratio
  ),
  on = .(location_id, year_id)
]

dt_excess2[
  dt_person_years_extend,
  `:=`(
    deaths_observed_covidem = deaths_observed_covidem * i.adjust_ratio,
    deaths_excess_covidem = deaths_excess_covidem * i.adjust_ratio
  ),
  on = .(location_id, year_id)
]


# Drop incomplete ---------------------------------------------------------

dt_excess_complete <- dt_excess_final[
  !dt_incomplete,
  on = .(location_id, year_id)
]

dt_excess2_complete <- dt_excess2[
  !dt_incomplete,
  on = .(location_id, year_id)
]

tmp <- map_locs[, 1:3][
  dt_excess_complete[is.na(deaths_excess) & draw == 1],
  on = "location_id"
]

tmp[!unique(dt_s1_draws_annual[, .(location_id, year_id)]), on = .(location_id, year_id)]


# Fill missing locations --------------------------------------------------

dt_excess_full <- dt_results_s3[
  data_frame != "pred_all_draw" & level > 2,
  j = .(
    location_id,
    year_id = as.integer(stringr::str_match(data_frame, "pred_(.*?)_draw")[, 2]),
    draw,
    deaths_excess
  )
]

dt_excess_full[
  dt_excess_complete,
  deaths_excess := i.deaths_excess,
  on = .(location_id, year_id, draw)
]


# Summarize ---------------------------------------------------------------

dt_excess_choices <- dt_excess[
  draw == 1,
  j = .(
    location_id, year_id,
    choice_expected = method_expected,
    choice_observed = method_observed,
    source_observed = source_observed_final
  )
]

dt_excess_choices[
  map_locs,
  `:=`(
    ihme_loc_id = i.ihme_loc_id,
    location_name = i.location_name
  ),
  on = "location_id"
]

setcolorder(dt_excess_choices, c("location_id", "ihme_loc_id", "location_name"))

dt_excess_complete_summary <- dt_excess_complete[
  j = .(
    n_negative_draws = sum(deaths_excess < 0),
    mean_deaths_observed = mean(deaths_observed),
    mean_deaths_expected = mean(deaths_expected),
    mean_deaths_excess = mean(deaths_excess)
  ),
  by = .(location_id, year_id)
]

dt_excess_full_summary <- dt_excess_full[
  j = .(
    n_negative_draws = sum(deaths_excess < 0),
    mean_deaths_excess = mean(deaths_excess)
  ),
  by = .(location_id, year_id)
]

dt_excess2_summary <- dt_excess2_complete[
  j = .(
    deaths_excess_covidem = mean(deaths_excess_covidem),
    deaths_excess_gbd = mean(deaths_excess_gbd)
  ),
  by = .(location_id, year_id)
]


# Save --------------------------------------------------------------------

dir_out <- fs::path(dir_covid_plots, current_date)
file_name_base <- glue::glue("gbd_excess-s1_{run_id_s1}-nsdn_{run_id_nsdn}")

saveRDS(
  dt_excess_complete[location_id != 95],
  fs::path(dir_out, paste0(file_name_base, "-covariate_input"), ext = "RDS")
)

readr::write_csv(
  dt_excess_complete_summary[location_id != 95],
  fs::path(dir_out, paste0(file_name_base, "-covariate_input-mean"), ext = "csv")
)

saveRDS(
  dt_excess_full,
  fs::path(dir_out, glue::glue("{file_name_base}-s3_{substr(run_id_s3, 4, 100)}"), ext = "RDS")
)

readr::write_csv(
  dt_excess_full_summary,
  fs::path(dir_out, glue::glue("{file_name_base}-s3_{substr(run_id_s3, 4, 100)}-mean"), ext = "csv")
)

readr::write_csv(
  dt_excess_choices[location_id != 95],
  fs::path(dir_out, paste0(file_name_base, "-sources"), ext = "csv")
)

readr::write_csv(
  dt_excess2_summary[location_id != 95],
  fs::path(dir_out, glue::glue("gbd_em_excess-s1_{run_id_s1}-nsdn_{run_id_nsdn}-mean.csv"))
)
