
# Meta --------------------------------------------------------------------

# Create population values for VR and Onemod mx estimates to be spliced into
# population estimates prior to UI generation.

# Only create population for older ages, and only for locations with a history
# of complete VR or some exceptional cases.


# Load packages -----------------------------------------------------------

library(data.table)


# Set parameters ----------------------------------------------------------

cfg <- config::get(file = fs::path_wd("population_postprocessing/config.yml"))

file_output <- glue::glue("onemod_pop-data={cfg$version$rs_data}-est={cfg$version$onemod}")
dir_output <- fs::path(cfg$dir$pop, "versioned_inputs/gbd2023")

years_data_threshold <- 20
min_age_start <- cfg$min_age_start
onemod_mx_type <- "spxmod"

id_cols_source <- c("location_id", "year_id", "sex_id", "nid", "underlying_nid")
id_cols_age <- c("location_id", "year_id", "sex_id", "age_start", "age_end")

file_name_handoff_2 <- paste0("onemod_handoff_2", "")


# Load maps ---------------------------------------------------------------

map_age_gbd <- demInternal::get_age_map(gbd_year = cfg$gbd_year, type = "gbd")
map_loc <- demInternal::get_locations(gbd_year = cfg$gbd_year, level = "estimate")

expected_age_starts <- map_age_gbd[age_start >= min_age_start, age_start]


# Load data ---------------------------------------------------------------

dt_rs_data <-
  fs::path(cfg$dir$rs_data, cfg$version$rs_data, "outputs") |>
  fs::path(file_name_handoff_2, ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::left_join(map_age_gbd, by = "age_group_id") |>
  dplyr::left_join(map_loc[, .(location_id, ihme_loc_id)], by = "location_id") |>
  dplyr::filter(
    age_start >= min_age_start,
    source_type_name == "VR" | (grepl("IND", ihme_loc_id) & source_type_name == "SRS")
  ) |>
  dplyr::select(
    dplyr::all_of(union(id_cols_age, id_cols_source)),
    ihme_loc_id, source_type_name, outlier, complete_vr, deaths_adj, mx_adj, population
  ) |>
  dplyr::relocate(ihme_loc_id, .after = location_id) |>
  dplyr::arrange(ihme_loc_id, year_id, source_type_name, nid, underlying_nid, sex_id, age_start) |>
  dplyr::collect() |>
  as.data.frame() |>
  setDT() |>
  _[
    ihme_loc_id %like% "IND" & source_type_name == "SRS",
    deaths_adj := mx_adj * population
  ] |>
  _[, c("mx_adj", "population") := NULL]

dt_onemod <-
  fs::path(cfg$dir$onemod, cfg$version$onemod, "predictions.parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::left_join(map_age_gbd, by = "age_group_id") |>
  dplyr::filter(age_start >= min_age_start) |>
  dplyr::select(dplyr::all_of(c(id_cols_age, onemod_mx_type))) |>
  dplyr::rename(mx = !!onemod_mx_type) |>
  dplyr::arrange(location_id, year_id, sex_id, age_end, -age_start) |>
  dplyr::collect() |>
  as.data.frame() |>
  setDT()

dt_pop <-
  demInternal::get_dem_outputs(
    "population estimate",
    run_id = cfg$version$pop_base,
    location_ids = map_loc$location_id,
    sex_ids = 1:2
  ) |>
  _[
    map_age_gbd[age_start >= min_age_start],
    c(..id_cols_age, "mean"),
    on = "age_group_id"
  ]


# Find incomplete age-series ----------------------------------------------

# location-year-sex-sources without a full age-series may indicate issues in
# data prep

dt_incomplete <-
  dt_rs_data |>
  _[
    j = .(
      has_vr = "VR" %in% source_type_name,
      n_sources = uniqueN(.SD, by = c("nid", "underlying_nid")),
      missing_ages = list(setdiff(expected_age_starts, age_start)),
      n_missing_ages = length(setdiff(expected_age_starts, age_start))
    ),
    by = .(location_id, ihme_loc_id, year_id, sex_id)
  ] |>
  _[(has_vr) & purrr::map_lgl(missing_ages, \(x) length(x) > 0), -"has_vr"] |>
  _[, lower_terminal := purrr::map_lgl(missing_ages, \(x) (setequal(diff(x), 5) & 95 %in% x) | all(x == 95))]

dt_incomplete |>
  copy() |>
  _[j = let(
    missing_ages = purrr::map_chr(missing_ages, \(x) paste(x, collapse = ",")),
    lower_terminal = as.integer(lower_terminal)
  )] |>
  readr::write_csv(fs::path(
    cfg$dir$rs_data, cfg$version$rs_data, "diagnostics",
    paste0(file_name_handoff_2, "_missing_ages_", min_age_start, "plus"),
    ext = "csv"
  ))


# Prep data ---------------------------------------------------------------

## Collect possible data to include ----

# Ages 90-94 and 95+ in all locations with at least 20 years of complete VR data
dt_ids_complete_vr <-
  dt_rs_data[outlier == 0 & complete_vr == 1 & source_type_name == "VR"] |>
  dcast(
    location_id + ihme_loc_id + source_type_name ~ fifelse(sex_id == 1, "male", "female"),
    value.var = "year_id",
    fun.aggregate = uniqueN
  ) |>
  _[female >= years_data_threshold & male >= years_data_threshold]

dt_data_prep <- dt_rs_data[
  (location_id %in% dt_ids_complete_vr$location_id & age_start >= 90) |
    (ihme_loc_id == "BGR" & age_start >= 80) |
    (ihme_loc_id == "MNE" & age_start >= 70) |
    (ihme_loc_id == "ALB" & age_start >= 70) |
    (ihme_loc_id == "PER" & age_start >= 85) |
    (ihme_loc_id == "MKD" & age_start >= 70) |
    (ihme_loc_id == "LKA" & age_start >= 75) |
    (ihme_loc_id == "SRB" & age_start >= 55) |
    (ihme_loc_id == "TKM" & age_start >= 70) |
    (ihme_loc_id == "UZB" & age_start >= 65) |
    (ihme_loc_id == "CHL" & age_start >= 80) |
    (ihme_loc_id == "CYP" & age_start >= 65) |
    (ihme_loc_id == "DEU" & age_start >= 85) |
    (ihme_loc_id == "ISR" & age_start >= 80) |
    (ihme_loc_id == "MCO" & age_start >= 85) |
    (ihme_loc_id == "SAU" & age_start >= 90) |
    (ihme_loc_id == "GTM" & age_start >= 85) |
    (ihme_loc_id == "BIH" & age_start >= 85) |
    (ihme_loc_id == "TJK" & age_start >= 90) |
    (ihme_loc_id == "GBR_434" & age_start >= 85) |
    (ihme_loc_id %like% "IRN" & age_start >= 80)
]

dt_data_prep <- dt_data_prep[!(ihme_loc_id == "SRB" & year_id %in% 1992:1997)]

## Choose best source ----

# Criteria: choose source with the most unoutliered ages (minimum 1). In case of
# ties, pick source with most "complete" ages

dt_ids_sources <- dt_data_prep[
  j = .(
    min_age = min(age_start),
    n = uniqueN(age_start),
    n_expected = 1 + (95 - min(age_start)) %/% 5,
    n_included = sum(outlier == 0),
    n_complete = sum(complete_vr == 1),
    n_included_complete = sum(outlier == 0 & complete_vr == 1),
    frac_complete = mean(complete_vr),
    frac_included = mean(outlier == 0)
  ),
  by = .(location_id, ihme_loc_id, year_id, sex_id, nid, underlying_nid, source_type_name)
]

dt_ids_update <-
  dt_ids_sources[frac_included > 0] |>
  setorder(ihme_loc_id, year_id, sex_id, -n_included, -n_complete) |>
  _[
    j = .SD[1],
    by = .(location_id, ihme_loc_id, year_id, sex_id)
  ]

## Get data to update ----

dt_update <- dt_data_prep[
  dt_ids_update,
  c(..id_cols_age, "deaths_adj"),
  on = id_cols_source
]

dt_update_loc_sex_age <-
  dt_update[, .(location_id, sex_id, age_start, age_end)] |>
  unique()


# Fill missing years of VR ------------------------------------------------

dt_update_fill <- dt_onemod[
  dt_update_loc_sex_age,
  on = setdiff(id_cols_age, "year_id")
]

dt_update_fill[dt_pop, pop_base := i.mean, on = id_cols_age]
dt_update_fill[
  dt_update,
  pop_ratio := i.deaths_adj / mx / pop_base,
  on = id_cols_age
]

dt_set_1_ratio <- dt_update_fill[
  year_id >= 2020,
  .(
    year_id = 2024,
    allna = all(is.na(pop_ratio))
  ),
  by = setdiff(id_cols_age, "year_id")
][(allna), -"allna"]

dt_update_fill[dt_set_1_ratio, pop_ratio := 1, on = id_cols_age]
dt_update_fill[year_id == 1950 & is.na(pop_ratio), pop_ratio := 1]

dt_update_fill[
  j = pop_ratio_filled := approx(year_id, pop_ratio, xout = year_id, rule = 2)$y,
  by = setdiff(id_cols_age, "year_id")
]

dt_update_fill[, pop_new := pop_ratio_filled * pop_base]
dt_update_fill[, c("pop_base", "pop_ratio", "pop_ratio_filled") := NULL]
setnames(dt_update_fill, "pop_new", "pop")


# Save --------------------------------------------------------------------

dt_update_fill |>
  as.data.frame() |>
  arrow::write_feather(fs::path(dir_output, file_output, ext = "arrow"))

dt_update_fill |>
  _[pop != 0, .(location_id, age_start)] |>
  unique() |>
  _[map_loc, ihme_loc_id := i.ihme_loc_id, on = "location_id"] |>
  setorderv(c("ihme_loc_id", "age_start")) |>
  setcolorder(c("location_id", "ihme_loc_id")) |>
  readr::write_csv(fs::path(dir_output, paste0(file_output, "-loc_ages-filled"), ext = "csv"))
