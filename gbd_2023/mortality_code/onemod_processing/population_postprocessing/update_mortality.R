
# Load packages -----------------------------------------------------------

library(data.table)


# Set parameters ----------------------------------------------------------

cfg <- config::get(file = fs::path_wd("population_postprocessing/config.yml"))

id_cols <- c("location_id", "year_id", "sex_id", "age_group_id")

file_update <- glue::glue("onemod_pop-data={cfg$version$rs_data}-est={cfg$version$onemod}-loc_ages-filled")

file_name_rs_data <- paste0("onemod_handoff_2_with_covariates_1950", "")
file_name_onemod <- "predictions"

file_rs_data_update <- paste(file_name_rs_data, "updated", cfg$version$rs_data, sep = "-")
file_onemod_update <- paste(file_name_onemod, "updated", cfg$version$onemod, sep = "-")

# Load maps ---------------------------------------------------------------

map_age_gbd <- demInternal::get_age_map(gbd_year = cfg$gbd_year, type = "gbd")


# Load data ---------------------------------------------------------------

dt_rs_data <-
  fs::path(cfg$dir$rs_data, cfg$version$rs_data, "outputs") |>
  fs::path(file_name_rs_data, ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::collect() |>
  setDT()

dt_onemod <-
  fs::path(cfg$dir$onemod, cfg$version$onemod, file_name_onemod, ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::collect() |>
  setDT()

dt_pop <-
  cfg$version |>
  purrr::keep_at(\(x) grepl("pop", x)) |>
  lapply(\(x) demInternal::get_dem_outputs(
    "population estimate",
    run_id = x,
    sex_ids = 1:2
  )) |>
  rbindlist(idcol = "version") |>
  dcast(
    location_id + year_id + sex_id + age_group_id ~ version,
    value.var = "mean"
  )

dt_loc_age_update <-
  fs::path(cfg$dir$pop, "versioned_inputs/gbd2023", file_update, ext = "csv") |>
  fread() |>
  _[map_age_gbd, age_group_id := i.age_group_id, on = "age_start"]


# Update results ----------------------------------------------------------

dt_rs_data[
  dt_pop,
  let(
    mx_updated = (mx * population) / i.pop_adj,
    mx_adj_updated = (mx_adj * population) / i.pop_adj
  ),
  on = .(location_id, year_id, sex_id, age_group_id)
]

# Handle HDSS fake locations (don't adjust mx)
dt_rs_data[
  source_type_name == "HDSS",
  let(
    mx_updated = mx,
    mx_adj_updated = mx_adj
  )
]

stopifnot(
  dt_rs_data[is.na(mx_updated) & !is.na(mx), .N] == 0,
  dt_rs_data[is.na(mx_adj_updated) & !is.na(mx_adj), .N] == 0
)

dt_onemod[
  dt_loc_age_update,
  mx_final := spxmod,
  on = .(location_id, age_group_id)
]

dt_onemod[is.na(mx_final), mx_final := kreg]

stopifnot(dt_onemod[is.na(mx_final), .N] == 0)


# Save --------------------------------------------------------------------

dt_rs_data |>
  as.data.frame() |>
  arrow::write_parquet(fs::path(cfg$dir$onemod_updated, file_rs_data_update, ext = "parquet"))

dt_onemod |>
  as.data.frame() |>
  arrow::write_parquet(fs::path(cfg$dir$onemod_updated, file_onemod_update, ext = "parquet"))
