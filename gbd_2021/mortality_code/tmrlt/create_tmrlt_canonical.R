
# Meta --------------------------------------------------------------------

# Create the Theoretical Minimum Risk Life Table (TMRLT) from the canonical
# life tables


# Load packages -----------------------------------------------------------

library(data.table)


# Set parameters ----------------------------------------------------------

gbd_year <- 2021

runs <- list(
  pop = 359,
  canon = 463
)

dir <- list(
  canonizer = "FILEPATH",
  tmrlt = "FILEPATH"
)

start_year <- 2010
end_year <- 2019
pop_threshold <- 5e6
interpolated_ages <- seq(0, 110, .01)

gen_new_version <- FALSE
best_new_version <- gen_new_version && TRUE
run_comment <- ""


# Create version ----------------------------------------------------------

if (gen_new_version) {

  stopifnot("Provide an informative comment" = run_comment != "")

  runs$tmrlt <- mortdb::gen_new_version(
    "theoretical minimum risk life table", "estimate",
    gbd_year = gbd_year,
    comment = run_comment
  )

  mortdb::gen_parent_child(
    child_process = "theoretical minimum risk life table estimate",
    child_id = runs$tmrlt,
    parent_runs = list(
      "population estimate" = runs$pop,
      "full life table estimate" = runs$canon
    )
  )

  fs::dir_create(fs::path(dir$tmrlt, runs$tmrlt), mode = "775")

}


# Load maps ---------------------------------------------------------------

map_loc <- demInternal::get_locations(gbd_year = gbd_year, level = "country")
map_age_all <- demInternal::get_age_map(gbd_year = gbd_year, type = "all")


# Load data ---------------------------------------------------------------

dt_pop <- demInternal::get_dem_outputs(
  "population estimate",
  run_id = runs$pop,
  location_ids = map_loc$location_id,
  age_group_ids = 22,
  sex_ids = 3,
  year_ids = end_year,
  name_cols = TRUE
)

# Look for locations where pop > 5 million in end_year
eligible_locs <- dt_pop[
  mean > pop_threshold,
  .(location_id, ihme_loc_id, location_name)
]

dt_canon <-
  fs::path(dir$canonizer, runs$canon, "output/final_full_lt_summary") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(
    location_id %in% eligible_locs$location_id,
    year_id %in% start_year:end_year,
    type == "with_shock",
    sex_id %in% 1:2
  ) |>
  dplyr::left_join(map_loc, by = "location_id") |>
  dplyr::left_join(map_age_all, by = "age_group_id") |>
  dplyr::select(ihme_loc_id, location_name, year_id, age_start, age_end, sex_id, mx, ax, qx) |>
  dplyr::collect()


# Create TMRLT ------------------------------------------------------------

tmrlt <- rbindlist(list(
  dt_canon[!is.infinite(age_end), .SD[which.min(qx)], by = "age_start"],
  dt_canon[is.infinite(age_end), .SD[which.max(ax)], by = "age_start"]
))

setcolorder(tmrlt, c("age_start", "age_end"))

# Don't use `demCore::lifetable()`, which would recalculate one of mx, ax, or
# qx. Instead, calculate the additional parameters from the existing values
# as-is.
demCore::gen_lx_from_qx(tmrlt, id_cols = c("age_start", "age_end"))
demCore::gen_dx_from_lx(tmrlt, id_cols = c("age_start", "age_end"))
demCore::gen_nLx(tmrlt, id_cols = c("age_start", "age_end"))
demCore::gen_Tx(tmrlt, id_cols = c("age_start", "age_end"))
demCore::gen_ex(tmrlt)

tmrlt[, age_length := NULL]


# Interpolate -------------------------------------------------------------

interpolated_ex <- approx(tmrlt$age_start, tmrlt$ex, xout = interpolated_ages)

tmrlt_interpolated <- data.table(
  precise_age = round(interpolated_ex$x, 2),
  mean = interpolated_ex$y,
  estimate_stage_id = 6,
  life_table_parameter_id = 5
)


# Save --------------------------------------------------------------------

readr::write_csv(tmrlt, fs::path(dir$tmrlt, runs$tmrlt, "tmrlt.csv"))
readr::write_csv(tmrlt_interpolated, fs::path(dir$tmrlt, runs$tmrlt, "tmrlt_interpolated.csv"))


# Upload ------------------------------------------------------------------

if (gen_new_version) {

  mortdb::upload_results(
    filepath = fs::path(dir$tmrlt, runs$tmrlt, "tmrlt_interpolated.csv"),
    model_name = "theoretical minimum risk life table",
    model_type = "estimate",
    run_id =  runs$tmrlt
  )

}

if (best_new_version) {

  mortdb::update_status(
    "theoretical minimum risk life table", "estimate",
    run_id = runs$tmrlt,
    new_status= "best"
  )

}
