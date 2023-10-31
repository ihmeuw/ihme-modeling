
# Meta --------------------------------------------------------------------

# Create pseudo MLT estimates using harmonizer

# First, copy results from a base MLT run for years other than 2020-2021. Then,
# for 2020-2021, get the difference in no-hiv, no-shock harmonizer mx and the
# mx from the input no-hiv, no-shock finalizer run used to create the
# harmonizer run. Finally, apply this differential mx to the 2020-2021 base
# MLT run no-hiv and with-hiv life tables.


# Load packages -----------------------------------------------------------

library(data.table)
library(furrr)


# Set parameters ----------------------------------------------------------

plan("multisession")

create_new_run <- FALSE

runs_input <- list(
  mlt_base = 560,
  harmonizer = "2023_07_11-13_25_08",
  nsdn_novr = 650
)

if (!create_new_run) {

  runs_output <- list(
    mlt_lt = 557,
    mlt_env = 554
  )

} else {

  mlt_run_comment <- glue::glue(
    "Harmonizer to MLT {runs_input$harmonizer} and MLT v{runs_input$mlt_base}"
  )

}

dirs <- list(
  mlt = "FILEPATH",
  finalizer = "FILEPATH",
  harmonizer = "FILEPATH"
)


get_nsdn_finalizer_run <- function(run_id_nsdn) {

  dt <- demInternal::get_parent_child(
    "no shock death number estimate",
    run_id = run_id_nsdn,
    lineage_type = "child"
  )

  dt[child_process_name == "with shock death number estimate", as.integer(child_run_id)]

}

runs_input <-
  runs_input[names(runs_input) %like% "nsdn"] |>
  lapply(get_nsdn_finalizer_run) |>
  rlang::set_names(\(x) gsub("nsdn", "finalizer", x)) |>
  append(runs_input, values = _) |>
  (\(x) x[match(unique(names(x)), names(x))])()


# Load maps ---------------------------------------------------------------

map_locs <- demInternal::get_locations(gbd_year = 2021, level = "countryplus")[ihme_loc_id != "CHN"]
map_ages <- demInternal::get_age_map(type = "lifetable", all_metadata = TRUE)
map_sex <- data.table(sex_id = 1:3, sex = c("male", "female", "both"))

# map_loc_yr <- CJ(loc = 4643, yr = 2020:2021)
map_loc_yr <- CJ(loc = map_locs$location_id, yr = 2020:2021)


# Define helper functions -------------------------------------------------

load_mlt_draws <- function(run, type, loc, years) {

  dir_in <- fs::path(dirs$mlt, run, paste0("lt_", type), "scaled")
  file_in <-  paste(type, "lt", years, sep = "_")

  fs::path(dir_in, file_in, ext = "h5") |>
    lapply(\(x) mortcore::load_hdf(x, by_val = loc)) |>
    rbindlist()

}

recalc_qx_ax <- function(mx, age_length) {

  new_qx <- demCore::mx_to_qx(mx, age_length)
  new_ax <- demCore::mx_qx_to_ax(mx, new_qx, age_length)

  list(new_qx, new_ax)

}

get_loc_ids <- function(loc_code_pattern) {
  map_locs[ihme_loc_id %like% loc_code_pattern, location_id]
}


# Create new version ------------------------------------------------------

if (create_new_run) {

  ## Generate run IDs ----

  runs_output <- list()
  runs_output$mlt_lt <- mortdb::gen_new_version("mlt life table", "estimate", comment = mlt_run_comment)
  runs_output$mlt_env <- mortdb::gen_new_version("mlt death number", "estimate", comment = mlt_run_comment)

  mortdb::gen_parent_child(
    "mlt life table estimate",
    child_id = runs_output$mlt_lt,
    parent_runs = list("mlt life table estimate" = runs_input$mlt_base)
  )

  mortdb::gen_parent_child(
    "mlt death number estimate",
    child_id = runs_output$mlt_env,
    parent_runs = list("mlt life table estimate" = runs_input$mlt_base)
  )

  ## Set up directory structure ----

  dirs_to_copy <- c("lt_with_hiv", "lt_hiv_free", "env_with_hiv")

  fs::dir_create(
    fs::path(dirs$mlt, runs_output$mlt_lt, dirs_to_copy, "scaled"),
    mode = "775"
  )

  fs::dir_create(
    fs::path(dirs$mlt, runs_output$mlt_lt, dirs_to_copy, "summary/upload"),
    mode = "775"
  )

  ## Link non-harmonized years to base MLT run ----

  purrr::walk(dirs_to_copy, \(x) {

    files_link <-
      fs::path(dirs$mlt, runs_input$mlt_base, x, "scaled") |>
      fs::dir_ls(type = "file", regexp = "2020|2021", invert = TRUE) |>
      basename()

    fs::link_create(
      fs::path(dirs$mlt, runs_input$mlt_base, x, "scaled", files_link),
      fs::path(dirs$mlt, runs_output$mlt_lt, x, "scaled", files_link)
    )


  })

}


# Load data ---------------------------------------------------------------

## MLT ----

dt_mlt <- c("hiv_free", "with_hiv") |>
  rlang::set_names() |>
  lapply(\(type) {
    unique(map_loc_yr$loc) |>
      furrr::future_map(\(loc) load_mlt_draws(runs_input$mlt_base, type, loc, unique(map_loc_yr$yr))) |>
      rbindlist()
  }) |>
  rbindlist(idcol = "type")

setnames(dt_mlt, "mx", "mx_original")

## Harmonizer ----

dt_lt_harmonizer <-
  fs::path(
    dirs$harmonizer, runs_input$harmonizer, "draws/loc_specific_lt",
    paste("harmonized_lt", map_loc_yr$loc, map_loc_yr$yr, sep = "_"),
    ext = "RDS"
  ) |>
  furrr::future_map(readRDS) |>
  rbindlist()

## Finalizer no-hiv mx ----

dt_finalizer <- map_loc_yr |>
  furrr::future_pmap(\(loc, yr) mortcore::load_hdf(
    fs::path(
      dirs$finalizer, runs_input$finalizer_novr, "lt_no_hiv",
      paste0("mx_ax_", yr, ".h5")
    ),
    by_val = loc
  )) |>
  rbindlist()


# Prep data ---------------------------------------------------------------

## Create mx adjustment ----

dt_mx_adj <- dt_finalizer[
  dt_lt_harmonizer[age_group_id %in% map_ages$age_group_id],
  .(
    location_id, year_id, age_group_id, sex_id, draw,
    mx_adj = x.mx - i.mx
  ),
  on = .(location_id, year_id, age_group_id, sex_id, draw)
]

rm(dt_lt_harmonizer, dt_finalizer)
gc()

dt_mx_adj[map_sex, sex := i.sex, on = "sex_id"]
dt_mx_adj[
  map_ages,
  `:=`(age = i.age_start, age_length = i.age_end - i.age_start),
  on = "age_group_id"
]
dt_mx_adj[, c("age_group_id", "sex_id") := NULL]

dt_mx_adj[, mx_adj_original := mx_adj]

setkey(dt_mx_adj, location_id, age, sex, year_id)

dt_neg_mx_adj <- map_locs[, 1:3][dt_mx_adj[
  j = .(
    n_neg_draws = sum(mx_adj < -1),
    neg_mx_adj = mean(fifelse(mx_adj < -1, mx_adj, NA_real_), na.rm = TRUE),
    mx_adj = mean(mx_adj)
  ),
  by = .(location_id, year_id, sex, age)
], on = "location_id"][n_neg_draws > 0]

# Artisanal adjustments (no longer needed)
# ages95p <- seq(95, 110, 5)
# dt_mx_adj[CJ(get_loc_ids("BGR"), ages95p, "male", 2020), mx_adj := mx_adj_original * .6]

## Apply mx adjustment ----

dt_mlt[
  dt_mx_adj,
  `:=`(
    mx = mx_original - i.mx_adj,
    age_length = i.age_length
  ),
  on = .(location_id, year = year_id, sex, age, sim = draw)
]

# Find locations where the adjustment would create negative mx values, which
# will require modification
dt_neg_mx <- map_locs[, 1:3][dt_mlt[
  mx < 0,
  .(n_draws = .N, mx_old = mean(mx_original), mx_new = mean(mx)),
  by = .(type, location_id, year, sex, age)
], on = "location_id"]
dt_neg_mx[, mx_adj := mx_old - mx_new]
readr::write_csv(dt_neg_mx, fs::path(dirs$mlt, runs_output$mlt_lt, "neg_adj_mx_draws.csv"))

id_cols <- c("type", "location_id", "year", "sex", "age")

# Set negative mx values to minimum positive draw post-adjustment
dt_mlt_min_pos_mx <- dt_mlt[mx > 0, .(mx = min(mx)), by = c(id_cols, "sim")]
dt_mlt_neg_mx <- dt_mlt[mx < 0, c(..id_cols, "sim", "mx")]
dt_mlt_neg_mx[dt_mlt_min_pos_mx, mx_new := i.mx, on = id_cols]
dt_mlt[dt_mlt_neg_mx, mx := i.mx_new, on = c(id_cols, "sim")]

rm(dt_mlt_min_pos_mx, dt_mlt_neg_mx)
gc()

stopifnot(
  !anyNA(dt_mlt),
  all(dt_mlt[, mx >= 0])
)

dt_mlt[, qx := demCore::mx_ax_to_qx(mx, ax, age_length)]
dt_mlt[(age != 110 & qx >= 1) | ax <= 0, c("qx", "ax") := recalc_qx_ax(mx, age_length)]

## Validate results ----

stopifnot(
  all(dt_mlt[, qx %between% c(0, 1)]),
  all(dt_mlt[, ax >= 0]),
  all(dt_mlt[age < 110, (qx < 1) & (ax %between% c(0, 5))]),
  all(dt_mlt[age == 110, qx == 1])
)

# tmp <- map_locs[, 1:3][dt_mlt[
#   j = lapply(.SD[, -c("sim", "age_length")], mean),
#   by = .(location_id, year, age, sex, type)
# ], on = "location_id"]
# tmp[, mx_adj := mx_original - mx]

dt_mlt[, c("age_length", "mx_original") := NULL]

rm(dt_mx_adj)
gc()


# Save harmonized MLT -----------------------------------------------------

dt_mlt[, yr := year]

dt_mlt[, .(data = list(.SD)), by = .(type, yr)] |>
  furrr::future_pwalk(\(type, yr, data) {
    dir_out <- fs::path(dirs$mlt, runs_output$mlt_lt, paste0("lt_", type), "scaled")
    path_out <- fs::path(dir_out, paste0(type, "_lt_", yr, ".h5"))
    data |>
      split(by = "location_id") |>
      purrr::imap(\(dt, loc) mortcore::save_hdf(
        data = dt,
        filepath = path_out,
        by_var = "location_id",
        by_val = as.integer(loc)
      ))
  })


# Update run status -------------------------------------------------------

mortdb::update_status(
  "mlt life table", "estimate",
  run_id = runs_output$mlt_lt,
  new_status = "completed"
)

mortdb::update_status(
  "mlt death number", "estimate",
  run_id = runs_output$mlt_env,
  new_status = "completed"
)


# Check -------------------------------------------------------------------

# tmp_paths <- unique(dt_mlt[, .(type, yr)]) |>
#   purrr::pmap(\(type, yr) {
#     dir_out <- fs::path(dirs$mlt, runs_output$mlt_lt, paste0("lt_", type), "scaled")
#     fs::path(dir_out, paste0(type, "_lt_", yr, ".h5"))
#   })
#
# tmp_dt <- tmp_paths |>
#   lapply(\(x) setDT(rhdf5::h5ls(x))) |>
#   rbindlist(idcol = TRUE)
#
# tmp_dt[, unique(dim)]
# tmp_dt[, .N, by = ".id"]
