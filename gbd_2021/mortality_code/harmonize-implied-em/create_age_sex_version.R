
# Meta --------------------------------------------------------------------

# Create pseudo Age-Sex estimates using harmonizer


# Load packages -----------------------------------------------------------

library(data.table)
library(furrr)


# Set parameters ----------------------------------------------------------

plan("multisession")

create_new_run <- TRUE

runs_input <- list(
  harmonizer = "2023_07_11-13_25_08",
  agesex_base = 383,
  nsdn_novr = 650
)

if (!create_new_run) {

  runs_output <- list(
    agesex = 378
  )

} else {

  agesex_run_comment <- glue::glue(
    "Harmonizer to age-sex {runs_input$harmonizer} and age-sex v{runs_input$agesex_base}"
  )

}

dirs <- list(
  harmonizer = "FILEPATH",
  finalizer = "FILEPATH",
  agesex = "FILEPATH"
)

get_nsdn_finalizer_run <- function(run_id_nsdn) {

  dt <- demInternal::get_parent_child(
    "no shock death number estimate",
    run_id = run_id_nsdn,
    lineage_type = "child"
  )

  dt[child_process_name == "with shock death number estimate", as.integer(child_run_id)]

}

runs_input$finalizer_novr <- get_nsdn_finalizer_run(runs_input$nsdn_novr)


# Load maps ---------------------------------------------------------------

map_locs <- demInternal::get_locations(gbd_year = 2021, level = "countryplus")[ihme_loc_id != "CHN"]

map_loc_yr <- CJ(loc = map_locs$location_id, yr = 2020:2021)

map_ages <- demInternal::get_age_map(gbd_year = 2021, type = "envelope", all_metadata = TRUE)[
  age_end <= 5 & age_group_name != "Neonatal",
  .(
    age_group_id,
    age_name = paste0("q_", age_group_name_short),
    age_start,
    age_end,
    age_length = age_end - age_start
  )
][order(age_end, age_length)]

map_ages[age_name == "q_0", age_name := "q_inf"]
map_ages[age_name == "q_1", age_name := "q_ch"]
map_ages[age_name == "q_<5", age_name := "q_u5"]

# Age-sex uses "both" instead of "all" sex
map_sex <- data.table(
  sex_id = 1:3,
  sex = c("male", "female", "both")
)


# Create new version ------------------------------------------------------

if (create_new_run) {

  ## Generate run IDs ----

  runs_output <- list()
  runs_output$agesex <- mortdb::gen_new_version("age sex", "estimate", comment = agesex_run_comment)

  dt_base_lineage <- mortdb::get_proc_lineage(
    "age sex", "estimate",
    run_id = runs_input$agesex_base,
    lineage_type = "parent"
  )

  mortdb::gen_parent_child(
    "age sex estimate",
    child_id = runs_output$agesex,
    parent_runs = list(
      "age sex estimate" = runs_input$agesex_base,
      "age sex data" = dt_base_lineage[parent_process_name == "age sex data", parent_run_id]
    )
  )

  ## Set up directory structure ----

  fs::dir_create(fs::path(dirs$agesex, runs_output$agesex, "draws"), mode = "775")

}


# Load data ---------------------------------------------------------------

## Harmonizer ----

dt_lt_harmonizer <- map_loc_yr |>
  furrr::future_pmap(\(loc, yr) readRDS(fs::path(
    dirs$harmonizer, runs_input$harmonizer, "draws/loc_specific_lt",
    paste("harmonized_lt", loc, yr, sep = "_"),
    ext = "RDS"
  ))) |>
  rbindlist()

## Finalizer no-hiv ----

dt_finalizer <- map_loc_yr |>
  furrr::future_pmap(\(loc, yr) mortcore::load_hdf(
    fs::path(
      dirs$finalizer, runs_input$finalizer_novr, "lt_no_hiv",
      paste0("mx_ax_", yr, ".h5")
    ),
    by_val = loc
  )) |>
  rbindlist()

## Age-sex ----

dt_agesex <-
  fs::path(dirs$agesex, runs_input$agesex_base, "draws") |>
  fs::dir_ls(type = "file", glob = "*.csv") |>
  furrr::future_map(fread) |>
  rbindlist()


# Prep data ---------------------------------------------------------------

## Merge harmonizer and finalizer parent ----

dt_adj <- dt_lt_harmonizer[
  map_ages,
  .(
    location_id,
    year_id,
    sex_id,
    age_group_id,
    age_name = i.age_name,
    draw,
    qx_harmonizer = demCore::mx_to_qx(mx, i.age_length)
  ),
  on = "age_group_id",
  nomatch = NULL
]

dt_adj[
  dt_finalizer,
  qx_finalizer := i.qx,
  on = .(location_id, year_id, sex_id, age_group_id, draw)
]

dt_adj[map_locs, ihme_loc_id := i.ihme_loc_id, on = "location_id"]
dt_adj[map_sex, sex := i.sex, on = "sex_id"]
dt_adj[, c("location_id", "sex_id", "age_group_id") := NULL]
dt_adj[, year_id := year_id + 0.5]
setnames(dt_adj, c("year_id", "draw"), c("year", "simulation"))

## Create age aggregates ----

dt_adj[age_name %in% c("q_pna", "q_pnb"), age_agg := "q_pnn"]
dt_adj[age_name %in% c("q_inf", "q_ch"), age_agg := "q_u5"]

dt_adj_agg <- dt_adj[
  !is.na(age_agg),
  .(
    qx_harmonizer = 1 - prod(1 - qx_harmonizer),
    qx_finalizer = 1 - prod(1 - qx_finalizer)
  ),
  by = .(ihme_loc_id, year, sex, age_name = age_agg, simulation)
]

dt_adj <- rbind(dt_adj[, -"age_agg"], dt_adj_agg, use.names = TRUE)

## Create adjustment ----

stopifnot(
  !anyNA(dt_adj),
  dt_adj[, all(qx_harmonizer %between% c(0, 1))],
  dt_adj[, all(qx_finalizer %between% c(0, 1))]
)

rm(dt_lt_harmonizer, dt_finalizer, dt_adj_agg)
gc()

dt_adj[, qx_adj := qx_harmonizer / qx_finalizer]

# tmp <- dt_adj[
#   j = lapply(.SD[, -"simulation"], mean),
#   by = .(ihme_loc_id, year, sex, age_name)
# ]


# Apply correction --------------------------------------------------------

dt_agesex_adjusted <- melt(
  dt_agesex[year %in% c(2020.5, 2021.5)],
  id.vars = c("ihme_loc_id", "year", "sex", "simulation"),
  variable.name = "age_name",
  value.name = "qx_original",
  variable.factor = FALSE
)

dt_agesex_adjusted[
  dt_adj,
  qx := qx_original * i.qx_adj,
  on = c("ihme_loc_id", "year", "sex", "simulation")
]

stopifnot(
  !anyNA(dt_agesex_adjusted),
  dt_agesex_adjusted[, all(qx %between% c(0, 1))]
)

dt_agesex_adjusted <- dcast(
  dt_agesex_adjusted[, -"qx_original"],
  ...~age_name,
  value.var = "qx"
)

setcolorder(dt_agesex_adjusted, colnames(dt_agesex))
dt_agesex <- dt_agesex[!year %in% c(2020.5, 2021.5)]

gc()

dt_agesex <- rbind(dt_agesex, dt_agesex_adjusted, use.names = TRUE)


# Save harmonized age-sex estimates ---------------------------------------

dt_agesex[map_locs, location_id := i.location_id, on = "ihme_loc_id"]

dt_agesex[, .(data = list(.SD)), by = .(location_id)] |>
  furrr::future_pwalk(\(data, location_id) readr::write_csv(
    data,
    fs::path(dirs$agesex, runs_output$agesex, "draws", location_id, ext = "csv")
  ))

# Update run status -------------------------------------------------------

mortdb::update_status(
  "age sex", "estimate",
  run_id = runs_output$agesex,
  new_status = "completed"
)
