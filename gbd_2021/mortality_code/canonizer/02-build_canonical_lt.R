
# Meta --------------------------------------------------------------------

# Create canonical life table for a location

# Steps:
# 1) Load / format shocks
# 2) Load / format HIV
# 3) Load / format mortality (MLT and U5 envelope)
# 4) Create most detailed (Single year + detailed U1) life table
# 5) Reckon HIV
# 6) Add shocks


# Load packages -----------------------------------------------------------

library(data.table)


# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

# Node arguments
parser$add_argument(
  "--loc_id",
  type = "integer",
  required = !interactive(),
  default = 44748
)

# Version arguments
parser$add_argument(
  "--version",
  type = "integer",
  required = !interactive(),
  default = 463,
  help = "Version of canonizer process"
)
parser$add_argument(
  "--version_mlt",
  type = "integer",
  required = !interactive(),
  default = 576
)
parser$add_argument(
  "--version_u5",
  type = "integer",
  required = !interactive(),
  default = 410
)
parser$add_argument(
  "--version_shocks",
  type = "integer",
  required = !interactive(),
  default = 123
)
parser$add_argument(
  "--version_hiv",
  type = "character",
  required = !interactive(),
  default = "version_hiv"
)

# Process arguments
parser$add_argument(
  "--gbd_year",
  type = "integer",
  required = !interactive(),
  default = 2021
)
parser$add_argument(
  "--start_year",
  type = "integer",
  required = !interactive(),
  default = 1950,
  help = "First year of estimation"
)
parser$add_argument(
  "--end_year",
  type = "integer",
  required = !interactive(),
  default = 2022,
  help = "Last year of estimation"
)
parser$add_argument(
  "--code_dir",
  type = "character",
  required = !interactive(),
  default = here::here(),
  help = "Working directory for code"
)

args <- parser$parse_args()


# Set parameters ----------------------------------------------------------

cfg <- config::get(file = fs::path(args$code_dir, "config.yml"))
devtools::load_all(fs::path(args$code_dir, "canonizer"))

dir <- list(
  main = fs::path(cfg$dir$base, args$version),
  shocks = fs::path(cfg$dir$shocks, args$version_shocks, "draws"),
  mlt = fs::path(cfg$dir$mlt, args$version_mlt),
  u5 = fs::path(cfg$dir$u5, args$version_u5)
)

dir$cache <- fs::path(dir$main, "cache")

year_range <- (args$start_year):(args$end_year)
pandemic_years <- 2020:2021
terminal_lt_age <- 110

draw_range <- do.call(seq, as.list(cfg$draw_range))

id_cols <- c("location_id", "year_id", "age_group_id", "sex_id")


# Load maps ---------------------------------------------------------------

## Location maps ----

map_loc <- arrow::read_ipc_file(fs::path(dir$cache, "loc_map.arrow"))
ihme_loc <- map_loc[location_id == args$loc_id, ihme_loc_id]
hiv_group <- map_loc[location_id == args$loc_id, hiv_group]
hiv_group_load <- map_loc[location_id == args$loc_id, hiv_group_load]

## Age maps ----

map_age_gbd <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_gbd.arrow"))
map_age_lt <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_lifetable.arrow"))
map_age_single <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_single_year.arrow"))
map_age_lt_mdu5 <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_lifetable_mdu5.arrow"))
map_age_single_mdu1 <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_single_year_mdu1.arrow"))

map_age_mdu5 <- map_age_single_mdu1[age_start < 5]
map_age_mdu5[age_group_name_short == "cha", age_group_name_short := "1"]

age_id_term_gbd <- map_age_gbd[is.infinite(age_end), age_group_id]

age_id_ext_lt <- map_age_lt[
  age_start >= map_age_gbd[is.infinite(age_end), age_start],
  age_group_id
]

map_age_lt_abr_sy <- map_age_single[age_start >= 2 & is.finite(age_end)][
  map_age_lt_mdu5[is.finite(age_end)],
  .(
    input = i.age_group_id,
    output = x.age_group_id
  ),
  on = .(abridged_age_start = age_start),
  nomatch = NULL
]


# Load data ---------------------------------------------------------------

## Population ----

dt_pop <- fs::path(dir$cache, "pop.arrow") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(location_id == args$loc_id, sex_id != 3) |>
  dplyr::select(-run_id) |>
  dplyr::collect()

dt_popsy <- fs::path(dir$cache, "pop_single_year.arrow") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(location_id == args$loc_id, sex_id != 3) |>
  dplyr::select(-run_id) |>
  dplyr::collect()

dt_pop_detailed <- fs::path(dir$cache, "pop_detailed.arrow") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(location_id == args$loc_id, sex_id != 3) |>
  dplyr::select(-run_id) |>
  dplyr::collect()

## Shocks ----

dt_shocks <- load_prep_shocks(
  loc = args$loc_id,
  pop = dt_pop,
  map_age = map_age_lt_abr_sy,
  term_age_id = age_id_term_gbd,
  ext_age_ids = age_id_ext_lt,
  dir_input = dir$shocks
)

assertable::assert_ids(
  dt_shocks,
  id_vars = list(
    location_id = args$loc_id,
    year_id = year_range,
    sex_id = 1:2,
    age_group_id = map_age_single_mdu1$age_group_id,
    draw = draw_range
  )
)

dt_shocks[map_age_single_mdu1, age_start := i.age_start, on = "age_group_id"]

## HIV ----

dt_hiv <- load_hiv(
  loc_id = args$loc_id,
  group = hiv_group_load,
  year_start = args$start_year,
  dt_pop = dt_pop,
  path_spectrum = fs::path(
    cfg$constant_inputs$hiv_spectrum_draws, args$version_hiv,
    paste0(ihme_loc, "_ART_deaths.csv")
  ),
  path_stgpr = cfg$constant_inputs$hiv_stgpr
)

assertable::assert_ids(
  dt_hiv,
  id_vars = list(
    year_id = year_range,
    draw = draw_range,
    age_group_id = map_age_gbd$age_group_id,
    sex_id = 1:2
  )
)

dt_hiv_ext_terminal <- replicate_age(dt_hiv, age_id_term_gbd, age_id_ext_lt)
dt_hiv <- rbindlist(
  list(
    dt_hiv[age_group_id != age_id_term_gbd],
    dt_hiv_ext_terminal
  ),
  use.names = TRUE
)
dt_hiv_single <- expand_age(dt_hiv, map_age_lt_abr_sy)

dt_hiv <- data.table::rbindlist(
  list(
    dt_hiv[!age_group_id %in% unique(map_age_lt_abr_sy$input)],
    dt_hiv_single
  ),
  use.names = TRUE
)

stopifnot(all(sort(unique(dt_hiv$age_group_id)) == sort(map_age_single_mdu1$age_group_id)))
rm(dt_hiv_ext_terminal, dt_hiv_single)

dt_hiv[map_age_single_mdu1, age_start := i.age_start, on = "age_group_id"]
dt_hiv[, age_group_id := NULL]

## MLT ----

dt_mlt <- c("hiv_free", "with_hiv") |>
  rlang::set_names() |>
  lapply(\(x) load_mlt(
    loc_id = args$loc_id,
    lt_type = x,
    year_range = year_range,
    age_map = map_age_lt,
    dir_mlt = fs::path(cfg$dir$mlt, args$version_mlt)
  ))

purrr::walk(dt_mlt, \(dt) assertable::assert_ids(
  dt,
  id_vars = list(
    year_id = year_range,
    draw = draw_range,
    age = map_age_lt$age_start,
    sex_id = 1:2
  )
))

# Set minimum qx threshold to avoid NaN ax values in later abridged LT. Set the
# threshold to the minimum "non-zero-ish" age/sex/year specific qx.
purrr::walk(dt_mlt, set_qx_threshold)

## U5 envelope ----

dt_u5 <- load_u5(
  loc_id = args$loc_id,
  age_map = map_age_mdu5,
  dir_u5 = fs::path(cfg$dir$u5, args$version_u5)
)

dt_u5[dt_pop_detailed, mx := deaths / i.mean, on = id_cols]
stopifnot(dt_u5[is.na(mx), .N] == 0)
dt_u5[, deaths := NULL]


# Create FLT --------------------------------------------------------------

dt_flt_regression <- fread(cfg$constant_inputs$flt_regression)

dt_flt <-
  dt_mlt |>
  lapply(\(dt) ltcore::gen_full_lt(
    abridged_lt = dt,
    regression_fits = dt_flt_regression,
    id_vars = c("location_id", "year_id", "sex_id", "draw"),
    terminal_age = terminal_lt_age,
    assert_qx = TRUE
  )) |>
  rbindlist(idcol = "lt_type", use.names = TRUE) |>
  dcast(...~lt_type, value.var = c("ax", "qx"))

stopifnot(dt_flt[, max(abs(ax_hiv_free - ax_with_hiv))] < 1e-10)
dt_flt[, ax_with_hiv := NULL]
setnames(dt_flt, "ax_hiv_free", "ax")

stopifnot(
  dt_flt[age == terminal_lt_age & (qx_hiv_free != 1 | qx_with_hiv != 1), .N] == 0,
  dt_flt[age < terminal_lt_age & !is_valid_qx(qx_hiv_free), .N] == 0,
  dt_flt[age < terminal_lt_age & !is_valid_qx(qx_with_hiv), .N] == 0
)

dt_flt[
  age >= 5 & age < terminal_lt_age,
  `:=`(
    mx_hiv_free = demCore::qx_to_mx(qx_hiv_free, 1),
    mx_with_hiv = demCore::qx_to_mx(qx_with_hiv, 1)
  )
]

dt_flt[
  age < 5 | age == terminal_lt_age,
  `:=`(
    mx_hiv_free = demCore::qx_ax_to_mx(qx_hiv_free, ax, 1),
    mx_with_hiv = demCore::qx_ax_to_mx(qx_with_hiv, ax, 1)
  )
]

# check with HIV > without HIV in case the full LT interpolation causes
# single-year variation across the two mx values
# quantile(dt_flt[mx_with_hiv < mx_hiv_free, mx_with_hiv - mx_hiv_free])
dt_flt[mx_with_hiv < mx_hiv_free, mx_with_hiv := mx_hiv_free]

if (!interactive()) rm(dt_mlt)


# Add detailed u5 ages ----------------------------------------------------

## Create HIV-free u5 mortality ----

dt_u5[, age_start_agg := fifelse(age_start < 1, 0, age_start)]
dt_u5[
  dt_flt,
  hiv_free_ratio := i.mx_hiv_free / i.mx_with_hiv,
  on = .(location_id, year_id, sex_id, draw, age_start_agg = age)
]

setnames(dt_u5, "mx", "mx_with_hiv")
dt_u5[, mx_hiv_free := hiv_free_ratio * mx_with_hiv]
stopifnot(all(dt_u5[, mx_with_hiv >= mx_hiv_free]))
dt_u5[, c("age_start_agg", "hiv_free_ratio") := NULL]

## Add ax and qx values ----

dt_u5[age_start >= 1, ax := 0.5]

# NOTE, the mx to ax conversion starts going wild for mx < 1e-8 in the
# detailed under 1 age groups. Since ax approaches 1/2 the age length for
# increasingly smaller mx, just set ax = age_length / 2 for cases with
# sufficiently low mx
dt_u5[map_age_mdu5, age_length := i.age_end - i.age_start, on = "age_group_id"]
dt_u5[age_start < 1, ax := demCore::mx_to_ax(mx_hiv_free, age_length)]
dt_u5[age_start < 1 & mx_hiv_free < 1e-8, ax := age_length / 2]

dt_u5[, `:=`(
  qx_hiv_free = demCore::mx_ax_to_qx(mx_hiv_free, ax, age_length),
  qx_with_hiv = demCore::mx_ax_to_qx(mx_with_hiv, ax, age_length)
)]

stopifnot(
  dt_u5[!is_valid_qx(qx_hiv_free), .N] == 0,
  dt_u5[!is_valid_qx(qx_with_hiv), .N] == 0
)

dt_u5[, c("age_group_id", "age_length") := NULL]
setnames(dt_u5, "age_start", "age")

## Combine ----

# Use ages 2, 3, and 4 from u5 instead of FLT single-year expansion
dt_canon <- rbindlist(
  list(dt_flt[age >= 5], dt_u5),
  use.names = TRUE
)

setnames(dt_canon, "age", "age_start")
setorder(dt_canon, year_id, sex_id, draw, age_start)
stopifnot(
  !anyNA(dt_canon),
  all(sort(unique(dt_canon$age_start)) == sort(map_age_single_mdu1$age_start))
)

if (!interactive()) rm(dt_flt)


# Reconcile HIV -----------------------------------------------------------

id_cols_canon <- c("location_id", "year_id", "sex_id", "age_start", "draw")

dt_canon[
  dt_hiv,
  `:=`(
    mx_spec_hiv = i.mx_spec_hiv,
    hiv_free_ratio = i.hiv_free_ratio
  ),
  on = id_cols_canon
]

stopifnot(dt_canon[is.na(mx_spec_hiv) | is.na(hiv_free_ratio), .N] == 0)

dt_canon[, implied_lt_hiv := mx_with_hiv - mx_hiv_free]

# This must be run before actual HIV reconciliation
dt_hiv_adj <- summarize_hiv_adjustment(
  dt = dt_canon,
  dt_pop = dt_pop_detailed,
  id_cols = id_cols_canon,
  value_cols = c("implied_lt_hiv", "mx_with_hiv", "mx_hiv_free", "mx_spec_hiv")
)
dt_hiv_adj[
  j = c("year_id", "sex_id") := lapply(.SD, as.integer),
  .SDcols = c("year_id", "sex_id")
]

reconcile_hiv(dt_canon, hiv_group)
dt_canon[, c("implied_lt_hiv", "mx_spec_hiv", "hiv_free_ratio") := NULL]


# Add shocks --------------------------------------------------------------

dt_canon[dt_shocks, mx_shock := i.mx, on = id_cols_canon]
stopifnot(dt_canon[is.na(mx_shock), .N] == 0)

## Prevent negative with-shock mx ----

# NOTE we must deal with negative with-shock mx under the constraint that
# shock-specific mortality cannot be altered.

check_neg_with_shock <- dt_canon[(mx_with_hiv + mx_shock) <= 0]

if (nrow(check_neg_with_shock) > 0) {

  # Fix 1: for ages that were expanded from the original shocks input, try
  # and redistribute shock deaths within the more granular ages
  if (check_neg_with_shock[age_start > 1, .N] > 0) {

    dt_redis <- redistribute_shocks(dt_canon, dt_pop_detailed, map_age_gbd)
    dt_canon[dt_redis$success, mx_shock := i.mx_shock_new, on = id_cols_canon]

    if (nrow(dt_redis$partial) > 0) {
      dt_partial_redis <- dt_canon[dt_redis$partial, on = id_cols_canon]
      arrow::write_ipc_file(
        dt_partial_redis,
        fs::path(dir$main, "logs/partial_shocks_redistribution", args$loc_id, ext = "arrow")
      )
      dt_canon[dt_redis$partial, mx_shock := i.mx_shock_new, on = id_cols_canon]
      rm(dt_partial_redis)
    }

  }

  # Fix 2: for ages where we can't redistribute shocks we have to instead
  # increase with- and without-HIV mx to compensate
  check_neg_with_shock2 <- dt_canon[(mx_with_hiv + mx_shock) <= 0]
  if (nrow(check_neg_with_shock2) > 0) {

    dt_absorb <- absorb_shocks(dt_canon, dt_pop_detailed)
    dt_absorb[, `:=`(
      mx_with_shock = mx_with_hiv + mx_shock,
      mx_with_shock_new = mx_with_hiv_new + mx_shock
    )]

    arrow::write_ipc_file(
      dt_absorb,
      fs::path(dir$main, "logs/absorbed_shocks_hiv", args$loc_id, ext = "arrow")
    )

    dt_canon[
      dt_absorb,
      `:=`(mx_hiv_free = i.mx_hiv_free_new, mx_with_hiv = i.mx_with_hiv_new),
      on = id_cols_canon
    ]

  }

  # If there are any remaining locations where with-shock mx is negative,
  # report an error
  check_neg_with_shock3 <- dt_canon[(mx_with_hiv + mx_shock) <= 0]
  if (nrow(check_neg_with_shock3) > 0) {

    arrow::write_ipc_file(
      check_neg_with_shock3,
      fs::path(dir$main, "logs/negative_mx_with_shock", args$loc_id, ext = "arrow")
    )
    stop(paste("Negative with-shock mx in", ihme_loc, nrow(check_neg_with_shock2), "rows"))

  }

  rm(check_neg_with_shock2, check_neg_with_shock3)

}

rm(check_neg_with_shock)

## Calculate with-shock mx ----

dt_canon[, mx_with_shock := mx_with_hiv + mx_shock]
stopifnot(dt_canon[mx_with_shock <= 0, .N] == 0)


# Validate ----------------------------------------------------------------

dt_canon[map_age_single_mdu1, age_length := i.age_end - i.age_start, on = "age_start"]

dt_canon[
  age_start < 5,
  `:=`(
    qx_hiv_free = demCore::mx_ax_to_qx(mx_hiv_free, ax, age_length),
    qx_with_hiv = demCore::mx_ax_to_qx(mx_with_hiv, ax, age_length),
    qx_with_shock = demCore::mx_ax_to_qx(mx_with_shock, ax, age_length)
  )
]

# Set minimum mx value so that mx -> qx conversion is always > 0
dt_canon[age_start >= 5 & mx_hiv_free < 1.110223e-16, mx_hiv_free := 1.110223e-16]

dt_canon[
  age_start >= 5 & age_start < terminal_lt_age,
  `:=`(
    qx_hiv_free = demCore::mx_to_qx(mx_hiv_free, 1),
    qx_with_hiv = demCore::mx_to_qx(mx_with_hiv, 1),
    qx_with_shock = demCore::mx_to_qx(mx_with_shock, 1)
  )
]

dt_canon[
  age_start == terminal_lt_age,
  c("qx_hiv_free", "qx_with_hiv", "qx_with_shock") := 1
]

# Handle extreme shocks in CHN, 1960, ages 2-4 by lowering ax
# (total of 2 draws in two locations)
if (grepl("CHN", ihme_loc)) {
  dt_canon[
    year_id == 1960 & age_start %between% c(2, 4) & qx_with_shock >= 1,
    `:=`(qx_with_shock = 0.99999, ax = demCore::mx_qx_to_ax(mx_with_shock, 0.99999, 1))
  ]
}

## Check mx ----

# NOTE: Shocks can be negative in pandemic years, so alter checks to exclude
# those years

stopifnot(
  dt_canon[mx_hiv_free <= 0, .N] == 0,
  dt_canon[mx_with_hiv <= 0, .N] == 0,
  dt_canon[mx_with_shock <= 0, .N] == 0,
  dt_canon[mx_hiv_free > mx_with_hiv, max(mx_hiv_free - mx_with_hiv)] < 1e-15,
  dt_canon[mx_with_hiv > mx_with_shock & !year_id %in% pandemic_years, max(mx_with_hiv - mx_with_shock)] < 1e-15
)

## Check qx ----

check_qx <- dt_canon[
  age_start < terminal_lt_age &
    !(is_valid_qx(qx_hiv_free) & is_valid_qx(qx_with_hiv) & is_valid_qx(qx_with_shock))
]

if (nrow(check_qx) > 0) {

  arrow::write_ipc_file(
    check_qx,
    fs::path(dir$main, "logs/bad_most_detailed_qx", args$loc_id, ext = "arrow")
  )
  stop(paste("Bad final qx in", ihme_loc, nrow(check_qx), "rows"))

}

stopifnot(
  dt_canon[qx_hiv_free > qx_with_hiv, max(qx_hiv_free - qx_with_hiv)] < 1e-15,
  dt_canon[qx_with_hiv > qx_with_shock & !year_id %in% pandemic_years, max(qx_with_hiv - qx_with_shock)] < 1e-15
)


# Format ------------------------------------------------------------------

dt_canon[map_age_single_mdu1, age_group_id := i.age_group_id, on = "age_start"]
stopifnot(dt_canon[is.na(age_group_id), .N] == 0)

dt_canon[, year_id := as.integer(year_id)]

dt_canon[, mx_shock := NULL]
cols_qx <- grep("^qx", names(dt_canon), value = TRUE)
cols_mx <- grep("^mx", names(dt_canon), value = TRUE)

dt_canon[, (cols_qx) := NULL]
dt_canon[, c("age_start", "age_length") := NULL]
setcolorder(dt_canon, c(id_cols, "draw", "ax", cols_mx))

# Reshape data to be wide by sex to reduce the overall size of the output
# and better prepare the data for aggregation in the next step

dt_canon <- dcast(dt_canon, ...~sex_id, value.var = c("ax", cols_mx))
setnames(dt_canon, gsub("1$", "male", names(dt_canon)))
setnames(dt_canon, gsub("2$", "female", names(dt_canon)))


# Save --------------------------------------------------------------------

# Save canonical life table as arrow files with custom batches by year to
# make it more efficient to load a single year of data in the aggregation
# step. Be sure and sort the batches by year because we can only select
# batches by (zero-based) index and want to ensure batch 0 -> 1950, 1 -> 1951,
# etc.

tbl_out <- dt_canon |>
  split(by = "year_id", sorted = TRUE) |>
  lapply(arrow::arrow_table)

file_obj <- arrow::FileOutputStream$create(
  fs::path(dir$main, "output/most_detailed_lt", args$loc_id, ext = "arrow")
)
writer <- arrow::RecordBatchFileWriter$create(file_obj, tbl_out[[1]]$schema)
purrr::walk(tbl_out, \(x) writer$write_table(x))
writer$close()
file_obj$close()
rm(file_obj, writer)

arrow::write_ipc_file(
  dt_hiv_adj,
  fs::path(dir$main, "output/hiv_adjust", args$loc_id, ext = "arrow")
)
