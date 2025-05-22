
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
  "--version_onemod",
  type = "character",
  required = !interactive(),
  default = "2024_08_20_21"
)
parser$add_argument(
  "--version_shocks",
  type = "integer",
  required = !interactive(),
  default = 123
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
  onemod = fs::path(cfg$dir$onemod, args$version_onemod, "draws")
)

dir$cache <- fs::path(dir$main, "cache")
dir$log <- fs::path(dir$main, "logs")
dir$out <- fs::path(dir$main, "output")

year_range <- (args$start_year):(args$end_year)
terminal_lt_age <- cfg$terminal_age
ax_age_boundary <- cfg$ax_age_boundary
max_nonterminal_qx <- cfg$max_nonterminal_qx

draw_range <- do.call(seq, as.list(cfg$draw_range))

id_cols <- c("location_id", "year_id", "age_group_id", "sex_id")
id_cols_canon <- c("location_id", "year_id", "sex_id", "age_start", "draw")


# Load maps ---------------------------------------------------------------

## Location maps ----

map_loc <- arrow::read_ipc_file(fs::path(dir$cache, "loc_map.arrow"))
ihme_loc <- map_loc[location_id == args$loc_id, ihme_loc_id]

## Age maps ----

map_age_gbd <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_gbd.arrow"))
map_age_canon <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_canonical.arrow"))

map_age_lt_abr_sy <- map_age_canon[age_start >= 2 & is.finite(age_end)][
  map_age_gbd[is.finite(age_end)],
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
  dplyr::filter(
    location_id == args$loc_id,
    year_id %in% year_range,
    sex_id != 3
  ) |>
  dplyr::select(-run_id) |>
  dplyr::collect()

dt_pop_detailed <- fs::path(dir$cache, "pop_detailed.arrow") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(
    location_id == args$loc_id,
    year_id %in% year_range,
    sex_id != 3
  ) |>
  dplyr::select(-run_id) |>
  dplyr::collect()

## Shocks ----

dt_shocks <- load_prep_shocks(
  loc = args$loc_id,
  load_years = year_range,
  pop = dt_pop,
  map_age = map_age_lt_abr_sy,
  dir_input = dir$shocks
)

assertable::assert_ids(
  dt_shocks,
  id_vars = list(
    location_id = args$loc_id,
    year_id = year_range,
    sex_id = 1:2,
    age_group_id = map_age_canon$age_group_id,
    draw = draw_range
  )
)

dt_shocks[map_age_canon, age_start := i.age_start, on = "age_group_id"]

## Onemod ----

dt_onemod <-
  fs::path(dir$onemod, paste0(args$loc_id, "-", year_range), ext = "arrow") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(year_id %in% year_range) |>
  dplyr::right_join(map_age_canon, by = "age_group_id") |>
  dplyr::mutate(age_length = age_end - age_start) |>
  dplyr::select(dplyr::all_of(id_cols_canon), age_length, mx = mx_age_scaled) |>
  dplyr::arrange(dplyr::across(dplyr::all_of(id_cols_canon))) |>
  dplyr::collect() |>
  as.data.frame() |>
  setDT()

assertable::assert_ids(
  dt_onemod,
  id_vars = list(
    location_id = args$loc_id,
    year_id = year_range,
    sex_id = 1:2,
    age_start = map_age_canon$age_start,
    draw = draw_range
  )
)

# Set minimum m95 value so that 95 + e95 < 110 (i.e. 1 / m95 < 15)
check_terminal_mx_floor <- dt_onemod[is.infinite(age_length) & mx < 0.067]

if (nrow(check_terminal_mx_floor) > 0) {

  check_terminal_mx_floor |>
    as.data.frame() |>
    arrow::write_ipc_file(fs::path(dir$log, "low_terminal_mx", args$loc_id, ext = "arrow"))

  dt_onemod[is.infinite(age_length) & mx < 0.067, mx := 0.067]

}

# The mx to ax conversion is unstable for mx < 1e-8 in the
# detailed under 1 age groups. Since ax approaches 1/2 the age length for
# increasingly smaller mx, just set ax = age_length / 2 for cases with
# sufficiently low mx
dt_onemod[age_start < 1, ax := demCore::mx_to_ax(mx, age_length)]
dt_onemod[age_start < 1 & mx < 1e-8, ax := age_length / 2]
dt_onemod[age_start < 1 & (ax <= 0 | ax >= age_length), ax := age_length / 2]
dt_onemod[age_start >= 1, ax := 0.5]
dt_onemod[is.infinite(age_length), ax := demCore::mx_to_ax(mx, age_length)]

stopifnot(!anyNA(dt_onemod))

dt_onemod[, age_length := NULL]
dt_canon <- dt_onemod
rm(dt_onemod)


# Add shocks --------------------------------------------------------------

dt_canon[dt_shocks, mx_shock := i.mx, on = id_cols_canon]
stopifnot(dt_canon[is.na(mx_shock), .N] == 0)

dt_canon[, mx_with_shock := mx + mx_shock]
stopifnot(dt_canon[mx_with_shock <= 0, .N] == 0)


# Validate ----------------------------------------------------------------

gen_qx <- function(dt, ax_age_cutoff) {

  dt[
    age_start < ax_age_cutoff,
    `:=`(
      qx = demCore::mx_ax_to_qx(mx, ax, age_length),
      qx_with_shock = demCore::mx_ax_to_qx(mx_with_shock, ax, age_length)
    )
  ]

  dt[
    age_start >= ax_age_cutoff & is.finite(age_length),
    `:=`(
      qx = demCore::mx_to_qx(mx, 1),
      qx_with_shock = demCore::mx_to_qx(mx_with_shock, 1)
    )
  ]

  dt[is.infinite(age_length), c("qx", "qx_with_shock") := 1]

}

dt_canon[map_age_canon, age_length := i.age_end - i.age_start, on = "age_start"]
gen_qx(dt_canon, ax_age_boundary)

# Handle extreme shocks in CHN, 1960, ages 2-4 by lowering ax
# (total of 2 draws in two locations)
if (args$loc_id %in% c(491, 516)) {
  dt_canon[
    year_id == 1960 & age_start %between% c(2, 4) & qx_with_shock >= 1,
    `:=`(
      qx_with_shock = max_nonterminal_qx,
      ax = demCore::mx_qx_to_ax(mx_with_shock, max_nonterminal_qx, 1)
    )
  ]
}

# If with-shock qx is >= 1 in non-terminal ages, recalculate with-shock mx using
# the maximum allowable non-terminal qx, and then subtract shock-specific mx
# from that value to get new no-shock mx.

dt_qx_shock_gte1 <- dt_canon[qx_with_shock >= 1 & is.finite(age_length)]

if (nrow(dt_qx_shock_gte1) > 0) {

  dt_qx_shock_gte1[age_start >= ax_age_boundary, mx_with_shock_new := demCore::qx_to_mx(max_nonterminal_qx, age_length)]
  dt_qx_shock_gte1[age_start < ax_age_boundary, mx_with_shock_new := demCore::qx_ax_to_mx(max_nonterminal_qx, ax, age_length)]
  dt_qx_shock_gte1[, mx_new := mx_with_shock_new - mx_shock]

  dt_canon[
    dt_qx_shock_gte1,
    c("mx", "mx_with_shock") := .(i.mx_new, i.mx_with_shock_new),
    on = id_cols_canon
  ]

  gen_qx(dt_canon, ax_age_boundary)

  dt_qx_shock_gte1 |>
    as.data.frame() |>
    arrow::write_ipc_file(fs::path(dir$log, "high_with_shock_qx", args$loc_id, ext = "arrow"))

}

rm(dt_qx_shock_gte1)

## Check mx ----

stopifnot(
  dt_canon[mx <= 0, .N] == 0,
  dt_canon[mx_with_shock <= 0, .N] == 0,
  dt_canon[mx > mx_with_shock, max(mx - mx_with_shock)] < 1e-15
)

## Check qx ----

check_qx <- dt_canon[
  age_start < terminal_lt_age & !(is_valid_qx(qx) & is_valid_qx(qx_with_shock))
]

if (nrow(check_qx) > 0) {

  arrow::write_ipc_file(
    check_qx,
    fs::path(dir$log, "bad_most_detailed_qx", args$loc_id, ext = "arrow")
  )
  stop(paste("Bad final qx in", ihme_loc, nrow(check_qx), "rows"))

}

stopifnot(dt_canon[qx > qx_with_shock, max(qx - qx_with_shock)] < 1e-15)


# Format ------------------------------------------------------------------

dt_canon[map_age_canon, age_group_id := i.age_group_id, on = "age_start"]
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
  fs::path(dir$out, "most_detailed_lt", args$loc_id, ext = "arrow")
)
writer <- arrow::RecordBatchFileWriter$create(file_obj, tbl_out[[1]]$schema)
purrr::walk(tbl_out, \(x) writer$write_table(x))
writer$close()
file_obj$close()
rm(file_obj, writer)
