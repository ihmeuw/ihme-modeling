
# Meta --------------------------------------------------------------------

# Collapse most detailed age groups into standard GBD abridged ages, for
# life tables and envelope.


# Load packages -----------------------------------------------------------

library(data.table)


# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

# Node arguments
parser$add_argument(
  "--loc_id",
  type = "integer",
  required = !interactive(),
  default = 53611
)

# Version arguments
parser$add_argument(
  "--version",
  type = "integer",
  required = !interactive(),
  default = 463,
  help = "Version of canonizer process"
)

# Process arguments
parser$add_argument(
  "--gbd_year",
  type = "integer",
  required = !interactive(),
  default = 2021
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

dir <- list(main = fs::path(cfg$dir$base, args$version))
dir$cache <- fs::path(dir$main, "cache")
dir$out <- fs::path(dir$main, "output")
dir$log <- fs::path(dir$main, "logs")

draw_range <- do.call(seq, as.list(cfg$draw_range))
max_nonterminal_qx <- cfg$max_nonterminal_qx
ax_age_boundary <- cfg$ax_age_boundary
max_expected_age <- cfg$max_expected_age

id_cols <- c("location_id", "year_id", "age_group_id", "sex_id", "draw")
id_cols_lt <- setdiff(c(id_cols, "type", "age_start", "age_end"), "age_group_id")


# Load maps ---------------------------------------------------------------

map_sex <- arrow::read_ipc_file(fs::path(dir$cache, "sex_map.arrow"))

map_age_canon <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_canonical.arrow"))
map_age_gbd <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_gbd.arrow"))
map_age_report_lt <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_reporting_lt.arrow"))
map_age_report_env <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_reporting_envelope.arrow"))
map_age_summary_lt <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_summary_lt.arrow"))
map_age_summary_env <- arrow::read_ipc_file(fs::path(dir$cache, "age_map_summary_envelope.arrow"))

map_age_report_u5 <- rbind(
  map_age_gbd[age_group_name_short %in% c("enn", "lnn")],
  map_age_report_lt[age_end <= 5]
) |> setorder(age_end, -age_start)

term_age_gbd <- map_age_gbd[is.infinite(age_end), age_start]


# Load data ---------------------------------------------------------------

dt_lt <- fs::path(dir$out, "agg_full_lt") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(location_id == args$loc_id) |>
  dplyr::collect()

dt_pop_detailed <- fs::path(dir$cache, "pop_detailed.arrow") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(location_id == args$loc_id) |>
  dplyr::left_join(map_age_canon, by = c("age_group_id", "age_start")) |>
  dplyr::select(dplyr::all_of(setdiff(id_cols_lt, c("draw", "type"))), mean) |>
  dplyr::collect()

dt_pop_gbd <- fs::path(dir$cache, "pop.arrow") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(location_id == args$loc_id) |>
  dplyr::left_join(map_age_gbd, by = c("age_group_id", "age_start")) |>
  dplyr::select(dplyr::all_of(setdiff(id_cols_lt, c("draw", "type"))), mean) |>
  dplyr::collect()

stopifnot(!anyNA(dt_lt), !anyNA(dt_pop_detailed), !anyNA(dt_pop_gbd))


# Format data -------------------------------------------------------------

cols_mx_ax <- grep("^(mx|ax)", names(dt_lt), value = TRUE)
setnames(
  dt_lt,
  cols_mx_ax,
  stringi::stri_replace_last_fixed(cols_mx_ax, "_", ".")
)

dt_lt <- melt(dt_lt, id.vars = setdiff(id_cols, "sex_id"), variable.factor = FALSE)
dt_lt[, c("variable", "sex") := tstrsplit(variable, ".", fixed = TRUE)]
dt_lt[map_sex, sex_id := i.sex_id, on = "sex"]
dt_lt[, sex := NULL]
dt_lt <- dt_lt |>
  dcast(...~variable, value.var = "value") |>
  setnames("mx", "mx_with_hiv") |>
  melt(id.vars = c(id_cols, "ax"), variable.name = "type", value.name = "mx")
levels(dt_lt$type) <- gsub("mx_", "", levels(dt_lt$type))

dt_lt[
  map_age_canon,
  `:=`(age_start = i.age_start, age_end = i.age_end),
  on = "age_group_id"
]
dt_lt[, age_group_id := NULL]

setcolorder(dt_lt, id_cols_lt)

stopifnot(!anyNA(dt_lt))


# Calculate LT parameters -------------------------------------------------

dt_lt[age_start < ax_age_boundary, qx := demCore::mx_ax_to_qx(mx, ax, age_end - age_start)]
dt_lt[age_start >= ax_age_boundary, qx := demCore::mx_to_qx(mx, age_end - age_start)]

check_full_qx <- dt_lt[is.finite(age_end) & qx >= 1]
if (nrow(check_full_qx) > 0) {

  warning(paste(
    "Location", args$loc_id, "full lt qx too high in",
    nrow(check_full_qx), "rows. Proceeding to alter ax"
  ))

  check_full_qx[, ax_new := demCore::mx_qx_to_ax(mx, 0.99999, age_end - age_start)]

  arrow::write_ipc_file(
    check_full_qx,
    fs::path(dir$log, "qx_ax_change", args$loc_id, ext = "arrow")
  )

  dt_lt[
    check_full_qx,
    `:=`(
      ax = ax_new,
      qx = demCore::mx_ax_to_qx(mx, ax_new, age_end - age_start)
    ),
    on = id_cols_lt
  ]

  check_full_qx2 <- dt_lt[is.finite(age_end) & qx >= 1]
  stopifnot("Full lt qx still > 1 after ax adjustment" = nrow(check_full_qx2) == 0)
  rm(check_full_qx2)

}

rm(check_full_qx)

# Avoid using `demCore:lifetable()` because we don't want to use or
# recalculate ax when getting qx (mx -> ax can give negative ax, and
# mx + ax -> qx can give qx > 1).

demCore::gen_lx_from_qx(dt_lt, id_cols_lt)
demCore::gen_dx_from_lx(dt_lt, id_cols_lt)
demCore::gen_nLx(dt_lt, id_cols_lt)
demCore::gen_Tx(dt_lt, id_cols_lt)
demCore::gen_ex(dt_lt)

dt_lt[, age_length := NULL]

check_above_max_age <- dt_lt[age_start + ex > max_expected_age]
if (nrow(check_above_max_age) > 0) {

  id_cols_lt_no_age <- setdiff(id_cols_lt, c("age_start", "age_end"))
  dt_bad_ids <- unique(check_above_max_age[, ..id_cols_lt_no_age])

  dt_bad_lts <- dt_lt[dt_bad_ids, on = id_cols_lt_no_age]
  dt_bad_lts |>
    as.data.frame() |>
    arrow::write_ipc_file(fs::path(dir$log, "high_ex", args$loc_id, ext = "arrow"))

  check_terminal_above_max_age <- check_above_max_age[is.infinite(age_end) & age + ex > max_expected_age]

  if (nrow(check_terminal_above_max_age) > 0) {

    stop(paste(
      "Location", args$loc_id, "terminal ex too high in",
      nrow(check_above_max_age), "rows."
    ))

  }

}

rm(check_above_max_age)


# Abridge LT --------------------------------------------------------------

dt_lt[
  dt_pop_detailed[age_start < term_age_gbd],
  pop := i.mean,
  on = setdiff(id_cols_lt, c("draw", "type"))
]

dt_abr <- abridge_lt(dt_lt, map_age_gbd, id_cols_lt)

# Create separate table of abridged reporting age groups
# `abridge_lt()` can't produce multiple aggregate age groups covering
# the same span of ages, so we must run this function multiple times for
# different subsets of our desired reporting age groups
dt_abr_report <-
  list(
    map_age_report_lt[age_group_name_short %in% c("nn", "pnn")],
    map_age_report_lt[age_group_name %in% c("<1 year", "1 to 4")],
    map_age_report_lt[age_group_name == "Under 5"],
    map_age_report_lt[age_group_name == "15 to 59"]
  ) |>
  lapply(\(x) abridge_lt(
    dt_lt[age_start >= min(x$age_start) & age_end <= max(x$age_end)],
    x,
    id_cols_lt,
    require_terminal = FALSE
  )) |>
  rbindlist()

dt_lt[, pop := NULL]
dt_lt[map_age_canon, age_group_id := i.age_group_id, on = .(age_start, age_end)]
stopifnot(!anyNA(dt_lt))

setorderv(dt_abr, id_cols_lt)
setorderv(dt_abr_report, id_cols_lt)

check_abr_lt <- \(abr_lt, abr_age_map) stopifnot(
  !anyNA(abr_lt),
  all(abr_age_map$age_start %in% unique(abr_lt$age_start)),
  nrow(abr_lt[, .N, by = setdiff(id_cols_lt, "draw")][N != length(draw_range)]) == 0,
  nrow(abr_lt[ax <= 0]) == 0,
  nrow(abr_lt[mx <= 0]) == 0,
  nrow(abr_lt[ex <= 0]) == 0
)

check_abr_lt(dt_abr, map_age_gbd)
check_abr_lt(dt_abr_report, map_age_report_lt)

check_big_ax <- dt_abr[ax >= (age_end - age_start)]
if (nrow(check_big_ax) > 0) {

  warning(paste(
    "Location", args$loc_id, "abridged LT has", nrow(check_big_ax),
    "rows where ax is greater than the age group length.",
    "Modifying ax to give near-1 qx value."
  ))

  check_big_ax[, ax_new := demCore::mx_qx_to_ax(mx, 0.99999, age_end - age_start)]

  arrow::write_ipc_file(
    check_big_ax,
    fs::path(dir$log, "big_abridged_ax", args$loc_id, ext = "arrow")
  )

  dt_abr[check_big_ax, ax := ax_new, on = id_cols_lt]

  stopifnot(
    nrow(dt_abr[ax >= (age_end - age_start)]) == 0,
    nrow(dt_abr[ax <= 0]) == 0
  )

}
rm(check_big_ax)

# Disallow qx >= 1 in non-terminal age_groups
dt_abr[is.finite(age_end) & qx > max_nonterminal_qx, qx := max_nonterminal_qx]
dt_abr_report[is.finite(age_end) & qx > max_nonterminal_qx, qx := max_nonterminal_qx]

check_bad_qx <- dt_abr[is.finite(age_end) & qx >= 1]
if (nrow(check_bad_qx) > 0) {

  warning(paste(
    "Location", args$loc_id, "abridged lt qx too high in",
    nrow(check_bad_qx), "rows. Proceeding to alter ax"
  ))

  check_bad_qx[, ax_new := demCore::mx_qx_to_ax(mx, max_nonterminal_qx, age_end - age_start)]

  arrow::write_ipc_file(
    check_bad_qx,
    fs::path(dir$log, "qx_ax_change_abridged", args$loc_id, ext = "arrow")
  )

  dt_abr[
    check_bad_qx,
    `:=`(
      ax = ax_new,
      qx = demCore::mx_ax_to_qx(mx, ax_new, age_end - age_start)
    ),
    on = id_cols_lt
  ]

  check_bad_qx2 <- dt_abr[is.finite(age_end) & qx >= 1]
  stopifnot(
    "qx still > 1 after ax adjustment" = nrow(check_bad_qx2) == 0,
    nrow(dt_abr[ax >= (age_end - age_start)]) == 0,
    nrow(dt_abr[ax <= 0]) == 0
  )
  rm(check_bad_qx2)

}
rm(check_bad_qx)


# Prepare envelope --------------------------------------------------------

dt_pop_report <- hierarchyUtils::agg(
  dt_pop_gbd,
  id_cols = setdiff(id_cols_lt, c("draw", "type")),
  value_cols = "mean",
  col_stem = "age",
  col_type = "interval",
  mapping = map_age_report_lt[, .(age_start, age_end)]
)

dt_abr[
  dt_pop_gbd,
  deaths := mx * i.mean,
  on = setdiff(id_cols_lt, c("draw", "type"))
]

dt_abr_report[
  dt_pop_report,
  deaths := mx * i.mean,
  on = setdiff(id_cols_lt, c("draw", "type"))
]


# Summarize ---------------------------------------------------------------

dt_lt_summary <- dt_lt[
  j = lapply(.SD, mean),
  by = c(setdiff(id_cols, "draw"), "type"),
  .SDcols = c("mx", "ax", "qx", "lx", "nLx", "Tx", "ex")
]

dt_abr_summary <-
  list("gbd" = dt_abr, "reporting" = dt_abr_report) |>
  rbindlist(use.names = TRUE, fill = TRUE) |>
  melt(id.vars = id_cols_lt, na.rm = TRUE) |>
  demUtils::summarize_dt(
    id_cols = c(id_cols_lt, "variable"),
    summarize_cols = "draw",
    value_cols = "value"
  ) |>
  setnames(c("q2.5", "q97.5"), c("lower", "upper"))

check_inrange_abr_summary <- dt_abr_summary[!inrange(mean, lower, upper)]
if (nrow(check_inrange_abr_summary) > 0) {
  check_inrange_abr_summary |>
    as.data.frame() |>
    arrow::write_ipc_file(fs::path(dir$log, "mean_outside_ui_abr", args$loc_id, ext = "arrow"))
}
rm(check_inrange_abr_summary)

dt_abr_summary_lt <- dt_abr_summary[variable != "deaths"]
setnames(dt_abr_summary_lt, "variable", "life_table_parameter_name")

dt_env_reporting_summary <-
  dt_abr[, c(..id_cols_lt, "deaths")] |>
  hierarchyUtils::agg(
    id_cols = id_cols_lt,
    value_cols = "deaths",
    col_stem = "age",
    col_type = "interval",
    mapping = map_age_report_env[, .(age_start, age_end)]
  ) |>
  demUtils::summarize_dt(
    id_cols = id_cols_lt,
    summarize_cols = "draw",
    value_cols = "deaths"
  ) |>
  setnames(c("q2.5", "q97.5"), c("lower", "upper"))

dt_abr_summary_env <- rbindlist(
  list(dt_abr_summary[variable == "deaths", -"variable"], dt_env_reporting_summary),
  use.names = TRUE
)

stopifnot(
  nrow(dt_abr_summary_lt[mean <= 0]) == 0,
  nrow(dt_abr_summary_lt[life_table_parameter_name == "ax" & mean >= (age_end - age_start)]) == 0,
  nrow(dt_abr_summary_lt[life_table_parameter_name == "ax" & lower >= (age_end - age_start)]) == 0,
  nrow(dt_abr_summary_lt[life_table_parameter_name == "ax" & upper >= (age_end - age_start)]) == 0,
  nrow(dt_abr_summary_lt[life_table_parameter_name == "qx" & is.finite(age_end) & mean >= 1]) == 0
)

stopifnot(
  nrow(dt_abr_summary_env[mean <= 0]) == 0,
  nrow(dt_abr_summary_env[lower <= 0]) == 0,
  nrow(dt_abr_summary_env[upper <= 0]) == 0
)

dt_report_u5 <- rbind(
  dt_abr[
    map_age_gbd[age_group_name_short %in% c("enn", "lnn"), .(age_start, age_end)],
    on = .(age_start, age_end)
  ],
  dt_abr_report[age_end <= 5]
) |> setorderv(id_cols_lt)


# Format ------------------------------------------------------------------

format_for_save <- function(dt, age_map, order_cols = NULL, remove_start_end = TRUE) {

  age_cols <- c("age_start", "age_end")
  dt[age_map, age_group_id := i.age_group_id, on = age_cols]
  if (remove_start_end) dt[, (age_cols) := NULL]
  stopifnot(!anyNA(dt))
  if (!is.null(order_cols)) setcolorder(dt, order_cols)
  invisible(dt)

}

format_for_save(dt_abr, map_age_gbd, c(id_cols, "type", "deaths"))
format_for_save(dt_report_u5, map_age_report_u5, c(id_cols, "type", "deaths"))
format_for_save(dt_abr_summary_lt, map_age_summary_lt, setdiff(id_cols, "draw"))
format_for_save(dt_abr_summary_env, map_age_summary_env, setdiff(id_cols, "draw"))


# Save --------------------------------------------------------------------

## Full LT draws ----

arrow::write_dataset(
  dt_lt[, c(..id_cols, "type", "mx", "ax", "qx", "lx", "ex")],
  fs::path(dir$out, "final_full_lt"),
  format = "arrow",
  partitioning = c("type"),
  basename_template = paste0(args$loc_id, "-{i}.arrow")
)

## Full LT summary ----

arrow::write_ipc_file(
  dt_lt_summary,
  fs::path(dir$out, "final_full_lt_summary", args$loc_id, ext = "arrow")
)


## Abridged LT and envelope draws ----

arrow::write_dataset(
  dt_abr,
  fs::path(dir$out, "final_abridged_lt_env"),
  format = "arrow",
  partitioning = c("type"),
  basename_template = paste0(args$loc_id, "-{i}.arrow")
)

## Reporting U5 LT and envelope draws ----

arrow::write_dataset(
  dt_report_u5,
  fs::path(dir$out, "final_report_u5_lt_env"),
  format = "arrow",
  partitioning = c("type"),
  basename_template = paste0(args$loc_id, "-{i}.arrow")
)

## Abridged LT and envelope summary ----

arrow::write_ipc_file(
  dt_abr_summary_lt,
  fs::path(dir$out, "final_abridged_lt_summary", args$loc_id, ext = "arrow")
)

arrow::write_ipc_file(
  dt_abr_summary_env,
  fs::path(dir$out, "final_abridged_env_summary", args$loc_id, ext = "arrow")
)
