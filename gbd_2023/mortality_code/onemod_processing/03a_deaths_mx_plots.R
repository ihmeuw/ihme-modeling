# Graphs for mx and deaths
# Compare to GK results and graph VR data as well
# additionally, make graphs comparing pre and post raked results for subnationals

library(data.table)
library(furrr)
library(ggplot2)
library(RColorBrewer)
library(dplyr)
library(mortdb)

plan("multisession")


parser <- argparse::ArgumentParser()

# Node arguments
parser$add_argument(
  "--dir_output",
  type = "character",
  required = !interactive(),
  default = "FILEPATH"
)
parser$add_argument(
  "--code_dir",
  type = "character",
  required = !interactive(),
  default = here::here()
)

args <- parser$parse_args()

list2env(args, .GlobalEnv)


# Set parameters ----------------------------------------------------------

cfg <- config::get(file = fs::path(args$dir_output, "config.yml"))

id_vars <- c("location_id", "year_id", "sex_id", "age_group_id")
value_vars <- c("mean", "lower", "upper")

dir_mx_env <- fs::path(args$dir_output, "diagnostics", "mx_env")

if (!fs::dir_exists(dir_mx_env)) fs::dir_create(dir_mx_env)


# Load maps ---------------------------------------------------------------

map_loc <- fread(fs::path(args$dir_output, "loc_map.csv"))
map_age_gbd <- fread(fs::path(args$dir_output, "age_map_gbd.csv"))
map_age_canon <- fread(fs::path(args$dir_output, "age_map_canonical.csv"))


# Load Data ---------------------------------------------------------------

dt <-
  fs::path(args$dir_output, "summaries") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::collect() |>
  setDT()

dt_onemod_msca_pred <-
  fs::path(args$dir_output, "msca_pred") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::collect() |>
  setDT()

dt_pop <-
  fs::path(args$dir_output, "population", ext = "parquet") |>
  arrow::read_parquet() |>
  setDT()

dt_data <-
  cfg$fp_inputs$handoff_2 |>
  arrow::read_parquet() |>
  setDT() |>
  _[, mx_graph := ifelse(!is.na(mx_adj_updated), mx_adj_updated, mx_updated)] |>
  _[outlier == 0, .(location_id, sex_id, age_group_id, year_id, mx = mx_graph, outlier, source_type_name)] |>
  _[dt_pop, population := i.mean, on = id_vars] |>
  _[, deaths := population * mx]

dt_gbd2021_deaths <-
  fs::path(cfg$dir_canonizer, cfg$run_ids_previous_round$canonizer, "output/final_abridged_env_summary") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(
    (year_id < 2020 & type == "with_hiv") | (year_id >= 2020 & type == "with_shock"),
    sex_id != 3
  ) |>
  dplyr::right_join(map_loc[level > 2], by = "location_id") |>
  dplyr::right_join(map_age_gbd, by = "age_group_id") |>
  dplyr::select(dplyr::all_of(id_vars), dplyr::all_of(value_vars)) |>
  dplyr::collect() |>
  setDT()

dt_gbd2021_mx <-
  fs::path(cfg$dir_canonizer, cfg$run_ids_previous_round$canonizer, "output/final_abridged_lt_summary") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::filter(
    (year_id < 2020 & type == "with_hiv") | (year_id >= 2020 & type == "with_shock"),
    sex_id != 3,
    life_table_parameter_name == "mx"
  ) |>
  dplyr::right_join(map_loc[level > 2], by = "location_id") |>
  dplyr::right_join(map_age_gbd, by = "age_group_id") |>
  dplyr::select(dplyr::all_of(id_vars), dplyr::all_of(value_vars)) |>
  dplyr::collect() |>
  setDT()

dt_wpp_deaths <-
  fs::path(cfg$fp_inputs$wpp_comparator, "Deaths5", ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::filter(
    sex_id != 3 &
      date_start <= 2024
  ) |>
  dplyr::rename(ihme_loc_id = country_ihme_loc_id) |>
  dplyr::left_join(map_loc[, .(location_id, ihme_loc_id)], by = "ihme_loc_id") |>
  dplyr::collect() |>
  dplyr::left_join(map_age_gbd[, .(age_group_id, age_start, age_end)], by = c("age_start", "age_end")) |>
  dplyr::filter(age_group_id %in% map_age_gbd$age_group_id) |>
  dplyr::select(location_id, sex_id, age_group_id, year_id = date_start, mean = value) |>
  setDT()

dt_wpp_mx <-
  fs::path(cfg$fp_inputs$wpp_comparator, "mx5", ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::filter(
    sex_id != 3 &
      date_start <= 2024
  ) |>
  dplyr::rename(ihme_loc_id = country_ihme_loc_id) |>
  dplyr::left_join(map_loc[, .(location_id, ihme_loc_id)], by = "ihme_loc_id") |>
  dplyr::collect() |>
  dplyr::left_join(map_age_gbd[, .(age_group_id, age_start, age_end)], by = c("age_start", "age_end")) |>
  dplyr::filter(age_group_id %in% map_age_gbd$age_group_id) |>
  dplyr::select(location_id, sex_id, age_group_id, year_id = date_start, mean = value) |>
  setDT()


# Format processing results -----------------------------------------------

dt_om_proc_final_mx <-
  dt[, .SD, .SDcols = c(id_vars, grep("mx_age_scaled", names(dt), value = TRUE))] |>
  setNames(c(id_vars, value_vars))

dt_om_proc_final_deaths <-
  dt[, .SD, .SDcols = c(id_vars, grep("deaths_age_scaled", names(dt), value = TRUE))] |>
  setNames(c(id_vars, value_vars))

dt_proc_interm <-
  dt[, .SD,
     .SDcols = c(
       id_vars,
       #subsets out some intermediate steps, change this if you want them back in
       grep("age_scaled|_raked", names(dt), value = TRUE, invert = TRUE)
     ) |>
       unique()
  ]

dt_proc_interm_mx <-
  dt_proc_interm[, .SD, .SDcols = c(id_vars, grep("mx", names(dt_proc_interm), value = TRUE))] |>
  melt(
    id.vars = id_vars,
    value.name = "mean",
    variable.name = "estimate_source",
    variable.factor = FALSE
  ) |>
  _[, estimate_source := gsub("mx_", "", estimate_source)]

dt_proc_interm_deaths <-
  dt_proc_interm[, .SD, .SDcols = c(id_vars, grep("deaths", names(dt_proc_interm), value = TRUE))] |>
  melt(
    id.vars = id_vars,
    value.name = "mean",
    variable.name = "estimate_source",
    variable.factor = FALSE
  ) |>
  _[, estimate_source := gsub("deaths_", "", estimate_source)]

rm(dt, dt_proc_interm)


# Format MSCA predictions -------------------------------------------------

dt_onemod_msca_pred_deaths <-
  dt_onemod_msca_pred[, .SD, .SDcols = c(id_vars, "deaths_age_scaled")] |>
  setnames("deaths_age_scaled", "mean")

dt_onemod_msca_pred_mx <-
  dt_onemod_msca_pred[, .SD, .SDcols = c(id_vars, "mx_age_scaled")] |>
  setnames("mx_age_scaled", "mean")

rm(dt_onemod_msca_pred)


# Graph 1: MX over time ---------------------------------------------------

dt_graph_1 <-
  list(
    `GBD 2021` = dt_gbd2021_mx,
    `Onemod Processing` = dt_om_proc_final_mx,
    `WPP` = dt_wpp_mx,
    `MSCA Point Estimate` = dt_onemod_msca_pred_mx
  ) |>
  rbindlist(
    fill = TRUE,
    idcol = "estimate_source",
  ) |>
  rbind(dt_proc_interm_mx, fill = TRUE) |>
  list(
    estimates = _,
    data = dt_data[, .SD, .SDcols = !c("deaths", "population")] |>
      setnames("mx", "mean")
  ) |>
  rbindlist(
    fill = TRUE,
    idcol = "geom_type"
  ) |>
  merge(map_age_gbd[, .(age_group_id, age_group_name, age_start, age_end)], by = "age_group_id") |>
  merge(map_loc[, .(location_name, location_id, ihme_loc_id, sort_order)], by = "location_id") |>
  _[, `:=` (
    estimate_source = factor(estimate_source),
    ihme_loc_id = reorder(ihme_loc_id, sort_order),
    sort_order = NULL
  )]

rm(dt_gbd2021_mx, dt_om_proc_final_mx, dt_wpp_mx, dt_proc_interm_mx)
gc()

scale_source_type <- c(
  "SBH" = 3,
  "CBH" = 4,
  "VR" = 16,
  "Census" = 17,
  "Survey" = 0,
  "SRS" = 15,
  "SIBS" = 8,
  "HHD" = 2,
  "DSP" = 1
)

unique_sources <-
  dt_graph_1$estimate_source |>
  unique() |>
  sort()
n_colors <- unique_sources |> length()

color_pallete <- RColorBrewer::brewer.pal(name = "Dark2", n = n_colors) |>
  setNames(unique_sources)

plot_graph_1 <- function(dt, y_var) {

  current_loc_id <- unique(dt$location_id)
  current_sex_id <- unique(dt$sex_id)
  current_ihme_loc_id <- unique(dt$ihme_loc_id)
  current_loc_name <- unique(dt$location_name)
  current_sex <- ifelse(current_sex_id == 1, "Males", "Females")

  title_string <- glue::glue("{current_ihme_loc_id} {current_loc_name}, {current_sex}: OneMod {y_var}, pre- and post-raking")

  dt[geom_type != "data"] |>
    ggplot(aes(x = year_id, y = mean)) +
    geom_ribbon(aes(ymin = lower, ymax = upper, fill = estimate_source), alpha = 0.3) +
    geom_line(aes(color = estimate_source), alpha = 0.6) +
    geom_point(
      data = dt[geom_type == "data"],
      aes(shape = source_type_name),
      size = 1
    ) +
    facet_wrap(~factor(reorder(age_group_name, age_start)), scales = "free_y") +
    scale_shape_manual(values = scale_source_type) +
    theme_bw() +
    scale_fill_manual(values = color_pallete) +
    scale_color_manual(values = color_pallete) +
    theme(legend.position = "bottom") +
    labs(
      y = y_var,
      x = "Year",
      title = title_string
    )
}

list_plots <-
  dt_graph_1 |>
  split(by = c("ihme_loc_id", "sex_id"), drop = TRUE, sorted = TRUE) |>
  purrr::map(\(x) plot_graph_1(dt = x, y_var = "mx"))

file_name_out <- "onemod_raking_compare_mx"
dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  \(x, y) withr::with_pdf(
    new = fs::path(dir_out_tmp, paste(file_name_out, y, sep = "-"), ext = "pdf"),
    width = 15,
    height = 9,
    code = plot(x)
  )
)

files_temp <- fs::path(dir_out_tmp, paste(file_name_out, names(list_plots), sep = "-"), ext = "pdf")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path(dir_mx_env, file_name_out, ext = "pdf")
)


# Graph 2: deaths over time -----------------------------------------------

dt_graph_2 <-
  list(
    `GBD 2021` = dt_gbd2021_deaths,
    `Onemod Processing` = dt_om_proc_final_deaths,
    `WPP` = dt_wpp_deaths,
    `MSCA Point Estimate` = dt_onemod_msca_pred_deaths
  ) |>
  rbindlist(
    fill = TRUE,
    idcol = "estimate_source",
  ) |>
  rbind(dt_proc_interm_deaths, fill = TRUE) |>
  list(
    estimates = _,
    data = dt_data[, .SD, .SDcols = !c("mx", "population")] |>
      setnames("deaths", "mean")
  ) |>
  rbindlist(
    fill = TRUE,
    idcol = "geom_type"
  ) |>
  merge(map_age_gbd[, .(age_group_id, age_group_name, age_start, age_end)], by = c("age_group_id")) |>
  merge(map_loc[, .(location_name, location_id, ihme_loc_id, sort_order)], by = c("location_id")) |>
  _[, `:=` (
    estimate_source = factor(estimate_source),
    ihme_loc_id = reorder(ihme_loc_id, sort_order),
    sort_order = NULL
  )]

rm(dt_gbd2021_deaths, dt_om_proc_final_deaths, dt_wpp_deaths, dt_proc_interm_deaths)
gc()

list_plots <-
  dt_graph_2 |>
  split(by = c("ihme_loc_id", "sex_id"), drop = TRUE, sorted = TRUE) |>
  purrr::map(\(x) plot_graph_1(dt = x, y_var = "deaths"))

file_name_out <- "onemod_raking_compare_deaths"
dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  \(x,y) withr::with_pdf(
    new = fs::path(dir_out_tmp, paste(file_name_out, y, sep = "-"), ext = "pdf"),
    width = 15,
    height = 9,
    code = plot(x)
  )
)

files_temp <- fs::path(dir_out_tmp, paste(file_name_out, names(list_plots), sep = "-"), ext = "pdf")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path(dir_mx_env, file_name_out, ext = "pdf")
)


# Find non-monotonic location-sex-years -----------------------------------


dt_mx_check <- dt_graph_1[estimate_source == "Onemod Processing" & age_start >= 80]
dt_mx_check <- dt_mx_check[, .(is_increasing = all(diff(mean) > 0)), by = .(ihme_loc_id, location_id, year_id, sex_id)]
dt_mx_check <- dt_mx_check[is_increasing == FALSE]

dt_mx_check |>
  readr::write_csv(fs::path(dir_mx_env, "non_monotonic_increasing_mx_over_80.csv"))
