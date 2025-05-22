
# Meta --------------------------------------------------------------------

# Plot comparison of updated VR data mx against Onemod mx after updating for
# the population adjustment


# Load packages -----------------------------------------------------------

library(data.table)
library(ggplot2)
library(furrr)

plan("multisession")


# Set parameters ----------------------------------------------------------

cfg <- config::get(file = fs::path_wd("population_postprocessing/config.yml"))

file_update <- glue::glue("onemod_pop-data={cfg$version$rs_data}-est={cfg$version$onemod}-loc_ages-filled")
file_rs_data_update <- paste("onemod_handoff_2", "updated", cfg$version$rs_data, sep = "-")
file_onemod_update <- paste("predictions", "updated", cfg$version$onemod, sep = "-")

dir_out <- fs::path(cfg$dir$onemod_updated)


# Load maps ---------------------------------------------------------------

map_loc <- demInternal::get_locations(gbd_year = cfg$gbd_year)
map_age_gbd <- demInternal::get_age_map(gbd_year = cfg$gbd_year, type = "gbd", all_metadata = TRUE)
map_sex <- data.table(sex_id = 1:2, sex_name = c("Male", "Female"))


# Load data ---------------------------------------------------------------

## Adjusted location-ages ----

dt_loc_age_update <-
  fs::path(cfg$dir$pop, "versioned_inputs/gbd2023", file_update, ext = "csv") |>
  fread()

dt_pop_diff <-
  fs::path(cfg$dir$pop, "popReconstruct", cfg$version$pop_adj, "diagnostics") |>
  fs::path("compare_onemod_baseline_pop-nonzero", ext = "csv") |>
  fread() |>
  _[abs(rel_diff) > 1e-14] |>
  dcast(
    ihme_loc_id + age_start + age_end ~ sex_name,
    value.var = "rel_diff",
    fun.aggregate = length
  ) |>
  _[, age_start := as.numeric(age_start)]

## Data / estimates ----

dt_rs_data <-
  fs::path(cfg$dir$onemod_updated, file_rs_data_update, ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::filter(
    source_type_name %in% c("VR", "SRS"),
    outlier == 0
  ) |>
  dplyr::left_join(map_loc, by = "location_id") |>
  dplyr::left_join(map_age_gbd, by = "age_group_id") |>
  dplyr::inner_join(map_sex, by = "sex_id") |>
  dplyr::inner_join(dt_pop_diff, by = c("ihme_loc_id", "age_start", "age_end")) |>
  dplyr::select(
    location_id, ihme_loc_id, location_name,
    year_id,
    sex_name,
    age_start, age_group_name,
    outlier,
    mx = mx_adj_updated
  ) |>
  dplyr::collect() |>
  setDT()

dt_data_loc_ages <- dt_rs_data[, .N, by = .(location_id, age_start)]

dt_onemod <-
  fs::path(cfg$dir$onemod_updated, file_onemod_update, ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::left_join(map_loc, by = "location_id") |>
  dplyr::left_join(map_age_gbd, by = "age_group_id") |>
  dplyr::inner_join(map_sex, by = "sex_id") |>
  dplyr::inner_join(dt_pop_diff, by = c("ihme_loc_id", "age_start", "age_end")) |>
  dplyr::select(
    location_id, ihme_loc_id, location_name,
    year_id,
    sex_name,
    age_start, age_group_name,
    mx = mx_final
  ) |>
  dplyr::collect() |>
  setDT()


# Prep data ---------------------------------------------------------------

dt_prep <-
  list(data = dt_rs_data, est = dt_onemod) |>
  rbindlist(use.names = TRUE, fill = TRUE, idcol = "type") |>
  _[, let(
    age_group_name = factor(age_group_name, levels = map_age_gbd$age_group_name, ordered = TRUE),
    ihme_loc_id = factor(ihme_loc_id, levels = map_loc[order(sort_order), ihme_loc_id], ordered = TRUE)
  )] |>
  setorderv(c("ihme_loc_id", "year_id", "sex_name", "age_start", "type"))


# Make plots --------------------------------------------------------------

make_plot <- function(data) {

  loc_name <- unique(data$location_name)
  loc_code <- unique(data$ihme_loc_id)
  sex_name <- unique(data$sex_name)

  plot_title <- glue::glue("({loc_code}) {loc_name} - {sex_name}s")

  ggplot(mapping = aes(x = year_id, y = mx)) +
    geom_line(data = data[type == "est"]) +
    geom_point(data = data[type == "data"]) +
    facet_wrap(vars(age_group_name), scales = "free_y") +
    scale_x_continuous(breaks = seq(1960, 2024, by = 20)) +
    scale_y_continuous(labels = scales::label_number(
      scale = 1e5,
      scale_cut = append(scales::cut_short_scale(), 1, 1))
    ) +
    theme_light() +
    labs(
      title = plot_title,
      x = "Year",
      y = "Deaths per 100k"
    )

}

list_plot_data <- dt_prep |> split(by = c("ihme_loc_id", "sex_name"), drop = TRUE)

make_plot(list_plot_data$ARM.Female)

list_plots <- list_plot_data |> lapply(make_plot)

# Save plots --------------------------------------------------------------

dir_out_tmp <- tempdir()
file_name_base <- glue::glue("plot_adjusted_mortality-data={cfg$version$rs_data}-est={cfg$version$onemod}")
file_name_out <- paste0(file_name_base, "")

furrr::future_iwalk(list_plots, \(p, i) withr::with_pdf(
  new = fs::path(dir_out_tmp, paste(file_name_base, i, sep = "-"), ext = "pdf"),
  width = 14,
  height = 8,
  code = plot(p)
))

files_temp <- fs::path(dir_out_tmp, paste(file_name_base, names(list_plots), sep = "-"), ext = "pdf")
all(fs::file_exists(files_temp))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path(dir_out, file_name_out, ext = "pdf")
)
