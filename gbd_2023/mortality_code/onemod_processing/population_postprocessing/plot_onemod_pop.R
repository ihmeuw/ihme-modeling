
# Meta --------------------------------------------------------------------

# Plot comparison of population estimates against estimated updated with
# Onemod mortality derived population.


# Load packages -----------------------------------------------------------

library(data.table)
library(ggplot2)
library(patchwork)
library(furrr)

plan("multisession")


# Set parameters ----------------------------------------------------------

cfg <- config::get(file = fs::path_wd("population_postprocessing/config.yml"))

run_onemod_pop <- glue::glue("onemod_pop-data={cfg$version$rs_data}-est={cfg$version$onemod}")
dir_out <- fs::path(cfg$dir$pop, "popReconstruct", cfg$version$pop_adj, "diagnostics")

min_age_start <- cfg$min_age_start


# Load maps ---------------------------------------------------------------

map_age <- demInternal::get_age_map(gbd_year = 2023, type = "gbd")
map_loc <- demInternal::get_locations(gbd_year = 2023, level = "estimate")
setorder(map_loc, sort_order)


# Load data ---------------------------------------------------------------

dt_pop <-
  cfg$version |>
  purrr::keep_at(\(x) grepl("pop", x)) |>
  lapply(\(x) demInternal::get_dem_outputs(
    "population estimate",
    run_id = x,
    sex_ids = 1:2,
    age_group_ids = map_age[age_start >= min_age_start, age_group_id],
    name_cols = TRUE
  )) |>
  rbindlist(idcol = "version") |>
  _[
    location_id %in% map_loc$location_id,
    .(version, location_id, ihme_loc_id, location_name, year_id, sex_name, age_start, age_end, pop = mean)
  ]

dt_pop[j = let(
  version = factor(version, levels = c("pop_base", "pop_adj"), labels = c("baseline", "onemod")),
  ihme_loc_id = factor(ihme_loc_id, levels = map_loc$ihme_loc_id, ordered = TRUE)
)]

dt_onemod_pop <-
  fs::path(cfg$dir$pop, "versioned_inputs/gbd2023") |>
  fs::path(run_onemod_pop, ext = "arrow") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::mutate(sex_name = dplyr::if_else(sex_id == 1, "male", "female")) |>
  dplyr::collect() |>
  setDT()


# Compare data ------------------------------------------------------------

dt_pop_compare <- dt_pop |>
  dcast(... ~ version, value.var = "pop") |>
  _[, let(diff = onemod - baseline, rel_diff = (onemod - baseline) / baseline)]

dt_pop_diff <- dt_pop_compare[rel_diff != 0][order(-abs(rel_diff))]
dt_pop_diff[
  dt_onemod_pop,
  onemod_pop := i.pop,
  on = .(location_id, year_id, sex_name, age_start, age_end)
]

readr::write_csv(
  dt_pop_compare[rel_diff != 0, -c("location_id", "location_name")],
  fs::path(dir_out, "compare_onemod_baseline_pop-nonzero.csv")
)


# Make plots --------------------------------------------------------------

make_plot <- function(data) {

  loc_name <- unique(data$location_name)
  loc_code <- unique(data$ihme_loc_id)

  plot_title <- glue::glue("({loc_code}) {loc_name}")

  ggs <- data |>
    split(by = "sex_name") |>
    lapply(\(d) {
      ggplot(d, aes(x = year_id, y = pop, color = version)) +
        geom_line() +
        facet_wrap(
          vars(age_start),
          nrow = 3,
          labeller = label_wrap_gen(multi_line = FALSE),
          scales = "free_y"
        ) +
        scale_color_brewer(palette = "Dark2") +
        scale_x_continuous(breaks = seq(1960, 2024, 20)) +
        scale_y_continuous(
          labels = scales::label_number(scale_cut = append(scales::cut_short_scale(), 1, 1))
        ) +
        labs(
          subtitle = stringr::str_to_title(d$sex_name),
          x = "Year",
          y = "Population",
          color = "Version"
        ) +
        theme_light()
    }) |>
    patchwork::wrap_plots(axis_titles = "collect", guides = "collect") +
    patchwork::plot_annotation(title = plot_title) &
    theme(legend.position = "bottom")

}

list_plot_data <- dt_pop |>
  setorderv(c("ihme_loc_id", "year_id", "sex_name", "age_start")) |>
  split(by = c("ihme_loc_id"))
list_plots <- lapply(list_plot_data, make_plot)


# Save plots --------------------------------------------------------------

dir_out_tmp <- tempdir()
file_name_base <- "onemod_population"
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
