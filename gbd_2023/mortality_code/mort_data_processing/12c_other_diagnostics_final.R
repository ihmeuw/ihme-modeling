
# Meta -------------------------------------------------------------------------

# Description: Create data comparison diagnostics for vetting the final dataset handoff

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in and format previous and new final datasets
#   4. Generate graphs

# Outputs:
#   1a. "FILEPATH": scatter of all data showing changes from last run
#   1b. "FILEPATH": scatter of all data showing changes from last run, by super region and age

# Load libraries ---------------------------------------------------------------

library(assertable)
library(data.table)
library(demInternal)
library(dplyr)
library(furrr)
library(ggplot2)
library(ggrepel)
library(mortdb)
library(tidyverse)
library(viridis)

source("FILEPATH")

plan("multisession")

# Command line arguments -------------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir",
  type = "character",
  required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# Define the main directory here
if (interactive()) {
  main_dir <- "INSERT_PATH_HERE"
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
}

# Setup ------------------------------------------------------------------------

# get config
config <- config::get(
  file = paste0("FILEPATH"),
  use_parent = FALSE
)
list2env(config$default, .GlobalEnv)

Sys.unsetenv("PYTHONPATH")

source("FILEPATH")

# Read in internal inputs ------------------------------------------------------

locs <- fread(fs::path("FILEPATH"))
locs <- locs[order(sort_order),]

age_map_gbd <- fread(fs::path("FILEPATH"))

# get age variables
age_names <- mortdb::get_age_map(type = "gbd")

gbd_age_order <- c(
  "Early Neonatal", "Late Neonatal", "Post Neonatal", "1-5 months",
  "6-11 months", "12 to 23 months", "2 to 4", "5 to 9", "10 to 14", "15 to 19",
  "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 plus",
  "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69", "70 to 74",
  "75 to 79", "80 to 84", "85 to 89", "90 to 94", "95 plus"
)

# get sex variables
sex_names <- get_ids("sex")

# Read in and format previous and new final datasets ---------------------------

old_handoff <- setDT(
  arrow::read_parquet(
    fs::path_norm(fs::path("FILEPATH"))
  )
)

new_handoff <-  setDT(
  arrow::read_parquet(
    fs::path("FILEPATH")
  )
)

id_cols <- c(
  "location_id", "location_name", "year_id", "sex_id", "age_group_id", "age_group_name",
  "nid", "underlying_nid", "ihme_loc_id", "source_type_name", "onemod_process_type",
  "sort_order", "super_region_name"
)
measure_cols <- c(
  "deaths", "deaths_adj", "population", "sample_size", "mx", "mx_adj", "mx_se",
  "mx_se_adj", "completeness", "complete_vr", "outlier", "outlier_note"
)

keep <- c(id_cols, measure_cols)

old_handoff[age_names, age_group_name := i.age_group_name, on = "age_group_id"]
new_handoff[age_names, age_group_name := i.age_group_name, on = "age_group_id"]

old_handoff[
  locs,
  ':=' (ihme_loc_id = i.ihme_loc_id, location_name = i.location_name, sort_order = i.sort_order, super_region_name = i.super_region_name),
  on = "location_id"
]
new_handoff[
  locs,
  ':=' (ihme_loc_id = i.ihme_loc_id, location_name = i.location_name, sort_order = i.sort_order, super_region_name = i.super_region_name),
  on = "location_id"
]

old_handoff <- old_handoff[, ..keep]
new_handoff <- new_handoff[, ..keep]

plot_data <- merge(
  new_handoff,
  old_handoff,
  by = id_cols,
  suffixes = c("_new", "_old"),
  all = TRUE
)

# age sort order
plot_data$age_group_name <- factor(
  plot_data$age_group_name,
  levels = gbd_age_order
)

# Generate graphs --------------------------------------------------------------

# 1a. scatter data time series vs previous run, by year, location, age and sex
h2_compare_plots <- function(dt) {

  loc <- unique(dt$ihme_loc_id)
  s <- unique(dt$sex_id)

  sex_name <- sex_names[sex_id == s, sex]

  dt |>
    ggplot(aes(x = year_id, group = interaction(as.factor(source_type_name)), shape = as.factor(source_type_name))) +
    geom_point(data = dt[outlier_old == 0], aes(y = mx_old, color = "Previous Data"), alpha = 0.9, size = 2) +
    geom_point(data = dt[outlier_old == 1], aes(y = mx_old, color = "Previous Data"), alpha = 0.33, size = 2) +
    geom_point(data = dt[outlier_new == 0], aes(y = mx_new, color = "Current Data"), alpha = 0.9) +
    geom_point(data = dt[outlier_new == 1], aes(y = mx_new, color = "Current Data"), alpha = 0.33) +
    scale_colour_manual(values = c("Red", "Black")) +
    scale_shape_manual(
      values = c(
        "SBH" = 0, "SIBS" = 1, "Survey" = 2, "CBH" = 3, "Census" = 4,
        "DSP" = 5, "VR" = 6, "HHD" = 7, "SRS" = 8, "MCCD" = 13
      )
    ) +
    labs(shape = "Source") +
    xlim(1950, 2023) +
    facet_wrap(~age_group_name, scales = "free_y") +
    labs(
      title = paste0(unique(dt$location_id), "; ", unique(dt$location_name),"; ", unique(dt$ihme_loc_id), ": ", sex_name),
      x = "Year", y = "Mx", color = "Version",
      caption = "1. Transparent points are outliered"
    ) +
    theme_bw()

}

list_plots <- plot_data[order(sort_order, sex_id)] |>
  split(by = c("location_id", "sex_id"), drop = TRUE, sorted = FALSE) |>
  purrr::map(\(x) h2_compare_plots(dt = x))

file_name_out <- "scatter_compare_final_data"
dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  \(x, y) withr::with_pdf(
    new = fs::path("FILEPATH"),
    height = 12,
    width = 24,
    code = plot(x)
  )
)

files_temp <- fs::path("FILEPATH")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path("FILEPATH")
)

# 1b. scatter data time series vs previous run, by super region and age

h2_compare_sr_plots <- function(dt) {

  dt[, mx_diff := ((mx_new - mx_old) / mx_old) * 100]
  dt[is.na(mx_diff), mx_diff := 0]

  dt[mx_diff > 10, label := "x"]

  dt |>
    ggplot(aes(group = interaction(as.factor(source_type_name)), shape = as.factor(source_type_name), x = mx_old, y = mx_new, label = ihme_loc_id, color = super_region_name)) +
    geom_point(data = dt[outlier_old == 0], alpha = 0.9, size = 2) +
    geom_point(data = dt[outlier_old == 1], alpha = 0.3, size = 2) +
    geom_text_repel(data = dt[label == "x"], size = 3, color = "black", segment.colour = "black", segment.alpha = 2/10,
                    point.padding = 0.25, box.padding = 0.25, show.legend = FALSE) +
    scale_shape_manual(
      values = c(
        "SBH" = 0, "SIBS" = 1, "Survey" = 2, "CBH" = 3, "Census" = 4,
        "DSP" = 5, "VR" = 6, "HHD" = 7, "SRS" = 8, "MCCD" = 13
      )
    ) +
    scale_x_log10(labels = scales::label_scientific(), guide = "axis_logticks") +
    scale_y_log10(labels = scales::label_scientific(), guide = "axis_logticks") +
    labs(shape = "Source") +
    labs(
      title = unique(dt$age_group_name),
      x = "Previous Mx (log)", y = "Current Mx (log)", color = "IHME Super Region",
      caption = "1. Transparent points are outliered"
    ) +
    theme_bw()

}

list_plots <- plot_data[order(age_group_name)] |>
  split(by = c("age_group_name"), drop = TRUE, sorted = FALSE) |>
  purrr::map(\(x) h2_compare_sr_plots(dt = x))

file_name_out <- "scatter_compare_final_data_by_superregion"
dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  \(x, y) withr::with_pdf(
    new = fs::path("FILEPATH"),
    height = 12,
    width = 24,
    code = plot(x)
  )
)

files_temp <- fs::path("FILEPATH")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path("FILEPATH")
)
