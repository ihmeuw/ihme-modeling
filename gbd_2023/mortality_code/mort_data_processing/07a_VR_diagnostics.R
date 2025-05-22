
# Meta -------------------------------------------------------------------------

# Description: Create data type-specific and comparison diagnostics for vetting
#              the age pattern handoff

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in data
#   4. Format VR data
#   5. Generate graphs

# Outputs:
#   1a. "FILEPATH": VR data (deaths or mx) by location and sex, with year x and faceted by age (showing outliers)
#   1b. "FILEPATH": VR data (log(mx)) by location and sex, with age x and faceted by year (showing outliers)

# Load libraries ---------------------------------------------------------------

library(assertable)
library(data.table)
library(demInternal)
library(dplyr)
library(furrr)
library(ggplot2)
library(mortdb)
library(tidyverse)
library(viridis)

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

# Read in data -----------------------------------------------------------------

handoff_1 <- arrow::read_parquet(
  fs::path("FILEPATH")
)

handoff_1 <- merge(
  handoff_1,
  age_names[, c("age_group_id", "age_group_name")],
  by = "age_group_id",
  all.x = TRUE
)

gbd <- mortdb::get_mort_outputs(
  model_name = "with shock life table",
  model_type = "estimate",
  run_id = "best",
  gbd_year = gbd_year,
  demographic_metadata = TRUE
)

gbd[sex == "Male", sex := "Males"]
gbd[sex == "Female", sex := "Females"]

gbd[, process := "GBD 2023"]

# Format VR data ---------------------------------------------------------------

vr_data <- handoff_1[source_type_name %in% c("VR", "SRS", "DSP")]

vr_data <- vr_data[,
  c("nid", "underlying_nid", "ihme_loc_id", "location_id", "year_id", "sex_id",
    "age_group_id", "age_group_name", "source_type_name", "deaths",
    "mx", "outlier", "series_name", "rank")
]

# check for duplicates
vr_data[, dup := .N, by = names(vr_data)]
assert_values(vr_data, "dup", test = "equal", test_val = 1)
vr_data[, dup := NULL]

vr_data <- melt(
  vr_data,
  measure.vars = patterns("^deaths|^mx"),
  value.name = c("val"),
  variable.name = "measure"
)
vr_data[, measure := as.character(measure)]

# check for duplicates
vr_data[, dup := .N, by = setdiff(names(vr_data), c("val"))]
assert_values(vr_data, "dup", test = "equal", test_val = 1)
vr_data[, dup := NULL]

vr_data$age_group_name <- factor(
  vr_data$age_group_name,
  levels = gbd_age_order
)

vr_data[, outlier := as.character(outlier)]
vr_data[, outlier := ifelse(outlier == 0, "Not Outliered", "Outliered")]
vr_data[, outlier := factor(outlier, levels = c("Outliered", "Not Outliered"))]

vr_data <- vr_data[order(measure, location_id, year_id, sex_id, outlier)]

vr_data <- merge(
  vr_data,
  locs[, c("ihme_loc_id", "location_name", "sort_order")],
  by = "ihme_loc_id",
  all.x = TRUE
)

# Generate graphs --------------------------------------------------------------

# 1a. VR data (deaths or mx) by location and sex, with year x and faceted by age
vr_data_plots <- function(dt, y_var) {

  dt |>
    ggplot(aes(x = year_id, group = interaction(as.factor(series_name)), shape = as.factor(series_name))) +
    geom_point(aes(y = val, color = outlier)) +
    xlim(estimation_year_start, estimation_year_end) +
    facet_wrap(~age_group_name, scales = "free_y") +
    scale_color_manual(values = c("Not Outliered" = "purple", "Outliered" = "orange")) +
    scale_shape_manual(
      values = c(
        "WHO" = 1, "DYB" = 2, "DSP" = 3, "MCCD" = 3, "HMD" = 4, "SRS" = 5,
        "CRVS" = 6, "Custom" = 7, "SSPC" = 8, "Census" = 9
      )
    ) +
    labs(
      title = paste0(unique(dt$location_id), "; ", unique(dt$location_name),"; ", unique(dt$ihme_loc_id), ": ", ifelse(unique(dt$sex_id) == 1, "Male", "Female"), "; VR"),
      x = "Year", y = str_to_title(mm), color = "Outlier", shape = "Category"
    ) +
    theme_bw()

}

for (mm in c("mx", "deaths")) {

  vr_plot_data <- vr_data[measure == mm & year_id >= estimation_year_start & year_id <= estimation_year_end]

  list_plots <- vr_plot_data[order(sort_order, sex_id)] |>
    split(by = c("location_id", "sex_id"), drop = TRUE, sorted = FALSE) |>
    purrr::map(\(x) vr_data_plots(dt = x, y_var = mm))

  file_name_out <- paste0("initial_vr_gbd_ages_", mm)
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

}

# 1b. VR data (log(mx)) by location and sex, with age x and faceted by year (showing outliers)
vr_log_mx_plots <- function(dt, y_var) {

  dt |>
    ggplot(aes(x = age_group_name, group = interaction(as.factor(series_name)), shape = as.factor(series_name))) +
    geom_point(aes(y = log(val), color = outlier)) +
    facet_wrap(~year_id, scales = "free_y") +
    scale_color_manual(values = c("Not Outliered" = "purple", "Outliered" = "orange")) +
    scale_shape_manual(values = c("WHO" = 1, "DYB" = 2, "DSP" = 3, "MCCD" = 3, "HMD" = 4, "SRS" = 5, "CRVS" = 6, "Custom" = 7, "SSPC" = 8, "Census" = 9)) +
    labs(
      title = paste0(unique(dt$location_id), "; ", unique(dt$location_name),"; ", unique(dt$ihme_loc_id), ": ", ifelse(unique(dt$sex_id) == 1, "Male", "Female"), "; VR"),
      x = "Age Name", y = "Mx (log)", color = "Outlier", shape = "Category"
    ) +
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))

}

vr_plot_data <- vr_data[measure == "mx"]

list_plots <- vr_plot_data[order(sort_order, sex_id)] |>
  split(by = c("location_id", "sex_id"), drop = TRUE, sorted = FALSE) |>
  purrr::map(\(x) vr_log_mx_plots(dt = x, y_var = mm))

file_name_out <- paste0("initial_vr_log_mx_by_loc_age_x_test")
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
