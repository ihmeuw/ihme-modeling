
# Meta -------------------------------------------------------------------------

# Description: Create data type-specific and comparison diagnostics for vetting
#              the age pattern handoff

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in data
#   4. Format CBH data
#   5. Format SIBS data
#   6. Generate graphs

# Outputs:
#   1.  "FILEPATH": CBH data (mx) by location and sex, with year x and faceted by age (showing outliers)
#   2a. "FILEPATH": SIBS data (qx) by location and sex, with year x and faceted by age (showing outliers)
#   2b. "FILEPATH": SIBS data (qx) by location and sex, with age x and faceted by year (showing outliers)

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

# Format CBH data --------------------------------------------------------------

cbh <- handoff_1[source_type_name == "CBH"]

cbh <- merge(
  cbh,
  locs[, c("location_id", "location_name")],
  by = "location_id",
  all.x = TRUE
)

cbh[sex_id == 1, sex := "Males"]
cbh[sex_id == 2, sex := "Females"]
cbh[sex_id == 3, sex := "Both Sexes"]

cbh[, age_group_name := factor(
  age_group_name,
  levels = c(
    "Early Neonatal", "Late Neonatal", "1-5 months", "6-11 months", "12 to 23 months",
    "2 to 4", "5 to 9", "10 to 14"
  )
)]

reference_5q0 <- fread(
  fs::path("FILEPATH")
)
reference_5q0_cbh <- reference_5q0[method_name == "CBH"]

cbh[, reference := ifelse(nid %in% reference_5q0_cbh$nid & !(age_group_id %in% 2:3 & enn_lnn_source == "age-sex"), 1, 0)]

cbh[outlier == 0, outlier_reason := "not outliered"]
cbh[outlier == 1, outlier_reason := "manual"]
cbh[outlier == 2, outlier_reason := ">15 years prior"]
cbh[outlier == 3, outlier_reason := "death cutoff = 7.5"]
cbh[outlier == 4, outlier_reason := "prep > age-sex"]

cbh[, process := "handoff 1"]

cbh <- rbind(
  cbh,
  gbd[
    life_table_parameter_id == 1 & age_group_name %in% cbh$age_group_name &
      ihme_loc_id %in% cbh$ihme_loc_id & sex_id %in% 1:2
  ],
  fill = TRUE
)

cbh <- merge(
  cbh,
  locs[, c("location_id", "sort_order")],
  by = "location_id",
  all.x = TRUE
)

# Format SIBS data -------------------------------------------------------------

sibs <- handoff_1[source_type_name == "SIBS"]

sibs <- merge(
  sibs,
  locs[, c("location_id", "location_name")],
  by = "location_id",
  all.x = TRUE
)

sibs[sex_id == 1, sex := "Males"]
sibs[sex_id == 2, sex := "Females"]
sibs[sex_id == 3, sex := "Both"]

sibs <- sibs[,
  c("nid", "ihme_loc_id", "location_name", "year_id", "sex", "sex_id",
    "age_start", "age_end", "age_group_name", "mx", "mx_semi_adj", "mx_adj",
    "qx", "qx_semi_adj", "qx_adj", "outlier")
]

sibs[, process := "handoff 1"]

sibs <- rbind(
  sibs,
  gbd[
    life_table_parameter_id == 3 & age_group_name %in% sibs$age_group_name &
      ihme_loc_id %in% sibs$ihme_loc_id & sex_id %in% 1:2
  ],
  fill = TRUE
)

sibs <- merge(
  sibs,
  locs[, c("ihme_loc_id", "sort_order")],
  by = "ihme_loc_id",
  all.x = TRUE
)

# Generate graphs --------------------------------------------------------------

# 1. CBH data (mx) by location and sex, with year x and faceted by age
cbh_plots_year_x <- function(dt, y_var) {

  loc <- unique(dt$ihme_loc_id)
  s <- unique(dt$sex_id)

  gbd_temp <- cbh[process == "GBD 2023" & ihme_loc_id == loc & sex_id == s]

  dt[process == "handoff 1"] |>
    ggplot(aes(x = year_id, y = mx)) +
    geom_point(aes(shape = as.factor(outlier_reason), color = as.factor(nid), fill = as.factor(nid)), size = 2.25, alpha = 0.8) +
    geom_point(data = dt[reference == 1], shape = 1, alpha = 0.5, size = 3.5) +
    geom_line(aes(group = interaction(as.factor(nid)), color = as.factor(nid), alpha = 0.8)) +
    geom_line(data = gbd_temp, aes(y = mean), color = "gray10") +
    scale_x_continuous(limits = c(1950, 2025), breaks = seq(1950, 2020, 10), expand = c(0.01, 0.01)) +
    scale_shape_manual(values = c("not outliered" = 16, "manual" = 4, ">15 years prior" = 15, "death cutoff = 7.5" = 17, "prep > age-sex" = 13)) +
    scale_alpha(guide = "none") +
    labs(
      title = paste0("CBH for ", unique(dt$location_name), " (", loc, ") in ", unique(dt$sex)),
      subtitle = "Gray circle around point indicates it's reference data in 5q0.",
      x = "Year", y = "mx", color = "NID", fill = "NID", shape = "Outlier"
    ) +
    facet_wrap(~ as.factor(age_group_name), scales = "free", nrow = 2, ncol = 4) +
    theme_bw()

}

list_plots <- cbh[order(sort_order, sex_id)] |>
  split(by = c("ihme_loc_id", "sex_id"), drop = TRUE, sorted = FALSE) |>
  purrr::map(\(x) cbh_plots_year_x(dt = x, y_var = "mx"))

file_name_out <- "initial_cbh_by_loc_year_x"
dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  \(x, y) withr::with_pdf(
    new = fs::path("FILEPATH"),
    height = 10,
    width = 18,
    code = plot(x)
  )
)

files_temp <- fs::path("FILEPATH")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path("FILEPATH")
)

# 2a. SIBS data (qx) by location and sex, with year x and faceted by age
sibs_plots_year_x <- function(dt, y_var) {

  loc <- unique(dt$ihme_loc_id)
  s <- unique(dt$sex_id)

  gbd_temp <- sibs[process == "GBD 2023" & ihme_loc_id == loc & sex_id == s]

  dt[process == "handoff 1"] |>
    ggplot(aes(x = year_id, color = as.factor(nid))) +
    geom_point(aes(y = qx, shape = as.factor(outlier)), size = 2.25, alpha = 0.15) +
    geom_point(aes(y = qx_semi_adj, shape = as.factor(outlier)), size = 2.25, alpha = 0.4) +
    geom_point(aes(y = qx_adj, shape = as.factor(outlier)), size = 2.25, alpha = 0.9) +
    geom_line(aes(y = qx, group = interaction(as.factor(nid)), alpha = 0.2, colour = as.factor(nid))) +
    geom_line(aes(y = qx_semi_adj, group = interaction(as.factor(nid)), alpha = 0.5, colour = as.factor(nid))) +
    geom_line(aes(y = qx_adj, group = interaction(as.factor(nid)), alpha = 0.9, colour = as.factor(nid))) +
    geom_line(data = gbd_temp, aes(y = mean), color = "gray10") +
    scale_shape_manual(values = c("0" = 16, "1" = 17)) +
    labs(
      title = paste0("SIBS for ", unique(dt$location_name), " (", loc, ") in ", unique(dt$sex)),
      x = "Year", y = "qx", color = "NID", shape = "Outlier"
    ) +
    facet_wrap(~ age_group_name, scales = "free", nrow = 2, ncol = 4) +
    scale_alpha(guide = "none") +
    theme_bw() +
    theme(legend.position = "bottom")

}

list_plots <- sibs[order(sort_order, sex_id)] |>
  split(by = c("ihme_loc_id", "sex_id"), drop = TRUE, sorted = FALSE) |>
  purrr::map(\(x) sibs_plots_year_x(dt = x, y_var = "qx"))

file_name_out <- "initial_sibs_by_loc_year_x"
dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  \(x, y) withr::with_pdf(
    new = fs::path("FILEPATH"),
    height = 9,
    width = 15,
    code = plot(x)
  )
)

files_temp <- fs::path("FILEPATH")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path("FILEPATH")
)

# 2b. SIBS data (qx) by location and sex, with age x and faceted by year
sibs_plots_age_x <- function(dt, y_var) {

  loc <- unique(dt$ihme_loc_id)
  s <- unique(dt$sex_id)

  gbd_temp <- sibs[
    process == "GBD 2023" & ihme_loc_id == loc & sex_id == s & year_id %in% unique(dt[process == "handoff 1"]$year_id)
  ]

  dt[process == "handoff 1"] |>
    ggplot(aes(x = age_start, color = as.factor(nid))) +
    geom_point(aes(y = qx, shape = as.factor(outlier)), size = 2.25, alpha = 0.15) +
    geom_point(aes(y = qx_semi_adj, shape = as.factor(outlier)), size = 2.25, alpha = 0.4) +
    geom_point(aes(y = qx_adj, shape = as.factor(outlier)), size = 2.25, alpha = 0.9) +
    geom_line(aes(y = qx, group = interaction(as.factor(nid)), alpha = 0.3, colour = as.factor(nid))) +
    geom_line(aes(y = qx_semi_adj, group = interaction(as.factor(nid)), alpha = 0.9, colour = as.factor(nid))) +
    geom_line(aes(y = qx_adj, group = interaction(as.factor(nid)), alpha = 0.9, colour = as.factor(nid))) +
    geom_line(data = gbd_temp, aes(x = age_group_years_start, y = mean), color = "gray10") +
    scale_shape_manual(values = c("0" = 16, "1" = 17)) +
    labs(
      title = paste0("SIBS for ", unique(dt$location_name), " (", loc, ") in ", unique(dt$sex)),
      x = "Year", y = "qx", color = "NID", shape = "Outlier"
    ) +
    facet_wrap(~ year_id, scales = "free") +
    scale_alpha(guide = "none") +
    theme_bw() +
    theme(legend.position = "bottom")
}

list_plots <- sibs[order(sort_order, sex_id)] |>
  split(by = c("ihme_loc_id", "sex_id"), drop = TRUE, sorted = FALSE) |>
  purrr::map(\(x) sibs_plots_age_x(dt = x, y_var = "qx"))

file_name_out <- "initial_sibs_by_loc_age_x"
dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  \(x, y) withr::with_pdf(
    new = fs::path("FILEPATH"),
    height = 13,
    width = 16,
    code = plot(x)
  )
)

files_temp <- fs::path("FILEPATH")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path("FILEPATH")
)
