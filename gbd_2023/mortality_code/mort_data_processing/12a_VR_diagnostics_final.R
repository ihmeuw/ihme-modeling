
# Meta -------------------------------------------------------------------------

# Description: Create data type-specific diagnostics for vetting the final dataset handoff

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in gbd estimates for additional trend lines
#   4. Read in and format final VR data
#   5. Generate graphs

# Outputs:
#   1a. "FILEPATH": final vr deaths or mx by age over time showing outliers
#   1b. "FILEPATH": final vr mx by age over time showing outliers

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

# wpp mx estimates
wpp <- fread(fs::path("FILEPATH"))

# Read in gbd estimates for additional trend lines -----------------------------

# read in and clean gbd estimates for VR
gbd_mx_est <- mortdb::get_mort_outputs(
  model_name = "with shock life table",
  model_type = "estimate",
  run_id = "best",
  gbd_year = gbd_year,
  life_table_parameter_ids = 1
)
gbd_mx_est <- gbd_mx_est[, c("location_id", "year_id", "sex_id", "age_group_id", "mean")]
setnames(gbd_mx_est, "mean", "mx")
gbd_mx_est <- melt(
  gbd_mx_est,
  measure.vars = "mx",
  value.name = "value_gbd",
  variable.name = "measure"
)

# make srs deaths
srs_pop <- fread(fs::path("FILEPATH"))
srs_pop <- srs_pop[ihme_loc_id %like% "IND" & source_type == "SRS"]
srs_pop <- merge(age_map_gbd, srs_pop, by = c("age_start", "age_end"))
setnames(srs_pop, "census_pop", "srs_pop")
srs_pop <- srs_pop[, c("location_id", "year_id", "sex_id", "age_group_id", "srs_pop")]
gbd_srs_est <- merge(
  gbd_mx_est,
  srs_pop,
  by = c("location_id", "year_id", "sex_id", "age_group_id")
)
gbd_srs_est[, ':=' (value_srs = value_gbd * srs_pop, measure = "deaths", srs_pop = NULL, value_gbd = NULL)]

gbd_deaths_est <- mortdb::get_mort_outputs(
  model_name = "with shock death number",
  model_type = "estimate",
  run_id = "best",
  gbd_year = gbd_year
)
gbd_deaths_est <- gbd_deaths_est[, c("location_id", "year_id", "sex_id", "age_group_id", "mean")]
setnames(gbd_deaths_est, "mean", "deaths")
gbd_deaths_est <- melt(
  gbd_deaths_est,
  measure.vars = "deaths",
  value.name = "value_gbd",
  variable.name = "measure"
)
gbd_deaths_est <- merge(
  gbd_deaths_est,
  gbd_srs_est,
  by = c("location_id", "year_id", "sex_id", "age_group_id", "measure"),
  all.x = TRUE
)
gbd_deaths_est[!is.na(value_srs), value_gbd := value_srs]
gbd_deaths_est <- gbd_deaths_est[!(is.na(value_srs) & location_id %in% unique(gbd_srs_est[!location_id == 163]$location_id))]
gbd_deaths_est[, value_srs := NULL]

gbd_vr_est <- rbind(gbd_mx_est, gbd_deaths_est)
gbd_vr_est <- merge(
  age_names[, c("age_group_id", "age_group_name")],
  gbd_vr_est,
  by = "age_group_id",
  all.y = TRUE
)

gbd_vr_est[, series_name := "gbd"]


# Read in and format final VR data ---------------------------------------------

final_vr_data <- fread(
  fs::path("FILEPATH")
)

final_vr_data <- merge(
  age_names[, c("age_group_id", "age_group_name")],
  final_vr_data,
  by = "age_group_id",
  all.y = TRUE
)

final_vr_data <- final_vr_data[,
  c("nid", "underlying_nid", "location_id", "year_id", "sex_id", "age_group_id",
    "age_group_name", "deaths", "deaths_adj", "mx", "mx_adj", "outlier",
    "series_name", "rank", "onemod_process_type"
  )
]

# check for duplicates
final_vr_data[, dup := .N, by = names(final_vr_data)]
assert_values(final_vr_data, "dup", test = "equal", test_val = 1)
final_vr_data[, dup := NULL]

final_vr_data <- melt(
  final_vr_data,
  measure.vars = patterns("^deaths|^mx"),
  value.name = c("val"),
  variable.name = "measure"
)
final_vr_data[, measure := as.character(measure)]
final_vr_data[, comp_adj := ifelse(measure %like% "adj", "value_adj", "value")]
final_vr_data[measure %like% "adj", measure := gsub("_adj", "", measure)]

# check for duplicates
final_vr_data[, dup := .N, by = setdiff(names(final_vr_data), c("comp_adj", "val"))]
assert_values(final_vr_data, "dup", test = "equal", test_val = 2)
final_vr_data[, dup := NULL]

final_vr_data <- dcast(
  final_vr_data,
  ... ~ comp_adj,
  value.var = "val"
)

# add age splitting type
age_splitting_int <- fread(fs::path("FILEPATH"))
age_splitting_int <- age_splitting_int[, c("location_id", "year_id", "sex_id", "age_group_id", "mx_column")]
final_vr_data <-  merge(
  final_vr_data,
  age_splitting_int,
  by = c("location_id", "year_id", "sex_id", "age_group_id"),
  all.x = TRUE
)
final_vr_data[!onemod_process_type == "age-sex split data", mx_column := NA]

# age sort order
final_vr_data$age_group_name <- factor(
  final_vr_data$age_group_name,
  levels = gbd_age_order
)
gbd_vr_est$age_group_name <- factor(
  gbd_vr_est$age_group_name,
  levels = gbd_age_order
)
wpp$age_group_name <- factor(
  wpp$age_group_name,
  levels = gbd_age_order
)

final_vr_data[, outlier := as.character(outlier)]
final_vr_data[, outlier := ifelse(outlier == 0, "Not Outliered", "Outliered")]
final_vr_data[, outlier := factor(outlier, levels = c("Outliered", "Not Outliered"))]

final_vr_data <- final_vr_data[order(measure, location_id, year_id, sex_id, outlier)]

final_vr_data <- rbind(
  final_vr_data,
  gbd_vr_est[location_id %in% unique(final_vr_data$location_id) & sex_id %in% 1:2],
  wpp[location_id %in% unique(final_vr_data$location_id) & sex_id %in% 1:2, !c("ihme_loc_id")],
  fill = TRUE
)

final_vr_data <- merge(
  final_vr_data,
  locs[, c("location_id", "ihme_loc_id", "location_name", "sort_order")],
  by = "location_id",
  all.x = TRUE
)

# Generate graphs --------------------------------------------------------------

# 1a. final VR time series, by age and location
h2_vr_plots <- function(dt, y_var) {

  loc <- unique(dt$ihme_loc_id)
  s <- unique(dt$sex_id)

  temp_gbd <- dt[
    series_name == "gbd" & year_id >= estimation_year_start & year_id <= estimation_year_end &
      ihme_loc_id == loc & sex_id == s & measure == mm & age_group_id %in% age_map_gbd$age_group_id
  ]
  temp_wpp <- dt[
    series_name == "wpp" & year_id >= estimation_year_start & year_id <= estimation_year_end &
      ihme_loc_id == loc & sex_id == s & measure == mm & age_group_id %in% age_map_gbd$age_group_id
  ]

  sex_name <- sex_names[sex_id == s, sex]

  dt[!(series_name %in% c("gbd", "wpp"))] |>
    ggplot(aes(x = year_id, group = interaction(as.factor(series_name)), shape = as.factor(series_name))) +
    geom_point(aes(y = value, color = outlier), alpha = 0.3) +
    geom_point(aes(y = value_adj, color = outlier), alpha = 0.9) +
    scale_shape_manual(
      values = c(
        "WHO" = 1, "DYB" = 2, "DSP" = 3, "MCCD" = 3, "HMD" = 4, "SRS" = 5,
        "CRVS" = 6, "low_gran" = 0, "Custom" = 7, "SSPC" = 8, "Census" = 9
      )
    ) +
    labs(shape = "Category") +
    ggnewscale::new_scale("shape") +
    scale_shape_manual(values = c("kreg" = 1, "spxmod" = 0, "gbd_mx" = 2)) +
    labs(shape = "Splitting Method") +
    geom_point(
      data = dt[onemod_process_type == "age-sex split data"],
      aes(y = value, shape = as.factor(mx_column)),
      alpha = 0.3, size = 3, color = "grey30"
    ) +
    geom_point(
      data = dt[onemod_process_type == "age-sex split data"],
      aes(y = value_adj, shape = as.factor(mx_column)),
      alpha = 0.9, size = 3, color = "grey30"
    ) +
    geom_line(data = temp_gbd, mapping = aes(x = year_id, y = value_gbd), color = "grey10", linewidth = 0.3) +
    geom_line(data = temp_wpp, mapping = aes(x = year_id, y = value_wpp), color = "brown", linewidth = 0.3) +
    xlim(estimation_year_start, estimation_year_end) +
    facet_wrap(~age_group_name, scales = "free_y") +
    scale_color_manual(values = c("Not Outliered" = "purple", "Outliered" = "orange")) +
    labs(
      title = paste0(unique(dt$location_id), "; ", unique(dt$location_name),"; ", unique(dt$ihme_loc_id), ": ", sex_name, "; VR"),
      x = "Year", y = str_to_title(mm), color = "Outlier",
      caption = paste0(
        "1. Transparent points are not completeness adjusted                         \n",
        "2. Surrounding black shapes indicate age/sex split point                    \n",
        "3. Grey trend lines show current best with shock GBD", gbd_year, " estimates\n",
        "4. Brown trend lines show wpp comparison estimates                         "
      )
    ) +
    theme_bw()

}

for (mm in c("mx", "deaths")) {

  vr_plot_data <- final_vr_data[measure == mm & year_id >= estimation_year_start & year_id <= estimation_year_end]

  list_plots <- vr_plot_data[order(sort_order, sex_id)] |>
    split(by = c("location_id", "sex_id"), drop = TRUE, sorted = FALSE) |>
    purrr::map(\(x) h2_vr_plots(dt = x, y_var = mm))

  file_name_out <- paste0("final_vr_gbd_ages_", mm)
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

# 1b. final VR mx over age, by year and location
h2_vr_mx_plots <- function(dt, y_var) {

  loc <- unique(dt$ihme_loc_id)
  loc_id <- unique(dt$location_id)
  s <- unique(dt$sex_id)

  temp_gbd <- gbd_vr_est[
    year_id %in% unique(dt[!(series_name %in% c("gbd", "wpp"))]$year_id) & location_id == loc_id & sex_id == s &
      measure == "mx" & age_group_id %in% age_map_gbd$age_group_id
  ]
  temp_gbd[, series_name := "gbd"]
  temp_wpp <- wpp[
    year_id %in% unique(dt[!(series_name %in% c("gbd", "wpp"))]$year_id) & location_id == loc_id & sex_id == s &
      measure == "mx" & age_group_id %in% age_map_gbd$age_group_id
  ]
  temp_wpp[, series_name := "wpp"]

  sex_name <- sex_names[sex_id == s, sex]

  dt[!(series_name %in% c("gbd", "wpp"))] |>
    ggplot(aes(x = age_group_name, group = interaction(as.factor(series_name)), shape = as.factor(series_name))) +
    geom_point(aes(y = log(value), color = outlier), alpha = 0.3) +
    geom_point(aes(y = log(value_adj), color = outlier), alpha = 0.9) +
    scale_shape_manual(values = c("WHO" = 1, "DYB" = 2, "DSP" = 3, "MCCD" = 3, "HMD" = 4, "SRS" = 5, "CRVS" = 6, "low_gran" = 0, "Custom" = 7, "SSPC" = 8, "Census" = 9)) +
    labs(shape = "Category") +
    ggnewscale::new_scale("shape") +
    scale_shape_manual(values = c("kreg" = 1, "spxmod" = 0, "gbd_mx" = 2)) +
    labs(shape = "Splitting Method") +
    geom_point(data = dt[onemod_process_type == "age-sex split data"], aes(y = log(value), shape = as.factor(mx_column)), alpha = 0.3, size = 3, color = "grey30") +
    geom_point(data = dt[onemod_process_type == "age-sex split data"], aes(y = log(value_adj), shape = as.factor(mx_column)), alpha = 0.9, size = 3, color = "grey30") +
    geom_line(data = temp_gbd, aes(y = log(value_gbd)), color = "grey10", linewidth = 0.3) +
    geom_line(data = temp_wpp, aes(y = log(value_wpp)), color = "brown", linewidth = 0.3) +
    facet_wrap(~year_id, scales = "free_y") +
    scale_color_manual(values = c("Not Outliered" = "purple", "Outliered" = "orange")) +
    labs(
      title = paste0(loc_id, "; ", unique(dt$location_name),"; ", loc, ": ", sex_name, "; VR"),
      x = "Age Name", y = "Mx (log)", color = "Outlier",
      caption = paste0(
        "1. Transparent points are not completeness adjusted                         \n",
        "2. Surrounding black shapes indicate age/sex split point                    \n",
        "3. Grey trend lines show current best with shock GBD2023 estimates\n",
        "4. Brown trend lines show wpp comparison estimates                         "
      )
    ) +
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))

}

vr_plot_data <- final_vr_data[measure == "mx"]

list_plots <- vr_plot_data[order(sort_order, sex_id)] |>
  split(by = c("location_id", "sex_id"), drop = TRUE, sorted = FALSE) |>
  purrr::map(\(x) h2_vr_mx_plots(dt = x, y_var = mm))

file_name_out <- paste0("final_vr_log_mx_by_age_loc_year")
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
