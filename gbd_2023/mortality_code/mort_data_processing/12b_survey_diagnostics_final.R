
# Meta -------------------------------------------------------------------------

# Description: Create data type-specific diagnostics for vetting the final dataset handoff

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in gbd estimates for additional trend lines
#   4. Read in and format final survey data
#   5. Generate graphs

# Outputs:
#   1a. "FILEPATH": all CBH and SBH data by age over time showing outliers
#   1b. "FILEPATH": all SIBS data by age over time showing outliers
#   1c. "FILEPATH": all Survey and Census data by age over time showing outliers

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

# read in and clean gbd estimates for survey
gbd_survey_est <- mortdb::get_mort_outputs(
  model_name = "with shock life table",
  model_type = "estimate",
  run_id = "best",
  gbd_year = gbd_year,
  life_table_parameter_ids = 1,
  demographic_metadata = TRUE
)

gbd_survey_est[sex == "Male", sex := "Males"]
gbd_survey_est[sex == "Female", sex := "Females"]

# Read in and format final survey data -----------------------------------------

handoff_2 <- setDT(arrow::read_parquet(fs::path("FILEPATH")))
survey_final <- fread(fs::path("FILEPATH"))

survey_results <- rbind(
  survey_final[source_type_name %in% c("CBH", "SBH", "SIBS")],
  handoff_2[source_type_name %in% c("Survey", "Census"), !c("age_start", "age_end", "ihme_loc_id")],
  fill = TRUE
)

# add in age_group_name
age_names <- mortdb::get_age_map(type = "gbd") |>
  select(age_group_id, age_group_name, age_group_years_start, age_group_years_end)
setnames(age_names, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

survey_results <- survey_results |>
  left_join(age_names, by = "age_group_id")

survey_results <- survey_results |>
  left_join(locs, by = "location_id")

stopifnot(sum(is.na(survey_results$age_group_name)) == 0)
stopifnot(sum(is.na(survey_results$location_name)) == 0)

survey_results[source_type_name == "CBH" & age_end_orig != 5, source_type_name := "CBH (split nn)"]
survey_results[source_type_name == "CBH" & age_end_orig == 5, source_type_name := "CBH (split 5q0)"]

# add age splitting type
age_splitting_int <- fread(fs::path("FILEPATH"))
age_splitting_int <- age_splitting_int[, c("location_id", "year_id", "sex_id", "age_group_id", "mx_column")]

survey_results <- merge(
  survey_results,
  age_splitting_int,
  by = c("location_id", "year_id", "sex_id", "age_group_id"),
  all.x = TRUE
)
survey_results[onemod_process_type == "standard gbd age group data", mx_column := NA]

gbd_survey_est <- gbd_survey_est[age_group_name %in% survey_results$age_group_name]

# Generate graphs --------------------------------------------------------------

# 1a. final CBH/SBH time series, by age and location
# 1b. final SIBS time series, by age and location
# 1c. final Survey_Census time series, by age and location
h2_survey_plots <- function(dt, y_var) {

  if (nrow(dt) != nrow(dt[process == "gbd"])) {

    loc <- unique(dt$ihme_loc_id)
    s <- unique(dt$sex_id)

    gbd_temp <- dt[
      process == "gbd" & ihme_loc_id == loc & sex_id == s &
        age_group_id %in% unique(dt[process == "handoff 2"]$age_group_id)
    ]

    if ("SIBS" %in% dt[process == "handoff 2"]$source_type_name) {
      year_start <- 1970
    } else {
      year_start <- 1950
    }
    gbd_temp <- gbd_temp[year_id %in% year_start:2023]
    dt <- dt[year_id >= year_start]

    dt[process == "handoff 2"] |>
      ggplot(aes(x = year_id, y = mx)) +
      geom_point(
        data = dt[onemod_process_type == "age-sex split data"],
        mapping = aes(color = as.factor(nid_underlying_nid), shape = source_type_name),
        alpha = 0.9, size = 2.25
      ) +
      geom_point(
        data = dt[onemod_process_type == "standard gbd age group data"],
        mapping = aes(color = as.factor(nid_underlying_nid), shape = source_type_name),
        alpha = 0.3, size = 2.25
      ) +
      geom_point(
        data = dt[onemod_process_type == "age-sex split data" & outlier == 0],
        mapping = aes(color = as.factor(nid_underlying_nid), fill = as.factor(nid_underlying_nid), shape = source_type_name),
        alpha = 0.9, size = 2.25
      ) +
      geom_point(
        data = dt[onemod_process_type == "standard gbd age group data" & outlier == 0],
        mapping = aes(color = as.factor(nid_underlying_nid), fill = as.factor(nid_underlying_nid), shape = source_type_name),
        alpha = 0.3, size = 2.25
      ) +
      scale_color_manual(
        breaks = sort(unique(dt$nid_underlying_nid)),
        values = viridis::turbo(length(unique(dt$nid_underlying_nid)))
      ) +
      scale_fill_manual(
        breaks = sort(unique(dt$nid_underlying_nid)),
        values = viridis::turbo(length(unique(dt$nid_underlying_nid)))
      ) +
      scale_shape_manual(
        values = c(
          "SBH" = 21, "SIBS" = 21, "Survey" = 21, "CBH" = 22, "CBH (split 5q0)" = 24,
          "Census" = 24, "CBH (split nn)" = 25
        )
      ) +
      labs(shape = "Source Type") +
      ggnewscale::new_scale("shape") +
      scale_shape_manual(values = c("kreg" = 5, "spxmod" = 0, "gbd_mx" = 2)) +
      scale_x_continuous(breaks = seq(year_start, 2020, 10)) +
      labs(shape = "Splitting Method") +
      geom_point(
        data = dt,
        aes(y = mx, shape = as.factor(mx_column)),
        alpha = 0.3, size = 3, color = "grey30"
      ) +
      geom_line(data = gbd_temp, mapping = aes(y = mean), color = "black") +
      facet_wrap(~ as.factor(age_group_name), scales = "free") +
      labs(
        title = paste0(
          unique(dt$location_id), "; ", unique(dt$location_name),"; ", loc, ": ",
          ifelse(s == 1, "Males", "Females"), "; Age-Sex Split ", source, " Diagnostics"
        ),
        x = "Year",
        y = "Mx",
        color = "NID"
      ) +
      guides(fill = "none") +
      theme_bw()

  }

}

for (source in c("SIBS", "CBH_SBH", "Survey_Census")) {

  print(paste0("Now graphing split survey data for: ", source))

  if (source == "SIBS") {

    plot_data <- survey_results[source_type_name == "SIBS"]

  } else if (source == "CBH_SBH") {

    plot_data <- survey_results[source_type_name %in% c("CBH", "CBH (split nn)", "CBH (split 5q0)", "SBH") & age_end <= 5]
    plot_data[, age_group_name := factor(
      age_group_name,
      levels = c(
        "Early Neonatal", "Late Neonatal", "1-5 months", "6-11 months",
        "12 to 23 months", "2 to 4", "5 to 9", "10 to 14"
      )
    )]

  } else if (source == "Survey_Census") {

    plot_data <- survey_results[source_type_name %in% c("Survey", "Census")]
    plot_data[, age_group_name := factor(
      age_group_name,
      levels = c(
        "Early Neonatal", "Late Neonatal", "Post Neonatal", "1-5 months",
        "6-11 months", "12 to 23 months", "2 to 4", "5 to 9", "10 to 14", "15 to 19",
        "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 plus",
        "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69", "70 to 74",
        "75 to 79", "80 to 84", "85 to 89", "90 to 94", "95 plus"
      )
    )]

  }

  plot_data <- plot_data[order(age_group_name)]

  plot_data[, nid_underlying_nid := ifelse(!is.na(underlying_nid), paste0(nid, "_", underlying_nid), nid)]

  plot_data[, process := "handoff 2"]
  gbd_survey_est[, process := "gbd"]

  plot_data[, loc_sex_age := paste(location_id, sex_id, age_group_id, sep = "_")]
  gbd_survey_est[, loc_sex_age := paste(location_id, sex_id, age_group_id, sep = "_")]

  plot_data <- rbind(
    plot_data,
    gbd_survey_est[loc_sex_age %in% plot_data$loc_sex_age],
    fill = TRUE
  )
  plot_data[, loc_sex_age := NULL]

  list_plots <- plot_data[order(sort_order, sex_id)] |>
    split(by = c("ihme_loc_id", "sex_id"), drop = TRUE, sorted = FALSE) |>
    purrr::map(\(x) h2_survey_plots(dt = x, y_var = "mx"))

  file_name_out <- paste0("age-sex_split_", source)
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
