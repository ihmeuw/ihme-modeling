# Meta --------------------------------------------------------------------
#'
#' Summary: Graph harmonized envelope
#'
#' Inputs:
#' 1. Reported and total COVID
#' 2. Positive and Negative OPRM
#' 3. No-shock envelope
#' 4. Implied EM
#' 5. Indirect covid shocks
#'
#' Methods:
#' 1. Convert deaths into death rates
#' 2. Add Negative OPRM to the No-shock envelope
#' 3. Proportionally redistribute implied EM among the no-shock envelope,
#'    covid deaths, and positive oprm
#' 4. Ensure that total covid is less than reported covid, if not reattempt
#'    redistribution with a different weighting scheme.
#' 5. convert rates back to death counts
#' 6. Format and save new no-shock envelope, postive OPRM, and Covid deaths


# Read Arguments ----------------------------------------------------------

parser <- argparse::ArgumentParser()

parser$add_argument(
  "--dir_output",
  type = "character",
  required = !interactive(),
  default = "FILEPATH",
  help = "Output directory for em adjustment"
)
parser$add_argument(
  "--run_id_ns_env",
  type = "integer",
  required = !interactive(),
  default = 620L,
  help = "No shock Envelope - no pandemic years data"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)

# Load packages -----------------------------------------------------------

library(data.table)
library(ggplot2)
library(furrr)


# Set parameters ----------------------------------------------------------

plan("multisession")

analysis_years <- 2010:2021

dir_diagnostics <- fs::path(dir_output, "diagnostics")
file_name_out <- "compare_noshock_env_with_em"


# Load maps ---------------------------------------------------------------

map_locs <- fread(
  fs::path(
    dir_output, "loc_map", ext = "csv"
  )
)

map_ages <- fread(
  fs::path(
    dir_output, "age_map", ext = "csv"
  )
)

# Data subsets ------------------------------------------------------------

cols_final <- c(
  "ihme_loc_id", "location_name",
  "year_id",
  "sex_name",
  "age_group_name", "age_start",
  "deaths"
)

subset_locs <- map_locs[level >= 3, location_id]


# Load data ---------------------------------------------------------------

dt_nslt_old <- demInternal::get_dem_outputs(
  "mlt death number estimate",
  run_id = run_id_ns_env,
  year_ids = analysis_years,
  sex_ids = 1:2,
  name_cols = TRUE
)


# dt_wslt <- demInternal::get_dem_outputs(
#   "no shock death number estimate",
#   run_id = run_id_wslt,
#   year_ids = analysis_years,
#   sex_ids = 1:2,
#   estimate_stage_ids = 5,
#   name_cols = TRUE
# )
# dt_vrp <- demInternal::get_dem_outputs(
#   "death number empirical data",
#   run_id = 354,
#   year_ids = analysis_years,
#   sex_ids = 1:2,
#   name_cols = TRUE
# )

dt_harmonizer <- fs::path(dir_output, "summary/scaled_env") |>
  fs::dir_ls(regexp = "scaled") |>
  future_map(readRDS) |>
  rbindlist(use.names = TRUE)


dt_harmonizer <- dt_harmonizer[!age_group_id %in% c(28) & sex_id != 3]

dt_unscaled <- fs::path(dir_output, "summary/loc_specific_env") |>
  fs::dir_ls(regexp = "harmonized") |>
  future_map(readRDS) |>
  rbindlist()

dt_unscaled <- dt_unscaled[!age_group_id %in% c(28) & sex_id != 3]

dt_indirect_covid <- fread(
  fs::path(
    dir_diagnostics,
    "adjusted_cc_inputs_for_graphing.csv"
  )
)

# VRP
dt_vrp <- fread(
  fs::path(
    dir_output,
    "diagnostics",
    "vr_for_plots.csv"
  )
)


# Prep data ---------------------------------------------------------------


## Data-specific formatting ----

dt_indirect_covid <- dt_indirect_covid[measure == "indirect_mean"]

# dt_nslt
dt_nslt_old <- dt_nslt_old[age_group_id %in% map_ages$age_group_id &
                             sex_id != 3]

# # dt_wslt
# dt_wslt <- dt_wslt[age_group_id %in% map_ages$age_group_id &
#                              sex_id != 3]

# for harmonizer results, use the life table results for 2019

dt_harmonizer <- demInternal::ids_names(dt_harmonizer, extra_output_cols = c("ihme_loc_id", "sex_name", "age_start", "age_end"))
dt_harmonizer[, mean := no_shock_mean]


dt_2019 <- dt_nslt_old[year_id == 2019 & location_id %in% graph_locs]

dt_harmonizer <- rbind(dt_harmonizer, dt_2019, fill = TRUE)

# create dt plus covid
dt_plus_covid <- copy(dt_harmonizer[year_id != 2019])

dt_plus_covid[, mean := no_shock_mean + covid_mean]

dt_plus_covid <- rbind(dt_plus_covid, dt_2019, fill = TRUE)

# create DT plus EM
dt_plus_em <- copy(dt_harmonizer[year_id != 2019])
dt_plus_em[dt_indirect_covid, deaths_indirect := i.deaths, on = .(location_id, year_id, sex_id, age_group_id)]

dt_plus_em[, mean := no_shock_mean + covid_mean + pos_oprm_mean + deaths_indirect]

dt_plus_em <- rbind(dt_plus_em, dt_2019, fill = TRUE)

# VRP
dt_vrp[map_ages, age_group_name := i.age_group_name, on = "age_group_id"]
dt_vrp[map_locs, ihme_loc_id := i.ihme_loc_id, on = .(location_id)]
dt_vrp[, mean := value]
dt_vrp[, version := "VR"]

dt_vrp <- dt_vrp[!age_group_name %in% c("1", "<1 year")]
dt_vrp <- dt_vrp[mean != 0]

## Combine data ----

list_prepped <- list(
  #"With 2020-21 Env" = dt_wslt,
  "Old no-shock Env" = dt_nslt_old,
  "New no-shock Env" = dt_harmonizer,
  "New Unscaled env" = dt_unscaled,
  "New no-shock Env + COVID" = dt_plus_covid,
  "New no-shock Env + EM" = dt_plus_em
)

dt_prepped <- list_prepped |>
  rbindlist(idcol = "version", fill = TRUE)


dt_prepped <- dt_prepped[location_id %in% graph_locs]


dt_1_4 <- dt_prepped[age_group_name %in% c("12 to 23 months", "2 to 4"), .(mean = sum(mean)),
                     by = .(location_id, location_name, ihme_loc_id, year_id, sex_name, sex_id, version)]
dt_1_4[, `:=` (age_group_name = "1 to 4",
               age_start = 1L,
               age_end = 5L)]

dt_prepped <- dt_prepped[!age_group_name %in% c("12 to 23 months", "2 to 4")]

dt_prepped <- rbind(dt_prepped, dt_1_4, fill = TRUE, use.names = TRUE)

vr_1_4 <- dt_vrp[age_group_name %in% c("12 to 23 months", "2 to 4"), .(mean = sum(mean)),
                 by = .(location_id, ihme_loc_id, year_id, sex_id, outlier, deaths_source, version)]
vr_1_4[, `:=` (age_group_name = "1 to 4",
               age_start = 1L,
               age_end = 5L)]

dt_vrp <- dt_vrp[!age_group_name %in% c("12 to 23 months", "2 to 4")]

dt_vrp <- rbind(dt_vrp, vr_1_4, fill = TRUE, use.names = TRUE)
dt_vrp <- dt_vrp[mean != 0]


# save this data
readr::write_csv(
  dt_prepped[nchar(ihme_loc_id) == 3, .(ihme_loc_id, location_name, year_id, sex_name, age_group_name, age_start, age_end, version, deaths = mean)],
  fs::path(
    dir_diagnostics,
    "deaths_graphs_data.csv"
  )
)


# Make plots --------------------------------------------------------------

make_plot <- function(data, deaths_scale = 1e5) {

  loc_code <- unique(data$ihme_loc_id)
  loc_name <- unique(data$location_name)
  yr <- unique(data$year_id)
  sex <- unique(data$sex_name)

  dt_vrp_temp <- dt_vrp[ihme_loc_id == loc_code & sex_id == ifelse(sex == "female", 2, 1)]

  ggplot(data, aes(x = year_id, y = mean, color = version)) +
    geom_line(alpha = 0.6) +
    geom_point() +
    geom_point(data = dt_vrp_temp, aes(x = year_id, y = mean, shape = deaths_source, alpha = outlier)) +
    facet_wrap(~ factor(reorder(age_group_name, age_start)), scales = "free_y") +
    scale_shape_manual(values = c(4, 3)) +
    # scale_y_continuous(labels = scales::label_number(
    #   scale = deaths_scale,
    #   scale_cut = scales::cut_short_scale()
    # )) +
    scale_alpha_continuous(breaks = 0:1, range = c(0.8, 0.1), limits = 0:1) +
    scale_color_brewer(palette = "Dark2") +
    theme_light() +
    labs(
      title = glue::glue("({loc_code}) {loc_name}, {sex}s"),
      x = "Year",
      y = glue::glue("Deaths"),
      color = "Version"
    )

}

list_plots <- dt_prepped |>
  split(by = c("ihme_loc_id", "sex_name")) |>
  lapply(make_plot)

list_plots[[1]]


# Save --------------------------------------------------------------------

dir_out_tmp <- tempdir()

furrr::future_iwalk(
  list_plots,
  ~withr::with_pdf(
    new = fs::path(dir_out_tmp, paste(file_name_out, .y, sep = "-"), ext = "pdf"),
    width = 15,
    height = 10,
    code = plot(.x)
  )
)

files_temp <- fs::path(dir_out_tmp, paste(file_name_out, names(list_plots), sep = "-"), ext = "pdf")
stopifnot(all(fs::file_exists(files_temp)))

qpdf::pdf_combine(
  input = files_temp,
  output = fs::path(dir_diagnostics, file_name_out, ext = "pdf")
)

