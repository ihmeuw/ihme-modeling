# Meta --------------------------------------------------------------------
#'
#' Summary: Graphs excess deaths and rates by age and sex
#'
#' Inputs: (all inputs are summary level)
#' 1. Implied excess mortality for each location with VR
#' 2. Global excess mortality rates
#'
#' Methods:
#' 1. Load implied excess
#' 2. Sum together excess to create global distribution of EM
#' 3. Divide by population to get EM rates
#' 4. Save


# Load packages -----------------------------------------------------------

library(data.table)
library(ggplot2)


# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

parser$add_argument(
  "--dir_output",
  type = "character",
  required = !interactive(),
  default = "FILEPATH",
  help = "Output directory for em adjustment"
)
parser$add_argument(
  "--current_year",
  type = "integer",
  required = !interactive(),
  default = 2021L,
  help = "year ID"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)


# Setup -------------------------------------------------------------------

id_cols <- c("location_name", "ihme_loc_id", "sex", "age")

loc_map <- fread(fs::path(dir_output, "loc_map", ext = "csv"))
age_map <- fread(fs::path(dir_output, "age_map", ext = "csv"))

dir_results <- fs::path(dir_output, "summary", "04_adjusted_age_sex")


# Load data ---------------------------------------------------------------

files_em <- list.files(
  dir_results,
  pattern = glue::glue("adjusted_em.*{current_year}"),
  full.names = TRUE
)

dt_em <- lapply(files_em, readRDS) |> rbindlist()


# Format data -------------------------------------------------------------

# add columns used for graphing
dt_em[loc_map, `:=` (location_name = i.location_name,
                     ihme_loc_id = i.ihme_loc_id),
      on = .(location_id)
]
dt_em[age_map, age := i.age_group_years_start, on = .(age_group_id)]

# add sex labels
dt_em[, sex := ifelse(sex_id == 1, "Male", "Female")]

# calculate total excess
dt_em[, total_excess_deaths := sum(new_excess_deaths_mean), by = .(location_id, year_id)]
dt_em[, old_total_excess_deaths := sum(old_excess_deaths_mean), by = .(location_id, year_id)]

# calculate proportions of deaths
dt_em[, new_prop := new_excess_deaths_mean / total_excess_deaths]
dt_em[, old_prop := old_excess_deaths_mean / old_total_excess_deaths]

# melt to compare new vs old distribution
dt_em <- melt(
  dt_em,
  id.vars = c(id_cols, "total_excess_deaths", "old_total_excess_deaths"),
  measure.vars = c(
    "old_em_rate_mean",
    "new_em_rate_mean"
  ),
  value.name = "em_rate",
  variable.name = "stage"
)


# Graph location specific -------------------------------------------------

make_plot <- function(data) {

  loc_name <- unique(data$location_name)
  loc_code <- unique(data$ihme_loc_id)

  total_excess <- formattable::comma(unique(data$total_excess_deaths))
  old_excess <- formattable::comma(unique(data$old_total_excess_deaths))

  gg <-
    ggplot(
      data,
      aes(x = age, y = em_rate, lty = stage)
    ) +
    geom_line() +
    scale_y_continuous(labels = scales::label_number(scale_cut = scales::cut_short_scale())) +
    theme_bw() +
    facet_wrap(~sex) +
    labs(
      title = glue::glue("({loc_code}) {loc_name}: Excess Mortality by age and sex {current_year}"),
      subtitle = glue::glue("Total Excess = {total_excess}"),
      x = "Age",
      y = "Excess Mortality Rate"
    )

  gg

}

# All locations
plot_list <- dt_em |>
  split(by = "total_excess_deaths",
        sorted = TRUE) |>
  lapply(make_plot)

withr::with_pdf(
  new = fs::path(dir_output, "diagnostics",
                 glue::glue("redistributed_em_{current_year}"),
                 ext = "pdf"),
  width = 15,
  height = 9,
  code = purrr::walk(plot_list, plot)
)

# Just subnationals
plot_list <- dt_em[nchar(ihme_loc_id) > 3] |>
  split(by = "total_excess_deaths",
        sorted = TRUE) |>
  lapply(make_plot)

withr::with_pdf(
  new = fs::path(dir_output, "diagnostics",
                 glue::glue("redistributed_em_subnationals{current_year}"),
                 ext = "pdf"),
  width = 15,
  height = 9,
  code = purrr::walk(plot_list, plot)
)


# Just nationals
plot_list <- dt_em[nchar(ihme_loc_id) == 3]  |>
  split(by = "total_excess_deaths",
        sorted = TRUE) |>
  lapply(make_plot)

withr::with_pdf(
  new = fs::path(dir_output, "diagnostics",
                 glue::glue("redistributed_em_nationals{current_year}"),
                 ext = "pdf"),
  width = 15,
  height = 9,
  code = purrr::walk(plot_list, plot)
)

