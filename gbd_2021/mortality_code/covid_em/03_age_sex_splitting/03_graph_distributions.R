# Meta --------------------------------------------------------------------
#'
#' Summary: Graphs global excess deaths and rates by age and sex
#'
#' Inputs: (all inputs are summary level)
#' 1. Global excess mortality rates
#'
#' Methods:
#' 1. Load global excess (positive and negative)
#' 2. Format
#' 3. Graph


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
args <- parser$parse_args()

list2env(args, .GlobalEnv)


# Setup -------------------------------------------------------------------

id_cols <- c("location_id", "year_id", "sex_id", "age_group_id")
agg_id_cols <- c("year_id", "sex_id", "age_group_id", "draw")

loc_map <- fread(fs::path(dir_output, "loc_map", ext = "csv"))
age_map <- fread(fs::path(dir_output, "age_map", ext = "csv"))

dir_results <- fs::path(dir_output, "summary", "01_implied_em")


# Load data ---------------------------------------------------------------

files_global <- list.files(
  dir_results,
  pattern = glue::glue("global_em_rate"),
  full.names = TRUE
)

dt_global <- lapply(files_global, readRDS) |> rbindlist(fill = TRUE)


# Format data -------------------------------------------------------------

dt_global[, type := ifelse(is.na(total_excess_sum_mean), "Negative", "Positive")]

dt_global[age_map, age := i.age_group_years_start, on = .(age_group_id)]

dt_global[, sex := ifelse(sex_id == 1, "Male", "Female")]


# Graph global ------------------------------------------------------------

pdf(
  fs::path(
    dir_output, "diagnostics", glue::glue("global_em_rates"),
    ext = "pdf"
  ),
  width = 15,
  height = 9
)
for(i in c("Positive", "Negative")) {
  p <- ggplot(dt_global[type == i], aes(x = age, y = em_rate_mean)) +
    geom_line() +
    facet_wrap(~sex) +
    theme_bw() +
    labs(
      title = glue::glue("{i} Global Excess mortality rate"),
      y = "Excess mx"
    )
  print(p)
}

dev.off()


pdf(
  fs::path(
    dir_output, "diagnostics", glue::glue("global_em_deaths"),
    ext = "pdf"
  ),
  width = 15,
  height = 9
)

for(i in c("Positive", "Negative")) {
  p <- ggplot(dt_global[type == i], aes(x = age, y = excess_implied_sum_mean)) +
    geom_line() +
    facet_wrap(~sex) +
    theme_bw() +
    labs(
      title = glue::glue("{i} Global Excess Death"),
      y = "Deaths"
    )
  print(p)
}

dev.off()

