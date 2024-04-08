# Scale/aggregate final results
# do this by year

# Also, save out draw files for COVID and OPRM
# OR maybe not

# Meta --------------------------------------------------------------------
#' Author: Darwin Jones
#'
#' Summary: distribute implied EM over the no-shock envelope, covid,
#' and
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

# Load packages -----------------------------------------------------------

library(data.table)
library(furrr)
library(mortcore)
library(mortdb)

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
  default = 2020L,
  help = "year ID"
)

args <- parser$parse_args()

list2env(args, .GlobalEnv)


# Load Location and Age Maps ----------------------------------------------

age_map <- fread(fs::path(dir_output, "age_map", ext = "csv"))
loc_map <- fread(fs::path(dir_output, "loc_map", ext = "csv"))


# Set parameters ----------------------------------------------------------

plan("multisession")

id_cols <- c("location_id", "year_id", "sex_id", "age_group_id")
id_draw <- c("location_id", "year_id", "sex_id", "age_group_id", "draw")

# set countries who need to be aggregated too
agg_countries <- c("GBR", "CHN", "UKR")

# causes to aggregate
measures <- c("no_shock", "pos_oprm", "covid")

# Load Data ---------------------------------------------------------------

dt_harmonizer <- fs::path(dir_output, "draws/loc_specific_env") |>
  fs::dir_ls(regexp = paste0("harmonized.*", current_year)) |>
  future_map(readRDS) |>
  rbindlist(use.names = TRUE, fill = TRUE)

dt_harmonizer[, c("total_deaths", "deaths_em", "indirect_covid") := NULL]


# Scale/Aggregate ---------------------------------------------------------

# Aggregate GBR UTLAs to level 5. Level 5 does not have data for pandemic years
# aggregate UTLAS to England's 9-region level
# parent locations need to be dropped first if they are present
level_5_gbr <- loc_map[ihme_loc_id %like% "GBR" & level == 5, location_id]
level_5_gbr_ihme <- loc_map[ihme_loc_id %like% "GBR" & level == 5, ihme_loc_id]
dt_harmonizer<- dt_harmonizer[!location_id %in% level_5_gbr]

dt_harmonizer <- mortcore::agg_results(data = dt_harmonizer,
                                       id_vars = id_draw,
                                       value_vars = measures,
                                       tree_only = "GBR",
                                       start_agg_level = 6,
                                       end_agg_level = 5,
                                       loc_scalars = FALSE)


dt_harmonizer[no_shock == 0, no_shock := 1e-10]
dt_harmonizer[covid == 0, covid := 1e-10]
dt_harmonizer[pos_oprm == 0, pos_oprm := 1e-10]

# Scale all locations, except those we want to aggregate
# dt_harmonizer <- lapply(measures, function(measure) {
#   dt_temp <- mortcore::scale_results(
#     data = dt_harmonizer,
#     id_vars = id_draw,
#     value_var = measure,
#     exclude_tree = agg_countries
#   )
#
#   setkeyv(dt_temp, id_draw)
# }
# )
# dt_harmonizer <- Reduce(merge, dt_harmonizer2)

for (measure in measures) {
  dt_harmonizer <- mortcore::scale_results(
    data = dt_harmonizer,
    id_vars = id_draw,
    value_var = measure,
    exclude_tree = agg_countries
  )
}

# aggregate to agg locations
agg_loc_ids <- loc_map[ihme_loc_id %in% agg_countries, location_id]

dt_harmonizer <- dt_harmonizer[!location_id %in% agg_loc_ids]

for (loc in agg_countries) {
  dt_harmonizer <- mortcore::agg_results(data = dt_harmonizer,
                                         id_vars = id_draw,
                                         value_vars = measures,
                                         tree_only = loc,
                                         start_agg_level = 4,
                                         end_agg_level = 3,
                                         loc_scalars = FALSE)
}


# Additional Aggregates ---------------------------------------------------

# aggregate to the 0-12 months and 1-4 age groups
# aggregate to 0-1 and 1-4
dt_u1_adj <- dt_harmonizer[age_group_id %in% c(2:3, 388:389)]
dt_u1_adj <- dt_u1_adj[, .(no_shock = sum(no_shock),
                           pos_oprm = sum(pos_oprm),
                           covid = sum(covid),
                           age_group_id = 28),
                       by = .(location_id, year_id, sex_id, draw)]

dt_1_4_adj <- dt_harmonizer[age_group_id %in% c(238, 34)]
dt_1_4_adj <- dt_1_4_adj[, .(no_shock = sum(no_shock),
                             pos_oprm = sum(pos_oprm),
                             covid = sum(covid),
                             age_group_id = 5),
                         by = .(location_id, year_id, sex_id, draw)]

dt_harmonizer <- rbind(dt_harmonizer, dt_u1_adj, dt_1_4_adj, use.names = TRUE)

# aggregate to both sexes
dt_both_sex <- dt_harmonizer[, .(no_shock = sum(no_shock),
                                 pos_oprm = sum(pos_oprm),
                                 covid = sum(covid),
                                 sex_id = 3L),
                             by = .(location_id, year_id, age_group_id, draw)]

dt_harmonizer <- rbind(dt_harmonizer, dt_both_sex, use.names = TRUE)


# Summarize Results -------------------------------------------------------

dt_harmonizer_summary <- demUtils::summarize_dt(
  dt_harmonizer,
  id_cols = id_draw,
  summarize_cols = "draw",
  value_cols = measures,
  probs = c()
)

# Save --------------------------------------------------------------------

dt_harmonizer[, age_group_id := as.integer(age_group_id)]
dt_harmonizer_summary[, age_group_id := as.integer(age_group_id)]

arrow::write_ipc_file(
  dt_harmonizer,
  fs::path(
    dir_output,
    "draws/scaled_env",
    glue::glue("scaled_env_{current_year}.arrow")
  )
)

saveRDS(
  dt_harmonizer_summary,
  fs::path(
    dir_output,
    "summary/scaled_env",
    glue::glue("scaled_env_{current_year}.RDS")
  )
)
