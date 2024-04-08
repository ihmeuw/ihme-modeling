# Meta --------------------------------------------------------------------
#'
#' Summary: Calculates global excess mortality rates by ages and sex.
#' Creates both positive and negative distributions
#'
#' Inputs: (all inputs are draw level)
#' 1. Implied excess mortality for each location with VR
#'
#' Methods:
#' 1. Load implied excess
#' 2. Sum together excess to create global distribution of EM
#' 3. Divide by population to get EM rates
#' 4. Save


# Load packages -----------------------------------------------------------

library(data.table)

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

file_postive_base <- "global_em_rate_positive"
file_negative_base <- "global_em_rate_negative"

# Load data ---------------------------------------------------------------

# Load all implied EM data from current year
files_em <- list.files(
  fs::path(
    dir_output,
    "draws",
    "01_implied_em"
  ),
  full.names = TRUE
)

dt_excess_implied <- lapply(
  files_em,
  readRDS
) |> rbindlist(fill = TRUE, use.names = TRUE)

# Load population
dt_pop <- fread(
  fs::path(
    dir_output,
    "pop",
    ext = "csv"
  )
)


# Format data -------------------------------------------------------------

# subset to needed columns
keep_cols <- c(id_cols, "draw", "excess_implied")
dt_excess_implied <- dt_excess_implied[, ..keep_cols]

# merge with population
dt_excess_implied[dt_pop, pop := i.mean, on = id_cols]

# use only level 3 (national) locations except with GBR (level 4 is national)
# UKR, and CHN (level 3 does not have VR data)
nat_locs <- loc_map[level == 3 & !ihme_loc_id %in% c("GBR", "UKR", "CHN", "USA", "JPN", "NOR")|
                      (level == 4 &
                         (ihme_loc_id %like% "GBR" |
                            ihme_loc_id %like% "UKR" |
                            ihme_loc_id %like% "CHN" |
                            ihme_loc_id %like% "USA" |
                            ihme_loc_id %like% "JPN" |
                            ihme_loc_id %like% "NOR"
                         )
                      ),
                    location_id]
dt_excess_implied <- dt_excess_implied[location_id %in% nat_locs]

# calculate total excess
dt_excess_implied[, total_excess := sum(excess_implied),
                  by = .(location_id, year_id, draw)]

dt_total_em <- dt_excess_implied[, .(total_excess = mean(total_excess)),
                                 by = .(location_id, year_id)]

# We are creating both a positive and negative global excess distribution.
# the positive distribution uses only locations from 2020
# the negative distributions uses all location years that are negative
pos_locs <- dt_total_em[year_id == 2020 & total_excess > 0, location_id]
neg_locs <- dt_total_em[total_excess < 0, .(location_id, year_id)]

dt_excess_pos <- dt_excess_implied[!location_id %in% c(62, 130) &
                                     location_id %in% pos_locs &
                                     year_id == 2020]


dt_excess_neg <- dt_excess_implied[neg_locs,
                                   keep := TRUE,
                                   on = .(year_id, location_id)]

dt_excess_neg_excluded <- dt_excess_neg[total_excess < 0 & (location_id %in% c(8) | # TWN
                                          (location_id %in% c(76, 69) & year_id == 2021) | # BEL, SGP 2021
                                          (location_id %in% c(156, 16, 72, 38) & year_id == 2020) | # ARE, PHL, NZL, MNG 2020
                                          (location_id %in% loc_map[ihme_loc_id %like% "JPN", location_id]))]

dt_excess_neg <- dt_excess_neg[keep == TRUE &
                                 !location_id %in% c(8) & # TWN
                                 !(location_id %in% c(76, 69) & year_id == 2021) & # BEL, SGP 2021
                                 !(location_id %in% c(156, 16, 72, 38) & year_id == 2020) & # ARE, PHL, NZL, MNG 2020
                                 !(location_id %in% loc_map[ihme_loc_id %like% "JPN", location_id])] # JPN subnationals

# Save which location years were used -------------------------------------

# Negatives
negative_summary <- unique(dt_excess_neg[, .(location_id, year_id, draw, total_excess)])
negative_excluded <- unique(dt_excess_neg_excluded[, .(location_id, year_id, draw, total_excess)])

negative_summary <- rbind(negative_summary[, keep := TRUE], negative_excluded[, keep := FALSE])
negative_summary <- negative_summary[, .(total_excess = mean(total_excess)),
                                     by = .(location_id, year_id, keep)]

# add location names and ihme_loc_ids
negative_summary[loc_map, `:=` (location_name = i.location_name,
                                ihme_loc_id = i.ihme_loc_id),
                 on = .(location_id)]

# Positive
positive_summary <- unique(dt_excess_pos[, .(location_id, year_id, draw, total_excess)])
positive_summary <- positive_summary[, .(total_excess = mean(total_excess)),
                                     by = .(location_id, year_id)]

# add location names and ihme_loc_ids
positive_summary[loc_map, `:=` (location_name = i.location_name,
                                ihme_loc_id = i.ihme_loc_id),
                 on = .(location_id)]

readr::write_csv(
  negative_summary,
  fs::path(dir_output, "diagnostics",
           "negative_global_locs", ext = "csv")
)

readr::write_csv(
  positive_summary,
  fs::path(dir_output, "diagnostics",
           "positive_global_locs", ext = "csv")
)

# Calculate positive global excess rate --------------------------------------------

# sum implied em and population to global age-sex specific values
dt_global_pos <- demUtils::summarize_dt(
  dt_excess_pos,
  id_cols = c(id_cols, "draw"),
  summarize_cols = "location_id",
  value_cols = setdiff(names(dt_excess_pos), c(id_cols, "draw")),
  summary_fun = c("sum"),
  probs = c()
)

# excess / pop = em rate
dt_global_pos[, em_rate := excess_implied_sum / pop_sum]


# Calculate negative global excess rate --------------------------------------------

# sum implied em and population to global age-sex specific values
dt_global_neg <- demUtils::summarize_dt(
  dt_excess_neg,
  id_cols = c(id_cols, "draw"),
  summarize_cols = "location_id",
  value_cols = setdiff(names(dt_excess_neg), c(id_cols, "draw")),
  summary_fun = c("sum"),
  probs = c()
)


# sum over years for locations with multiple years
dt_global_neg <- dt_global_neg[, .(excess_implied_sum = sum(excess_implied_sum),
                                   pop_sum = sum(pop_sum)), by = .(sex_id, age_group_id, draw)]


dt_global_neg[, year_id := 2020]

# excess / pop = em rate
dt_global_neg[, em_rate := excess_implied_sum / pop_sum]


# Summarize results -------------------------------------------------------

# calculate mean values
dt_global_pos_summary <- demUtils::summarize_dt(
  dt_global_pos,
  id_cols = agg_id_cols,
  summarize_cols = "draw",
  value_cols = setdiff(names(dt_global_pos), agg_id_cols),
  summary_fun = c("mean")
)

# calculate mean values
dt_global_neg_summary <- demUtils::summarize_dt(
  dt_global_neg,
  id_cols = agg_id_cols,
  summarize_cols = "draw",
  value_cols = setdiff(names(dt_global_neg), agg_id_cols),
  summary_fun = c("mean")
)


# Save results -------------------------------------------------------------

saveRDS(
  dt_global_pos,
  fs::path(dir_output, "draws", "01_implied_em",
           file_postive_base, ext = "RDS")
)

saveRDS(
  dt_global_pos_summary,
  fs::path(dir_output, "summary", "01_implied_em",
           file_postive_base, ext = "RDS")
)

saveRDS(
  dt_global_neg,
  fs::path(dir_output, "draws", "01_implied_em",
           file_negative_base, ext = "RDS")
)

saveRDS(
  dt_global_neg_summary,
  fs::path(dir_output, "summary", "01_implied_em",
           file_negative_base, ext = "RDS")
)
