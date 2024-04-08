# Meta --------------------------------------------------------------------
#' Author: AUTHOR
#'
#' Summary: Adjust the 2021 envelope in locations where we do have 2020
#'          data and don't have 2021 data.
#'
#' Inputs:
#' 1. No-shock mx (pre-adjusted: 2020-2021)
#' 2. Adjusted no-shock mx (2020)
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
  "--run_id_finalizer",
  type = "integer",
  required = !interactive(),
  default = 556L,
  help = "With-shock death number estimate run id of finalizer run for no-2020 VR no-shock envelope"
)
parser$add_argument(
  "--loc_id",
  type = "integer",
  required = !interactive(),
  default = 74L,
  help = "Location ID"
)
parser$add_argument(
  "--current_year",
  type = "integer",
  required = !interactive(),
  default = 2021L,
  help = "year ID, for this step will be 2021"
)
parser$add_argument(
  "--run_id_em",
  type = "character",
  required = !interactive(),
  default = "s3-2023-04-18-14-51",
  help = "Version of EM splitting to use implied EM from"
)
parser$add_argument(
  "--run_id_splitting",
  type = "character",
  required = !interactive(),
  default = "2023_04_18-16_01_39",
  help = "Version of EM splitting to use implied EM from"
)
parser$add_argument(
  "--run_id_oprm",
  type = "integer",
  required = !interactive(),
  default = 82L,
  help = "Version of Central Comp COVID balancing process"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)

# load CC functions
source("FILEPATH")
source("FILEPATH")

# Load helper functions
source("FILEPATH")


# Load Location and Age Maps ----------------------------------------------

age_map <- fread(fs::path(dir_output, "age_map", ext = "csv"))
ages_most_detailed <- c(age_map[is_aggregate == 0, age_group_id], 235)

loc_map <- fread(fs::path(dir_output, "loc_map", ext = "csv"))


# Set parameters ----------------------------------------------------------

# Input directories
dir_mlt <- "FILEPATH"
dir_splitting <- "FILEPATH"
dir_em <- "FILEPATH"

id_cols <- c("location_id", "year_id", "sex_id", "age_group_id")

ihme_loc <- loc_map[location_id == loc_id, ihme_loc_id]


# Load data ---------------------------------------------------------------

# Load population
# dt_pop <- fread(
#   fs::path(
#     dir_output,
#     "pop",
#     ext = "csv"
#   )
# )

# load no-shock life tables
dt_env <- lapply(2020:2021, function(yr) {
  mortcore::load_hdf(
    fs::path(
      dir_mlt, run_id_finalizer,
      "env_with_hiv", "scaled", glue::glue("with_hiv_env_{yr}.h5")
    ),
    by_val = loc_id
  ) |> setDT()
}) |> rbindlist()


setnames(dt_env, "sim", "draw")

# load adjusted stuff
dt_adj <- readRDS(
  fs::path(
    dir_output,
    "draws",
    "loc_specific_env",
    glue::glue("harmonized_env_{loc_id}_2020"),
    ext = "RDS"
  )
)

# Load expected deaths from EM process
dt_em_expected <- fread(
  "FILEPATH"
)
dt_cc_draws <- get_current_cc_draws(
  "total_covid_draws_corrected.csv", 1048, run_id_oprm, loc_id, current_year, loc_map
)
dt_oprm <- get_current_cc_draws(
  "other_pandemic_draws_corrected.csv", 1058, run_id_oprm, loc_id, current_year, loc_map
)

# separate positive and negative OPRM
dt_neg_oprm <- copy(dt_oprm)[deaths > 0, deaths := 0]
dt_oprm <- dt_oprm[deaths < 0, deaths := 0]


dt_cc_draws <- rbind(dt_cc_draws, dt_oprm)

# get reported covid
reported_covid <- fread(
  "FILEPATH"
)

reported_covid <- reported_covid[year_id == current_year & location_id == loc_id,
                                 deaths_reported_covid]

# calculate the original mean distributions to help redistribution
dt_covid_summary <- demUtils::summarize_dt(dt_cc_draws[cause_id == 1048],
                                           id_cols = c(id_cols, "draw"),
                                           summarize_cols = "draw",
                                           value_cols = c("deaths"),
                                           probs = c())
dt_pos_oprm_summary <- demUtils::summarize_dt(dt_oprm,
                                              id_cols = c(id_cols, "draw"),
                                              summarize_cols = "draw",
                                              value_cols = c("deaths"),
                                              probs = c())
dt_env_summary <- demUtils::summarize_dt(dt_env[year_id == current_year],
                                         id_cols = c(id_cols, "draw"),
                                         summarize_cols = "draw",
                                         value_cols = c("deaths"),
                                         probs = c())

# Format Data -------------------------------------------------------------

# get rid of less detailed ages and sex_id 3 from no-shock lts
dt_env <- dt_env[age_group_id %in% age_map$age_group_id &
                   sex_id != 3]

# go wide on year
dt_env <- dcast(
  dt_env,
  location_id + sex_id + age_group_id + draw ~ year_id,
  value.var = "deaths"
)


# Add Negative OPRM -------------------------------------------------------

dt_env[dt_neg_oprm, `:=` (`2021` = `2021` + i.deaths,
                          neg_oprm_deaths = i.deaths),
       on = .(location_id, sex_id, age_group_id, draw)]

# For location-years with no VR data, move a fraction of negative
# OPRM deaths from the no-shock envelope to covid deaths in 5-9 and 10-14 age
# groups
adj_age_groups <- age_map[age_start %in% c(5, 10), age_group_id]
adj_ratio <- 1/2

hotfix_locs <- c("PHL")
if (loc_id %in% hotfix_locs) adj_ratio <- 2/3

# Remove negative OPRM from no-shock envelope
dt_env[
  age_group_id %in% adj_age_groups,
  `2021` := `2021` - adj_ratio * neg_oprm_deaths
]

# Add negative OPRM to covid deaths
dt_cc_draws[
  dt_neg_oprm,
  neg_oprm_deaths := i.deaths,
  on = .(location_id, sex_id, age_group_id, draw)
]

dt_cc_draws[
  cause_id == 1048 & age_group_id %in% adj_age_groups,
  deaths := deaths + adj_ratio * neg_oprm_deaths
]

dt_cc_draws[, neg_oprm_deaths := NULL]

# Move negative covid deaths back to no-shock
dt_env[
  dt_cc_draws[cause_id == 1048 & age_group_id %in% adj_age_groups & deaths < 0],
  `2021` := `2021` - i.deaths,
  on = .(location_id, sex_id, age_group_id, draw)
]
dt_cc_draws[
  cause_id == 1048 & age_group_id %in% adj_age_groups & deaths < 0,
  deaths := 0
]


# Adjust env --------------------------------------------------------------

# Use the ratio of 2020 adjusted / non-adjusted to adjust the envelope
# in 2021.
# EXCEPT when 2020 has negative EM and we have positive EM in 2021.
# in this case replace the envelope with the EM expected value (plus negative OPRM)

# Checks for negative to positive sign change in EM
dt_em_expected <- dt_em_expected[location_id == loc_id]

if(nrow(dt_em_expected) < 2) {
  negative_sign_change <- FALSE
} else {
  negative_sign_change <- dt_em_expected[year_id == 2020, mean_deaths_excess < 0] &
    dt_em_expected[year_id == 2021, mean_deaths_expected > 0]

}


phl_locs <- loc_map[ihme_loc_id %like% "PHL", location_id]

if(negative_sign_change) {

  # multiply by a ratio of EM expected / GBD expected
  # sum to all ages for GBD
  dt_env_total <- dt_env[, .(deaths_gbd_expected = sum(`2021`)),
                         by = .(location_id, draw)]
  dt_env_total[dt_em_expected[year_id == 2021],
               deaths_em_expected := i.mean_deaths_expected,
               on = .(location_id)]
  dt_env_total[, env_scalar := deaths_em_expected / deaths_gbd_expected]

  if (loc_id %in% phl_locs) {
    dt_env_total[, env_scalar := 1]
  }

  if (ihme_loc == "AUS") dt_env_total[, env_scalar := env_scalar * 1.03]

  dt_env[dt_env_total, env_scalar := i.env_scalar, on = .(draw)]

  dt_env[, no_shock_adj_2021 := env_scalar * `2021`]

} else {

  # calculate a ratio between pre and post adjusted 2020 mx
  # apply this scalar at the draw level
  dt_env[dt_adj, no_shock_adj_2020 := i.no_shock, on = .(location_id, sex_id, age_group_id, draw)]
  dt_env[, pandemic_ratio := no_shock_adj_2020 / `2020`]

  dt_env[, no_shock_adj_2021 := pandemic_ratio * `2021`]

}

check1 <- dt_env[, lapply(.SD, mean), by =  c("location_id","sex_id", "age_group_id")]

# Adjust COVID and OPRM ---------------------------------------------------

# take the difference between the adjusted and unadjusted 2021 lts and
# split it proportionally among covid and negative OPRM
dt_cc_draws[, total_deaths := sum(deaths), by = .(location_id, year_id, sex_id, age_group_id, draw)]
dt_cc_draws[, prop := deaths / total_deaths]

dt_cc_draws[dt_env, env_diff := i.2021 - i.no_shock_adj_2021, on = .(location_id, sex_id, age_group_id, draw)]

dt_cc_draws[, new_deaths := deaths + prop * env_diff]

check2 <- dt_cc_draws[, lapply(.SD, mean), by =  c("location_id","sex_id", "age_group_id", "cause_id")]


# Move any negative COVID and OPRM to the envelope ------------------------

dt_env[dt_cc_draws[cause_id == 1058 & new_deaths < 0], neg_oprm := i.new_deaths,
       on = .(location_id, sex_id, age_group_id, draw)]
dt_env[dt_cc_draws[cause_id == 1048 & new_deaths < 0], neg_covid := i.new_deaths,
       on = .(location_id, sex_id, age_group_id, draw)]

dt_env[!is.na(neg_oprm), no_shock_adj_2021 := no_shock_adj_2021 + neg_oprm]
dt_env[!is.na(neg_covid), no_shock_adj_2021 := no_shock_adj_2021 + neg_covid]

# set draws to 0
dt_cc_draws[new_deaths < 0, new_deaths := 0]


# Check that estimated covid is less than reported ------------------------

# final results are data tables which are wide by cause
dt_combined <- dcast(dt_cc_draws, location_id + year_id + sex_id + age_group_id + draw ~ cause_id, value.var = "deaths")

setnames(dt_combined,
         c("1048", "1058"),
         c("covid", "pos_oprm"))

# merge on no shock envelope for 2021
dt_env[, year_id := 2021L]
dt_combined[dt_env, no_shock := i.no_shock_adj_2021, on = c(id_cols, "draw")]

calc_props <- function(dt) {
  dt[, sum_deaths := sum(mean), by = c("location_id", "year_id")]
  dt[, prop := mean / sum_deaths]
}

# COVID estimates should be greater than the reported amount
# if it is not, subtract the diff (which is negative) to total covid
# and add that amount to OPRM
dt_total_covid <- dt_combined[, .(total_covid = sum(covid)), by = .(location_id, year_id, draw)]
mean_total_covid <- dt_total_covid$total_covid |> mean()

if (mean_total_covid < reported_covid) {

  covid_diff <- mean_total_covid - reported_covid

  # add env to oprm distribution. this prevents overattraibution of
  # negative oprm which can create very low envelope in certain ages
  dt_pos_oprm_summary[dt_env_summary, mean := mean + i.mean,
                      on = id_cols]


  calc_props(dt_covid_summary)
  calc_props(dt_pos_oprm_summary)

  dt_covid_summary[, death_adj := - prop * covid_diff]
  dt_pos_oprm_summary[, death_adj := prop * covid_diff]

  dt_combined[dt_covid_summary, covid_adj := i.death_adj, on = id_cols]
  dt_combined[dt_pos_oprm_summary, oprm_adj := i.death_adj, on = id_cols]

  dt_combined[, covid := covid + covid_adj]
  dt_combined[, pos_oprm := pos_oprm + oprm_adj]

  dt_combined[, c("covid_adj", "oprm_adj") := NULL]
}

# handle negative OPRM one last time, adding it to no_shock
# or setting both it and no shock to 0
dt_combined[pos_oprm < 0 &
              pos_oprm + no_shock < 0, c("pos_oprm", "no_shock") := 0]
dt_combined[pos_oprm < 0 &
              pos_oprm + no_shock >= 0, `:=` (pos_oprm = 0,
                                              no_shock = pos_oprm + no_shock)]

# if covid_mx + oprm_mx < abs(no-shock_mx), set all 3 to 0
dt_combined[no_shock < 0 &
              covid + pos_oprm < abs(no_shock),
            c("covid", "no_shock", "pos_oprm") := 0]

# if covid_mx + oprm_mx > abs(no-shock_mx), distribute proportionally
dt_combined[no_shock < 0 &
              covid + pos_oprm > abs(no_shock),
            `:=` (covid = covid + covid / (covid + pos_oprm) * no_shock,
                  pos_oprm = pos_oprm + pos_oprm / (covid + pos_oprm) * no_shock,
                  no_shock = 0)]

# Checks ------------------------------------------------------------------

# check that all values for covid, oprm, and covid are positive
assertable::assert_values(dt_combined, c("no_shock", "covid", "pos_oprm"), "gte", 0)


# Summarize Results -------------------------------------------------------

dt_combined_summary <- demUtils::summarize_dt(dt_combined,
                                              id_cols = c(id_cols, "draw"),
                                              summarize_cols = "draw",
                                              value_cols = c("no_shock", "covid", "pos_oprm"),
                                              probs = c())


# Save Results ------------------------------------------------------------

saveRDS(
  dt_combined,
  fs::path(
    dir_output,
    "draws",
    "loc_specific_env",
    glue::glue("harmonized_env_{loc_id}_{current_year}"),
    ext = "RDS"
  )
)

saveRDS(
  dt_combined_summary,
  fs::path(
    dir_output,
    "summary",
    "loc_specific_env",
    glue::glue("harmonized_env_{loc_id}_{current_year}"),
    ext = "RDS"
  )
)
