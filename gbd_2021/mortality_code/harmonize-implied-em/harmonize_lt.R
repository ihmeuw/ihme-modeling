# Meta --------------------------------------------------------------------
#' Author: AUTHOR
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
  "--run_id_finalizer_with_data",
  type = "integer",
  required = !interactive(),
  default = 558L,
  help = "With-shock death number estimate run id of finalizer run for with-2020 VR no-shock envelope"
)
parser$add_argument(
  "--loc_id",
  type = "integer",
  required = !interactive(),
  default = 43900L,
  help = "Location ID"
)
parser$add_argument(
  "--current_year",
  type = "integer",
  required = !interactive(),
  default = 2021L,
  help = "year ID"
)
parser$add_argument(
  "--run_id_em",
  type = "character",
  required = !interactive(),
  default = "s3-2023-04-18-14-51",
  help = "Version of EM (ours)"
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
source("functions/get_current_cc_draws.R")


# Load Location and Age Maps ----------------------------------------------

age_map <- fread(fs::path(dir_output, "age_map", ext = "csv"))
loc_map <- fread(fs::path(dir_output, "loc_map", ext = "csv"))

vr_locs <- fread(
  fs::path(
    dir_output, "vr_pandemic_years_locations", ext = "csv"
  )
)

# Set parameters ----------------------------------------------------------

# Input directories
dir_mlt <- "FILEPATH"
dir_splitting <- "FILEPATH"
dir_covid_em <- "FILEPATH"

id_cols <- c("location_id", "year_id", "sex_id", "age_group_id")

# Determine if this location year has VR
has_vr <- nrow(vr_locs[location_id == loc_id & year_id == current_year]) == 1

ihme_loc <- loc_map[location_id == loc_id, ihme_loc_id]

# !!! HOTFIX: Use OPRM version without IND 2020 SRS included
if (grepl("IND", ihme_loc)) run_id_oprm <- 85


# Load data ---------------------------------------------------------------

file_ns_env <- glue::glue("with_hiv_env_{current_year}.h5")

# load no-shock envelope
dt_env <- setDT(mortcore::load_hdf(
  fs::path(
    dir_mlt, run_id_finalizer,
    "env_with_hiv", "scaled", file_ns_env
  ),
  by_val = loc_id
))

dt_env_with_vr <- setDT(mortcore::load_hdf(
  fs::path(
    dir_mlt, run_id_finalizer_with_data,
    "env_with_hiv", "scaled", file_ns_env
  ),
  by_val = loc_id
))

# HOTFIX: for now just change sim to draw
setnames(dt_env, "sim", "draw")
setnames(dt_env_with_vr, "sim", "draw")

dt_cc_draws <- get_current_cc_draws(
  "total_covid_draws_corrected.csv", 1048, run_id_oprm, loc_id, current_year, loc_map
)
dt_oprm <- get_current_cc_draws(
  "other_pandemic_draws_corrected.csv", 1058, run_id_oprm, loc_id, current_year, loc_map
)
dt_indirect_draws <- get_current_cc_draws(
  "indirect_covid_draws_corrected.csv", 9999, run_id_oprm, loc_id, current_year, loc_map
)

# Load reported COVID
# reported_covid <- fread(
#   fs::path(
#     dir_covid_em,
#     run_id_em,
#     "outputs",
#     glue::glue("covid_reported_total-mean-{run_id_em}.csv")
#   )
# )

reported_covid <- fread(
  "FILEPATH"
)

reported_covid <- reported_covid[year_id == current_year & location_id == loc_id,
                                 deaths_reported_covid]


# Format Data -------------------------------------------------------------

# get rid of less detailed ages and sex_id 3 from no-shock env
dt_env <- dt_env[age_group_id %in% age_map$age_group_id &
                   sex_id != 3]
dt_env_with_vr <- dt_env_with_vr[age_group_id %in% age_map$age_group_id &
                                   sex_id != 3]


# Add Negative OPRM -------------------------------------------------------

# split positive and negative OPRM into separate data tables
dt_neg_oprm <- copy(dt_oprm)[deaths > 0, deaths := 0]
dt_pos_oprm <- copy(dt_oprm)[deaths < 0, deaths := 0]


# add negative OPRM to no shock life tables
dt_env[dt_neg_oprm, neg_oprm_deaths := i.deaths,
       on = c("location_id", "year_id","sex_id", "age_group_id", "draw")]
dt_env[, deaths_w_neg_oprm := deaths + neg_oprm_deaths]

# Format ------------------------------------------------------------------

# combine COVID, no-shock env, and positive oprm
keep_cols <- c("location_id", "year_id", "sex_id",
               "age_group_id", "draw",
               "deaths")

# format and rbind no-shock lt, positive oprm, and COVID
dt_env[, deaths := deaths_w_neg_oprm]

dt_combined <- rbindlist(
  list(
    no_shock = dt_env[, ..keep_cols],
    covid = dt_cc_draws[, ..keep_cols],
    pos_oprm = dt_pos_oprm[, ..keep_cols]
  ),
  idcol = "measure"
)


if(has_vr) {
  # Calculate Total Adjustment ----------------------------------------------

  # the amount we want to adjust is the difference between:
  # 1. The with pandemic data envelope
  # 2. The without pandemic data envelope + EM
  # EM is composed of COVID, positive OPRM, and indirect covid causes

  # calculate no-data no-shock + EM
  dt_env_with_vr[dt_env, no_data_deaths := i.deaths_w_neg_oprm, on = c(id_cols, "draw")]
  dt_env_with_vr[dt_pos_oprm, oprm_deaths := i.deaths, on = c(id_cols, "draw")]
  dt_env_with_vr[dt_cc_draws, covid_deaths := i.deaths, on = c(id_cols, "draw")]
  dt_env_with_vr[dt_indirect_draws, indirect_deaths := i.deaths, on = c(id_cols, "draw")]

  dt_env_with_vr[, no_data_deaths_w_em := no_data_deaths + oprm_deaths + covid_deaths + indirect_deaths]
  dt_env_with_vr[, em_diff_deaths := deaths - no_data_deaths_w_em]

  # Redistribute Implied EM difference --------------------------------------

  # Distribute Implied EM among positive OPRM, COVID, and the no-shock envelope
  # this is based on the proportion of the values, but also scalar weight
  scalar_weights <- data.table(
    measure = c(
      "no_shock",
      "pos_oprm",
      "covid"
    ),
    weights = c(
      0.3, # for no-shock deaths
      1, # positive OPRM
      1 # COVID
    )
  )
  # calculate proportions of each measure to the sum
  dt_combined[, sum_deaths := sum(deaths),
              by = c("location_id", "year_id","sex_id", "age_group_id", "draw")]
  dt_combined[, prop := deaths / sum_deaths]

  # redistribute implied EM based on both weights and proportion
  dt_combined[dt_env_with_vr, em_diff_deaths := i.em_diff_deaths,
              on = c("location_id", "year_id","sex_id", "age_group_id", "draw")]
  dt_combined[scalar_weights, weights := i.weights, on = .(measure)]
  dt_combined[, total_prop_weights := sum(weights * prop), by = c("location_id", "year_id","sex_id", "age_group_id", "draw")]

  # View(dt_combined[sex_id == 1 & age_group_id == 18 & draw == 0])

  dt_combined[, deaths_redistributed := deaths + (prop * weights * em_diff_deaths) / total_prop_weights]
  dt_combined[, deaths_added := (prop * weights * em_diff_deaths)  / total_prop_weights]
  dt_combined[, sum_deaths_added := sum(deaths_added), by = c("location_id", "year_id","sex_id", "age_group_id", "draw")]

  # new test
  # check2 <- dt_combined[, .(death_rate = mean(death_rate),
  #                           sum_death_rate = mean(sum_death_rate),
  #                           prop = mean(prop),
  #                           adj_rate = mean(adj_rate),
  #                           weights = mean(weights),
  #                           added_rate = mean(added_rate),
  #                           new_death_rate = mean(new_death_rate)),
  #                       by = c(id_cols, "measure")]
  # check2[, new_sum_death_rate := sum(new_death_rate), by = id_cols]
  # check2[, sum_added_rate := sum(added_rate), by = id_cols]
  # check2[, test := new_death_rate - (death_rate + prop * weights * adj_rate)]
  # all.equal(rep(0, nrow(check2)),check2$test)

  # Redistribute newly negative OPRM ----------------------------------------

  dt_lt_redist <- dt_combined[measure == "no_shock"]

  # add newly negative OPRM to the no-shock life table
  dt_lt_redist[dt_combined[measure == "pos_oprm" & deaths_redistributed < 0],
               new_neg_oprm := i.deaths_redistributed,
               on = c("location_id", "year_id","sex_id", "age_group_id", "draw")]
  dt_lt_redist[!is.na(new_neg_oprm), deaths_redistributed := deaths_redistributed + new_neg_oprm]

  # set negative oprm to 0
  dt_combined[measure == "pos_oprm" & deaths_redistributed < 0,
              deaths_redistributed := 0]

  # add the adjusted lt to the combined data.table
  dt_combined <- rbind(
    dt_combined[measure != "no_shock"],
    dt_lt_redist,
    fill = TRUE
  )

  #  ----------------------------------------------------------

  # to insure positive (or 0) values for mx in covid, positive oprm, and
  # no-shock-mx, we will
  # 1) move negative draws of OPRM or COVID to no shock envelope, set them to 0
  # 2) distribute negative draws of no-shock to COVID and OPRM as much as possible
  # 3) if no-shock is still < 0, just set it to 0

  dt_combined <- dcast(
    dt_combined,
    location_id + year_id + sex_id + age_group_id + draw ~ measure,
    value.var = "deaths_redistributed"
  )

  # move negative covid and oprm to no-shock
  dt_combined[covid < 0, `:=` (no_shock = no_shock + covid,
                               covid = 0)]
  dt_combined[pos_oprm < 0, `:=` (no_shock = no_shock + pos_oprm,
                                  pos_oprm = 0)]


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

} else {

  dt_combined <- dcast(
    dt_combined,
    location_id + year_id + sex_id + age_group_id + draw ~ measure,
    value.var = "deaths"
  )

  # cap negative no shock envelope to 0 if the location is national level,
  # 0.1 if it is a subnational
  current_level <- loc_map[location_id == loc_id, level]

  dt_combined[no_shock < 0, no_shock := ifelse(current_level > 3, 0.1, 0)]
}

# COVID accounting --------------------------------------------------------

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

  # calculate the original mean distributions to help redistribution
  dt_covid_summary <- demUtils::summarize_dt(dt_cc_draws,
                                             id_cols = c(id_cols, "draw"),
                                             summarize_cols = "draw",
                                             value_cols = c("deaths"),
                                             probs = c())
  dt_pos_oprm_summary <- demUtils::summarize_dt(dt_pos_oprm,
                                                id_cols = c(id_cols, "draw"),
                                                summarize_cols = "draw",
                                                value_cols = c("deaths"),
                                                probs = c())
  dt_env_summary <- demUtils::summarize_dt(dt_combined,
                                           id_cols = c(id_cols, "draw"),
                                           summarize_cols = "draw",
                                           value_cols = c("no_shock"),
                                           probs = c())

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

# Handle negative covid the same way
dt_combined[covid < 0 &
              covid + no_shock < 0, c("covid", "no_shock") := 0]
dt_combined[covid < 0 &
              covid + no_shock >= 0, `:=` (covid = 0,
                                           no_shock = covid + no_shock)]



# Set minimum no-shock deaths ---------------------------------------------

# check that all values for covid, oprm, and covid are positive
assertable::assert_values(dt_combined, c("no_shock", "covid", "pos_oprm"), "gte", 0)

# For locations without VR, we can directly adjust no-shock deaths to ensure a
# small positive value
if (isTRUE(!has_vr && dt_combined[no_shock < 0.1, .N > 0])) {
  dt_check_min_adj <- dt_combined[
    no_shock < 0.1,
    .(
      n_draws = .N,
      mean_no_shock = mean(no_shock),
      mean_pos_oprm = mean(pos_oprm),
      mean_covid = mean(covid)),
    by = id_cols
  ]
  readr::write_csv(
    dt_check_min_adj,
    fs::path(dir_output, "diagnostics/min_adj", paste(ihme_loc, current_year, sep = "-"), ext = "csv")
  )
  dt_combined[no_shock < 0.1, no_shock := 0.1]
}

# Assert that no-shock deaths + implied EM is at least 0, and if not, move
# the difference to no-shock and positive OPRM deaths
dt_combined[dt_indirect_draws, indirect_covid := i.deaths, on = c(id_cols, "draw")]
dt_combined[, total_deaths := no_shock + covid + pos_oprm + indirect_covid]
dt_combined[
  total_deaths < 0,
  `:=`(
    no_shock = no_shock + (0.5 * abs(total_deaths)),
    pos_oprm = pos_oprm + (0.5 * abs(total_deaths))
  )
]
dt_combined[, total_deaths := no_shock + covid + pos_oprm + indirect_covid]
stopifnot(dt_combined[total_deaths < -1e-14, .N] == 0)


# Set terminal no-shock deaths --------------------------------------------

file_terminal_check <- fs::path(paste(ihme_loc, current_year, sep = "-"), ext = "csv")
dir_max_adj <- fs::path(dir_output, "diagnostics/max_adj")
path_terminal_check <- fs::path(dir_max_adj, "loc_specific", file_terminal_check)

if (fs::file_exists(path_terminal_check)) {

  merge_cols <- c("location_id", "year_id", "sex_id", "draw")
  if (loc_map[location_id == loc_id, level] > 3) merge_cols <- setdiff(merge_cols, "draw")

  dt_ext_lt <- unique(fread(path_terminal_check, select = merge_cols))

  dt_combined_term <- dt_combined[age_group_id == 235]
  dt_combined_term[dt_ext_lt, adjust := TRUE, on = merge_cols]

  redistribute_deaths <- function(noshock, oprm, covid, diff, extra_adj = 1) {
    subtotal <- noshock + oprm + covid
    adj_ratio <- (1 -  diff / subtotal) * extra_adj
    list(
      noshock = noshock * adj_ratio,
      oprm = oprm * adj_ratio,
      covid = covid * adj_ratio
    )
  }

  extra_adj <- 1

  dt_combined_term[
    (adjust) & total_deaths > no_shock,
    c("no_shock", "pos_oprm", "covid") := redistribute_deaths(
      no_shock, pos_oprm, covid,
      diff = total_deaths - no_shock,
      extra_adj = extra_adj
    )
  ]

  dt_combined_term[, adjust := NULL]
  dt_combined_term[, total_deaths := no_shock + covid + pos_oprm + indirect_covid]

  stopifnot("negative total terminal deaths" = dt_combined_term[total_deaths < -1e-14, .N] == 0)

  dt_combined <- rbindlist(
    list(dt_combined[age_group_id != 235], dt_combined_term),
    use.names = TRUE
  )

}


# Checks ------------------------------------------------------------------

mean_total_covid <- dt_combined[
  j = .(total_covid = sum(covid)),
  by = .(location_id, year_id, draw)
][,  mean(total_covid)]

stopifnot(
  "reported covid is greater than total" = (mean_total_covid - reported_covid) >= -1e-10
)


# Summarize Results -------------------------------------------------------

dt_combined_summary <- demUtils::summarize_dt(dt_combined,
                                              id_cols = c(id_cols, "draw"),
                                              summarize_cols = "draw",
                                              value_cols = c("no_shock", "covid", "pos_oprm"),
                                              probs = c())

# # TEST: summarize indirect covid, calculate total EM for this loc-year
# dt_indirect_summary <- demUtils::summarize_dt(dt_indirect_draws,
#                                               id_cols = c(id_cols, "draw"),
#                                               summarize_cols = "draw",
#                                               value_cols = c("deaths"),
#                                               probs = c())
#
# dt_combined_summary[dt_indirect_summary,
#                     indirect_covid_mean := i.mean,
#                     on = .(location_id, year_id, sex_id, age_group_id)
#                     ]
#
# dt_combined_summary[, em := covid_mean + pos_oprm_mean + indirect_covid_mean]
#
# dt_total_em <- dt_combined_summary[, .(em = sum(em)), by = .(location_id, year_id)]


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
