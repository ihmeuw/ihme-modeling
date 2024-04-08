# Meta --------------------------------------------------------------------
#'
#' Summary: Apply an age-sex distribution to estimated covid-em. This can
#' either be the implied EM distribution, final distribution from a previous
#' year or a global distribution
#'
#' Inputs: (all inputs are draw level)
#' 1. Total Esitamted COVID EM
#' 2. Age-sex distribution of EM rates
#' 3. Population counts
#'
#' Methods:
#' 1. Calculate a factor which splits estimated excess to be in line with
#'    the new age-sex-specific mortality rates
#' 2. Split the total excess using this scalar to get age-sex specific
#'    estimates of excess
#' 3. Scale the split excess to total the same as the original total excess
#' 4. Save results


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
  "--loc_id",
  type = "integer",
  required = !interactive(),
  default = 44533L,
  help = "Location ID"
)
parser$add_argument(
  "--current_year",
  type = "integer",
  required = !interactive(),
  default = 2020L,
  help = "year ID"
)
parser$add_argument(
  "--em_threshold",
  type = "integer",
  required = !interactive(),
  default = 75e3L,
  help = "A location with implied EM under this amount uses the global em rates"
)
parser$add_argument(
  "--force_global",
  type = "logical",
  required = !interactive(),
  default = FALSE,
  help = "For special cases, use global EM even if EM is above threshold"
)
parser$add_argument(
  "--run_id_em",
  type = "character",
  required = !interactive(),
  default = "VERSION",
  help = "COVID-EM version (for total EM)"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)

if(current_year > 2020 & !interactive()) Sys.sleep(80)

# Setup -------------------------------------------------------------------

loc_map <- fread(fs::path(dir_output, "loc_map", ext = "csv"))

loc_level <- loc_map[location_id == loc_id, level]

# some locs, due to data availability, are treated as national for this process
locs_faux_nat <- loc_map[level == 4 &
                           ihme_loc_id %like% "GBR" |
                           ihme_loc_id %like% "CHN" |
                           ihme_loc_id %like% "UKR",
                         location_id]

# location_id of the parent to this location, EXCEPT GBR UTLAs should use
# England (GBR_4749) as the parent
parent_loc_id <- ifelse(loc_level == 6,
                        4749,
                        loc_map[location_id == loc_id, parent_id])

id_cols <- c("location_id", "year_id", "sex_id", "age_group_id")
em_ids <- c("location_id", "year_id", "draw")
merge_ids <- c("year_id", "sex_id", "age_group_id", "draw")

file_out_base <- glue::glue("adjusted_em_{loc_id}_{current_year}")

u5_age_groups <- c(2, 3, 388, 389, 238, 34)

# Get names of files for potential age-sex distributions
# implied (draws)
file_implied_draw <- fs::path(
  dir_output, "draws", "01_implied_em",
  glue::glue(
    "implied_excess_{loc_id}_{current_year}.RDS"
  )
)

file_implied_mean <- fs::path(
  dir_output, "summary", "01_implied_em",
  glue::glue(
    "implied_excess_{loc_id}_{current_year}.RDS"
  )
)

# previous year implied (draws)
file_prev_year_implied <- fs::path(
  dir_output, "draws", "01_implied_em",
  glue::glue(
    "implied_excess_{loc_id}_{current_year - 1}.RDS"
  )
)

# previous year adjusted (draws)
file_prev_year_final <- fs::path(
  dir_output, "draws", "04_adjusted_age_sex",
  glue::glue(
    "adjusted_em_{loc_id}_{current_year - 1}.RDS"
  )
)

# global postive implied (draws)
file_global_positive <- fs::path(
  dir_output, "draws", "01_implied_em",
  glue::glue(
    "global_em_rate_positive.RDS"
  )
)

# global negative implied (draws)
file_global_negative <- fs::path(
  dir_output, "draws", "01_implied_em",
  glue::glue(
    "global_em_rate_negative.RDS"
  )
)

# Load data ---------------------------------------------------------------

# Estimated EM
dt_em <- fread(
  fs::path(
    "FILEPATH", run_id_em, paste0("/outputs/covid_em_scalars-draw-", run_id_em),
    ext = "csv"
  )
)

# Load population to calculate EM rates
dt_pop <- fread(
  fs::path(
    dir_output,
    "pop",
    ext = "csv"
  )
)

# load mean implied em (if it exists)
if (file.exists(file_implied_mean)) {
  dt_implied <- readRDS(file_implied_mean)

  em_implied <- unique(dt_implied$total_excess_implied_mean)
}

# Format data -------------------------------------------------------------

# EM
# Get EM from parent location
dt_parent <- dt_em[year_id == current_year &
                     location_id == parent_loc_id]

# get EM from previous year
dt_prev_year <- dt_em[year_id == current_year - 1 &
                        location_id == loc_id]

# subset to current location and year
dt_em <- dt_em[year_id == current_year &
                 location_id == loc_id]
mean(dt_em$deaths_excess)

# upscale COVID EM draws 100 -> 1,000
set.seed(27)

# this changes from a totally random sample (with replacement)
# to a method which choses each draw 10 times, in a random order
sample_draws <- sample.int(100, 100) |> rep(times = 10)

dt_em <- dt_em[sample_draws]

dt_em[, draw := 0:999]

dt_em <- dt_em[, .SD, .SDcols = c(em_ids, "deaths_excess")]
mean(dt_em$deaths_excess)

# Determine which distribution to use -------------------------------------

#' Below are the rules of which EM distribution to use
#'
#' National (and some level 4 locations) locations:
#' IF the total implied EM is below the em_threshold,
#' OR if force_global is true,
#' OR if estimated EM is positive and implied is negative
#' THEN use the global distribution.
#'
#' ELSE, use current location year if data is available
#' OR previous year if it is not
#'
#' For Subnational locations:
#' IF the parent location has positive excess,
#' THEN use the EM distribution of the parent location
#' EXCEPT: for GBR, use England (GBR_4749) as parent

em_estimated <- mean(dt_em$deaths_excess)

if (loc_level == 3 | loc_id %in% locs_faux_nat) {

  # If implied excess exists, and estimated excess surpasses the threshold
  # or is negative
  # then use implied.
  if (em_estimated > em_threshold &
      !force_global) {

    # use implied from the the current year or previous if current is not there
    if (file.exists(file_implied_draw)) {

      dt_new_rates <- readRDS(file_implied_draw)

    } else if (file.exists(file_prev_year_final)) {

      # Make sure previous year and current year excess are the same sign
      prev_year_total_em <- mean(dt_prev_year$deaths_excess)

      if (prev_year_total_em * em_estimated > 0) {
        dt_new_rates <- readRDS(file_prev_year_final)
        use_prev_year <- TRUE
      }
    }
  }

  # If we don't have a distribution yet, use the global distribution
  # for ages > 5 and the implied for < 5 if available
  if (!exists("dt_new_rates")) {

    # Use the positive global distribution if em is postive,
    # the negative distribution if em is negative
    if(em_estimated < 0) {
      dt_rates_adult <- readRDS(file_global_negative)
      dt_rates_adult[, em_rate := mean(em_rate), by = .(sex_id, age_group_id)]
    } else {
      dt_rates_adult <- readRDS(file_global_positive)
    }

    # as we want to preserve the u5 trends of the implied envelope
    # read in implied for u5 ages if it exists and append it
    if (file.exists(file_implied_draw)) {
      dt_rates_child <- readRDS(file_implied_draw)

      dt_rates_child <- dt_rates_child[age_group_id %in% u5_age_groups]
      dt_rates_adult <- dt_rates_adult[!age_group_id %in% u5_age_groups]

      dt_new_rates <- rbind(dt_rates_child, dt_rates_adult, fill = TRUE, use.names = TRUE)
    } else {
      dt_new_rates <- dt_rates_adult
    }
  }

} else if (loc_level > 3) {

  # National locations are run first so subnationals can take their rates from
  # this step from their parent.
  # This means that if the parent had to use the global em distribution,
  # its subnationals are able to inherit it.

  # Only use parent distribution if the child and parent total excess
  # have the same sign, otherwise use the respective global distribution
  parent_total_em <- mean(dt_parent$deaths_excess)

  dt_parent_rates <- readRDS(
    fs::path(
      dir_output, "draws", "04_adjusted_age_sex",
      glue::glue(
        "adjusted_em_{parent_loc_id}_{current_year}.RDS"
      )
    )
  )

  if (em_estimated > 0) {
    dt_global_rates <- readRDS(file_global_positive)
  } else {
    dt_global_rates <- readRDS(file_global_negative)
    dt_global_rates[, em_rate := mean(em_rate), by = .(sex_id, age_group_id)]
  }

  # if total EM sign matches its parent, use the parent rates,
  # if it does not match, use parent u5 and global above 5
  if (parent_total_em * em_estimated > 0) {
    dt_new_rates <- dt_parent_rates
  } else {
      dt_rates_child <- dt_parent_rates[age_group_id %in% u5_age_groups]

      setnames(dt_rates_child, "new_em_rate", "em_rate")
      dt_rates_adult <- dt_global_rates[!age_group_id %in% u5_age_groups]

      dt_new_rates <- rbind(dt_rates_child, dt_rates_adult, fill = TRUE, use.names = TRUE)
  }

}

# Format the new rates
setnames(dt_new_rates, "new_em_rate", "em_rate", skip_absent = TRUE)
dt_new_rates[, year_id := current_year]
dt_new_rates[, location_id := loc_id]


# Calculate new EM distribution -------------------------------------------

if(!exists("dt_new_rates")) stop("EM rates not assigned")

# format new rates
dt_new_rates <- dt_new_rates[, .SD, .SDcols = c(id_cols, "draw", "em_rate")]

# merge on new EM rates
dt_em <- merge(dt_em, dt_new_rates, on = em_ids, all.y = TRUE)

# NOTE: if we have em < 0, we are using negative rates for the distribution
# in the case where some draws are positive, multiply them by negative 1
if(em_estimated < 0) {
  dt_em[deaths_excess > 0, `:=` (deaths_excess = deaths_excess * -1,
                                 reversed = TRUE)]
}

# merge on population
dt_em[dt_pop, pop := i.mean, on = id_cols]

if(loc_id %in% loc_map[ihme_loc_id %like% "CHN", location_id] |
   loc_id %in% loc_map[ihme_loc_id %like% "PRK", location_id] |
   (loc_id %in% loc_map[ihme_loc_id %like% "VNM", location_id] & current_year == 2020) |
   (loc_id %in% loc_map[ihme_loc_id %like% "NZL", location_id] & current_year == 2021)
) {
  # The splitting factor we calculate is (pop * new_em_rate) / Total excess.
  # This is multiplied by total implied to get new age-specific EM deaths
  # which give the location the same excess mx as the global values
  dt_em[, split_factor := (em_rate * pop) / deaths_excess]
  dt_em[, split_excess_deaths := deaths_excess * split_factor]


  dt_em[, split_total_excess := sum(split_excess_deaths),
        by = .(location_id, year_id, draw)]

  if("reversed" %in% names(dt_em)) {
    dt_em[reversed == TRUE, deaths_excess := deaths_excess * -1]
    dt_em[, reversed := NULL]
  }

  # preserve under 5 deaths and only scale the deaths above 5
  # such that u5 deaths + scaled above-5 deaths = total estimated excess
  dt_em[, scalar := deaths_excess / split_total_excess]
  dt_em[, new_excess_deaths := scalar * split_excess_deaths]

  # Calculate the new excess rate
  dt_em[, new_em_rate := new_excess_deaths / pop]

} else {


  # The splitting factor we calculate is (pop * new_em_rate) / Total excess.
  # This is multiplied by total implied to get new age-specific EM deaths
  # which give the location the same excess mx as the global values
  dt_em[, split_factor := (em_rate * pop) / deaths_excess]
  dt_em[, split_excess_deaths := deaths_excess * split_factor]

  dt_em_child <- dt_em[age_group_id %in% u5_age_groups]

  dt_u5_em <- dt_em_child[, .(u5_deaths = sum(split_excess_deaths)),
                          by = .(location_id, year_id, draw)]

  dt_em[dt_u5_em, u5_deaths := i.u5_deaths,
        on = .(location_id, year_id, draw)]

  dt_em <- dt_em[!age_group_id %in% u5_age_groups]

  dt_em[, split_total_excess := sum(split_excess_deaths),
        by = .(location_id, year_id, draw)]

  if("reversed" %in% names(dt_em)) {
    dt_em[reversed == TRUE, deaths_excess := deaths_excess * -1]
    dt_em[, reversed := NULL]
  }

  # preserve under 5 deaths and only scale the deaths above 5
  # such that u5 deaths + scaled above-5 deaths = total estimated excess
  dt_em[, deaths_excess_sans_u5 := deaths_excess - u5_deaths]
  dt_em[, scalar := deaths_excess_sans_u5 / split_total_excess]
  dt_em[, new_excess_deaths := scalar * split_excess_deaths]

  # Calculate the new excess rate
  dt_em[, new_em_rate := new_excess_deaths / pop]

  # add new columns to u5 deaths and re-append
  dt_em_child[, `:=` (new_excess_deaths = split_excess_deaths,
                      new_em_rate = em_rate)]

  dt_em <- rbind(dt_em_child, dt_em, fill = TRUE, use.names = TRUE)

}
# sometimes estimated excess is just 0, which creates NaN
# replace mortality rates which are NaN with 0
dt_em[is.nan(new_em_rate) |
        is.na(new_em_rate),
      `:=` (new_em_rate = 0,
            new_excess_deaths = 0)]

dt_em[is.nan(new_excess_deaths) |
        is.na(new_excess_deaths),
      `:=` (new_em_rate = 0,
            new_excess_deaths = 0)]


# test check that for each draw the sum of split and scaled EM is
# equal to the estimate of EM
dt_em[, summed_new_deaths := sum(new_excess_deaths), by = .(location_id, year_id, draw)]
dt_em[, diff := summed_new_deaths - deaths_excess]

stopifnot(all.equal(abs(dt_em$summed_new_deaths), abs(dt_em$deaths_excess)))

# Format Results ----------------------------------------------------------

setnames(dt_em,
         c("deaths_excess"),
         c("total_estimated_em"))

# Keep columns needed for results and graphing
keep_cols <- c(id_cols, "draw", "total_estimated_em", "new_em_rate",
               "pop", "new_excess_deaths")
dt_em <- dt_em[, ..keep_cols]

# create a summary version
dt_em_summary <- dt_em[, .(total_estimated_em_mean = mean(total_estimated_em),
                           new_em_rate_mean = mean(new_em_rate),
                           pop_mean = mean(pop),
                           new_excess_deaths_mean = mean(new_excess_deaths)),
                       by = .(location_id, year_id, sex_id, age_group_id)]

dt_new_rates_summary <- dt_new_rates[, .(em_rate_mean = mean(em_rate)),
                                     by = .(location_id, year_id, sex_id,
                                            age_group_id)]

# Save Results ------------------------------------------------------------

saveRDS(
  dt_em,
  fs::path(dir_output, "draws", "04_adjusted_age_sex",
           file_out_base, ext = "RDS")
)

saveRDS(
  dt_em_summary,
  fs::path(dir_output, "summary", "04_adjusted_age_sex",
           file_out_base, ext = "RDS")
)

saveRDS(
  dt_new_rates_summary,
  fs::path(dir_output, "summary", "04_adjusted_age_sex",
           glue::glue("em_rates_applied_{loc_id}_{current_year}"),
           ext = "RDS")
)
