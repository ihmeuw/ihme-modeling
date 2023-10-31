# Meta --------------------------------------------------------------------
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
  "--run_id_vrp",
  type = "integer",
  required = !interactive(),
  default = 354L,
  help = "VR data version"
)
parser$add_argument(
  "--run_id_elt",
  type = "integer",
  required = !interactive(),
  default = 526L,
  help = "ELT data version"
)
parser$add_argument(
  "--run_id_ddm",
  type = "integer",
  required = !interactive(),
  default = 504L,
  help = "DDM estimate version"
)
args <- parser$parse_args()

list2env(args, .GlobalEnv)


# Setup -------------------------------------------------------------------


most_detailed_age_groups <- c(2:3, 388:389, 238, 34, 6:20, 30:32, 235)


# Load Data ---------------------------------------------------------------


dt_vrp <- get_mort_outputs(model_name = "death number empirical",
                           model_type = "data",
                           run_id = run_id_vrp,
                           year_ids = 2010:2021,
                           age_group_ids = most_detailed_age_groups,
                           demographic_metadata = TRUE,
                           sex_ids = 1:2)

# HOTFIX: weird duplicate in USA 2019
dt_vrp <- dt_vrp[!(detailed_source == "WHO_causesofdeath" & nid == 472996)]

# Pull the uploaded elt results for outliering information
dt_elt_outliering <- get_mort_outputs(model_name = "life table empirical",
                                      model_type = "data",
                                      run_id = run_id_elt,
                                      life_table_parameter_ids = 3,
                                      age_group_ids = 5) # subset to save memory and potentially time

# Load DDM to get completeness
dt_ddm <- get_mort_outputs(model_name = "ddm",
                           model_type = "estimate",
                           run_id = run_id_ddm,
                           sex_ids = 1:2,
                           estimate_stage_ids = 14)

dt_ddm_child <- get_mort_outputs(model_name = "ddm",
                                 model_type = "estimate",
                                 run_id = run_id_ddm,
                                 estimate_stage_ids = 11)

# collect outlier status for each source in ELT
dt_elt_outliering <- unique(dt_elt_outliering[outlier == 0, .(location_id, year_id, sex_id,
                                                              nid, underlying_nid, outlier)])

# Create DDM completeness for ages 5-9 and 10-14 (same method as ELT)
setnames(dt_ddm,
         c("mean"),
         c("adult_comp"))

dt_ddm[dt_ddm_child, child_comp := i.mean, on = c("location_id", "year_id")]

# Cap completeness. set all values >= 0.95 to 1
dt_ddm[adult_comp >= 0.95, adult_comp := 1]
dt_ddm[child_comp >= 0.95, child_comp := 1]

# 5-9 and 10-14 age groups are weighted averages of adult and child completeness
dt_ddm[, `:=` (comp_5_to_9 = (1/3) * adult_comp + (2/3) * child_comp,
               comp_10_to_14 = (2/3) * adult_comp + (1/3) * child_comp)]

# format
dt_ddm <- melt(dt_ddm,
               id.vars = c("location_id", "year_id", "sex_id"),
               measure.vars = c("adult_comp", "child_comp",
                                "comp_5_to_9", "comp_10_to_14"),
               variable.name = "age_group",
               value.name = "completeness",
               variable.factor = FALSE)

# assign ages to completeness
ddm_age_map <- data.table(age_group = c("child_comp", "comp_5_to_9", "comp_10_to_14", "adult_comp"),
                          parent_age_start = c(0, 5, 10, 15),
                          parent_age_end = c(5, 10, 15, Inf))

dt_ddm <- merge(dt_ddm, ddm_age_map)

# format_vr_data
dt_vrp <- dt_vrp[, .(location_id,
                     year_id,
                     nid,
                     underlying_nid,
                     sex_id,
                     age_start = age_group_years_start,
                     age_end = age_group_years_end,
                     age_group_id,
                     deaths_vr = mean)]

# create a DDM adjusted VR
dt_vrp[dt_ddm, completeness := i.completeness,
       on = .(age_start >= parent_age_start,
              age_end <= parent_age_end,
              location_id,
              year_id)]

dt_vrp[, deaths_vr_adj := deaths_vr / completeness]

dt_vrp <- melt(dt_vrp, id.vars = c("location_id", "year_id", "nid",
                                   "underlying_nid", "sex_id", "age_start",
                                   "age_end", "age_group_id"),
               measure.vars = c("deaths_vr", "deaths_vr_adj"),
               variable.name = "deaths_source",
               measure.name = "deaths")

# determine outliered sources
dt_vrp[dt_elt_outliering, outlier := i.outlier, on = c("location_id", "year_id", "nid",
                                                       "underlying_nid", "sex_id")]

# all sources not included are outliered
dt_vrp[is.na(outlier), outlier := 1]


readr::write_csv(
  dt_vrp,
  fs::path(
    dir_output,
    "diagnostics",
    "vr_for_plots.csv"
  )
)

