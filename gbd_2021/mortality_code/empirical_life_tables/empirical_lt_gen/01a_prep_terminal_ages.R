
# Meta --------------------------------------------------------------------

# Title: Prep original (pre-split) terminal ages
# Description:
#  1) Load combined cod/non-cod VR from pre age-sex splitting in VR prep
#     process
#  2) Subset to terminal age groups
#  3) standardize source ID columns
#  4) Remove duplicates


# Load libraries ----------------------------------------------------------

library(data.table)
library(fs)
library(argparse)
library(assertable)

library(mortdb, lib.loc = "FILEPATH")


# Set parameters ----------------------------------------------------------

# Get input arguments
parser <- ArgumentParser()
parser$add_argument("--version_id", type = "integer", required = TRUE,
                    help = "The version_id for this run")
parser$add_argument("--gbd_year", type = "integer", required = TRUE,
                    help = "GBD year associated with process run")

args <- parser$parse_args()
run_id <- args$version_id
gbd_year <- args$gbd_year

# Set unique identifying variables
source_ids <- c("ihme_loc_id", "source_type", "sex", "year", "deaths_nid", "deaths_underlying_nid")

usable_data_sources <- c("VR", "SRS", "DSP", "Civil Registration")

# Set output directories
main_folder       <- fs::path("FILEPATH", run_id)
run_input_folder  <- fs::path(main_folder, "inputs")
run_output_folder <- fs::path(main_folder, "outputs")


# Get maps ----------------------------------------------------------------

loc_map <- get_locations(gbd_year = gbd_year, level = "all")[, c(1, 3)]

sex_map <- data.table(sex_id = 1:3, sex = c("male", "female", "both"))

age_map <- get_age_map(gbd_year = gbd_year, type = "all")[, c(1, 6, 7)]
setnames(age_map, c("age_group_id", "age_start", "age_end"))
setorderv(age_map, c("age_end", "age_start"))
age_map[age_end == 125, age_end := Inf]

source_type_map <- get_mort_ids("source_type")[, 1:2]

# Match data source types to usable data source type names
invisible(lapply(usable_data_sources, function(x) {
  source_type_map[type_short %like% x, source_type := x]
  return(NULL)
}))

usable_source_type_map <- source_type_map[
  source_type %in% usable_data_sources,
  c("source_type_id", "source_type")
]


# Load pre-split data -----------------------------------------------------

pre_split_deaths <- fread(fs::path(run_input_folder, "vrp_pre_split.csv"))


# Get terminal ages -------------------------------------------------------

## NOTE:
#  Since we know every row to have a non-missing `origin` column (it was added
#  in the setup script), rows with missing `origin` are therefore artifacts
#  of merging on terminal age groups not present in the data and can be removed.

pre_split_terminal_deaths <- pre_split_deaths[
  age_map[is.infinite(age_end)],
  on = "age_group_id"
][!is.na(origin)]

setnames(pre_split_terminal_deaths, "age_start", "terminal_age_start")


# Standardize source ID columns -------------------------------------------

setnames(
  pre_split_terminal_deaths,
  c("nid", "underlying_nid", "year_id"),
  c("deaths_nid", "deaths_underlying_nid", "year")
)

terminal_age_by_source <- sex_map[
  loc_map[
    pre_split_terminal_deaths[usable_source_type_map, on = "source_type_id"],
    on = "location_id"
  ],
  c(..source_ids, "terminal_age_start", "origin"),
  on = "sex_id"
][!is.na(origin)]


# Remove duplicates -------------------------------------------------------

## Deduplication criteria:
#  1) Prefer higher terminal ages

## NOTE:
#  This code is a useful way to see how many terminal ages there are for a
#  source-year, and the origin (cod/noncod) of the differences:
#
# terminal_age_by_source[
#   ,
#   .(
#     N_total = .N,
#     N_origins = length(unique(origin)),
#     N_terms = length(unique(terminal_age_start))
#   ),
#   by = source_ids
# ]

dedup_terminal_age_by_source <- terminal_age_by_source[
  ,
  .(terminal_age_start = max(terminal_age_start)),
  by = source_ids
]


# Save --------------------------------------------------------------------

readr::write_csv(
  dedup_terminal_age_by_source,
  fs::path(run_output_folder, "original_terminal_ages.csv")
)
