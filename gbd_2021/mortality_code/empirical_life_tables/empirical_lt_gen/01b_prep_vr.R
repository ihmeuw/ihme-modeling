
# Meta --------------------------------------------------------------------

# Title: Prep DDM VR input
# Description:
#  1) Read in VR data from DDM output, re-format, perform exclusions
#  2) Completeness adjustment
#  3) Collapse to common terminal age between deaths and pop by source-year
#  4) Calculate mx = deaths/population
#  5) Perform mx-based exclusions


# Load libraries ----------------------------------------------------------

library(data.table)
library(argparse)
library(assertable)

library(mortdb, lib.loc = "FILEPATH")

# ELT functions loaded after command line args

# Set parameters ----------------------------------------------------------

# Get input arguments
parser <- ArgumentParser()
parser$add_argument("--version_id", type = "integer", required = TRUE,
                    help = "The version_id for this run")
parser$add_argument("--gbd_year", type = "integer", required = TRUE,
                    help = "GBD year associated with process run")
parser$add_argument("--covid_years", type="integer", nargs="+", required = TRUE,
                    help = "Years where VR data should be treated as including COVID")
parser$add_argument("--code_dir", type = "character", required = TRUE,
                    help = "Directory where ELT code is cloned")

args <- parser$parse_args()
run_id <- args$version_id
gbd_year <- args$gbd_year
code_dir <- args$code_dir
covid_years <- args$covid_years

# load ELT functions
devtools::load_all(paste0(code_dir, "/empirical_lt_gen/empir_gen_funcs"))

# Set global script parameters
terminal_age_end <- 120

# Get parent version for DDM
parent_lineage <- get_proc_lineage("life table empirical", "data", run_id = run_id)
parent_ddm_run <- parent_lineage[parent_process_name == "ddm estimate" & exclude == 0, parent_run_id]

# Get parent version for VRP
parent_lineage_ddm <- get_proc_lineage("ddm", "estimate", run_id = parent_ddm_run)
parent_vrp_run <- parent_lineage_ddm[
  parent_process_name == "death number empirical data" & exclude == 0,
  parent_run_id
]

# Set unique identifying variables
source_ids <- c("ihme_loc_id", "source_type", "sex", "year", "deaths_nid", "deaths_underlying_nid")

# Set output directories
main_folder     <- paste0("FILEPATH", run_id)
run_output_folder <- paste0(main_folder, "/outputs")


# Get maps ----------------------------------------------------------------

# Import location table and country lifetable-type designation
loc_map <- setDT(get_locations(gbd_year = gbd_year, level = "all", hiv_metadata = T))


# Import and format VR and pop --------------------------------------------

## NOTE:
#  Using deaths and pop prepped in DDM process:
#   * deaths originally from VR prep process
#   * pop is from empirical population process

vr_raw_wide <- setDT(haven::zap_formats(haven::read_dta(
  paste0("FILEPATH", parent_ddm_run, "/data/d10_45q15.dta"),
  encoding = "latin1"
)))

vr_raw <- melt(
  vr_raw_wide,
  id.vars = c(source_ids, "comp_u5", "comp", "max_age_gap"),
  measure.vars = grep("^vr_|^c1_|^obsasmr_", colnames(vr_raw_wide), value = TRUE),
  variable.name = "measure"
)

# Convert column types
vr_raw[, `:=`(
  deaths_nid = as.numeric(deaths_nid),
  deaths_underlying_nid = as.numeric(deaths_underlying_nid)
)]

# Extract measure and age group information
vr_raw[, c("measure", "age_group_name") := tstrsplit(measure, "_", fixed = TRUE, type.convert = TRUE)]
vr_raw[, c("age", "age_end") := tstrsplit(age_group_name, "to|plus", type.convert = TRUE)]
vr_raw[, age_group_name := NULL]

# Recode values
vr_raw[measure == "vr", measure := "deaths"]
vr_raw[measure == "c1", measure := "population"]
vr_raw[measure == "obsasmr", measure := "obs_mx"]
vr_raw[source_type == "CR", source_type := "Civil Registration"]
vr_raw[comp_u5 > 1, comp_u5 := 1]
vr_raw[comp > 1, comp := 1]

# Exclude data ------------------------------------------------------------

## Exclusion criteria:
#
#  1) Drop missing deaths or population value
#  2) Drop missing completeness or under 5 completeness
#  3) Drop years before 1950
#  4) Drop sources with age groups wider than 5 years
#  5) Drop sources outside of VR or registries
#  6) Drop old ages in South Africa

## NOTE on age group width exlusions:
#
#  Drop locations where the input data has more than 5-year-wide age groups,
#  because DDM will then equally split the data from, say, a 10-year age group
#  to a 5-year one. This messes with age patterns of the life tables in age 
#  groups such as 15-19 or older ages where mortality shifts rapidly and a 
#  direct split does not suffice.

## NOTE on South Africa exclusions:
#
#  Drop:
#   1) non-terminal age groups 80 and upwards
#   2) terminal ages group 70 and upwards
#  because of unrealistic old-age trends and time discontinuities (2005-2006).

vr_included <- vr_raw[
  !is.na(value) &
    !is.na(comp) &
    !is.na(comp_u5) &
    year >= 1950 &
    max_age_gap <= 5 &
    source_type %in% c("VR", "SRS", "DSP", "Civil Registration") &
    !(
      ihme_loc_id %like% "^ZAF" &
      ((age >= 80 & !is.na(age_end)) | (is.na(age_end) & age >= 70))
    )
]

vr_included[, max_age_gap := NULL]


# Completeness designations -----------------------------------------------

## NOTE on completeness exlcusion criteria:
#
#  1) Outlier if completeness < 0.5
#  2) Mark as location specific if completeness < 0.85

comp_outliers <- unique(vr_included[
  comp < .5 & !ihme_loc_id %in% comp_exclusion_locs,
  .(ihme_loc_id, year, sex, source_type)
])

comp_loc_specific <- unique(vr_included[
  comp < .85,
  .(ihme_loc_id, year, sex, source_type)
])

## NOTE:
#  Save completeness outliers and completeness-based location-specific
#  designations, used in `03_select_lts` script

comp_outliers[, comp_under_50 := 1]
comp_loc_specific[, comp_under_85 := 1]

readr::write_csv(comp_outliers, paste0(run_output_folder, "/completeness_outliers.csv"))
readr::write_csv(comp_loc_specific, paste0(run_output_folder, "/comp_loc_specific.csv"))


# Completeness adjustments ------------------------------------------------

vr_adj <- copy(vr_included)

## NOTE:
#  For 5-9 and 10-14, proportionally weight the under-5 and adult completeness
#  as they're partially contained within both.

vr_adj[, `:=`(
  comp_5_9 = (comp_u5 * 2/3) + (comp * 1/3),
  comp_10_14 = (comp_u5 * 1/3) + (comp * 2/3)
)]

vr_adj[measure != "population" & age_end < 5,               value := value / comp_u5]
vr_adj[measure != "population" & age ==  5 & age_end == 9,  value := value / comp_5_9]
vr_adj[measure != "population" & age == 10 & age_end == 14, value := value / comp_10_14]
vr_adj[measure != "population" & age >= 15,                 value := value / comp]

comp_cols <- grep("^comp", colnames(vr_adj), value = TRUE)
vr_adj[, (comp_cols) := NULL]


# Reset terminal ages -----------------------------------------------------

original_terminal_ages <- fread(paste0(run_output_folder, "/original_terminal_ages.csv"))
setnames(original_terminal_ages, "terminal_age_start", "terminal_age_reset")

# Make column types match VR data
original_terminal_ages <- original_terminal_ages[, `:=`(
  year = as.numeric(year),
  deaths_nid = as.numeric(deaths_nid),
  deaths_underlying_nid = as.numeric(deaths_underlying_nid)
)]

vr_new_term <- vr_adj[dt_reset_ids, on = source_ids]
vr_keep_term <- vr_adj[!dt_reset_ids, on = source_ids]

vr_reset_deaths <- reset_terminal_ages(
  dt = vr_new_term[measure == "deaths"],
  reset_dt = original_terminal_ages,
  source_ids = source_ids
)

vr_reset <- rbindlist(
  list(vr_reset_deaths, vr_new_term[measure == "population"]),
  use.names = TRUE
)

vr_terminal_reset <- rbind(vr_keep_term, vr_reset)


# Collapse ages -----------------------------------------------------------

## NOTE:
#  The open age interval for deaths and population aren't necessarily the same
#  within a source-year, so we collapse data to the lowest open age interval
#  by source-year. This lets us later calculate a terminal mx for every 
#  source-year.

new_terminal_values <- collapse_terminal_ages(
  vr_terminal_reset[measure %in% c("deaths", "population")],
  source_ids
)

vr_collapse <- rbind(
  vr_terminal_reset[!new_terminal_values, on = c(source_ids, "measure", "age", "age_end")],
  new_terminal_values,
  use.names = TRUE,
  fill = TRUE
)

# Manually set open age for some locations
dt_manual_open_age <- data.table(
  ihme_loc_id = c("ARE"),
  age = c(70)
)

# NOTE: join must be in this order to get right age column
dt_manual_to_collapse <- dt_manual_open_age[
  vr_collapse,
  on = .(ihme_loc_id, age <= age),
  nomatch = NULL
]

dt_manual_collapsed<- dt_manual_to_collapse[
  j = .(value = sum(value), age = min(age), age_end = NA),
  by = c(source_ids, "measure")
]

vr_collapse2 <- rbind(
  vr_collapse[
    !dt_manual_to_collapse,
    on = c(source_ids, "measure", "age", "age_end")
  ],
  dt_manual_collapsed,
  use.names = TRUE
)


# Compute mx --------------------------------------------------------------

vr_mx <- dcast(vr_collapse2, ...~measure, value.var = "value")
vr_mx[, mx := deaths / population]

# Use mx from India SRS reports directly,
vr_mx[
  ihme_loc_id %like% "IND" & source_type == "SRS" & year >= 1995 &
    !is.na(obs_mx),
  mx := obs_mx
]

vr_mx <- vr_mx[!is.na(mx), -"obs_mx"]

test_no_terminal <- vr_mx[
  ,
  .(n_terminal_mx = sum(is.na(age_end))),
  by = source_ids
][n_terminal_mx == 0]

if (nrow(test_no_terminal[!ihme_loc_id %like% "ZAF"]) > 0) {
  stop("Non ZAF location are still missing terminal mx values")
}


# Final exclusions --------------------------------------------------------

## NOTE on final exclusion criteria:
#
#  Drop lifetables where there is a value of mx less than 1e-5 under age 80.
#  If zeros are above age 80, we will drop those ages and above only, not the 
#    entire LT

low_mx_exclusions <- unique(vr_mx[age < 80 & mx < 1e-5, ..source_ids])

readr::write_csv(low_mx_exclusions, paste0(run_output_folder, "/mx_below_one_in_100000.csv"))

vr_mx[age >= 80 & mx < 1e-5, age_mx_low := min(age), by = source_ids]
vr_mx[is.na(age_mx_low), age_mx_low := 999]
vr_mx[, min_age_mx_low := min(age_mx_low, na.rm = TRUE), by = source_ids]

vr_final <- vr_mx[!low_mx_exclusions, on = source_ids][age < min_age_mx_low]
vr_final[, c("age_mx_low", "min_age_mx_low") := NULL]


# Save formatted VR prep --------------------------------------------------

vr_formatted <- copy(vr_final)

vr_formatted[is.na(age_end), age_end := terminal_age_end]

readr::write_csv(vr_formatted, paste0(run_output_folder, "/all_lts_1.csv"))
