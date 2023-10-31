
# Meta --------------------------------------------------------------------

# Description: Fits classic death distribution methods (GGB, SEG, and GGBSEG)
# Steps:
#   1. Load input population and deaths and subset to one location
#   2. Run GGB, SEG, and GGBSEG and save results
# Inputs:
#   * Processed data: "FILEPATH"
# Outputs:
#   * DDM results: "FILEPATH"


# Load libraries ----------------------------------------------------------

message(Sys.time(), " | Setup")

library(data.table)
library(haven)
library(DDMethods)
library(argparse)
library(magrittr)


# Command line arguments --------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", type = "character", required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)


# Load data ---------------------------------------------------------------

message(Sys.time(), " | Load inputs")

dt <- haven::read_dta(paste0("FILEPATH")) %>% setDT

# reshape dta file
dt <- dt[!is.na(agegroup0)]

agegroup_cols <- names(dt)[names(dt) %like% "agegroup"]
c1_cols <- names(dt)[names(dt) %like% "c1_"]
c2_cols <- names(dt)[names(dt) %like% "c2_"]
vr_cols <- names(dt)[names(dt) %like% "vr_"]
id_cols <- setdiff(names(dt), c(agegroup_cols, c1_cols, c2_cols, vr_cols))

dt <- melt(dt, id.vars = id_cols)
dt[variable %in% agegroup_cols, `:=` (
  variable = "age_start",
  grouping = gsub("agegroup", "", variable)
)]
dt[variable %in% c1_cols, `:=` (
  variable = "pop1",
  grouping = gsub("c1_", "", variable)
)]
dt[variable %in% c2_cols, `:=` (
  variable = "pop2",
  grouping = gsub("c2_", "", variable)
)]
dt[variable %in% vr_cols, `:=` (
  variable = "deaths",
  grouping = gsub("vr_", "", variable)
)]

dt <- dcast(
  data = dt,
  formula = ... ~ variable,
  value.var = "value"
)
dt <- dt[!is.na(age_start)]

dt[sex == "both", sex := "all"]
dt[, date1 := lubridate::ymd(paste0(substr(pop_years, 1, 4), "-01-01"))]
dt[, date2 := date1 + lubridate::days(round(time * 365))]

id_cols <- c(id_cols, "age_start")

# Data checks -------------------------------------------------------------

message(Sys.time(), " | Data checks")

# check for missing deaths
dt_missing_deaths <- unique(
  dt[is.na(deaths), list(ihme_loc_id, location_id, pop_years, sex)]
)
if (nrow(dt_missing_deaths) > 0) {
  warning(
    "Dropping missing deaths for these location-year-sex groupings:\n",
    paste(capture.output(print(dt_missing_deaths)), collapse = "\n")
  )
  dt_missing_deaths[, missing_deaths := T]
  dt <- merge(
    dt, dt_missing_deaths,
    by = c("ihme_loc_id", "location_id", "pop_years", "sex"),
    all = T
  )
  dt <- dt[is.na(missing_deaths)]
  dt[, c("missing_deaths") := NULL]
}

# check for zero deaths
dt[, `:=` (
  deaths_10to40 = sum(deaths[age_start >= 10 & age_start < 40]),
  deaths_40to60 = sum(deaths[age_start >= 40 & age_start < 60])
), by = setdiff(id_cols, "age_start")]
dt_zero_deaths <- unique(
  dt[deaths_10to40 == 0 | deaths_40to60 == 0,
     list(ihme_loc_id, location_id, pop_years, sex)]
)
if (nrow(dt_zero_deaths) > 0) {
  warning(
    "Dropping these location-year-sex groupings with too many zeros for deaths:\n",
    paste(capture.output(print(dt_zero_deaths)), collapse = "\n")
  )
  dt_zero_deaths[, zero_deaths := T]
  dt <- merge(
    dt, dt_zero_deaths,
    by = c("ihme_loc_id", "location_id", "pop_years", "sex"),
    all = T
  )
  dt <- dt[is.na(zero_deaths)]
  dt[, c("zero_deaths") := NULL]
}

# check for terminal age < 60 (will result in NA 20d40 which is needed for SEG)
dt[, open_age := max(age_start), by = setdiff(id_cols, "age_start")]
dt_terminal_below_60 <- unique(
  dt[open_age < 60, list(ihme_loc_id, location_id, pop_years, sex)]
)
if (nrow(dt_terminal_below_60) > 0) {
  warning(
    "Dropping these location-year-sex groupings with terminal age below 60:\n",
    paste(capture.output(print(dt_terminal_below_60)), collapse = "\n")
  )
  dt_terminal_below_60[, low_terminal := T]
  dt <- merge(
    dt, dt_terminal_below_60,
    by = c("ihme_loc_id", "location_id", "pop_years", "sex"),
    all = T
  )
  dt <- dt[is.na(low_terminal)]
  dt[, c("low_terminal") := NULL]
}

# check for zero population
dt_zero_pop <- unique(
  dt[pop1 == 0 | pop2 == 0, list(ihme_loc_id, location_id, pop_years, sex)]
)
if (nrow(dt_zero_pop) > 0) {
  warning(
    "Dropping these location-year-sex groupings with zeros in population:\n",
    paste(capture.output(print(dt_zero_pop)), collapse = "\n")
  )
  dt_zero_pop[, zero_pop := T]
  dt <- merge(
    dt, dt_zero_pop,
    by = c("ihme_loc_id", "location_id", "pop_years", "sex"),
    all = T
  )
  dt <- dt[is.na(zero_pop)]
  dt[, c("zero_pop") := NULL]
}


# Get Coale Demeny region -------------------------------------------------

message(Sys.time(), " | Coale Demeny setup")

# load in file with pre-specified CD regions per location
cd_types <- fread("FILEPATH")
cd_types <- cd_types[location_type == 4] # country-level
setnames(cd_types, c("iso3", "cd_type"), c("ihme_loc_id_merge", "cd_region"))
cd_types[lt_type != "Model life tables", cd_region := "CD West"]
cd_types[, cd_region := tolower(gsub("CD ", "", cd_region))]
cd_types <- cd_types[, .SD, .SDcols = c("ihme_loc_id_merge", "cd_region")]
cd_types[is.na(cd_region), cd_region := "west"]

cd_types[ihme_loc_id_merge == "ZAF", cd_region := "west"]

# merge to create 'cd_region' column for use in SEG
dt[, ihme_loc_id_merge := substr(ihme_loc_id, 1, 3)]
dt <- merge(dt, cd_types, by = "ihme_loc_id_merge", all.x = T)
dt[is.na(cd_region), cd_region := "west"]
dt[, c("ihme_loc_id_merge") := NULL]


# Run DDMs ----------------------------------------------------------------

message(Sys.time(), " | Run GGB")

# 1. GGB
ggb_results <- DDMethods::ggb(
  dt = dt,
  age_trim_lower = 5,
  age_trim_upper = 75,
  drop_open_interval = T,
  id_cols = id_cols,
  migration = F,
  input_deaths_annual = T,
  method_growth_rate = "hyc"
)[[2]]

message(Sys.time(), " | Run SEG")

# 2. SEG
seg_results <- DDMethods::seg(
  dt = dt,
  age_trim_lower = 55,
  age_trim_upper = 90,
  drop_open_interval = T,
  id_cols = id_cols,
  migration = F,
  input_deaths_annual = T,
  method_numerator_denominator_type = "Nx"
)[[2]]

message(Sys.time(), " | Run GGBSEG")

# 3. GGBSEG
ggbseg_results <- DDMethods::ggbseg(
  dt = dt,
  age_trim_lower_ggb = 5,
  age_trim_upper_ggb = 75,
  age_trim_lower_seg = 55,
  age_trim_upper_seg = 90,
  drop_open_interval_ggb = T,
  drop_open_interval_seg = T,
  id_cols = id_cols,
  migration = F,
  input_deaths_annual = T,
  method_growth_rate = "hyc",
  method_numerator_denominator_type = "Nx"
)[[2]]


# Combine and save --------------------------------------------------------

message(Sys.time(), " | Combine and save")

setnames(ggb_results, "completeness", "ggb")
setnames(seg_results, "completeness", "seg")
setnames(ggbseg_results, "completeness", "ggbseg")

keep_cols <- c("ihme_loc_id", "pop_years", "deaths_years", "source_type",
               "country", "pop_source", "pop_footnote", "pop_nid",
               "underlying_pop_nid", "deaths_source", "deaths_footnote",
               "deaths_nid", "deaths_underlying_nid", "sex")

ggb_results <- ggb_results[, .SD, .SDcols = c(keep_cols, "ggb")]
seg_results <- seg_results[, .SD, .SDcols = c(keep_cols, "seg")]
ggbseg_results <- ggbseg_results[, .SD, .SDcols = c(keep_cols, "ggbseg")]

# merge
all_results <- merge(ggb_results, seg_results, by = keep_cols, all = T)
all_results <- merge(all_results, ggbseg_results, by = keep_cols, all = T)

# reformat ihme_loc_id
all_results[, ihme_loc_id := tstrsplit(ihme_loc_id, "&&", keep = 1)]

# reformat sex
all_results[sex == "all", sex := "both"]

# save all results for vetting / diagnostics
readr::write_csv(
  all_results, paste0("FILEPATH")
)

# do some subsetting
all_results[!data.table::between(ggb, 0, 5), ggb := NA]
all_results[!data.table::between(seg, 0, 5), seg := NA]
all_results[!data.table::between(ggbseg, 0, 5), ggbseg := NA]
all_results <- all_results[!(is.na(ggb) & is.na(seg) & is.na(ggbseg))]

# save cleaned up results
readstata13::save.dta13(all_results, paste0("FILEPATH"))
