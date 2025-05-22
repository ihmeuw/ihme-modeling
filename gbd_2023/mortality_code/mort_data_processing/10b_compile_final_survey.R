
## Meta ------------------------------------------------------------------------

# Description: Compile gbd ages data and split nonstandard ages data to produce final survey data

# Steps:
#   1. Initial setup
#   2. Read in internal inputs
#   3. Read in and clean split nonstandard ages survey data
#   4. Read in and clean gbd ages survey data
#   5. Combine
#   6. Save

# Load libraries ---------------------------------------------------------------

library(arrow)
library(assertable)
library(data.table)
library(dplyr)

# Command line arguments -------------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir",
  type = "character",
  required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# Define the main directory here
if (interactive()) {
  main_dir <- "INSERT_PATH_HERE"
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
}

# Setup ------------------------------------------------------------------------

# get config
config <- config::get(
  file = paste0("FILEPATH"),
  use_parent = FALSE
)
list2env(config$default, .GlobalEnv)

Sys.unsetenv("PYTHONPATH")

source(fs::path("FILEPATH"))

# Read in internal inputs ------------------------------------------------------

age_map_extended <- fread(fs::path("FILEPATH"))
age_map_gbd <- fread(fs::path("FILEPATH"))

locs <- fread(fs::path("FILEPATH"))

# read in formatted pop
pop <- fread(fs::path("FILEPATH"))

# read in nonstandard survey data
pre_split_survey_orig <- fread(
  fs::path("FILEPATH")
)
pre_split_survey <- pre_split_survey_orig[location_id %in% locs$location_id]
pre_split_survey <- pre_split_survey[!(source_type_name %in% c("Census", "Survey") & outlier == 1)]

# read in shocks
shocks_aggregated <- fread(fs::path("FILEPATH"))

# read in reference 5q0
reference_5q0 <- fread(fs::path("FILEPATH"))

reference_vr_5q0 <- reference_5q0[source_name == "VR"]
reference_vr_5q0[, loc_yr := paste(location_id, year_id, sep = "_")]

# read in completeness estimates
dt_comp_est <- fread(fs::path("FILEPATH"))

# Read in and clean gbd ages survey data ---------------------------------------

onemod_h1 <- arrow::read_parquet(
  fs::path("FILEPATH")
)

survey_onemod <- onemod_h1[source_type_name %in% c("CBH", "SIBS", "Census", "Survey")]

survey_onemod <- purrr::discard(survey_onemod, ~all(is.na(.)))

survey_onemod[source_type_name == "SIBS", sample_size := sample_size_adj]
survey_onemod <- survey_onemod[,
  !c("mx_semi_adj", "mx_se_semi_adj", "qx", "qx_semi_adj", "qx_adj", "qx_se",
     "qx_se_semi_adj", "qx_se_adj", "sample_size_semi_adj", "sample_size_adj")
]

survey_onemod[outlier > 1, outlier := 1]
survey_onemod <- survey_onemod[, !c("ihme_loc_id", "age_start", "age_end")]

survey_onemod[, onemod_process_type := "standard gbd age group data"]

# Read in and clean split nonstandard ages survey data -------------------------

survey_split <- fread(
  fs::path("FILEPATH")
)

# handle sex_id 3 rows by giving a proportion of it (based on population) to the sex-specific rows
sex_id_3_survey <- pre_split_survey[sex_id == 3]

orig_names <- names(sex_id_3_survey)
nrow_before <- nrow(sex_id_3_survey)
sex_split_survey <- sex_id_3_survey |>
  slice(rep(1:n(), each = 2)) |>
  mutate(sex_id = rep(c(1, 2), length.out = n())) |>
  filter(sex_id %in% c(1, 2)) |>
  left_join(pop, by = c("location_id", "year_id", "age_group_id", "sex_id"))

stopifnot(nrow(sex_split_survey) / 2 == nrow_before)

# group by location-year-age_group_id and calculate the proportion of the population
sex_split_survey <- sex_split_survey |>
  group_by(location_id, year_id, age_group_id) |>
  mutate(pop_prop = population / sum(population)) |>
  ungroup() |>
  mutate(sample_size = sample_size * pop_prop) |>
  select(all_of(orig_names))

# remove the original rows with sex_id == 3 from the data
pre_split_survey <- pre_split_survey[sex_id != 3]

# remove sex split rows that we already have sex specific data for
sex_split_survey <- sex_split_survey |>
  anti_join(
    pre_split_survey,
    by = c("location_id", "year_id", "sex_id", "age_group_id", "nid", "underlying_nid", "source_type_name")
  )

# combine the modified sex split rows back with the original dataframe
pre_split_survey <- bind_rows(pre_split_survey, sex_split_survey)

pre_split_survey <- pre_split_survey |>
  select(location_id, year_id, sex_id, age_group_id, nid, underlying_nid, source_type_name, sample_size) |>
  left_join(age_map_extended, by = "age_group_id") |>
  rename(age_start_orig = age_start, age_end_orig = age_end) |>
  select(-age_group_id)

# merge on the original sample_size
survey_split <- merge(
  survey_split,
  pre_split_survey,
  by = c(
    "location_id", "year_id", "sex_id", "nid", "underlying_nid", "source_type_name",
    "age_start_orig", "age_end_orig"
  ),
  all.x = TRUE
)

demUtils::assert_is_unique_dt(
  survey_split,
  id_cols = c(
    "location_id", "year_id", "sex_id", "nid", "underlying_nid", "source_type_name",
    "age_start", "age_end", "age_start_orig", "age_end_orig"
  )
)

survey_split[, sample_size_PRE := sample_size]

# calculate sample_size using binomial equation
survey_split[
  source_type_name %in% c("Census", "Survey"),
  sample_size := (ifelse(mx >= 0.99, 0.99, mx) * (1 - ifelse(mx >= 0.99, 0.99, mx))) / mx_se^2
]

# calculate sample_size by splitting aggregate sample_size based on the population distribution
survey_split[, pop_dist := population / sum(population), by = "id"]
survey_split[
  source_type_name %in% c("CBH", "SBH", "SIBS"),
  sample_size := sample_size * pop_dist
]
survey_split[, pop_dist := NULL]

# check where back calculated sample sizes from the age-split mx_se values might be hugely inflated
survey_split_inflated <- survey_split |>
  filter(sample_size > sample_size_PRE) |>
  mutate(ratio = sample_size / sample_size_PRE)

survey_split_adjust <- survey_split |>
  filter(id %in% survey_split_inflated$id)

# check if we have any ids where there are more than one sample_size_PRE values
check <- survey_split_adjust |>
  group_by(id) |>
  summarise(n_unique_sample_size_PRE = n_distinct(sample_size_PRE)) |>
  filter(n_unique_sample_size_PRE > 1)
stopifnot(nrow(check) == 0)

survey_split_adjust <- survey_split_adjust |>
  group_by(id) |>
  mutate(sum_sample_size = sum(sample_size)) |>
  ungroup() |>
  mutate(fraction_of_summed_ss = sample_size / sum_sample_size) |>
  mutate(final_sample_size = fraction_of_summed_ss * sample_size_PRE) |>
  select(location_id, year_id, sex_id, nid, underlying_nid, source_type_name, age_start, age_end, age_start_orig, age_end_orig, final_sample_size)

# bring adjusted sample size data back into main data
survey_split <- survey_split |>
  left_join(
    survey_split_adjust,
    by = c(
      "location_id", "year_id", "sex_id", "nid", "underlying_nid", "source_type_name",
      "age_start", "age_end", "age_start_orig", "age_end_orig"
    )
  ) |>
  mutate(sample_size = ifelse(is.na(final_sample_size), sample_size, final_sample_size)) |>
  select(-final_sample_size)

# final check that sample_size is never greater than sample_size_PRE
if (nrow(survey_split[sample_size > sample_size_PRE]) > 0) stop("Sample size greater than original group's sample size.")

# drop oldest SIBS age groups
survey_split <- survey_split[!(source_type_name == "SIBS" & age_group_id %in% 15:16)]

# for CBH, SBH, and SIBS, we want the population to be the same as sample_size
survey_split[source_type_name %in% c("CBH", "SBH", "SIBS"), population := sample_size]

# final cleaning
survey_split[, `:=` (id = NULL, age_start = NULL, age_end = NULL)]
survey_split[, onemod_process_type := "age-sex split data"]

# add outliers to split data
survey_split[, outlier := ifelse(mx >= 1 & !(age_group_id %in% 2:3) & !source_type_name %in% c("Survey", "Census"), 1, 0)]

survey_split[source_type_name == "SBH" & location_id == 53, outlier := 1] # SRB
survey_split[source_type_name == "SBH" & nid == 19787, outlier := 1] # IND DHS 1992-93

# deduplicate split CBH data (by outliering duplicates)
survey_split_cbh <- survey_split[source_type_name == "CBH"]
unsplit_cbh <- survey_onemod[source_type_name == "CBH"]

survey_split_cbh[, nid_loc := paste0(nid, "_", location_id)]
unsplit_cbh[, nid_loc := paste0(nid, "_", location_id)]

historical_cbh <- fread(
  fs::path("FILEPATH")
)

survey_split_cbh[, nid_loc_year := paste0(nid, "_", location_id, "_", year_id)]
historical_cbh[, nid_loc_year := paste0(nid, "_", location_id, "_", year_id)]

survey_split_cbh[
  age_group_id %in% 2:3 & age_end_orig == 0.0767123 & nid_loc %in% unique(unsplit_cbh[age_group_id %in% 2:3]$nid_loc),
  outlier := 1
]

survey_split_cbh[
  age_group_id %in% 2:3 & age_end_orig == 5 & !(nid_loc_year %in% historical_cbh$nid_loc_year) &
    nid_loc %in% unique(survey_split_cbh[age_end_orig == 0.0767123]$nid_loc),
  outlier := 1
]

survey_split_cbh[
  age_group_id %in% 2:3 & !(nid_loc_year %in% historical_cbh$nid_loc_year) &
    nid_loc %in% unique(unsplit_cbh[age_group_id %in% 2:3 & outlier == 0]$nid_loc),
  outlier := 1
]

survey_split_cbh[, nid_loc_age := paste0(nid, "_", location_id, "_", age_group_id)]
unsplit_cbh[, nid_loc_age := paste0(nid, "_", location_id, "_", age_group_id)]

survey_split_cbh[
  !(age_group_id %in% 2:3) & !(nid_loc_year %in% historical_cbh$nid_loc_year) &
    nid_loc_age %in% unique(unsplit_cbh[!(age_group_id %in% 2:3) & outlier == 0]$nid_loc_age),
  outlier := 1
]

survey_split_cbh[, nid_loc := NULL]
survey_split_cbh[, nid_loc_age := NULL]
survey_split_cbh[, nid_loc_year := NULL]

demUtils::assert_is_unique_dt(
  survey_split_cbh[outlier == 0],
  id_cols = c("location_id", "year_id", "sex_id", "nid", "underlying_nid", "source_type_name", "age_group_id")
)

survey_split <- rbind(survey_split[source_type_name != "CBH"], survey_split_cbh)

# checks
assertable::assert_values(
  data = survey_split,
  colnames = "sample_size",
  test = "gt",
  test_val = 0,
  warn_only = TRUE
)

assertable::assert_values(
  data = survey_split,
  colnames = "population",
  test = "gt",
  test_val = 0,
  warn_only = TRUE
)

assertable::assert_values(
  survey_split[!source_type_name %in% c("Census", "Survey")],
  colnames = "sample_size",
  test = "lte",
  test_val = survey_split[!source_type_name %in% c("Census", "Survey")]$population,
  warn_only = TRUE
)

# combine
survey_final <- rbind(
  survey_onemod,
  survey_split,
  fill = TRUE
)

# Add sample_size_orig for values that are not adjusted - should be the same as sample size.
survey_final <- survey_final |>
  mutate(sample_size_orig = sample_size) |>
  rename(population_orig = population) |>
  left_join(pop[, c("location_id", "sex_id", "year_id", "age_group_id", "population")], by = c("location_id", "sex_id", "year_id", "age_group_id")) |>
  select(-population_orig, -sample_size_PRE)

# outlier shock years if shock ratio > 0.005 for full age group
pop_0_5 <- pop[sex_id == 3 & age_group_id %in% c(2, 3, 388, 389, 238, 34)]
pop_0_5[, population_0_5 := sum(population), by = c("location_id", "year_id")]

pop_0_15 <- pop[sex_id == 3 & age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7)]
pop_0_15[, population_0_15 := sum(population), by = c("location_id", "year_id")]

pop_15_49 <- pop[sex_id == 3 & age_group_id %in% 8:14]
pop_15_49[, population_15_49 := sum(population), by = c("location_id", "year_id")]

survey_final <- merge(
  survey_final,
  unique(pop_0_5[, c("location_id", "year_id", "population_0_5")]),
  by = c("location_id", "year_id"),
  all.x = TRUE
)

survey_final <- merge(
  survey_final,
  unique(pop_0_15[, c("location_id", "year_id", "population_0_15")]),
  by = c("location_id", "year_id"),
  all.x = TRUE
)

survey_final <- merge(
  survey_final,
  unique(pop_15_49[, c("location_id", "year_id", "population_15_49")]),
  by = c("location_id", "year_id"),
  all.x = TRUE
)

shocks_0_5 <- shocks_aggregated[age_group_id %in% c(2, 3, 388, 389, 238, 34)]
shocks_0_5[, shock_deaths_0_5 := sum(shock_deaths), by = c("location_id", "year_id")]

shocks_0_15 <- shocks_aggregated[age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7)]
shocks_0_15[, shock_deaths_0_15 := sum(shock_deaths), by = c("location_id", "year_id")]

shocks_15_49 <- shocks_aggregated[age_group_id %in% 8:14]
shocks_15_49[, shock_deaths_15_49 := sum(shock_deaths), by = c("location_id", "year_id")]

survey_final <- merge(
  survey_final,
  unique(shocks_0_5[, c("location_id", "year_id", "shock_deaths_0_5")]),
  by = c("location_id", "year_id"),
  all.x = TRUE
)

survey_final <- merge(
  survey_final,
  unique(shocks_0_15[, c("location_id", "year_id", "shock_deaths_0_15")]),
  by = c("location_id", "year_id"),
  all.x = TRUE
)

survey_final <- merge(
  survey_final,
  unique(shocks_15_49[, c("location_id", "year_id", "shock_deaths_15_49")]),
  by = c("location_id", "year_id"),
  all.x = TRUE
)

survey_final[source_type_name == "SBH" & shock_deaths_0_5 / population_0_5 > 0.005, outlier := 2]
survey_final[source_type_name == "CBH" & shock_deaths_0_15 / population_0_15 > 0.005, outlier := 2]
survey_final[source_type_name == "SIBS" & shock_deaths_15_49 / population_15_49 > 0.005, outlier := 2]

survey_shock_years <- unique(survey_final[outlier == 2, c("location_id", "year_id", "source_type_name")])
survey_shock_years <- merge(
  survey_shock_years,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)
readr::write_csv(
  survey_shock_years,
  fs::path("FILEPATH")
)

survey_final[outlier == 2, outlier := 1]

survey_final[, `:=` (population_0_5 = NULL, population_0_15 = NULL, population_15_49 = NULL)]
survey_final[, `:=` (shock_deaths_0_5 = NULL, shock_deaths_0_15 = NULL, shock_deaths_15_49 = NULL)]

survey_final[source_type_name == "SIBS" & nid == 19546, outlier := 0] # unoutlier ERI 1994-95 DHS

# outlier split SBH if CBH available
survey_final[, id := paste0(nid, "_", location_id, "_", sex_id, "_", age_group_id)]
survey_final[
  source_type_name == "SBH" & age_group_id %in% 2:3 &
    id %in% survey_final[source_type_name == "CBH" & age_group_id %in% 2:3 & outlier == 0]$id,
  outlier := 1
]
survey_final[
  source_type_name == "SBH" & !(age_group_id %in% 2:3) &
    id %in% survey_final[source_type_name == "CBH" & !(age_group_id %in% 2:3) & outlier == 0]$id,
  outlier := 1
]
survey_final[, id := NULL]

# outlier CBH and SBH for locations where there is reference VR data in 5q0
survey_final[, loc_yr := paste(location_id, year_id, sep = "_")]

survey_final[source_type_name %in% c("CBH", "SBH") & location_id != 122 & loc_yr %in% reference_vr_5q0$loc_yr, outlier := 1] # excluding ECU
survey_final[, loc_yr := NULL]

survey_final[source_type_name %in% c("CBH", "SBH") & location_id == 13, outlier := 1] # MYS
survey_final[source_type_name %in% c("CBH", "SBH") & location_id == 52, outlier := 1] # ROU
survey_final[source_type_name %in% c("CBH", "SBH") & location_id == 97, outlier := 1] # ARG
survey_final[source_type_name %in% c("CBH", "SBH") & location_id == 99, outlier := 1] # URY
survey_final[source_type_name %in% c("CBH", "SBH") & location_id == 126, outlier := 1] # CRI
survey_final[source_type_name %in% c("CBH", "SBH") & location_id == 133, outlier := 1] # VEN
survey_final[source_type_name %in% c("CBH", "SBH") & location_id == 145, outlier := 1] # KWT
survey_final[source_type_name %in% c("CBH", "SBH") & location_id == 186, outlier := 1] # SYC
survey_final[source_type_name %in% c("CBH", "SBH") & location_id == 361, outlier := 1] # CHN_361 (Macao)

# outlier SIBS for locations where there is complete VR
# exceptions: don't outlier NAM and ZAF subs; fully outlier BRA and PHL subs
complete_vr <- dt_comp_est[
  source_type == "VR" & comp_adult >= 1
]

complete_vr <- complete_vr[
  !(location_id %in% locs[ihme_loc_id == "NAM" | ihme_loc_id %like% "ZAF_"]$location_id)
]

complete_vr[, loc_yr_sex := paste(location_id, year_id, sex_id, sep = "_")]
survey_final[, loc_yr_sex := paste(location_id, year_id, sex_id, sep = "_")]

survey_final[
  source_type_name == "SIBS" & loc_yr_sex %in% complete_vr$loc_yr_sex,
  outlier := 1
]
survey_final[, loc_yr_sex := NULL]

# manual outliering
survey_final[mx >= 1 & !(age_group_id %in% 2:3) & !source_type_name %in% c("Survey", "Census"), outlier := 1]

survey_final[location_id == 63 & !(nid %in% reference_5q0[location_id == 63 & reference == 1 & method_name == "CBH"]$nid), outlier := 1] # UKR
survey_final[location_id %in% c(44934, 44939, 50559), outlier := 1] # UKR subs

# manual outliering - CBH
survey_final[source_type_name == "CBH" & location_id == 26 & sex_id == 1 & age_group_id == 7, outlier := 1] # PNG 10-14 males
survey_final[source_type_name == "CBH" & location_id == 53, outlier := 1] # SRB

survey_final[source_type_name == "CBH" & nid == 19533, outlier := 1] # SLV DHS 1985
survey_final[source_type_name == "CBH" & nid == 76850, outlier := 1] # COM DHS 2012-13
survey_final[source_type_name == "CBH" & nid == 427983, outlier := 1] # SUR MICS 2018
survey_final[source_type_name == "CBH" & nid == 453346 & year_id == 1996 & sex_id == 1, outlier := 1] # TON MICS 2019
survey_final[source_type_name == "CBH" & nid == 541281, outlier := 1] # COM MICS 2022

survey_final[
  source_type_name == "CBH" & location_id %in% c(165, 53615:53621) & year_id < 1976 & onemod_process_type == "age-sex split data",
  outlier := 1
] # PAK pre-1976 split data

# manual outliering - SBH
survey_final[source_type_name == "SBH" & nid == 1048, outlier := 1] # BLR 1999 Census
survey_final[source_type_name == "SBH" & nid == 43566, outlier := 1] # PAK 1973 Survey (also Punjab)

survey_final[source_type_name == "SBH" & location_id == 164 & nid == 140201 & underlying_nid == 9217, outlier := 1] # NPL 1971 Census
survey_final[source_type_name == "SBH" & location_id == 164 & nid == 140201 & underlying_nid == 9218, outlier := 1] # NPL 1981 Census

survey_final[source_type_name == "SBH" & location_id == 99, outlier := 1] # URY
survey_final[source_type_name == "SBH" & location_id == 176, outlier := 1] # COM

survey_final[
  source_type_name == "SBH" & location_id %in% locs[ihme_loc_id %like% "KEN_"]$location_id & nid == 20109,
  outlier := 1
] # KEN DHS 1988-89

survey_final[source_type_name == "SBH" & location_id == 115 & age_group_id == 2, outlier := 1] # JAM ENN

# manual outliering - SIBS
survey_final[
  source_type_name == "SIBS" & location_id %in% locs[ihme_loc_id %like% "BRA_|PHL_"]$location_id,
  outlier := 1
]
survey_final[
  source_type_name == "SIBS" & location_id == 26 & year_id == 2008 & sex_id == 2 & age_group_id == 13,
  outlier := 1
] # PNG
survey_final[
  source_type_name == "SIBS" & location_id == 26 & year_id == 2006 & sex_id == 2 & age_group_id == 14,
  outlier := 1
] # PNG
survey_final[
  source_type_name == "SIBS" & location_id == 123 & nid %in% c(210231, 270404),
  outlier := 1
] # PER Continuous DHS 2009 & 2013
survey_final[
  source_type_name == "SIBS" & location_id == 160,
  outlier := 1
] # AFG
survey_final[
  source_type_name == "SIBS" & location_id == 162 & year_id == 1995 & sex_id == 1,
  outlier := 1
] # BTN
survey_final[
  source_type_name == "SIBS" & location_id == 185 & year_id %in% 1993:1994,
  outlier := 1
] # RWA
survey_final[
  source_type_name == "SIBS" & location_id == 197 & year_id %in% 1992:1993 & age_group_id == 12,
  outlier := 1
] # SWZ
survey_final[
  source_type_name == "SIBS" & location_id == 197 & year_id %in% c(1994, 1997),
  outlier := 1
] # SWZ
survey_final[
  source_type_name == "SIBS" & location_id == 198 & year_id %in% 1979:1983,
  outlier := 1
] # ZWE DHS 1994 (only initial years)
survey_final[
  source_type_name == "SIBS" & location_id == 198 & year_id <= 1990 & age_group_id %in% 11:14,
  outlier := 1
] # ZWE

# completeness adjustment for survey and census data
survey_final_adjusted <- survey_final[source_type_name %in% c("Survey", "Census")]
survey_final_adjusted <- merge(
  survey_final_adjusted,
  dt_comp_est,
  by = c("location_id", "year_id", "sex_id", "source_type_id", "source_type"),
  all.x = TRUE
)

survey_final_adjusted[
  is.na(comp_adult) & is.na(comp_10_14) & is.na(comp_5_9) & is.na(comp_u5),
  ':=' (comp_adult = 1, comp_10_14 = 1, comp_5_9 = 1, comp_u5 = 1, missing_completeness = 1)
]

survey_final_adjusted[
  !is.na(comp_adult) & is.na(comp_10_14) & is.na(comp_5_9) & is.na(comp_u5),
  ':=' (comp_10_14 = comp_adult, comp_5_9 = comp_adult, comp_u5 = comp_adult)
]

# NA values ok for now in outliered data
assertable::assert_values(survey_final_adjusted[outlier == 0], "comp_adult", test = "not_na")

# calculate deaths_adj and mx_adj
survey_final_adjusted[age_map_gbd, ':=' (age_start = i.age_start, age_end = i.age_end), on = "age_group_id"]
survey_final_adjusted[age_start >= 0 & age_end <= 5, ':=' (deaths_adj = deaths / comp_u5, mx_adj = mx / comp_u5, completeness = comp_u5)]
survey_final_adjusted[age_start >= 5 & age_end <= 10, ':=' (deaths_adj = deaths / comp_5_9, mx_adj = mx / comp_5_9, completeness = comp_5_9)]
survey_final_adjusted[age_start >= 10 & age_end <= 15, ':=' (deaths_adj = deaths / comp_10_14, mx_adj = mx / comp_10_14, completeness = comp_10_14)]
survey_final_adjusted[age_start >= 15, ':=' (deaths_adj = deaths / comp_adult, mx_adj = mx / comp_adult, completeness = comp_adult)]

# flag complete VR
survey_final_adjusted[, complete_vr := ifelse(completeness == 1, 1, 0)]
survey_final_adjusted[missing_completeness == 1, ':=' (completeness = NA, complete_vr = NA)]
assertable::assert_values(
  survey_final_adjusted[is.na(missing_completeness)],
  colnames = c("completeness", "complete_vr"),
  test = "not_na"
)
survey_final_adjusted[, missing_completeness := NULL]

# calculate mx_se_adj
survey_final_adjusted[, mx_se_adj := sqrt((ifelse(mx_adj >= 0.99, 0.99, mx_adj) * (1 - ifelse(mx_adj >= 0.99, 0.99, mx_adj))) / sample_size)]
survey_final_adjusted[mx_adj == 0, mx_se_adj := 0]

# calculate qx
survey_final_adjusted[, qx := demCore::mx_to_qx(mx, age_end - age_start)]
survey_final_adjusted[, qx_adj := demCore::mx_to_qx(mx_adj, age_end - age_start)]

# reset outliering based on measures in handoff 1
survey_final_adjusted[!outlier_note == "" & !is.na(outlier_note), outlier_note := gsub("$", "; ", outlier_note)]
survey_final_adjusted[, outlier_note := gsub("Bad old age distribution; |Improper sex ratio in older ages; |Mx >= 1; |Low qx in old age groups; ", "", outlier_note)]

# outlier based on drops in mx causing bad age distribution
country_pop <- unique(pop[age_group_id == 22 & sex_id == 3, c("location_id", "year_id", "population")])
setnames(country_pop, "population", "country_pop")
survey_final_adjusted <- merge(
  survey_final_adjusted,
  country_pop,
  by = c("location_id", "year_id"),
  all.x = TRUE
)
age_group_id_order <- c(2, 3, 388, 389, 238, 34, 6:20, 30:32, 235)
survey_final_adjusted[, age_group_id := factor(age_group_id, levels = age_group_id_order)]
survey_final_adjusted <- survey_final_adjusted[order(location_id, year_id, sex_id, age_group_id)]
survey_final_adjusted[outlier == 0, mx_adj_diff_prev := mx_adj - shift(mx_adj), by = c("location_id", "year_id", "sex_id")]
survey_final_adjusted[mx_adj_diff_prev < 0 & age_group_id %in% c(20, 30:32, 235), bad_age_dist := 1]
survey_final_adjusted <- survey_final_adjusted %>%
  group_by(location_id, year_id, sex_id) %>%
  tidyr::fill(bad_age_dist, .direction = "down") %>%
  dplyr::ungroup() %>% setDT
survey_final_adjusted[bad_age_dist == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Bad old age distribution; "))]
survey_final_adjusted[, ':=' (mx_adj_diff_prev = NULL, bad_age_dist = NULL, country_pop = NULL,
                              age_group_id = as.numeric(as.character(age_group_id)))]

# outlier mx >= 1
survey_final_adjusted[mx_adj >= 1 & !age_group_id %in% 2:3, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Mx >= 1; "))]

# outlier based on qx being too low in old ages (80-90)
survey_final_adjusted[deaths > 50 & age_group_id == 32 & sex_id == 1 & qx_adj < 0.25, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
survey_final_adjusted[deaths > 50 & age_group_id == 32 & sex_id == 2 & qx_adj < 0.20, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
survey_final_adjusted[deaths > 50 & age_group_id == 31 & sex_id == 1 & qx_adj < 0.15, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
survey_final_adjusted[deaths > 50 & age_group_id == 31 & sex_id == 2 & qx_adj < 0.10, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
survey_final_adjusted[deaths > 50 & age_group_id == 30 & sex_id == 1 & qx_adj < 0.10, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]
survey_final_adjusted[deaths > 50 & age_group_id == 30 & sex_id == 2 & qx_adj < 0.05, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Low qx in old age groups; "))]

# outlier based on improper sex ratio in older age groups
demUtils::assert_is_unique_dt(
  survey_final_adjusted,
  id_cols = c("nid", "underlying_nid", "location_id", "year_id", "age_group_id", "sex_id", "source_type_id", "age_start_orig", "age_end_orig")
)
vr_survey_sex_ratios <- dcast(
  survey_final_adjusted,
  nid + underlying_nid + location_id + year_id + age_group_id + source_type_id + age_start_orig + age_end_orig ~ sex_id,
  value.var = c("mx_adj", "deaths_adj")
)
vr_survey_sex_ratios[, sex_ratio := mx_adj_2 / mx_adj_1]
vr_survey_sex_ratios[deaths_adj_1 > 25 & deaths_adj_2 > 25 & sex_ratio > 1.25 & age_group_id %in% c(20, 30:32, 235), outlier_new := 1]
survey_final_adjusted <- merge(
  survey_final_adjusted,
  vr_survey_sex_ratios[, -c("mx_adj_1", "mx_adj_2", "deaths_adj_1", "deaths_adj_2", "sex_ratio")],
  by = c("nid", "underlying_nid", "location_id", "year_id", "age_group_id", "source_type_id", "age_start_orig", "age_end_orig"),
  all.x = TRUE
)
survey_final_adjusted[outlier_new == 1, ':=' (outlier = 1, outlier_note = paste0(outlier_note, "Improper sex ratio in older ages; "))]
survey_final_adjusted[, outlier_new := NULL]

# combine
drop <- c(
  "age_start", "age_end", "comp_u5", "comp_adult", "comp_5_9", "comp_10_14",
  "qx", "qx_adj"
)
survey_final_adjusted <- survey_final_adjusted[, -..drop]
survey_final <- rbind(
  survey_final[!source_type_name %in% c("Survey", "Census")],
  survey_final_adjusted,
  fill = TRUE
)

# fix outlier_note ending
survey_final[, outlier_note := gsub("; $", "", outlier_note)]
survey_final[outlier_note == "", outlier_note := NA]

survey_final[, ':=' (title = NULL, underlying_title = NULL)]

survey_final <- survey_final[location_id %in% locs$location_id] # subset to GBD locations

# final checks
assertable::assert_nrows(
  survey_final[!source_type_name %in% c("SIBS", "Survey", "Census") & !is.na(mx_adj)],
  target_nrows = 0
)

assertable::assert_values(
  survey_final,
  colnames = colnames(
    survey_final[,
      !c("underlying_nid", "deaths", "deaths_adj", "mx_adj", "mx_se_adj",
         "age_start_orig", "age_end_orig", "completeness", "complete_vr",
         "outlier_note", "source_type", "source_type_id", "enn_lnn_source",
         "extraction_source")
    ]
  ),
  test = "not_na"
)

# Save -------------------------------------------------------------------------

# compare mx to previous version
prev_version <- fread(
  fs::path_norm(fs::path("FILEPATH"))
)
mx_comparison <- quick_mx_compare(
  test_file = survey_final,
  compare_file = prev_version,
  id_cols = c(
    "location_id", "nid", "underlying_nid", "year_id", "sex_id", "age_group_id",
    "source_type", "source_type_id", "source_type_name", "outlier", "onemod_process_type"
  ),
  warn_only = TRUE
)
mx_comparison[locs, ihme_loc_id := i.ihme_loc_id, on = "location_id"]
readr::write_csv(
  mx_comparison,
  fs::path("FILEPATH")
)

readr::write_csv(
  survey_final,
  fs::path("FILEPATH")
)
