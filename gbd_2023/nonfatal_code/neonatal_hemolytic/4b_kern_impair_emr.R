################################################################################
## Purpose: Pull all cause mortality rate and convert to excess mortality rate
## Input:   All cause mortality envelope;
##          cerebral palsy literature data
## Output:  Excess mortality rate to be appended to Bundle 458
################################################################################

library(data.table)
library(dplyr)

# pull age group metadata ------------------------------------------------------
age_group_metadata <-
  ihme::get_age_metadata(release_id = nch::id_for("release", "GBD 2023"))

# pull standard mortality ratio ------------------------------------------------

# TEMP save smr flat file as dismod shaped bundle
smr <- data.table::fread("FILEPATH")
smr_hemo <- smr %>%
  filter(acause == "neonatal_hemolytic")

# preparing age_group_metadata df for merging
age_group_metadata <- age_group_metadata %>%
  mutate(age_start = case_when(
    age_group_years_end <= 20 ~ 0,
    age_group_years_start >= 20 ~ 20
  ))

# merge with age_group_metadata
smr_hemo <- smr_hemo %>%
  filter(sex == 1) #use one of the sex since smr is same for both
smr_hemo <- merge(
  smr_hemo,
  age_group_metadata,
  by = "age_start",
  all = TRUE
)

# keep necessary rows and rename values to smr values
smr_hemo <- smr_hemo %>%
  select(
    mean,
    std,
    lower,
    upper,
    age_group_id,
    age_group_name,
    age_group_years_start,
    age_group_years_end,
    smr = mean,
    smr_std = std,
    smr_lower = lower,
    smr_upper = upper
  )

# pull all cause mortality -----------------------------------------------------
demographics <- ihme::get_demographics(
  gbd_team = "epi",
  release_id = nch::id_for("release", "GBD 2023"),
)

mort_rate <- ihme::get_life_table(
  with_shock = 1,
  with_hiv = 1,
  life_table_parameter_id = 1,
  location_id = demographics$location_id,
  release_id = nch::id_for("release", "GBD 2023"),
  year_id = demographics$year_id,
  age_group_id = demographics$age_group_id,
  sex_id = demographics$sex_id
)

# calculate excess mortality rate ----------------------------------------------
emr <- merge(
  mort_rate,
  smr_hemo,
  by = "age_group_id",
  all = TRUE
)

emr <- emr %>%
  mutate(
    emr = mean * (smr - 1),
    emr_lower = mean * (smr_lower - 1),
    emr_upper = mean * (smr_upper - 1)
  )

# save as csv ------------------------------------------------------------------
fwrite(
  emr,
  "/FILEPATH"
)
