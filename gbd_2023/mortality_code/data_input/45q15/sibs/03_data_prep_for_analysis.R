## 	Adult Mortality through Sibling Histories: #2. Cleaning data to ready for analysis

rm(list = ls())
library(haven)
library(glue)
library(readr)
library(data.table)
library(plyr)

user <- "USERNAME"
if (Sys.info()[1] == "Windows") {
  root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  root <- "FILEPATH"
  h_root <- "FILEPATH"
}
gbd_year <- 2023
source_list <- glue("FILEPATH/source_list.csv") |> fread()

sib_dir <- "FILEPATH"

# Read in surveys including appended missing siblings
input <- fs::path(sib_dir, "FILEPATH/allsibhistories_with_missing_sibs.csv") |>
  fread()

# simplify location groups
survid <- input[, .N, by = c("location_name", "surveyyear")]
survid[, `:=`(id = .GRP, N = NULL)]
with_survid <- merge(input, survid, by = c("location_name", "surveyyear"))

# Coerce rand, yod columns to numeric
with_survid <- with_survid[, `:=`(
  rand = as.numeric(rand),
  yod = as.integer(yod)
)]

# create weights and dummies for sex
with_survid[, `:=`(
  male = ifelse(sex == 1, 1, 0), female = ifelse(sex == 2, 1, 0),
  death = ifelse(is.na(yod), 0, 1), aged = ifelse(yod - yob < 0, NA, yod - yob)
)]

# generate 5 year age groups for age at death
with_survid[, agedcat := ifelse((aged %/% 5) * 5 < 75, (aged %/% 5) * 5, 75)]
with_survid[between(yod - yob, 1, 4) & !is.na(yod), agedcat := 1]

# recode non-standard sexes
with_survid[!sex %in% c(1, 2), sex := NA]

# 5 year age blocks for age at time of interview
with_survid[, age := yr_interview - yob]
with_survid[, ageblock := (age %/% 5) * 5]

# Respondents older than 50 are set to the 45-49 age group
with_survid[ageblock == 50 & id_sm == 0, ageblock := 45]

# Compute sex distribution of alive sibs by age and survey ----------------

with_survid[, `:=`(males = sum(male, na.rm = T), females = sum(female, na.rm = T)),
  by = c("id", "ageblock")
]
with_survid <- with_survid[order(id, ageblock, males, females)]
with_survid[is.na(males), males := max(males), by = c("id", "ageblock")]
with_survid[is.na(females), females := max(females), by = c("id", "ageblock")]
with_survid[, pctmale := males / (males + females), by = c("id", "ageblock")]
# For 0 deaths, split 50/50
with_survid[is.na(pctmale), pctmale := 0.5]

# Randomly assigns sex to unknowns --------------------------------------

## based on sex distribution of age group and survey
set.seed(5)
with_survid[, rnd := runif(.N)]
with_survid[, sex := as.integer(sex)]
with_survid[is.na(sex) & death == 0,
  sex := ifelse(rnd <= pctmale, 1L, 2L),
  by = c("id", "ageblock")
]

with_survid[, c("males", "females", "rnd", "pctmale") := NULL]

# Compute sex distribution by age for dead sibs (pools surveys) --------

# For dead sibs of unknown sex, redistribute according to sex distribution of dead sibs
# within age group of death, pooling over all surveys (not enough deaths to do within survey)
d_redist <- copy(with_survid)

d_redist[death == 1, `:=`(males = sum(male, na.rm = T), females = sum(female, na.rm = T)),
  by = "agedcat"
]
d_redist <- d_redist[order(males, females, agedcat)]
d_redist[is.na(males), males := max(males), by = "agedcat"]
d_redist[is.na(females), males := max(females), by = "agedcat"]

d_redist[, pctmale := males / (males + females), by = "agedcat"]

# Randomly assigns sex to unknowns (dead sibs) --------------------------

d_redist[, rnd := runif(.N)]
d_redist[, sex := as.integer(sex)]
d_redist[
  death == 1 & is.na(sex),
  sex := ifelse(rnd <= pctmale, 1L, 0L)
]

d_redist[, c(
  "males", "females", "rnd", "pctmale", "male", "female",
  "id", "death", "aged", "agedcat", "age", "ageblock"
) := NULL]

# drop siblings where alive/dead status unknown
unknown <- d_redist[!alive %in% c(0, 1)]
for (u_svy in unique(unknown$svy)) {
  output_dir <- source_list[svy == u_svy, output_dir]
  unknown[svy == u_svy] |>
    fwrite(glue::glue("{output_dir}/unknown_living_status.csv"))
}
d_redist <- d_redist[alive == 0 | alive == 1]

# List survey swith sample size ------------------------------------------

sibhistlist <- d_redist[, .N, by = c("iso3", "surveyyear", "svy")]
sibhistlist[, surveyyear := surveyyear + 1]

# check column types
sapply(sibhistlist, class)

# write outputs
## full surveys
fwrite(sibhistlist, fs::path("FILEPATH/allsibs_sibhistlist.csv"))
fwrite(d_redist, fs::path("FILEPATH/allsibs_surveys.csv"))

## loc specific
for (input_svy in unique(d_redist$svy)) {
  print(input_svy)
  d_redist_svy <- d_redist[svy == input_svy]
  output_dir <- source_list[svy == input_svy, output_dir]
  fwrite(d_redist_svy, glue::glue("{output_dir}/{input_svy}_surveys.csv"))
}
