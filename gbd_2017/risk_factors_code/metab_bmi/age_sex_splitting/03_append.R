#
# prep_before_input.R
# April 2018
#
# Convert 'prep_before_input.do' from Stata to R script
#

rm(list = ls())

library(dplyr)
library(data.table)
library(tidyr)
library(haven)

j_drive <- ifelse(Sys.info()["sysname"] == "Linux", "/home/j/", "J:/")

dev <- FALSE

if (dev) {
  folder <- "FILEPATH"
} else {
  args <- commandArgs(trailingOnly = TRUE)
  folder <- args[1]
}

df_ages <- read.csv(paste0("FILEPATH")) %>%
  select(age_group_id, age_start, age_end) %>%
  filter(age_group_id %in% 5:21)

#####
# Source 1 
# Read in new compiled GBD 2016 microdata sources

df_microdata <- read.csv(paste0(folder, "/prepped_2017.csv"), as.is = TRUE) %>%
  # # TODO: I think this next line is wrong; it was in the Stata script but excluding it here
  # filter(!var %in% c("obese", "obese_rep")) %>%
  mutate(
    cv_diagnostic = ifelse(var %in% c("bmi", "overweight", "obese"), "measured", "self-report"),
    # eventually will want to accurately track urbanicity of all sources to adjust for this
    cv_urbanicity = "" ) %>%
  setnames(old = c("sample_size", "standard_error", "standard_deviation"), new = c("ss", "se", "sd")) %>%
  mutate(
    var = ifelse(var == "bmi_rep", "bmi", var),
    var = ifelse(var == "obese_rep", "obese", var),
    var = ifelse(var == "overweight_rep", "overweight", var) )
  

# convert from long to wide format
group_vars <- c(
  "nid", "survey_name", "ihme_loc_id", "year_start", "year_end", 
  "sex_id", "age_start", "age_end", "cv_diagnostic", "cv_urbanicity", "var"
)



df_microdata2 <- df_microdata %>%
  group_by(.dots = group_vars) %>%
  summarize(mean = mean(mean), ss = mean(ss), se = mean(se), sd = mean(sd) ) %>%
  as.data.frame(.)

df_list <- lapply(c("mean", "ss", "se", "sd"), function(x) {
  tmp <- df_microdata2[, c(group_vars, x)] %>%
    spread(key = "var", value = x) %>%
    # setnames(old = c("bmi", "overweight"), new = paste0(c("bmi_", "overweight_"), x))
    setnames(old = c("bmi", "obese", "overweight"), new = paste0(c("bmi_", "obese_", "overweight_"), x))
  return(tmp)
})

df_microdata3 <- Reduce(function(...) merge(..., all=T), df_list) # 1717 observations; matches Stata version


#####
# Source 2
# Read in new GBD 2016 report tabulations and literature sources (all compiled together)

df_tab_in <- read.csv(
  file = paste0("FILEPATH"),
  as.is = TRUE
)

df_tab <- df_tab_in %>%
  # drop if group_review is zero; these are sources taht report data in multiple ways
  # only want to keep one data point from each source
  mutate(
    sex_id = as.numeric(NA),
    sex_id = ifelse(sex == "Male", 1, sex_id),
    sex_id = ifelse(sex == "Female", 2, sex_id),
    sex_id = ifelse(sex == "Both", 3, sex_id),
    me_name = tolower(me_name) ) %>%
  filter(me_name %in% c("bmi", "overweight", "obese")) %>%
  filter(group_review %in% c(1, NA)) %>%
  mutate(
    # create self-report indicator
    cv_diagnostic = ifelse(height_weight_measured == 1, "measured", "self-report") ) %>%
  # generate mean if only cases and sample size
  mutate(mean = ifelse(is.na(mean), (cases / sample_size), mean) ) %>%
  # drop if there is still no mean reported
  filter(!is.na(mean)) %>%
  # sort out uncertainty
  mutate(
    sd = ifelse(uncert_type == "SD", error, as.numeric(NA)),
    se = ifelse(uncert_type == "SE", error, as.numeric(NA)),
    se = ifelse(uncert_type == "95 CI", ((upper - lower) / 3.92), se) ) %>%
  setnames(old = "sample_size", new = "ss") %>%
  select(
    nid, ihme_loc_id, year_start, year_end, age_start, age_end, 
    me_name, mean, ss, sd, se, sex_id, cv_diagnostic, smaller_site_unit ) %>%
  mutate(cv_urbanicity = "")
  
group_vars2 <- c(
  "me_name", "nid", "ihme_loc_id", "year_start", "year_end", "sex_id", "age_start", "age_end", 
  "cv_diagnostic", "cv_urbanicity", "smaller_site_unit"
)

df_tab2 <- df_tab %>%
  group_by(.dots = group_vars2) %>%
  summarize(mean = mean(mean), ss = mean(ss), se = mean(se), sd = mean(sd) )

df_list <- lapply(c("mean", "ss", "se", "sd"), function(x) {
  tmp <- df_tab2[, c(group_vars2, x)] %>%
    spread(key = "me_name", value = x) %>%
    setnames(old = c("bmi", "obese", "overweight"), new = paste0(c("bmi_", "obese_", "overweight_"), x))
  return(tmp)
})

df_tab3 <- Reduce(function(...) merge(..., all=T), df_list) # 1580 observations; matches Stata

  
#####
# Combine microdata and tabulated data

df <- bind_rows(list(df_microdata3, df_tab3)) # 3297 observations; matches Stata

# Make a list of nid-location indicators so that we know which data to keep from previous GBD extractions
df_nids <- df %>%
  select(nid, ihme_loc_id) %>%
  filter(!duplicated(.)) %>%
  mutate(flag = 1)

#####
# Read in GBD 2015 input data
dat_path_2015 <- paste0("FILEPATH")

df_2015_in <- haven::read_dta(dat_path_2015, encoding = "iso-8859-1") %>%
  as.data.frame(.) # 91,098 observations; matches Stata version

# -- convert variables from atomic to the appropriate format
vars_integer <- c(
  "nid", "year_start", "year_end", "sex_id", "age_start", "age_end", "smaller_site_unit", "is_adult")
vars_char <- c(
  "ihme_loc_id", "cv_urbanicity", "cv_diagnostic", "extraction_type", "study_name", "ntr2")
vars_num <- c(
  "bmi_mean", "obese_mean", "overweight_mean", 
  "bmi_se", "obese_se", "overweight_se",
  "bmi_ss", "obese_ss", "overweight_ss", 
  "bmi_sd", "obese_sd", "overweight_sd"
)

# check which variables are all NA, for exclusion
# -- 'bh' excluded as well because the only value is an empty string
# lapply(df_2015_in, function(x) all(is.na(x)))

for (j in vars_integer) df_2015_in[, j] <- as.integer(df_2015_in[, j])
for (j in vars_char) df_2015_in[, j] <- as.character(df_2015_in[, j])
for (j in vars_num) df_2015_in[, j] <- as.numeric(df_2015_in[, j])

keep_vars <- names(df_2015_in)[names(df_2015_in) %in% c(vars_integer, vars_char, vars_num)]

df_2015 <- df_2015_in[, keep_vars] %>%
  left_join(df_nids, by = c("nid", "ihme_loc_id")) %>%
  filter(is.na(flag)) %>% # exclude NID/ihme_loc_id combinations that appear in the GBD 2017 data
  select(-flag) # 87831 observations; matches Stata


df2 <- bind_rows(df, df_2015) # 91128 observations; matches Stata

# check implausible values
with(df2, table(overweight_mean > 0.98 & !is.na(overweight_mean) & ihme_loc_id != "KWT" & ihme_loc_id != "TON")) # 115
with(df2, table(overweight_mean < 0.01)) # 904

# do some cleaning of implausible values
df3 <- df2 %>%
  # this deletes 907 observations in Stata
  filter(overweight_mean >= 0.01 | is.na(overweight_mean)) %>%
  # this deletes 115 observations in Stata
  filter(!(overweight_mean > 0.98 & !is.na(overweight_mean) & ihme_loc_id != "KWT" & ihme_loc_id != "TON"))

# 90108; Stata version is 90,106
# 

saveRDS(df3, paste0(folder, "/wide_full.RDS"))


# Save various datasets that are needed for modeling
for (i in c("bmi", "overweight", "obese")) {
  # i <- "bmi" # dev
  tmp_keep_vars <- c(
    "nid", "year_start", "year_end", "ihme_loc_id", "sex_id", "cv_urbanicity", "cv_diagnostic", 
    paste0(i, "_mean"),  paste0(i, "_se"),  paste0(i, "_ss"), paste0(i, "_sd"), "age_start", "age_end"
  )
  tmp <- df3[, tmp_keep_vars]
  
  tmp2 <- tmp %>%
    filter(!is.na(get(paste0(i, "_mean"))))
  
  saveRDS(tmp2, paste0(folder, paste0("/wide_full_", i, ".RDS")))
  
}

# Save gold standard datasets
#   (standard age groups, sex-specific, nationally representative, national level, sufficient sample size)

df4 <- df3 %>%
  mutate(
    age_start = ifelse(age_end == 4, 1, age_start),
    age_end = ifelse(age_start >= 80, 123, age_end) ) %>%
  left_join(df_ages, by = c("age_start", "age_end")) %>%
  filter(sex_id != 3) %>%
  filter(!grepl("_", ihme_loc_id)) %>%  
  # filter(smaller_site_unit != 1) %>%  # 'smaller_site_unit' exclusion commented out in Stata script
  filter(!(is.na(bmi_mean) & bmi_ss < 20))

saveRDS(df4, paste0(folder, "/wide_gs_microdata.RDS"))


for (i in c("bmi", "overweight", "obese")) {
  # i <- "bmi" # dev
  
  tmp_keep_vars <- c(
    "nid", "year_start", "year_end", "ihme_loc_id", "sex_id", "cv_urbanicity", "cv_diagnostic",  
    paste0(i, "_mean"), paste0(i, "_se"), paste0(i, "_ss"), paste0(i, "_sd"), "age_start", "age_end"
  )
  tmp <- df4[, tmp_keep_vars] %>%
    filter(!is.na(get(paste0(i, "_mean")))) %>%
    left_join(df_ages, by = c("age_start", "age_end")) %>%
    filter(!is.na(age_group_id)) %>% # exclude non-standard age groups (see comment before the for-loop)
    select(-age_group_id)
  
  saveRDS(tmp, paste0(folder, "/wide_gs_", i, ".RDS"))
}

