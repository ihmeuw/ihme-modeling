##########################################################################
### Author: USER
### Date: 03/12/2025
### Project: GBD Nonfatal Estimation
### Purpose: MS data prep 2023
##########################################################################

rm(list=ls())

cause_path <- "MS/"
cause_name <- "ms_"
home_dir <- "FILEPATH"
date <- gsub("-", "_", Sys.Date())

# Source central functions
invisible(sapply(list.files("FILEPATH", full.names = TRUE), source))

# Load libraries
pacman::p_load(data.table, openxlsx, ggplot2, dplyr, rlang, viridis, magrittr)

release_id_age_sex <- 16
loc <- get_location_metadata(35, release_id = release_id_age_sex)
loc <- loc[, c("location_id", "location_name", "super_region_name", "region_name")]
output_path <- home_dir

################################################################################
# LOAD BUNDLE VERSION
################################################################################

source("FILEPATH")
bundle_version_id <- 49169
bundle_version_df <- get_bundle_version(bundle_version_id)

# Remove all group-reviewed rows
bundle_version_dt <- as.data.table(bundle_version_df)
bundle_version_dt <- bundle_version_dt[group_review == 1 | is.na(group_review), ]

outliers <- bundle_version_dt %>% filter(is_outlier == 1)
clinical_data <- bundle_version_dt %>% filter(clinical_version_id == 4)
unique(clinical_data$field_citation_value)

# Exclude select data sources
bundle_version_dt <- subset(
  bundle_version_dt,
  !(
    field_citation_value %like% "Russian Federation" |
      field_citation_value %like% "MarketScan Claims and Medicare Data - 2000" |
      field_citation_value %like% "Health Insurance Review and Assessment Service" |
      field_citation_value %like% "Mongolia"
  )
)

# Mark CMS data as outlier
bundle_version_dt <- bundle_version_dt %>%
  mutate(is_outlier = ifelse(field_citation_value %like% "CMS", 1, is_outlier))

# Ensure max age_end is 99
bundle_version_dt$age_end <- pmin(bundle_version_dt$age_end, 99)

df <- bundle_version_dt %>%
  filter(splitting == "" | splitting %in% c(0, 1, 2, 3, "1,2"))

################################################################################
# 1. WITHIN LITERATURE AGE-SEX SPLIT
################################################################################
df$cases <- as.numeric(df$cases)
df$mean <- as.numeric(df$mean)

# Adjust within-lit age/sex split for rows with large age bins
df$specificity[df$nid == 165691 & df$sex == "Both"] <- "age"
df$specificity[df$nid == 221263 & df$measure == "incidence" & df$sex == "Both"] <- "age"
df$specificity[df$nid == 489707 & df$age_start == 65] <- "age"
df$specificity[df$nid == 490064 & df$age_start == 75] <- "age"
df$is_outlier[df$nid == 490344 & df$mean == 0.0000143 & df$lower == 0.0000077] <- 1
df$specificity[df$nid == 490474 & df$age_start == 65] <- "age"
df$specificity[df$nid == 490659 & df$age_start == 70] <- "age"
df$specificity[df$nid == 498332 & df$age_start == 70] <- "age"
df$specificity[df$nid == 498719 & df$age_start == 65] <- "age"

# Separate data
df1 <- df %>% filter(splitting == 3)
df2 <- df %>% filter(splitting != 3 | is.na(splitting) | splitting == "")

write.csv(df, paste0(home_dir, "01_", cause_name, "bundle_version_", bundle_version_id, ".csv"), row.names = FALSE)

source("FILEPATH")
apply_split_dt <- age_sex_split(df1)
split_dt <- bind_rows(apply_split_dt, df2)

write.csv(split_dt, paste0(home_dir, "02_", cause_name, "_age_sex_split", ".csv"), row.names = FALSE)

library(stringr)
unique_nids <- split_dt %>%
  filter(str_detect(note_modeler, "age,sex split using sex ratio")) %>%
  dplyr::select(nid) %>%
  distinct()

print("Unique NIDs with the specified pattern in note_modeler:")
print(unique_nids)

issues <- split_dt %>% filter(splitting == 3, sex == "Both")

################################################################################
# SEX SPLIT
################################################################################
source("FILEPATH")

dt_sexprocessed <- df %>% filter(flag_processed == 1)

dt_sexspecific <- df %>%
  filter(
    sex != "Both",
    mean >= 0,
    (is.na(flag_processed) | flag_processed != 1)
  ) %>%
  mutate(
    crosswalk_parent_seq = NA,
    exclude_xwalk = NA
  )

write.csv(dt_sexspecific, file = paste0(home_dir, cause_name, "sexspecific.csv"))

dt_bothsex <- split_dt %>% filter(sex == "Both")
write.csv(dt_bothsex, file = paste0(home_dir, cause_name, "bothsex.csv"))

sex_split(
  topic_name = cause_name,
  output_dir = paste0(home_dir),
  bundle_version_id = NULL,
  data_all_csv = NULL,
  data_to_split_csv = paste0(home_dir, cause_name, "bothsex.csv"),
  data_raw_sex_specific_csv = paste0(home_dir, cause_name, "sexspecific.csv"),
  nids_to_drop = c(),
  cv_drop = c(),
  mrbrt_model = NULL,
  mrbrt_model_age_cv = FALSE,
  release_id = 16,
  measure = c("incidence", "prevalence"),
  vetting_plots = TRUE
)

library(ggforce)

####### MERGE WITH WITHIN-LIT SPLIT DATA AND SAVE CROSSWALK VERSION
within_lit_specific <- split_dt %>%
  filter(str_detect(note_modeler, "age,sex split using sex ratio"))

within_lit_specific_and_processed <- bind_rows(within_lit_specific, dt_sexprocessed)

post_processed <- read.csv("FILEPATH")
post_processed <- post_processed %>% filter(splitting != 3 | is.na(splitting))

crosswalk_save <- bind_rows(within_lit_specific_and_processed, post_processed)
crosswalk_save$origin_seq <- as.integer(crosswalk_save$origin_seq)

crosswalk_save <- data.table(crosswalk_save)
crosswalk_save[, crosswalk_parent_seq := origin_seq]
crosswalk_save[, seq := ""]

crosswalk_save[clinical_version_id %in% c(4), group_review := NA]

filtered_rows <- crosswalk_save %>% filter(group_review == 1 & is.na(group))

crosswalk_save <- crosswalk_save %>% filter(sex %in% c("Male", "Female"))

issues_sexsplit <- crosswalk_save %>% filter(sex == "Both")

crosswalk_save <- crosswalk_save %>%
  mutate(is_outlier = if_else(location_name %in% c("Sicilia", "Molise") & measure == "incidence", 1, is_outlier))

# Save crosswalk
write.xlsx(
  crosswalk_save,
  file = file.path(home_dir, "xwalk_for_at_migration_11.25.24.xlsx"),
  sheetName = "extraction",
  rowNames = FALSE
)

bundle_version_id <- bundle_version_id
data_filepath <- "FILEPATH"
description <- "11/25/24. Bundle v48947 post age/sex & sex-split for upload to AT-bundle."
result <- save_crosswalk_version(
  bundle_version_id = bundle_version_id,
  data_filepath = data_filepath,
  description = description
)

print(sprintf("Request status: %s", result$request_status))
print(sprintf("Request ID: %s", result$request_id))
print(sprintf("Crosswalk version ID: %s", result$crosswalk_version_id))


################################################################################
# SEX SPLIT
################################################################################

library(reticulate)
reticulate::use_python("FILEPATH")
splitter <- import("pydisagg.ihme.splitter")

invisible(sapply(list.files("FILEPATH", full.names = T), source))

### LOAD THE DATASET
#if continuing from datasets created by pipeline:
dt <- copy(crosswalk_save)
#if continuing from pre-age split xwalk version:
#dt <- get_crosswalk_version(45423)
#print(length(unique(dt$nid)))
#unique(dt$splitting)

library(dplyr)

#### DATASET WHICH DOESN'T NEED SPLITTING - WILL BE MERGED BACK AFTER
ms_no_agesplit <- dt %>%
  filter((age_end - age_start < 25) | measure %in% c("mtstandard", "mtwith"))

#### 1. DATASET WHICH NEEDS SPLITTING
ms_df <- dt %>%
  filter((age_end - age_start) >= 25 & (measure == "prevalence" | measure == "incidence"))
ms_df <- ms_df[age_end > 99, age_end := 99]
ms_df <- ms_df[age_start < 5, age_start := 5]

ms_df <- ms_df %>% filter(mean != 0)
print(length(unique(ms_df$nid)))
unique(ms_df$splitting)

#round age groups
#ms_df[, age_start := age_start - age_start %%5]
#ms_df[, age_end := age_end - age_end %%5 + 4]

# add in sex_id column to help with AgeSplitter function
ms_df <- ms_df %>%
  mutate(sex_id = ifelse(sex == "Male", 1, 2))

#### 2. POPULATION OF INTEREST
ms_pop <- get_population(
  location_id = "all", 
  release_id = 16, 
  sex_id = c(1,2), 
  single_year_age = FALSE, 
  age_group_id = c(6:32, 235),
  year_id = 2010
)

#### 3. AGE PATTERN FOR MEASURE IN QUESTION
ages <- get_age_metadata(release_id = 16)
ages_ready_for_merge <- ages[, c("age_group_years_start", "age_group_years_end", "age_group_id")]

model_pat <- get_model_results(
  "epi",
  gbd_id = 24349,
  release_id = 16,
  year_id = 2010,
  age_group_id = c(6:32, 235),
  measure = c(5,6),
  location_id = 1,
  sex_id = c(1,2),
  model_version_id = 880345
)

model_pat <- model_pat[, standard_error := (upper - lower) / 3.92]

#draws <- get_draws(
#  gbd_id_type = "modelable_entity_id", 
#  source = "epi",
#  gbd_id = 24349,
#  release_id = 16,
#  measure_id = c(5),
#  year_id = 2010,
#  age_group_id = c(6:32, 235),
#  location_id = 1,
#  sex_id = c(1,2)
#)

# Merge the datasets based on the common key
ms_age_pat <- merge(model_pat, ages_ready_for_merge, by = "age_group_id")
#ms_age_pat <- merge(draws, ages_ready_for_merge, by = "age_group_id")

# Rearrange vars to make it easier to see
ms_age_pat <- ms_age_pat %>%
  select(
    sex_id, age_group_years_start, age_group_years_end, age_group_id, 
    location_id, year_id, everything()
  ) %>%
  arrange(age_group_id)

#### 4. LOAD IN DATASETS
pre_split <- ms_df
pattern <- ms_age_pat
pop <- ms_pop

data_config <- splitter$AgeDataConfig(
  index = c("nid","origin_seq", "location_id", "sex_id", "year_start", "year_end", "measure"),
  age_lwr = "age_start",
  age_upr = "age_end",
  val = "mean",
  val_sd = "standard_error"
)

# IF USING DRAWS
#draw_cols <- grep("^draw_", names(pattern), value = TRUE)

pattern_config <- splitter$AgePatternConfig(
  by = list("sex_id", "measure"),
  age_key = "age_group_id",
  age_lwr = "age_group_years_start",
  age_upr = "age_group_years_end",
  val = "mean",
  val_sd = "standard_error"
)

# REMOVED YEAR_ID FROM INDEX TO GET IT TO WORK
pop_config <- splitter$AgePopulationConfig(
  index = c("age_group_id", "location_id", "sex_id"),
  val = "population"
)

age_splitter <- splitter$AgeSplitter(
  data = data_config, 
  pattern = pattern_config, 
  population = pop_config
)

result <- age_splitter$split(
  data = pre_split,
  pattern = pattern,
  population = pop,
  model = "rate",
  output_type = "rate"
)

print(length(unique(result$nid)))
print(length(unique(ms_df$nid )))

library(openxlsx)
file_path <- file.path(home_dir, "agesplit_data.xlsx")
write.xlsx(result, file = file_path)

###### NOW INCIDENCE

#### 1. DATASET WHICH NEEDS SPLITTING
ms_df_inc <- dt %>%
  filter((age_end - age_start) >= 25 & (measure == "incidence"))
ms_df_inc <- ms_df_inc[age_end > 99, age_end := 99]
ms_df_inc <- ms_df_inc[age_start < 5, age_start := 5]

ms_df_inc <- ms_df_inc[mean != 0]
print(length(unique(ms_df_inc$nid))) #58 NIDs
unique(ms_df_inc$splitting)

#round age groups
#ms_df[, age_start := age_start - age_start %%5]
#ms_df[, age_end := age_end - age_end %%5 + 4]

# add in sex_id column to help with AgeSplitter function
ms_df_inc <- ms_df_inc %>%
  mutate(sex_id = ifelse(sex == "Male", 1, 2))

#### 2. POPULATION OF INTEREST
ms_pop <- get_population(
  location_id = "all", 
  release_id = 16, 
  sex_id = c(1,2), 
  single_year_age = FALSE, 
  age_group_id = c(6:32, 235),
  year_id = 2010
)

#### 3. AGE PATTERN FOR MEASURE IN QUESTION
ages <- get_age_metadata(release_id = 16)
ages_ready_for_merge <- ages[, c("age_group_years_start", "age_group_years_end", "age_group_id")]

model_pat_inc <- get_model_results(
  "epi",
  gbd_id = 24349,
  release_id = 16,
  year_id = 2010,
  age_group_id = c(6:32, 235),
  measure = c(6),
  location_id = 1,
  sex_id = c(1,2),
  model_version_id = 842648
)

model_pat_inc <- model_pat[, standard_error := (upper - lower)/3.92]

#draws <- get_draws(
#  gbd_id_type = "modelable_entity_id", 
#  source = "epi",
#  gbd_id = 24349,
#  release_id = 16,
#  measure_id = c(5),
#  year_id = 2010,
#  age_group_id = c(6:32, 235),
#  location_id = 1,
#  sex_id = c(1,2)
#)

# Merge the datasets based on the common key
ms_age_pat <- merge(model_pat_inc, ages_ready_for_merge, by = "age_group_id")
#ms_age_pat <- merge(draws, ages_ready_for_merge, by = "age_group_id")

# Rearrange vars to make it easier to see
ms_age_pat <- ms_age_pat %>%
  select(
    sex_id, age_group_years_start, age_group_years_end, age_group_id, 
    location_id, year_id, everything()
  ) %>%
  arrange(age_group_id)

#### 4. LOAD IN DATASETS
pre_split <- ms_df_inc
pattern <- ms_age_pat
pop <- ms_pop

data_config <- splitter$AgeDataConfig(
  index = c("nid","origin_seq", "location_id", "sex_id", "year_start", "year_end"),
  age_lwr = "age_start",
  age_upr = "age_end",
  val = "mean",
  val_sd = "standard_error"
)

pattern_config <- splitter$AgePatternConfig(
  by = list("sex_id"),
  age_key = "age_group_id",
  age_lwr = "age_group_years_start",
  age_upr = "age_group_years_end",
  val = "mean",
  val_sd = "standard_error"
)

pop_config <- splitter$AgePopulationConfig(
  index = c("age_group_id", "location_id", "sex_id"),
  val = "population"
)

age_splitter <- splitter$AgeSplitter(
  data = data_config, 
  pattern = pattern_config, 
  population = pop_config
)

result_inc <- age_splitter$split(
  data = pre_split,
  pattern = pattern,
  population = pop,
  model = "rate",
  output_type = "rate"
)

print(length(unique(result_inc$nid)))
print(length(unique(ms_df_inc$nid ))) #58 NIDs

combined_result <- rbind(result, result_inc)
print(length(unique(combined_result$nid))) #131

#############################
final_result <- result

ms_df <- dt %>%
  filter((age_end - age_start) >= 25 & (measure == "incidence" | measure == "prevalence"))
ms_df <- ms_df[age_end > 99, age_end := 99]
ms_df <- ms_df[age_start < 5, age_start := 5]

ms_df <- ms_df[mean != 0]
print(length(unique(ms_df$nid)))
unique(ms_df$splitting)

ms_df <- ms_df %>%
  mutate(sex_id = ifelse(sex == "Male", 1, 2))

final_agesplit <- merge(
  ms_df, final_result, 
  by = c("origin_seq", "nid", "location_id", "measure", "year_start", "year_end", "age_start", "age_end", "mean", "standard_error"),
  all.x = TRUE
)

print(length(unique(final_agesplit$nid)))
library(data.table)

validation_check <- final_agesplit %>% filter(is.na(age_start))

# Rename the columns to preserve their original values
setnames(final_agesplit, "age_start", "age_start_presplit")
setnames(final_agesplit, "age_end", "age_end_presplit")
setnames(final_agesplit, "mean", "mean_presplit")
setnames(final_agesplit, "standard_error", "se_presplit")
setnames(final_agesplit, "upper", "upper_presplit")
setnames(final_agesplit, "lower", "lower_presplit")

set(final_agesplit, j = c("upper", "lower", "uncertainty_type_value"), value = NA)

# Copy values from specified columns to new columns
final_agesplit <- as.data.table(final_agesplit)
final_agesplit[, age_start := pat_age_group_years_start]
final_agesplit[, age_end := pat_age_group_years_end]
final_agesplit[, mean := age_split_result]
final_agesplit[, standard_error := age_split_result_se]

final_agesplit[age_start > 1, age_end := age_end - 1]
final_agesplit <- final_agesplit[age_end > 99, age_end := 99]

message <- "| age split using pydisagg (model v880345, global age pattern using 2010 data)"
final_agesplit[, note_modeler := paste0(note_modeler, message)]

final_agesplit <- final_agesplit %>%
  select(
    measure, nid, seq, origin_seq, location_name, location_id,
    year_start, year_end, sex, age_start, age_end, mean,
    standard_error, everything()
  ) %>%
  arrange(final_agesplit)

crosswalk_save <- bind_rows(ms_no_agesplit, final_agesplit)

validation <- crosswalk_withnewcsmr %>% filter(is.na(age_start))

crosswalk_withnewcsmr <- data.table(crosswalk_withnewcsmr)
crosswalk_withnewcsmr[, crosswalk_parent_seq := origin_seq]
crosswalk_withnewcsmr[, seq := ""]

library(openxlsx)

# Write flat file
write.xlsx(crosswalk_withnewcsmr, file = file.path(home_dir, "final_processed_data.xlsx"),
           sheetName = "extraction", rowNames = FALSE)

csmr <- read.xlsx("FILEPATH")

crosswalk_withnewcsmr <- bind_rows(crosswalk_save, old_csmr)

crosswalk_withcsmr <- as.data.table(crosswalk_withcsmr)
crosswalk_withcsmr$specificity[crosswalk_withcsmr$nid == 562835] <- "age,sex"
crosswalk_withcsmr$group[crosswalk_withcsmr$nid == 562835] <- 1
crosswalk_withcsmr[is.na(group_review), group_review := 1]
crosswalk_withcsmr[, specificity := "age,sex"]
crosswalk_withcsmr[, group := 1]

crosswalk_withcsmr <- data.table(crosswalk_withcsmr)
crosswalk_withcsmr[, crosswalk_parent_seq := origin_seq]
crosswalk_withcsmr[, seq := ""]

validation_check <- crosswalk_withnewcsmr %>% filter(is.na(measure))

# Write flat file
write.xlsx(crosswalk_withnewcsmr, file = file.path(home_dir, "final_processed_with_csmr.xlsx"),
           sheetName = "extraction", rowNames = FALSE)

check <- crosswalk_withnewcsmr %>% filter(nid == 488744)

bundle_version_id <- bundle_version_id
data_filepath <- "FILEPATH"
description <- "DESCRIPTION HERE"
result <- save_crosswalk_version(
  bundle_version_id = bundle_version_id,
  data_filepath = data_filepath,
  description = description
)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))