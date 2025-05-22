###########################################################
### Author: USER
### Date: 3/12/2025
### Project: GBD Nonfatal Estimation
### Purpose: GBS Age and Sex Splitting
### Lasted edited: USER July 2024
###########################################################

rm(list=ls())

cause_path <- "GBS/"
cause_name <- "gbs_"
home_dir <- "FILEPATH"
date <- gsub("-", "_", Sys.Date())

# Source central functions
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# Libraries
pacman::p_load(data.table, openxlsx, ggplot2, dplyr, rlang, viridis, magrittr)

release_id_age_sex <- 16
loc <- get_location_metadata(35, release_id = release_id_age_sex)
loc <- loc[, c("location_id", "location_name", "super_region_name", "region_name")]

# Set output path and make directory if does not exist
output_path <- "FILEPATH"

################################################################################
# LOAD BUNDLE VERSION
################################################################################

bundle_version_id <- XXX
bundle_version_df <- get_bundle_version(bundle_version_id)

# Remove all group reviewed rows
bundle_version_dt <- as.data.table(bundle_version_df)
bundle_version_dt <- bundle_version_df[group_review == 1 | is.na(group_review), ]

# Remove specific data sources
bundle_version_dt <- subset(bundle_version_dt, 
                            !(field_citation_value %like% "MarketScan" | 
                                field_citation_value %like% "CMS" | 
                                field_citation_value %like% "Mongolia"))

outliers <- bundle_version_dt %>% filter(is_outlier == 1)
clinical_data <- bundle_version_dt %>% filter(clinical_version_id == 4)

# Ensure max age_end is 99
crosswalk_save$age_end <- pmin(crosswalk_save$age_end, 99)

################################################################################
# AGE & SEX SPLITTING
################################################################################

write.csv(df, paste0(home_dir, "01_", cause_name, "bundle_version_", bundle_version_id, ".csv"), row.names = FALSE)
source("FILEPATH")
split_dt <- age_sex_split(df)

write.csv(split_dt, paste0(home_dir, "02_", cause_name, "_age_sex_split", ".csv"), row.names = FALSE)

################################################################################
# SEX SPLIT
################################################################################

source("FILEPATH")

dt_sexspecific <- df %>% filter(sex != "Both" & mean >= 0) %>%
  mutate(crosswalk_parent_seq = NA, exclude_xwalk = NA)

write.csv(dt_sexspecific, file = paste0(home_dir, "gbs_sexspecific.csv"))

dt_bothsex <- split_dt %>% filter(sex == "Both")
write.csv(dt_bothsex, file = paste0(home_dir, "gbs_bothsex.csv"))

sex_split_info <- sex_split(topic_name ="gbs", 
                            output_dir = paste0(home_dir, "gbs_"), 
                            bundle_version_id = NULL, 
                            data_to_split_csv = paste0(home_dir, "gbs_bothsex.csv"), 
                            data_raw_sex_specific_csv = paste0(home_dir, "gbs_sexspecific.csv"), 
                            release_id = 16, 
                            measure = c("incidence", "prevalence"),
                            vetting_plots = TRUE )

################################################################################
# MERGE WITH WITHIN-LIT SPLIT DATA AND SAVE CROSSWALK VERSION FOR USE IN AGE SPLIT
################################################################################

# Within-literature age/sex split studies
within_lit_specific <- split_dt %>% filter(splitting == 3)
print(length(unique(within_lit_specific$nid)))

post_processed <- read.csv(paste0(home_dir, "FILEPATH"))
post_processed <- post_processed %>% filter(splitting != 3 | is.na(splitting))

# Identify any remaining overlap
issues <- post_processed %>% filter(splitting == 3)

# Bind together
post_processed$nid <- as.integer(post_processed$nid)
crosswalk_save <- bind_rows(within_lit_specific, post_processed)

# Fix common issues
crosswalk_save <- data.table(crosswalk_save)
crosswalk_save[, crosswalk_parent_seq := origin_seq]
crosswalk_save[, seq := ""]
crosswalk_save[clinical_version_id %in% c(3, 4), group_review := NA]
crosswalk_save$age_end <- pmin(crosswalk_save$age_end, 99)

################################################################################
# AGE PATTERN
################################################################################

library(openxlsx)
crosswalk_save <- read.xlsx("FILEPATH")
age_pattern <- crosswalk_save %>% filter(age_end - age_start < 25)
length(unique(age_pattern$nid))

write.xlsx(age_pattern, file = file.path(home_dir, "age_pattern.xlsx"), sheetName = "extraction", rowNames = FALSE)

bundle_version_id <- bundle_version_id
data_filepath <- "FILEPATH"
description <- "Age pattern from age-specific data <25yrs post age/sex & sex-split."
result <- save_crosswalk_version(
  bundle_version_id = bundle_version_id,
  data_filepath = data_filepath,
  description = description)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

################################################################################
# SAVE CROSSWALK VERSION TO LAUNCH IN DISMOD
################################################################################

write.xlsx(crosswalk_save, file = file.path(home_dir, "crosswalk_pre_agesplit.xlsx"), sheetName = "extraction", rowNames = FALSE)

data_filepath <- "FILEPATH"
description <- "Data post sex split and pre-age split."
result <- save_crosswalk_version(
  bundle_version_id = bundle_version_id,
  data_filepath = data_filepath,
  description = description)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

################################################################################
# AGE SPLIT CODE
################################################################################

rm(list = ls())

cause_path <- "GBS/"
cause_name <- "gbs_"
home_dir <- "FILEPATH"
date <- gsub("-", "_", Sys.Date())

library(reticulate)
reticulate::use_python("FILEPATH")
splitter <- import("pydisagg.ihme.splitter")

invisible(sapply(list.files("FILEPATH", full.names = T), source))
library(dplyr)

################################################################################
# LOAD DATASET
################################################################################

dt <- get_crosswalk_version(44246)
print(length(unique(dt$nid)))
unique(dt$splitting)

dt <- dt[age_end > 99, age_end := 99]
gbs_no_agesplit <- dt %>% filter((age_end) - (age_start) < 25)

gbs_df <- dt %>% filter((age_end) - (age_start) >= 25)
gbs_df <- gbs_df[mean != 0]
print(length(unique(gbs_df$nid)))
unique(gbs_df$splitting)

gbs_df <- gbs_df %>% mutate(sex_id = ifelse(sex == "Male", 1, 2))

################################################################################
# AGE PATTERN
################################################################################

ages <- get_age_metadata(release_id = 16)
info <- get_demographics(gbd_team = "epi", release_id = 16)
age_ids <- unique(info$age_group_id)

model_pat <- get_model_results(
  "epi",
  gbd_id = 24350,
  release_id = 16,
  year_id = 2010,
  age_group_id = age_ids,
  location_id = 1,
  sex_id = c(1, 2),
  model_version_id = 814524)

model_pat <- model_pat[, standard_error := (upper - mean) / 1.96]

gbs_age_pat <- merge(model_pat, ages, by = "age_group_id")
gbs_age_pat <- gbs_age_pat %>%
  select(mean, sex_id, age_group_years_start, age_group_years_end, age_group_id, location_id, year_id, everything()) %>%
  arrange(age_group_id)

################################################################################
# POPULATION DATA
################################################################################

gbs_pop <- get_population(
  location_id = "all",
  release_id = 16,
  sex_id = c(1, 2),
  single_year_age = FALSE,
  age_group_id = "all",
  year_id = 2010)

global_pop<-gbs_pop %>% filter(location_id==1)
# get under 5 population
pops_u5<- get_population(location_id="all", year_id=2010, release_id=16, sex_id=c(1,2), single_year_age=F, age_group_id=1)
#remove more granular age groups population 
pops_final<-gbs_pop %>% filter(!(age_group_id %in% c(2, 3,388, 389, 238, 34)))
#combine
pops_final<-rbind(pops_final, pops_u5)

################################################################################
# SUMMING UNDER 5 AGE GROUPS BY AGE WEIGHTS
################################################################################

age_1 <- copy(gbs_age_pat)
age_1 <- age_1[age_group_id %in% c(2, 3, 388, 389, 238, 34), ] #filter to the more granular under 5 age groups
se <- copy(age_1)

se <- se[age_group_id==34, .(modelable_entity_id, model_version_id, bundle_id, crosswalk_version_id, year_id, measure, measure_id, sex_id, standard_error, location_id)] ##just use standard error from 2-4 age group, previously advised by Theo)] ##just use standard error from 2-4 age group, previously advised by Theo

#if pattern = global
age_1 <- merge(age_1, global_pop, by = c("age_group_id", "sex_id", "location_id"))

age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")] #sum population across age group
age_1[, frac_pop := population / total_pop] #get proportion of population in each age group
age_1[, weight_mean := age_1$mean * frac_pop] #get weighted rates
age_1[, mean := sum(weight_mean), by = c("sex_id", "measure_id", "location_id")] #sum weighted rates
age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id")) # collapse it to the new summed weighted rates
age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, mean)] 
age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))

age_1[, age_group_id := 1] #change this to under 5 age_group_id
age_1$age_group_years_start<-0
age_1$age_group_years_end<-5

pat_all_new <- as.data.table(gbs_age_pat)
pat_all_new <- gbs_age_pat[!age_group_id %in% c(2, 3,388, 389, 238, 34)]  #remove the age groups 2,3,4,5,
pat_all_new<- rbind(pat_all_new, age_1, fill=T) #add in the new under 5 age groups
pat_all_new$age_group_weight_value<-NULL
pat_all_new <- pat_all_new[measure == "incidence"]  #remove the age groups 2,3,4,5,
#double check values
unique(pat_all_new$age_group_years_end)
#write out patterns
write.csv(pat_all_new, paste0(home_dir, "gbs_global_age_pattern.csv"))

################################################################################
# AGE SPLITTING PROCESS
################################################################################

pre_split <- gbs_df
pattern <- gbs_age_pat
pop <- gbs_pop

data_config <- splitter$AgeDataConfig(
  index = c("nid", "seq", "location_id", "sex_id", "year_start", "year_end"),
  age_lwr = "age_start",
  age_upr = "age_end",
  val = "mean",
  val_sd = "standard_error")

pattern_config <- splitter$AgePatternConfig(
  by = list("sex_id"),
  age_key = "age_group_id",
  age_lwr = "age_group_years_start",
  age_upr = "age_group_years_end",
  val = "mean",
  val_sd = "standard_error")

pop_config <- splitter$AgePopulationConfig(
  index = c("age_group_id", "location_id", "sex_id"),
  val = "population")

age_splitter <- splitter$AgeSplitter(
  data = data_config, pattern = pattern_config, population = pop_config)

result <- age_splitter$split(
  data = pre_split,
  pattern = pattern,
  population = pop,
  model = "rate",
  output_type = "rate")

################################################################################
# FINAL FORMATTING AND SAVE
################################################################################

final_result <- result
final_agesplit <- merge(gbs_df, final_result, by = 
                          c("seq", "nid", "location_id", "year_start", "year_end", "age_start", "age_end", "mean", "standard_error"),
                        all.x = TRUE)

final_agesplit <- final_agesplit %>%
  mutate(note_modeler = paste0(note_modeler, " | age split using pydisagg (model 814524 - 2010 global age pattern)"))

write.xlsx(final_agesplit, file = file.path(home_dir, "final_processed_data.xlsx"), sheetName = "extraction", rowNames = FALSE)

data_filepath <- "FILEPATH"
description <- "Data post age and sex splitting."
result <- save_crosswalk_version(
  bundle_version_id = 46233,
  data_filepath = data_filepath,
  description = description)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
