rm(list = ls())

# Libraries
library(data.table)
library(tidyverse)
library(dplyr)
library(haven) # for reading in dta files
library(reshape2)
library(openxlsx)

# Shared functions
source("/FILEPATH/get_ids.R")

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "/FILEPATH/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/FILEPATH/", user, "/") else if (os == "Windows") "H:/"

# Arguments/Variables (interactive is mainly for running the script individually)
if(interactive()) {
  gbd_round <- 'gbd2022'
  version <- '2022'
} else {
  args <- commandArgs(trailingOnly = TRUE)
  # print(args)
  gbd_round <- args[1]
  release_id <- args[2]
  version <- args[3]
}

# Path to files
data_path <- paste0("FILEPATH")


input_path <- paste0("FILEPATH")
output_path <- paste0("FILEPATH")
dir.create(output_path)


# Import data
compiled_nutrients <- read_csv(paste0(input_path, "compiled_nutrients_", version, "_data.csv"))
simple_sua <- read_csv(paste0(data_path, "simple_sua.csv"))


ihme_risk_conv <- openxlsx::read.xlsx(paste0(data_path, "nutrient_codes.xlsx"), sheet = 'risk')

grain_data <- read_csv("FILEPATH/whole_grain_contributors.csv") # new whole grain method
comp_long <- melt(compiled_nutrients, id.vars = c("countries", "year", "location_id")) %>%
  rename("mean" = "value", "risk" = "variable")


write_csv(comp_long, paste0(output_path, "indiv_gen_nutrients_", version, ".csv"))

# Sort grain codes and create subsets representing whole grains and rice
total_grains <- filter(grain_data, grain == 1 | whole_grain == 1 | whole_grain == 2 | whole_grain == 3) %>%
  mutate(subsets = case_when(
    whole_grain == 0 ~ "not_whole_wheat",
    whole_grain == 1 ~ "whole_wheat_all",
    whole_grain == 2 ~ "whole_wheat_part",
    whole_grain == 3 ~ "rice",
    TRUE ~ "unknown"),
  risk = "total_grains") %>%  
  dplyr::select(product_codes, grain, whole_grain, risk)

# create subset dataframes for each grain type that we want to be estimated individually
whole_grains <- filter(total_grains, whole_grain == 1) %>%
  mutate(risk = "whole_grains")

wheat <- filter(total_grains, whole_grain == 2) %>% 
  mutate(risk = "whole_wheat")

rice <- filter(total_grains, whole_grain == 3) %>% 
  mutate(risk = "whole_rice")

# recombine
grains <- rbind(total_grains, whole_grains, wheat, rice)

merged_data <- merge(simple_sua, grains, "product_codes") 
merged_data <- filter(merged_data, data != 'NA')

# This is where we apply the 18.5% conversion factor to the data column for whole_wheat
merged_data <- merged_data %>%
  mutate(data = case_when(
    risk == "whole_grains" ~ data,
    risk == "whole_wheat" ~ data * 0.185,
    risk == "whole_rice" ~ data * 0, 
    TRUE ~ data))

# Recode whole_what and whole_rice to whole_grains because we want them to be aggregated together
merged_data <- merged_data %>%
  mutate(risk = case_when(
    risk == "whole_wheat" ~ "whole_grains",
    risk == "whole_rice" ~ "whole_grains",
    TRUE ~ risk))

# Collapse data
collapsed_data <- aggregate(data ~ countries + year + location_id + risk, merged_data, sum)
collapsed_data <- collapsed_data %>%
  rename("mean" = "data")
comp_appended <- rbind(comp_long, collapsed_data)
comp_appended <- filter(comp_appended, mean != 0)
comp_appended <- comp_appended %>% 
  mutate(comp_appended, year_start = year, year_end = year)
comp_appended <- comp_appended %>%
  rename("location_name" = "countries")

# duplicate United Kingdom observations into constituent countries

ukdata <- filter(comp_appended, location_id == 95)
ukdata <- ukdata[rep(seq_len(nrow(ukdata)), each = 4), ] # creates four duplicate rows
ukdata <- rownames_to_column(ukdata) # using the new indices created by duplication

ukdata$location_name[ukdata$rowname %like% "\\.1"] <- 'Scotland'
ukdata$location_name[ukdata$rowname %like% "\\.2"] <- 'Wales'
ukdata$location_name[ukdata$rowname %like% "\\.3"] <- 'Northern Ireland'
ukdata$location_name[ukdata$location_name %in% c('United Kingdom', 'UK', 'United Kingdom of Great Britain and Northern Ireland')] <- 'England'

ukdata$location_id[ukdata$location_name == 'Scotland'] <- 434
ukdata$location_id[ukdata$location_name == 'Wales'] <- 4636
ukdata$location_id[ukdata$location_name == 'Northern Ireland'] <- 433
ukdata$location_id[ukdata$location_name == 'England'] <- 4749

ukdata <- ukdata %>% dplyr::select(-c(rowname)) # remove indices column

# Replace UK observations with the same info but for constituent countries
comp_appended <- filter(comp_appended, location_id != 95) # everything but UK countries
data <- rbind(comp_appended, ukdata)

# Adding more columns...
data$nid <- 203327 
data$svy <- "FAO_SUA_USDA_SR Nutrients"
data$data_status <- "active"
data$representative_name <- "Nationally representative only"
data$is_outlier <- 0
data$cv_natl_rep <- 1
data$cv_fao_data <- 1 # covariate stating that this is FAO data

metc <- c(20, 18, 22, 13, 16, 19, 12)
modelable_entity_id <- c(2427, 2428, 2429, 2436, 2441, 2438, 2439)
ntr <- c('calcium_mg_sum',
         'fiber_g_sum',
         'omega3_g_all_sum',
         'pufa_g_all_sum',
         'tfa_g_sum',
         'sodium_mg_sum',
         'satfat_g_sum')

metc_df <- data.frame(metc, modelable_entity_id, ntr)
get_me <- get_ids("modelable_entity") %>%
  dplyr::select(modelable_entity_id, modelable_entity_name)
metc_df <- merge(metc_df, get_me, "modelable_entity_id")

counter <- 1
for (n in metc_df$ntr) {
  data$metc[data$risk == n] <- metc_df$metc[counter] 
  counter <- counter + 1
}


counter <- 1
for (rsk in ihme_risk_conv$risk) { #ihme_risk_conv$risk
  data$ihme_risk[data$risk == rsk] <- ihme_risk_conv$risk[counter] 
  counter <- counter + 1
}

# Merge modelable_entity_name into data
data <- merge(data, dplyr::select(metc_df, -c(ntr), 'metc'), all = TRUE)

data <- data %>% 
  mutate(mean = ifelse(data$risk == 'zinc_mg_sum', mean/1000, mean),
         mean = ifelse(data$risk == 'calcium_mg_sum', mean/1000, mean),
         across(risk, str_replace, 'zinc_mg_sum', 'zinc_g_sum'),
         across(risk, str_replace, 'calcium_mg_sum', 'calcium_g_sum'))

data$me_risk <- data$ihme_risk
data$case_definition <- 'g/day'
data$case_definition[data$risk %like% "\\mg"] <- 'mg/day'
data$case_definition[data$risk %like% "\\ug_"] <- 'ug/day'
data$case_definition[data$risk == 'energy_kcal_sum'] <- 'kcal/day'
data$case_definition[data$risk == 'vit_a_iu_sum'] <- 'iu/day'

enrgy <- filter(data, risk == 'energy_kcal_sum') %>%
  dplyr::select(location_name, year, mean) %>%
  rename("energy" = "mean")

data_with_energy <- merge(data, enrgy, c('location_name', 'year')) %>% 
  mutate(energy_adj_scalar = 2000 / energy,
         total_calories = energy, #energy_adj_scalar
         mean = ifelse(ihme_risk == 'diet_pufa' | ihme_risk == 'diet_transfat' | ihme_risk == 'diet_satfat' | ihme_risk == 'diet_mufa' | ihme_risk == 'diet_fats',
                       mean * 9 / energy, mean),
         mean = ifelse(ihme_risk == 'diet_carbohydrates' | ihme_risk == 'diet_starch' | ihme_risk == 'diet_protein',
                       mean * 4 / energy, mean),
         case_definition= ifelse(ihme_risk == 'diet_pufa' | ihme_risk == 'diet_transfat' | ihme_risk == 'diet_satfat' | ihme_risk == 'diet_mufa' | ihme_risk == 'diet_fats' | ihme_risk == 'diet_carbohydrates' | ihme_risk == 'diet_starch' | ihme_risk == 'diet_protein',
                                 '% of total dietary energy', case_definition))

data_with_energy <- filter(data_with_energy, risk != 'total_carbohydrates_diff_g_sum') 

write_csv(data_with_energy, paste0(output_path, "FBS_USDA_nutrients_", version, "_data.csv"))
