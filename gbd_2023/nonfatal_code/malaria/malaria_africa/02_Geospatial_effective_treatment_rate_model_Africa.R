# The Effective coverage of Antimalarial is a product of Three (multiple) but distinct models and stages

#1. The proportional Usage Model
#2. The AM Efficacy Model 

######################
### AM Usage
######################
rm(list=ls())


####STEP 1
#run data preparation and covariate cleaning steps first
# source("FILEPATH/Covariates_preparation_cleaning.R")

#This list is for all packages needed to be used here but their current order may induce some conflicts (hence commented out sections of packages are still left in their initial positions)
library(data.table)
library(deldir)
library(devtools)
library(dplyr)
library(fields)
library(geoR)
library(ggforce)
library(ggplot2)
library(ggpubr)
library(gridExtra)
library(gstat)
library(haven)
library(imputeTS)
library(INLA)
library(janitor)
library(lattice)
library(lemon)
library(maptools)
library(maps)
library(mclogit)
library(mlogit)
library(mvtnorm)
library(parallel)
library(patchwork)
library(readr)
library(readxl)
library(reshape2)
library(rgeos)
library(sf)
library(sp)
library(spdep)
library(terra)
library(tictoc)
library(tidyr)
library(tidyverse)
library(viridis)

setwd("FILEPATH")
# Define the directory where your files are located
directory <- "FILEPATH"

# List all WHO AM distribution Excel files in the directory
all_files <- list.files(path = directory, pattern = "\\.xlsx$", full.names = TRUE)

# Filter files based on the pattern you provided
target_files <- grep("AMdistribution\\d{4}-\\d{4}_OutsideAfrica.xlsx", all_files, value = TRUE, ignore.case = TRUE)

# Read Excel files into a list of data frames
data_list <- lapply(target_files, read_excel)

# Extract sheet names from file names
sheet_names <- gsub(".*/(AMdistribution\\d{4}-\\d{4})_OutsideAfrica.xlsx", "\\1", target_files)

# ###Create a function to standardize country names based on your existing logic:
#source("FILEPATH/name_standardization_function.R")

# Combine data frames with corresponding sheet names
combined_data <- Map(function(df, sheet) {
  df %>%
    rename(location_name = Country) %>%
    standardize_country_names() %>%
    rename_with(~ gsub("coursesrcvd_(\\w+)", "\\1", .x), starts_with("coursesrcvd_")) %>%
    rename(courses_1st = `1stline`, courses_act=act) %>%
    mutate(unid_st = paste(location_name, Year, sep = "")) %>%
    arrange(unid_st)
}, data_list, sheet_names)

# Save individual data frames as CSV files
for (i in seq_along(combined_data)) {
  file_name <- paste0("am", i, ".csv")
  file_path <- file.path(directory, file_name)
  write.csv(combined_data[[i]], file_path, row.names = FALSE)
}

# Combine all data frames into one and standardize names
all_data <- bind_rows(combined_data) %>%  
  standardize_country_names() %>% 
  mutate(unid_st = paste(location_name, Year, sep = ""))

# Optionally, save the combined data frame as a CSV file or any other format
combined_file_path <- file.path(directory, paste0("AMDdistribution_OAfrica",lubridate::year(Sys.Date()),".csv"))

#save it
write.csv(all_data, combined_file_path, row.names = FALSE)

#Merge with Africa Data and Incidence Data
africa_data <- read_excel("FILEPATH/AMdistribution2000-2019_Africa.xlsx", sheet = "data", col_names = TRUE) %>%
  rename(courses_1st = coursesrcvd_1stline, courses_act = coursesrcvd_act, Year=year) %>%
  rename(location_name = country) %>%
  standardize_country_names()

combined_data <- bind_rows(all_data, africa_data) %>%
  mutate(unid_st = ifelse(is.na(unid_st), paste(location_name, Year, sep = ""), unid_st)) %>% 
  dplyr::select(c(location_name,	Year,	courses_1st,	courses_act,	unid_st)) %>% 
  distinct() %>% #remove all duplicates
  arrange(unid_st) %>% 
  ungroup() %>% 
  group_by(unid_st) %>%
  summarise(
    location_name = first(location_name),
    Year = first(Year),
    courses_1st = max(courses_1st, na.rm = TRUE), # Use max to prioritize non-NA values
    courses_act = max(courses_act, na.rm = TRUE)  # Use max to prioritize non-NA values
  ) %>%
  ungroup() %>% 
  mutate(
    courses_1st = ifelse(is.infinite(courses_1st), NA, courses_1st),
    courses_act = ifelse(is.infinite(courses_act), NA, courses_act)
  )

write.csv(combined_data, paste0(directory,"amdistribution_global", lubridate::year(Sys.Date()), ".csv"), row.names = FALSE)

#########################
###Prepare Incidence Data
incidenc_path <- "FILEPATH/Pf_incidence_mean.csv"
incidence <- read.csv(incidenc_path) %>% 
  filter(ihme_age_group_id =="All_Ages", year > 1990,admin_unit_level =="ADMIN0") %>% 
  rename("location_name"=country_name ) %>% 
  dplyr::select(-c(admin_unit_name, admin_unit_level, ihme_location_id, ihme_age_group_id)) %>% 
  standardize_country_names() %>%
  mutate(unid_st = paste(location_name, year, sep = "")) %>%
  arrange(unid_st)%>%
  standardize_country_names()

##create a continent column for every country

# Initialize the continent column with NA values
data <- incidence %>%
  mutate(continent = NA_character_)

# Replace country names with continent names
incidence <- data %>%
  mutate(continent = case_when(
    location_name == "Afghanistan" ~ "Asia",
    location_name == "Algeria" ~ "Africa",
    location_name == "Angola" ~ "Africa",
    location_name == "Argentina" ~ "South America",
    location_name == "Armenia" ~ "Asia",
    location_name == "Azerbaijan" ~ "Asia",
    location_name == "Bangladesh" ~ "Asia",
    location_name == "Belize" ~ "Central America",
    location_name == "Benin" ~ "Africa",
    location_name == "Bhutan" ~ "Asia",
    location_name == "Bolivia" ~ "South America",
    location_name == "Botswana" ~ "Africa",
    location_name == "Brazil" ~ "South America",
    location_name == "Burkina Faso" ~ "Africa",
    location_name == "Burundi" ~ "Africa",
    location_name == "Cambodia" ~ "Asia",
    location_name == "Cameroon" ~ "Africa",
    location_name == "Cape Verde" ~ "Africa",
    location_name == "Central African Republic" ~ "Africa",
    location_name == "Chad" ~ "Africa",
    location_name == "China" ~ "Asia",
    location_name == "Colombia" ~ "South America",
    location_name %in% c("Congo", "Republic of Congo", "Republic Of Congo") ~ "Africa",
    location_name %in% c("Côte d'Ivoire", "Cote d'Ivoire") ~ "Africa",
    location_name %in% c("Democratic Republic of the Congo", "Democratic Republic Of The Congo") ~ "Africa",
    location_name == "Djibouti" ~ "Africa",
    location_name == "Dominican Republic" ~ "Central America",
    location_name == "Ecuador" ~ "South America",
    location_name == "Egypt" ~ "Africa",
    location_name == "El Salvador" ~ "Central America",
    location_name == "Equatorial Guinea" ~ "Africa",
    location_name == "Eritrea" ~ "Africa",
    location_name == "Ethiopia" ~ "Africa",
    location_name == "French Guiana" ~ "South America",
    location_name == "Gabon" ~ "Africa",
    location_name == "Gambia" ~ "Africa",
    location_name == "Georgia" ~ "Asia",
    location_name == "Ghana" ~ "Africa",
    location_name == "Guatemala" ~ "Central America",
    location_name == "Guinea" ~ "Africa",
    location_name == "Guinea-Bissau" ~ "Africa",
    location_name == "Guyana" ~ "South America",
    location_name == "Haiti" ~ "Central America",
    location_name == "Honduras" ~ "Central America",
    location_name == "India" ~ "Asia",
    location_name == "Indonesia" ~ "Asia",
    location_name == "Iran" ~ "Asia",
    location_name == "Iraq" ~ "Asia",
    location_name == "Kenya" ~ "Africa",
    location_name == "Kyrgyzstan" ~ "Asia",
    location_name == "Lao People's Democratic Republic" ~ "Asia",
    location_name == "Laos" ~ "Asia",
    location_name == "Liberia" ~ "Africa",
    location_name == "Madagascar" ~ "Africa",
    location_name == "Malawi" ~ "Africa",
    location_name == "Malaysia" ~ "Asia",
    location_name == "Maldives" ~ "Asia",
    location_name == "Mali" ~ "Africa",
    location_name == "Mauritania" ~ "Africa",
    location_name == "Mayotte" ~ "Africa",
    location_name == "Mexico" ~ "Central America",
    location_name == "Morocco" ~ "Africa",
    location_name == "Mozambique" ~ "Africa",
    location_name == "Myanmar" ~ "Asia",
    location_name == "Namibia" ~ "Africa",
    location_name == "Nepal" ~ "Asia",
    location_name == "Nicaragua" ~ "Central America",
    location_name == "Niger" ~ "Africa",
    location_name == "Nigeria" ~ "Africa",
    location_name %in% c("North Korea", "Democratic People's Republic of Korea") ~ "Asia",
    location_name == "Oman" ~ "Asia",
    location_name == "Pakistan" ~ "Asia",
    location_name == "Panama" ~ "Central America",
    location_name == "Papua New Guinea" ~ "Oceania",
    location_name == "Paraguay" ~ "South America",
    location_name == "Peru" ~ "South America",
    location_name == "Philippines" ~ "Asia",
    location_name %in% c("Sao Tome and Principe", "Sao Tome And Principe") ~ "Africa",
    location_name == "Saudi Arabia" ~ "Asia",
    location_name == "Senegal" ~ "Africa",
    location_name == "Sierra Leone" ~ "Africa",
    location_name == "Solomon Islands" ~ "Oceania",
    location_name == "Somalia" ~ "Africa",
    location_name == "South Africa" ~ "Africa",
    location_name == "South Korea" ~ "Asia",
    location_name == "Sri Lanka" ~ "Asia",
    location_name == "Sudan" ~ "Africa",
    location_name == "Suriname" ~ "South America",
    location_name == "Swaziland" ~ "Africa",
    location_name == "Eswatini" ~ "Africa",
    location_name %in% c("Syria", "Syrian Arab Republic") ~ "Asia",
    location_name == "Tajikistan" ~ "Asia",
    location_name %in% c("Timor-Leste", "East Timor") ~ "Oceania",
    location_name == "Tanzania" ~ "Africa",
    location_name == "Thailand" ~ "Asia",
    location_name == "Togo" ~ "Africa",
    location_name == "Turkey" ~ "Europe",
    location_name == "Turkmenistan" ~ "Asia",
    location_name == "Uganda" ~ "Africa",
    location_name == "United Arab Emirates" ~ "Asia",
    location_name == "Uzbekistan" ~ "Asia",
    location_name == "Vanuatu" ~ "Oceania",
    location_name == "Venezuela" ~ "South America",
    location_name == "Vietnam" ~ "Asia",
    location_name == "Viet Nam" ~ "Asia",
    location_name == "Yemen" ~ "Asia",
    location_name == "Zambia" ~ "Africa",
    location_name == "Zanzibar" ~ "Africa",
    location_name == "Zimbabwe" ~ "Africa",
    location_name == "Rwanda" ~ "Africa",
    location_name == "Comoros" ~ "Africa",### newly added after checking
    location_name == "Costa Rica" ~ "Central America",
    location_name == "South Sudan" ~ "Africa",
    TRUE ~ continent), # Keep the existing value if no match is found
    unid_st = paste(location_name, year, sep = ""))



incidence %>% 
  filter(is.na(continent)) %>% View()

write.csv(incidence, paste0("pfincidence_global", lubridate::year(Sys.Date()), ".csv"), row.names = FALSE)

##Read in the file with information on "country", "continent", "Firstline drug name" and year
policy_issue<- read_dta("FILEPATH/antimalarial_policystatus_allyears.dta")

#################################create new column for latter years###########

# Create a sequence of the new years to add
new_years <- c(2023, 2024)

# Filter the data to get the rows from the last available year (2022)
last_year_data_no_year <- policy_issue %>% filter(year == 2022) %>% select(-year)

# Create new rows for each new year
extended_data <- tidyr::expand_grid(last_year_data_no_year, year = new_years) %>%
  mutate(unid_st = paste(location_name, year, sep = "")) %>% 
  bind_rows(policy_issue) %>%
  arrange(location_name, year)

#################################################################
### Final Merging and Cleaning -if not in the env, then use code below, else just use "combined_data" which is in the env already
final_data <- extended_data %>%
  left_join(combined_data) %>%
  left_join(incidence) %>%
  mutate(junk1 = ifelse(courses_1st < courses_act & !is.na(courses_1st) & !is.na(courses_act), 1, 0),
         courses_1st = ifelse(junk1 == 1, courses_act, courses_1st),
         courses_act = ifelse(junk1 == 1, courses_1st, courses_act),
         year = as.numeric(as.character(year))
         # firstline2 = ifelse(firstline==0, "NonACT", "ACT")
  ) %>%
  select(-c(iso2, LCI_incidence_rate, median_incidence_rate, UCI_incidence_rate,junk1))

##add missing iso3 for French Guiana
final_data$iso3[final_data$location_name=="French Guiana"] <- "GUF"

# List of countries and years for replacement or addition from to the new data since they have all NA in the WHO report
countries <- c("Egypt", "French Guiana", "Maldives", "Mayotte", "Morocco", "Oman", "Syria", "United Arab Emirates", "Turkmenistan", "North Korea")
years <- 2019:2024

# Manual data filling function
manual_fill <- function(df, countries, years) {
  df %>%
    mutate(courses_1st = ifelse(location_name %in% countries & year %in% years & is.na(courses_1st), 0, courses_1st),
           courses_act = ifelse(location_name %in% countries & year %in% years & is.na(courses_act), 0, courses_act))
}

final_data <- manual_fill(final_data, countries, years)

# View the first few rows of the final data to verify the changes
print(head(final_data))

##check for any duplicates
final_data %>% group_by(unid_st) %>% summarise(n=n()) %>%  filter(n>1)%>% View()

write.csv(final_data, paste0(directory,"amdistribution+policy+incidence", lubridate::year(Sys.Date()), ".csv"), row.names = FALSE)

####Imputation in R
# Convert courses_1st and courses_act to numeric, handling non-numeric values appropriately
final_data_am_dist <- final_data %>%
  mutate(
    courses_1st = as.numeric(as.character(courses_1st)),
    courses_act = as.numeric(as.character(courses_act))) %>%
  arrange(unid_st) %>% 
  group_by(location_name) %>%
  mutate(
    courses_1st = na_interpolation(courses_1st, option = "linear"),
    courses_act = na_interpolation(courses_act, option = "linear")) %>%
  ungroup() %>%
  group_by(location_name) %>%
  mutate(
    courses_1st = ifelse(is.na(courses_1st) & year == max(year), lag(courses_1st), courses_1st),
    courses_act = ifelse(is.na(courses_act) & year == max(year), lag(courses_act), courses_act)) %>%
  ungroup()

final_data_am_dist$courses_act[final_data_am_dist$is_act == 0] <- 0  # to declare the period where ACT was not 
# final_data$courses_act[final_data$is_asct == 0] <- NA  # could use NA to declare the period where ACT was not 

write.csv(final_data, paste0(directory,"amdistribution+policy+incidence_imputed", lubridate::year(Sys.Date()), ".csv"), row.names = FALSE)

plot_folder <- "FILEPATH"

########################################POPULATION  - GBD2023 - Already in the file Merge_AMDistribution####################################################

## This "amdistribution_global.dta" data is manually processed- and separately extracted WMR tables, cleaned up and arranged as in the code above. 
data.am <- final_data %>%
  rename(firstline2 = firstline) %>% 
  mutate(firstline = ifelse(firstline2==0, "NonACT", "ACT"))

country.year.list <- data.am %>% select(location_name, year)

## populationa at risk
pop_path <- "FILEPATH/ihme_populations.csv"
popn <- read.csv(pop_path) %>% 
  filter(year > 1990 & ihme_admin_unit_level== "ADMIN0" & age_bin == "All_Ages" & sex == 3 ) %>%
  rename(location_name = country_name) %>%
  mutate(location_name = fct_recode(location_name, "Timor-Leste" = "East Timor","Eswatini" = "Swaziland"))  %>%
  group_by(location_name, year) %>% 
  select(iso3, location_name, who_region, year,pop_at_risk_pf) %>% 
  right_join(country.year.list,  by = c("location_name", "year"))

## Adding missing ##Zanzibar countries 
levels(popn$iso3)<-c(levels(popn$iso3),"EAZ")
popn$iso3[which(popn$location_name == "Zanzibar" & is.na(popn$iso3))] <- "EAZ"

popn$who_region[which(popn$location_name == "Zanzibar" & is.na(popn$who_region))] <- "AFRO"

## Zanzibar popn - preojected from census data of 2022 with rate of 3.7% [https://www.nbs.go.tz/nbs/takwimu/Census2022/Administrative_units_Population_Distribution_Report_Tanzania_Zanzibar_volume1c.pdf]
# Existing population data from 1991 to 2022, last year of census is 2022 =1889773
# znz.popn <- c(
#   694327, 716540, 739463, 763120, 787534, 812728, 838729, 865561, 893252, 921829, 
#   951320, 981754, 1012188, 1043566, 1075917, 1109270, 1143658, 1179111, 1215663, 
#   1253349, 1292203, 1303569, 1362189, 1420810, 1479430, 1538051, 1596671, 1655291, 
#   1713912, 1772532, 1831153, 1889773, 1925524, 1961274)#, 2025==1997025

popn$pop_at_risk_pf[which(popn$location_name == "Zanzibar" & is.na(popn$pop_at_risk_pf))] <- znz.popn
popn$logpopn <- log(popn$pop_at_risk_pf)

## treatment seeking -Use most latest Ts estimates
ts_path <- "FILEPATH/TS_predictions_GAMkNN.csv"
ts <- read.csv(ts_path) %>% 
  filter(Year > 1990 & Admin_Unit_Level== "ADMIN0") %>%
  rename(iso3 = ISO3,location_name = Country_Name, year = Year, trtseek = Any_pred_high) %>% 
  mutate(location_name = fct_recode(location_name, "Timor-Leste" = "East Timor"))  %>%
  group_by(location_name, year) %>% 
  select(iso3, location_name, year,trtseek)#who_region,

## Adding missing countries
ts.zanzibar <- ts %>% filter(location_name == "Tanzania") %>% 
  mutate(location_name = fct_recode(location_name, "Zanzibar" = "Tanzania"), 
         iso3 = fct_recode(iso3, "EAZ" = "TZA"))

## Full data
ts <- bind_rows(ts,ts.zanzibar) %>% 
  right_join(country.year.list, by = c("location_name", "year")) 

## merge/join "amdistribution_africa" + populationa at risk + ts.afr.any
options(scipen = 999)

# ## replace all imputed ACT courses to NA if the "firstline" AM was not ACT
 
data.am.pop <- data.am %>%
  group_by(location_name, year) %>%
  select(location_name, year, courses_1st, courses_act, unid_st, is_act, is_cq, is_sp, is_other, firstline, firstline2, continent, mean_incidence_rate) %>%
  left_join(popn, by = c("location_name", "year")) %>%
  left_join(ts, by = c("location_name", "year", "iso3")) %>%
  group_by(location_name) %>%
  mutate(impcourse1st.l = na_interpolation(courses_1st, option = "linear"),
         impcourseact.l = na_interpolation(courses_act, option = "linear"),
         impcourseact.l = ifelse(is_act == 0, 0, impcourseact.l),  # Apply the condition hereis_act == 0, NA,
         courses_nact = impcourse1st.l - impcourseact.l,
         incidence_popn = mean_incidence_rate * pop_at_risk_pf,
         pop_hmis = incidence_popn * trtseek, #pop_hmis
         acthmis_pc = impcourse1st.l / pop_hmis,
         acthmis_pc = ifelse(is.infinite(acthmis_pc), 0, acthmis_pc), #acthmis_pc
         actpop_pc = impcourse1st.l / incidence_popn,
         actpop_pc = ifelse(is.infinite(actpop_pc), 0, actpop_pc))#actpop_pc

data.am.pop$ptrt_act.hmis = data.am.pop$impcourseact.l/data.am.pop$pop_hmis
data.am.pop$ptrt_act.hmis = ifelse(is.infinite(data.am.pop$ptrt_act.hmis), 0, data.am.pop$ptrt_act.hmis)

data.am.pop$ptrt_act.popn = data.am.pop$impcourseact.l/data.am.pop$incidence_popn
data.am.pop$ptrt_act.popn = ifelse(is.infinite(data.am.pop$ptrt_act.popn), 0, data.am.pop$ptrt_act.popn) 

## replace 0 where is is NaN due to zero over zero (for the 1st line)
data.am.pop$acthmis_pc = ifelse(is.infinite(data.am.pop$acthmis_pc), 0, data.am.pop$acthmis_pc) 
data.am.pop$acthmis_pc = ifelse(is.na(data.am.pop$acthmis_pc), 0, data.am.pop$acthmis_pc) 

data.am.pop$actpop_pc = ifelse(is.infinite(data.am.pop$actpop_pc), 0, data.am.pop$actpop_pc) 
data.am.pop$actpop_pc = ifelse(is.na(data.am.pop$actpop_pc), 0, data.am.pop$actpop_pc) 

write.csv(data.am.pop, paste0(directory,"amdistribution_formodeling", lubridate::year(Sys.Date()), ".csv"), row.names = FALSE)

# Plots
ggplot(data = popn) +
  geom_point(aes(x=year, y=pop_at_risk_pf),color="black") +
  labs(title = "pop_at_risk_pf") +
  facet_wrap(~ location_name, scales = "free_y")  + 
  theme_bw() + 
  theme(legend.position = "top", legend.key.width = unit(1.5, "cm"), 
        legend.key.height = unit(0.2, "cm"))
ggsave(file.path(plot_folder, "Population_at_Risk.png"),
       width = 12, height = 8, dpi = 600)

# TS
ggplot(data = data.am.pop) +
  geom_point(aes(x=year, y=trtseek),color="black") +
  labs(title = "Treatment Seeking") +
  facet_wrap(~ location_name)  + 
  theme_bw() + 
  theme(legend.position = "top", legend.key.width = unit(1.5, "cm"), 
        legend.key.height = unit(0.2, "cm"))
ggsave(file.path(plot_folder, "TreatmentSeeking_global.png"),
       width = 12, height = 8, dpi = 600)

# AM availability per All (popn-based) incidence
ggplot(data = data.am.pop) +
  geom_point(aes(x=year, y=ptrt_act.popn,color="ACT")) +
  geom_line(aes(x=year, y = actpop_pc,color="1st Line")) + 
  labs(title = "Courses per Population-based Incidence \n >1 = Surplus ; <1 = Shortage ",color = "AM availability \n Population") +
  facet_wrap(~ location_name, scales = "free_y")  
ggsave(file.path(plot_folder, "am_availability_popn_global.png"),
       width = 12, height = 8, dpi = 600)

# AM availability per HMIS incidence
ggplot(data = data.am.pop) +
  geom_point(aes(x=year, y=ptrt_act.hmis,color="ACT")) +
  geom_line(aes(x=year, y = acthmis_pc,color="1st Line")) + 
  labs(title = "Courses per HMIS Incidence \n >1 = Surplus ; <1 = Shortage ",color = "AM availability \n HMIS") +
  facet_wrap(~ location_name, scales = "free_y")  
ggsave(file.path(plot_folder, "am_availability_hmis_global.png"),
       width = 12, height = 8, dpi = 600)

# Pamoja
ggplot(data = data.am.pop) +
  geom_point(aes(x=year, y=ptrt_act.popn,color="ACT-popn")) +
  geom_point(aes(x=year, y=ptrt_act.hmis,color="ACT-hmis")) +
  geom_line(aes(x=year, y = actpop_pc,color="1stLine-popn")) + 
  geom_line(aes(x=year, y = acthmis_pc,color="1stLine-hmis")) + 
  labs(title = "Courses per Incidence \n >1 = Surplus ; <1 = Shortage ",color = "AM availability \n Population and HMIS") +
  facet_wrap(~ location_name, scales = "free_y")  
ggsave(file.path(plot_folder, "am_availability_popn+hmis_global.png"),
       width = 12, height = 8, dpi = 600)

######this code prepare the config data for easy and accurate joins later ################
##extract uids from the last amusage file
country_id <- read.csv("FILEPATH/amusage_covariates.csv") %>% 
  select(uid,	admin_level, location_name) %>% 
  distinct()

# Organize National Config Data
national_config_data <- read_csv("FILEPATH/National_Config_Data.csv") %>%
  janitor::clean_names() %>% 
  select(map_country_name:who_subregion) %>%
  rename(location_name = map_country_name) %>% 
  standardize_country_names() %>% 
  left_join(country_id) %>% 
  arrange(location_name) 

zanzibar_data <- national_config_data %>%
  filter(location_name == "Tanzania") %>%
  mutate(location_name = "Zanzibar",
         iso3 = "EAZ",
         iso2 = "",
         gaul_code = NA,
         geometry_id = NA) %>% 
  bind_rows(national_config_data) %>% 
  arrange(location_name)

#################organizing policy
# Import Excel data for year ACT adopted
act_adopted_config_combined <- read_excel(paste0(directory,"year_ACT_adopted.xlsx"), sheet = "yearACTadopted") %>% 
  select(-c(...3, ...4)) %>%
  rename(location_name = Country) %>% 
  arrange(location_name) %>% 
  mutate(actyears20 = 2022 - yadopted) %>% 
  mutate(actyears20 = ifelse(yadopted== 0, 0, actyears20)) %>% 
  standardize_country_names() %>% # # Source the script for standardizing country names and applying filters
  right_join(national_config_data)

# Save the dataframe to a .dta file
write.csv(act_adopted_config_combined, paste0(directory,"National_Config_Data_local.csv"))

##########################################preparing IHME Covariates#############################################
#Read in the covariates files from IHME
IHME_cov_path <- "FILEPATH/am_covariates.RData"

load(IHME_cov_path)

# Define the original and new column names for all variables
original_colnames <- c("ANC1_coverage_prop", "ANC4_coverage_prop", "DTP3_coverage_prop", "hospital_beds_per1000", "IFD_coverage_prop",
                       "LDI_pc", "measles_vacc_cov_prop", "SBA_coverage_prop", "GDPpc_id_b2010", "prop_urban",
                       "he_cap", "frac_oop_hexp", "universal_health_coverage", "haqi", "measles_vacc_cov_prop_2",
                       "ind_health", "education_all_ages_and_sexes_pc", "log_the_pc", "MCI",
                       "education_yrs_pc", "educ_yrs_age_std_pc", "underweight_all_age", "malaria_incidence_map")

new_colnames <- c("anc1cov", "anc4cov", "dpt3cov", "hospbeds", "ifdcov",
                  "ldipc", "measlescov1", "sbacov", "gdppc", "propurban",
                  "hecap", "oopfrac", "univcover", "haqi", "measlescov2",
                  "indvhealth", "eduallagsx", "logthepc", "hcaccess", #note that mci is renamed to hcaccess
                  "educyrspc", "eduagestdpc", "underweight_all_age", "malincid")

# Create a named vector for renaming all variables
rename_vector <- setNames(new_colnames, original_colnames)

ihme_covs<- bind_rows(am_covs)

# Process all variables together
combined_ihme_covs_cleaning <- ihme_covs %>%
  filter(
    (sex_id == 3 & age_group_name == "All Ages") | covariate_name_short %in% original_colnames[20:23], #deals with both simple and complex covariates
    year_id > 1990 & year_id < 2024,
    !(location_id %in% c(35, 213, 44796, 4867) & location_name %in% c("Georgia", "Niger", "Nairobi", "Punjab"))) %>%
  group_by(covariate_name_short, location_id, location_name, year_id) %>%
  summarise(mean_value = mean(mean_value, na.rm = TRUE)) %>%
  ungroup() %>%
  pivot_wider(names_from = covariate_name_short, values_from = mean_value) %>%
  rename_with(~ rename_vector[.x], all_of(original_colnames)) %>%
  janitor::clean_names() %>%
  standardize_country_names() %>% 
  mutate(unid_st = paste(location_name, year_id, sep = ""))

df <- combined_ihme_covs_cleaning

# Add missing countries
# Function to add missing countries
add_country <- function(df, original, new) {
  df_new <- df %>% filter(location_name == original)
  df_new <- df_new %>% mutate(location_name = new)
  df <- bind_rows(df, df_new)
  df <- df %>% mutate(unid_st = ifelse(location_name == new, paste(location_name, year_id, sep = ""), unid_st))
  return(df)}

# Add Zanzibar using Tanzania data
# df <- add_country(df, "United Republic of Tanzania", "Zanzibar")
df <- add_country(df, "Tanzania", "Zanzibar")

# Add Mayotte using Comoros data
df <- add_country(df, "Comoros", "Mayotte")

# Add French Guiana using Suriname data
df <- add_country(df, "Suriname", "French Guiana")

combined_ihme_covs_cleaning <- df

final_data <- combined_ihme_covs_cleaning %>%
  left_join(ts, by = c("location_name", "year_id"="year")) %>% 
  left_join(final_data_am_dist %>% dplyr::select(-iso3,-firstline,-Year),  by = c("location_name", "year_id"="year", "unid_st")) %>% 
  left_join(data.am.pop %>% dplyr::select(-c(courses_1st, courses_act, is_act, is_cq, is_sp, is_other, firstline2, continent))) %>% 
  left_join(act_adopted_config_combined)

##final covariates table
write_csv(final_data, paste0("FILEPATH/covariates_", lubridate::year(Sys.Date()),".csv"))
# write_csv(final_data, paste0("FILEPATH/covariates_", lubridate::year(Sys.Date()),".csv"))

#STEP 2:
##Running and visualising raw AM Proportinal Usage
# #run Proportional_usage model

# Prepare the Input Data

# Initial manipulation and cleaning
DHS_surveys_path <- "FILEPATH/antimalarial_summary_any_symptom.csv"

summ.table <- read.csv(DHS_surveys_path) %>%
  filter(year > 2000) %>% # Excluded surveys before WHO Global Policy for ACT (2001)
  filter(!country %in% c("Albania", "Algeria","Barbados","Belarus","Bosnia and Herzegovina","Cuba", "Fiji","Kazakhstan",
                         "Jordan","Jamaica", "Kiribati","Kosovo under UNSC res. 1244","Lebanon","Lesotho",
                         "Moldova", "Moldova, Republic of","Montenegro","North Macedonia, Republic of","Panama","
                               Qatar","Saint Lucia","Samoa","State of Palestine", "Syrian Arab Republic","Tonga",
                         "Trinidad and Tobago","Tunisia", "Turks and Caicos Islands", "Tuvalu" ,"Serbia" 
  )) %>% ## Remove countries we dont model
  dplyr::select(-c(url)) %>% 
  rename("location_name" = "country",
         "treated_total" = "n_antimalarial_total",
         "treated_act" = "n_artemisinin_total") %>% 
  filter(asked_artemisinin != "") %>%  # not asked about use of specific ACT
  filter(survey_subset == "") %>%  # done in specific sub regions - not representative
  mutate(location_name = recode(location_name, "Congo, Democratic Republic of the" = "Democratic Republic Of The Congo",
                                "Congo Democratic Republic" = "Democratic Republic Of The Congo",
                                "Congo" = "Republic Of Congo",
                                "CÃ´te d'Ivoire" = "Cote d'Ivoire",
                                "Côte d'Ivoire" = "Cote d'Ivoire",
                                "Kyrgyz Republic" = "Kyrgyzstan",
                                "Lao People's Democratic Republic" = "Laos",
                                "South Sudan, Republic of" = "South Sudan",
                                "Sao Tome and Principe" = "Sao Tome And Principe",
                                "Viet Nam" = "Vietnam" )) %>% 
  mutate(treated_nonact =treated_total - treated_act,
         prop_act = treated_act/treated_total) 

# Burundi 2016 : Disclaimer in the report - ASAQ confused with AQ
# Adjust the ratio to something meaningful -here asssumed the trend between 2010 and 2012 continued
summ.table$treated_nonact[summ.table$location_name == "Burundi" & summ.table$year == 2016] = (summ.table$treated_total[summ.table$location_name == "Burundi" & summ.table$year == 2016] - summ.table$treated_act[summ.table$location_name == "Burundi" & summ.table$year == 2016])

summ.table <- summ.table %>% 
  mutate(treated_total = treated_nonact+treated_act, 
         prop_act = treated_act/treated_total ) 

# Drop  survey when Total treated  == 0
# Dominican Republic
summ.table <- summ.table %>% 
  filter(!(location_name == "Dominican Republic" & year == 2007 & surveytype == "SPE"))

# Tajikistan
summ.table <- summ.table %>% 
  filter(!(location_name == "Tajikistan" & year == 2017 & surveytype == "DHS"))


# Drop one survey when multiple available within the same year - note DHS tends to be more consistent
ggplot(summ.table, aes(y=prop_act, x=year))+
  geom_line()+
  geom_point(aes(y=prop_act, x=year, col = surveytype)) +
  facet_wrap(~location_name)

ggsave("FILEPATH/sanity_check.png", width = 13, height = 8.5 )

# Dominican Republic 2013 (Conducted DHS and SPE) 
# We retain the DHS
summ.table <- summ.table %>% 
  filter(!(location_name == "Dominican Republic" & year == 2013 & surveytype == "SPE"))

# Mali 2015 (Conducted DHS and MICS) 
# We retain the DHS
summ.table <- summ.table %>% 
  filter(!(location_name == "Mali" & year == 2015 & surveytype == "MICS"))

# Senegal 2020 
# Numbers for Dakar looks extremely small, map showing some regions not surveyed
summ.table <- summ.table %>% 
  filter(!(location_name == "Senegal" & year == 2020 & surveytype == "MIS"))

# Togo 2017 (Conducted MIS and MICS) 
# We retain the MIS (MIS are better quality)
summ.table <- summ.table %>% 
  filter(!(location_name == "Togo" & year == 2017 & surveytype == "MICS"))

# Nigeria 2021 (Conducted MIS and MICS) 
# We retain the MIS (MIS are better quality)
summ.table <- summ.table %>% 
  filter(!(location_name == "Nigeria" & year == 2021 & surveytype == "MICS"))

# Drop a survey when multiple available within the same year but not similar estimates - note DHS tends to be more consistent
#Visualise
ggplot(summ.table, aes(y=prop_act, x=year))+
  geom_line()+
  # geom_point(aes(y=prop_act, x=year, col = surveytype)) +
  geom_point(aes(y=prop_act, x=year, col = surveytype)) +
  facet_wrap(~location_name)

ggsave("FILEPATH/sanity_checking.png", width = 13, height = 8.5 )

write.csv(summ.table,
          "FILEPATH/summarytable_fromsurveys_allparameters.csv")


# Model Input Dataset - with selected parameters/variables
summ.table <- summ.table %>%  
  dplyr::select(c(location_name, year, surveytype,treated_nonact,treated_total,treated_act,prop_act))  
#filter(treated_total != 0) %>% 

# save the input data file
write.csv(summ.table,
          "FILEPATH/amusage_binomial+surveytype.csv")


## Plotting 1 -Prior to any wrangling and further cleaning

ObservedData_vis <- "FILEPATH"
dir.create(ObservedData_vis, showWarnings=FALSE, recursive = TRUE) # folder to save plots

c.list <- unique(summ.table$location_name)

pdf("FILEPATH/countryyear.actusage_mse_raw.pdf")

for(i in 1:length(c.list)){
  country <- c.list[i]
  
  data.sub<- summ.table %>% filter(location_name == country )
  ymin <- min(data.sub$year)
  ymax <- max(data.sub$year)
  
  plot1<-data.sub %>% 
    filter(location_name == {{country}}) %>% 
    ggplot(aes(x=year,y=prop_act), show.legend = F) +
    geom_line()+
    geom_point(aes(x=year,y=prop_act, col = surveytype)) +
    geom_text(aes(label=paste0(surveytype,"; nRx=", round(treated_total, 0))), vjust = 0, 
              nudge_x =0.0,nudge_y =0.1, angle = 90, cex=2.5) +
    theme_classic() +
    scale_x_continuous(limits = c(ymin,ymax), breaks = seq(ymin,ymax,1)) +
    scale_y_continuous(breaks =seq(0,1,0.20), limits=c(0, 1))+
    labs(title = {{country}},y="Proportion of ACT use", x = "Years") +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
  plot(plot1)
}

dev.off()

## Notes
# The model for the AM proportional usage
# Fitted using Empirical logit as the Outcome
# Covariates Standardized and detrended
# Include funtions with country, WHO_Sub-region and WHO_Region temporal trends
# BYM (Space-time)
# Model fitted from 2001 since there is no data 

# # Read needed data
# ## Load covariates 
###################
# Read needed data Treatment Seeking & Covariates Data
am.cov.data <- read.csv("FILEPATH/covariates_2024.csv")  %>% dplyr::select(c(location_name, year, pop_at_risk_pf, impcourse1st.l, pop_hmis,actpop_pc, acthmis_pc,logpopn))

## Join with treatment seeking outputs
ts_path<- "FILEPATH/TS_predictions_GAMkNN.csv"
ts <- read.csv(ts_path) %>%
  filter(Year > 1990 & Admin_Unit_Level== "ADMIN0") %>%
  rename(iso3 = ISO3,location_name = Country_Name, year = Year, trtseek = Any_pred_high) %>%
  dplyr::select(location_name, iso3, year,trtseek) %>% 
  mutate(location_name = fct_recode(location_name, "Timor-Leste" = "East Timor"))  %>%
  group_by(location_name, year) 

## Adding missing countries
ts.zanzibar <- ts %>% filter(location_name == "Tanzania") %>% 
  mutate(location_name = fct_recode(location_name, "Zanzibar" = "Tanzania"), 
         iso3 = fct_recode(iso3, "EAZ" = "TZA"))

## Full TS data
ts <- rbind(ts,ts.zanzibar)

# Save TS datafile
write.csv(ts, "FILEPATH/amusage_ts.csv")

########
am.cov.data<- am.cov.data %>%
  left_join(ts, by=c("location_name","year"))

am.cov.data$location_name <- as.character(am.cov.data$location_name)

rm(ts, ts.zanzibar) # remove not needed any longer

# Define attributes
country.list <- unique(am.cov.data$location_name)
Ncountry <- length(country.list)
years <- 2001:2024
Nyears <- length(years)

# Define models needed to be compared (i.e model performance after various exclusions of outliers/versions)

scenario<- c("M1:All","M2:Ex.Senegalof2018", "M3:Ex.Nigeriaof2016","M4:Ex.Malawiof2019","M5:Ex.Kenyaof2020",
             "M6:Ex.MICS","M7:Ex.Cases_U5","M8:Ex.all2022","M9:Ex.allbad") 

Nmodels = length(scenario)

# Functions needed ##### 
# To convert predicted values to probabilities

ilogit <- function(x) {return(1/(1+exp(-x)))} 

# Define Output matrices and tables

models_all<- matrix(NA, Ncountry*Nyears, Nmodels+4) # 4 for extra var to be added later-country, iso3, year, obs
table_stats<- matrix(NA, 9, 8) # Model Stats

colnames(table_stats) <-c("Model","Description", "S.Included","N.Cases", "WAIC", "CPO(CPO)","CPO(PIT)", "R2")

# Loop within am.data subset for Input data ( # Survey removal sensitivity)

for (m in 1:Nmodels) {
  
  model <- paste0("m", m)
  am.data <- read.csv("FILEPATH/amusage_binomial+surveytype.csv")
  
  if (model == "m1") {
    am.data <- am.data
    write.csv(am.data, paste0("FILEPATH/amusage_binomial+surveytype",model,".csv"))
  }
  if (model == "m2") {
    am.data <-
      am.data %>% filter(!(location_name == "Senegal" & year == 2018))
    write.csv(am.data, paste0("FILEPATH/amusage_binomial+surveytype",model,".csv"))
  }
  else if (model == "m3") {
    am.data <-
      am.data %>% filter(!(location_name == "Nigeria" & year == 2016))
    write.csv(am.data, paste0("FILEPATH/amusage_binomial+surveytype",model,".csv"))
  }
  else if (model == "m4") {
    am.data <-
      am.data %>% filter(!(location_name == "Malawi" & year == 2019))
    write.csv(am.data, paste0("FILEPATH/amusage_binomial+surveytype",model,".csv"))
  }
  else if (model == "m5") {
    am.data <-
      am.data %>% filter(!(location_name == "Kenya" & year == 2020))
    write.csv(am.data, paste0("FILEPATH/amusage_binomial+surveytype",model,".csv"))
  }
  else if (model == "m6") {
    am.data <-
      am.data %>% filter(!(surveytype == "MICS"))
    write.csv(am.data, paste0("FILEPATH/amusage_binomial+surveytype",model,".csv"))
  }  
  else if (model == "m7") {
    am.data <-
      am.data %>% filter(!(treated_total < 5))
    write.csv(am.data, paste0("FILEPATH/amusage_binomial+surveytype",model,".csv"))
  }  
  else if (model == "m8") {
    am.data <-
      am.data %>% filter(!(year == 2022))
    write.csv(am.data, paste0("FILEPATH/amusage_binomial+surveytype",model,".csv"))
  }  
  else if (model == "m9") {
    am.data <-
      am.data %>% filter(!(location_name=="Senegal" & year == 2018 & surveytype == "DHS" |
                             location_name=="Kenya" & year == 2020 & surveytype == "MIS" | 
                             location_name=="Malawi" & year == 2019 & surveytype == "MICS"|
                             location_name=="Nigeria" & year == 2016 & surveytype == "MICS"),
                         !(treated_total < 5)) 
    write.csv(am.data, paste0("FILEPATH/amusage_binomial+surveytype",model,".csv"))
  }
  
  cat(paste0("Excute ", "Model", m, " Scenario = ", scenario[m]), "\n")
  
  
  # Combined with the amusage data
  ## EDA
  am.data.combined<- am.cov.data %>% left_join(am.data, by=c("location_name","year"))
  am.data.combined$propusage <- am.data.combined$treated_act/am.data.combined$treated_total
  
  if (model == "m1") {
    write.csv(am.data.combined, "FILEPATH/amusage_modelinputdata.csv")
  }
  
  #glimpse(am.data.combined) 
    eda_data <- am.data.combined %>%
    select(propusage,propurban,anc1cov, oopfrac, eduallagsx, logthepc, trtseek)
  
  png(paste0("FILEPATH", "histogram_rawdata_", model, ".png"))
  
  PerformanceAnalytics::chart.Correlation(eda_data, histogram = TRUE, pch = 19)
  
  dev.off()
  
  #
  ## Load shape file and creating the BYM GRAPH
  
  load("FILEPATH/nb.am.mat.pred.Rdata")
  
  ### Defining Outcome variable
    # Cleaned Data (check which version is used)
  country.year.act.empirical.logit <- country.year.act.empirical.logit.var <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      n.act <- am.data$treated_act[as.character(am.data$location_name)==as.character(country.list[i]) & am.data$year==years[j]]
      n.nonact <- am.data$treated_nonact[as.character(am.data$location_name)==as.character(country.list[i]) & am.data$year==years[j]]
      if (length(n.act) > 0) {country.year.act.empirical.logit[i,j] <- log((n.act+0.5)/(n.nonact+0.5))
      country.year.act.empirical.logit.var[i,j] <- 1/(n.act+0.5)+1/(n.nonact+0.5)
      } 
    }
  }
  
  ## For typical logistic model
  country.year.act.positive <- country.year.act.tested <- country.year.act.prop<- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      n.act <- am.data$treated_act[as.character(am.data$location_name)==as.character(country.list[i]) & am.data$year==years[j]]
      n.nonact <- am.data$treated_nonact[as.character(am.data$location_name)==as.character(country.list[i]) & am.data$year==years[j]]
      if (length(n.act) > 0) {country.year.act.positive[i,j] <- n.act
      country.year.act.tested[i,j] <- n.act+n.nonact
      country.year.act.prop[i,j] <- n.act/(n.act+n.nonact)
      } 
    }
  }
  
  ## Survey type
  country.year.surveytype <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      surveytype <- am.data$surveytype[as.character(am.data$location_name) == as.character(country.list[i]) & am.data$year == years[j]]
      if (length(surveytype) > 0) {
        country.year.surveytype[i, j] <- surveytype
      }
    }
  }
  
  ## Survey year
  country.year.surveyyear <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      surveyyear <- am.data$year[as.character(am.data$location_name) == as.character(country.list[i]) & am.data$year == years[j]]
      if (length(surveyyear) > 0) {
        country.year.surveyyear[i, j] <- surveyyear
      }
    }
  }
  
  ## Prepare Covariates, Index and IDs 
  ## Covariates + standardizing + Truncate extreme values
  
  country.year.covariate.trtseek <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.trtseek[i,j] <- am.cov.data$trtseek[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  country.year.covariate.trtseek <- (country.year.covariate.trtseek-mean(country.year.covariate.trtseek,na.rm=T))/sd(country.year.covariate.trtseek,na.rm=T)
  country.year.covariate.trtseek[country.year.covariate.trtseek > 3]  <- 3
  country.year.covariate.trtseek[country.year.covariate.trtseek < -3] <- -3
  
  
  country.year.covariate.propurban <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.propurban[i,j] <- am.cov.data$propurban[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.propurban <- (country.year.covariate.propurban-mean(country.year.covariate.propurban,na.rm=T))/sd(country.year.covariate.propurban,na.rm=T)
  country.year.covariate.propurban[country.year.covariate.propurban > 3]  <- 3
  country.year.covariate.propurban[country.year.covariate.propurban < -3] <- -3
  
  
  country.year.covariate.measles1 <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.measles1[i,j] <- am.cov.data$measlescov1[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.measles1 <- (country.year.covariate.measles1-mean(country.year.covariate.measles1,na.rm=T))/sd(country.year.covariate.measles1,na.rm=T)
  country.year.covariate.measles1[country.year.covariate.measles1 > 3]  <- 3
  country.year.covariate.measles1[country.year.covariate.measles1 < -3] <- -3
  
  
  country.year.covariate.measles2 <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.measles2[i,j] <- am.cov.data$measlescov2[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.measles2 <- (country.year.covariate.measles2-mean(country.year.covariate.measles2,na.rm=T))/sd(country.year.covariate.measles2,na.rm=T)
  country.year.covariate.measles2[country.year.covariate.measles2 > 3]  <- 3
  country.year.covariate.measles2[country.year.covariate.measles2 < -3] <- -3
  
  
  
  country.year.covariate.ifdcov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.ifdcov[i,j] <- am.cov.data$measlescov2[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.ifdcov <- (country.year.covariate.ifdcov-mean(country.year.covariate.ifdcov,na.rm=T))/sd(country.year.covariate.ifdcov,na.rm=T)
  country.year.covariate.ifdcov[country.year.covariate.ifdcov > 3]  <- 3
  country.year.covariate.ifdcov[country.year.covariate.ifdcov < -3] <- -3
  
  
  ############
  country.year.covariate.underweight <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.underweight[i,j] <- am.cov.data$underweight[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.underweight <- (country.year.covariate.underweight-mean(country.year.covariate.underweight,na.rm=T))/sd(country.year.covariate.underweight,na.rm=T)
  country.year.covariate.underweight[country.year.covariate.underweight > 3]  <- 3
  country.year.covariate.underweight[country.year.covariate.underweight < -3] <- -3
  
  
  country.year.covariate.hospbeds <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.hospbeds[i,j] <- am.cov.data$hospbeds[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.hospbeds <- (country.year.covariate.hospbeds-mean(country.year.covariate.hospbeds,na.rm=T))/sd(country.year.covariate.hospbeds,na.rm=T)
  country.year.covariate.hospbeds[country.year.covariate.hospbeds > 3]  <- 3
  country.year.covariate.hospbeds[country.year.covariate.hospbeds < -3] <- -3
  
  
  ############
  country.year.covariate.indvhealth <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.indvhealth[i,j] <- am.cov.data$indvhealth[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.indvhealth <- (country.year.covariate.indvhealth-mean(country.year.covariate.indvhealth,na.rm=T))/sd(country.year.covariate.indvhealth,na.rm=T)
  country.year.covariate.indvhealth[country.year.covariate.indvhealth > 3]  <- 3
  country.year.covariate.indvhealth[country.year.covariate.indvhealth < -3] <- -3
  
  country.year.covariate.univcover <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.univcover[i,j] <- am.cov.data$univcover[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.univcover <- (country.year.covariate.univcover-mean(country.year.covariate.univcover,na.rm=T))/sd(country.year.covariate.univcover,na.rm=T)
  country.year.covariate.univcover[country.year.covariate.univcover > 3]  <- 3
  country.year.covariate.univcover[country.year.covariate.univcover < -3] <- -3
  
  country.year.covariate.malincid <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.malincid[i,j] <- am.cov.data$malincid[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.malincid <- (country.year.covariate.malincid-mean(country.year.covariate.malincid,na.rm=T))/sd(country.year.covariate.malincid,na.rm=T)
  country.year.covariate.malincid[country.year.covariate.malincid > 3]  <- 3
  country.year.covariate.malincid[country.year.covariate.malincid < -3] <- -3
  
  
  ###############
  country.year.covariate.anc4cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.anc4cov[i,j] <- am.cov.data$anc4cov[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.anc4cov <- (country.year.covariate.anc4cov-mean(country.year.covariate.anc4cov,na.rm=T))/sd(country.year.covariate.anc4cov,na.rm=T)
  country.year.covariate.anc4cov[country.year.covariate.anc4cov > 3]  <- 3
  country.year.covariate.anc4cov[country.year.covariate.anc4cov < -3] <- -3
  
  
  country.year.covariate.anc1cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.anc1cov[i,j] <- am.cov.data$anc1cov[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.anc1cov <- (country.year.covariate.anc1cov-mean(country.year.covariate.anc1cov,na.rm=T))/sd(country.year.covariate.anc1cov,na.rm=T)
  country.year.covariate.anc1cov[country.year.covariate.anc1cov > 3]  <- 3
  country.year.covariate.anc1cov[country.year.covariate.anc1cov < -3] <- -3
  
  
  country.year.covariate.dpt3cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.dpt3cov[i,j] <- am.cov.data$dpt3cov[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.dpt3cov <- (country.year.covariate.dpt3cov-mean(country.year.covariate.dpt3cov,na.rm=T))/sd(country.year.covariate.dpt3cov,na.rm=T)
  country.year.covariate.dpt3cov[country.year.covariate.dpt3cov > 3]  <- 3
  country.year.covariate.dpt3cov[country.year.covariate.dpt3cov < -3] <- -3
  
  
  country.year.covariate.oopfrac <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.oopfrac[i,j] <- am.cov.data$oopfrac[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.oopfrac <- (country.year.covariate.oopfrac-mean(country.year.covariate.oopfrac,na.rm=T))/sd(country.year.covariate.oopfrac,na.rm=T)
  country.year.covariate.oopfrac[country.year.covariate.oopfrac > 3]  <- 3
  country.year.covariate.oopfrac[country.year.covariate.oopfrac < -3] <- -3
  
  
  country.year.covariate.hcaccess <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.hcaccess[i,j] <- am.cov.data$hcaccess[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.hcaccess <- (country.year.covariate.hcaccess-mean(country.year.covariate.hcaccess,na.rm=T))/sd(country.year.covariate.hcaccess,na.rm=T)
  country.year.covariate.hcaccess[country.year.covariate.hcaccess > 3]  <- 3
  country.year.covariate.hcaccess[country.year.covariate.hcaccess < -3] <- -3
  
  
  country.year.covariate.eduallagsx <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.eduallagsx[i,j] <- am.cov.data$eduallagsx[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  
  country.year.covariate.eduallagsx <- (country.year.covariate.eduallagsx-mean(country.year.covariate.eduallagsx,na.rm=T))/sd(country.year.covariate.eduallagsx,na.rm=T)
  country.year.covariate.eduallagsx[country.year.covariate.eduallagsx > 3]  <- 3
  country.year.covariate.eduallagsx[country.year.covariate.eduallagsx < -3] <- -3
  
  
  country.year.covariate.sbacov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.sbacov[i,j] <- am.cov.data$sbacov[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.sbacov <- (country.year.covariate.sbacov-mean(country.year.covariate.sbacov,na.rm=T))/sd(country.year.covariate.sbacov,na.rm=T)
  country.year.covariate.sbacov[country.year.covariate.sbacov > 3]  <- 3
  country.year.covariate.sbacov[country.year.covariate.sbacov < -3] <- -3
  
  
  country.year.covariate.hecap <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.hecap[i,j] <- am.cov.data$hecap[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.hecap <- (country.year.covariate.hecap-mean(country.year.covariate.hecap,na.rm=T))/sd(country.year.covariate.hecap,na.rm=T)
  country.year.covariate.hecap[country.year.covariate.hecap > 3]  <- 3
  country.year.covariate.hecap[country.year.covariate.hecap < -3] <- -3
  
  
  country.year.covariate.haqi <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.haqi[i,j] <- am.cov.data$haqi[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.haqi <- (country.year.covariate.haqi-mean(country.year.covariate.haqi,na.rm=T))/sd(country.year.covariate.haqi,na.rm=T)
  country.year.covariate.haqi[country.year.covariate.haqi > 3]  <- 3
  country.year.covariate.haqi[country.year.covariate.haqi < -3] <- -3
  
  
  country.year.covariate.actyears20 <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.actyears20[i,j] <- am.cov.data$actyears20[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.actyears20 <- (country.year.covariate.actyears20-mean(country.year.covariate.actyears20,na.rm=T))/sd(country.year.covariate.actyears20,na.rm=T)
  country.year.covariate.actyears20[country.year.covariate.actyears20 > 3]  <- 3
  country.year.covariate.actyears20[country.year.covariate.actyears20 < -3] <- -3
  
  
  country.year.covariate.logthepc <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.logthepc[i,j] <- am.cov.data$logthepc[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  
  country.year.covariate.logthepc <- (country.year.covariate.logthepc-mean(country.year.covariate.logthepc,na.rm=T))/sd(country.year.covariate.logthepc,na.rm=T)
  country.year.covariate.logthepc[country.year.covariate.logthepc > 3]  <- 3
  country.year.covariate.logthepc[country.year.covariate.logthepc < -3] <- -3
  
  
  country.year.covariate.ldipc <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.ldipc[i,j] <- am.cov.data$ldipc[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.ldipc <- (country.year.covariate.ldipc-mean(country.year.covariate.ldipc,na.rm=T))/sd(country.year.covariate.ldipc,na.rm=T)
  country.year.covariate.ldipc[country.year.covariate.ldipc > 3]  <- 3
  country.year.covariate.ldipc[country.year.covariate.ldipc < -3] <- -3
  
  country.year.covariate.logpopn <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.logpopn[i,j] <- am.cov.data$logpopn[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.covariate.is_act <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.covariate.is_act[i,j] <- am.cov.data$is_act[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  ## Country, year and country.year index
  country.year.country.id <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.country.id[i,j] <- i
    }
  }
  country.year.year.id <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.year.id[i,j] <- j
    }
  }
  
  country.year.year.year <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.year.year[i,j] <- am.cov.data$year[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.country.names <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.country.names[i,j] <- am.cov.data$location_name[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  country.year.country.iso3 <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.country.iso3[i,j] <- am.cov.data$iso3[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]
    }
  }
  
  ## Regional Index and covariates/identifiers
  whoregions <- unique(am.cov.data$who_region)
  country.year.whoregion.id <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whoregion.id[i,j] <- match(as.character(am.cov.data$who_region[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),as.character(whoregions))
    }
  }
  
  country.year.whoregion.afro.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whoregion.afro.cov[i,j] <- ifelse(as.character((am.cov.data$who_region=="AFRO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whoregion.emro.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whoregion.emro.cov[i,j] <- ifelse(as.character((am.cov.data$who_region=="EMRO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whoregion.euro.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whoregion.euro.cov[i,j] <- ifelse(as.character((am.cov.data$who_region=="EURO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whoregion.paho.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whoregion.paho.cov[i,j] <- ifelse(as.character((am.cov.data$who_region=="PAHO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whoregion.searo.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whoregion.searo.cov[i,j] <- ifelse(as.character((am.cov.data$who_region=="SEARO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whoregion.wpro.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whoregion.wpro.cov[i,j] <- ifelse(as.character((am.cov.data$who_region=="WPRO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  
  ## Sub_regional Index and covariates/identifiers
  whosubregions <- unique(am.cov.data$who_subregion)
  country.year.whosubregion.id <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whosubregion.id[i,j] <- match(as.character(am.cov.data$who_subregion[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),as.character(whosubregions))
    }
  }
  
  country.year.whosubregion.afro.c.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whosubregion.afro.c.cov[i,j] <- ifelse(as.character((am.cov.data$who_subregion=="AFRO-C")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whosubregion.afro.e.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whosubregion.afro.e.cov[i,j] <- ifelse(as.character((am.cov.data$who_subregion=="AFRO-E")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whosubregion.afro.s.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whosubregion.afro.s.cov[i,j] <- ifelse(as.character((am.cov.data$who_subregion=="AFRO-S")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whosubregion.afro.w.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whosubregion.afro.w.cov[i,j] <- ifelse(as.character((am.cov.data$who_subregion=="AFRO-W")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  
  country.year.whosubregion.emro.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whosubregion.emro.cov[i,j] <- ifelse(as.character((am.cov.data$who_subregion=="EMRO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whosubregion.euro.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whosubregion.euro.cov[i,j] <- ifelse(as.character((am.cov.data$who_subregion=="EURO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whosubregion.paho.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whosubregion.paho.cov[i,j] <- ifelse(as.character((am.cov.data$who_subregion=="PAHO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whosubregion.searo.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whosubregion.searo.cov[i,j] <- ifelse(as.character((am.cov.data$who_subregion=="SEARO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  country.year.whosubregion.wpro.cov <- matrix(NA,nrow=Ncountry,ncol=Nyears)
  for (i in 1:Ncountry) {
    for (j in 1:Nyears) {
      country.year.whosubregion.wpro.cov[i,j] <- ifelse(as.character((am.cov.data$who_subregion=="WPRO")[as.character(am.cov.data$location_name)==as.character(country.list[i]) & am.cov.data$year==years[j]][1]),1,0)
    }
  }
  
  
  # gen iter1 = trtseek*propurban
  ## De-trending covariates
  
  linear.trend <- lm(colMeans(country.year.covariate.propurban,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.propurban,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.propurban.linear.detrended <- as.numeric(country.year.covariate.propurban)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.measles1,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.measles1,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.measles1.linear.detrended <- as.numeric(country.year.covariate.measles1)-linear.trend[as.numeric(country.year.year.id)]
  
  
  linear.trend <- lm(colMeans(country.year.covariate.measles2,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.measles2,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.measles2.linear.detrended <- as.numeric(country.year.covariate.measles2)-linear.trend[as.numeric(country.year.year.id)]
  
  
  linear.trend <- lm(colMeans(country.year.covariate.ifdcov,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.ifdcov,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.ifdcov.linear.detrended <- as.numeric(country.year.covariate.ifdcov)-linear.trend[as.numeric(country.year.year.id)]
  
  
  linear.trend <- lm(colMeans(country.year.covariate.underweight,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.underweight,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.underweight.linear.detrended <- as.numeric(country.year.covariate.underweight)-linear.trend[as.numeric(country.year.year.id)]
  
  
  linear.trend <- lm(colMeans(country.year.covariate.hospbeds,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.hospbeds,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.hospbeds.linear.detrended <- as.numeric(country.year.covariate.hospbeds)-linear.trend[as.numeric(country.year.year.id)]
  
  
  linear.trend <- lm(colMeans(country.year.covariate.indvhealth,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.indvhealth,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.indvhealth.linear.detrended <- as.numeric(country.year.covariate.indvhealth)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.trtseek,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.trtseek,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.trtseek.linear.detrended <- as.numeric(country.year.covariate.trtseek)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.anc4cov,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.anc4cov,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.anc4cov.linear.detrended <- as.numeric(country.year.covariate.anc4cov)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.anc1cov,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.anc1cov,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.anc1cov.linear.detrended <- as.numeric(country.year.covariate.anc1cov)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.dpt3cov,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.dpt3cov,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.dpt3cov.linear.detrended <- as.numeric(country.year.covariate.dpt3cov)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.oopfrac,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.oopfrac,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.oopfrac.linear.detrended <- as.numeric(country.year.covariate.oopfrac)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.hcaccess,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.hcaccess,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.hcaccess.linear.detrended <- as.numeric(country.year.covariate.hcaccess)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.eduallagsx,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.eduallagsx,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.eduallagsx.linear.detrended <- as.numeric(country.year.covariate.eduallagsx)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.sbacov,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.sbacov,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.sbacov.linear.detrended <- as.numeric(country.year.covariate.sbacov)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.hecap,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.hecap,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.hecap.linear.detrended <- as.numeric(country.year.covariate.hecap)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.haqi,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.haqi,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.haqi.linear.detrended <- as.numeric(country.year.covariate.haqi)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.actyears20,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.actyears20,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.actyears20.linear.detrended <- as.numeric(country.year.covariate.actyears20)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.logthepc,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.logthepc,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.logthepc.linear.detrended <- as.numeric(country.year.covariate.logthepc)-linear.trend[as.numeric(country.year.year.id)]
  
  linear.trend <- lm(colMeans(country.year.covariate.ldipc,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.ldipc,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.ldipc.linear.detrended <- as.numeric(country.year.covariate.ldipc)-linear.trend[as.numeric(country.year.year.id)]
  
  
  linear.trend <- lm(colMeans(country.year.covariate.univcover,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.univcover,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.univcover.linear.detrended <- as.numeric(country.year.covariate.univcover)-linear.trend[as.numeric(country.year.year.id)]
  
  
  linear.trend <- lm(colMeans(country.year.covariate.malincid,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.covariate.malincid,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.covariate.malincid.linear.detrended <- as.numeric(country.year.covariate.malincid)-linear.trend[as.numeric(country.year.year.id)]
  
  ## Detrending the outcome variable
  # 
  linear.trend <- lm(colMeans(country.year.act.prop,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.act.prop,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.act.prop.detrended <- as.numeric(country.year.act.prop)-linear.trend[as.numeric(country.year.year.id)]
  #plot(linear.trend)
  
  linear.trend <- lm(colMeans(country.year.act.empirical.logit,na.rm=TRUE) ~ years)$coefficients[2]*years+lm(colMeans(country.year.act.empirical.logit,na.rm=TRUE) ~ years)$coefficients[1]
  country.year.act.empirical.logit.linear.detrended <- as.numeric(country.year.act.empirical.logit)-linear.trend[as.numeric(country.year.year.id)]
  #plot(linear.trend)
  
  # Compile data

  data <- list('emplogitact'=as.numeric(country.year.act.empirical.logit),
               'emplogitact.d'=as.numeric(country.year.act.empirical.logit.linear.detrended),
               'emplogitactvar'=1/sqrt(as.numeric(country.year.act.empirical.logit.var)),
               'tested'=as.numeric(country.year.act.tested),
               'positive'=as.numeric(country.year.act.positive),
               'act.prop'=as.numeric(country.year.act.prop),
               'act.prop.d'=as.numeric(country.year.act.prop.detrended),
               'country.year.country.id'=as.numeric(country.year.country.id),
               'country.year.country.idsp1'=as.numeric(country.year.country.id),
               'country.year.country.idsp2'=as.numeric(country.year.country.id),
               'country.year.year.idx'=as.numeric(country.year.year.id),
               'country.year.year.idy'=as.numeric(country.year.year.id),
               'country.year.year.idz'=as.numeric(country.year.year.id),
               'country.year.year.idsp1'=as.numeric(country.year.year.id),
               'country.year.year.idsp2'=as.numeric(country.year.year.id),
               'country.year.year.idsp3'=as.numeric(country.year.year.id),
               'country.year.countryyear.id'=1:(Ncountry*Nyears),
               'country.year.whoregion.id'=as.numeric(country.year.whoregion.id),
               'country.year.whoregion.afro.cov'=as.numeric(country.year.whoregion.afro.cov),
               'country.year.whoregion.emro.cov' = as.numeric(country.year.whoregion.emro.cov),
               'country.year.whoregion.euro.cov' = as.numeric(country.year.whoregion.euro.cov),
               'country.year.whoregion.paho.cov' = as.numeric(country.year.whoregion.paho.cov),
               'country.year.whoregion.searo.cov' = as.numeric(country.year.whoregion.searo.cov),
               'country.year.whoregion.wpro.cov'=as.numeric(country.year.whoregion.wpro.cov),
               'country.year.whosubregion.id'=as.numeric(country.year.whosubregion.id),
               'country.year.whosubregion.afro.c.cov'=as.numeric(country.year.whosubregion.afro.c.cov),
               'country.year.whosubregion.afro.e.cov'=as.numeric(country.year.whosubregion.afro.e.cov),
               'country.year.whosubregion.afro.s.cov'=as.numeric(country.year.whosubregion.afro.s.cov),
               'country.year.whosubregion.afro.w.cov'=as.numeric(country.year.whosubregion.afro.w.cov),
               'country.year.whosubregion.emro.cov' = as.numeric(country.year.whosubregion.emro.cov),
               'country.year.whosubregion.euro.cov' = as.numeric(country.year.whosubregion.euro.cov),
               'country.year.whosubregion.paho.cov' = as.numeric(country.year.whosubregion.paho.cov),
               'country.year.whosubregion.searo.cov' = as.numeric(country.year.whosubregion.searo.cov),
               'country.year.whosubregion.wpro.cov'=as.numeric(country.year.whosubregion.wpro.cov),
               'country.factor.cov'=as.factor(country.list),
               'trtseek'= as.numeric(country.year.covariate.trtseek),
               'trtseek.d'= as.numeric(country.year.covariate.trtseek.linear.detrended), 
               'hospbeds' = as.numeric(country.year.covariate.hospbeds),
               'hospbeds.d' = as.numeric(country.year.covariate.hospbeds.linear.detrended),
               'measles1'=as.numeric(country.year.covariate.measles1),
               'measles1.d'=as.numeric(country.year.covariate.measles1.linear.detrended),
               'measles2'=as.numeric(country.year.covariate.measles2),
               'measles2.d'=as.numeric(country.year.covariate.measles2.linear.detrended),
               'popurban'= as.numeric(country.year.covariate.propurban), 
               'popurban.d'= as.numeric(country.year.covariate.propurban.linear.detrended),
               'anc4cov' = as.numeric(country.year.covariate.anc4cov),
               'anc4cov.d' = as.numeric(country.year.covariate.anc4cov.linear.detrended),
               'anc1cov' = as.numeric(country.year.covariate.anc1cov),
               'anc1cov.d' = as.numeric(country.year.covariate.anc1cov.linear.detrended),             
               'dpt3cov' = as.numeric(country.year.covariate.dpt3cov),
               'dpt3cov.d' = as.numeric(country.year.covariate.dpt3cov.linear.detrended),
               'indvhealth' = as.numeric(country.year.covariate.indvhealth),
               'indvhealth.d' = as.numeric(country.year.covariate.indvhealth.linear.detrended),
               'ifdcov' = as.numeric(country.year.covariate.ifdcov),
               'ifdcov.d' = as.numeric(country.year.covariate.ifdcov.linear.detrended),
               'malincid' = as.numeric(country.year.covariate.malincid),
               'malincid.d' = as.numeric(country.year.covariate.malincid.linear.detrended),
               'oopfrac' = as.numeric(country.year.covariate.oopfrac),
               'oopfrac.d' = as.numeric(country.year.covariate.oopfrac.linear.detrended),
               'hcaccess' = as.numeric(country.year.covariate.hcaccess),
               'hcaccess.d' = as.numeric(country.year.covariate.hcaccess.linear.detrended),
               'eduallagsx' = as.numeric(country.year.covariate.eduallagsx),
               'eduallagsx.d' = as.numeric(country.year.covariate.eduallagsx.linear.detrended),
               'sbacov' = as.numeric(country.year.covariate.sbacov),
               'sbacov.d' = as.numeric(country.year.covariate.sbacov.linear.detrended),
               'hecap' = as.numeric(country.year.covariate.hecap),
               'hecap.d' = as.numeric(country.year.covariate.hecap.linear.detrended),
               'haqi' = as.numeric(country.year.covariate.haqi),
               'haqi.d' = as.numeric(country.year.covariate.haqi.linear.detrended),
               'actyears20' = as.numeric(country.year.covariate.actyears20),
               'actyears20.d' = as.numeric(country.year.covariate.actyears20.linear.detrended),
               'logthepc' = as.numeric(country.year.covariate.logthepc),
               'logthepc.d' = as.numeric(country.year.covariate.logthepc.linear.detrended),
               'ldipc' = as.numeric(country.year.covariate.ldipc),
               'ldipc.d' = as.numeric(country.year.covariate.ldipc.linear.detrended),
               'underweight' = as.numeric(country.year.covariate.underweight),
               'underweight.d' = as.numeric(country.year.covariate.underweight.linear.detrended),
               'univcover' = as.numeric(country.year.covariate.univcover),
               'univcover.d' = as.numeric(country.year.covariate.univcover.linear.detrended),
               'logpopn' = as.numeric(country.year.covariate.logpopn),
               #'weight_svtype' = as.numeric(country.year.act.weight),
               'is_act' = as.numeric(country.year.covariate.is_act),
               'country.year.year.id'= as.numeric(country.year.year.id)
  )
  
 
  ##  Modeling and calculate validation/calibration statistics
  #Running the model
  source("FILEPATH/2d.amusage_modelfit+predict+saveoutput_exp.R")
  ##  Model: EL AR1 
  ## The components are added/removed until the best model is obtained
  
  # Part I: Fitting 
  
  ###############################
    ## Model structure (FINAL) ####
  ###############################
  res <- inla(
    emplogitact.d ~ 0 +
      is_act + #
      trtseek.d +
      popurban.d + #
      anc1cov.d +
      oopfrac.d +
      eduallagsx.d +
      logthepc.d + 
      logamdist +#
      f(
        country.year.country.id,
        model = "bym2",
        graph = nb.am.mat.pred,
        scale.model = T,
        constr = TRUE,
        hyper = list(
          prec.unstruct = list(prior = 'loggamma', param = c(0.1, 0.05)),
          prec.spatial = list(prior =  'loggamma', param = c(0.1, 0.01))
        )
      ) + 
      country.year.year.idsp1 +
      country.year.country.idsp1 +
      f(
        country.year.year.idz,
        model = "ar",
        order = 1,
        group = country.year.whoregion.id,
        constr = FALSE
      ),
    data = data,
    quantiles = c(0.025, 0.25, 0.5, 0.75, 0.975),
    control.inla = list(int.strategy = "eb"),
    scale = emplogitactvar ,
    control.compute =
      list(config = TRUE, waic = TRUE, cpo = TRUE),
    control.predictor = list(compute = TRUE),
    verbose = F
  )
  
  res.improved.result= inla.rerun(res)
  res.improved.result= inla.rerun(res.improved.result)
  
  res <- res.improved.result
  
  # Save model object ####
  
  save(res, file = paste0("FILEPATH/outputs.modelobject_", model, ".Rdata"))
  
  load(paste0("FILEPATH/outputs.modelobject_", model, ".Rdata"))
  
  #  CPO PIT AND PLOTS ####
  png(paste0("FILEPATH", "Figure_PIT_AMUsage_", model,".png"))
  
  plot(res, plot.fixed.effects=FALSE, plot.lincomb=FALSE, plot.random.effects=FALSE,
       plot.hyperparameters=FALSE, plot.predictor=FALSE, plot.q=FALSE, plot.cpo=TRUE,
       single=FALSE)
  
  dev.off()
  
  # Save Model statistics  ####
  
  table_stats[m,1] <- model
  table_stats[m,2] <- scenario[m]
  table_stats[m,3] <- dim(am.data)[1]
  table_stats[m,4] <- round(sum(am.data$treated_total),0)
  table_stats[m,5] <- round(res$waic$waic, 1)
  table_stats[m,6] <- round(sum(res$cpo$cpo, na.rm = T), 1)
  table_stats[m,7] <- round(sum(res$cpo$pit, na.rm = T), 1)
  
  # Save Summary of fixed effects  ####
  write.table(round(res$summary.fixed[, c(1:2, 5:7)], 2), 
              file = "FILEPATH/fixedeffects.csv", sep = ",", col.names = NA,
              qmethod = "double", append = TRUE)
  summary(res)
  
  # Saving predicted results on a Table format  ####
  # Get predicted values
  
  pred.val.model <- matrix(res$summary.linear.predictor$`0.5quant`,nrow=Ncountry,ncol=Nyears)+
    matrix(linear.trend[as.numeric(country.year.year.id)],nrow=Ncountry) 
  
  prob.bymsp.d <- ilogit(pred.val.model)
  
  # convert to dataframe
  actpred.out<-as.data.frame(cbind(country.list,prob.bymsp.d)) %>%
    rename(location_name = country.list)
  
  # pivot to long format
  actpred.out.long <- actpred.out %>% gather(year, value, -c(location_name))  %>%
    mutate(year = fct_recode(
      year,"2001" = "V2","2002" = "V3", "2003" = "V4","2004" = "V5",
      "2005" = "V6",  "2006" = "V7",  "2007" = "V8",  "2008" = "V9",  "2009" = "V10",  "2010" = "V11",  
      "2011" = "V12",  "2012" = "V13",  "2013" = "V14",  "2014" = "V15",  "2015" = "V16",  "2016" = "V17",  
      "2017" = "V18",  "2018" = "V19",  "2019" = "V20",  "2020" = "V21",  "2021" = "V22",  "2022" = "V23",
      "2023" = "V24",  "2024" = "V25"),
      location_name = as.character(location_name)) %>%
    rename(predpropact = value)
  
  actpred.out.long$year = as.numeric(as.character(actpred.out.long$year))
  actpred.out.long$predpropact = as.numeric(actpred.out.long$predpropact)
  
  actpred.out.long$predpact_map	<- actpred.out.long$predpropact
  actpred.out.long$predpnact_map	<- 1 - (actpred.out.long$predpact_map)
  
  ## Prepare the final proportional usage table for making rasters ###
  ## AND merge with Geometry table  ###
  am.usage.table<-am.cov.data %>%
    dplyr::select(location_name, uid, admin_level, year, year_id, who_region, who_subregion) %>%
    group_by(location_name, uid, admin_level, year, year_id, who_region, who_subregion) %>%
    summarise(year_id= mean(year_id)) %>%
    left_join(actpred.out.long, by = c("location_name", "year"))
  
  ## Fill the missing values for years before 2001 as same as 2001
  am.usage.table$predpropact[am.usage.table$year== 2000] <- am.usage.table$predpropact[am.usage.table$year == 2001]
  am.usage.table$predpropact[am.usage.table$year== 1999] <- am.usage.table$predpropact[am.usage.table$year == 2001]
  am.usage.table$predpropact[am.usage.table$year== 1998] <- am.usage.table$predpropact[am.usage.table$year == 2001]
  am.usage.table$predpropact[am.usage.table$year== 1997] <- am.usage.table$predpropact[am.usage.table$year == 2001]
  am.usage.table$predpropact[am.usage.table$year== 1996] <- am.usage.table$predpropact[am.usage.table$year == 2001]
  am.usage.table$predpropact[am.usage.table$year== 1995] <- am.usage.table$predpropact[am.usage.table$year == 2001]
  am.usage.table$predpropact[am.usage.table$year== 1994] <- am.usage.table$predpropact[am.usage.table$year == 2001]
  am.usage.table$predpropact[am.usage.table$year== 1993] <- am.usage.table$predpropact[am.usage.table$year == 2001]
  am.usage.table$predpropact[am.usage.table$year== 1992] <- am.usage.table$predpropact[am.usage.table$year == 2001]
  am.usage.table$predpropact[am.usage.table$year== 1991] <- am.usage.table$predpropact[am.usage.table$year == 2001]
  
  # Calculate the non_act vector 
  am.usage.table$predpact_map	<- am.usage.table$predpropact
  am.usage.table$predpnact_map	<- 1 - (am.usage.table$predpact_map)
  
  write.csv(am.usage.table,paste0("FILEPATH/pred_actusage_bym_table_1991_2022_",model,".csv"))
  
  # Write Table File for making ACTusage rasters
  act.table.forrasters<- am.usage.table %>%
    dplyr::select(location_name, uid, admin_level, year, predpact_map, predpnact_map)
  
  write.csv(act.table.forrasters,paste0("FILEPATH/pred_actusage_forrasters_bym_", lubridate::year(Sys.Date()), ".csv"))
  
  ##  Plots from the INLA model object

  # Get predicted values
  
  pred.val.model <- matrix(res$summary.linear.predictor$`0.5quant`,nrow=Ncountry,ncol=Nyears)+
    matrix(linear.trend[as.numeric(country.year.year.id)],nrow=Ncountry) 
  
  models_all[,m+4] <- as.numeric(ilogit(pred.val.model))
  
  # Plot Predicted vs. Observed
  
  x<- as.data.frame(as.numeric(country.year.act.prop))
  y<-as.data.frame(as.numeric(ilogit(pred.val.model)))
  z1<-as.data.frame(as.character(country.year.covariate.is_act))
  z2<-as.data.frame(as.character(country.year.surveytype))
  z3<-as.data.frame(as.character(country.year.surveyyear))
  z4<-as.data.frame(as.character(country.year.country.names))
  z5<-as.data.frame(as.character(country.year.country.iso3))
  
  xy<-cbind(x,y,z1, z2, z3, z4,z5)
  colnames(xy) <-c("obs","pred","act", "survey","year","country", "iso3") 
  xy$diff <- (abs(xy$obs-xy$pred))/xy$obs
  xy$diff[xy$diff == Inf] <- 0
  xy <-xy %>% filter(!is.na(as.numeric(country.year.act.prop)))
  
  # Record AM type
  xy <-xy %>% 
    mutate(act = recode(act, 
                        "1" = "ACT", 
                        "0" ="nACT"))
  
  xy <-xy %>% filter(!is.na(obs))
  
  # Rsqaured Observed vs Predicted (In sample)
  table_stats[m,8] <- round(cor(xy$obs, xy$pred), 2)
  
  # Save plots
  png(paste0("FILEPATH", "Scatter_Observedvs.Predicted_", model,".png"),
      width = 725, height = 831)
  
  #plot(xy$x, xy$y)
  
  p1 <- ggplot(xy, aes(obs,pred,)) +
    geom_point(alpha=0.5, size=2, aes(color = country), show.legend = F) + 
    geom_smooth(method = "lm", se=FALSE) +
    #stat_regline_equation(label.y = 0.9, aes(label = ..eq.label..)) +
    stat_regline_equation(label.y = 1, aes(label = ..rr.label..))+
    labs(x = "Observed",y = "Predicted", 
         title = paste0("Model", m, " Scenario = ", scenario[m],"\n", "Insample Observed vs. Predicted"),
         subtitle = "ACT usage Overall")  +
    geom_text(data = xy %>% filter(diff >0.2), aes(label=iso3), vjust = 0, 
              nudge_x =0.0,nudge_y =0.05, angle = 90, cex=2.5) 
  
  p2 <- ggplot(xy, aes(obs,pred )) +
    geom_point(alpha=0.5, size=2) + 
    geom_smooth(method = "lm", se=FALSE) +
    #stat_regline_equation(label.y = 0.9, aes(label = ..eq.label..)) +
    stat_regline_equation(label.y = 1, aes(label = ..rr.label..))+
    labs(x = "Observed",y = "Predicted", subtitle = "ACT usage by ACT Policy")  +
    facet_wrap(~act)
  
  p3 <- ggplot(xy, aes(obs,pred )) +
    geom_point(alpha=0.5, size=2) + 
    geom_smooth(method = "lm", se=FALSE) +
    #stat_regline_equation(label.y = 0.9, aes(label = ..eq.label..)) +
    stat_regline_equation(label.y = 1, aes(label = ..rr.label..))+
    labs(x = "Observed",y = "Predicted", subtitle = "ACT usage by Survey Type")  +
    facet_wrap(~survey)
  
  library(cowplot)
  obj_is_vector = function(x){TRUE}
  # Combine the plots
  combined_plots <- plot_grid(p1, p2, p3, ncol = 1)
  
  # Print the combined plots
  print(combined_plots)
  
  dev.off()
} # end of model loop

## Save other generated files
# Model statistics 
stat_table <- insight::format_table(table_stats)
insight::export_table(stat_table, format = "html")
write.table(stat_table, file = "FILEPATH/modelstat_table.csv")

# ###############################  # ############################### # ############################### 

# Spatial plots from the model - spatial fields and quick visualization of the predicted values

# Best model
# model <- paste0("m", 4)
model <- paste0("m", 9)#for gbd2024
# load model object
load(paste0("FILEPATH/outputs.modelobject_", model, ".Rdata"))

# load combined data observed data, covs ++
load("FILEPATH/data.combined_spatialpolygon.RData")

global.gen <- st_as_sf(data.combined)

global.gen$location_name <- global.gen$country.list

id_country<-unique(as.data.frame(cbind(ID=as.numeric(data$country.year.country.id), location_name= as.character(data$country.factor.cov))))
id_country$ID<- as.numeric(id_country$ID)

global.gen <- global.gen %>% left_join(id_country)

global.gen.copy <- global.gen

# Check results of hyperparameters and plot
png("FILEPATH/Figure_Posteriorfit_hyperparameters.png")

res$summary.hyperpar

# Transform to get the standard deviation
sd_mar <- as.data.frame(inla.tmarginal(function(x) exp(-1/2*x), 
                                       res$internal.marginals.hyperpar$`Log precision for country.year.country.id (spatial component)`))
##head(sd_mar)

# Plot the posteriors of the hyperparameters of the spatial field
ggplot() + geom_line(data = sd_mar, aes(x = x, y = y)) + theme_bw() + 
  ggtitle("Posterior- sd spatial field") -> p1

# plot the posteriors of the hyperparameters of the iid field
sd_iid <- as.data.frame(inla.tmarginal(function(x) exp(-1/2*x), 
                                       res$internal.marginals.hyperpar$`Log precision for country.year.country.id (idd component)`))
#head(sd_iid)

ggplot() + geom_line(data = sd_iid, aes(x = x, y = y)) + theme_bw() + 
  ggtitle("Posterior sd IID field") -> p2

# the Rho
ggplot() + geom_line(data = as.data.frame(res$marginals.hyperpar$`GroupRho for country.year.year.idz`), 
                     aes(x = x, y = y)) + theme_bw() + 
  ggtitle("Posterior sd mixing parameter") -> p3

grid.arrange(p1,p2,p3,
             ncol = 1)

dev.off()

# Could be Variation in the field captured by the BYM prior, nevertheless the mixing parameter is very small, 
# most variation is an attribute to the unstructured spatial components. 
# The field is dominated by overdispersion rather than spatial autocorelation.

# Plot the predicted values
p_act <-matrix(res$summary.linear.predictor$`0.5quant`,nrow=Ncountry,ncol=Nyears)+
  matrix(linear.trend[as.numeric(country.year.year.id)],nrow=Ncountry)

p_act <- as.numeric(ilogit(p_act))
p_act <-as.data.frame(p_act)

p_act$location_name<-c(country.year.country.names)
p_act$ID<-c(country.year.country.id)
p_act$year<-c(country.year.year.year)

global.gen<- global.gen %>% left_join(p_act)

#
pdf(paste0("FILEPATH/Maps_predamusage_2001-2024_",lubridate::year(Sys.Date()), ".pdf"))

ggplot() + geom_sf(data = global.gen, aes(fill = p_act), col = NA) + theme_bw() +
  scale_fill_viridis_c() +
  labs(title = "Proportion of ACT usage:2001-2024", fill = "%ACTuse") +
  facet_wrap(~year)

dev.off()

# # Plot the spatial random effect
global.gen$sp <- res$summary.random$country.year.country.id$`0.5quant`[1:110]

pdf(paste0("FILEPATH/Maps_spatialfield_amusage_2001-2024_",lubridate::year(Sys.Date()), ".pdf"))

ggplot() + geom_sf(data = global.gen, aes(fill = sp), col = NA) + theme_bw() +
  scale_fill_viridis_c()+
  labs(title = "Spatial field", fill = "Spatial.re") +
  facet_wrap(~year)

dev.off()

#

# Calculate posterior probability that the field is larger than 0
threshold <- log(1)
exceed.prob <- lapply(X= res$marginals.random$country.year.country.id[1:110], FUN = function(x) inla.pmarginal(marginal = x, threshold))
exceed.prob <- 1 - unlist(exceed.prob)

global.gen$ex <- exceed.prob
temp.ex <-  global.gen[global.gen$ex >=0.80,]

pdf(paste0("FILEPATH/Maps_postprob_sp_greaterthan0_2001-2022_",lubridate::year(Sys.Date()), ".pdf"))

ggplot() + geom_sf(data = global.gen, aes(fill = ex), col = NA) + scale_fill_viridis_c() + 
  geom_sf(data = temp.ex, col = "red", fill = NA) + theme_bw() +
  facet_wrap(~year) 

dev.off()


#######################################################################################################
## STEP 3: RUNNING PUBLIC PRIVATE SPLITS
##############################################

####running split for AM usage
source('FILEPATH/1b.plotting+summarizing.usage.observed_2024_clean.R')

standardize_country_names <- function(data) {
  standardized_data <- data %>%
    mutate(location_name = case_when(
      location_name == "Bangladesh " ~ "Bangladesh",
      location_name == "Bolivia (Plurinational State of)" ~ "Bolivia",
      location_name == "Cabo Verde" ~ "Cape Verde",
      location_name %in% c("Côte d'Ivoire", "Ivory Coast", "CÃ´te d'Ivoire") ~ "Cote d'Ivoire",
      location_name == "Republic of Korea" ~ "North Korea",
      location_name == "Democratic People's Republic of Korea" ~ "South Korea",
      location_name == "Viet Nam" ~ "Vietnam",
      location_name == "Syrian Arab Republic" ~ "Syria",
      location_name == "Iran (Islamic Republic of)" ~ "Iran",
      location_name %in% c("Sao Tome and Principe", "Sao Tome and Principle") ~ "Sao Tome And Principe",
      location_name == "Venezuela (Bolivarian Republic of)" ~ "Venezuela",
      location_name %in% c("United Republic of Tanzania", "United Republic of Tanzania (Mainland)") ~ "Tanzania",
      location_name == "United Republic of Tanzania (Zanzibar)" ~ "Zanzibar",
      location_name %in% c("Gambia", "The Gambia") ~ "Gambia",
      location_name == "Guinea Bissau" ~ "Guinea-Bissau",
      location_name == "Indonesia " ~ "Indonesia",
      location_name %in% c("Kenya (Bungoma County)", "Kenya (Kakamega County)", "Kenya (Turkana County)") ~ "Kenya",
      location_name %in% c("Kyrgyz", "Kyrgyz Republic") ~ "Kyrgyzstan",
      location_name %in% c("Lao", "Lao PDR", "Lao People's Democratic Republic") ~ "Laos",
      location_name %in% c("Pakistan (Punjab)", "Pakistan Punjab", "Pakistan (Sindh)") ~ "Pakistan",
      location_name == "Senegal (Dakar)" ~ "Senegal",
      location_name == "Sierra" ~ "Sierra Leone",
      location_name %in% c("Sudan (south)", "South Sudan, Republic of") ~ "South Sudan",
      location_name == "Sudan (north)" ~ "Sudan",
      location_name == "Thailand Bangkok" ~ "Thailand",
      location_name %in% c("Democratic Republic of the Congo", "Democratic Republic of The Congo", "DRCongo", "Congo Democratic Republic", "Congo, Democratic Republic of the") ~ "Democratic Republic Of The Congo",
      location_name == "Swaziland" ~ "Eswatini",
      location_name %in% c("East Timor", "Democratic Republic of TimorLeste") ~ "Timor-Leste",
      location_name == "Congo" ~ "Republic Of Congo",
      location_name == "Türkiye" ~ "Turkey",
      TRUE ~ location_name
    ))
  
  needed_countries <- c(
    "Afghanistan", "Algeria", "Angola", "Argentina", "Armenia", "Azerbaijan", "Bangladesh", "Belize", "Benin", "Bhutan",
    "Bolivia", "Botswana", "Brazil", "Burkina Faso", "Burundi", "Cambodia", "Cameroon", "Cape Verde", "Central African Republic", 
    "Chad", "China", "Colombia", "Comoros", "Costa Rica", "Cote d'Ivoire", "Democratic Republic Of The Congo", "Djibouti", 
    "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Eswatini", "Ethiopia", 
    "French Guiana", "Gabon", "Gambia", "Georgia", "Ghana", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", 
    "Honduras", "India", "Indonesia", "Iran", "Iraq", "Kenya", "Kyrgyzstan", "Laos", "Liberia", "Madagascar", "Malawi", 
    "Malaysia", "Maldives", "Mali", "Mauritania", "Mayotte", "Mexico", "Morocco", "Mozambique", "Myanmar", "Namibia", 
    "Nepal", "Nicaragua", "Niger", "Nigeria", "North Korea", "Oman", "Pakistan", "Panama", "Papua New Guinea", "Paraguay", 
    "Peru", "Philippines", "Republic Of Congo", "Rwanda", "Sao Tome And Principe", "Saudi Arabia", "Senegal", "Sierra Leone", 
    "Solomon Islands", "Somalia", "South Africa", "South Korea", "South Sudan", "Sri Lanka", "Sudan", "Suriname", "Syria", 
    "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo", "Turkey", "Turkmenistan", "Uganda", "United Arab Emirates", 
    "Uzbekistan", "Vanuatu", "Venezuela", "Vietnam", "Yemen", "Zambia", "Zanzibar", "Zimbabwe"
  )
  
  filtered_data <- standardized_data %>%
    filter(location_name %in% needed_countries)
  
  return(filtered_data)
}


###################
tic()
#load splits 
countryyear.pbpv.ALLCATEGORIES <- read.csv('FILEPATH/sectors_drugs_20240619.csv') %>% dplyr::select(-c(surveyid, iso3, program)) %>% 
  standardize_country_names() %>% 
  filter(pbpv_sum >= 9, year>2000, !(location_name=="Senegal" & year %in% c(2018, 2020)),!(location_name=="Kenya" & year %in% c(2013, 2020)),!(location_name=="Malawi" & year==2019),!(location_name=="Mozambique" & year==2008),
         !(location_name=="Nigeria" & year==2016),!(location_name=="Burundi" & year==2016),!(location_name=="Pakistan" & year==2010), !(location_name=="Mali" & year==2015 & pbpv_sum==354.139),!(location_name=="Dominican Republic" & year==2013 & pbpv_sum==32), !(location_name=="Senegal" & year==2015 & pbpv_sum==21.329956), 
         !(location_name=="Togo" & year==2017 & pbpv_sum==200.52188),!(location_name=="Nigeria" & year==2021 & pbpv_sum==3025.3271),!(location_name=="Somalia" & year==2011 & pbpv_sum==10.771685), !(location_name=="Liberia" & year %in% c(2013, 2019)))

##indicate what the minimum is
sample_min <- 9
# Filter and summarize for Pakistan in specified years
pakistan_data <- countryyear.pbpv.ALLCATEGORIES %>%
  filter(location_name == "Pakistan" & year %in% c(2014, 2017, 2019) | location_name == "Dominican Republic" & year %in% c(2013)) %>%
  group_by(location_name, year, variable) %>%
  summarize(
    pbpv_sum = sum(pbpv_sum, na.rm = TRUE),
    n = sum(n, na.rm = TRUE),
    pbpv_prop = sum(n, na.rm = TRUE) / sum(pbpv_sum, na.rm = TRUE),
    .groups = 'drop'
  )

# Combine the summarized data back with the rest of the dataset
countryyear.pbpv.ALLCATEGORIES <- countryyear.pbpv.ALLCATEGORIES %>%
  filter(!(location_name == "Pakistan" & year %in% c(2014, 2017, 2019)) & !(location_name == "Dominican Republic" & year %in% c(2013))) %>%
  bind_rows(pakistan_data)

save(countryyear.pbpv.ALLCATEGORIES, file ="FILEPATH/countryyear.pbpv.ALLCATEGORIES.Rdata")


p.data0 <- countryyear.pbpv.ALLCATEGORIES %>% 
  rename(pbpv_cat=variable, p=pbpv_prop) %>% 
  filter(pbpv_cat %in% c("pb_act", "pb_nact", "pv_act", "pv_nact")) %>%
  group_by(year, pbpv_cat) %>%
  summarise(n = sum(n, na.rm = TRUE), .groups = 'drop') %>%  # Sum the counts for each year and category
  group_by(year) %>%
  mutate(p = n / sum(n, na.rm = TRUE)) %>%  # Calculate the proportion within each year
  ungroup()


pdf("FILEPATH/OverallTRTOptions_ALLCATEGORIES.geom.pdf")

ggplot(p.data0, aes(x=year, y=p, fill=pbpv_cat))+ geom_area() +
  labs(y="Proportion/AM used for Fever Treatment", x = "Years",fill = "Trt Option/Sector")
graphics.off
dev.off()

("FILEPATH/OverallTRTOptions_ALLCATEGORIES.bars.pdf")

ggplot(data=p.data0 ,aes(x=year,y =p, fill=pbpv_cat)) +
  geom_bar(stat = "identity",width=0.8,position="stack") + 
  labs(title = "", y="Proportion/AM used for Fever Treatment", x = "Years", fill = "AM Type/Sector") 

dev.off()

Data.act.obs <- countryyear.pbpv.ALLCATEGORIES
# Check one Country
lpp3 <- Data.act.obs %>% filter(location_name == "Niger" )

ggplot(lpp3, aes(x=year, y=pbpv_prop, fill=variable))+
  geom_bar(stat = "identity",width=0.8,position="stack") +
  labs(fill = "AM Type/Sector", y = "Probability", x = "Year", shape = "variable") +
  scale_fill_manual(values=c("#333333", "#CCCCCC","#336600", "#99FF66")) +
  theme_classic()


# Calculate the number of unique locations
unique_locations <- unique(Data.act.obs$location_name)
num_locations <- length(unique_locations)
locations_per_page <- 4  # 2 columns x 2 rows
num_pages <- ceiling(num_locations / locations_per_page)


pdf("FILEPATH/countryyear.pbpv.ALLCATEGORIES.pdf")

# for(i in 1:15){
for (i in 1:num_pages) {
  print(ggplot(Data.act.obs, aes(x=year, y=pbpv_prop, fill=variable))+
          geom_bar(stat = "identity",width=0.8,position="stack") +
          labs(fill = "AM Type/Sector", y = "Probability", x = "Year", shape = "Observed") +
          scale_fill_manual(values=c("#333333", "#CCCCCC","#336600", "#99FF66")) +
          theme_bw() +
          facet_wrap_paginate(~ location_name,ncol = 2, nrow = 2, page = i)
  )
  
}
dev.off()

## All categories Trend/Lines ####
Data.act.obs<- Data.act.obs %>% filter(!is.na(variable))


country.list <- unique(Data.act.obs$location_name)

pdf("FILEPATH/countryyear.pbpv.ALLCATEGORIES_Trend.pdf")

for(i in 1:length(country.list)){
  
  lpp3 <- Data.act.obs %>% filter(location_name == country.list[i])
  ymin <- min(lpp3$year)
  ymax <- max(lpp3$year)
  
  plot1 <- ggplot(lpp3, aes(x=year, y=pbpv_prop)) +
    geom_point(aes(x=year, y=pbpv_prop, shape = variable), position=position_stack(), show.legend = F) + 
    geom_line(aes(x=year, y=pbpv_prop, linetype = variable), position=position_stack(), show.legend = F) + 
    labs(linetype = "AM Type/Sector", shape = "AM Type/Sector", y = "Probability", x = "Year", title = country.list[i]) +
    theme_bw() +
    scale_x_continuous(limits = c(ymin, ymax), breaks = seq(ymin, ymax, 1)) +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
  
  plot2 <- ggplot(lpp3, aes(x=year, y=pbpv_prop)) +
    geom_point(aes(x=year, y=pbpv_prop, shape = variable)) + 
    geom_line(aes(x=year, y=pbpv_prop, linetype= variable)) + 
    labs(linetype = "AM Type/Sector", shape = "AM Type/Sector", y = "Probability", x = "Year", title = country.list[i]) +
    theme_bw() +
    scale_x_continuous(limits = c(ymin, ymax), breaks = seq(ymin, ymax, 1)) +
    scale_y_continuous(limits = c(0, 1), breaks = c(0, 0.25, 0.5, 0.75, 1)) +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
  
  print(grid.arrange(plot1, plot2, ncol = 1))
  
}

dev.off()


## Public/Private - Sectors Only ####
Data.obs.pbpv.sectorsonly <- read.csv('FILEPATH/sectors_only_20240619.csv') %>%  dplyr::select(-c(surveyid, iso3, program)) %>% 
  standardize_country_names() %>% 
  filter(pbpv_sum >= 9, year>2000, !(location_name=="Senegal" & year %in% c(2018, 2020)),!(location_name=="Kenya" & year %in% c(2013, 2020)),!(location_name=="Malawi" & year==2019),!(location_name=="Mozambique" & year==2008),
         !(location_name=="Nigeria" & year==2016),!(location_name=="Burundi" & year==2016),!(location_name=="Pakistan" & year==2010), !(location_name=="Mali" & year==2015 & pbpv_sum==354.139),!(location_name=="Dominican Republic" & year==2013 & pbpv_sum==32), !(location_name=="Senegal" & year==2015 & pbpv_sum==21.329956), 
         !(location_name=="Togo" & year==2017 & pbpv_sum==200.52188),!(location_name=="Nigeria" & year==2021 & pbpv_sum==3025.3271),!(location_name=="Somalia" & year==2011 & pbpv_sum==10.771685), !(location_name=="Liberia" & year %in% c(2013, 2019)))

# Filter and summarize for Pakistan in specified years
pakistan_data <- Data.obs.pbpv.sectorsonly %>%
  filter(location_name == "Pakistan" & year %in% c(2014, 2017, 2019) | location_name == "Dominican Republic" & year %in% c(2013)) %>%
  group_by(location_name, year, variable) %>%
  summarize(
    pbpv_sum = sum(pbpv_sum, na.rm = TRUE),
    n = sum(n, na.rm = TRUE),
    pbpv_prop = sum(n, na.rm = TRUE) / sum(pbpv_sum, na.rm = TRUE),
    .groups = 'drop'
  )

# Combine the summarized data back with the rest of the dataset
Data.obs.pbpv.sectorsonly <- Data.obs.pbpv.sectorsonly %>%
  filter(!(location_name == "Pakistan" & year %in% c(2014, 2017, 2019)) & !(location_name == "Dominican Republic" & year %in% c(2013))) %>%
  bind_rows(pakistan_data)

Data.act.obs<- Data.obs.pbpv.sectorsonly

save(Data.obs.pbpv.sectorsonly, file ="FILEPATH/Data.obs.pbpv.sectorsonly.Rdata")

# Check one Country
lpp3 <- Data.act.obs %>% filter(location_name == "Niger" )

ggplot(lpp3, aes(x=year, y=pbpv_prop, fill=variable))+
  geom_bar(stat = "identity",width=0.8,position = position_stack(reverse = TRUE)) +
  labs(fill = "Sector", y = "Probability", x = "Year", shape = "variable") +
  scale_fill_manual(values=c("#CCCCCC","#333333")) +
  theme_classic()


pdf("FILEPATH/countryyear.pbpv.SECTORS.pdf")

# for(i in 1:15){
for (i in 1:num_pages) {
  print(ggplot(Data.act.obs, aes(x=year, y=pbpv_prop, fill=variable))+
          geom_bar(stat = "identity",width=0.8,position = position_stack(reverse = TRUE)) +
          labs(fill = "Sector", y = "Probability", x = "Year", shape = "Observed") +
          scale_fill_manual(values=c("#CCCCCC","#333333")) +
          theme_bw() +
          facet_wrap_paginate(~ location_name,ncol = 2, nrow = 2, page = i)
  )
  
}
dev.off()


## ACT/nACT -  Drugs only ####

Data.obs.pbpv.drugsonly <- read.csv('FILEPATH/drugs_only_20240619.csv') %>% dplyr::select(-c(surveyid, iso3, program)) %>%  
  standardize_country_names() %>% 
  filter(pbpv_sum >= 9, year>2000, !(location_name=="Senegal" & year %in% c(2018, 2020)),!(location_name=="Kenya" & year %in% c(2013, 2020)),!(location_name=="Malawi" & year==2019),!(location_name=="Mozambique" & year==2008),
         !(location_name=="Nigeria" & year==2016),!(location_name=="Burundi" & year==2016),!(location_name=="Pakistan" & year==2010), !(location_name=="Mali" & year==2015 & pbpv_sum==354.139),!(location_name=="Dominican Republic" & year==2013 & pbpv_sum==32), !(location_name=="Senegal" & year==2015 & pbpv_sum==21.329956), 
         !(location_name=="Togo" & year==2017 & pbpv_sum==200.52188),!(location_name=="Nigeria" & year==2021 & pbpv_sum==3025.3271),!(location_name=="Somalia" & year==2011 & pbpv_sum==10.771685), !(location_name=="Liberia" & year %in% c(2013, 2019)))

# Filter and summarize for Pakistan in specified years
pakistan_data <- Data.obs.pbpv.drugsonly %>%
  filter(location_name == "Pakistan" & year %in% c(2014, 2017, 2019) | location_name == "Dominican Republic" & year %in% c(2013)) %>%
  group_by(location_name, year, variable) %>%
  summarize(
    pbpv_sum = sum(pbpv_sum, na.rm = TRUE),
    n = sum(n, na.rm = TRUE),
    pbpv_prop = sum(n, na.rm = TRUE) / sum(pbpv_sum, na.rm = TRUE),
    .groups = 'drop'
  )

# Combine the summarized data back with the rest of the dataset
Data.obs.pbpv.drugsonly <- Data.obs.pbpv.drugsonly %>%
  filter(!(location_name == "Pakistan" & year %in% c(2014, 2017, 2019)) & !(location_name == "Dominican Republic" & year %in% c(2013))) %>%
  bind_rows(pakistan_data)

Data.act.obs<- Data.obs.pbpv.drugsonly
save(Data.obs.pbpv.drugsonly, file ="FILEPATH/Data.obs.pbpv.drugsonly.Rdata")

# Check one Country
lpp3 <- Data.act.obs %>% filter(location_name == "Niger" )

ggplot(lpp3, aes(x=year, y=pbpv_prop, fill=variable))+
  geom_bar(stat = "identity",width=0.8,position = position_stack(reverse = TRUE)) +
  labs(fill = "AM Type", y = "Probability", x = "Year", shape = "variable") +
  scale_fill_manual(values=c("#CCCCCC", "#99FF66")) +
  theme_classic()

# Calculate the number of unique locations
unique_locations <- unique(Data.act.obs$location_name)
num_locations <- length(unique_locations)
locations_per_page <- 4  # 2 columns x 2 rows
num_pages <- ceiling(num_locations / locations_per_page)


pdf("FILEPATH/countryyear.pbpv.DRUGS.pdf")

for (i in 1:num_pages) {
  print(
    ggplot(Data.act.obs, aes(x=year, y=pbpv_prop, fill=variable)) +
      geom_bar(stat = "identity", width=0.8, position = position_stack(reverse = TRUE)) +
      labs(fill = "AM Type", y = "Probability", x = "Year", shape = "Observed") +
      scale_fill_manual(values=c("#CCCCCC", "#99FF66")) +
      theme_bw() +
      facet_wrap_paginate(~ location_name, ncol = 2, nrow = 2, page = i)
  )
}

dev.off()

############################

## ACT/nACT -  Drugs only PUBLIC ONLY ####
Data.act.obs<-countryyear.pbpv.ALLCATEGORIES %>% dplyr::select(location_name, year,pbpv_prop,n, "pbpv_cat"=variable) %>% 
  filter(pbpv_cat %in% c("pb_act","pb_nact"))

Data.act.obs$drug_cat <- ""

Data.act.obs$drug_cat[Data.act.obs$pbpv_cat == "pb_act"] <- "ACT"
Data.act.obs$drug_cat[Data.act.obs$pbpv_cat == "pb_nact"] <- "nACT"

Data.obs.public.drugsonly<-Data.act.obs %>% 
  group_by(location_name, year,drug_cat) %>%
  summarise(n = sum(n, na.rm = TRUE), .groups = 'drop') %>% 
  group_by(location_name, year) %>%
  mutate(pbpv_prop = n / sum(n, na.rm = TRUE)) %>%
  rename(variable =drug_cat) #%>% 


save(Data.obs.public.drugsonly, file ="FILEPATH/countryyear.PUBLIC.DRUGS.Rdata")

# Check one Country
lpp3 <- Data.obs.public.drugsonly %>% filter(location_name == "Niger" )

ggplot(lpp3, aes(x=year, y=pbpv_prop, fill=variable))+
  geom_bar(stat = "identity",width=0.8,position = position_stack(reverse = TRUE)) +
  labs(fill = "AM Sector", y = "Probability within public", x = "Year", shape = "variable") +
  scale_fill_manual(values=c("#CCCCCC", "#99FF66")) +
  theme_classic()

pdf("FILEPATH/countryyear.PUBLIC.DRUGS.pdf")

for (i in 1:num_pages) {
  print(ggplot(Data.obs.public.drugsonly, aes(x=year, y=pbpv_prop, fill=variable))+
          geom_bar(stat = "identity",width=0.8,position = position_stack(reverse = TRUE)) +
          labs(fill = "AM Type", y = "Probability within public", x = "Year", shape = "Observed") +
          scale_fill_manual(values=c("#CCCCCC", "#99FF66")) +
          theme_bw() +
          facet_wrap_paginate(~ location_name,ncol = 2, nrow = 2, page = i)
  )
  
}
dev.off()


## ACT/nACT -  Drugs only PRIVATE ONLY ####
Data.act.obs<-countryyear.pbpv.ALLCATEGORIES %>% dplyr::select(location_name, year,pbpv_prop,n, "pbpv_cat"=variable) %>% 
  filter(pbpv_cat %in% c("pv_act","pv_nact"))

Data.act.obs$drug_cat <- ""

Data.act.obs$drug_cat[Data.act.obs$pbpv_cat == "pv_act"] <- "ACT"
Data.act.obs$drug_cat[Data.act.obs$pbpv_cat == "pv_nact"] <- "nACT"

Data.obs.public.drugsonly<-Data.act.obs %>% 
  group_by(location_name, year,drug_cat) %>%
  summarise(n = sum(n, na.rm = TRUE), .groups = 'drop') %>% 
  group_by(location_name, year) %>%
  mutate(pbpv_prop = n / sum(n, na.rm = TRUE)) %>%
  rename(variable =drug_cat) #%>% 


save(Data.obs.public.drugsonly, file ="FILEPATH/countryyear.PRIVATE.DRUGS.Rdata")

# Check one Country
lpp3 <- Data.obs.public.drugsonly %>% filter(location_name == "Niger" )

ggplot(lpp3, aes(x=year, y=pbpv_prop, fill=variable))+
  geom_bar(stat = "identity",width=0.8,position = position_stack(reverse = TRUE)) +
  labs(fill = "AM Sector", y = "Probability within public", x = "Year", shape = "variable") +
  scale_fill_manual(values=c("#CCCCCC", "#99FF66")) +
  theme_classic()

pdf("FILEPATH/countryyear.PRIVATE.DRUGS.pdf")

for (i in 1:num_pages) {
  print(ggplot(Data.obs.public.drugsonly, aes(x=year, y=pbpv_prop, fill=variable))+
          geom_bar(stat = "identity",width=0.8,position = position_stack(reverse = TRUE)) +
          labs(fill = "AM Type", y = "Probability within private", x = "Year", shape = "Observed") +
          scale_fill_manual(values=c("#CCCCCC", "#99FF66")) +
          theme_bw() +
          facet_wrap_paginate(~ location_name,ncol = 2, nrow = 2, page = i)
  )
  
}
dev.off()

# source('FILEPATH/2.amusage_prepare_covariates_2024_v2.R')
# library(MASS)

load("FILEPATH/countryyear.pbpv.ALLCATEGORIES.Rdata")

############################################################

# Pivot the data to a wider format
am.data.wide <- countryyear.pbpv.ALLCATEGORIES %>% dplyr::select(-pbpv_prop) %>% 
  pivot_wider(
    names_from = variable,
    values_from = c(n),
    names_glue = "{variable}_{.value}"
  ) %>%
  rename(pb_act = pb_act_n,
         pb_nact = pb_nact_n,
         pv_act = pv_act_n,
         pv_nact = pv_nact_n)

# Read needed data newTS_oldCov and new AMD
am.commodity.data <- read.csv("FILEPATH/covariates_2024.csv") %>%
  dplyr::select(c(location_name, year, iso3, pop_at_risk_pf, impcourse1st.l, pop_hmis, actpop_pc, acthmis_pc, logpopn, trtseek))

# Load covariates
am.cov.data <- read.csv("FILEPATH/amusage_covariates_23.csv")

# Identify the maximum year in the data
max_year <- max(am.cov.data$year)
max_year_data <- am.cov.data %>% filter(year == max_year)

# Replicate the data for the years 2023 and 2024
new_data_2023 <- max_year_data %>% mutate(year = 2023)
new_data_2024 <- max_year_data %>% mutate(year = 2024)
am.cov.data <- bind_rows(am.cov.data, new_data_2023, new_data_2024)

# Adjust year_id
am.cov.data$year_id <- am.cov.data$year - 1990  # Assuming year_id is equivalent to year

# Merge covariates with commodity data
data.combined <- am.cov.data %>%
  left_join(am.commodity.data, by = c("location_name", "year"))

# # Inspect the resulting data.combined
# Merge with am.data.wide and transform
am.data.combined <- data.combined %>%
  left_join(am.data.wide, by = c('location_name', 'year')) %>%
  pivot_longer(cols = c(pb_act, pb_nact, pv_act, pv_nact),
               names_to = "pbpv_cat",
               values_to = "n") %>%
  group_by_all() %>%
  summarize(n = sum(n, na.rm = TRUE), .groups = 'drop') %>%
  mutate(pbpv_prop = n / pbpv_sum) %>%
  ungroup() %>%
  as.data.frame() 
save(am.data.combined, file = "FILEPATH/am.data.combined_excloutliers.Rdata")

## Covariates + standardizing + Truncate extreme values
cov.list <- c("trtseek","propurban","anc1cov","oopfrac","hcaccess","eduallagsx","sbacov","haqi","logthepc","actpop_pc","logpopn","actyears20", "univcover")

cov_matrix <- matrix(NA,nrow=nrow(am.data.combined),ncol=length(cov.list)+6)

cov_matrix[,1] <- am.data.combined$trtseek
cov_matrix[,2] <- am.data.combined$propurban
cov_matrix[,3] <- am.data.combined$anc1cov
cov_matrix[,4] <- am.data.combined$oopfrac
cov_matrix[,5] <- am.data.combined$hcaccess
cov_matrix[,6] <- am.data.combined$eduallagsx
cov_matrix[,7] <- am.data.combined$sbacov
cov_matrix[,8] <- am.data.combined$haqi
cov_matrix[,9] <- am.data.combined$logthepc
cov_matrix[,10] <- am.data.combined$actpop_pc
cov_matrix[,11] <- am.data.combined$logpopn
cov_matrix[,12] <- am.data.combined$actyears20
cov_matrix[,13] <- am.data.combined$univcover

cov_matrix_org <- cov_matrix[,1:13]
cov_matrix_org <- as.data.frame(cov_matrix_org)

colnames(cov_matrix_org) <- c("trtseek","propurban","anc1cov","oopfrac","hcaccess","eduallagsx","sbacov","haqi","logthepc","actpop_pc","logpopn","actyears20", "univcover")


pdf("FILEPATH/transformation_lambda.pdf")

for(i in 1:length(cov.list)) {
  boxcox((cov_matrix[,i][cov_matrix[,i]>0]) ~ 1) 
  mtext({cov.list[i]}, side=3)
}

dev.off()

# Transformation - based on selected "lambda"
cov_matrix_trans <- cov_matrix_org
cov_matrix_trans$trtseek.sqd <- (cov_matrix_org$trtseek)^2
cov_matrix_trans$propurban.sqr <- (cov_matrix_org$propurban)^0.5
cov_matrix_trans$anc1cov.sqd <- (cov_matrix_org$anc1cov)^2
cov_matrix_trans$hcaccess.sqd <- (cov_matrix_org$hcaccess)^0.5
cov_matrix_trans$sbacov2 <- (cov_matrix_org$sbacov)^1.5
cov_matrix_trans$haqi.log <- log(cov_matrix_org$haqi)
cov_matrix_trans$actpop_pc.log <- log(cov_matrix_org$actpop_pc + 0.0001)
cov_matrix_trans$actyears20.sqd <- (cov_matrix_org$actyears20)^2
cov_matrix_trans$univcover.sqd <- (cov_matrix_org$univcover)^0.5

# Convert to Standardize normal
standarize <- function(x){(x - mean(x))/sd(x)} 
cov_matrix_std <- cov_matrix_trans
n.cov<- dim(cov_matrix_std)[2]

#MARGIN=2 the function will be applied accross columns
cov_matrix_std[, 1:n.cov] <- apply(cov_matrix_std[, 1:n.cov], MARGIN = 2, FUN = function(x){(x - mean(x))/sd(x)}) 

cov_matrix_std[cov_matrix_std > 3]  <- 3
cov_matrix_std[cov_matrix_std < -3] <- -3

#check if the covariates normalised (all should have mean 0) and no NAs
summary(cov_matrix_std)

#ilogit <- function(x) {return(1/(1+exp(-x)))}

pdf("FILEPATH/Covariate_distribution.pdf")

par(mfrow = c(2, 2))

hist(cov_matrix_org$trtseek)
hist(cov_matrix_trans$trtseek.sqd)
hist(cov_matrix_std$trtseek)
hist(cov_matrix_std$trtseek.sqd)

hist(cov_matrix_org$propurban)
hist(cov_matrix_trans$propurban.sqr)
hist(cov_matrix_std$propurban)
hist(cov_matrix_std$propurban.sqr)

hist(cov_matrix_org$anc1cov)
hist(cov_matrix_trans$anc1cov.sqd)
hist(cov_matrix_std$anc1cov)
hist(cov_matrix_std$anc1cov.sqd)

hist(cov_matrix_org$hcaccess)
hist(cov_matrix_trans$hcaccess.sqd)
hist(cov_matrix_std$hcaccess)
hist(cov_matrix_std$hcaccess.sqd)

hist(cov_matrix_org$sbacov)
hist(cov_matrix_trans$sbacov2)
hist(cov_matrix_std$sbacov)
hist(cov_matrix_std$sbacov2)

hist(cov_matrix_org$haqi)
hist(cov_matrix_trans$haqi.log)
hist(cov_matrix_std$haqi)
hist(cov_matrix_std$haqi.log)

hist(cov_matrix_org$actpop_pc)
hist(cov_matrix_trans$actpop_pc.log)
hist(cov_matrix_std$actpop_pc)
hist(cov_matrix_std$actpop_pc.log)

hist(cov_matrix_org$actyears20)
hist(cov_matrix_trans$actyears20.sqd)
hist(cov_matrix_std$actyears20)
hist(cov_matrix_std$actyears20.sqd)

hist(cov_matrix_org$univcover)
hist(cov_matrix_trans$univcover.sqd)
hist(cov_matrix_std$univcover)
hist(cov_matrix_std$univcover.sqd)

hist(cov_matrix_org$oopfrac)
hist(cov_matrix_std$oopfrac)

hist(cov_matrix_org$eduallagsx)
hist(cov_matrix_std$eduallagsx)

hist(cov_matrix_org$logthepc)
hist(cov_matrix_std$logthepc)

hist(cov_matrix_org$logpopn)
hist(cov_matrix_std$logpopn)

dev.off()

par(mfrow = c(1, 1))

# Create Main Covariates matrix
cov_matrix <- matrix(NA,nrow=nrow(am.data.combined),ncol=length(cov.list)+n.cov+6)
cov_matrix <- as.data.frame(cov_matrix)

cov_matrix[,1:13] <- cov_matrix_org[,1:13]
cov_matrix[,14:35] <- cov_matrix_std[,1:22]

# Location information
cov_matrix[,36] <- am.data.combined$location_name
cov_matrix[,37] <- am.data.combined$year
cov_matrix[,38] <- am.data.combined$year_id
cov_matrix[,39] <- am.data.combined$who_region
cov_matrix[,40] <- am.data.combined$who_subregion
cov_matrix[,41] <- am.data.combined$is_act  
colnames(cov_matrix) <- c("trtseek","propurban","anc1cov","oopfrac","hcaccess","eduallagsx","sbacov","haqi","logthepc","actpop_pc","logpopn","actyears20", "univcover","trtseek.std","propurban.std","anc1cov.std","oopfrac.std","hcaccess.std","eduallagsx.std","sbacov.std","haqi.std","logthepc.std","actpop_pc.std","logpopn.std","actyears20.std","univcover.std","trtseek.sqd.std","propurban.sqr.std", "anc1cov.sqd.std","hcaccess.sqd.std","sbacov2.std","haqi.log.std","actpop_pc.log.std","actyears20.sqd.std","univcover.sqd.std","location_name","year","year_id","who_region" ,"who_subregion" ,"is_act")

cov_matrix <- cov_matrix %>%
  group_by(location_name, year, year_id, who_region, who_subregion, is_act) %>%
  summarize(across(everything(), ~ mean(.x, na.rm = TRUE)), .groups = 'drop')


# Unique Identifiers: 
# country.year.year.idx
cov_matrix$cy.yid <- cov_matrix$cy.yidx <- cov_matrix$cy.yidy <- cov_matrix$cy.yidz <- cov_matrix$cy.yidsp1 <- cov_matrix$year_id
cov_matrix$cy.yidx1 <- cov_matrix$year_id

# country.year.country.id
cov_matrix <- arrange(cov_matrix, location_name, year_id)
data.table::setDT(cov_matrix)[, cy.cid := .GRP, by = location_name]
cov_matrix$cy.cid <- cov_matrix$cy.cid #
cov_matrix$cy.cidsp1 <- cov_matrix$cy.cid

#country.year.whoregion.id
cov_matrix <- arrange(cov_matrix, who_region, year_id)
data.table::setDT(cov_matrix)[, cy.whoid := .GRP, by = who_region]
cov_matrix$cy.whoid <- cov_matrix$cy.whoid #


## Sub_regional Index and covariates/identifiers
#country.year.whosubregion.id
cov_matrix <- arrange(cov_matrix, who_subregion, year_id)
data.table::setDT(cov_matrix)[, cy.whosubid := .GRP, by = who_subregion]
cov_matrix$cy.whosubid <- cov_matrix$cy.whosubid #

cov_matrix$whosub.afro.c.cov <- ifelse(cov_matrix$who_subregion=="AFRO-C", 1,0)
cov_matrix$whosub.afro.e.cov <- ifelse(cov_matrix$who_subregion=="AFRO-E", 1,0)
cov_matrix$whosub.afro.s.cov <- ifelse(cov_matrix$who_subregion=="AFRO-S", 1,0)
cov_matrix$whosub.afro.w.cov <- ifelse(cov_matrix$who_subregion=="AFRO-W", 1,0)
cov_matrix$whosub.emro.cov <- ifelse(cov_matrix$who_subregion=="EMRO", 1,0)
cov_matrix$whosub.euro.cov <- ifelse(cov_matrix$who_subregion=="EURO", 1,0)
cov_matrix$whosub.paho.cov <- ifelse(cov_matrix$who_subregion=="PAHO", 1,0)
cov_matrix$whosub.searo.cov <- ifelse(cov_matrix$who_subregion=="SEARO", 1,0)
cov_matrix$whosub.wpro.cov <- ifelse(cov_matrix$who_subregion=="WPRO", 1,0)

save(cov_matrix, file = "FILEPATH/cov_matrix.Rdata")


# Correlation between variables
library(ggcorrplot)

#n.cov.all<- (dim(cov_matrix)[2])-6

png("FILEPATH/Correlation_matrix_covariates.png")

ggcorrplot(corr = cor(cov_matrix[,7:41]),
           type = "lower",
           ggtheme = ggplot2::theme_minimal,
           hc.order = FALSE,
           show.diag = FALSE,
           outline.col = "white",
           lab = F,
           legend.title = "Correlation",
           tl.cex = 11, tl.srt = 55)

dev.off()

# Expanded cov_matrix (by treatment categories)
Data.act.expanded0 <-am.data.combined
Data.act.expanded0$pbpv_cat[is.na(Data.act.expanded0$pbpv_cat)] <- "pb_act"
Data.act.expanded0 <- as.data.frame(Data.act.expanded0)
Data.act.expanded0 <- Data.act.expanded0 %>% tidyr::expand(location_name, year, pbpv_cat)

cov_matrix.exp <- cov_matrix %>% left_join(Data.act.expanded0, by=c("location_name","year"))

save(cov_matrix.exp, file = "FILEPATH/cov_matrix.exp.Rdata")

rm(Data.act.expanded0)

## Covariates + standardizing + Truncate extreme values
cov.list <- c("trtseek","propurban","anc1cov","oopfrac","hcaccess","eduallagsx","sbacov","haqi","logthepc","actpop_pc","logpopn","actyears20")

cov_matrix <- matrix(NA,nrow=nrow(am.data.combined),ncol=length(cov.list)+6)

cov_matrix[,1] <- am.data.combined$trtseek
cov_matrix[,2] <- am.data.combined$propurban
cov_matrix[,3] <- am.data.combined$anc1cov
cov_matrix[,4] <- am.data.combined$oopfrac
cov_matrix[,5] <- am.data.combined$hcaccess
cov_matrix[,6] <- am.data.combined$eduallagsx
cov_matrix[,7] <- am.data.combined$sbacov
cov_matrix[,8] <- am.data.combined$haqi
cov_matrix[,9] <- am.data.combined$logthepc
cov_matrix[,10] <- am.data.combined$actpop_pc
cov_matrix[,11] <- am.data.combined$logpopn
cov_matrix[,12] <- am.data.combined$actyears20

for(i in 1:length(cov.list)) {
  cov_matrix[,i]<- (cov_matrix[,i]-mean(cov_matrix[,i],na.rm=T))/sd(cov_matrix[,i],na.rm=T)
}
cov_matrix[cov_matrix > 3]  <- 3
cov_matrix[cov_matrix < -3] <- -3

cov_matrix <- as.data.frame(cov_matrix)

# Location information
cov_matrix[,13] <- am.data.combined$location_name
cov_matrix[,14] <- am.data.combined$year
cov_matrix[,15] <- am.data.combined$year_id
cov_matrix[,16] <- am.data.combined$who_region
cov_matrix[,17] <- am.data.combined$who_subregion
cov_matrix[,18] <- am.data.combined$is_act  

colnames(cov_matrix) <- c("trtseek","propurban","anc1cov","oopfrac","hcaccess","eduallagsx","sbacov","haqi","logthepc","actpop_pc","logpopn","actyears20","location_name","year","year_id","who_region" ,"who_subregion" ,"is_act")

# Collapsing to only location (plus), year
cov_matrix <- cov_matrix %>% 
  group_by(location_name,year,year_id,who_region,who_subregion,is_act) %>%
  summarize(trtseek = mean(trtseek),
            propurban = mean(propurban),
            anc1cov = mean(anc1cov),
            oopfrac = mean(oopfrac),
            hcaccess = mean(hcaccess),
            eduallagsx = mean(eduallagsx),
            sbacov = mean(sbacov),
            haqi = mean(haqi),
            logthepc = mean(logthepc),
            actpop_pc = mean(actpop_pc),
            logpopn = mean(logpopn),
            actyears20 = mean(actyears20)
  )

# Unique Identifiers: 
# country.year.year.idx
cov_matrix$cy.yid <- cov_matrix$cy.yidx <- cov_matrix$cy.yidy <- cov_matrix$cy.yidz <- cov_matrix$cy.yidsp1 <- cov_matrix$year_id
cov_matrix$cy.yidx1 <- cov_matrix$year_id

# country.year.country.id

cov_matrix <- arrange(cov_matrix, location_name, year_id)
data.table::setDT(cov_matrix)[, cy.cid := .GRP, by = location_name]
cov_matrix$cy.cid <- cov_matrix$cy.cid #
cov_matrix$cy.cidsp1 <- cov_matrix$cy.cid

#country.year.whoregion.id
cov_matrix <- arrange(cov_matrix, who_region, year_id)
data.table::setDT(cov_matrix)[, cy.whoid := .GRP, by = who_region]
cov_matrix$cy.whoid <- cov_matrix$cy.whoid #


## Sub_regional Index and covariates/identifiers
#country.year.whosubregion.id
cov_matrix <- arrange(cov_matrix, who_subregion, year_id)
data.table::setDT(cov_matrix)[, cy.whosubid := .GRP, by = who_subregion]
cov_matrix$cy.whosubid <- cov_matrix$cy.whosubid #

cov_matrix$whosub.afro.c.cov <- ifelse(cov_matrix$who_subregion=="AFRO-C", 1,0)
cov_matrix$whosub.afro.e.cov <- ifelse(cov_matrix$who_subregion=="AFRO-E", 1,0)
cov_matrix$whosub.afro.s.cov <- ifelse(cov_matrix$who_subregion=="AFRO-S", 1,0)
cov_matrix$whosub.afro.w.cov <- ifelse(cov_matrix$who_subregion=="AFRO-W", 1,0)
cov_matrix$whosub.emro.cov <- ifelse(cov_matrix$who_subregion=="EMRO", 1,0)
cov_matrix$whosub.euro.cov <- ifelse(cov_matrix$who_subregion=="EURO", 1,0)
cov_matrix$whosub.paho.cov <- ifelse(cov_matrix$who_subregion=="PAHO", 1,0)
cov_matrix$whosub.searo.cov <- ifelse(cov_matrix$who_subregion=="SEARO", 1,0)
cov_matrix$whosub.wpro.cov <- ifelse(cov_matrix$who_subregion=="WPRO", 1,0)


# Correlation between variables
library(ggcorrplot)

png("FILEPATH/Correlation_matrix_covariates2.png")

#plot.cor <- 
ggcorrplot(corr = cor(cov_matrix[,7:(length(cov.list)+6)]),
           type = "lower",
           ggtheme = ggplot2::theme_minimal,
           hc.order = FALSE,
           show.diag = FALSE,
           outline.col = "white",
           lab = TRUE,
           legend.title = "Correlation",
           tl.cex = 11, tl.srt = 55)

dev.off()

# Expanded cov_matrix (by treatment categories)
Data.act.expanded0 <-am.data.combined
Data.act.expanded0$pbpv_cat[is.na(Data.act.expanded0$pbpv_cat)] <- "pb_act"
Data.act.expanded0 <- as.data.frame(Data.act.expanded0)
Data.act.expanded0 <- Data.act.expanded0 %>% tidyr::expand(location_name, year, pbpv_cat)

cov_matrix.exp <- cov_matrix %>% left_join(Data.act.expanded0, by=c("location_name","year"))

cov_matrix.exp.ssa <- cov_matrix.exp %>% filter(location_name %in% c('Algeria', 'Angola','Benin','Botswana',	'Burkina Faso',	'Burundi',	'Cameroon',	'Cape Verde',	'Central African Republic',	'Chad',	'Comoros',"Cote d'Ivoire",'Democratic Republic Of The Congo',	'Djibouti',	'Egypt', 'Equatorial Guinea',	'Eritrea', 'Eswatini',	'Ethiopia',	'Gabon',	'Gambia',	'Ghana', 'Guinea',	'Guinea-Bissau','Kenya',	'Liberia',	'Madagascar',	'Malawi',	'Mali',	'Mauritania', 'Mayotte', 'Morocco',	'Mozambique',	'Namibia',	'Niger',	'Nigeria', 'Republic Of Congo', 'Rwanda',	'Sao Tome And Principe',	'Senegal',	'Sierra Leone', 'Somalia',	'South Africa',	'South Sudan',	'Sudan', 'Tanzania',	'Togo',	'Uganda', 'Zambia',	'Zimbabwe',	'Zanzibar'))


rm(Data.act.expanded0)

## Libraries
list.of.packages <- c("gstat","geoR","maptools","tidyverse","spdep", "sf", "ggplot2", "ggforce", "maps","sp","deldir","rgeos","mvtnorm","gridExtra","mlogit","reshape2")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

if(length(new.packages)) install.packages(new.packages)

# The main Model ####

load("FILEPATH/am.data.combined_excloutliers.Rdata")

load("FILEPATH/cov_matrix.Rdata")

# Group by location_name, year, and pbpv_cat, then calculate proportions
Data.grp <- am.data.combined %>%
  dplyr::select(location_name, year, yadopted, pbpv_cat, n) %>%
  group_by(location_name, year, yadopted, pbpv_cat) %>%
  summarise(n = sum(n, na.rm = TRUE), .groups = 'drop') %>%
  ungroup() %>% 
  group_by(location_name, year) %>%
  mutate(pbpv_prop = n / sum(n, na.rm = TRUE),
         total= sum(n, na.rm = TRUE)) %>%
  ungroup()

Data.grp <- Data.grp %>% 
  mutate(nodata = ifelse(is.na(pbpv_prop),"datano","datayes"))

# # Merge with the covariates
Data.grp <-Data.grp %>%
  left_join(cov_matrix, by=c("location_name","year")) # join with covariates

with(Data.grp, table(who_region, cy.whoid))
with(Data.grp, table(who_subregion, cy.whosubid))

# Revise WHO categorization for some countries

Data.grp$cy.whosubid[Data.grp$location_name == "Algeria" & Data.grp$cy.whosubid == 4] <- 5
Data.grp$cy.whosubid[Data.grp$location_name == "Burundi" & Data.grp$cy.whosubid == 4] <- 2


Data.grp$pbpv_cat <- as.factor(Data.grp$pbpv_cat)
Data.grp$pbpv_cat <- relevel(Data.grp$pbpv_cat, ref = "pv_nact") # set reference group
Data.grp$country.id <- as.factor(Data.grp$location_name) # for random effects
Data.grp$subreg.fc <- as.factor(Data.grp$cy.whosubid) # 

# revise year id
Data.grp <- Data.grp %>% group_by(location_name) %>% arrange(year)
data.table::setDT(Data.grp)[, year_id := .GRP, by = year]
Data.grp$year_id <- Data.grp$year_id #


# country.year ID
Data.grp <- Data.grp %>% group_by(location_name) %>% arrange(year)
data.table::setDT(Data.grp)[, country.year.id := .GRP, by = c("location_name","year")]
Data.grp$country.year.id <- Data.grp$country.year.id #
Data.grp$country.year.id.fc <- as.factor(Data.grp$country.year.id) # 

## Model 1: 
# Full data set
mblogit.grp.re<- mblogit(pbpv_cat ~ is_act + trtseek.std + eduallagsx.std + propurban.std + haqi.log.std+propurban.sqr.std +
                           year_id+ factor(country.id) +
                           sbacov2.std + logpopn.std + univcover.sqd.std ,
                         weights = n, data=Data.grp, 
                         groups = ~ country.id+ cy.whosubid + cy.whoid,
                         random=~1|country.id)

summary(mblogit.grp.re)

# Removed + actyears20
if (sample_min == 50) {
  save(mblogit.grp.re, file = "FILEPATH/outputs.multi.mclogit1.cov+re_nooutliersn50+country.Rdata")
} else {
  save(mblogit.grp.re, file = "FILEPATH/outputs.multi.mclogit1.cov+re_nooutliersn09+country.Rdata")
}

summary(mblogit.grp.re)

# Predicting
# no NEW data
table.mblogit.re <-predict(mblogit.grp.re, type = "response")
table.mblogit.re <- as.data.frame(table.mblogit.re)

head(table.mblogit.re)

# Prediction Table
Data.pred <- Data.grp %>% dplyr::select(location_name, year, yadopted, who_region, who_subregion, is_act) 
Data.pred <- cbind(Data.pred, table.mblogit.re)

if (sample_min == 50) {
  save(Data.pred, file = "FILEPATH/predictedvalues.multi.mclogit1.cov+re_nooutliersn50+country.Rdata")
} else {
  save(Data.pred, file = "FILEPATH/predictedvalues.multi.mclogit1.cov+re_nooutliersn09+country.Rdata")
  }
## Plotting ####
if (sample_min == 50) {
  load("FILEPATH/predictedvalues.multi.mclogit1.cov+re_nooutliersn50+country.Rdata")
} else {
  load("FILEPATH/predictedvalues.multi.mclogit1.cov+re_nooutliersn09+country.Rdata")
}

# Summarize the data
lpp <- Data.pred %>%
  group_by(location_name, year) %>%
  summarise(
    pb_act = mean(pb_act, na.rm = TRUE),
    pb_nact = mean(pb_nact, na.rm = TRUE),
    pv_act = mean(pv_act, na.rm = TRUE),
    pv_nact = mean(pv_nact, na.rm = TRUE),
    .groups = 'drop'  # Remove grouping after summarizing
  )

# Pivot longer instead of using melt
lpp2 <- lpp %>%
  pivot_longer(
    cols = c(pb_act, pb_nact, pv_act, pv_nact),
    names_to = "variable",
    values_to = "probability"
  )


rm(Data.obs.pbpv.sectorsdrugs)
load("FILEPATH/countryyear.pbpv.ALLCATEGORIES.Rdata")

Data.obs.pbpv.sectorsdrugs <- countryyear.pbpv.ALLCATEGORIES

lpp2 <- lpp2 %>%
  left_join(Data.obs.pbpv.sectorsdrugs, by=c("location_name","year","variable"))


# Check single country 
par(mfrow = c(2,2))
ggplot(lpp2 %>% filter(location_name == "Niger"), aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title ="Niger" )

ggplot(lpp2 %>% filter(location_name == "Senegal" ), aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title = "Senegal" )

ggplot(lpp2 %>% filter(location_name == "Cameroon" ), aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title ="Cameroon" )

ggplot(lpp2 %>% filter(location_name == "Tanzania" ), aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title ="Tanzania" )

ggplot(lpp2 %>% filter(location_name == "Nigeria" ), aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title ="Nigeria" )

ggplot(lpp2 %>% filter(location_name == "Bangladesh" ), aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title ="Bangladesh" )


# Run 5a.modelfit_assessment

# Country-year plots
c.list <- unique(lpp2$location_name)

# Files
pdf("FILEPATH/countryyear.multi.mclogit+obs_sectorsdrugs.pdf")

for(i in 1:length(c.list)){
  
  print(ggplot(lpp2, aes(x=year, y=probability, fill=variable))+ geom_area() +
          geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
          labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "Observed") +
          scale_fill_manual(values=c("#333333", "#CCCCCC","#336600", "#99FF66")) +
          facet_wrap_paginate(~ location_name,ncol = 1, nrow = 1, page = i)
  )
  
}
dev.off()


## Public/Private - Sectors Only

lpp<-Data.pred %>%
  group_by(location_name, year) %>%
  summarise(pb_act = mean(pb_act),
            pb_nact = mean(pb_nact), 
            pv_act = mean(pv_act),
            pv_nact = mean(pv_nact)
  ) %>%  rowwise() %>% 
  mutate(pb = sum(pb_act,pb_nact),
         pv = sum(pv_act, pv_nact))  %>% 
  group_by(location_name, year) %>%
  summarise(Public = mean(pb),
            Private = mean(pv)
  ) 

# lpp2 <- melt(lpp, id.vars = c('location_name', 'year'), value.name = "probability") 
# Pivot longer instead of using melt
lpp2 <- lpp %>% pivot_longer( cols = c(Public,Private), names_to = "variable", values_to = "probability")


rm(Data.obs.pbpv.sectorsonly)
load("FILEPATH/Data.obs.pbpv.sectorsonly.Rdata")

lpp2 <- lpp2 %>%
  left_join(Data.obs.pbpv.sectorsonly, by=c("location_name","year","variable"))

lpp2.sector <- lpp2 

# Check single country 
lpp3 <- lpp2 %>% filter(location_name == "Ghana" )

ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
  labs(fill = "AM Sector", y = "Probability", x = "Year", shape = "variable")


## ACT/nACT -  Drugs only 

lpp<-Data.pred %>% rowwise() %>% 
  mutate(act = sum(pb_act,pv_act),
         nact = sum(pb_nact, pv_nact)) %>% 
  group_by(location_name, year) %>%
  summarise(ACT = mean(act),
            nACT = mean(nact)
  ) 


# lpp2 <- melt(lpp, id.vars = c('location_name', 'year'), value.name = "probability") 
lpp2 <- lpp %>% pivot_longer( cols = c(ACT,nACT), names_to = "variable", values_to = "probability")

rm(Data.obs.pbpv.drugsonly)
load("FILEPATH/Data.obs.pbpv.drugsonly.Rdata")

lpp2 <- lpp2 %>%
  left_join(Data.obs.pbpv.drugsonly, by=c("location_name","year","variable"))

lpp2.drugs <- lpp2 

# Check single country 
lpp3 <- lpp2 %>% filter(location_name == "Uganda" )

ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
  labs(fill = "AM Type", y = "Probability", x = "Year", shape = "variable")

# Combined = Drugs and Sector
library(patchwork)
library(gridExtra)
lpp3.sector <- lpp2.sector %>% filter(location_name == "Uganda" )
lpp3.drugs <- lpp2.drugs %>% filter(location_name == "Uganda" )

(ggplot(lpp3.drugs, aes(x=year, y=probability, fill=variable))+ geom_area() +
    geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
    labs(fill = "AM Type", y = "Probability", x = "Year", shape = "variable"))+
  
  (ggplot(lpp3.sector, aes(x=year, y=probability, fill=variable))+ geom_area() +
     geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 1, position=position_stack()) +
     labs(fill = "Source", y = "Probability", x = "Year"))  
# 
library(ggforce)  # For facet_wrap_paginate
library(dplyr)

pdf("FILEPATH/countryyear.multi.mclogit+obs_sectorsdrugs_separated.pdf")

# Calculate the number of pages needed
num_pages_drugs <- ceiling(n_distinct(lpp2.drugs$location_name) / 2)
num_pages_sector <- ceiling(n_distinct(lpp2.sector$location_name) / 2)
num_pages <- max(num_pages_drugs, num_pages_sector)

for (i in 1:num_pages) {
  print(ggplot(lpp2.drugs, aes(x = year, y = probability, fill = variable)) +
          geom_area() +
          # geom_area(alpha = 1/12) +  
          stat_smooth(geom = 'area', method = 'loess', span = 1/2, alpha = 1/12) +
          geom_point(aes(x = year, y = pbpv_prop, shape = variable), size = 1, position = position_stack()) +
          labs(fill = "AM Type", y = "Probability", x = "Year", shape = "Observed") +
          scale_fill_manual(values = c("#CCCCCC", "#99FF66")) +
          facet_wrap_paginate(~ location_name, ncol = 1, nrow = 2, page = i) +
          
          ggplot(lpp2.sector, aes(x = year, y = probability, fill = variable)) +
          geom_area() +
          # stat_smooth(geom = 'area', method = 'loess', span = 1/2, alpha = 1/12) +
          geom_point(aes(x = year, y = pbpv_prop, shape = variable), size = 1, position = position_stack()) +
          labs(fill = "AM Sector", y = "Probability", x = "Year", shape = "Observed") +
          scale_fill_manual(values = c("#CCCCCC", "#333333")) +
          facet_wrap_paginate(~ location_name, ncol = 1, nrow = 2, page = i)
  )
}

dev.off()

# Overall trend
lpp<-Data.pred %>%
  group_by(year) %>%
  summarise(pb_act = mean(pb_act),
            pb_nact = mean(pb_nact), 
            pv_act = mean(pv_act),
            pv_nact = mean(pv_nact)
  ) 

# lpp2 <- melt(lpp, id.vars = c('year'), value.name = "probability")
lpp2 <- lpp %>% pivot_longer(cols = c(pb_act, pb_nact, pv_act, pv_nact), names_to = "variable", values_to = "probability")

pdf("FILEPATH/Overall.trend.multi.mclogit.pdf")

ggplot(lpp2, aes(x=year, y=probability, fill=variable))+ geom_area() +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year")+
  scale_fill_manual(values=c("#333333", "#CCCCCC","#336600", "#99FF66")) 

dev.off()

# Set the output to a PNG file with specified dimensions and resolution
png('FILEPATH/Globaltrend_actusage_pub_priv.png', width=2029, height=1800, units = "px", res = 300)

# Create the plot
plot1 <- ggplot(lpp2, aes(x = year, y = probability, fill = variable)) +
  geom_area(show.legend = FALSE) +
  labs(title = "(A) Global", fill = "Trt Option/Source", y = "Probability of treatment option", x = "Year") +
  scale_fill_manual(values = c("#333333", "#CCCCCC", "#336600", "#99FF66"))

# Print the plot to the PNG device
print(plot1)

# Close the PNG device
dev.off()


# Trend by ACT 1st and Not
lpp<-Data.pred %>%
  group_by(year, is_act) %>%
  summarise(pb_act = mean(pb_act),
            pb_nact = mean(pb_nact), 
            pv_act = mean(pv_act),
            pv_nact = mean(pv_nact)
  ) 

lpp$is_act <- as.character(lpp$is_act)


# lpp2 <- melt(lpp, id.vars = c('year'), value.name = "probability")
lpp2 <- lpp %>% pivot_longer(cols = c(pb_act, pb_nact, pv_act, pv_nact), names_to = "variable", values_to = "probability")%>% 
  mutate(is_act = recode(is_act, "0" = "1stLine:NonACT", "1" = "1stLine:ACT"))

pdf("FILEPATH/Firstline.trend.multi.mclogit.pdf")

ggplot(lpp2, aes(x=year, y=probability, fill=variable))+ geom_area() +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year") +
  scale_fill_manual(values=c("#333333", "#CCCCCC","#336600", "#99FF66")) +
  facet_wrap(~is_act)

dev.off()

# 
lpp4 <- lpp %>% pivot_longer(cols = c(pb_act, pb_nact, pv_act, pv_nact), names_to = "variable", values_to = "probability")%>% 
  mutate(is_act = recode(is_act, "0" = "NonACT", "1" = "ACT"))


png('FILEPATH/Firstlinetrend_actusage_pub_priv.png', width=2029, height=1800, units = "px", res = 300)

plot2 <- ggplot(lpp4, aes(x=year, y=probability, fill=variable))+ geom_area(show.legend = FALSE) +
  labs(title ="(B) 1stline Treatment Policy",fill = "Trt Option/Source", x = "Year") +
  scale_fill_manual(values=c("#333333", "#CCCCCC","#336600", "#99FF66")) +
  facet_wrap(~is_act)+
  theme(axis.text.y=element_blank(),axis.title.y=element_blank())

plot2
dev.off()


# Trend by WHO Sub-Regions
lpp<-Data.pred %>%
  group_by(who_subregion, year) %>%
  summarise(pb_act = mean(pb_act),
            pb_nact = mean(pb_nact), 
            pv_act = mean(pv_act),
            pv_nact = mean(pv_nact)
  ) 

# lpp2 <- melt(lpp, id.vars = c('year', 'who_subregion'), value.name = "probability") 
lpp2 <- lpp %>% pivot_longer(cols = c(pb_act, pb_nact, pv_act, pv_nact), names_to = "variable", values_to = "probability")

pdf("FILEPATH/WHOSubregions.trend.multi.mclogit.pdf")

ggplot(lpp2, aes(x=year, y=probability, fill=variable))+ geom_area() +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year") +
  scale_fill_manual(values=c("#333333", "#CCCCCC","#336600", "#99FF66")) +
  facet_wrap(~who_subregion)

dev.off()


png('FILEPATH/HOSubregionstrend_actusage_pub_priv.png', width=2029, height=1800, units = "px", res = 300)

plot3 <- ggplot(lpp2, aes(x=year, y=probability, fill=variable))+ geom_area() +
  labs(title ="(C) WHO Subregions",fill = "Trt Option/Source", y = "Probability of treatment option", x = "Year") +
  scale_fill_manual(values=c("#333333", "#CCCCCC","#336600", "#99FF66")) +
  facet_wrap(~who_subregion)
plot3
dev.off()


# Merge the plots
png('FILEPATH/Global+Policy+WHOSubregionstrend_actusage_pub_priv.png', width=2029, height=1800, res = 300)

plot_all <- (plot1 + plot2)/plot3

plot_all
dev.off()

## Making Output Tables 
country.year.output.table <- Data.pred %>% dplyr::select(location_name, year, yadopted, pb_act,pb_nact, pv_act, pv_nact) %>%
  # left_join((am.data.combined %>% dplyr::select(location_name, year, yadopted)), by= c('location_name', 'year')) %>% 
  group_by(location_name, year, yadopted) %>% 
  summarise(public_act = mean(pb_act),
            public_nact = mean(pb_nact), 
            private_act = mean(pv_act),
            private_nact = mean(pv_nact)
  )

write.csv(country.year.output.table, "FILEPATH/country.year.output.table.csv")
write.csv(country.year.output.table, "FILEPATH/countryyear.actuse.publicprivate.csv")
write.csv(country.year.output.table, "FILEPATH/countryyear.actuse.publicprivate.csv")


#source('FILEPATH/4a.compare_obs_vs_modelled_all categories.R')
library(dplyr)
library(ggplot2)

## All categories Trend/Lines ####
Data.act.obs#<- countryyear.pbpv.ALLCATEGORIES

# Step 1: Remove duplicates
Data.pred <- Data.pred %>% distinct()

# Step 2: Group by the necessary columns and summarize using across
Data.pred_v1 <- Data.pred %>%
  group_by(location_name, year, who_region, who_subregion, is_act) %>%
  summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE)), .groups = 'drop')

# Step 3: Ensure the final data is unique
Data.pred_v1 <- Data.pred_v1 %>% distinct()

# Step 4: Pivot longer to get the desired format
Data.pred_v1_long <- Data.pred_v1 %>%
  pivot_longer(cols = c(pv_nact, pb_act, pb_nact, pv_act),
               names_to = 'variable',
               values_to = 'pbpv_prop')

# Step 5: Ensure the final data is unique again
Data.pred_v1_long <- Data.pred_v1_long %>% distinct()

# Check the result
print(Data.pred_v1_long)

country.list <- unique(Data.pred_v1_long$location_name)

pdf("FILEPATH/modelled_countryyear.pbpv.ALLCATEGORIES_Trend_v3.pdf")

for(i in seq_along(country.list)) {
  
  lpp3 <- Data.pred_v1_long %>% filter(location_name == country.list[i])
  obs_data <- Data.act.obs %>% filter(location_name == country.list[i])
  yadopted <- Data.pred_v1_long %>% filter(location_name == country.list[i]) %>% distinct(yadopted)
  if(nrow(lpp3) == 0) next  # Skip if no predicted data for the country
  
  ymin <- min(lpp3$year, na.rm = TRUE)
  ymax <- max(lpp3$year, na.rm = TRUE)
  
  plot1 <- ggplot(lpp3, aes(x = year, y = pbpv_prop)) +
    geom_point(aes(shape = variable), position = position_stack(), show.legend = F) + 
    geom_point(data = obs_data, aes(color = variable, shape = variable), position = position_stack(), show.legend = F) + 
    geom_line(aes(linetype = variable), position = position_stack(), show.legend = F) +
    geom_vline(aes(xintercept = as.numeric(yadopted)), linetype = "dashed", color = "green") +
    labs(linetype = "AM Type/Sector", shape = "AM Type/Sector", y = "Probability", x = "Year", title = country.list[i]) +
    theme_bw() +
    scale_x_continuous(limits = c(ymin, ymax), breaks = seq(ymin, ymax, 1)) +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))
  
  plot2 <- ggplot(lpp3, aes(x = year, y = pbpv_prop)) +
    geom_point(data = obs_data, aes(color = variable, shape = variable)) +
    geom_point(aes(shape = variable)) + 
    geom_line(aes(linetype = variable)) + 
    geom_vline(aes(xintercept = as.numeric(yadopted)), linetype = "dashed", color = "green") +
    labs(linetype = "AM Type/Sector", shape = "AM Type/Sector", y = "Probability", x = "Year", title = country.list[i]) +
    theme_bw() +
    scale_x_continuous(limits = c(ymin, ymax), breaks = seq(ymin, ymax, 1)) +
    scale_y_continuous(limits = c(0, 1), breaks = seq(0, 1, by = 0.25)) +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))
  
  print(grid.arrange(plot1, plot2, ncol = 1))
}

dev.off()

# Check the result
Data.pred_v1_long2 <- Data.pred_v1_long %>% filter(year>2000)

pdf("FILEPATH/modelled_countryyear.pbpv.ALLCATEGORIES_Trend_2001-2024_v3.pdf")

for(i in seq_along(country.list)) {
  
  lpp3 <- Data.pred_v1_long2 %>% filter(location_name == country.list[i])
  obs_data <- Data.act.obs %>% filter(location_name == country.list[i])
  yadopted <- Data.pred_v1_long2 %>% filter(location_name == country.list[i]) %>% distinct(yadopted)
  
  if(nrow(lpp3) == 0) next  # Skip if no predicted data for the country
  
  ymin <- min(lpp3$year, na.rm = TRUE)
  ymax <- max(lpp3$year, na.rm = TRUE)
  
  plot1 <- ggplot(lpp3, aes(x = year, y = pbpv_prop)) +
    geom_point(aes(shape = variable), position = position_stack(), show.legend = F) + 
    geom_point(data = obs_data, aes(color = variable, shape = variable), position = position_stack(), show.legend = F) + 
    geom_line(aes(linetype = variable), position = position_stack(), show.legend = F) +
    geom_vline(aes(xintercept = as.numeric(yadopted)), linetype = "dashed", color = "green") +
    labs(linetype = "AM Type/Sector", shape = "AM Type/Sector", y = "Probability", x = "Year", title = country.list[i]) +
    theme_bw() +
    scale_x_continuous(limits = c(ymin, ymax), breaks = seq(ymin, ymax, 1)) +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))
  
  plot2 <- ggplot(lpp3, aes(x = year, y = pbpv_prop)) +
    geom_point(data = obs_data, aes(color = variable, shape = variable)) +
    geom_point(aes(shape = variable)) + 
    geom_line(aes(linetype = variable)) + 
    geom_vline(aes(xintercept = as.numeric(yadopted)), linetype = "dashed", color = "green") +
    labs(linetype = "AM Type/Sector", shape = "AM Type/Sector", y = "Probability", x = "Year", title = country.list[i]) +
    theme_bw() +
    scale_x_continuous(limits = c(ymin, ymax), breaks = seq(ymin, ymax, 1)) +
    scale_y_continuous(limits = c(0, 1), breaks = seq(0, 1, by = 0.25)) +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))
  
  print(grid.arrange(plot1,plot2, ncol = 1))
}

dev.off()

lpp.plot<-Data.pred %>%
  group_by(location_name, year) %>%
  summarise(pb_act = mean(pb_act),
            pb_nact = mean(pb_nact), 
            pv_act = mean(pv_act),
            pv_nact = mean(pv_nact)
  ) 

lpp2.plot <- lpp.plot %>% pivot_longer(cols = c(pb_act, pb_nact, pv_act, pv_nact), names_to = "variable", values_to = "probability")

load("FILEPATH/countryyear.pbpv.ALLCATEGORIES.Rdata")
Data.obs.pbpv.sectorsdrugs <- countryyear.pbpv.ALLCATEGORIES

lpp2.plot <- lpp2.plot %>%
  left_join(Data.obs.pbpv.sectorsdrugs, by=c("location_name","year","variable")) %>% filter(!is.na(n)) %>% dplyr::select(location_name,year,variable, probability,pbpv_prop) 

## Sector 
lpp.sector<-Data.pred %>%
  group_by(location_name, year) %>% #rowwise() %>% 
  summarise(Public= sum(pb_act,pb_nact),
            Private = sum(pv_act,pv_nact)
  ) 

# lpp2.plot.sector <- melt(lpp.sector, id.vars = c('location_name', 'year'), value.name = "probability") 
lpp2.plot.sector <- lpp.sector %>% 
  pivot_longer(cols = c(Public, Private), names_to = "variable", values_to = "probability")

rm(Data.obs.pbpv.sectorsonly)
load("FILEPATH/Data.obs.pbpv.sectorsonly.Rdata")

lpp2.plot.sector <- lpp2.plot.sector %>%
  left_join(Data.obs.pbpv.sectorsonly, by=c("location_name","year","variable")) %>% 
  filter(!is.na(n)) %>% 
  dplyr::select(location_name, year, variable, probability, pbpv_prop) 

lpp.sector<-Data.pred %>%
  group_by(year) %>% rowwise() %>% 
  summarise(Public= sum(pb_act,pb_nact),
            Private = sum(pv_act,pv_nact)
  ) 

lpp.sector<-lpp.sector %>%
  group_by(year) %>% 
  # group_by(year) %>% rowwise() %>%
  summarise(Public= mean(Public),
            Private = mean(Private)
  ) 

lpp2.plot.sector3 <- lpp.sector %>% pivot_longer(cols = c(Public, Private), names_to = "variable", values_to = "probability")

# Drugs
lpp.drugs<-Data.pred %>%
  group_by(location_name, year) %>% rowwise() %>% 
  summarise(ACT= sum(pb_act,pv_act),
            nACT = sum(pb_nact,pv_nact)
  ) 

lpp2.plot.drugs <- lpp.drugs %>% pivot_longer(cols = c(ACT, nACT), names_to = "variable", values_to = "probability")


rm(Data.obs.pbpv.drugsonly)
load("FILEPATH/Data.obs.pbpv.drugsonly.Rdata")

lpp2.plot.drugs <- lpp2.plot.drugs %>%
  left_join(Data.obs.pbpv.drugsonly, by=c("location_name","year","variable")) %>% filter(!is.na(n)) %>% dplyr::select(location_name,year,variable, probability,pbpv_prop) 

# Scatter Observed vs. Predicted
library(patchwork)

library(dplyr) 
library(devtools)
library(ggpubr)

cor(lpp2.plot$probability,lpp2.plot$pbpv_prop )
cor(lpp2.plot.drugs$probability,lpp2.plot.drugs$pbpv_prop )
cor(lpp2.plot.sector$probability,lpp2.plot.sector$pbpv_prop )

## Split by categories
pdf("FILEPATH/obs_pred.mclogit_multi_SPLIT.SECTORS+DRUGS.pdf")


ggplot(lpp2.plot, aes(pbpv_prop,probability)) +
  geom_point(alpha=0.5, size=2) + 
  geom_smooth(method = "lm", se=FALSE) +
  stat_regline_equation(label.y = 1, aes(label = ..eq.label..)) +
  stat_regline_equation(label.y = 0.9, aes(label = ..rr.label..))+
  labs(x = "Observed",y = "Predicted", title = "Model = Multinomial")+ facet_wrap(~variable)

dev.off()

# Color coded  
pdf("FILEPATH/obs_pred.mclogit_multi.SECTORS+DRUGS_colour.pdf")

(ggplot(lpp2.plot, aes(pbpv_prop,probability, color=variable)) +
    geom_point(alpha=0.5, size=2) +  geom_abline(size = 1.0) +
    labs(x = "Observed",y = "Predicted",title = "Drugs+Sectors \nModel = Multinomial")) /
  
  (ggplot(lpp2.plot.sector, aes(pbpv_prop,probability, color=variable)) +
     geom_point(alpha=0.5, size=2) + 
     geom_smooth(method = "lm", se=FALSE) +
     stat_regline_equation(label.y = 1, aes(label = ..rr.label..))+
     labs(x = "Observed",y = "Predicted",title = "Sectors")+ facet_wrap(~variable, ncol = 2)) / 
  
  (ggplot(lpp2.plot.drugs, aes(pbpv_prop,probability, color=variable)) +
     geom_point(alpha=0.5, size=2) + 
     geom_smooth(method = "lm", se=FALSE) +
     stat_regline_equation(label.y = 1, aes(label = ..rr.label..))+
     labs(x = "Observed",y = "Predicted",title = "Drugs")+ facet_wrap(~variable, ncol = 2))

dev.off()


## Country
n.countries <- unique(lpp2.plot.drugs$location_name)
n.pages <- ceiling(length(n.countries) / 3)
pdf("FILEPATH/countrypoints.obs_pred.SECTORS+DRUGS.pdf")

for (i in 1:n.pages) {
  
  print(ggplot(lpp2.plot.drugs, aes(pbpv_prop,probability, color=variable)) +
          geom_point(alpha=0.5, size=2) + 
          geom_abline(size = 1.0) +
          labs(x = "Observed",y = "Predicted") +
          facet_wrap_paginate(~ location_name,ncol = 1, nrow = 3, page = i)
        +
          
          ggplot(lpp2.plot.sector, aes(pbpv_prop,probability, color=variable)) +
          geom_point(alpha=0.5, size=2) + 
          geom_abline(size = 1.0) +
          labs(x = "Observed",y = "Predicted") +
          facet_wrap_paginate(~ location_name,ncol = 1, nrow = 3, page = i)
          )
  
}
dev.off()


## Plot by WHO regions
# All categories

lpp<-Data.pred %>%
  group_by(who_subregion, location_name, year) %>%
  summarise(pb_act = mean(pb_act),
            pb_nact = mean(pb_nact), 
            pv_act = mean(pv_act),
            pv_nact = mean(pv_nact)
  ) 

# lpp2 <- melt(lpp, id.vars = c('who_subregion','location_name', 'year'), value.name = "probability") 
lpp2 <- lpp %>% pivot_longer(cols = c(pb_act, pb_nact, pv_act, pv_nact), names_to = "variable", values_to = "probability")

rm(Data.obs.pbpv.sectorsdrugs)
load("FILEPATH/countryyear.pbpv.ALLCATEGORIES.Rdata")
Data.obs.pbpv.sectorsdrugs <- countryyear.pbpv.ALLCATEGORIES

lpp2 <- lpp2 %>%
  left_join(Data.obs.pbpv.sectorsdrugs, by=c("location_name","year","variable"))


# Check single subregion (run each separate - lack of data creates issue)
pdf("FILEPATH/countryyear_subregions.multi.mclogit+obs.pdf")

lpp3 <- lpp2 %>% filter(who_subregion == "AFRO-C" )
ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 0.8, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title = "AFRO-W")+
  facet_wrap(~ location_name)

lpp3 <- lpp2 %>% filter(who_subregion == "AFRO-E" )
ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 0.8, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title = "AFRO-E")+
  facet_wrap(~ location_name)


lpp3 <- lpp2 %>% filter(who_subregion == "AFRO-S" )
ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 0.8, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title = "AFRO-S")+
  facet_wrap(~ location_name)

lpp3 <- lpp2 %>% filter(who_subregion == "AFRO-W" )
ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 0.8, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title = "AFRO-C")+
  facet_wrap(~ location_name)

lpp3 <- lpp2 %>% filter(who_subregion == "EMRO" )
ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 0.8, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title = "EMRO")+
  facet_wrap(~ location_name)

lpp3 <- lpp2 %>% filter(who_subregion == "EURO" )
ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  #geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 0.8, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title = "EURO")+
  facet_wrap(~ location_name)


lpp3 <- lpp2 %>% filter(who_subregion == "PAHO" )
ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 0.8, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title = "PAHO")+
  facet_wrap(~ location_name)


lpp3 <- lpp2 %>% filter(who_subregion == "SEARO" )
ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 0.8, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title = "SEARO")+
  facet_wrap(~ location_name)

lpp3 <- lpp2 %>% filter(who_subregion == "WPRO" )
ggplot(lpp3, aes(x=year, y=probability, fill=variable))+ geom_area() +
  #geom_point(aes(x=year, y=pbpv_prop, shape = variable), size = 0.8○, position=position_stack()) +
  labs(fill = "Trt Option/Source", y = "Probability", x = "Year", shape = "variable", title = "WPRO")+
  facet_wrap(~ location_name)

dev.off()

# Drugs
lpp.drugs<-Data.pred %>%
  group_by(location_name, year) %>% rowwise() %>% 
  summarise(ACT= sum(pb_act,pv_act),
            nACT = sum(pb_nact,pv_nact))

lpp.plot<-lpp.drugs %>%
  group_by(location_name, year) %>%
  summarise(ACT = mean(ACT),
            nACT = mean(nACT)
  ) 

lpp2.plot <- lpp.plot %>% pivot_longer(cols = c(ACT, nACT), names_to = 'variable', values_to = "probability") 

rm(Data.obs.pbpv.drugsonly)
load("FILEPATH/Data.obs.pbpv.drugsonly.Rdata")

lpp2.plot <- lpp2.plot %>%
  left_join(Data.obs.pbpv.drugsonly, by=c("location_name","year","variable")) %>% filter(!is.na(n)) %>% dplyr::select(location_name,year,variable, probability,pbpv_prop) 


# Scatter Observed vs. Predicted

cor(lpp2.plot$probability,lpp2.plot$pbpv_prop )

## Split by categories

pdf("FILEPATH/obs_pred.mclogit_n50.categories_SPLIT_DRUGS.pdf")

ggplot(lpp2.plot, aes(pbpv_prop,probability)) +
  geom_point(alpha=0.5, size=2) + 
  geom_smooth(method = "lm", se=FALSE) +
  stat_regline_equation(label.y = 1, aes(label = ..eq.label..)) +
  stat_regline_equation(label.y = 0.9, aes(label = ..rr.label..))+
  labs(x = "Observed",y = "Predicted", title = "Model=mclogit<50 \n Binary Outcome")+ facet_wrap(~variable)

dev.off()

# # Overall ## By All categories
pdf("FILEPATH/obs_pred.mclogit_n50.categories_colour_DRUGS.pdf")

(ggplot(lpp2.plot, aes(pbpv_prop,probability, color=variable)) +
    geom_point(alpha=0.5, size=2) +  geom_abline(size = 1.0) +
    labs(x = "Observed",y = "Predicted",title = "Drugs Only \nModel=mclogit<50")) 

dev.off()


ncountry <- unique(lpp2.plot$location_name)
n.pages <- ceiling(length(n.countries) / 2)

################################

## Country
pdf("FILEPATH/countrypoints.mclogit_n50_obs_pred_DRUGS.pdf")

# for(i in 1:length(ncountry)){
for(i in 1:n.pages){
  
  print(ggplot(lpp2.plot, aes(pbpv_prop,probability, color=variable)) +
          geom_point(alpha=0.5, size=2) + 
          geom_abline(size = 1.0) +
          labs(x = "Observed",y = "Predicted") +
          facet_wrap_paginate(~ location_name,ncol = 1, nrow = 2, page = i)
        
  )
  
}
dev.off()

## Sector
lpp.sector<-Data.pred %>%
  group_by(location_name, year) %>% rowwise() %>% 
  summarise(Public= sum(pb_act,pb_nact),
            Private = sum(pv_act,pv_nact))

lpp.plot<-lpp.sector %>%
  group_by(location_name, year) %>%
  summarise(Public= mean(Public),
            Private = mean(Private)
  ) 

lpp2.plot <- lpp.plot %>% pivot_longer(cols = c(Public, Private), names_to  = 'variable', values_to = "probability") 

rm(Data.obs.pbpv.sectorsonly)
load("FILEPATH/Data.obs.pbpv.sectorsonly.Rdata")

lpp2.plot <- lpp2.plot %>%
  left_join(Data.obs.pbpv.sectorsonly, by=c("location_name","year","variable")) %>% filter(!is.na(n)) %>% dplyr::select(location_name,year,variable, probability,pbpv_prop) 

# Scatter Observed vs. Predicted

cor(lpp2.plot$probability,lpp2.plot$pbpv_prop )

## Split by categories
pdf("FILEPATH/obs_pred.mclogit_n50_multi.categories_SPLIT_SECTORS.pdf")

ggplot(lpp2.plot, aes(pbpv_prop,probability)) +
  geom_point(alpha=0.5, size=2) + 
  geom_smooth(method = "lm", se=FALSE) +
  stat_regline_equation(label.y = 1, aes(label = ..eq.label..)) +
  stat_regline_equation(label.y = 0.9, aes(label = ..rr.label..))+
  labs(x = "Observed",y = "Predicted", title = "Model=mclogit<50 \n Binary Outcome")+ facet_wrap(~variable)

dev.off()

# Color coded  

pdf("FILEPATH/obs_pred.mclogit_n50.categories_colour_SECTORS.pdf")

(ggplot(lpp2.plot, aes(pbpv_prop,probability, color=variable)) +
    geom_point(alpha=0.5, size=2) +  geom_abline(size = 1.0) +
    labs(x = "Observed",y = "Predicted",title = "Sectors Only \nModel=mclogit<50")) 

dev.off()

ncountry <- unique(lpp2.plot$location_name)

## Country
pdf("FILEPATH/countrypoints.mclogit_n50_obs_pred_SECTORS.pdf")

for(i in 1:length(ncountry)){
  # for(i in 1:length(n.pages)){
  
  print(ggplot(lpp2.plot, aes(pbpv_prop,probability, color=variable)) +
          geom_point(alpha=0.5, size=2) + 
          geom_abline(size = 1.0) +
          labs(x = "Observed",y = "Predicted") +
          facet_wrap_paginate(~ location_name,ncol = 1, nrow = 1, page = i)
        
  )
  
}
dev.off()

toc()


###STEP 4: GENERATING RASTERS FOR ACT/nACT/PUBLIC/PRIVATE SPLITS
#######
tic()

##########################################################################################
###Incorporating Efficacy model
############################################################################################s

### load efficacy stacks and turn into csvs

output.path           <- paste0(FILEPATH)
dir.create(output.path)
output.path.tables    <- paste0(FILEPATH)
dir.create(output.path.tables)
output.path.rasters    <- paste0(FILEPATH)
dir.create(output.path.rasters)


output.path.tables.africa    <- paste0(FILEPATH)
dir.create(output.path.tables.africa)
output.path.tables.global   <- paste0(FILEPATH)
dir.create(output.path.tables.global)
output.path.rasters.africa    <- paste0(FILEPATH)
dir.create(output.path.rasters.africa)
output.path.rasters.global   <- paste0(FILEPATH)
dir.create(output.path.rasters.global)

## filepaths for versioning
careseeking.table.path <- "FILEPATH/TS_predictions_GAMkNN.csv"
propact.table.path     <- "FILEPATH/20240711_ACTproportional_withsplines.csv"
nact.africa.path       <- "FILEPATH/20240705_nonACTproportional_africa.csv"

### read in tables
core_table <- read.csv(careseeking.table.path) %>% 
  dplyr::select(iso = ISO3, Country_Name,IHME_location_id,  Admin_Unit_Level, Admin_Unit_Name, year= Year,
                Public = HMIS_pred, Private = Private_pred) %>% 
  group_by(iso, Country_Name, IHME_location_id,  Admin_Unit_Level, Admin_Unit_Name, year) %>% 
  dplyr::summarise(Public = mean(Public),
                   Private = mean(Private)) %>% 
  pivot_longer(cols = c(Public, Private), names_to = "sector", values_to = "careseeking")
propact    <- read.csv(propact.table.path) %>% dplyr::select(iso = iso3, year, sector, prop_act = pred_act)
core_table <- core_table %>% left_join(propact) %>% mutate(prop_act = if_else(year < 2001, 0, prop_act))
core_table <- core_table %>% mutate(prop_nonact = 1 - prop_act)

nact.africa <- read.csv(nact.africa.path) %>% dplyr::select(location_name, sector, pred_cq, pred_sp, pred_amo, pred_quinine, year = year_id, is_act) %>% 
  mutate(iso = countrycode::countrycode(location_name, "country.name", "iso3c")) %>% 
  mutate(sector = if_else(sector=="public", "Public", "Private"))

### split into inside/outside africa before rejoining later
africa_isos <- unique(nact.africa$iso)
'%!in%' <- function(x,y)!('%in%'(x,y))
### outside africa table
africa_table <- core_table %>% filter(iso %in% africa_isos)
### inside africa table
non_africa_table <- core_table %>% filter(iso %!in% africa_isos)

### deal with africa table first

africa_table <- africa_table %>% left_join(nact.africa) %>% 
  group_by(iso, Country_Name, sector) %>% 
  fill( pred_cq, pred_sp, pred_amo, pred_quinine, .direction = "up") %>% 
  dplyr::select(-location_name)

africa_table <- africa_table %>% 
  mutate(pred_cq = pred_cq * prop_nonact,
         pred_sp = pred_sp * prop_nonact,
         pred_amo = pred_amo * prop_nonact,
         pred_quinine = pred_quinine * prop_nonact)

africa_table <- africa_table %>% mutate(is_act = if_else(is.na(is_act), 0, 1))

asaq_list <- c("Burundi", "Cameroon", "Chad", "Republic Of Congo",
               "Cote d'Ivoire", "Democratic Republic Of The Congo",
               "Equatorial Guinea", "Gabon", "Ghana", "Guinea",
               "Liberia", "Madagascar", "Mauritania", "Sao Tome And Principe",
               "Senegal", "Sierra Leone", "Tanzania") # based on WMR 2008
asaq_list_iso <- countrycode::countrycode(asaq_list, "country.name", "iso3c")

## shapefile
africa_sf <- st_read("FILEPATH/admin2023_0_MG_5K.shp") %>% 
  filter(ISO %in% africa_isos)

## get efficacy stacks and tabulate 
# efficacy stacks
cq.efficacy.stack      <-rast("FILEPATH/CQeff_19802024.tif")
sp.efficacy.stack      <-rast("FILEPATH/SPeff_19802024.tif")
amo.efficiacy.stack    <-rast("FILEPATH/Othereff_19802024.tif")

mean.effectiveness.ofact <-  0.9576*0.92
quinine.efficiacy        <- 0.6539852 # global value of Quinine efficacy
africa_table$act_eff     <- mean.effectiveness.ofact
africa_table$qui_eff     <- quinine.efficiacy

africa_table_list <- list()
for(y in 1980:2024){
# for(y in 1991:2025){
  idx <- y -1979
  print(y)
  if(y==2024){
    eff_year <- 2024
  } else{
    eff_year <- y
  }
  sf_tmp <- africa_sf %>% dplyr::rename(iso = ISO) %>% dplyr::select(iso, geometry)
  sf_tmp$cq_eff <- exactextractr::exact_extract(cq.efficacy.stack[[eff_year-1979]], sf_tmp, fun = "mean")
  sf_tmp$sp_eff <- exactextractr::exact_extract(sp.efficacy.stack[[eff_year-1979]], sf_tmp, fun = "mean")
  sf_tmp$amo_eff <- exactextractr::exact_extract(amo.efficiacy.stack[[eff_year-1979]], sf_tmp, fun = "mean")

    
  fill_cq <- sf_tmp[which(sf_tmp$iso=="DZA"),]$cq_eff
  sf_tmp <- sf_tmp %>% mutate(cq_eff= if_else(is.na(cq_eff), fill_cq, cq_eff))
  
  fill_sp <- sf_tmp[which(sf_tmp$iso=="DZA"),]$sp_eff
  sf_tmp <- sf_tmp %>% mutate(sp_eff= if_else(is.na(sp_eff), fill_sp, sp_eff))
  
  fill_amo <- sf_tmp[which(sf_tmp$iso=="DZA"),]$amo_eff
  sf_tmp <- sf_tmp %>% mutate(amo_eff= if_else(is.na(amo_eff), fill_amo, amo_eff))
  
  africa_table_list[[idx]] <- africa_table %>% filter(year==y) %>% left_join(sf_tmp %>% st_drop_geometry())
}
 

africa_table_final <- do.call(rbind, africa_table_list) # recombine

asaq_list <- c("Burundi", "Cameroon", "Chad", "Republic Of Congo",
               "Cote d'Ivoire", "Democratic Republic Of The Congo",
               "Equatorial Guinea", "Gabon", "Ghana", "Guinea",
               "Liberia", "Madagascar", "Mauritania", "Sao Tome And Principe",
               "Senegal", "Sierra Leone", "Tanzania") # based on WMR 2008

africa_table_final <- africa_table_final %>% 
  group_by(iso, sector) %>% 
  fill(prop_act, prop_nonact, pred_cq, pred_sp, pred_amo, pred_quinine) %>% 
  mutate(cq_eff = if_else(cq_eff > act_eff, act_eff, cq_eff),
         sp_eff = if_else(sp_eff > act_eff, act_eff, sp_eff),
         amo_eff = if_else(amo_eff > act_eff, act_eff, amo_eff)) %>% 
  ungroup() %>% 
  mutate(
    effective_antimalarial_treatment = prop_act*act_eff + pred_cq*cq_eff + pred_sp*sp_eff + pred_amo*amo_eff + pred_quinine*qui_eff
        ) %>% 
  dplyr::select(iso, Country_Name, IHME_location_id, Admin_Unit_Level, Admin_Unit_Name,
                         year, sector, careseeking, effective_antimalarial_treatment)   %>% 
  distinct()

africa_table_final_final <- africa_table_final %>% ungroup() %>%
   
  dplyr::select(iso, Country_Name, IHME_location_id, Admin_Unit_Level, Admin_Unit_Name, year, effective_antimalarial_treatment, careseeking, sector) %>% 
  distinct() %>% 
  mutate(
    EFT = careseeking*effective_antimalarial_treatment
    
    ) %>%  distinct() %>% 
  pivot_wider(id_cols = c(iso, Country_Name, IHME_location_id, Admin_Unit_Level, Admin_Unit_Name, year),
              
              names_from = sector, values_from = EFT, names_prefix = "EFT_")

africa_table_final_final <- africa_table_final_final %>% 
  mutate(EFT = EFT_Public + EFT_Private)

ggplot(africa_table_final_final %>% filter(Admin_Unit_Level=="ADMIN0")) + 
  geom_line(aes(x=  year, y= EFT, group = IHME_location_id)) +
  facet_wrap(~iso)

###take only Admin 0
africa_table_final_final <- africa_table_final_final %>% filter(Admin_Unit_Level=="ADMIN0")%>% left_join(population_table)#, by=c( "iso_africa"="iso", "year")


# ######population add for pop weighting
population_table <- read.csv("FILEPATH/ihme_populations.csv") %>%
  filter(ihme_admin_unit_level == "ADMIN0") %>%
  filter(age_bin %in% c("MAP_adults" ,"MAP_children" ,"MAP_infants")) %>%
  group_by(year, iso3) %>%
  dplyr::summarise(population = sum(total_pop)) %>%
  ungroup() %>%
  dplyr::rename(iso = iso3)


# public_care_eff_act public_care_eff_nonact private_care_eff_act private_care_eff_nonact
global <- read.csv('FILEPATH/AFRICA_global_EFT.csv') %>% mutate(Country_Name= case_when(
  Country_Name=="East Timor"~ "Timor-Leste", TRUE~Country_Name)) %>% #View()
  mutate(EFT = public_care_eff_act + private_care_eff_act + public_care_eff_nonact + private_care_eff_nonact, EFT_Public = public_care_eff_nonact + public_care_eff_act, 
         EFT_Private = private_care_eff_nonact + private_care_eff_act, EFT_ACT = public_care_eff_act + private_care_eff_act, EFT_NonACT =  public_care_eff_nonact + private_care_eff_nonact) %>% 
  dplyr::select(iso = ISO3, IHME_location_id, Country_Name, year = Year, EFT, EFT_Public, EFT_Private) %>% 
  distinct(iso, IHME_location_id, Country_Name, year, .keep_all = TRUE)


global <- global %>% left_join(population_table) %>% mutate(continent="Non Africa")

# Ensure column names are consistent
colnames(africa_table_final_final)[colnames(africa_table_final_final) == "iso"] <- "iso_africa"
africa_table_final_final$continent <- "Africa"

# Step 1: Identify matching rows
matching_rows <- africa_table_final_final %>%
  rename(iso = iso_africa) %>%
  semi_join(global, by = c("iso", "year", "Country_Name"))

# Step 2: Remove the matching rows from global
global_updated <- global %>%
  anti_join(matching_rows, by = c("iso", "year", "Country_Name"))

# Step 3: Combine global_updated with matching rows from Africa
final_result <- bind_rows(global_updated, matching_rows)

final_result<- final_result %>%  dplyr::select(-c(Admin_Unit_Level, Admin_Unit_Name, continent))

write.csv(final_result,
          paste0("FILEPATH", "EFT.csv"),
          row.names = F)

# Set the desired resolution (approximately 5 km in degrees)
resolution_degrees <- 5 / 111.32  # ~0.04488 degrees

# # Create an empty raster with the extent and resolution based on the shapefile
# Use the base raster as a template
empty_raster <- base_raster  # Copy the base raster
values(empty_raster) <- NA

# Rasterize the shapefile using the 'EFT' field
for(y in 1980:2024) {
  africa_table_final_toraster <- final_result %>% filter(year == y)
  make_sf <- global_sf %>% rename(iso = ISO) %>% left_join(africa_table_final_toraster)
  
  # Rasterize the shapefile into the empty raster
  r <- rasterize(make_sf, empty_raster, field = "EFT", fun = mean)
  
  # Plot and save the raster
  plot(r, main = paste0("EFT ", y), col = scico::scico(20, palette = 'lapaz'), breaks = seq(0,1,length.out = 20))
  
  writeRaster(r, 
              paste0("FILEPATH", 
                     "EFT_", y, ".tif"), 
              overwrite = TRUE)
}

africa_table_forcompare <- global %>% filter(year %in% 2000:2024) %>% 
  rename(EFT_GBD2024_v2 = EFT)


gdb2023_table_list <- list()
for(y in 2000:2024){
  idx <- y-1999
  gdb2023_rast <- rast(paste0("FILEPATH/",y,".effective.treatment.tif"))
  sf_tmp <- africa_sf %>% dplyr::rename(iso = ISO) %>% dplyr::select(iso, geometry)
  sf_tmp$EFT_GBD2024_v1 <- exactextractr::exact_extract(gdb2023_rast, sf_tmp, fun = "mean")
  
  gdb2023_table_list[[idx]] <- africa_table_forcompare %>% filter(year==y) %>% left_join(sf_tmp %>% st_drop_geometry())
}

africa_table_forcompare <- do.call(rbind, gdb2023_table_list) # recombine

africa_table_forcompare <- africa_table_forcompare %>% 
  pivot_longer(cols = c(EFT_GBD2024_v1,EFT_GBD2024_v2 )) %>% 
  left_join(population_table)

africa_table_forcompare_africa <- africa_table_forcompare %>% 
  filter(iso %!in% c("CPV")) %>% 
  group_by(year, name) %>% 
  dplyr::summarise(value = weighted.mean(value, population))
africa_table_forcompare_africa$Country_Name <- "Africa"

africa_table_forcompare <- bind_rows(africa_table_forcompare_africa, africa_table_forcompare)


africa_table_forcompare <- africa_table_forcompare %>% filter(Country_Name != "Africa")

ggplot(africa_table_forcompare) +
  geom_line(aes(x = year, y = value, colour = name)) +
  facet_wrap(~Country_Name) +
  theme_bw()

ggsave(
  filename = "2024_v2_versions_comparisons_plots.pdf",  
  width = 15, height = 12)


plotlist <- list()
for(cou in unique(africa_table_forcompare$Country_Name)){
  tmp <- africa_table_forcompare %>% filter(Country_Name==cou)
  p <- ggplot(tmp) +
    geom_line(aes(x = year, y = value, colour = name), linewidth = 1) +
    geom_point(aes(x = year, y = value, colour = name), size = 3) +
    labs(x = "Year", y = "EFT", title = paste0("Access to effective antimalarial treatment"),
         subtitle = cou, colour = "") +
    scale_x_continuous(expand = c(0,0)) +
    scale_y_continuous(limits = c(0,1), label = scales::label_percent()) +
    # khroma::scale_colour_highcontrast() + 
    scale_color_manual(values = c("red", "blue", "green", "purple"))
    theme_bw() +
    theme(text = element_text(size = 16))
  
  plotlist[[cou]] <- p
}

ggsave(
  filename = "FILEPATH/2024_v2_version_comparison_plots.pdf", 
  plot = marrangeGrob(plotlist, nrow=1, ncol=1), 
  width = 15, height = 9
)



