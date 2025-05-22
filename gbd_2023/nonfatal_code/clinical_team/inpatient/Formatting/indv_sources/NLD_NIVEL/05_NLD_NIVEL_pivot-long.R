rm(list=ls())
library(readxl)
library(tidyverse)
library(purrr)

year_sheets <- c("2011",
                 "2012",
                 "2013",
                 "2014",
                 "2015",
                 "2016",
                 "2017",
                 "2018",
                 "2019",
                 "2020",
                 "2021",
                 "2022")

files <- c("adjusted",
           "intermediate-harmonized",
           "unadjusted")

for (filename in files) {
  
  excel_path <- paste0('FILEPATH/NLD_NIVEL_PCD_IHME_wide-format_', filename, '.xlsx')
  
  ip_path <- "FILEPATH/NLD_NIVEL_PRIMARY_CARE_DATABASE_2011_2022_combined_Y2024M04D09_Y2024M05D06.xlsx"
  
  # Function to read each sheet
  read_sheet <- function(sheet_name) {
    read_excel(path = excel_path, sheet = sheet_name)
  }
  
  combined_data <- map_dfr(year_sheets, read_sheet)
  # Remove the last two columns: (i_p and 50-59)
  combined_data <- combined_data %>% select(-c(i_p, `50_59`))
  
  # Convert column names to match GBD age group id names:
  
  df <- combined_data %>% 
    mutate(across(c("0_4", "5_9", "10_14", "15_19", "20_24", "25_29", "30_34", "35_39", "40_44", "45_49", "50_54", "55_59", "60_64", "65_69", "70_74", "75_79", "80_84", "85_plus"), as.numeric))
  
  df <- df %>%
    rename_with(~ gsub("_", " to ", .x))
  
  df <- df %>% 
    rename("Under 5" = "0 to 4",
           "85 plus" = "85 to plus",
           "5-14 years" = "5 to 14",
           "15-49 years" = "15 to 49",
           "50-69 years" = "50 to 69",
           "70+ years" = "70 to plus")
  
  # Pivot to long format:
  
  long_df <- pivot_longer(df, 
                          cols = -c(ICPC, measure, Description, year, sex), 
                          names_to = "age_group_name", 
                          values_to = "cases"
                          )
  
  
  # Import and format age group id data:
  
  age_groups <- as.data.table(read.csv("FILEPATH/age_group_ids.csv"))
  age_groups <- age_groups[, c("age_group_id","age_group_name","age_group_years_start","age_group_years_end")]
  age_groups <- rename(age_groups, age_start = age_group_years_start)
  age_groups <- rename(age_groups, age_end = age_group_years_end)
  
  
  # Merge age group id data onto long formatted data:
  
  long_df_ages <- long_df %>%
    left_join(age_groups, by = c("age_group_name"))
  
  
  # Read in population data sheet and adjust age group names:
  
  dutch_pop <- read_excel(path = excel_path, sheet = "Population Denominators")
  
  dutch_pop <- dutch_pop %>%
    mutate(age = str_replace_all(age, "_", " to "))
  
  dutch_pop <- dutch_pop %>%
    mutate(age = case_when(
      age == "0 to 4" ~ "Under 5",
      age == "5 to 14" ~ "5-14 years",
      age == "15 to 49" ~ "15-49 years",
      age == "50 to 69" ~ "50-69 years",
      age == "70 to plus" ~ "70+ years",
      age == "85 to plus" ~ "85 plus",
      TRUE ~ age
    ))
  
  dutch_pop_long <- pivot_longer(dutch_pop, 
                                 cols = -c(sex, age), 
                                 names_to = "year", 
                                 values_to = "population"
                                )
  
  dutch_pop_long <- rename(dutch_pop_long, age_group_name = age)
  
  dutch_pop_long <- dutch_pop_long %>%
    mutate(year = as.double(year))
  
  # Merge populations onto long data:
  
  long_df_ages_pop <- long_df_ages %>%
    left_join(dutch_pop_long, by = c("age_group_name", "year", "sex"))
  
  long_df_ages_pop <- rename(long_df_ages_pop, cause_code = ICPC)
  long_df_ages_pop <- rename(long_df_ages_pop, code_name = Description)
  long_df_ages_pop <- rename(long_df_ages_pop, sample_size = population)
  
  long_df_ages_pop <- long_df_ages_pop %>%
    select(cause_code, code_name, measure, age_group_id, age_start, age_end, year, sex, cases, sample_size)
  
  long_df_ages_pop <- long_df_ages_pop %>% 
    filter(!is.na(cases))
  
  # Left join IP info sheet from Theo to perform prevalence calculations:
  
  ip_sheet <- read_excel(path = ip_path, sheet = "selection Nivel")
  
  ip_sheet <- ip_sheet %>%
    rename("cause_code" = "ICPC code", 
           #"i_p" = "I or P",
           "sex_spec" = "sex-specific?") 
  
  ip_sheet <- ip_sheet %>%
    select(c(cause_code, sex_spec))
  
  ip_sheet <- ip_sheet %>%
    distinct()
  
  ip_sheet <- ip_sheet %>%
    mutate(sex_spec = as.numeric(case_when(
      sex_spec == "M" ~ "1",
      sex_spec == "F" ~ "2",
      sex_spec == "B" ~ "3",
      TRUE ~ sex_spec
    )))
  
  long_df_ages_pop_ip <- long_df_ages_pop %>% 
    left_join(ip_sheet, by = "cause_code")
  
  long_df_prepped <- long_df_ages_pop_ip %>%
    filter(!(sex == 1 & sex_spec == 2) & !(sex == 2 & sex_spec == 1) | sex_spec == 3)
  
  long_df_prepped <- long_df_prepped %>%
    select(-sex_spec)
  
  # Write final output to csv
  
  write_csv(long_df_prepped, paste0("FILEPATH", filename, ".csv"))
}