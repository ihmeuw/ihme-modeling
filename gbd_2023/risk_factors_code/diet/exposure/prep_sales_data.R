# Clear all variables
rm(list=ls())

# Set working directory
if (Sys.info()['sysname'] == "Windows") {
  prefix <- "J:"
} else {
  prefix <- "FILEPATH"
}

############################################
# Set relevant variables
version <- "2016_4_adj"

# Import energy estimates for energy adjustment
energy_estimates <- read_csv("FILEPATH")

############################################

# Load location metadata
library(devtools)
source("FILEPATH/get_location_metadata.R")
locs <- get_location_metadata(release_id = 16, location_set_id = 22) %>%
  dplyr::select(c('location_name', 'location_id', 'level', 'super_region_name', 'region_name', 'ihme_loc_id'))

# Filter data
locs <- locs[locs$level == 3 | locs$location_id == 354, ] # Hong Kong


# Load necessary libraries
library(readxl)
library(dplyr)

# Define the filenames
filenames <- c("milk", "eggs", "fruit", "meat", "nuts", "processed_meats_seafood", "pulses", "starchy_roots", "veg", "redmeat", "hvo", "ssb")

# Define the countries 
countries <- c("China", "Hong Kong, China", "India", "Indonesia", "Japan", "Malaysia", "Philippines", "Singapore", 
               "South Korea", "Taiwan", "Thailand", "Vietnam", "Australia", "New Zealand", "Bulgaria", "Hungary", "Poland", 
               "Romania", "Russia", "Slovakia", "Ukraine", "Argentina", "Brazil", "Chile", "Colombia", "Mexico", 
               "Peru", "Venezuela", "Egypt", "Israel", "Morocco", "Saudi Arabia", "South Africa", "United Arab Emirates", "USA", 
               "Austria", "Belgium", "Denmark", "Finland", "France", "Germany", "Greece", "Ireland", "Italy", 
               "Netherlands", "Norway", "Portugal", "Spain", "Sweden", "Switzerland", "Turkey", "United Kingdom")

# Define the additional countries for milk 

additional_countries <- c("Azerbaijan", "Kazakhstan", "Pakistan", "Uzbekistan", "Belarus", "Bosnia-Herzegovina", "Croatia", 
                          "Czech Republic", "Estonia", "Georgia", "Latvia", "Lithuania", "Macedonia", "Serbia", "Slovenia", 
                          "Bolivia", "Costa Rica", "Dominican Republic", "Ecuador", "Guatemala", "Uruguay", "Venezuela", 
                          "Algeria", "Cameroon", "Iran", "Kenya", "Morocco", "Nigeria", "Tunisia", "Canada")

# Initialize an empty data frame to store the results
results <- data.frame()
counter <- 1

# Loop over the filenames
for (file in filenames) {
  
  
  # Import the data
  data <- read_excel(paste0("FILEPATH"))
  
  if (file == "milk") {
    data <- data[data$Country %in% c(countries, additional_countries), ]
    data <- data[data$`Data Type` == "Total Volume (Tonnes)", ]
    data$Category <- "Milk"
  }
  
  # Filter the data based on the countries
  if (file != "hvo" & file != "milk") {
    data <- data[data$Country %in% countries, ]
  }
  
  # select columns containing (Per Capita)
  PC_colnames <- grep("PC \\d{4} \\(Per Capita\\)", names(data), value = TRUE)
  PC_years <- gsub("PC (\\d{4}) \\(Per Capita\\)", "\\1", PC_colnames)
  
  for(year in min(PC_years):max(PC_years)) {
        old_name <- paste0("PC ", year, " (Per Capita)")
        new_name <- paste0("PC_", year)
        data <- setnames(data, old_name, new_name)
      }
  
  # Filter the data based on the DataType
  if (file != "milk") {
    data <- data[data$`Data Type` %in% c("Total Volume", "Volume Consumption"), ]
  }
  
  if (file == "redmeat") {
    data <- filter(data, Subcategory != "Meat") %>%
      group_by(Country, Category) %>%
      summarise(across(starts_with("PC"), sum, na.rm = TRUE)) %>%
      ungroup()
    data$Category <- "redmeat"
  }
  
  if (file == "ssb") {
    data <- data %>% 
      group_by(Country) %>%
      summarise(across(starts_with("PC"), sum, na.rm = TRUE)) %>%
      ungroup()
    data$Category <- "ssb"
  }
  
  if (file == "hvo") {
    data$Category <- "hvo"
  }
  
  if (file == "processed_meats_seafood") {
    data$Category <- "procmeat"
  }
  
  if (file == "starchy_roots") {
    data$Category <- "starchy_veg"
  }
  
  data <- dplyr::select(data, -contains("Per Household"))
  
  data <- data %>%
    pivot_longer(cols = starts_with("PC_"),
                 names_to = "year_id",
                 values_to = "PC_",
                 names_prefix = "PC_")
  
  data$PC_ <- data$PC_ * 1000 / 365
  setnames(data, 'PC_', 'g_pc')
  
  # Hydrogenated vegetable oil
  if (file == "hvo") {
    data <- filter(data, g_pc != 0)
  }
  
  # Nuts
  if (file == "nuts") {
    data <- filter(data, Country != "United Kingdom")
  }
  
  # Processed Meats
  if (file == "processed_meats_seafood") {
    data <- data[!data$Country %in% c("Cameroon", "India", "Kenya", "Nigeria", "Pakistan"), ]
  }
  
  # Pulses/Legumes
  if (file == "pulses") {
    data <- data[!data$Country %in% c("Austria", "Finland", "Norway", "Japan"), ]
  }
  
  data <- data %>% dplyr::select("Country", "Category", "year_id", "g_pc") 
  

  # Append the data to the results
  results <- rbind(results, data)
}

setnames(results, c('Country', 'Category', 'year_id', 'g_pc'), c('location_name', 'risk', 'year', 'mean'))



results <- results %>% 
  mutate(risk = tolower(risk),
         risk = paste0(risk, "_sales"),
         year_start = year,
         year_end = year,
         is_outlier = 0,
         ihme_risk = paste0("diet_", risk),
         me_risk = ihme_risk,
         representative_name = "Nationally representative only",
         case_definition = "g/day",
         metc = NA,
         svy = "Euromonitor Sales Data",
         nid = NA, 
         data_status = "active",
         modelable_entity_name = ".",
         modelable_entity_id = NA,
         cv_natl_rep = 1,
         cv_fao_data = 0,
         total_calories = NA,
         energy_adj_scalar = NA,
         mean_adj = NA 
  )

results$location_name[results$location_name == "USA"] <- "United States"
results$location_name[results$location_name == "Bosnia-Herzegovina"] <- "Bosnia and Herzegovina"
results$location_name[results$location_name == "Hong Kong, China"] <- "Hong Kong Special Administrative Region of China"

results_with_locs <- merge(results, locs, by = "location_name", all = FALSE)

setnames(energy_estimates, 'year_id', 'year')

results <- merge(results_with_locs, energy_estimates, by = c("location_id", "year"), all = FALSE) 

 write.csv(results, "FILEPATH/sales_data_outliered_", version, ".csv", row.names = FALSE)

 