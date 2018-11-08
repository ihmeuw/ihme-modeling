library(data.table)
library(readr)

rm(list=ls())

# Get settings ------------------------------------------------------------

main_dir <- commandArgs(trailingOnly=T)[1]
ihme_loc <- commandArgs(trailingOnly=T)[2]

source("settings.R")
get_settings(main_dir)

location_hierarchy <- fread(paste0(temp_dir, '/../database/location_hierarchy.csv'))[ihme_loc_id == ihme_loc]

# Set up id vars for assertion checks later -------------------------------

srb_1_id_vars <- list(ihme_loc_id = ihme_loc,
                            year_id = seq(min(years), max(years), 1))
srb_5_id_vars <- list(ihme_loc_id = ihme_loc,
                            year_id = seq(min(years), max(years), 5))

# Create srb data ---------------------------------------------------

srb_dir <- "FILEPATH"
srb_1_data <- fread(srb_dir)

# subnational srbs not modeled, so pull national value
national_location <- location_hierarchy[, level == 3 | ihme_loc_id %in% c("CHN_354", "CHN_361", "CHN_44533")]
srb_ihme_loc <- ifelse(national_location, ihme_loc, substr(ihme_loc, 1, 3))
if (srb_ihme_loc == "CHN") srb_ihme_loc <- "CHN_44533"

srb_1_data <- srb_1_data[ihme_loc_id == srb_ihme_loc, list(ihme_loc_id, year_id, value_mean = srb)]
srb_1_data[, ihme_loc_id := ihme_loc]

# aggregate into five year intervals
srb_5_data <- copy(srb_1_data)
srb_5_data[, year_id := plyr::round_any(year_id, accuracy = 5, f = floor)]
srb_5_data <- srb_5_data[, list(value_mean = mean(value_mean)), by = c("ihme_loc_id", "year_id")]

# Calculate values of interest --------------------------------------------

setkeyv(srb_1_data, c("ihme_loc_id", "year_id"))
srb_1_matrix <- matrix(srb_1_data$value_mean, ncol = length(srb_1_id_vars$year_id))
colnames(srb_1_matrix) <- srb_1_id_vars$year_id

setkeyv(srb_5_data, c("ihme_loc_id", "year_id"))
srb_5_matrix <- matrix(srb_5_data$value_mean, ncol = length(srb_5_id_vars$year_id))
colnames(srb_5_matrix) <- srb_5_id_vars$year_id


# Assertion checks --------------------------------------------------------

assertable::assert_ids(srb_1_data, id_vars = srb_1_id_vars)
assertable::assert_values(srb_1_data, colnames="value_mean", test="gte", test_val=0)

assertable::assert_ids(srb_5_data, id_vars = srb_5_id_vars)
assertable::assert_values(srb_5_data, colnames="value_mean", test="gte", test_val=0)


# Save prepped data -------------------------------------------------------

write_csv(srb_1_data, path = paste0(temp_dir, "/inputs/srb_1.csv"))
write_csv(srb_5_data, path = paste0(temp_dir, "/inputs/srb_5.csv"))

save(srb_1_matrix, srb_5_matrix,
     file=paste0(temp_dir, "/inputs/srb_matrix.rdata"))
