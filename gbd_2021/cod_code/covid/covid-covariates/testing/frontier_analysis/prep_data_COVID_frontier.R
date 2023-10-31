# Project: Estimate country-specific COVID daily testing rate caps based on LDI per capita
# Purpose: Create input data for COVID frontier work
# Author: INDIVIDUAL_NAME INDIVIDUAL_NAME
# Date created: 7/20/2020
library(data.table)

# Read in testing input data passed along by INDIVIDUAL_NAME INDIVIDUAL_NAME
dt <- fread("FILEPATH/data_smooth.csv")
dt[, date := as.Date(date)]
dt <- dt[, .(location_id, location_name, date, year_id = 2020, total_pc_100= daily_total / pop * 1e5)]

source("FILEPATH/get_location_metadata.R")
glm <- get_location_metadata(location_set_id = 35, gbd_round_id = 7, decomp_step = "iterative")
glm_us_subnats <- glm[parent_id==102]
us_subnats_location_id <- unique(glm_us_subnats$location_id)
glm_all <- copy(glm)
glm <- glm[level==3]

# Use mean 2020 LDIpc as xvar, latest best estimates (model_version_id = 36887)
source("FILEPATH/get_covariate_estimates.R")
x_wsubnats <- get_covariate_estimates(covariate_id = 57, gbd_round_id = 7, decomp_step = 'iterative', year_id = 2020)
x <- merge(x_wsubnats, glm, by = "location_id", all.y = T)

# Also output data for a few missing locations
missing_locs <- c("Luxembourg", "Iceland", "India", "United Arab Emirates", "Chad", "Azerbaijan")
fwrite(x[location_name.x %in% missing_locs, .(location_name = location_name.x, location_id, 
                                              ineff = 0, ldipc = mean_value)], "~/ldipc_missing_locs.csv")
fwrite(x_wsubnats[location_name %in% c("Washington", "Georgia") & location_id !=35, .(location_name, location_id, 
                                              ineff = 0, ldipc = mean_value)], "~/USA_ldipc_missing_locs.csv")

x <- x[, .(location_id, ldipc = mean_value)]
x_wsubnats <- x_wsubnats[, .(location_id, ldipc = mean_value)]

# Merge xvar and yvar data
dt_all <- merge(dt, x, by = "location_id", all.x = T, all.y = T)
dt_all <- dt_all[!(is.na(total_pc_100))]
dt_all <- dt_all[!(is.na(ldipc))]
# Force in some nonzero yvar data variance, since the model needs this to run correctly
dt_all[, variance := (0.1*total_pc_100)^2]

# Save out all-country data for frontier
# fwrite(dt_all, "~/covid_frontier_data.csv")

# Merge on subnational data
dt_all_wsubnats <- merge(dt, x_wsubnats, by = "location_id", all.x = T, all.y = T)
dt_all_wsubnats <- dt_all_wsubnats[!(is.na(total_pc_100)) & !(is.na(ldipc))]
dt_all_wsubnats[, variance := (0.1*total_pc_100)^2]
# Save out all-location data, but drop testing outliers of LUX and ISL
fwrite(dt_all_wsubnats[!(location_name %in% c("Luxembourg", "Iceland"))], "~/covid_frontier_data_withsubnats_noLUXISL.csv")

# Save out dataset for only US Subnats
dt_usa <- dt_all_wsubnats[location_id %in% us_subnats_location_id]
fwrite(dt_usa, "~/covid_frontier_data_withUSAsubnats.csv")

# Save out missing locations out of all GBD locations
gbd_out <- glm_all[, .(location_id)]
locs_existing <- unique(dt_all_wsubnats$location_id)
gbd_out <- merge(gbd_out, x_wsubnats, by = "location_id", all.y = T, all.x = T)
gbd_out <- gbd_out[!(location_id %in% locs_existing)]
gbd_out <- merge(gbd_out, glm_all[, .(location_id, location_name)], by = "location_id", all.x = T)
fwrite(gbd_out[, .(location_name, location_id, ineff = 0, ldipc)], "~/GBD_ldipc_missing_locs.csv")


# Calculate initial knot placement: 4 evenly spaced knots between endpoints
# seq(min(dt_all$ldipc), max(dt_all$ldipc), length.out = 6)
