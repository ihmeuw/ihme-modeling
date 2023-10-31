##################################################
## Project: CVPDs
## Script purpose: Calculate seasonality for measles and flu using Regmod outputs
## Date: April 2021
## Author: username
##################################################
rm(list=ls())

pacman::p_load(data.table, openxlsx, ggplot2, plyr, dplyr)

username <- Sys.info()[["user"]]
date <- gsub("-", "_", Sys.Date())
gbd_round_id <- 7
decomp_step <- "step3"
cause <- "flu"

# ARGUMENTS -------------------------------------
out_dir <- paste0("/filepath/", date, "/", cause, "/")
if(!dir.exists(out_dir)) dir.create(out_dir, recursive = T)

# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("/filepath/", full.names = T), source))
source("/filepath/2019GBD_MAP.R")

bin_latitude <- function(dt, latitude_bins, lat_column_name){ # dt must have a column called latitude that contains the latitude value that you want to bin for each row
  dt[, lat_bin_start := sapply(get(lat_column_name), function(x) {max(latitude_bins[bins < x, bins])})]
  dt[, lat_bin_end := sapply(get(lat_column_name), function(x) {min(latitude_bins[bins >= x, bins])})]
  dt[, lat_bin := paste0(">", lat_bin_start, "to", lat_bin_end)]
  return(dt)
}

# get location data
hierarchy <- get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
countries <- hierarchy[level==3]

# Read in and prep RegMod outputs (no need to drop bad locs since we are using 2017-19 only)
regmod_data <- fread(paste0("FILEPATH"))#
setnames(regmod_data, c("deaths","cases"), c("observed_cases", "predicted_cases"))
regmod_data[, cause := cause]
regmod_data <- regmod_data[year_id >= reference_pd_start & year_id <= 2019]
regmod_data <- regmod_data[, which(!names(regmod_data) %like% "draw"), with = F]
# Keep only regmod data with all months for *at least* one year
# Group 2 locations must also have ALL months present in ANY year
completeness_chk <- regmod_data[,.N, by=c("location_id", "year_id")]
regmod_data <- regmod_data[location_id %in% completeness_chk[N==12]$location_id]

# Drop locations with at least 6 months of missing data in any yr from start of reference period to end of predicting for all etios
if(Sys.Date() > as.Date("2022-09-20")){
  regmod_data[, sumNA := sum(is.na(observed_cases)), by = c("location_id", "year_id")]
  regmod_data <- regmod_data[year_id %in% reference_pd_start:2021][, max_missing := max(sumNA), by = "location_id"]
  locs_drop <- unique(regmod_data[max_missing > 6]$location_id) # 67 when go back to 2015
  regmod_data <- regmod_data[!(location_id %in% locs_drop)]
}

# Generate latitude-binned RegMod outputs
latitude_cutoffs <- data.table(bins = c(-90, -30, -15, 0, 15, 30, 90))
lat_data <- get_covariate_estimates(covariate_id = 56,
                                    location_id = "all",
                                    year_id = 2020,
                                    gbd_round_id = gbd_round_id,
                                    decomp_step = decomp_step)
lat_data <- lat_data[, .(location_id, mean_value)]
setnames(lat_data, "mean_value", "latitude")
regmod_data <- merge(regmod_data, lat_data, by = "location_id")
regmod_data <- bin_latitude(regmod_data, latitude_bins = latitude_cutoffs, lat_column_name = "latitude")
regmod_copy <- copy(regmod_data)

# Take monthly cases divided by annual average (by year)
regmod_data[, average_cases := mean(predicted_cases, na.rm = T), by = c("location_id", "year_id")]
# Get the ratio (seasonality value in given month)
regmod_data[, ratio := predicted_cases/average_cases]
# Average the ratios across 2017-2019
regmod_data[, loc_ratio := mean(ratio), by = c("location_id", "month")]
# Squeeze the averages to 1 - this is necessary for Flu, where not every year has a full time series
regmod_data[, scale_factor := 12/sum(unique(loc_ratio)), by = c("location_id")]
regmod_data[, loc_ratio := loc_ratio*scale_factor]
# Get a latitude-bin-wide average to use for Group 3 locations
regmod_data[, lat_ratio := mean(ratio), by = c("lat_bin", "month")]
regmod_data[, scale_factor_lat := 12/sum(unique(lat_ratio)), by = c("lat_bin")]
regmod_data[, lat_ratio := lat_ratio*scale_factor_lat]
regmod_data[, weighted_lat_ratio := 1]
# Write out the latitude-binned seasonality
write.xlsx(unique(regmod_data[,.(month, lat_ratio, weighted_lat_ratio, lat_bin)]),
           paste0(out_dir, "latitude_binned_seasonality.xlsx"))

# Calculate seasonality weights for each location set
# Group 1 - NOT USED when calculating only the seasonality
# Group 2 - take country-specific RegMod weights from 2017-2019
# Group 3 - Take latitude-binned average seasonality from RegMod

# Group 2: locations that are not trusted BUT have RegMod outputs (ALSO INCLUDES Trusted locations with RegMod outputs, so they get a seasonality)
grp2_loc_cn <- unique(regmod_data[,.(location_id, month, cause, loc_ratio, lat_ratio, weighted_lat_ratio)]) # keep one row instead of 3 rows, one for each year

# Group 3 - countries without complete RegMod data from 2017-2019
grp3_loc_cn <- expand.grid(location_id = countries[!(location_id %in% c(grp2_loc_cn$location_id)), location_id],
                           month = 1:12,
                           cause = cause)
setDT(grp3_loc_cn)
grp3_loc_cn <- merge(grp3_loc_cn, lat_data, by = "location_id")
grp3_loc_cn <- bin_latitude(grp3_loc_cn, latitude_bins = latitude_cutoffs, lat_column_name = "latitude")
# merge on the binned seasonality
grp3_loc_cn <- merge(grp3_loc_cn, unique(regmod_data[,.(month, lat_ratio, weighted_lat_ratio, lat_bin)]), by = c("lat_bin", "month"))

# Run checks
# Is there any overlap in locations between the data tables
if (any(intersect(grp2_loc_cn$location_id, grp3_loc_cn$location_id))) stop("Location groups are not mutually exclusive")
# Are all GBD national locations represented
if (any(!countries$location_id %in% union(grp2_loc_cn$location_id, grp3_loc_cn$location_id))) stop("Some GBD national locs missing")

# If locs are good, bind Group 2 and Group 3 DTs together
est_cases <- rbind(grp2_loc_cn, grp3_loc_cn, fill = T)

# Use loc_ratio as ratio if present, otherwise, use lat_ratio
est_cases[!is.na(loc_ratio), wt := loc_ratio]
est_cases[is.na(loc_ratio), wt := lat_ratio]

# Check that all the weights average to 1 for a given location
est_cases[, check := mean(wt), by = "location_id"]
if(any(round(unique(est_cases$check),3)!=1)) stop("Seasonality ratios do not average to 1")
# Write out all locations' seasonality
est_cases <- merge(est_cases, hierarchy[,.(location_name, location_id, super_region_name)], by = "location_id")
write.xlsx(est_cases, paste0(out_dir, "all_locs_seasonality.xlsx"))

# Make some plots of the seasonality
seasonality_prediction <- function(my_work_dir, sr_name, dt, hierarchy){
  
  pdf(paste0(my_work_dir, "/", sr_name, "_prediction_on_seasonality_by_country.pdf"), width = 8, height = 5)
  
  # one page for each country that we have measles OR flu data for, showing fit to overall data and fit to country data. If did for(location in countries), would have some pages with only SR and GLB fits, which isn't what we want to vet here
  for(location in unique(dt[location_id %in% countries$location_id & super_region_name == sr_name, location_id])){ #
    dt_tmp <- copy(dt[location_id == location])
    if (nrow(dt_tmp)<1) next
    my_location_name <- hierarchy[location_id == location, location_name ]
    message(my_location_name)
    
    # plot smooth fits (from simulated mob data -- is.na(cum_mob)) for each cause
    seasonality_pred <- ggplot(dt_tmp) + 
      geom_line(aes(x = month, y = loc_ratio, color = "Location-Specific Regmod Modeled Seasonality")) + 
      geom_line(aes(x = month, y = lat_ratio, color = "Latitude Binned Regmod Modeled Seasonality")) + 
      geom_line(aes(x = month, y = weighted_lat_ratio, color = "Population Weighted Latitude Binned Regmod Modeled Seasonality")) + 
      labs(title = paste(my_location_name, cause, "Versions of Seasonality"),
           x = "Month",
           y = "Seasonality") +
      theme_bw() +
      theme(text = element_text(size = 10)) +
      scale_x_continuous(limits = c(1,12))
    print(seasonality_pred)
  }
  dev.off()
}

lapply(unique(est_cases$super_region_name), seasonality_prediction, my_work_dir = out_dir, dt = est_cases, hierarchy = hierarchy)
