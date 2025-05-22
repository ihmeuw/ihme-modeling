# rescale 1990, 1995, and 2000-2005 using overlapping years of exposure outputs between GBD2021 and now
# this script comes after running v5_exposures_worker.R
# because of location differences between GBD2021 and GBD2023, we will be missing the new locations for these years...

rm(list=ls())

script_start_time <- Sys.time()

library(data.table)
library(dplyr)
library(fst)
library(pbapply)
library(magrittr)
library(ggplot2)
library(raster)
library(ncdf4)
library(sf)
library(magick)
library(feather)
library(parallel)

if (interactive()) {
  loc_id <- 95069
  n_draws <- 250
  # year <- 2004
} else {
  arg <- tail(commandArgs())
  print(paste0("args are: ", arg))
  loc_id <- arg[5]
  n_draws <- arg[6]
  # year <- arg[3]
}

smooth_2006_2008 <- TRUE

# filepaths
gbd2021_dir <- "FILEPATH/exp/gridded/48/draws/"
current_dir <- "FILEPATH/exp/gridded/v5_4/draws/"

print(paste0("loc_id = ", loc_id))
print(paste0("n_draws = ", n_draws))

# years <- c(2005:2020)
years <- c(2006:2020)

############## GBD 2021 ##############
ndraws_gbd2021 <- 1000
pred_cols <- paste0("pred_", 1:ndraws_gbd2021)

# this chunk is adapted from the save_results.R script, it basically makes it easy to get location means
# we are not using population weighted here because that happens for all exposure outputs in the save_results.R script
save_draws_2021 <- function(year){
  if (loc_id == 60908 | loc_id == 95069 | loc_id == 94364) {
    
    new_loc_id <- 179 # ethiopia
  
    data <- read.fst(paste0(gbd2021_dir,new_loc_id,'_',year,'.fst')) %>% as.data.table
    exp <- sapply(1:ndraws_gbd2021,function(draw.number){mean(data[[pred_cols[draw.number]]], na.rm = TRUE)})
    exp <- mean(exp, na.rm = TRUE)
    exp <- as.data.table(exp)
    exp$location_id <- data[1,"location_id"]
    exp$year_id <- data[1,"year_id"]
    
    exp$location_id <- loc_id # apply subnational location_id for ethiopia GBD 2021
  
  } else {
    
    data <- read.fst(paste0(gbd2021_dir,loc_id,'_',year,'.fst')) %>% as.data.table
    exp <- sapply(1:ndraws_gbd2021,function(draw.number){mean(data[[pred_cols[draw.number]]], na.rm = TRUE)})
    exp <- mean(exp, na.rm = TRUE)
    exp <- as.data.table(exp)
    exp$location_id <- data[1,"location_id"]
    exp$year_id <- data[1,"year_id"]
    
  }
  
  return(exp)
}

print(paste0("Reading GBD 2021 exposures for location ", loc_id))
cores.provided <- 50
save_2021 <- pblapply(years, save_draws_2021, cl=cores.provided)
out_exp_2021 <- rbindlist(save_2021)

############## v5 ##############

pred_cols <- paste0("draw_", 1:n_draws)

save_draws_v5 <- function(year){
  data <- read.fst(paste0(current_dir,loc_id,'_',year,'.fst')) %>% as.data.table
  exp <- sapply(1:n_draws,function(draw.number){mean(data[[pred_cols[draw.number]]], na.rm = TRUE)})
  exp <- mean(exp, na.rm = TRUE)
  exp <- as.data.table(exp)
  exp$location_id <- data[1,"location_id"]
  exp$year_id <- data[1,"year_id"]
  return(exp)
}

print(paste0("Reading GBD 2023 exposures for location ", loc_id))
cores.provided <- 50
save <- pblapply(years, save_draws_v5, cl=cores.provided)
out_exp <- rbindlist(save)

############## rescale ##############

combined <- merge(out_exp_2021, out_exp, by=c("location_id", "year_id"), suffixes=c("_2021", "_v5"))
combined$ratio <- combined$exp_v5 / combined$exp_2021

scale_factor <- mean(combined$ratio)

# rescale 1990, 1995, 2000-2004

years <- c(1990, 1995, 2000:2005) 

draw_scaling_factor <- as.numeric(ndraws_gbd2021) / as.numeric(n_draws) # need to convert 1000 draws to whatever is needed for upload...

rescale_gbd2021_to_v5 <- function(year){
  
  if (loc_id == 60908 | loc_id == 95069 | loc_id == 94364) {
    
    new_loc_id <- 179 # ethiopia
  
    data <- read.fst(paste0(gbd2021_dir,new_loc_id,'_',year,'.fst')) %>% as.data.table # testing gbd21 outputs
    
    # multiply scaling factor to all draws columns
    data[, paste0("pred_", 1:1000) := lapply(.SD, function(x) x * scale_factor), .SDcols = paste0("pred_", 1:1000)] # testing gbd21 outputs
    
    # rename pred_ columns to draw_ columns
    setnames(data, old = paste0("pred_", 1:1000), new = paste0("draw_", 1:1000)) # testing gbd21 outputs
  
    # resample draws (for GBD 2021 outputs only)
    draw_cols_only <- dplyr::select(data, starts_with("draw_")) # get draw cols
    selected_indices <- draw_cols_only[, c(paste0("draw_", seq(1, 1000, by = draw_scaling_factor)))] # select every nth draw (this method works well enough to capture original distribution)
    reduced_df <- draw_cols_only[, ..selected_indices]
    setnames(reduced_df, old = paste0("draw_", seq(1, 1000, by = draw_scaling_factor)), new = paste0("draw_", seq(1, as.numeric(n_draws), by = 1))) # rename columns to match new draw count
  
    # drop old draw columns and replace with new ones
    data <- data[, !grepl("draw_", names(data)), with = FALSE]
    data <- cbind(data, reduced_df)
    
    data$location_id <- loc_id # apply subnational location_id for ethiopia GBD 2021
  
  } else {
    
    data <- read.fst(paste0(gbd2021_dir,loc_id,'_',year,'.fst')) %>% as.data.table # testing gbd21 outputs
    
    # multiply scaling factor to all draws columns
    data[, paste0("pred_", 1:1000) := lapply(.SD, function(x) x * scale_factor), .SDcols = paste0("pred_", 1:1000)] # testing gbd21 outputs
    
    # rename pred_ columns to draw_ columns
    setnames(data, old = paste0("pred_", 1:1000), new = paste0("draw_", 1:1000)) # testing gbd21 outputs
    
    # resample draws (for GBD 2021 outputs only)
    draw_cols_only <- dplyr::select(data, starts_with("draw_")) # get draw cols
    selected_indices <- draw_cols_only[, c(paste0("draw_", seq(1, 1000, by = draw_scaling_factor)))] # select every nth draw (this method works well enough to capture original distribution)
    reduced_df <- draw_cols_only[, ..selected_indices]
    setnames(reduced_df, old = paste0("draw_", seq(1, 1000, by = draw_scaling_factor)), new = paste0("draw_", seq(1, as.numeric(n_draws), by = 1))) # rename columns to match new draw count
  
    # drop old draw columns and replace with new ones
    data <- data[, !grepl("draw_", names(data)), with = FALSE]
    data <- cbind(data, reduced_df)
  }
    
  new_dir <- paste0("FILEPATH/exp/gridded/v5_4/draws_rescaled_gbd2021_including_2005_testing/") # testing gbd21 outputs wtih 2005
  
  if (!dir.exists(new_dir)) {
    dir.create(new_dir, recursive = TRUE)
  }
  
  write.fst(data, paste0(new_dir,loc_id,'_',year,'.fst'))
  
  # return(data)
}

print(paste0("Rescaling GBD 2021 exposures for location ", loc_id))
cores.provided <- 50
pblapply(years, rescale_gbd2021_to_v5, cl=cores.provided)

script_end_time <- Sys.time()

print(paste0("Finished rescaling for location ", loc_id, " in ", script_end_time - script_start_time))

#############################################################################################

if (smooth_2006_2008 == TRUE) {
  # attempt to smooth out 2005-2008 transition
  
  data2005 <- read.fst(paste0("FILEPATH/exp/gridded/v5_4/draws_rescaled_gbd2021_including_2005_testing/",loc_id,'_2005.fst')) %>% as.data.table
  data2005$IDGRID <- as.numeric(data2005$IDGRID)
  data2006 <- read.fst(paste0("FILEPATH/exp/gridded/v5_4/draws/",loc_id,'_2006.fst')) %>% as.data.table
  data2007 <- read.fst(paste0("FILEPATH/exp/gridded/v5_4/draws/",loc_id,'_2007.fst')) %>% as.data.table
  data2008 <- read.fst(paste0("FILEPATH/exp/gridded/v5_4/draws/",loc_id,'_2008.fst')) %>% as.data.table
  data2009 <- read.fst(paste0("FILEPATH/exp/gridded/v5_4/draws/",loc_id,'_2009.fst')) %>% as.data.table
  
  # Define years of interest
  years <- 2005:2009
  
  # Initialize an empty list to store data frames for final output
  list_final_data <- list()
  
  # Loop over the years, excluding the first and last since they don't have both previous and next years
  for(i in 2:(length(years)-1)) {
    current_year <- years[i]
    prev_year <- years[i-1]
    next_year <- years[i+1]
    
    # Read and melt data for the current year, previous year, and next year
    data_list <- lapply(c(prev_year, current_year, next_year), function(year) {
      file_path <- paste0("FILEPATH/exp/gridded/v5_4/draws", ifelse(year == 2005, "_rescaled_gbd2021_including_2005_testing/", "/"), loc_id, '_', year, '.fst')
      data <- read.fst(file_path) %>% as.data.table()
      data$IDGRID <- as.numeric(data$IDGRID)
      melted_data <- melt(data, id.vars = "IDGRID", measure.vars = paste0("draw_", 1:n_draws))
      melted_data$year <- year  # Add year for identification
      return(melted_data)
    })
    
    # Combine the data for the three years
    combined_data <- rbindlist(data_list)
    
    # Calculate the mean for each IDGRID and variable across the three years
    combined_data[, mean_value := mean(value, na.rm = TRUE), by = .(IDGRID, variable)]
    
    # Select the unique rows for the current year after calculating the mean
    unique_combined_data <- unique(combined_data[year == current_year, .(IDGRID, variable, mean_value)])
    
    # Reshape back to wide format with mean_value being the new draw value for the current year
    combined_wide <- dcast(unique_combined_data, IDGRID ~ variable, value.var = "mean_value")
    
    # Store the combined wide data frame in the list with the year as the name
    list_final_data[[as.character(current_year)]] <- combined_wide
  }
  
  # combine list
  test_2006 <- list_final_data[["2006"]]
  test_2007 <- list_final_data[["2007"]]
  test_2008 <- list_final_data[["2008"]]
  
  new_dir <- paste0("FILEPATH/exp/gridded/v5_4/draws_rescaled_gbd2021_rolling_avg_2006_2008/") # testing gbd21 outputs with rolling average
  
  if (!dir.exists(new_dir)) {
    dir.create(new_dir, recursive = TRUE)
  }
  
  # update data for 2006-2008
  data2006_other <- dplyr::select(data2006, -starts_with("draw_"))
  data2006_final <- merge(data2006_other, test_2006, by="IDGRID")
  write.fst(data2006_final, paste0(new_dir,loc_id,'_',2006,'.fst'))
  
  data2007_other <- dplyr::select(data2007, -starts_with("draw_"))
  data2007_final <- merge(data2007_other, test_2007, by="IDGRID")
  write.fst(data2007_final, paste0(new_dir,loc_id,'_',2007,'.fst'))
  
  data2008_other <- dplyr::select(data2008, -starts_with("draw_"))
  data2008_final <- merge(data2008_other, test_2008, by="IDGRID")
  write.fst(data2008_final, paste0(new_dir,loc_id,'_',2008,'.fst'))
}


