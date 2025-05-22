# attempting to get some understanding of uncertainty between rasters in order to extrapolate uncertainty to the past

# load the data (raster .nc files)

library(data.table)
library(dplyr)
library(fst)
library(pbapply)
library(magrittr)
library(ggplot2)
library(raster)
library(terra)
library(ncdf4)
library(sf)
library(magick)
library(feather)

old.in.dir <- 'FILEPATH/exp/input/Data/Raw/V5_pm25_.1resolution/gbd2021_inputs/'
new.in.dir <- 'FILEPATH/exp/input/Data/Raw/V5_pm25_.1resolution/'
new.unc.in.dir <- 'FILEPATH/exp/input/Data/Raw/V5_pm25_.1resolution/uncertainty/'

# years <- c(2000:2020)
years <- c(2005:2020)

data <- list()
unc <- list()

# load in uncertainty datasets
for (i in 1:length(years)) {
  year <- years[i]
  
  file_data <- paste0(new.in.dir, 'V5GL04.HybridPM25c_0p10.Global.', year, '01-', year, '12.nc')
  file_unc_new <- paste0(new.unc.in.dir, 'V5GL04.HybridPM25Ec_0p10.Global.', year, '01-', year, '12.nc')
  data[i] <- terra::rast(file_data)
  unc[i] <- terra::rast(file_unc_new)
  
}

all_data_values <- vector("list", length(years))
all_unc_values <- vector("list", length(years))

for (i in 1:length(years)) {
  all_data_values[[i]] <- values(data[[i]])#, na.rm = TRUE)
  all_unc_values[[i]] <- values(unc[[i]])#, na.rm = TRUE)
}

# Combine all years into one vector each
all_data_values <- unlist(all_data_values)
all_unc_values <- unlist(all_unc_values)

# model
model <- lm(all_unc_values ~ all_data_values)

# summary(model)

# Create a data frame from your vectors for plotting
plot_data <- data.frame(all_data_values, all_unc_values)

set.seed(123)  # Set a seed for reproducibility
sample_size <- 100000  # Adjust based on your needs
plot_data_sample <- plot_data[sample(nrow(plot_data), sample_size), ]

ggplot(plot_data_sample, aes(x = all_data_values, y = all_unc_values)) +
  geom_point(alpha = 0.5) +
  geom_smooth(method = "lm", color = "blue", se = FALSE) +
  labs(x = "Data Values", y = "Uncertainty Values", title = "Sampled Data Values vs. Uncertainties") +
  theme_minimal()

# predict uncertainty
earlier_years <- c(1990, 1995, 2000:2004)

for (year in earlier_years) {
  if (year == 1990 | year == 1995) {
    file_data_early <- paste0(old.in.dir, 'NEW_SATnoGWR_', year, '.nc')
  } else {
    file_data_early <- paste0(old.in.dir, 'NEW_SATnoGWR_', year, '-0.1.nc')
  }
  
  raster_early <- terra::rast(file_data_early)
  
  # Assuming you have a model fitted without removing NaNs from the spatial structure
  # and 'raster_early' is the raster for which you're predicting uncertainty
  
  # 1. Extract values from the raster while keeping NaNs
  early_data_values <- values(raster_early)
  
  # 2. Prepare a template for predictions that includes NaNs
  predicted_uncertainty_values <- early_data_values  # Copy including NaNs
  
  # 3. Predict only for non-NaN values
  non_nan_indices <- which(!is.na(early_data_values))
  predictions <- predict(model, newdata=data.frame(all_data_values=early_data_values[non_nan_indices]))
  
  # 4. Fill in predictions in the non-NaN positions
  predicted_uncertainty_values[non_nan_indices] <- predictions
  
  # 5. Create a new raster for the predicted uncertainty with the same dimensions and projection as 'raster_early'
  predicted_uncertainty_raster <- raster_early
  values(predicted_uncertainty_raster) <- predicted_uncertainty_values

  # Now, 'predicted_uncertainty_raster' should maintain the spatial integrity and be comprehensible
  
  # browser()
  
  # Save the new uncertainty raster
  out_filename <- paste0(new.unc.in.dir, "NEW_SATnoGWR_", year, "_uncertainty.nc")
  # browser()
  writeRaster(uncertainty_raster, out_filename, overwrite=TRUE)
}


