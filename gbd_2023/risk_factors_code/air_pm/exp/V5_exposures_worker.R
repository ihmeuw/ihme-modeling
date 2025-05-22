# Looking at DIMAQ outputs and trying to replicate them with raw V5 data.

#### NOTES ####
# The format of the new file is intended to look like the original DIMAQ output.
# Draws are calculated directly using v5 uncertainties in conjunction with raw v5 data.
# It is not clear if some of the columns are relevant for downstream work or just leftover outputs from DIMAQ, nonetheless they are replicated.
# WorldPop raster is aggregated and resampled to the V5 data resolution, but the scaling factor is not perfect.
# Weights file comes from the DIMAQ prep code. We create a new IDGRID column to merge with the weights file (see the r0 raster) -- not sure if this is flawed.
# GBD analysis shapefile not used because of boundary issues. See notes at the bottom of the script.


#### IMPORTANT INFORMATION!!!! ####
# v5 data has a latitude limit that unfortunately cuts out the top subnational of Norway (id 60137)
# After running this script, you might see that files for 60137 don't exit
# Our current solution is to copy over the neighboring subnational Nordland (id 4926) to 60137
# CUSTOM SCRIPT INCLUDED  norway_subnational_quick_fix.R

#### MORE IMPORTANT INFORMATION!!!! ####
# The 1990 and 1995 data doesn't capture Tuvalu well, resulting in empty draws
# Current solution is just to copy over 1998 for these two years (see launcher script)

rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
  arg <- tail(commandArgs(),n=5) # First args are for unix use only
  n_draws <- 250
  GBD <- "GBD2022"
  out.dir <- "FILEPATH/gridded/v5/draws/"
  year <- 2002
} else {
  j_root <- "J:"
  h_root <- "H:"
  central_lib <- "FILEPATH"
  n_draws <- 250
  GBD <- "GBD2022"
  out.dir <- "FILEPATH/gridded/v5/draws/"
  year <- 2002
}

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

#----Directories and functions------------------------------------------------------------------------------------------
source(file.path("FILEPATH/get_model_results.R"))
source(file.path("FILEPATH/get_bundle_version.R"))
source(file.path("FILEPATH/get_location_metadata.R"))
source(file.path("FILEPATH/get_covariate_estimates.R"))
source(file.path("FILEPATH/get_population.R"))
source(file.path("FILEPATH/get_outputs.R"))
locs <- get_location_metadata(35,release_id=16)
source(file.path(h_root,"/FILEPATH/00_Source.R")) # needed for population weighted exposure outputs


n_draws <- arg[1] 
GBD <- arg[2]
out.dir <- arg[3]
out.sum.dir <- arg[4]
year <- arg[5]

# print args
print(args)
print(paste0("n_draws = ", n_draws))
print(paste0("GBD = ", GBD))
print(paste0("output directory = ", out.dir))
print(paste0("year = ", year))

# Filepaths
home.dir <- paste0("FILEPATH")
in.dir <- file.path(home.dir,"input")
v5.dir <- file.path(paste0("FILEPATH/V5_pm25_.1resolution/"))
v5_error.dir <- file.path(paste0("FILEPATH/uncertainty/"))
pop.in.dir <- "FILEPATH"
shapefile.dir <- "FILEPATH/shapefiles/"

# Location weights file generated in DIMAQ prep (this accounts for pixels that fall on borders and is used in PAF prep)
message("reading in location weights")
weights <- fread(file.path("FILEPATH/Weights.csv")) %>% 
  dplyr::select(-V1)

# Blank Raster for extending other rasters
r0 <- raster(xmx = 180,
             xmn = -180,
             ymx = 90,
             ymn = -90,
             res = 0.1)

r0[] <- 1:(dim(r0)[1]*dim(r0)[2])

# Read v5 data and uncertainties
message("reading in v5 data")


if (year %in% c(1990, 1995, 2000:2004)) {
  if (year == 1990 | year == 1995) {
    nc_1 <- brick(paste0(v5.dir, "gbd2021_inputs/NEW_SATnoGWR_",year,".nc"))
    nc_2 <- brick(paste0(v5_error.dir,"/NEW_SATnoGWR_",year,"_uncertainty.nc")) # error data
  } else {
    nc_1 <- brick(paste0(v5.dir, "gbd2021_inputs/NEW_SATnoGWR_",year,"-0.1.nc"))
    nc_2 <- brick(paste0(v5_error.dir,"/NEW_SATnoGWR_",year,"_uncertainty.nc")) # error data
  }
} else {
  nc_1 <- brick(paste0(v5.dir,"/V5GL04.HybridPM25c_0p10.Global.",year,"01-",year,"12.nc"))
  nc_2 <- brick(paste0(v5_error.dir,"/V5GL04.HybridPM25Ec_0p10.Global.",year,"01-",year,"12.nc")) # error data
}

nc_1 <- extend(nc_1, r0)
nc_2 <- extend(nc_2, r0)

# load in population raster and aggregate to v5 data
message("reading in population raster and aggregating to v5 data")
pop_raster <- brick(file.path(pop.in.dir,'worldpop_total_1y_2021_00_00.tif'))
aggregated_raster <- aggregate(pop_raster, fact = 3, fun = sum) # aggregates it to 0.123 degree resolution
resampled_raster <- resample(aggregated_raster, nc_1, method='bilinear') # resamples it to 0.1 degree resolution
correction_factor <- cellStats(pop_raster, sum, na.rm = TRUE) / cellStats(resampled_raster, sum, na.rm = TRUE) # calculate correction factor based on ratio of total sums
corrected_pop_raster <- resampled_raster * correction_factor # apply correction factor to resampled raster
pop_raster <- crop(corrected_pop_raster, extent(nc_1)) # match extent of other rasters

# load in urban raster
message("reading in urban raster")
urban_raster <- brick(file.path(in.dir,'Data/Raw/Misc/UrbanRural_LongLat_10km.tif'))
urban_raster <- extend(urban_raster, r0)

# Check the extent of r0
message("checking extents and resolutions of r0")
print(extent(r0))
print(res(r0))

# Check the extent of nc_1
message("checking extents and resolutions of nc_1")
print(extent(nc_1))
print(res(nc_1))

# Check the extent of nc_2
message("checking extents and resolutions of nc_2")
print(extent(nc_2))
print(res(nc_2))

# Check the extent of pop_raster
message("checking extents and resolutions of pop_raster")
print(extent(pop_raster))
print(res(pop_raster))

# Check the extent of urban_raster
message("checking extents and resolutions of urban_raster")
print(extent(urban_raster))
print(res(urban_raster))

# Stack the rasters
message("stacking rasters")
raster_stack <- stack(r0, nc_1, nc_2, pop_raster, urban_raster)

# rename the layers
names(raster_stack) <- c('IDGRID', 'mean', 'uncertainty', 'POP', 'Urban')

# Convert to dataframe
message("converting to dataframe")
data <- as.data.frame(raster_stack, xy = TRUE)

# drop rows where means are NA
data <- data[!is.na(data$mean),]

# this is where weights get merged in
message("merging weights")
data <- merge(data, weights, by = 'IDGRID')


# make the V5 data into a spatial vector file (i.e. not a raster anymore)
message("making V5 data into a spatial vector file")
wgs84 = '+proj=longlat +datum=WGS84'
data_sf <- st_as_sf(data, coords = c("x", "y"), crs = wgs84)

## add IHME GBD location metadata
relevant_locs <- locs %>% dplyr::select('region_name', 'super_region_name', 'location_id')

# get metadata
message("getting location metadata")
final_df <- merge(data_sf, relevant_locs, by='location_id', all.x = TRUE)

# rename columns to match previous GBD output (in case it matters)
colnames(final_df) <- c('location_id', 'IDGRID', 'mean', 'uncertainty', 'POP', 'Urban', 'Weight', 'location_name', 'GBDregion', 'GBDSuperRegion', 'geometry')

# add year_id column
final_df$year_id <- year
final_df$Global <- 'Global' # is this necessary?

# create draws
message("creating draws")
start_time <- Sys.time()
draws <- replicate(n_draws, final_df$mean + final_df$uncertainty * rnorm(nrow(final_df)), simplify = FALSE)
names(draws) <- paste0("draw_", seq_len(n_draws))
data_sf_draws <- dplyr::bind_cols(final_df, draws)
end_time <- Sys.time()
print(paste0("Time taken to create draws: ", end_time - start_time))

# convert geometry column back into lon/lat columns
message("converting geometry column back into lon/lat columns")
data_sf_draws <- st_as_sf(data_sf_draws)
data_sf_draws$Longitude <- st_coordinates(data_sf_draws)[,1] # Extract X coordinate
data_sf_draws$Latitude <- st_coordinates(data_sf_draws)[,2] # Extract Y coordinate

data_final <- data_sf_draws %>%
  st_set_geometry(NULL)

# reorganize everything
draw_columns <- grep("draw_", names(data_final), value = TRUE)
data_final <- data_final %>% dplyr::select(everything(), all_of(draw_columns))

message("grouping by location_id and writing to disk")
groups <- data_final %>% group_by(location_id) %>% group_split()

lapply(seq_along(groups), function(i) {
  filepath <- file.path(paste0(out.dir, groups[[i]]$location_id[1], "_", year, ".fst"))

  message("Writing ", filepath, " for location_id ", groups[[i]]$location_id[1], " and year ", year)

  write_fst(groups[[i]], path = filepath)
  # browser()
})
