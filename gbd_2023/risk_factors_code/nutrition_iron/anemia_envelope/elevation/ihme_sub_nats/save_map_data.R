# load in libraries -------------------------------------------------------

library(rgdal)
library(raster)
library(data.table)

# read in command line args -----------------------------------------------

command_args <- commandArgs(trailingOnly = TRUE)
output_directory <- command_args[1]

# read in files -----------------------------------------------------------

# load in admin2 level world plot
world <- rgdal::readOGR("FILEPATH/GBD2022_mapping_final.shp")
# reshape it to be the same dimensions as the raster files to be loaded in
world <- spTransform(world, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"))

admin_df <- as.data.table(world@data)

#load in elevation raster
elevation_map <- raster("FILEPATH/elevation_mean_synoptic.tif")

# write out image ---------------------------------------------------------

if(!(dir.exists(output_directory))){
  dir.create(output_directory)
}

file_name <- file.path(output_directory, "elevation_map_data.RData")
save(elevation_map, admin_df, world, file = file_name)