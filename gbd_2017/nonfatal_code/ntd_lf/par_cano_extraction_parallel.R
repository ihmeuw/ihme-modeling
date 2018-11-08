####################################
# Date: 3/11/2016
# Purpose: Generate latitude population proportion covariates for GBD2015 locations, including subnationals
# Last edited: 20 December 2016
# updated for GBD 2017: 3 November 2017
####################################

# Clean workspace and setup
rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "FILEPATH"
} else {
  jpath <- "FILEPATH"
  package_lib <- paste0(jpath,'FILEPATH')
}
.libPaths(package_lib)

require(maptools)
require(raster)
require(rgdal)
require(data.table)


# Read location argument
print(paste('THE ARGS', as.character(commandArgs())))
ihme_lc_id <- as.character(commandArgs()[3])
#year <- as.numeric(commandArgs()[5])
#ihme_lc_id = "LVA" ## for testing

# Establish directories
envir_suit <- raster(paste0(jpath, "FILEPATH/lf_32bitv2.tif"))
shapefile_dir <- paste0(jpath,"FILEPATH")
save_dir <- paste0(jpath, "FILEPATH")
#save_dir <- ("FILEPATH") #test output directory

# Read in the county shapefile and project it onto the WGS84 coordinate system (same as the temperature grids)
world = readOGR(paste0(jpath, 'FILEPATH'), 'GBD2017_analysis_final')
proj4string(world) <- "+proj=longlat +datum=WGS84 +no_defs"

# Subset to location of interest
world<-world[!is.na(world$ihme_lc_id),]
geog<-world[world$ihme_lc_id==ihme_lc_id,]
loc_id = geog$loc_id

#define years
#years = c(1990, 1995, 2000, 2005, 2010, 2017)

years <- 2017

for (yr in years){

  # Read in the population counts file
  pop <- raster(paste0(jpath, "FILEPATH", "_00_00.tif"))
  # crop population raster to geog
  pop <- mask(crop(pop,geog),geog)

  #select out pop count for appropriate year
  totalpopcount <- as.matrix(pop)
  totalpopcount <- sum((totalpopcount), na.rm = T)

  # crop environmental suitability raster to geography
  envir_suit <- mask(crop(envir_suit,geog),geog)
  # if ti's 2017, stamp out TAS areas that have passed
  if(yr == 2017) {
    tas_raster <- raster(paste0(jpath, "FILEPATH/tas_raster.tif"))
    tas_raster <- mask(crop(tas_raster,geog),geog)
    pop <- pop * tas_raster
  }

  # multiply rasters to get raster of population counts in areas w/ 1 in elevation raster
  pop_envir <- pop*envir_suit

  # sum values from pop_elev to get pop_count
  pop_envir <- as.matrix(pop_envir)
  pop_count <- sum((pop_envir),na.rm=T)

  # get proportion of population in elevation category
  proportion <- pop_count/totalpopcount

  output <- data.frame(location_id = loc_id, ihme_loc_id=ihme_lc_id,year_id=yr,par=proportion)

  rm(list=c("pop"))
  gc()

  write.csv(output,paste0(FILEPATH, ".csv"),row.names=FALSE)
}
