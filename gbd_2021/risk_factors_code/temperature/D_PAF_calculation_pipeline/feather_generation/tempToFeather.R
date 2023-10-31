#-------------------Header------------------------------------------------
# Author: EOD team
# Date: 2021
# Purpose: Script extracts era % for each day of each year and GBD location (subnationals); 
#          This is done so that code can be run in parallel 
#          The script also creates 100 draws of each daily temperature pixel using the spread provided in the ERA5 data set
#***************************************************************************

rm(list = ls())

# runtime configuration

if (Sys.info()["sysname"] == "Linux") {
  
  j <- "/home/j/" 
  h <- "/homes/USERNAME/"
  
} else { 
  
  j <- "J:"
  h <- "H:"
  
}

library(ncdf4)  
library(raster)
library(data.table)
library(rgdal)
library(feather)

arg <- commandArgs(trailingOnly = T)  #[-(1:5)]  # First args are for unix use only
YEAR <- arg[1]
locs <- arg[2]

#################### Read in shapefile ###############################################################
shapefile.dir <- file.path(j, "FILEPATH")
shapefile.version <- "GBD2021_analysis_final"
shape <- readOGR(dsn = shapefile.dir, layer = shapefile.version)

if (locs=="all") {
  locList <- unique(shape@data$loc_id)
} else {
  locList <- as.integer(strsplit(locs, "_")[[1]])
}


print(YEAR)
   
#outdir <- paste0(j,"FILEPATH/", YEAR)
outdir <- paste0("FILEPATH/", YEAR)
dir.create(outdir)
#end_day <- ifelse(YEAR==1992|YEAR==1996|YEAR==2000|YEAR==2004|YEAR==2008|YEAR==2012|YEAR==2016,366,365)
BRICK_temp <- brick(paste0("/FILEPATH", YEAR ,".gri"), overwrite=TRUE, format="raster")
BRICK_spread <- brick(paste0("/FILEPATH", YEAR ,".gri"), overwrite=TRUE, format="raster")

if (YEAR<2019){ 
  POP<-fread(paste0(j,"FILEPATH/world_pop_temp",YEAR,".csv"))
} else {
  POP<-fread(paste0(j,"FILEPATH/world_pop_temp",2018,".csv"))
}

POP <- rotate(rasterFromXYZ(POP[,c("longitude","latitude","population")]))
crs(BRICK_temp)   <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
crs(BRICK_spread) <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
crs(POP)          <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0" 


BRICK_spread <- resample(BRICK_spread, BRICK_temp)
POP <- resample(POP, BRICK_temp)

for (LOCATION_ID in locList) {
  warning(LOCATION_ID)
  sub_shape<-shape[shape@data$loc_id == LOCATION_ID,]
  
  temp_crop <- crop(BRICK_temp, sub_shape)
  spread_crop <- crop(BRICK_spread, sub_shape)
  pop_crop <- crop(POP, sub_shape)
  
  temp_sub  <- raster::mask(temp_crop, sub_shape)
  
  if(is.na(mean(temp_sub@data@values, na.rm=TRUE))==FALSE) {
    temp_crop   <- temp_sub
    spread_crop  <- raster::mask(spread_crop, sub_shape)
    pop_crop  <- mask(pop_crop,sub_shape)
  } 
  
  data_temp <- as.data.table(rasterToPoints(temp_crop, spatial = TRUE))
  data_temp <- melt(data_temp, id.vars = c("x", "y"), measure.vars = grep("layer", names(data_temp), value = T), value.name = "temp")
  
  
  data_spread <- as.data.table(rasterToPoints(spread_crop, spatial = TRUE))
  data_spread <- melt(data_spread, id.vars = c("x", "y"), measure.vars = grep("layer", names(data_spread), value = T), value.name = "spread")
  
  data_pop <- as.data.table(rasterToPoints(pop_crop, spatial = TRUE))

  rm(list = c("temp_crop", "spread_crop", "pop_crop", "temp_sub"))

  data <- merge(data_temp, data_spread, by = c("x", "y", "variable"), all.x=TRUE)
  data <- merge(data, data_pop, by = c("x", "y"), all.x = T)
  
  data[, loc_id := LOCATION_ID]
  data[, date := as.Date(0:(.N-1), origin=paste0(YEAR,"-01-01"))]
         
  data$spread[is.na(data$spread)] <- mean(data_spread$spread, na.rm=T)
  
  write_feather(data, paste0(outdir, "/era5_",YEAR,"_",LOCATION_ID,".feather"))
}

