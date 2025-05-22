# Calculation the proportion of urban

# clear memory
rm(list=ls())

# libraries ####################################################
library(raster)
library(readr)
library(sf)
library(arrow)
library(data.table)
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")

# values ########################################################
arg <- commandArgs(trailingOnly = T)
print(arg)

release<-as.numeric(arg[1])
loc_set<-as.numeric(arg[2])
gbd_cycle<-as.character(arg[3])
gbdcycle<-as.character(arg[4])
urb_date<-as.character(arg[5])
pop_date<-as.character(arg[6])
out.dir<-as.character(arg[7])
grid_yr<-as.numeric(arg[8])
loc<-as.numeric(arg[9])

# values for STGPR input
nid<-564313
age_group<-22 #age_group_id
var<-0.0001 #variance
sex<-3 #sex_id
measure<-18 #measure_id, proportion

## directories###############
#global shapefile directory
shapefile.dir <- paste0("FILEPATH/",gbd_cycle,"/FILEPATH")
shapefile.version <- paste0(gbdcycle,"FILEPATH") # loc_set_22 is covariates location set

# population directory
pop.dir<-file.path("FILEPATH",pop_date,"FILEPATH")
pop_files<-list.files(pop.dir,pattern = "\\.tif$",full.names = T)
#only keep the years that we need
pop_files <- pop_files[sapply(pop_files, function(filepath) any(sapply(grid_yr, function(year) grepl(year, filepath))))]

# raster dt (urban and pop) directory
data<-as.data.table(read_feather(paste0(out.dir,"FILEPATH",grid_yr,"FILEPATH")))

# functions ####################################################

# Function to round coordinates to a specified number of digits
# This helps avoid floating point errors when merging on coordinates
round_coords <- function(dt, digits = 4, coord_vars = c('x', 'y')) {
  multiplier <- 10^digits
  dt[, c(coord_vars) := lapply(.SD, function(x) as.integer(round(x, digits = digits) * multiplier)), 
     .SDcols = coord_vars]
  return(dt)
}

# Function to convert a raster to a data.table
# We'll want all the data in standard format for easy manipulation, merging, and math

raster_to_datatable <- function(x) {
  
  # Convert the raster to a data.table
  x <- as.data.table(rasterToPoints(x, spatial = TRUE))

  # Round coordinates to avoid floating point merge mismatches
  x <- round_coords(x)
  return(x)
}

# For very small locations (e.g. islands) we may lose all pixels if we mask to the exact location polygon, 
# for these locations we'll buffer the polygon until we pick up pixels with populations

find_pixels <- function(x, sub_shape, ref_res) {
  while (TRUE) {
    test_pixels <- raster::mask(x, sub_shape)
    test_values <- as.numeric(values(test_pixels))

    
    if (sum(test_values, na.rm = T)<1) {
      sub_shape <- st_buffer(sub_shape, units::set_units(ref_res, degree)*2)
    } else {
      return(list(test_pixels = test_pixels, sub_shape = sub_shape))
    }
  }
}


# Create a reference raster ###################################
#read any one of the pop files to use as a reference. We only need one of them as the borders of the countries would not have changed over time.
# we only need the reference so we can rescale (also called resample) the other rasters since the extent of the urbanicity rasters does not line up with 
# the pop rasters. We are using the pop rasters as the reference because it has the smaller extent, and during resampling it is either upsampling
# or downsampling. During this process the program is guessing what the values of the cells should be.

#load in the raster 
ref<-raster(pop_files[1])

#now get the resolution of the reference
ref_res<-res(ref)[1]

# # Borders and pop ###########################################################
# # load in the borders for the countries
borders<-sf::read_sf(shapefile.dir, layer = shapefile.version)
borders$location_id <- borders$loc_id # this variable is misnamed, fix it here
borders$ihme_loc_id <- borders$ihme_lc_id # ditto

#grab population to use as the sample size later
pop<-get_population(age_group_id = age_group, year_id = grid_yr, sex_id = sex, release_id = release, location_id = loc, 
                    location_set_id = loc_set)[,-c("run_id")]
setnames(pop,"population","sample_size")


# Calc prop ###########################################################

  print(loc)
  
  print("borders")
  
  #get the borders of the location
  country_border<-borders[borders$location_id==loc,]
  
  print("cells")
  #find the pixels associated with that location
  cells<-find_pixels(ref,country_border,ref_res)
  
  print("cells dt")
  #convert cells into a datatable
  cells<-raster_to_datatable(cells$test_pixels)
  
  #remove the first column
  cells[,1:=NULL]
  
  print("merge cells")
  #Merge the coordinates with the data to only get the needed country
  data_temp<-merge(cells,data,by=c("x","y"),all.x=T)
  
  #if weight is NA, fill it in with 0
  data_temp[is.na(weight),weight:=0]
  
  print("mean")
  #Calc the weighted mean
  data_temp[,mean:=weighted.mean(weight,pop,na.rm=T),by=c("year_id")]
  
  #clean up
  data_temp[,location_id:=loc]
  data_temp<-data_temp[,.(location_id,year_id,mean)]
  data_temp<-unique(data_temp)
  
  print("stgpr edits")
  # STGPR edits ######################################
data_temp[,':='(nid=nid,
             age_group_id=age_group, 
             variance=var, 
             sex_id=sex, 
             is_outlier=0, 
             measure_id=measure)]
setnames(data_temp,"mean","val")

print("merge pop")
#add sample size which is population
data_final<-merge(data_temp,pop,by=c("age_group_id","location_id","year_id","sex_id"),all.x=T)

print("save")
# save final #####################################
write_feather(data_final,paste0(out.dir,"FILEPATH",grid_yr,"FILEPATH",grid_yr,"FILEPATH",loc,"FILEPATH"))












