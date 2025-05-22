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

## directories###############
# population directory
pop.dir<-file.path("FILEPATH",pop_date,"FILEPATH")
pop_files<-list.files(pop.dir,pattern = "\\.tif$",full.names = T)
#only keep the years that we need
pop_files <- pop_files[sapply(pop_files, function(filepath) any(sapply(grid_yr, function(year) grepl(year, filepath))))]

#urbanicity categorical directory
urb.dir<-paste0("FILEPATH/",urb_date,"/FILEPATH")
urb_files<-list.files(urb.dir,pattern = "\\.tif$",full.names = T)
#only keep the years that we need
urb_files <- urb_files[sapply(urb_files, function(filepath) any(sapply(grid_yr, function(year) grepl(year, filepath))))]

# functions ####################################################

# Function to read in a raster, and optionally resample
# We use this to ensure that all imported rasters are in the same format and pixels align

prep_raster <- function(fname, ref, resample = FALSE, expand_by = 100) {
  message('Reading raster...')
  rst<-raster(fname)

  if (resample) {
    message('Resampling raster...')
    rst <- raster::resample(rst, ref, method = 'ngb')
  }

  message('Done.')
  return(rst)
}

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

# Prep and convert rasters to DT ###############################################

    urb_data <- prep_raster(fname = urb_files, ref = ref, resample = TRUE)
    pop_data <- prep_raster(fname = pop_files, resample = FALSE)
  
  #convert the raster to a data.table
  urb_data<-raster_to_datatable(urb_data)
  pop_data<-raster_to_datatable(pop_data)
  
  #update column names
  urb_data[,year:=grid_yr]
  pop_data[,year:=grid_yr]
  
  
  setnames(urb_data,1,"urb_class")
  setnames(pop_data,1,"pop")

# Urban Weights #############################################################
#Add weights for each urban category
#these urban category (class) numbers are from the SMOD legend: https://human-settlement.emergency.copernicus.eu/ghs_smod2023.php
#the population and density values are from its methods: https://op.europa.eu/en/publication-detail/-/publication/ce98358f-1a32-11ea-8c1f-01aa75ed71a1/language-en
urb_weight <- data.table(urb_class=c(30,23), 
                         weight=c(1,1))

#add to urban data
urb_data<-merge(urb_data,urb_weight,by=c("urb_class"),all.x=T)

#fill in all other classes with a 0 if the weight is empty
urb_data[is.na(weight),weight:=0]

#merge the urb and pop data into 1 table
data<-merge(urb_data,pop_data,by=c("x","y","year"),all=T)
setnames(data,"year","year_id")

#save 
write_feather(data,paste0(out.dir,"/FILEPATH",grid_yr,"FILEPATH"))


