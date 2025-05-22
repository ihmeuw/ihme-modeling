#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: 
# Purpose: generate location-specific air_no2 exposure files and bin them for PAF calculator

#----Set-up-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

project <- "proj_erf"

# set parameters based on arguments from parent script
arg <- commandArgs(trailingOnly = T)

loc_id <- arg[1]
year <- arg[2]
draws_required <- arg[3]
draws_required <- as.numeric(draws_required)
gbd_round_id <- arg[4]
grid.version <- arg[5]
release_id <- arg[6]

if (interactive()){
  loc_id <- 349 #83
  year <- 2020 # 2000 # "covid_cf"
  draws_required <- 250 # 1000
  gbd_round_id <- 9 # 7
  grid.version <- 10 # 9
  release_id <- 16
}

# GBD 2023
years_larkin <- c(1990,1995,2000,2005:2019) # rescaled gbd2021 inputs
years_lur <- c(2020:2023) # new data

packages <- c("ncdf4","raster","data.table","rgdal","magrittr","zoo","parallel","terra")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}


# Must change debug in script itself, since used for running line by line
debug <- T # F

# Output the settings into the log for later reference
warning(paste0("Location      = ", loc_id))
warning(paste0("Year          = ", year))
warning(paste0("Draws         = ", draws_required))
warning(paste0("GBD round     = ", gbd_round_id))
warning(paste0("Grid version  = ", grid.version))

#----Directories--------------------------------------------------------------------------------------------------------

home_dir <- "FILEPATH"
in_dir <- paste0(home_dir,"input/")

lbd_dir <- "FILEPATH/worldpop/total/2024_05_10/1y/"

out_dir <- paste0("FILEPATH/air_no2/exp/gridded/",grid.version)
dir.create(out_dir,recursive = T,showWarnings = F)

source(file.path(central_lib,"current/r/get_location_metadata.R"))
# locs <- get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
locs <- get_location_metadata(35, gbd_round_id = gbd_round_id, release_id = release_id) %>% as.data.table
locs <- locs[most_detailed==1]

draw_cols <- paste0("draw_",1:draws_required) # befor ewas 1:1000


### READ IN SHAPEFILE ###

start <- Sys.time()

if (gbd_round_id==9) {
  shapefile.dir <- file.path(j_root, "FILEPATH")
  shapefile.version <- "GBD2023_analysis_final"
} else if (gbd_round_id==7) {
  shapefile.dir <- file.path(j_root, "FILEPATH")
  shapefile.version <- "GBD2021_analysis_final"
}

shape <- readOGR(dsn = shapefile.dir, layer = shapefile.version)

end <- Sys.time()
(shpReadTime <- difftime(end, start))

warning("Shapefile loaded")

if (loc_id==1) {
  sub_shape <- shape
} else if (loc_id == 349) {
  sub_shape <- shape[shape@data$loc_id == 83,] ### using iceland for greenland for LUR data
} else {
  sub_shape <- shape[shape@data$loc_id == loc_id,] ### choose location
}

sub_shape <- terra::vect(sub_shape) # terra

warning("Shapefile subsetted")


### READ IN AND CROP POP & NO2 DATA ###

start <- Sys.time()

# read in the WorldPop file
if(year=="covid_cf"){
  pop2017 <- raster(paste0(lbd_dir,"/worldpop_total_1y_",2017,"_00_00.tif"))
  pop2017 <- mask(crop(pop2017,sub_shape),sub_shape)
  
  pop2018 <- raster(paste0(lbd_dir,"/worldpop_total_1y_",2018,"_00_00.tif"))
  pop2018 <- mask(crop(pop2018,sub_shape),sub_shape)
  
  pop2019 <- raster(paste0(lbd_dir,"/worldpop_total_1y_",2019,"_00_00.tif"))
  pop2019 <- mask(crop(pop2019,sub_shape),sub_shape)
  
  pop <- stack(c(pop2017,pop2018,pop2019))
  pop <- calc(pop, fun = mean)

  rm(list=c("pop2017","pop2018","pop2019"))

} else {
  
  pop <- terra::rast(paste0(lbd_dir,"/worldpop_total_1y_",year,"_00_00.tif"))
  pop <- terra::mask(terra::crop(pop,sub_shape),sub_shape)
  

}

warning("Pop raster loaded and cropped")



if (year %in% years_larkin) {
  # loading data
  no2 <- terra::rast(paste0(in_dir,"no2_exposure/",year,"_final_1km_rescaled.tif"))
  
  no2 <- terra::mask(terra::crop(no2,sub_shape),sub_shape)

  
} else if (year %in% years_lur) {
  # loading data
  no2 <- terra::rast(paste0(in_dir,"no2_exposure/GlobalNO2LUR_",year,"_resampled_masked.tif"))
  
  no2 <- terra::mask(terra::crop(no2,sub_shape),sub_shape)

}

warning("NO2 raster loaded and cropped")

# ENSURE THERE ARE PIXELS FOR BOTH POP AND NO2 #

# ensure we gots some pixels
testVals_no2 <- as.numeric(values(no2))
testVals_pop <- as.numeric(values(pop))
warning("Test values extracted")


warning("Rasters tested for missingness and buffered")

# ENSURE ALL RASTERS HAVE SAME PROJECTION #
crs(pop)          <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0" 
crs(no2)          <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0" 

warning("Rasters reprojected")


# ENSURE ALL RASTERS HAVE SAME PIXEL SIZE AND ALIGNMENT #
# GBD 2023 required new resampling strategy because pop data is now 5km instead of 1km
# To get population at 1km, will resample and apply a scaling factor to retain total sum population


pop_sum <- as.numeric(global(pop, fun="sum", na.rm=TRUE)) # terra

pop <- terra::resample(x=pop, y=no2, method = "near") # terra

resampled_sum <- as.numeric(global(pop, fun="sum", na.rm=TRUE)) # terra

scaling_factor <- pop_sum/resampled_sum
pop <- pop * scaling_factor
  
warning("Pop raster resampled")

end <- Sys.time()
(dataReadTime <- difftime(end, start))

### CONVERT THE RASTERS TO DATA.TABLES ###
# convert pop to points
# pop <- rasterToPoints(pop) %>% as.data.table
pop <- as.data.table(pop, xy=TRUE) # terra
setnames(pop,c("latitude","longitude","pop"))

# convert no2 to points
# no2 <- rasterToPoints(no2) %>% as.data.table
no2 <- as.data.table(no2, xy=TRUE) # terra
setnames(no2,c("latitude","longitude","air_no2"))

# merge components
exp <- merge(pop,no2,by=c("latitude","longitude"),all.x=F,all.y=F)
warning("Components merged")

### WARN IF THERE ARE MISSING VALUES OF ANY NEEDED VARIABLES ###    
if (sum(is.na(exp$pop))>0) warning("Population values are missing for some pixels")
if (sum(is.na(exp$air_no2))>0) warning("Mean NO2 values are missing for some pixels")

# ### ENSURE THAT POPULATION VALUES ARE NOT ALL NA OR ZERO (E.G. MISASLIGNMENT WITH SMALL ISLAND)


# Drop any pixels with zero or missing populations #
exp <- exp[pop > 0 & is.na(pop)==F, ]

if (debug==T) expBkup <- copy(exp)


### CREATE NO2 DRAWS ###

# load uncertainty
region_id <- locs[location_id==loc_id,region_id]
uncertainty <- fread("FILEPATH/air_no2/exp/uncertainty.csv")
uncertainty <- uncertainty[location_id==region_id,mae]

# to preserve uncertainty estimates, we need to make sure that all gridcells are correlated for their deviation draws
# note: right now we are assuming total correlation (being very conservative because we have no way of knowing how correlation varies in spacetime)

# calculate 1000 draws of the zscore and use that to generate draws of the mean using the region's uncertainty estimate (MAE)
# doing it this way ensures that draws are also correlated across all locations/regions
set.seed(153) # set seed so we have the same distribution for all locations
zscore_draws <- rnorm(n=draws_required,mean=0,sd=1)

exp_draws <- matrix(data = sapply(zscore_draws,function(x){x*uncertainty + exp$air_no2}),
                    nrow = nrow(exp),
                    ncol = draws_required) %>% as.data.table

exp_draws <- setNames(exp_draws,draw_cols)

exp <- cbind(exp[,.(latitude,longitude,pop)],exp_draws)


### COLLAPSE POPULATION BY MEAN ANNUAL NO2 ###
collapse.draws <- function(draw) { 
  longNo2 <- copy(exp)[, paste0("draw_",draw) := round(get(paste0("draw_",draw)),digits=2)]
  
  longNo2 <- longNo2[, lapply(.SD, sum), by = .(get(paste0("draw_",draw))), .SDcols = "pop"] 
  longNo2[, draw := as.integer(draw)]
  
  return(longNo2)
}

long <- do.call(rbind, mclapply(1:draws_required, collapse.draws))

setnames(long,"get","no2")

warning("Reshape to long & collapse completed")

# Save the output #
write.csv(long, file = paste0(out_dir, "/draws/", loc_id,"_",year, ".csv"), row.names = F)

warning("File exported -- done :)")

