# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"

}
central_lib <- "FILEPATH"

# set parameters based on arguments from parent script
# arg <- tail(commandArgs(),n=2)
# grid.version <- arg[1]
# loc <- as.numeric(arg[2])
grid.version <- 7
loc <- 367

packages <- c("magrittr","fst","tiff","rgdal","raster","data.table")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}


#----Functions and directories----------------------------------------------------------------------------------------------------------

home_dir <- "FILEPATH"
source(paste0(h_root,"FILEPATH/functions.R")) # this sources the helper function

in_dir <- paste0(home_dir,"FILEPATH/")
lbd_dir <- "FILEPATH"
# the country-year draws go in here
draws_dir <- paste0(home_dir,"FILEPATH/",grid.version,"/FILEPATH/")
dir.create(draws_dir,recursive = T)

finished <- list.files(draws_dir)

source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))
locs <- get_location_metadata(35)
locs <- locs[most_detailed==1]

source(paste0(h_root,"FILEPATH/functions.R"))

#----Create gridded summary files for ea. country/year---------------------------------------------------------------------------------
years <- c(1990, 1995, 2000, 2005:2020)
years <- 2020

# load the gbd 2020 shapefile
shape <- readOGR(dsn="FILEPATH", layer = "GBD2020_analysis_final")

# select all levels (so we can get files for all of the subnats)
shape <- subset(shape,level%in%c(3:6))

  
  for (year in years){
  
      # read in the NO2 file
      no2 <- raster(paste0(in_dir,"FILEPATH/",year,"_final_1km.tif"))
      
      grid <- rasterToPoints(no2)
      colnames(grid) <- c("Latitude","Longitude","no2")
      grid <- as.data.table(grid)
      
      # find grids for the NO2 file 
      # (this function takes the "grid" object and then chops it into a gridded data.table slightly larger than the country of interest to identify pixels)
      found_grids <- estimate_small_locs(country = loc,
                                 borders = shape,
                                 location_id.list = locs)
      
      setnames(found_grids,c("Latitude","Longitude"),c("latitude","longitude"))


      # add on population and needed values
      out <- found_grids
      setnames(out,"Weight","population")
      out[,location_id:=loc]
      out[,year_id:=year]
      setnames(out,"no2","air_no2")
      out <- out[,c("year_id","location_id","location_name","latitude","longitude","population","air_no2")]
      
      write.csv(out,paste0(draws_dir,loc,"_",year,".csv"),row.names=FALSE)
      
      print(paste0("Done with ", loc,", ", year))
      
    }
    



