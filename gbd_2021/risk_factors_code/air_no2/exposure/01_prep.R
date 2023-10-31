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
arg <- tail(commandArgs(),n=3)
grid.version <- arg[1]
loc <- as.numeric(arg[2])
year <- arg[3]

packages <- c("magrittr","fst","tiff","rgdal","raster","data.table")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

row_cut <- 380000 # this number of rows = slightly less than 32 MB; requires about 80 GB to run

#----Functions and directories----------------------------------------------------------------------------------------------------------

home_dir <- "FILEPATH"

tracker_dir <- paste0(home_dir,"FILEPATH",grid.version)

in_dir <- paste0(home_dir,"FILEPATH/")
lbd_dir <- "FILEPATH"
# the country-year draws go in here
draws_dir <- paste0(home_dir,"FILEPATH/",grid.version,"/FILEPATH/")
dir.create(draws_dir,recursive = T)


finished <- list.files(draws_dir)

source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))
locs <- get_location_metadata(35)
locs <- locs[most_detailed==1]


#----Create gridded summary files for ea. country/year---------------------------------------------------------------------------------

# load the gbd 2020 shapefile
shape <- readOGR(dsn="FILEPATH", layer = "GBD2020_analysis_final")

# select all levels (so we can make files for all of the subnats)
shape <- subset(shape,level%in%c(3:6))

# select only the loc_id we need
shape <- subset(shape, loc_id==loc)

# read in the WorldPop file
pop <- raster(paste0(lbd_dir,"/FILEPATH",year,"FILEPATH.tif"))
pop <- mask(crop(pop,shape),shape)
      
# read in and crop the NO2 file
no2 <- raster(paste0(in_dir,"FILEPATH/",year,"_final_1km.tif"))
no2 <- mask(crop(no2,shape),shape)
      
# resamples the pop file so it matches the no2 file
pop <- resample(x=pop, y=no2, method = "bilinear")

# convert pop to points
pop <- rasterToPoints(pop) %>% as.data.table
setnames(pop,c("latitude","longitude","population"))
      
# drop all the zero population rows
pop <- pop[population>0]

# convert no2 to points
no2 <- rasterToPoints(no2) %>% as.data.table
setnames(no2,c("latitude","longitude","air_no2"))
      
# merge and add needed values
out <- merge(pop,no2,by=c("latitude","longitude"),all.x=F,all.y=F)

out[,location_id:=loc]
out[,year_id:=year]
out <- merge(out,locs[,.(location_name,location_id)],by="location_id")
setcolorder(out,c("year_id","location_id","location_name","latitude","longitude","population","air_no2"))
      
# split very large locations into chunks
if(nrow(out)>row_cut){
  nfiles <- nrow(out)/row_cut
  nfiles <- ceiling(nfiles)
  
  out[,file_no:=cut(1:nrow(out),
                     breaks = quantile(1:nrow(out),
                                       probs = seq(0,1, length.out = (nfiles + 1))),
                     labels = 1:nfiles,
                     include.lowest = TRUE)]
  
  for(n in 1:nfiles){
    temp <- out[file_no==n]
    write.csv(temp,paste0(draws_dir,loc,"_",n,"_",year,".csv"),row.names=FALSE)
    print(paste("Done writing file:",loc,year,"--",n,"of",nfiles,sep = " "))
  }
  
  # add it to the tracker
  tracker <- fread(paste0(tracker_dir,"/split_locs.csv"))
  new_row <- data.table("location_id" = loc,
                        "year_id" = year,
                        "nfiles" = nfiles)
  tracker <- rbind(tracker,new_row)
  write.csv(tracker,paste0(tracker_dir,"/split_locs.csv"),row.names = F)
  
} else{
  
  write.csv(out,paste0(draws_dir,loc,"_",year,".csv"),row.names=FALSE)
  
  print(paste("Done writing file:",loc,year,"-- 1 of 1",sep = " "))
  
  
}

