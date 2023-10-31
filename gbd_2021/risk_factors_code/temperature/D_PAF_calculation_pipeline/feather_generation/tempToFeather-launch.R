# source("~/temperature/code/tempToFeather-launch.R", echo = T)

#clear memory
rm(list=ls())

library("data.table")
library("dplyr")
library(ncdf4) 
library(raster)
library(rgdal)
library(feather)

# set up directories
j <- "/FILEPATH/"
h <- "~/" 


#define args:
user <- "USERNAME"



# set up run environment
project <- "PROJECT" 
sge.output.dir <- paste0(" -o /FILEPATH/", user, "/output/%x.o%j -e /share/temp/sgeoutput/", user, "/errors/%x.e%j ")
#r.shell <- "FILEPATH/health_fin_forecasting_shell_singularity.sh"
r.shell <- "FILEPATH/execRscript.sh"
slots <- 4
mem <- "50G"
save.script <- paste0("-s ",  "FILEPATH/tempToFeather.R")

locs <- "all"
#locs <- paste(l, collapse = "_") # use this syntax to collapse a list of location ids

for (year in 1990:2021) {
  args <- paste(year, locs)
    
  jname <- paste("temp2feather", year, sep = "_")
    
  # Create submission call
  sys.sub <- paste0("sbatch -J ", jname, " -C archive --mem=", mem, " -c ", slots, " -t 08:00:00 -A ", project, " -p all.q", sge.output.dir)

    
  # Run
  system(paste(sys.sub, r.shell, save.script, args))
  Sys.sleep(0.01)
}




#################### Read in shapefile ###############################################################
shapefile.dir <- file.path(j, "FILEPATH")
shapefile.version <- "GBD2021_analysis_final"
shape <- readOGR(dsn = shapefile.dir, layer = shapefile.version)

status <- data.table()
for (year in 1990:2021) {
  print(year)
  outdir <- paste0("/FILEPATH/", year)
  
  for (loc_id in unique(shape@data$loc_id)) {
    status <- rbind(status, data.table(year = year, location_id = loc_id, status = file.exists(paste0(outdir, "/era5_", year ,"_", loc_id, ".feather"))))
  }
}  




to.mean.temp <- function(file) {
  temp <- read_feather(file)
  setDT(temp)
  temp <- temp[, lapply(.SD, function(x) {mean(x, na.rm = T)}), by = c("x", "y", "loc_id"), .SDcols = "temp"]
  return(temp)
}

outdir <- "/FILEPATH/"
dir.create(outdir)

for (LOCATION_ID in unique(shape@data$loc_id)) {
  print(LOCATION_ID)
  
  temp <- do.call(rbind, lapply(1990:2021, function(YEAR) {
    (to.mean.temp(paste0("/FILEPATH/", YEAR, "/era5_", YEAR, "_", LOCATION_ID,".feather")))}))
  
  temp <- temp[, lapply(.SD, function(x) {mean(x, na.rm = T)}), by = c("x", "y", "loc_id"), .SDcols = "temp"]
  temp[, meanTempCat := round(temp - 273.15)][, temp := NULL]
  
  write_feather(temp, paste0(outdir, "meanTemps_", LOCATION_ID, ".feather"))
}
