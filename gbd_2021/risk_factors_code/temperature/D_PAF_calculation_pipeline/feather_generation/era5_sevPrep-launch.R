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
project <- "NAME" 
sge.output.dir <- paste0(" -o /FILEPATHt/", user, "/FILEPATH/", user, "/errors/%x.e%j ")
#r.shell <- "/FILEPATH/health_fin_forecasting_shell_singularity.sh"
r.shell <- "/FILEPATH/execRscript.sh"
slots <- 8
mem <- "100G"
save.script <- paste0("-s ",  "/FILEPATH/era5_sevPrep.R")
end_year <- 2021


#dir.create(paste0(j, "FILEPATH/"))
dir.create("/FILEPATH/")

#for (year in c(1998, 2004))  { #1990:2020) {
for (year in 1990:end_year) {
  args <- paste(year)
  
  jname <- paste("eraSevPrep", year, sep = "_")
  
  sys.sub <- paste0("sbatch -J ", jname, " -C archive --mem=", mem, " -c ", slots, " -t 08:00:00 -A ", project, " -p all.q", sge.output.dir)
  
  
  # Run
  system(paste(sys.sub, r.shell, save.script, args))
  Sys.sleep(0.01)
}


## Check that all files have been created
if (F) {
  status <- data.table()
  for (year in 1990:end_year) {
    print(year)
    outdir <- paste0("/FILEPATH/")
    
    for (zone in 6:28) {
      status <- rbind(status, data.table(year = year, zone = zone, status = file.exists(paste0(outdir, "/tempCollapsed_", year, "_", zone, ".feather"))))
    }
  } 
  
  if (nrow(status[status == F]) > 0) {
    print("Some files have not been successfully created!")
  }
}
