#----HEADER----------------------------------------------------------------------------------------------------------------------
# Project: air_pm
# Purpose: Take the gridded exposure model from collaborators, apply to GBD shapefile, UN gridded pops, generate draws,
#   and save by location-year for PAF calculation
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
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

if (Sys.getenv('SGE_CLUSTER_NAME') == "prod" ) {
  

  
} else {
  

  
}

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# set project values
location_set_id <- 22
years <- c(1990,1995,2000,2005,2010:2017)

#Packages:
lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","fst", "magrittr", "RMySQL", "raster","ggplot2", "magrittr", "maptools", "rgdal", "rgeos", "sp", "stringr", "RMySQL","snow","ncdf4")


for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}


# set options
run.assign <- F 
run.draws <- F
run.save <- T
retry <- F 
max.cores <- 75 
draws.required <- 1000
#********************************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#Air EXP functions#
exp.function.dir <- file.path(h_root, "FILEPATH")
file.path(exp.function.dir, "assign_tools.R") %>% source

#general functions#
central.function.dir <- file.path(h_root, "FILEPATH/")
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source

#Central function for locations#
source(file.path(j_root,"FILEPATH/get_location_metadata.R"))


#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator
#***********************************************************************************************************************

#----VERSIONING------------------------------------------------------------------------------------------------------------------
#version history#
#grid.version <- "3" # final GBD2013 grid.version, fixed extrapolation formula error
#grid.version <- "4" # preliminary GBD2015 grid.version (still using GBD2013 input grids, has new locations/shapefiles/extrapolation function
#grid.version <- "5" #new GBD2015 verison that uses natural splines instead of smooth splines
#grid.version <- "6" #test of grid.version should match#5
#grid.version <- "7" #back to using AROC for extrapolation
#grid.version <- "8" #first with GBD2015 data (no ozone)
#grid.version <- "9" #fixed issue with uncertainty
#grid.version <- 10 #new data sent by gavin to fix issue with island uncertainty
grid.version <- 11 #run with new shapefile, includes india urb/rural
grid.version <- 12 #run with fix to generating draws from median/ci (done in log then need to exponet)
grid.version <- 13 #running v12 again, some countries failed to save..
grid.version <- 14 #running v12 again, some countries failed to save..
grid.version <- 15 #running without the fix to test (in normal space, no exponent)
grid.version <- 16 #should match v14, testing a new parallelized grid.version (log_space)
grid.version <- 17 #should match v15, testing a new parallelized grid.version (normal_space)
grid.version <- 18 #first run for GBD2016
grid.version <- 19 #fixed ihme loc ids
grid.version <- 20 #more problems with loc ids, for some reason CHN missing
grid.version <- 21 #after receiving new data from NAME to fix 2016 splines
grid.version <- 22 #rural only for HAP calculation
grid.version <- 23 #rerunning v21 using the fst format
grid.version <- 25 #GBD2017
grid.version <- 26 #GBD 2017 fixed error which should allow for reproducibility
grid.version <- 27 #updated shapefile, Norway subnational changes
grid.version <- 28  #Testing with new GBD2017 exp model
grid.version <- 29 #only most detailed, fixing weights, GBD 17 model separates Africa, nonAfrica and Islands #Misalignment error
grid.version <- 30 #trying again with new model ^^
grid.version <- 31 #changed min pop from 1 to 1e-6. Added all available years.
grid.version <- 32 #test using NAME's pop rather than UN-adjusted to test trends. 100 draws
grid.version <- 33 #weighted sampling approach for proper country level uncertainty
#********************************************************************************************************************************

#----IN/OUT----------------------------------------------------------------------------------------------------------------------
# Set directories and load files
###Input###
code.dir <- file.path(h_root, 'FILEPATH')
assign.script <- file.path(code.dir, "02_assign.R")
draw.script <- file.path(code.dir, "03_gen_draws.R")
save.script <- file.path(code.dir,"04_save.R")
r.shell <- file.path(h_root, "FILEPATH")
r.shell <- "FILEPATH"

# Get the list of GBD locations
locs <- get_location_metadata(location_set_id)

###Output###
# where to output the split gridded files
exp.dir <- file.path('FILEPATH', grid.version)
dir.create(file.path(exp.dir, "summary"), recursive=T, showWarnings=F)
dir.create(file.path(exp.dir, "draws"), recursive=T, showWarnings=F)
exp.dir.sum <- file.path("FILEPATH")
dir.create(exp.dir.sum, recursive=T, showWarnings=F)
# file that will be created if running the assign codeblock
assign.output <- file.path(exp.dir, "grid_map.fst")

#********************************************************************************************************************************

#----RUN ASSIGN------------------------------------------------------------------------------------------------------------------
#if necessary, run the assign code to prep the grid map to map exp dataset to ihme shapefile
if (run.assign==TRUE) {
  
  source(assign.script, echo=T)
  
} else grid_map <- read.fst(assign.output, as.data.table=TRUE)

#********************************************************************************************************************************

#----Generate Draws--------------------------------------------------------------------------------------------------------------

#what's already been saved?
files <- list.files(file.path(exp.dir, "draws"))



if(run.draws==T){
  for(year in years){
    
    cores.provided <- 20
    mem <- cores.provided * 4
    slots <- cores.provided * 2
    message("launching draw generation for  year ",year, "\n --using ", slots, " slots and ", mem, "GB of mem")
    
    # Launch jobs
    out.args <- paste(year,
                      grid.version,
                      draws.required,
                      cores.provided)
    
    # Prepare job settings
    jname <- paste0("air_exp_draws_",year)
    
    # Create submission call
    sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")
    
    # Run
    paste(sys.sub, r.shell, draw.script, out.args) %>% system
  }
}


#********************************************************************************************************************************

#----LAUNCH SAVE-----------------------------------------------------------------------------------------------------------------
#create vector of all the different countries in the pollution file
countries <- unique(grid_map$location_id) %>% sort


#see if files have already been saved
files <- list.files(path=exp.dir.sum, pattern=".csv",full.names=T)

if(run.save==T){
  for (country in countries) {
    for(year in years){
    
  
      if(length(grep(paste0(exp.dir.sum,"/",country,"_",year,".csv"),files))==0){ #checks the directory that saves pop-weighted draws
  
      #set the number of cores to provide based on the number of grids in the country (object size)
  
      if (retry == FALSE) {
  
        grid.size <- grid_map[location_id==country] %>% object.size
  
        if (grid.size > 5e7) {
          cores.provided <- 20 #give 40 slots and 80gb of mem to any files larger than 50mb
        } else if(grid.size > 15e6) {
          cores.provided <- 12 #give 24 slots and 48gb of mem to any files larger than 15mb
        } else if(grid.size > 25e4) {
          cores.provided <- 8 #give 16 slots and 32gb of mem to any files larger than .25mb
        } else if(country == "GLOBAL") {
          cores.provided <- 20
        } else cores.provided <- 6 #give 12 slots and 24gb of mem to any files less than .25 mb
  
        mem <- cores.provided * 4
        slots <- cores.provided * 2
  
      } else if (retry==TRUE) {
  
        cores.provided <- 1
        mem <- 100
        slots <- cores.provided 
  
      }
  
      message("launching exp calc for loc ", country, " year ",year, "\n --using ", slots, " slots and ", mem, "GB of mem")
  
      # Launch jobs
      out.args <- paste(country,
                        year,
                        grid.version,
                        draws.required,
                        cores.provided)
  
      # Prepare job settings
      jname <- paste0("air_exp_", country, "_",year)
  
      # Create submission call
      sys.sub <- paste0("qsub -hold_jid air_exp_draws_", year ," ",
                        project, sge.output.dir, " -N ", jname, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")
      
  
      # Run
      paste(sys.sub, r.shell, save.script, out.args) %>% system
      }

    }

  }
}

#********************************************************************************************************************************

