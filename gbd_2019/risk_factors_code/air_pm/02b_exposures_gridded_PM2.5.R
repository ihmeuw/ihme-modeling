#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  NAME
# Date: 3/13/2018
# Project: air_pm
# Purpose: generate location-specific exposure files
# source("FILEPATH.R", echo=T)
#***********************************************************************************************************************

#----SET-UP-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- tail(commandArgs(),n=5) # First args are for unix use only
  if (length(arg)!=5) {
    arg <- c(1990, "36", 1000, 5, "test") #toggle targetted run 
  }
  
} else {
  
  j_root <- "J:"
  h_root <- "H:"
  arg <- c(1990, "28", 50, 2) 
  
}

#set parameters based on arguments from master
this.year <- arg[1]
grid.version <- arg[2]
draws.required <- as.numeric(arg[3])
cores.provided <- as.numeric(arg[4])

#----FUNCTIONS AND DIRECTORIES----------------------------------------------------------------------------------------------------------
# file of helper functions
source(file.path(h_root,"FILEPATH.R"))

home.dir <- "FILEPATH"

# INPUT
exp.dir <- file.path(home.dir,"FILEPATH", grid.version)

out.summary <- file.path(home.dir,"FILEPATH",grid.version)
dir.create(out.summary)

# Loading weights
load(file.path(home.dir,'FILEPATH',grid.version,'FILEPATH.RData'))

pred_dat <- read.fst(paste0(exp.dir,"FILEPATH","FILEPATH",this.year,".fst"))

  # Adding gloabl variable
  pred_dat$Global <- 'Global'

  # Deleting year
  pred_dat$Year <- NULL

  # Opening cluster
  cl <- makePSOCKcluster(cores.provided)
  registerDoParallel(cl)

  # Unweighted country level exposures
  UnweightedGlobal  <- summaryExposures(dat = pred_dat,
                                        prefix = 'pred_',
                                        byvar = 'Global')
  print(paste(Sys.time(),'Done: Global-level exposures'))

  # Population-weighted country level exposures
  WeightedGlobal  <- summaryExposures(dat = pred_dat,
                                      weights = 'POP',
                                      prefix = 'pred_',
                                      byvar = 'Global')
  print(paste(Sys.time(),'Done: Population-weighted Global-level exposures'))

  # Unweighted country level concentrations
  UnweightedGBDSuperRegion  <- summaryExposures(dat = pred_dat,
                                                prefix = 'pred_',
                                                byvar = 'GBDSuperRegion')
  print(paste(Sys.time(),'Done: SuperRegion-level concentrations'))

  # Population-weighted country level concentrations
  WeightedGBDSuperRegion  <- summaryExposures(dat = pred_dat,
                                              weights = 'POP',
                                              prefix = 'pred_',
                                              byvar = 'GBDSuperRegion')
  print(paste(Sys.time(),'Done: Population-weighted SuperRegion-level concentrations'))

  # Unweighted country level concentrations
  UnweightedGBDRegion  <- summaryExposures(dat = pred_dat,
                                           prefix = 'pred_',
                                           byvar = 'GBDRegion')
  print(paste(Sys.time(),'Done: Region-level concentrations'))

  # Population-weighted country level concentrations
  WeightedGBDRegion  <- summaryExposures(dat = pred_dat,
                                         weights = 'POP',
                                         prefix = 'pred_',
                                         byvar = 'GBDRegion')
  print(paste(Sys.time(),'Done: Population-weighted region-level concentrations'))

  #Merging on aggregation weights for GBD reporting areas
  pred_dat <- merge(pred_dat,
               Weights,
               by = 'IDGRID',
               all.x = TRUE)

  # Unweighted country level exposures
  UnweightedCountry  <- summaryExposures(dat = pred_dat,
                                         weights = 'Weight',
                                         prefix = 'pred_',
                                         byvar = 'location_id')
  print(paste(Sys.time(),'Done: Country-level concentrations'))

  print(paste(Sys.time(),"Saving grids for locations and years"))
  
  pred_dat$year_id <- this.year
  
  #save an .fst for every location and year
  for(loc_id in unique(pred_dat$location_id) %>% na.omit){
    
    N_pop <- sum(pred_dat[which(pred_dat$location_id==loc_id),]$POP)
    
    #some small islands have zero population, set to 1 to equally weight all grids
    if(N_pop==0){
      pred_dat[which(pred_dat$location_id==loc_id),]$POP <-1
    }
    
    # Drop all zero pop grids for faster computation
    
      out <- copy(pred_dat[which(pred_dat$location_id==loc_id & pred_dat$POP!=0),])
    # if all populations are zero, replace with 1, otherwise delete grids with zero pop for faster PAF computation. 
    write.fst(out,paste0(exp.dir,"FILEPATH",loc_id,"_",this.year,".fst"))
  }
  
  print(paste(Sys.time(),"Done: Saving grids for locations and years"))
  
  # Getting pop-weights aggregation weights
  pred_dat$Weight <- pred_dat$Weight * pred_dat$POP
  
  # Drop all zero pop grids for faster computation
  pred_dat <- pred_dat[which(pred_dat$Weight>0),]
  
  # Population-weighted country level exposures
  WeightedCountry  <- summaryExposures(dat = pred_dat,
                                       weights = 'Weight',
                                       prefix = 'pred_',
                                       byvar = 'location_id')
  print(paste(Sys.time(),'Done: Population-weighted country-level concentrations'))
  
  # Closing cluster
  stopCluster(cl)
  
  # Adding year as a variable 
  UnweightedCountry$outsumm$Year <- this.year; UnweightedCountry$outsamp$Year <- this.year
  WeightedCountry$outsumm$Year <- this.year; WeightedCountry$outsamp$Year <- this.year
  UnweightedGBDRegion$outsumm$Year <- this.year; UnweightedGBDRegion$outsamp$Year <- this.year
  WeightedGBDRegion$outsumm$Year <- this.year; WeightedGBDRegion$outsamp$Year <- this.year
  UnweightedGBDSuperRegion$outsumm$Year <- this.year; UnweightedGBDSuperRegion$outsamp$Year <- this.year
  WeightedGBDSuperRegion$outsumm$Year <- this.year; WeightedGBDSuperRegion$outsamp$Year <- this.year
  UnweightedGlobal$outsumm$Year <- this.year; UnweightedGlobal$outsamp$Year <- this.year
  WeightedGlobal$outsumm$Year <- this.year; WeightedGlobal$outsamp$Year <- this.year

#----SAVE OBJECTS-------------------------------------------------------------------------------------------------------
save(UnweightedCountry, WeightedCountry, UnweightedGBDRegion, WeightedGBDRegion, 
     UnweightedGBDSuperRegion, WeightedGBDSuperRegion, UnweightedGlobal, WeightedGlobal,
     file = paste0(out.summary,"FILEPATH", this.year,".RData"))