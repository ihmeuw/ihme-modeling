#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  NAME
# Editor: NAME (03/24/2020)
# Date: 3/13/2018
# Project: air_pm
# Purpose: generate draws of ambient exposure based on code from NAME
# source("FILEPATH.R", echo=T)
# qsub -N air_pm_preds -P ADDRESS -l fthread=5 -l h_rt=24:00:00 -l m_mem_free=200G -q all.q -o FILEPATH -e FILEPATH FILEPATH.sh FILEPATH.R 

#----SET-UP-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

arg <- commandArgs(trailingOnly=T)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  arg <- tail(commandArgs(),n=5) # First args are for unix use only
  if (length(arg)!=5) {
    arg <- c(2019, "37", 10, 5, "test") #toggle targetted run 
  }
  
  
} else {
  
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  arg <- c(1990, "36", 50, 2,"test") 
  
}

# set the seed to ensure reproducibility and preserve covariance across parallelized countries
set.seed(42) 

# set parameters based on arguments from parent script
this.year <- arg[1]
grid.version <- arg[2]
draws.required <- as.numeric(arg[3])
cores.provided <- as.numeric(arg[4])

library(fst)
library(doParallel)
library(data.table)
library(raster)
library(ggplot2)

#install.packages("INLA", repos="https://inla.r-inla-download.org/R/testing", dependencies=TRUE)

# Update to the most recent INLA version (2/26/2020)
# install.packages("INLA", repos=c(getOption("repos"), INLA="https://inla.r-inla-download.org/R/testing"), dep=TRUE, lib="ADDRESS")
library(INLA, lib="ADDRESS")
inla.setOption(pardiso.license="FILEPATH.lic") # Pardiso license valid until 2/26/2021
INLA:::inla.dynload.workaround()

#----FUNCTIONS AND DIRECTORIES----------------------------------------------------------------------------------------------------------

# file of helper functions
# source(file.path(h_root,"FILEPATH.R"))
source(file.path(h_root,"FILEPATH.R")) # updated 03/24/2020

home.dir <- "FILEPATH"

# INPUT
in.dir <- file.path(home.dir,"FILEPATH")  

# OUTPUT
exp.dir <- file.path(home.dir,"FILEPATH", grid.version)
dir.create(file.path(exp.dir,"FILEPATH"),recursive=T)
dir.create(file.path(exp.dir,"FILEPATH"),recursive=T)

#-------LOAD FILES------------------------------------------------------------------------------------------------------
# Loading grid data
# load(file.path(in.dir, "FILEPATH", grid.version,'FILEPATH.RData'))
load(file.path(home.dir, "FILEPATH", "FILEPATH.RData"))

# Loading INLA object
load(file.path(in.dir, "FILEPATH", grid.version, "FILEPATH.RData"))
load(file.path(in.dir, "FILEPATH", grid.version, "FILEPATH.RData"))

#####################################################
### Prepping variables that dont change over time ###
#####################################################
# Regional Variables
grid_dat$Region1 <-  as.character(grid_dat$GBDRegion)
grid_dat$Region2 <-  as.character(grid_dat$GBDRegion)
grid_dat$Region3 <-  as.character(grid_dat$GBDRegion)

# ID GRID CALL variables
grid_dat$IDGRID <- as.character(grid_dat$IDGRID)

# Other Variables
grid_dat$UnspecifiedType <- 0
grid_dat$PM25Conv <- 0
grid_dat$ApproxLoc <- 0

# Empty columns for covariates
grid_dat$SAT <- NA
grid_dat$SANOC <- NA
grid_dat$DUST <- NA
grid_dat$POP <- NA
grid_dat$ELEVDIFFALTD <- NA

# Splitting grid into two parts for predictions
grid_dat_Africa <- subset(grid_dat, GBDSuperRegion %in% c('Sub-Saharan Africa','North Africa / Middle East') | CountryName == 'Western Sahara')
grid_dat_nonAfrica <- subset(grid_dat, !(GBDSuperRegion %in% c('Sub-Saharan Africa','North Africa / Middle East') | CountryName == 'Western Sahara'))

# Removing unecessary datasets
rm(grid_dat)

# #########################################
# ### Joint samples of parameter fields ###
# #########################################
# # # Samples from non-African Countries
samp_nonAfrica <- inla.posterior.sample(n = draws.required, out_nonAfrica$INLAObj)
samp_Africa <- inla.posterior.sample(n = draws.required, out_Africa$INLAObj, add.names=TRUE)

save(samp_nonAfrica, file=file.path(in.dir, "FILEPATH", grid.version, "FILEPATH.RData"))
save(samp_Africa, file=file.path(in.dir, "FILEPATH", grid.version, "FILEPATH.RData"))

##################
### A Matrices ###
##################
# Creating a dataset
Coords_nonAfrica <- grid_dat_nonAfrica[,c('Longitude','Latitude')]
Coords_Africa <- grid_dat_Africa[,c('Longitude','Latitude')]

# Converting to SpatialPointsDataFrame
coordinates(Coords_nonAfrica) <- ~ Longitude + Latitude
coordinates(Coords_Africa) <- ~ Longitude + Latitude

# Settign coordinate reference system
proj4string(Coords_nonAfrica) <- CRS("+proj=longlat +ellps=WGS84")
proj4string(Coords_Africa) <- CRS("+proj=longlat +ellps=WGS84")

# A matrix
A_nonAfrica <- inla.spde.make.A(out_nonAfrica$INLAMesh, loc = Coords_nonAfrica)
A_Africa <- inla.spde.make.A(out_Africa$INLAMesh,loc = Coords_Africa)

# Removing uncessary dataset
rm(Coords_nonAfrica, Coords_Africa)

###############################
### Summarising predictions ###
###############################
i <- as.numeric(this.year)

old.dir <- "FILEPATH"
  
  # Printing year
  print(paste('Predictions for: ', i, sep = ''))

  if(this.year %in% c(1990,1995)){
    # Loading covariate data by year
    r_tmp1 <- raster(paste(old.dir,'FILEPATH',i,'.nc', sep = ''))
    r_tmp2 <- raster(paste(old.dir,'FILEPATH',i,'.nc', sep = ''))
    r_tmp3 <- raster(paste(old.dir,'FILEPATH',i,'.nc', sep = ''))
    r_tmp4 <- raster(paste(old.dir,'FILEPATH',i,'.tif', sep = ''))
    r_tmp5 <- raster(paste(old.dir,'FILEPATH',i,'-0.1.nc', sep = ''))
  }else{
    # Loading covariate data by year
    r_tmp1 <- raster(paste(old.dir,'FILEPATH',i,'-0.1.nc', sep = ''))
    r_tmp2 <- raster(paste(old.dir,'FILEPATH',i,'-0.1.nc', sep = ''))
    r_tmp3 <- raster(paste(old.dir,'FILEPATH',i,'-0.1.nc', sep = ''))
    r_tmp4 <- raster(paste(old.dir,'FILEPATH',i,'.tif', sep = ''))
    r_tmp5 <- raster(paste(old.dir,'FILEPATH',i,'-0.1.nc', sep = ''))
  }

  # Extracting
  grid_dat_nonAfrica$SAT <- extract(r_tmp1, grid_dat_nonAfrica[,c('Longitude','Latitude')])
  grid_dat_nonAfrica$SANOC <- extract(r_tmp2, grid_dat_nonAfrica[,c('Longitude','Latitude')])
  grid_dat_nonAfrica$DUST <- extract(r_tmp3, grid_dat_nonAfrica[,c('Longitude','Latitude')])
  grid_dat_nonAfrica$POP <- extract(r_tmp4, grid_dat_nonAfrica[,c('Longitude','Latitude')])
  grid_dat_nonAfrica$ELEVDIFFALTD <- extract(r_tmp5, grid_dat_nonAfrica[,c('Longitude','Latitude')])

  # Extracting
  grid_dat_Africa$SAT <- extract(r_tmp1, grid_dat_Africa[,c('Longitude','Latitude')])
  grid_dat_Africa$SANOC <- extract(r_tmp2, grid_dat_Africa[,c('Longitude','Latitude')])
  grid_dat_Africa$DUST <- extract(r_tmp3, grid_dat_Africa[,c('Longitude','Latitude')])
  grid_dat_Africa$POP <- extract(r_tmp4, grid_dat_Africa[,c('Longitude','Latitude')])
  grid_dat_Africa$ELEVDIFFALTD <- extract(r_tmp5, grid_dat_Africa[,c('Longitude','Latitude')])

  # Removing unecessary datasets
  rm(r_tmp1, r_tmp2, r_tmp3, r_tmp4, r_tmp5)

  # Setting Missings and negatives to zero
  grid_dat_nonAfrica$SAT[is.na(grid_dat_nonAfrica$SAT)] <- 0;   grid_dat_nonAfrica$SAT[grid_dat_nonAfrica$SAT <= 0] <- 0
  grid_dat_nonAfrica$DUST[is.na(grid_dat_nonAfrica$DUST)] <- 0;  grid_dat_nonAfrica$DUST[grid_dat_nonAfrica$DUST <= 0] <- 0
  grid_dat_nonAfrica$SANOC[is.na(grid_dat_nonAfrica$SANOC)] <- 0;  grid_dat_nonAfrica$SANOC[grid_dat_nonAfrica$SANOC <= 0] <- 0
  grid_dat_nonAfrica$POP[is.na(grid_dat_nonAfrica$POP)] <- 0;  grid_dat_nonAfrica$POP[grid_dat_nonAfrica$POP <= 0] <- 0
  grid_dat_Africa$SAT[is.na(grid_dat_Africa$SAT)] <- 0;  grid_dat_Africa$SAT[grid_dat_Africa$SAT <= 0] <- 0
  grid_dat_Africa$DUST[is.na(grid_dat_Africa$DUST)] <- 0;  grid_dat_Africa$DUST[grid_dat_Africa$DUST <= 0] <- 0
  grid_dat_Africa$SANOC[is.na(grid_dat_Africa$SANOC)] <- 0;  grid_dat_Africa$SANOC[grid_dat_Africa$SANOC <= 0] <- 0
  grid_dat_Africa$POP[is.na(grid_dat_Africa$POP)] <- 0;  grid_dat_Africa$POP[grid_dat_Africa$POP <= 0] <- 0

  # Taking logs of CTM
  grid_dat_nonAfrica$logDUST <- log(grid_dat_nonAfrica$DUST + 1)
  grid_dat_nonAfrica$logSAT <- log(grid_dat_nonAfrica$SAT + 1)
  grid_dat_nonAfrica$logSANOC <- log(grid_dat_nonAfrica$SANOC + 1)
  grid_dat_nonAfrica$logPOP <- log(grid_dat_nonAfrica$POP + 1)
  grid_dat_Africa$logDUST <- log(grid_dat_Africa$DUST + 1)
  grid_dat_Africa$logSAT <- log(grid_dat_Africa$SAT + 1)
  grid_dat_Africa$logSANOC <- log(grid_dat_Africa$SANOC + 1)
  grid_dat_Africa$logPOP <- log(grid_dat_Africa$POP + 1)

  # Time
  t <- i - 1990 + 1
  if (t > 28) {t <- 28}

  # Time variables
  grid_dat_nonAfrica$time1 <- t; grid_dat_Africa$time1 <- t;
  grid_dat_nonAfrica$time2 <- t; grid_dat_Africa$time2 <- t;

  # Converting to character
  grid_dat_nonAfrica$time1 <-  as.character(grid_dat_nonAfrica$time1)
  grid_dat_nonAfrica$time2 <-  as.character(grid_dat_nonAfrica$time2)
  grid_dat_Africa$time1 <-  as.character(grid_dat_Africa$time1)
  grid_dat_Africa$time2 <-  as.character(grid_dat_Africa$time2)

  # Creating joint samples
  pred_dat_nonAfrica <- joint.samp.inla.downscaling2(dat = grid_dat_nonAfrica,
                                                     timevar = c('Region1'),
                                                     N.Years = 28,
                                                     t = t,
                                                     samp = samp_nonAfrica,
                                                     INLAOut = out_nonAfrica,
                                                     A = A_nonAfrica,
                                                     prefix = 'pred_',
                                                     spat.slope = logSAT,
                                                     keep = c('Longitude','Latitude','ISO3','CountryName','GBDRegion','GBDSuperRegion','WHORegion','SDGRegion','WHOIncomeRegion','POP','Urban','IDGRID'))

  # Creating joint samples
  pred_dat_Africa <- joint.samp.inla.downscaling(dat = grid_dat_Africa,
                                                 samp = samp_Africa,
                                                 A = A_Africa,
                                                 spat.slope = logSAT,
                                                 INLAOut = out_Africa,
                                                 prefix = 'pred_',
                                                 keep = c('Longitude','Latitude','ISO3','CountryName','GBDRegion','GBDSuperRegion','WHORegion','SDGRegion','WHOIncomeRegion','POP','Urban','IDGRID'))

  # Appending datasets together
  pred_dat <- rbind(pred_dat_Africa, pred_dat_nonAfrica)

  # Removing unecessayr datasets
  rm(pred_dat_Africa, pred_dat_nonAfrica)

  # Opening cluster
  # cl <- makePSOCKcluster(cores.provided)
  # registerDoParallel(cl)
  
  # Create cluster with desired number of cores, leave one open for the machine core processes
  # cores<-detectCores()
  # cl <- makeCluster(cores[1]-1)
  # registerDoParallel(cl)

  # # Marginal predictions on the log scale by grid cell
  # pred_dat_log  <- summaryPred(dat = pred_dat,
  #                              prefix = 'pred_',
  #                              keep = c('Longitude','Latitude','ISO3','CountryName','GBDRegion','GBDSuperRegion','SDGRegion','WHORegion','WHOIncomeRegion','POP','Urban'))
  #
  # # Adding year as a variable
  # pred_dat_log$Year <- i

  # Exponentiating predictions
  pred_dat[,grep('pred_',names(pred_dat))] <- exp(pred_dat[,grep('pred_',names(pred_dat))])

  write.fst(pred_dat, path = paste0(exp.dir,"FILEPATH",this.year,".fst"))
  
  # Marginal predictions by grid cell
  pred_dat  <- summaryPred(dat = pred_dat,
                           prefix = 'pred_',
                           keep = c('Longitude','Latitude','CountryName','ISO3','GBDRegion','GBDSuperRegion','SDGRegion','WHORegion','WHOIncomeRegion','POP','Urban','IDGRID'),
                           nCluster=cores.provided)
  
  # Adding year as a variable
  pred_dat$Year <- i
  
  # Closing cluster
  # stopCluster(cl)

######################
### Saving Objects ###
######################

write.fst(pred_dat, path = paste0(exp.dir,"FILEPATH",this.year,".fst"))
  
# write raster objects for sharing with LBD and other external audiences

  pred_dat <- as.data.table(pred_dat)

  mean_raster <- rasterFromXYZ(pred_dat[,.(Longitude,Latitude,Mean)])  #Convert first two columns as lon-lat and third as value
  writeRaster(mean_raster, paste0(exp.dir,"FILEPATH",this.year,".tif"),format="GTiff",overwrite=T)

  median_raster <- rasterFromXYZ(pred_dat[,.(Longitude,Latitude,Median)])  #Convert first two columns as lon-lat and third as value
  writeRaster(median_raster, paste0(exp.dir,"FILEPATH",this.year,".tif"),format="GTiff",overwrite=T)

  stddev_raster <- rasterFromXYZ(pred_dat[,.(Longitude,Latitude,StdDev)])  #Convert first two columns as lon-lat and third as value
  writeRaster(stddev_raster, paste0(exp.dir,"FILEPATH",this.year,".tif"),format="GTiff",overwrite=T)

# plot

  pdf(paste0(exp.dir,"FILEPATH",this.year,".pdf"))
  ggplot(data=pred_dat,aes(x=Longitude,y=Latitude,fill=log(Median)))+geom_raster()+scale_fill_gradientn(colours = terrain.colors(10))
  dev.off()