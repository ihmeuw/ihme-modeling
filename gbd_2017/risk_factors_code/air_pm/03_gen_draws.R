#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: air_pm
# Purpose: generate draws based on code from NAME
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- tail(commandArgs(),n=4) # First args are for unix use only
  if (length(arg)!=4) {
    arg <- c(2011, "33", 100, 5) #toggle targetted run 
  }
  
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- c(1990, "28", 50, 2) 
  
}

#set the seed to ensure reproducibility and preserve covariance across parallelized countries
set.seed(42) #the answer of course

#set parameters based on arguments from master
this.year <- arg[1]
grid.version <- arg[2]
draws.required <- as.numeric(arg[3])
cores.provided <- as.numeric(arg[4])

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

#Packages:
lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("INLA","rgdal","data.table","fst","magrittr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

#***********************************************************************************************************************

#----FUNCTIONS AND DIRECTORIES----------------------------------------------------------------------------------------------------------
##function lib##
#Air EXP functions#
exp.function.dir <- file.path(h_root, 'FILEPATH')
file.path(exp.function.dir, "assign_tools.R") %>% source


draw.tools.dir <- file.path(home.dir,"FILEPATH")
file.path(draw.tools.dir, "0_Source.R") %>% source

# OUTPUT
exp.dir <- file.path('FILEPATH')
# file that will be created if running the assign codeblock
assign.output <- file.path(exp.dir, "grid_map.fst")


#***********************************************************************************************************************

#-------LOAD FILES------------------------------------------------------------------------------------------------------
# Loading grid data
load(file.path(draw.tools.dir,'grid_dat.RData'))
grid_dat <- as.data.table(grid_dat)

# Loading INLA object 
load(file.path(draw.tools.dir,'INLAObjects_nonAfrica.RData'))
load(file.path(draw.tools.dir,'INLAObjects_Africa.RData'))
load(file.path(draw.tools.dir,'INLAObjects_Islands.RData'))

# Loading samples
load(file.path(draw.tools.dir,'Samples_nonAfrica_1000.RData'))
load(file.path(draw.tools.dir,'Samples_Africa_1000.RData'))
load(file.path(draw.tools.dir,'Samples_Islands_1000.RData'))

# Loading A matrix
load(file.path(draw.tools.dir, 'AMatrix_nonAfrica.RData'))
load(file.path(draw.tools.dir, 'AMatrix_Africa.RData'))
load(file.path(draw.tools.dir,'AMatrix_Islands.RData'))


# Loding UN Pop new version for GBD17 from NAME
gridded.pop.dir <- file.path(j_root, "FILEPATH")
gridded.pop <- file.path(gridded.pop.dir, "popGPWv4r10_UN_allyears.csv") # GBD2017 new pop
pop<-fread(gridded.pop)
#We want them to be uniformly named pop_yyyy
#using v4r10
setnames(pop,paste0("pop",c(2000,2005,2010:2017),"v4r10"),paste0("pop_",c(2000,2005,2010:2017)))
#make sure all the pop columns are numeric, not integer
pop[,c(paste0("pop_",this.year)):=as.numeric(get(paste0("pop_",this.year)))]



#************************************************************************************************************************

# Removing missings
grid_dat <- subset(grid_dat, !is.na(get(paste0("SAT_",(this.year)))))

# Country Variables 
grid_dat$Country1 <- as.character(grid_dat$CountryCode)
grid_dat$Country2 <-  as.character(grid_dat$CountryCode)
grid_dat$Country3 <-  as.character(grid_dat$CountryCode)

# Regional Variables 
grid_dat$Region1 <-  as.character(grid_dat$GBDRegion)
grid_dat$Region2 <-  as.character(grid_dat$GBDRegion)
grid_dat$Region3 <-  as.character(grid_dat$GBDRegion)

# Superregion Variables 
grid_dat$SRegion1 <-  as.character(grid_dat$GBDSuperRegion)
grid_dat$SRegion2 <-  as.character(grid_dat$GBDSuperRegion)
grid_dat$SRegion3 <-  as.character(grid_dat$GBDSuperRegion)

# ID GRID CALL variables 
grid_dat$IDGRID <- as.character(grid_dat$IDGRID)

# Other Variables
grid_dat$UnspecifiedType <- 0
grid_dat$PM25Conv <- 0
grid_dat$ApproxLoc <- 0


###############################
### Summarising predictions ###
###############################
# Extracting time varying columns 
grid_dat$SAT <- grid_dat[,c(get(paste0("SAT_",this.year)))]
grid_dat$DUST <- grid_dat[,c(get(paste0("DUST_",this.year)))]
grid_dat$SANOC <- grid_dat[,c(get(paste0("SANOC_",this.year)))]
grid_dat$ELEVDIFFALTD <- grid_dat[,c(get(paste0("ELEVDIFFALTD_",this.year)))]
grid_dat$POP <- grid_dat[,c(get(paste0("POP_",this.year)))]

# Time 
t <- as.numeric(this.year) - 2009

# If outside 2010-2016 take the nearest calibration 
if (t < 1) {t <- 1} #sets 1990:2010 to 1
if (t > 7) {t <- 7} #2016 and 2017 to 7

# Time variables
grid_dat$Time <- t
grid_dat$time1 <- t
grid_dat$time2 <- t

# Converting to character
grid_dat$Time <-  as.character(grid_dat$Time)
grid_dat$time1 <-  as.character(grid_dat$time1)
grid_dat$time2 <-  as.character(grid_dat$time2)

# Setting Missings to zero
grid_dat$SAT[is.na(grid_dat$SAT)] <- 0
grid_dat$DUST[is.na(grid_dat$DUST)] <- 0
grid_dat$SANOC[is.na(grid_dat$SANOC)] <- 0
grid_dat$POP[is.na(grid_dat$POP)] <- 0


# Removing zeroes for taking logs
grid_dat$SAT[grid_dat$SAT <= 0] <- 1E-6
grid_dat$DUST[grid_dat$DUST <= 0] <- 1E-6
grid_dat$SANOC[grid_dat$SANOC <= 0] <- 1E-6
grid_dat$POP[grid_dat$POP <= 1] <- 1 

# Taking logs of CTM
grid_dat$logSAT <- log(grid_dat$SAT)
grid_dat$logDUST <- log(grid_dat$DUST)
grid_dat$logSANOC <- log(grid_dat$SANOC)
grid_dat$logPOP <- log(grid_dat$POP)


# Splitting grid into two parts for predictions
grid_dat_Africa <- subset(grid_dat, GBDSuperRegion %in% c('Sub-Saharan Africa','North Africa / Middle East') | CountryName == 'Western Sahara')
grid_dat_Islands <- subset(grid_dat, (GBDRegion %in% c('Oceania','Caribbean','Unknown') | CountryName == 'Timor-Leste') & !(CountryName == 'Western Sahara'))
grid_dat_nonAfrica <- subset(grid_dat, !(CountryName %in% unique(c(grid_dat_Africa$CountryName, grid_dat_Islands$CountryName))))

# Removing unecessary datasets
rm(grid_dat)

# Creating joint samples 
pred_dat_nonAfrica <- joint.samp.inla.downscaling(dat = as.data.frame(grid_dat_nonAfrica),
                                         samp = samp_nonAfrica,
                                         N=draws.required,
                                         A = A_nonAfrica, 
                                         spat.slope = logSAT,
                                         INLAOut = out_nonAfrica,
                                         prefix = 'draw_',
                                         keep = c('Longitude','Latitude',"POP"))

pred_dat_Africa <- joint.samp.inla.downscaling(dat = as.data.frame(grid_dat_Africa),
                                                   samp = samp_Africa,
                                                   N=draws.required,
                                                   A = A_Africa, 
                                                   spat.slope = logSAT,
                                                   INLAOut = out_Africa,
                                                   prefix = 'draw_',
                                                   keep = c('Longitude','Latitude',"POP"))

pred_dat_Islands <- joint.samp.inla.downscaling(dat = as.data.frame(grid_dat_Islands),
                                               samp = samp_Islands,
                                               N=draws.required,
                                               A = A_Islands, 
                                               spat.slope = logSAT,
                                               INLAOut = out_Islands,
                                               prefix = 'draw_',
                                               keep = c('Longitude','Latitude',"POP"))

pred <- rbindlist(list(pred_dat_Africa, pred_dat_nonAfrica,pred_dat_Islands))


#merge on UW pop file from Mike
pred <-merge(pred,pop[,.(longitude,latitude,get(paste0("pop_",this.year)))],
                by.x=c("Longitude","Latitude"),
                by.y=c("longitude","latitude"),
                all.x=T)
setnames(pred,"V3","pop")
pred[is.na(pop),pop:=1e-6]
pred[pop < 0 ,pop:=1e-6]


# Exponentiating predictions 
pred[,paste0("draw_",1:draws.required):=exp(.SD),.SDcols=paste0("draw_",1:draws.required)]


# formatting
pred <- as.data.table(pred)

if(grid.version==32){
  setnames(pred,"POP","pop_2")
}else{pred$POP <- NULL}

names(pred)<-tolower(names(pred))

#merge on location IDs from GBD borders shapefile, output created in assign step
grid_map <- read.fst(assign.output, as.data.table=TRUE)

#merge location_id, name, and weight on to pollution dt
pred <- merge(pred, grid_map[,.(location_id,location_name,weight,latitude,longitude)] , by=c("latitude", "longitude"))



######################
### Saving Objects ###
######################
# Saving marginal predictions 
write.fst(pred, path = paste0(exp.dir,"FILEPATH/all_grids_",this.year,".fst"))

