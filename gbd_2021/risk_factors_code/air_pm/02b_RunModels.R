# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

#runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- tail(commandArgs(),n=3) # First args are for unix use only
  if (length(arg)!=3) {
    arg <- c("41",1000) #toggle targetted run 
  }
  
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- c("41",1000) 
  
}

#set parameters based on arguments from master
grid.version <- arg[1]
draws.required <- arg[2]

# load Matt Thomas's file of helper functions
source(file.path(h_root,"FILEPATH/00_Source.R"))

home.dir <- "FILEPATH"
in.dir <- file.path(home.dir,"input","FILEPATH",grid.version) 

out.dir <- file.path(home.dir,"input","FILEPATH",grid.version)
dir.create(out.dir)

# load GM data
load(file.path(in.dir,'GM_dat.RData'))

# load gridded data
load(file.path(in.dir,'grid_dat.RData'))

# load shapefiles 
load(file.path(in.dir,'shapefiles.RData'))

####################################
### Prepping Ground Monitor data ###
####################################
# Only keeping those with Coverage over 0.75 and those with missing coverage
GM_dat$PM25Grading[is.na(GM_dat$PM25Grading)] <- GM_dat$PM10Grading[is.na(GM_dat$PM25Grading)] 
GM_dat <- subset(GM_dat,  !((PM25Grading == 2 | PM25Grading == 3) & Year < 2010))

# If before 2010, set to 2010, if after 2020 set to 2020 (this is the range of years we have for GM data)
GM_dat$Year[which(GM_dat$Year < 2000)] <- 2000
GM_dat$Year[which(GM_dat$Year > 2020)] <- 2020

# Getting unique region data
tmp <- data.frame(GBDRegion = rep(unique(grid_dat$GBDRegion), each = length(c(1990:2020))),
                  Year = rep(1990:2020, times = length(unique(grid_dat$GBDRegion))))

# Appending to dataset
GM_dat <- subset(GM_dat, PM25  > 0)
GM_dat <- rbind.fill(GM_dat, tmp)

# Taking logs of CTM
GM_dat$logPM25 <- log(GM_dat$PM25)
GM_dat$logSAT <- log(GM_dat$SAT + 1)
GM_dat$logDUST <- log(GM_dat$DUST + 1)
GM_dat$logSANOC <- log(GM_dat$SANOC + 1)
GM_dat$logPOP <- log(GM_dat$POP + 1)

# Country/Region Variables
GM_dat$Country1 <- GM_dat$CountryCode
GM_dat$Country2 <-  GM_dat$CountryCode
GM_dat$Country3 <-  GM_dat$CountryCode
GM_dat$Region <-  as.character(GM_dat$GBDRegion)
GM_dat$Region1 <-  as.character(GM_dat$GBDRegion)
GM_dat$Region2 <-  as.character(GM_dat$GBDRegion)
GM_dat$Region3 <-  as.character(GM_dat$GBDRegion)

# Time variables
GM_dat$time1 <- GM_dat$Year - min(GM_dat$Year) + 1
GM_dat$time2 <- GM_dat$Year - min(GM_dat$Year) + 1

# List of hyperpriors for model
hyperpriors <- list(prior.range1 = c(pi, 0.9),
                    prior.sigma1 = c(0.01, 0.9),
                    prior.range2 = c(pi, 0.9),
                    prior.sigma2 = c(0.01, 0.9),
                    prior.prec = c(0.01, 0.9))

# Removing unecessary datasets
rm(tmp, grid_dat)

####################################
### Creating a spatial dataframe ###
####################################
# Creating a dataset
GM_spdf <- subset(GM_dat, is.na(Longitude) == FALSE & is.na(Latitude) == FALSE)

# Converting to SpatialPointsDataFrame
coordinates(GM_spdf) <- ~ Longitude + Latitude

# Settign coordinate reference system
proj4string(GM_spdf) <- CRS("+proj=longlat +ellps=WGS84")

#########################
### Mesh for analysis ###
#########################
# Creating a raster
CountryCode <- raster(xmn = -180,
                      xmx = 180,
                      ymn = -55,
                      ymx = 70,
                      res = 0.5)

# Creating a country raster to turn into points
CountryCode <- rasterize(WHO_map, CountryCode,'CountryCode',fun='first')

# Extracting points in the raster that are on land 
CountryCode <- data.frame(rasterToPoints(CountryCode))[,1:2]
names(CountryCode) <- c('Longitude','Latitude')

# Creating spatial points dataframe for creating mesh
Points = SpatialPoints(coords = as.matrix(rbind(coordinates(GM_spdf),CountryCode)),
                       proj4string = CRS("+proj=longlat +ellps=WGS84"))

# Creating mesh for INLA runs 
mesh <- inla.mesh.create(loc = Points,
                         crs = inla.CRS("sphere"),
                         cutoff=2*pi/36000)

# Removing unecessary data
rm(CountryCode, Points, WHO_map)

##############################################################
### Spatio-temporal DIMAQ2 model for non-African countries ###
##############################################################
# Running DIMAQ model
out_nonAfrica <- inla.spat.downscaling(dat = GM_spdf,
                                       Y = logPM25,
                                       spat.slope = logSAT,
                                       effects =  logSAT + logPOP + logDUST + SANOC + ELEVDIFFALTD +
                                         PM25Conv + UnspecifiedType + ApproxLoc +
                                         logSAT * PM25Conv +
                                         logSAT * UnspecifiedType +
                                         logSAT * ApproxLoc +
                                         f(IDGRID, model="iid", hyper = list(prec = list(prior = 'pc.prec', param = hyperpriors$prior.prec)))+
                                         f(Region1, group = time1,  control.group = list(model = 'rw1'),model="iid", hyper = list(prec = list(prior = 'pc.prec', param = hyperpriors$prior.prec)), constr = FALSE) +
                                         f(Region2, logSAT, model="iid", hyper = list(prec = list(prior = 'pc.prec', param = hyperpriors$prior.prec)))+
                                         f(Region3, logPOP, model="iid", hyper = list(prec = list(prior = 'pc.prec', param = hyperpriors$prior.prec))),
                                       mesh = mesh,
                                       mode = list(theta = c(3.840875,3.591698,5.439992,3.183200,6.341872,-2.472978,-1.028626,-6.635792,-2.256992),
                                                   restart = TRUE),
                                       hyperpriors = hyperpriors,
                                       verbose = TRUE,
                                       config = TRUE)

# Summary of model
summary(out_nonAfrica$INLAObj)

# also save samples for reading in to generate predictions for each year

# Samples from non-African Countries
samp_nonAfrica <- inla.posterior.sample(n = draws.required, out_nonAfrica$INLAObj)

# Saving INLA objects
save(out_nonAfrica, samp_nonAfrica, file = file.path(out.dir,'INLAObjects_nonAfrica.RData'))

############################################
### Spatial DIMAQ2 for African Countries ###
############################################
# Only keeping non-African data
GM_spdf <- subset(GM_spdf, GBDSuperRegion %in% c('Sub-Saharan Africa', 'North Africa / Middle East') | CountryName == 'Western Sahara')

# Running DIMAQ model
out_Africa <- inla.spat.downscaling(dat = GM_spdf,
                                    Y = logPM25,
                                    spat.slope = logSAT,
                                    effects = intercept + logSAT + logPOP + logDUST + SANOC + ELEVDIFFALTD +
                                      PM25Conv + UnspecifiedType + ApproxLoc +
                                      logSAT * PM25Conv +
                                      logSAT * UnspecifiedType +
                                      logSAT * ApproxLoc +
                                      f(IDGRID, model="iid", hyper = list(prec = list(prior = 'pc.prec', param = hyperpriors$prior.prec))) +
                                      f(Region1, model="iid", hyper = list(prec = list(prior = 'pc.prec', param = hyperpriors$prior.prec)))+
                                      f(Region2, logSAT, model="iid", hyper = list(prec = list(prior = 'pc.prec', param = hyperpriors$prior.prec)))+
                                      f(Region3, logPOP, model="iid", hyper = list(prec = list(prior = 'pc.prec', param = hyperpriors$prior.prec))),
                                    mesh = mesh,
                                    mode = list(theta = c(2.4506,2.5395,5.2570,6.4893,8.9992,-1.0379,-2.6694,-2.4231,-2.3428),
                                                restart = TRUE),
                                    hyperpriors = hyperpriors,
                                    verbose = TRUE,
                                    config = TRUE)

# Summary of model
summary(out_Africa$INLAObj)

# also save samples for reading in to generate predictions for each year

# Samples from African Countries
samp_Africa <- inla.posterior.sample(n = draws.required, out_Africa$INLAObj)

# Saving INLA objects
save(out_Africa, samp_Africa, file = file.path(out.dir,'INLAObjects_Africa.RData'))

# Clearing workspace
rm(list = ls())


