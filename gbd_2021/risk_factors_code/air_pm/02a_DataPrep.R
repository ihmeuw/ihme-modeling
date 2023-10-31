# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- tail(commandArgs(),n=3) # First args are for unix use only
  if (length(arg)!=3) {
    arg <- c("43") #toggle targetted run 
  }
  
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- c("FILEPATH") 
  
}

# set parameters based on arguments from parent
grid.version <- arg[1]

library(data.table)
# source helper functions
source(file.path(h_root,"FILEPATH/00_Source.R"))

home.dir <- "FILEPATH"
in.dir <- file.path(home.dir,"input")
out.dir <- file.path(home.dir,"input","FILEPATH",grid.version)
dir.create(out.dir)

##############################################
### Creating a 10km iso3 raster over Globe ###
##############################################
# Reading in shapefiles
WHO_map <- readOGR(dsn=file.path(in.dir,"FILEPATH"),
                   layer = "LAYER")

# Creating a rank
WHO_map$CountryCode <- as.numeric(sapply(slot(WHO_map, "polygons"), function(x) slot(x, "ID"))) + 1

# Contentious borders
WHO_lines <- readOGR(file.path(in.dir,"FILEPATH.shp"))

# Contentious areas
WHO_regions <- readOGR(file.path(in.dir,"FILEPATH.shp"))

# # load GBD2020 analysis shapefile
# GBD_map <- readOGR(dsn=file.path(in.dir,"FILEPATH"), layer = "layer")
# # create a rank
# GBD_map$CountryCode <- as.numeric(sapply(slot(GBD_map, "polygons"), function(x) slot(x, "ID"))) + 1
# # load contentious borders and areas
# GBD_disputed <- readOGR(file.path(in.dir,"FILEPATH.shp"))

################### The following code takes a while to run, so country allocation  ###################
################### was run separately and saved to a raster (found underneath)     ###################
# # Creating a raster
# CountryCode <- raster(xmn = -180,
#                       xmx = 180,
#                       ymn = -55,
#                       ymx = 70,
#                       res = 0.01)
# 
# # Creating a country raster
# CountryCode <- rasterize(GBD_map, CountryCode,'CountryCode',fun='first')
# 
# # Loading raster
# writeRaster(CountryCode, filename = file.path(in.dir, 'FILEPATH.tif'), overwrite = TRUE)
# 
# # Aggregating to 10km
# CountryCode <- aggregate(CountryCode,
#                          fact = 10,
#                          fun = modal,
#                          na.rm = TRUE)
# 
# # Saving raster
# writeRaster(CountryCode, filename = file.path(in.dir, 'FILEPATH.tif'))
#######################################################################################################
#######################################################################################################

# Blank Raster for extending other rasters 
r0 <- raster(xmx = 180, 
             xmn = -180,
             ymx = 90, 
             ymn = -90,
             res = 0.1)

# Loading Country Raster 
CountryCode <- raster(file.path(in.dir,'FILEPATH.tif')); CountryCode <- extend(CountryCode, r0)


###############################################
### Calculating weights for the small areas ###
### Waiting for Kate to send                ###
###############################################
# Shapefiles used for lowest aggregration
GBD_map <- readOGR(dsn=file.path(in.dir,"FILEPATH"), layer = "layer")

#######################################################################################################
################### The following code takes a while to run, so aggregation weights ###################
################### were run separately and saved to a raster (found underneath)    ###################
#######################################################################################################
# # Creating a grid identifier
# r0[] <- 1:(dim(r0)[1]*dim(r0)[2])
# 
# # New ID for the outputs
# GBD_map$ID_new <- 1:nrow(GBD_map)
# 
# # Splitting between clusters
# GBD_map$Proc <- cut(1:nrow(GBD_map),
#                     breaks = quantile(1:nrow(GBD_map),
#                                       probs = seq(0,1, length.out = (nCluster + 1))),
#                     labels = 1:nCluster,
#                     include.lowest = TRUE)
# 
# # Number of clusters opened
# nCluster = 16
# 
# # Opening cluster
# cl <- makePSOCKcluster(nCluster)
# registerDoParallel(cl)
# 
# # Parallel loop for each
# Weights <- foreach(i = 1:nCluster, .combine = rbind) %dopar% {
#   # Only keeping allocated chunk
#   test <- subset(GBD_map, Proc == i)
#   # Loading packages
#   library(raster)
#   # Loop for each region
#   for (j in 1:nrow(test)){
#     # Only keeping allocated chunk
#     test2 <- test[j,]
#     # Extracting values
#     a1 <- raster::extract(r0, # DEFRA estimates
#                           test2, # Data zones
#                           weight = TRUE, # Give us Weights of the cells so we can do a weighted average of the cells we overlap
#                           small = TRUE) # Small areas in comparison to the raster
#     # Converting Weights to dataframes
#     tmp <- as.data.frame(a1)
#     # Removing NAs
#     tmp <- subset(tmp, !is.na(value))
#     # Reweighting the Weights after zeroes removed
#     tmp$weight <- tmp$weight/sum(tmp$weight)
#     # New Region Name
#     tmp$location_id <- test@data$loc_id[j]
#     tmp$location_name <- test@data$loc_name[j]
#     # Altering column names
#     names(tmp) <- c('IDGRID','Weight','location_id','location_name')
#     # Creating new dataset
#     if (j == 1) {out <- tmp}
#     else {out <- rbind(out, tmp)}
#   }
#   # Returning weights
#   return(out)
# }
# 
# # Closing cluster
# stopCluster(cl)
# 
# # Saving datasets
# write.csv(Weights, file = file.path(in.dir,'FILEPATH.csv'))
#########################################################################################################
#########################################################################################################

# Loading in aggregation weights 
Weights <- read.csv(file.path(in.dir,'FILEPATH.csv'))

#####################################
### Creating population estimates ###
#####################################
#########################################################################################################
################### The following code takes a while to run, so population estimates  ###################
################### were run separately and saved to a raster (found underneath)      ###################
#########################################################################################################

# # Turning raster to dataframe
# tmp <- as.data.frame(rasterToPoints(CountryCode))
# names(tmp) <- c('Longitude','Latitude','CountryCode')
# rm(CountryCode)
# 
# # Reading in GPWv3 estimates for 1990 and 1995
# data <- read.csv(file.path(in.dir,"FILEPATH.csv"),
#                  head = TRUE)
# 
# # Only keeping relevant columns
# data <- data[,c('longitude', 'latitude','pop_1990','pop_1995')]
# 
# # Altering column names
# names(data) <- c('Longitude', 'Latitude','POP_1990','POP_1995')
# 
# # Rounding coordinates
# tmp[,c('Longitude','Latitude')] <- round(tmp[,c('Longitude','Latitude')], digits = 2)
# data[,c('Longitude','Latitude')] <- round(data[,c('Longitude','Latitude')], digits = 2)
# 
# # Merging ISO3 to
# data <- merge(tmp,
#               data,
#               by = c('Longitude', 'Latitude'),
#               all.x = TRUE)
# 
# # Removing unecessary data
# rm(tmp)
# 
# # Loading
# files <- list.files("./Data/Raw/Population/", pattern = ".tif", full.names = T)
# files <- files[grep('gpw',files)]
# 
# # Loop for each file
# for (i in 1:length(files)){
#   # Open raster
#   pop = raster(files[i])
#   # Raster projection
#   projection(pop) = "+proj=longlat +datum=WGS84"
#   # Aggregating to 0.1^o x 0.1^o
#   pop = aggregate(pop, 12, sum)
#   # Converting raster to data frame
#   test <- data.frame(rasterToPoints(pop))
#   # Altering column names
#   names(test)[1:2] <- c('Longitude','Latitude')
#   # Rounding coordinates
#   test[,c('Longitude','Latitude')] <- round(test[,c('Longitude','Latitude')], digits = 2)
#   # Merging to database
#   data <- merge(data,
#                 test,
#                 by = c('Longitude','Latitude'),
#                 all.x = TRUE)
#   # Printing index
#   print(i)
# }
# 
# # Removing unecessary datasets
# rm(test, pop, files, i, r0)
# 
# # Altering column names
# names(data) <- c("Longitude","Latitude","CountryCode","POP_1990","POP_1995","POP_2000",
#                  "POP_2005","POP_2010","POP_2015","POP_2020")
# 
# # Zero for missing data
# data[is.na(data)] <- 0
# 
# # Blank columns for other years
# data[,paste('POP_',c(2001:2004, 2006:2009, 2011:2014, 2016:2019), sep = '')] <- NA
# 
# # Places to put the knots for the interpolating splines
# years <- seq(2000, 2020, 5)
# 
# # Number of clusters opened
# nCluster = 16
# 
# # Splitting between clusters
# data$Proc <- cut(1:nrow(data),
#                  breaks = quantile(1:nrow(data),
#                                    probs = seq(0,1, length.out = (nCluster + 1))),
#                  labels = 1:nCluster,
#                  include.lowest = TRUE)
# 
# # Opening cluster
# cl <- makePSOCKcluster(nCluster)
# registerDoParallel(cl)
# 
# # Parallel loop for each
# data <- foreach(i=1:nCluster, .combine=rbind) %dopar% {
#   # Only keeping allocated chunk
#   tmp <- subset(data, Proc == i)
#   # Loading packages
#   library(mgcv)
#   # Loop for each
#   for (j in 1:nrow(tmp)){
#     # Extracting grid cell
#     row <- tmp[j,c("POP_2000", "POP_2005", "POP_2010", "POP_2015", "POP_2020")]
#     # Skipping if population is empty
#     if (all(is.na(row))){next}
#     # Fitting spline
#     fit = spline(years, row, 21, method = "natural")
#     # Taking interpolating value
#     tmp$POP_2001[j] <- fit$y[fit$x == 2001]
#     tmp$POP_2002[j] <- fit$y[fit$x == 2002]
#     tmp$POP_2003[j] <- fit$y[fit$x == 2003]
#     tmp$POP_2004[j] <- fit$y[fit$x == 2004]
#     tmp$POP_2006[j] <- fit$y[fit$x == 2006]
#     tmp$POP_2007[j] <- fit$y[fit$x == 2007]
#     tmp$POP_2008[j] <- fit$y[fit$x == 2008]
#     tmp$POP_2009[j] <- fit$y[fit$x == 2009]
#     tmp$POP_2011[j] <- fit$y[fit$x == 2011]
#     tmp$POP_2012[j] <- fit$y[fit$x == 2012]
#     tmp$POP_2013[j] <- fit$y[fit$x == 2013]
#     tmp$POP_2014[j] <- fit$y[fit$x == 2014]
#     tmp$POP_2016[j] <- fit$y[fit$x == 2016]
#     tmp$POP_2017[j] <- fit$y[fit$x == 2017]
#     tmp$POP_2018[j] <- fit$y[fit$x == 2018]
#     tmp$POP_2019[j] <- fit$y[fit$x == 2019]
#   }
#   return(tmp)
# }
# 
# # Closing cluster
# stopCluster(cl)
# 
# # Ordering data
# data <- data[,c("Longitude","Latitude",'CountryCode',"POP_1990","POP_1995",
#                 paste('POP_',c(2000:2020), sep = ''))]
# 
# # Zero for missing data
# data[is.na(data)] <- 0
# 
# # Saving datasets
# for (i in 4:ncol(data)){
#   # Extracting relevant data
#   r <- data[,c(1:2,i)]
#   # Converting to raster
#   names(r) <- c('x','y','layer')
#   r <- rasterFromXYZ(r)
#   # Writing raster
#   writeRaster(r, filename = paste(names(data)[i], '.tif', sep = ''), overwrite = TRUE)
#   # Printing index
#   print(i)
# }
# 
# # Removing unecessary datasets
# rm(r)
#########################################################################################################
#########################################################################################################

############################################
### Projecting Satellite, SANOC and DUST ###
############################################
#########################################################################################################
################### The following code takes a while to run, so satellite estimates   ###################
################### were run separately and saved to rasters (found underneath)       ###################
#########################################################################################################
# # Loading Country Raster 
# grid_dat <- CountryCode
# names(grid_dat) <- 'CountryCode'
# 
# #Loop for each year
# for (i in c(2000:2018)){
#   # Loading in raster 
#   r_tmp1 <- raster(paste(in.dir, '/Data/Raw/Satellite/NEW_SATnoGWR_',i,'-0.1.nc', sep = '')); r_tmp1 <- extend(r_tmp1, r0)
#   r_tmp2 <- raster(paste(in.dir, '/Data/Raw/SANOC/NEW_SATnoGWR_SANOC_',i,'-0.1.nc', sep = '')); r_tmp2 <- extend(r_tmp2, r0)
#   r_tmp3 <- raster(paste(in.dir, '/Data/Raw/Dust/NEW_SATnoGWR_DUST_',i,'-0.1.nc', sep = '')); r_tmp3 <- extend(r_tmp3, r0)
#   # Giving the vector a name 
#   names(r_tmp1) <- paste('SAT_',i, sep = '')
#   names(r_tmp2) <- paste('SANOC_',i, sep = '')
#   names(r_tmp3) <- paste('DUST_',i, sep = '')
#   # Appending dataset 
#   grid_dat <- stack(grid_dat, r_tmp1, r_tmp2, r_tmp3)
#   # Printing index 
#   print(i)
# }
# 
# # Removing unecessary datasets 
# rm(r_tmp1, r_tmp2, r_tmp3)
# 
# # Converting to dataframe
# grid_dat <- as.data.frame(rasterToPoints(grid_dat))
# 
# # Removing unecessary data
# grid_dat <- subset(grid_dat, !is.na(CountryCode))
# 
# # Loop for each year
# for (i in c(2000:2018)){
#   # Resetting zeroes
#   grid_dat[, paste('SAT_',i, sep = '')] <- grid_dat[, paste('SAT_',i, sep = '')] + 1
#   grid_dat[, paste('SANOC_',i, sep = '')] <- grid_dat[, paste('SANOC_',i, sep = '')] + 1
#   grid_dat[, paste('DUST_',i, sep = '')] <- grid_dat[, paste('DUST_',i, sep = '')] + 1
#   # Printing index 
#   print(i)
# }
# 
# # Creating empty columns for projected years 
# grid_dat[,c('SAT_2019','SANOC_2019','DUST_2019',
#             'SAT_2020','SANOC_2020','DUST_2020',
#             'SAT_2021','SANOC_2021','DUST_2021',
#             'SAT_2022','SANOC_2022','DUST_2022')] <- as.numeric(NA)
#
# # Removing rows with data missing for all years 
# grid_dat <- grid_dat[!apply(grid_dat[,paste('SAT_',2000:2020, sep = '')], 1, function(x) all(is.na(x))),]
# 
# # Vector of years for spline basis
# year <- c(2000:2018)
# 
# # Number of clusters opened
# nCluster = 4
# 
# # Splitting
# grid_dat$Proc <- cut(1:nrow(grid_dat),
#                      breaks = quantile(1:nrow(grid_dat),
#                                        probs = seq(0,1, length.out = (nCluster + 1))),
#                      labels = 1:nCluster,
#                      include.lowest = TRUE)
# 
# # Opening cluster
# cl <- makePSOCKcluster(nCluster)
# registerDoParallel(cl)
# 
# # Parallel loop for each
# grid_dat <- foreach(i=1:nCluster, .combine=rbind) %dopar% {
#   # Only keeping allocated chunk
#   tmp <- subset(grid_dat, Proc == i)
#   # Loading packages
#   library(mgcv)
#   for (j in 1:nrow(tmp)){
#     # Extracting medians from 2010-2018
#     test1 <- as.numeric(tmp[j,paste('SANOC_',2000:2018, sep = '')])
#     test2 <- as.numeric(tmp[j,paste('DUST_',2000:2018, sep = '')])
#     test3 <- as.numeric(tmp[j,paste('SAT_',2000:2018, sep = '')])
#     # Creating spline fit
#     fit1 <- gam(log(test1) ~ 1 + s(year))
#     fit2 <- gam(log(test2) ~ 1 + s(year))
#     fit3 <- gam(log(test3) ~ 1 + s(year))
#     # Predicting for 2019-2022
#     tmp$SANOC_2019[j] <- exp(predict(fit1, newdata=list(year=2019)))
#     tmp$DUST_2019[j] <- exp(predict(fit2, newdata=list(year=2019)))
#     tmp$SAT_2019[j] <- exp(predict(fit3, newdata=list(year=2019)))
#     tmp$SANOC_2020[j] <- exp(predict(fit1, newdata=list(year=2020)))
#     tmp$DUST_2020[j] <- exp(predict(fit2, newdata=list(year=2020)))
#     tmp$SAT_2020[j] <- exp(predict(fit3, newdata=list(year=2020)))
#   }
#   return(tmp)
# }
# 
# # Closing cluster
# stopCluster(cl)
# 
# # Getting locations with more than  100 increase in 2018
# grid_dat$flag_SAT <- 0
# grid_dat$flag_SAT[which((grid_dat$SAT_2018 - grid_dat$SAT_2017)/grid_dat$SAT_2017 > 1)] <- 1
# grid_dat$flag_SANOC <- 0
# grid_dat$flag_SANOC[which((grid_dat$SANOC_2018 - grid_dat$SANOC_2017)/grid_dat$SANOC_2017 > 1.5)] <- 1
# grid_dat$flag_DUST <- 0
# grid_dat$flag_DUST[which((grid_dat$DUST_2018 - grid_dat$DUST_2017)/grid_dat$DUST_2017 > 1.5 & grid_dat$DUST_2017 > 0)] <- 1
# 
# # Vector of years for spline basis
# year <- c(2000:2018)
# 
# # Loading packages
# library(mgcv)
# 
# # Loop for each DUST grid cell 
# for (j in 1:nrow(grid_dat)){
#   if (grid_dat$flag_DUST[j] == 1){
#     # Extracting medians from 2010-2018
#     test <- as.numeric(grid_dat[j,paste('DUST_',2000:2018, sep = '')])
#     # Creating spline fit
#     fit1<- gam(log(test) ~ 1 + s(year))
#     # Predicting for 2019-2022
#     grid_dat$DUST_2019[j] <- exp(predict(fit1, newdata=list(year=2019)))
#     grid_dat$DUST_2020[j] <- exp(predict(fit1, newdata=list(year=2020)))
#   }
#   if (grid_dat$flag_SANOC[j] == 1){
#     # Extracting medians from 2010-2018
#     test <- as.numeric(grid_dat[j,paste('SANOC_',2000:2018, sep = '')])
#     # Creating spline fit
#     fit1<- gam(log(test) ~ 1 + s(year))
#     # Predicting for 2019-2022
#     grid_dat$SANOC_2019[j] <- exp(predict(fit1, newdata=list(year=2019)))
#     grid_dat$SANOC_2020[j] <- exp(predict(fit1, newdata=list(year=2020)))
#   }
#   if (grid_dat$flag_SAT[j] == 1){
#     # Extracting medians from 2010-2018
#     test <- as.numeric(grid_dat[j,paste('SAT_',2000:2018, sep = '')])
#     # Creating spline fit
#     fit1<- gam(log(test) ~ 1 + s(year))
#     # Predicting for 2019-2022
#     grid_dat$SAT_2019[j] <- exp(predict(fit1, newdata=list(year=2019)))
#     grid_dat$SAT_2020[j] <- exp(predict(fit1, newdata=list(year=2020)))
#   }
#   if (grid_dat$flag_SAT[j] == 1 | grid_dat$flag_SANOC[j] == 1 | grid_dat$flag_DUST[j] == 1){print(j)}
# }
# 
# # Saving forecasted rasters
# for (i in c(2019, 2020)){
#   # Extracting column
#   tmp1 <- grid_dat[,c('x','y',paste('SAT_', i, sep = ''))]
#   tmp2 <- grid_dat[,c('x','y',paste('SANOC_', i, sep = ''))]
#   tmp3 <- grid_dat[,c('x','y',paste('DUST_', i, sep = ''))]
#   # Altering column names
#   names(tmp1) <- c('x','y','layer')
#   names(tmp2) <- c('x','y','layer')
#   names(tmp3) <- c('x','y','layer')
#   # Converting to raster
#   tmp1 <- rasterFromXYZ(tmp1)
#   tmp2 <- rasterFromXYZ(tmp2)
#   tmp3 <- rasterFromXYZ(tmp3)
#   # Removing by one 
#   tmp1 <- tmp1 - 1
#   tmp2 <- tmp2 - 1
#   tmp3 <- tmp3 - 1
#   # Check that they are not below zero 
#   tmp1[tmp1 < 0] <- 0
#   tmp2[tmp2 < 0] <- 0
#   tmp3[tmp3 < 0] <- 0
#   # Outputting raster
#   writeRaster(tmp1, filename = paste(in.dir,'/','FILEPATH/NEW_SATnoGWR_',i,'-0.1.nc',sep = ''), overwrite = TRUE)
#   writeRaster(tmp2, filename = paste(in.dir,'/','FILEPATH/NEW_SATnoGWR_SANOC_',i,'-0.1.nc',sep = ''), overwrite = TRUE)
#   writeRaster(tmp3, filename = paste(in.dir,'/','FILEPATH/NEW_SATnoGWR_DUST_',i,'-0.1.nc',sep = ''), overwrite = TRUE)
# }
# 
# # Removing unecessary datasets
# rm(grid_dat)
# 
# # Backcasting to 1990 and 1995
# for (i in c(1990,1995)){
#   # Loading 2000 estimates for backcasting
#   r_tmp1 <- raster('Data/Raw/Satellite/NEW_SATnoGWR_2000-0.1.nc')
#   r_tmp2 <- raster('Data/Raw/SANOC/NEW_SATnoGWR_SANOC_2000-0.1.nc')
#   r_tmp3 <- raster('Data/Raw/Dust/NEW_SATnoGWR_DUST_2000-0.1.nc')
#   r_tmp4 <- raster('Data/Raw/ELEVDIFFALTD/NEW_SATnoGWR_ELEVDIFFALTD_2000-0.1.nc')
#   # Loading in conversions
#   Conv <- raster(paste('Data/Raw/Misc/Scalar2000To',i,'.nc', sep = ''))
#   # Backcasting
#   r_tmp1 <- Conv * r_tmp1
#   r_tmp2 <- Conv * r_tmp2
#   r_tmp3 <- Conv * r_tmp3
#   r_tmp4 <- r_tmp4
#   # Outputting rasters
#   writeRaster(r_tmp1, filename = paste('FILEPATH/NEW_SATnoGWR_',i,'.nc',sep = ''), overwrite = TRUE)
#   writeRaster(r_tmp2, filename = paste('FILEPATH/NEW_SATnoGWR_SANOC_',i,'.nc',sep = ''), overwrite = TRUE)
#   writeRaster(r_tmp3, filename = paste('FILEPATH/NEW_SATnoGWR_DUST_',i,'.nc',sep = ''), overwrite = TRUE)
#   writeRaster(r_tmp4, filename = paste('FILEPATH/NEW_SATnoGWR_ELEVDIFFALTD_',i,'-0.1.nc',sep = ''), overwrite = TRUE)
#   # Printing index
#   print(i)
# }
# 
# # Removing unecessary datasets
# rm(Conv, r_tmp1, r_tmp2, r_tmp3, r_tmp4)
#########################################################################################################
#########################################################################################################

#################################
### Urban Rural classfication ###
#################################
#########################################################################################################
################### The following code takes a while to run, so Urban/rural class     ###################
################### were run separately and saved to a raster (found underneath)      ###################
#########################################################################################################
# # Loading raster
# Urban <- raster('FILEPATH.tif')
# 
# # Turning to dataframe
# Urban <- as.data.frame(rasterToPoints(Urban))
# 
# # Copying to turn into a
# Urban_spdf <- Urban
# 
# # Converting to spatial dataframe
# coordinates(Urban_spdf) <- ~ x + y
# 
# # Adding projection
# proj4string(Urban_spdf) <- CRS('+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
# 
# # Converting to UTM
# Urban_spdf <- spTransform(Urban_spdf,
#                                     CRS = CRS("+proj=moll +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"))
# 
# # Extracting new coordinates
# Urban_spdf <- data.frame(coordinates(Urban_spdf))
# 
# # Altering colimn names
# names(Urban_spdf) <- c('X1','Y1')
# 
# # Appending together
# Urban <- cbind(Urban, Urban_spdf)
# 
# # Removing unecessary data
# rm(Urban_spdf)
# 
# # Loading raster
# s <- raster('FILEPATH.tif')
# 
# # Extracting grid values from
# Urban$layer <- extract(s, Urban[,c('X1','Y1')])
# 
# # Only keeping relevant data
# Urban <- Urban[,c('x','y','layer')]
# 
# # Converting to raster
# Urban <- rasterFromXYZ(Urban)
# 
# # Saving raster at 1km
# writeRaster(Urban, filename = 'FILEPATH.tif', overwrite = TRUE)
# 
# # Aggregating to a 10km x 10km grid
# Urban <- aggregate(Urban,
#                    fact = 10,
#                    fun = max,
#                    na.rm = TRUE)
# 
# # Saving raster at 10km
# writeRaster(Urban, filename = 'FILEPATH.tif', overwrite = TRUE)
#########################################################################################################
#########################################################################################################

# Loading urban rural classification
Urban <- raster(file.path(in.dir,'FILEPATH.tif')); Urban <- extend(Urban, r0)

############################
### Grid Cell Identifier ###
############################
# Creating a grid identifier
r0[] <- 1:(dim(r0)[1]*dim(r0)[2])

############################
### Regional Information ###
############################
# Extracting WHO regional information
Regions <- WHO_map@data[,c("CountryCode", "ISO_3_CODE", "CNTRY_TERR", "WHO_REGION")]

# Altering column names
names(Regions) <- c("CountryCode", "ISO3", "CountryName", "WHORegion")

# WHO Income Regions
WHOIncomeRegions <- read.csv(file.path(in.dir,'FILEPATH.csv'),
                             stringsAsFactors = FALSE)

# Putting
WHOIncomeRegions$WHOIncomeRegion[WHOIncomeRegions$WHOIncomeRegion == 'AFR HI'] <- 'AFR'
WHOIncomeRegions$WHOIncomeRegion[WHOIncomeRegions$WHOIncomeRegion == 'AFR LM'] <- 'AFR'

# Merging to WHO regions
Regions <- merge(Regions,
                 WHOIncomeRegions[,c('ISO3','WHOIncomeRegion')],
                 by = 'ISO3',
                 all.x = TRUE)

# Converting from factors
Regions$ISO3 <- as.character(Regions$ISO3)
Regions$CountryName <- as.character(Regions$CountryName)
Regions$WHORegion <- as.character(Regions$WHORegion)
Regions$WHOIncomeRegion <- as.character(Regions$WHOIncomeRegion)

# Putting unknown for income regions
Regions$WHOIncomeRegion[is.na(Regions$WHOIncomeRegion)] <- 'Unknown'

# Removing unecessary datasets
rm(WHOIncomeRegions)

# Reading in SDG Region
SDGRegion <- read.dta(file.path(in.dir,'FILEPATH.dta'))

# Updating Region names as hidden characters we read in
SDGRegion$sdg1[SDGRegion$sdg1 == sort(unique(SDGRegion$sdg1))[2]] <- "Australia and New Zealand"
SDGRegion$sdg1[SDGRegion$sdg1 == sort(unique(SDGRegion$sdg1))[3]] <- "Central Asia and Southern Asia"
SDGRegion$sdg1[SDGRegion$sdg1 == sort(unique(SDGRegion$sdg1))[4]] <- "Eastern Asia and South-eastern Asia"
SDGRegion$sdg1[SDGRegion$sdg1 == sort(unique(SDGRegion$sdg1))[5]] <- "Latin America and the Caribbean"
SDGRegion$sdg1[SDGRegion$sdg1 == sort(unique(SDGRegion$sdg1))[6]] <- "Northern America and Europe"
SDGRegion$sdg1[SDGRegion$sdg1 == sort(unique(SDGRegion$sdg1))[7]] <- "Oceania excluding Australia and New Zealand"
SDGRegion$sdg1[SDGRegion$sdg1 == sort(unique(SDGRegion$sdg1))[8]] <- "Sub-Saharan Africa"
SDGRegion$sdg1[SDGRegion$sdg1 == sort(unique(SDGRegion$sdg1))[9]] <- "Western Asia and Northern Africa"

# Only keeping relevant columns
SDGRegion <- SDGRegion[,c('iso3','sdg1')]
names(SDGRegion) <- c('ISO3', 'SDGRegion')

# Merging to WHO regions
Regions <- merge(Regions,
                   SDGRegion,
                   by = 'ISO3',
                   all.x = TRUE)

# If no GBD Region defined, place as unknown
Regions$SDGRegion[is.na(Regions$SDGRegion)] <- 'Unknown'
Regions$SDGRegion[Regions$SDGRegion == ''] <- 'Unknown'

# GBD Regional information
GBDRegion <- read.csv(file.path(in.dir,'FILEPATH.csv'),
                      stringsAsFactors = FALSE)

# Only keeping relevant information
GBDRegion <- GBDRegion[,c('iso3','reporting_region_name','Super_region_name')]

# Altering column names
names(GBDRegion) <- c('ISO3','GBDRegion','GBDSuperRegion')

# Merging to WHO regions
Regions <- merge(Regions,
                   GBDRegion,
                   by = 'ISO3',
                   all.x = TRUE)

# If no GBD Region defined, place as unknown
Regions$GBDRegion[is.na(Regions$GBDRegion)] <- 'Unknown'
Regions$GBDSuperRegion[is.na(Regions$GBDSuperRegion)] <- 'Unknown'

# Removing unecessary datasets
rm(GBDRegion, SDGRegion)

################################
### Creating prediction grid ###
################################
# Population needed for data cleaning rules 
POP_2016 <- raster(file.path(in.dir,'FILEPATH.tif')); POP_2016 <- extend(POP_2016, r0)

# Creating stack
r <- stack(CountryCode, r0, Urban, POP_2016)

# Altering names 
names(r) <- c('CountryCode', 'IDGRID', 'Urban', 'POP_2016')

# Removing unecessary datasets
rm(CountryCode, r0, Urban, POP_2016)

# Converintg raster to points
grid_dat <- rasterToPoints(r)

# Converting to data frame 
grid_dat <- as.data.frame(grid_dat)

# Removing unecessary data
grid_dat <- subset(grid_dat, !is.na(CountryCode))

# Altering column names
names(grid_dat)[1] <- 'Longitude'
names(grid_dat)[2] <- 'Latitude'

# Rounding coordinates 
grid_dat$Longitude <- round(grid_dat$Longitude, digits = 2)
grid_dat$Latitude <- round(grid_dat$Latitude, digits = 2)

# Merging Regional information to prediction data
grid_dat <- merge(grid_dat,
                  Regions,
                  by = 'CountryCode',
                  all.x = TRUE)

##############################
### Ground monitoring data ###
##############################

# Read in processed data
GM_dat <- read.csv(file.path(in.dir,'FILEPATH.csv'),
                   stringsAsFactors = FALSE) %>% as.data.table

# Outlier observations w/ PM10 > 1000 ug/m3
GM_dat <- GM_dat[is.na(PM10) | PM10<1000.00]

# Number of ground monitors by countries
numGMs <- ddply(GM_dat, 
                .(CountryName),
                summarise,
                N.GMs = length(CountryName))

# Number of grid cells and Population by countries
numGrid <- ddply(grid_dat, 
                 .(CountryName),
                 summarise,
                 N.Cells = length(CountryName),
                 POP = sum(POP_2016, na.rm = TRUE))

# Merging number of grid cells to number of GMs to calculate density
numGMs <- merge(numGMs, 
                numGrid,
                by = 'CountryName',
                all.x = FALSE,
                all.y = FALSE)

# Monitor Density 
numGMs$GMsPer100000 <- 100000 * numGMs$N.GMs / numGMs$POP
numGMs$GMsPer100KM2 <- numGMs$N.GMs / numGMs$N.Cells

# Keeping those with less than 0.1 per 100,000
tmp <- subset(GM_dat, CountryName %in% unique(numGMs$CountryName[numGMs$GMsPer100000 < 0.1]))

# Removing these GMs 
GM_dat <- subset(GM_dat, !(CountryName %in% unique(numGMs$CountryName[numGMs$GMsPer100000 < 0.1])))

# Only keeping those with Coverage over 0.75 and those with missing coverage
GM_dat <- subset(GM_dat, (PM25Grading == 0 | PM10Grading == 0 |
                          PM25Grading == 1 | PM10Grading == 1 |
                          PM25Grading == 4 | PM10Grading == 4))

# Removing remaining data which don't meet the standard 
GM_dat$PM25[which(GM_dat$PM25Grading %in% 2:3)] <- NA
GM_dat$PM10[which(GM_dat$PM10Grading %in% 2:3)] <- NA
GM_dat$PM25PercCoverage[which(GM_dat$PM25Grading %in% 2:3)] <- NA
GM_dat$PM10PercCoverage[which(GM_dat$PM10Grading %in% 2:3)] <- NA
GM_dat$PM25Grading[which(GM_dat$PM25Grading %in% 2:3)] <- NA
GM_dat$PM10Grading[which(GM_dat$PM10Grading %in% 2:3)] <- NA

# Adding those withdrawn sites back in 
GM_dat <- rbind(GM_dat, tmp)

# Removing unecessary datasets
rm(numGMs, numGrid, tmp)

# Converting to PM2.5
RatioQuantiles <- quantile(GM_dat$PM25 / GM_dat$PM10, na.rm = TRUE, probs = c(0.025, 0.975))
Predictions <- PredictPM25(GM_dat, method = "newlocal", default = "median", minvalue = RatioQuantiles[1], maxvalue = RatioQuantiles[2])
GM_dat$PM25[Predictions$PredIndex] <- Predictions$PM25Pred

# Calculating conversion factor 
GM_dat$ConvFactor <- GM_dat$PM25/GM_dat$PM10

# Adding column for prediction method (used within RMarkdown)
PredMethod <- c(rep(NA, dim(GM_dat)[1]))
PredMethod[Predictions$PredIndex] <- Predictions$PM25Method
GM_dat <- cbind(GM_dat, PredMethod)
GM_dat$PredMethod <- as.character(GM_dat$PredMethod)

# Removing unecessary datasets
rm(Predictions, PredMethod, RatioQuantiles)

# Extracting grid values from
GM_dat <- cbind(GM_dat,
                as.data.frame(raster::extract(r, GM_dat[,c('Longitude','Latitude')])))

# Setting blank entries for the covariates 
GM_dat$SAT <- as.numeric(NA)
GM_dat$SANOC <- as.numeric(NA)
GM_dat$DUST <- as.numeric(NA)
GM_dat$POP <- as.numeric(NA)
GM_dat$ELEVDIFFALTD <- as.numeric(NA)

# Covariates by year
for (i in sort(unique(GM_dat$Year))){
  # Loading covariate data by year
  if(i %in% c(1990,1995)){
    # Loading covariate data by year
    r_tmp1 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_',i,'.nc', sep = ''))
    r_tmp2 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_SANOC_',i,'.nc', sep = ''))
    r_tmp3 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_DUST_',i,'.nc', sep = ''))
    r_tmp4 <- raster(paste(in.dir,'FILEPATH/POP_',i,'.tif', sep = ''))
    r_tmp5 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_ELEVDIFFALTD_',i,'-0.1.nc', sep = ''))
 }else if(i %in% c(2020)){
    # Loading covariate data by year
    r_tmp1 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_',i,'-0.1.nc', sep = ''))
    r_tmp2 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_SANOC_',i,'-0.1.nc', sep = ''))
    r_tmp3 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_DUST_',i,'-0.1.nc', sep = ''))
    r_tmp4 <- raster(paste(in.dir,'FILEPATH/POP_',i,'.tif', sep = ''))
    r_tmp5 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_ELEVDIFFALTD_',i,'-0.1.nc', sep = ''))
  }else{
    # Loading covariate data by year
    r_tmp1 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_',i,'-0.1.nc', sep = ''))
    r_tmp2 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_SANOC_',i,'-0.1.nc', sep = ''))
    r_tmp3 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_DUST_',i,'-0.1.nc', sep = ''))
    r_tmp4 <- raster(paste(in.dir,'FILEPATH/POP_',i,'.tif', sep = ''))
    r_tmp5 <- raster(paste(in.dir,'FILEPATH/NEW_SATnoGWR_ELEVDIFFALTD_',i,'-0.1.nc', sep = ''))
  }
  # Extracting 
  GM_dat$SAT[which(GM_dat$Year == i)] <- raster::extract(r_tmp1, GM_dat[which(GM_dat$Year == i),c('Longitude','Latitude')])
  GM_dat$SANOC[which(GM_dat$Year == i)] <- raster::extract(r_tmp2, GM_dat[which(GM_dat$Year == i),c('Longitude','Latitude')])
  GM_dat$DUST[which(GM_dat$Year == i)] <- raster::extract(r_tmp3, GM_dat[which(GM_dat$Year == i),c('Longitude','Latitude')])
  GM_dat$POP[which(GM_dat$Year == i)] <- raster::extract(r_tmp4, GM_dat[which(GM_dat$Year == i),c('Longitude','Latitude')])
  GM_dat$ELEVDIFFALTD[which(GM_dat$Year == i)] <- raster::extract(r_tmp5, GM_dat[which(GM_dat$Year == i),c('Longitude','Latitude')])
  # Printing index
  print(i)
}

# Removing unecessary datasets
rm(r, r_tmp1, r_tmp2, r_tmp3, r_tmp4, r_tmp5)

# Redefining CountryCode
GM_dat$CountryCode <- NULL
GM_dat <- merge(GM_dat, 
                Regions[,c('ISO3','CountryCode','WHOIncomeRegion')],
                by = 'ISO3',
                all.x = TRUE)

# Removing unecessary datasets
rm(Regions)

# Removing those with missing PM2.5
GM_dat <- subset(GM_dat, !is.na(PM25))

# Sorting dataset 
GM_dat <- GM_dat[order(GM_dat$ISO3, GM_dat$Longitude, GM_dat$Latitude, GM_dat$Year),]

# Creating unique ID for the observation
GM_dat <- cbind(ID = 1:nrow(GM_dat), GM_dat)

# Ordering variables 
GM_dat <- GM_dat[,c("ID","StationID", 'City',"ISO3","CountryName","CountryCode","Year","PM25",'PM10',"PM25PercCoverage",
                 'PM10PercCoverage','PM25Grading','PM10Grading',"Longitude","Latitude","MonitorType","UnspecifiedType",
                 "ApproxLoc","PM25Conv","IDGRID","SAT","DUST","SANOC","POP","ELEVDIFFALTD","GBDRegion","GBDSuperRegion", 
                 "WHORegion","WHOIncomeRegion","LocationInfo", "PredMethod", "ConvFactor", "WebLink",'Source')]

#######################
### Saving datasets ###
#######################
# Shapefiles
save(WHO_map, WHO_lines, WHO_regions, GBD_map, 
     file = file.path(out.dir,'shapefiles.RData'))

# GM data
save(GM_dat, file = file.path(out.dir,'GM_dat.RData'))
write.csv(GM_dat, file = file.path(out.dir,'GM_dat.csv'), row.names = FALSE)

# Gridded data
save(grid_dat, file = file.path(out.dir,'grid_dat.RData'))
write.csv(grid_dat, file = file.path(out.dir,'grid_dat.csv'), row.names = FALSE)

# Weights for aggregation
save(Weights, file = file.path(out.dir,'Weights.RData'))
write.csv(Weights, file= file.path(out.dir,'Weights.csv'), row.names = FALSE)



