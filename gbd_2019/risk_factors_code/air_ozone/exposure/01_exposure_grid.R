
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 10/4/2019
# Purpose: Read in ozone and produce .fst file of all grids with location ids
#          
# source("FILEPATH.R", echo=T)
# qsub -N air_ozone_exp -l fthread=30 -l m_mem_free=200G -l h_rt=10:00:00 -l archive -q long.q -P ADDRESS -o FILEPATH -e FILEPATH FILEPATH.sh FILEPATH.R 
#
# 150 GB, 30 threads, and 4.5 hrs for 1990:2019

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  } else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  }

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table", "ggplot2", "parallel", "magrittr", "maptools", "raster", "reshape2", 
              "rgdal", "rgeos", "sp", "splines", "ini", "fst", "ncdf4","doParallel","mgcv","zoo")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

cores.provided <- 30

years <- c(1990:2019)
extrap_years <- 2008:2017

run_map <- FALSE

version <- 11

# Directories -------------------------------------------------------------

#Get location Metadata
source(file.path(central_lib,"FILEPATH.R"))
#custom fx
"%ni%" <- Negate("%in%")

#helper functions
source(file.path(h_root,"FILEPATH.R"))

# input directory
in.dir <- file.path("FILEPATH")

out.dir <-  file.path("FILEPATH", version)
dir.create(out.dir, recursive=T, showWarnings=F)

# Read in files -----------------------------------------------------------

# Get the list of most detailed GBD locations
locs <- get_location_metadata(35)
locations <- locs[is_estimate==1, unique(location_id)]

# read in ozone raster
o3 <- nc_open(file.path(in.dir,"FILEPATH.nc"))
lon <- ncvar_get(o3,"longitude")
lat <- ncvar_get(o3,"latitude")
y <- ncvar_get(o3,"year")

ozone <- ncvar_get(o3,"ozone") %>% as.vector
ozone_var <- ncvar_get(o3,"variance") %>% as.vector

lonlat <- as.matrix(expand.grid(lon,lat,y))

pollution <- data.table(cbind(lonlat,ozone))
pollution <- data.table(cbind(pollution,ozone_var))
setnames(pollution ,c("Var1","Var2","Var3"),c("Longitude","Latitude","year_id"))
pollution <- pollution[!is.na(ozone)]
pollution <- pollution[year_id %in% c(years,extrap_years)]

pollution[,Latitude:=round(Latitude,digits=2)]
pollution[,Longitude:=round(Longitude,digits=2)]

setkeyv(pollution,c("Longitude","Latitude"))

rm(o3,lon,lat,y)

# Create id to extract to
pollution[, id := .GRP, by = c('Latitude', 'Longitude')]

if(run_map){
  
  # Read in GBD shapefile (this was prepped by lucas earl - mapping specialist)
  shapefile.dir <- file.path(j_root, "FILEPATH")
  shapefile.version <- "FILEPATH"
  borders <- readOGR(shapefile.dir, layer = shapefile.version)
  borders$location_id <- borders$loc_id
  
  
  grid<- unique(pollution[,.(id,Latitude,Longitude)])
  
  
  # Make a raster of the id column using sp package to create objects of class, "spatial points data frame"
  grid_map.sp <- grid
  coordinates(grid_map.sp) = ~Longitude+Latitude
  proj4string(grid_map.sp) = CRS("+init=epsg:4326") #this references a specific geodetic coding system explained here: https://www.nceas.ucsb.edu/~frazier/RSpatialGuides/OverviewCoordinateReferenceSystems.pdf
  gridded(grid_map.sp) = TRUE #promotes SpatialPointsDataFrame to SpatialGridDataFrame
  
  grid_map.sp <- raster(grid_map.sp[, c("id")]) #changes SpatialGridDataFrame to raster)
  
  
  # subset IHME shapefile to countries missing at least 10% of their area
  # sometimes it treats this variable as a factor, if so, here is a fix:
  if(data.class(borders$location_id)=="factor"){
    borders$location_id <- as.numeric(levels(borders$loc_id))[borders$loc_id] 
  }
  
  gc() # attempt to free up memory
  
  # Run extract function
  cl <- makePSOCKcluster(cores.provided)
  registerDoParallel(cl)
  
  raster.ids <- extract(grid_map.sp, borders, weights=TRUE, small=TRUE)
  
  endCluster()
  
  # Convert to dataframe with two columns
  temp <- NULL
  for (iii in 1:length(raster.ids)) {
    if (!is.null(raster.ids[[iii]])) {
      
      #message("binding locs in border #", iii)
      
      temp <- rbind(temp, data.table(location_id=borders$location_id[iii],
                                     location_name=borders$loc_nm_sh[iii],
                                     id=raster.ids[[iii]][,1],
                                     Weight=raster.ids[[iii]][,2]))
    }
  }
  
  # check for countries with some grids
  temp <- temp[!is.na(id)]
  # make sure weights scale to 1 for each location
  temp[,Weight:=Weight/sum(Weight),by="location_id"]
  
  map <- merge(temp,grid,by=c("id"),all.x=T)
  
  # Some countries are islands too small to pick up any grids. To correct for this,
  # we will take the average values for the rectangular area around islands + 1 (or more if necessary) degrees in any direction
  # Find out which ones
  missing.countries <- setdiff(unique(borders$location_id),unique(map$location_id))
  
  missing.countries.vals <- mclapply(missing.countries,
                                     estimateIslands,
                                     borders=borders,
                                     location_id.list=locs,
                                     mc.cores=cores.provided) %>% rbindlist()
  
  # make sure weights scale to 1 for each location
  missing.countries.vals[,Weight:=Weight/sum(Weight),by="location_id"]
  map <- rbind(map,missing.countries.vals,use.names=T)
  
  if(length(setdiff(unique(borders$location_id),unique(map$location_id)))>0){
    warning(paste("After estimate islands, still missing grids for the following location ids:
                ", setdiff(unique(borders$location_id),unique(map$location_id))))
  }
  
  # Save map for future use
  save(map,file=file.path(out.dir,"grid_map.RData"))
}else{
  load(file.path(out.dir,"grid_map.RData"))
}


# extrapolate each gridcell
# predict out for each gridcell for 2018 and 2019 using average trend from last 10 years
extrap <- mclapply(1:length(unique(map$id)),
                   lm_extrap,
                   dt=pollution,
                   in_years=extrap_years,
                   out_years=years[years>max(extrap_years)],
                   pred_vars=c("ozone","ozone_var"),
                   mc.cores=cores.provided) %>% rbindlist()

pollution <- rbind(pollution,extrap)

save(pollution, file=paste0(out.dir,"FILEPATH.RData"))

# merge on populations

#population
pop <- fread(file.path(in.dir,"FILEPATH.csv"))

weird_years <-  setdiff(c(years,extrap_years),c(1990:1999,2001:2004,2006:2009))

pop <- pop[, c("longitude","latitude","pop_1990","pop_1995",
               paste0("pop",weird_years,"v4r10")),
           with=F]

setnames(pop,c("latitude","longitude",paste0("pop",weird_years,"v4r10")),c("Latitude","Longitude",paste0("pop_",weird_years)))

pop[,pop_1990:=as.numeric(pop_1990)]
pop[,pop_1995:=as.numeric(pop_1995)]

pop <- melt.data.table(pop,id.vars=c("Longitude","Latitude"),measure.vars=patterns("pop_"),variable.name="year_id",value.name="pop")

pop[,year_id:=tstrsplit(year_id,split="_",keep=2)]
pop[,year_id:=as.integer(year_id)]

pop[is.na(pop),pop:=0] # set NA populations to zero for interpolation

# interpolate missing years of pop
for(this.year in setdiff(unique(pollution$year_id),unique(pop$year_id))){
  temp <- copy(pop[year_id==1990])
  temp[,pop:=NA]
  temp[,year_id:=this.year]
  
  pop <- rbind(pop,temp)
}


setkeyv(pop,c("Longitude","Latitude","year_id"))

pop <- na.approx(pop) %>% as.data.table

pollution <- merge(pollution,pop,by=c("Latitude","Longitude","year_id"),all.x=T)

# merge on map with location ids and weights
pollution <- merge(pollution, map[,-c("Latitude","Longitude")], by="id", all.x=T, allow.cartesian=T)

# save all grids and one file for each location, year
pdf(paste0(out.dir,"FILEPATH.pdf"))

for(year in unique(pollution$year_id)){
  write.fst(pollution[year_id==year],paste0(out.dir,"FILEPATH",year,".fst"))
  for(loc in na.omit(unique(pollution$location_id))){
    print(paste(loc,year))
    write.fst(pollution[year_id==year & location_id==loc],paste0(out.dir,"FILEPATH",loc,"_",year,".fst"))
  }
  
  
 gg <-ggplot(pollution[year_id==year],aes(x=Longitude,y=Latitude,fill=ozone))+
   geom_raster()+
   ggtitle(paste(year, "Ozone"))+
   scale_fill_gradientn(colours = terrain.colors(10))
 print(gg)
 
 gg <-ggplot(pollution[year_id==year],aes(x=Longitude,y=Latitude,fill=ozone))+
   geom_raster()+
   ggtitle(paste(year, "Pop"))+
   scale_fill_gradientn(colours = terrain.colors(10))
 print(gg)
  
}

dev.off()