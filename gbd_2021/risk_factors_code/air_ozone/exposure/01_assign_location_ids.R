
#----Set-up----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list= ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}

# load packages, install if missing
lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table", "ggplot2", "parallel", "magrittr", "maptools", "raster", "reshape2", 
              "rgdal", "rgeos", "sp", "splines", "ini", "fst", "ncdf4", "foreach", "doParallel")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}



air_pm_version <- "VERSION"  #version to use for mapping gridcells to shapefile
cores.provided <- 5

years <- c(1990,2010,2017)

# ---Functions and directories------------------------------------------------------------------------------------------
# directories
in.dir <- file.path("FILEPATH")
out.dir <-  file.path("FILEPATH", version) # output the split gridded files here
dir.create(paste0(out.dir), recursive=T, showWarnings=F)

source(file.path(central_lib,"get_location_metadata.R"))
# Get the list of most detailed GBD locations
locs <- get_location_metadata(35)
locations <- locs[is_estimate==1, unique(location_id)]

"%ni%" <- Negate("%in%") # create a reverse %in% operator

# Island Function
# this function is used to estimate pollution for islands or small countries that don't have large enough borders to pick up a grid
# the way it works is by walking around the border using 1 degree of padding (+.5 degree everytime it fails) until we pick up more than 10 grids
# we use the average of these grids as the estimate
estimateIslands <- function(country,
                            borders,
                            location_id.list) {
  
  # Get a rectangle around the island in question
  poly <- borders@polygons[[which(borders$location_id == country)]]
  
  # Begin with 5 degrees of padding in every direction
  distance.to.look <- 1
  looping <- TRUE
  
  #define the true extent of the island polygon
  min.x <- 1000
  min.y <- 1000
  max.x <- -1000
  max.y <- -1000
  
  for (iii in 1:length(poly@Polygons)) {
    max.x <- max(max.x, max(poly@Polygons[[iii]]@coords[,1]))
    max.y <- max(max.y, max(poly@Polygons[[iii]]@coords[,2]))
    min.x <- min(min.x, min(poly@Polygons[[iii]]@coords[,1]))
    min.y <- min(min.y, min(poly@Polygons[[iii]]@coords[,2]))
  }
  
  # this loop will continue to add a half degree of padding in every direction as long as we are unable to find 10 surrounding grids
  while (looping) {
    
    #print loop status
    cat(location_id.list[location_id==country, location_name],
        "-trying w/ degrees of padding:",
        distance.to.look,
        "\n"); flush.console
    
    #add the padding to your island polygon
    padded.min.x <- min.x - distance.to.look
    padded.min.y <- min.y - distance.to.look
    padded.max.x <- max.x + distance.to.look
    padded.max.y <- max.y + distance.to.look
    
    # find out how many grids fall within the current (padded) extent
    temp <- pollution[which(pollution$longitude <= padded.max.x
                            & pollution$longitude >= padded.min.x
                            & pollution$latitude <= padded.max.y
                            & pollution$latitude >= padded.min.y), ]
    # drop missing grids
    temp <- na.omit(temp)
    
    # add a half degree to the distance in case we end up needing to reloop
    distance.to.look <- distance.to.look + .5
    
    # inform the loop whether we have discovered more than 10 grids nearby using the current padding
    looping <- !(nrow(temp) > 10)
    
    # output loop status and if we were successful how many grids were found
    loop.output <- ifelse(looping==TRUE, "FAILED", paste0("SUCCESS, pixels found #", nrow(temp)))
    cat(loop.output, "\n"); flush.console()
    
  }
  
  # must convert location IDs from factor in order to collapse them, take as character to avoid returning the underlying values which seem to be incorrect
  temp$location_id <- as.numeric(as.character(temp$location_id))
  
  temp$one <- 1
  temp <- aggregate(temp[,-c("location_name")], by=list(temp$one), FUN=mean)
  temp$Group.1 <- temp$one <- NULL
  
  # Prep to be added on to the dataset
  temp$location_id <- country
  temp$location_name <- as.character(location_id.list[location_id==country,.(location_name)])
  temp$longitude <- round(mean(max.x, min.x) * 20) / 20 # All real grids are at .05 units of latitude/longitude.
  temp$latitude <- round(mean(max.y, min.y) * 20) / 20
  temp$weight <- 1
  
  temp <- as.data.table(temp)
  
  return(temp)
  
}


# this is a wrapper that subsets to country, reshapes wide, and saves a csv
castAndSave <- function(global.dt,
                        country) {
  
  cat(country, "\n"); flush.console()
  
  #check to see if already saved for re running failed files
  if (paste0(country,".csv") %ni% list.files(out.dir)){
    
    #subset to country
    temp <- global.dt[location_id==country, ]
    
    #reshape the dt wide
    temp <- dcast(temp,
                  location_id + location_name + longitude + latitude + perurban + year ~ var, #formula to cast over
                  value.var="value") %>% as.data.table()
    
    # create a grid ID variable, can only be done on a single grid at a time
    temp[, grid := as.numeric(as.factor(paste0(longitude,latitude)))]
    
    # ensure that year is a numeric, after cast it is becoming character and it needs to be a number to be used in the extrapolation formula
    temp[, year := as.numeric(year)]
    
    
    #write the country CSV so we can run in parallel at later stages
    write.csv(temp,
              file.path(out.dir, paste0(country, ".csv")),
              row.names=F)
    
    #also return each country to a list in case we want to look at them interactively or do testing
    return(temp)
    
  }
}


#----PREP GRID-------------------------------------------------------------------------------------------------------------------
#Already prepped the grid for ambient, use this for ozone.
load(paste0("FILEPATH",air_pm_version,"/grid_dat.RData"))
load(paste0("FILEPATH",air_pm_version,"/Weights.RData"))

pm_grid <- merge(grid_dat,Weights,by="IDGRID") %>% as.data.table()
pm_grid <- pm_grid[,.(Longitude, Latitude, Weight, location_id,location_name)]

rm(grid_dat,Weights)

# read in ozone raster
o3 <- nc_open(file.path(in.dir,"Ozone1990_2017.nc"))
lon <- ncvar_get(o3,"longitude")
lat <- ncvar_get(o3,"latitude")
y <- ncvar_get(o3,"year")

ozone <- ncvar_get(o3,"ozone") %>% as.vector
ozone_var <- ncvar_get(o3,"variance") %>% as.vector

lonlat <- as.matrix(expand.grid(lon,lat,y))

dt <- data.table(cbind(lonlat,ozone))
dt <- data.table(cbind(dt,ozone_var))
setnames(dt ,c("Var1","Var2","Var3"),c("Longitude","Latitude","year_id"))
dt <- dt[!is.na(ozone)]

# Create id to extract to for countries with missingness
dt[, id := .GRP, by = c('Latitude', 'Longitude')]

rm(ozone,ozone_var,lon,lat,lonlat)

pm_grid[,Longitude:=round(Longitude,2)]
pm_grid[,Latitude:=round(Latitude,2)]
dt[,Longitude:=round(Longitude,2)]
dt[,Latitude:=round(Latitude,2)]

setkeyv(pm_grid,c("Longitude","Latitude"))
setkeyv(dt,c("Longitude","Latitude"))

ozone_grid <- merge(pm_grid,unique(dt[,.(Latitude,Longitude,id)]),all.x=F,allow.cartesian=T)
ozone_grid[,missing:=0]
ozone_grid[is.na(id),missing:=1]

# countries we're missing more than 10% of their grids
missing <- ozone_grid[,mean(missing),by=c("location_name","location_id")][V1>.1]

ozone_grid <- ozone_grid[!(location_id %in% missing)]

#make sure weights scale to 1 for each location
ozone_grid[,test:=Weight/sum(Weight),by="location_id"]

gc()


# For these missing countries, obtain from the shapefile
# Pull in the global shapefile
shapefile.dir <- file.path(j_root, "FILEPATH")
shapefile.version <- "VERSION"
borders <- readOGR(shapefile.dir, layer = shapefile.version)
borders$location_id <- borders$loc_id

# Make a raster of the id column using sp package to create objects of class, "spatial points data frame"
grid_map.sp <- dt[, c("Latitude", "Longitude", "id"), with=F] %>% unique()
coordinates(grid_map.sp) = ~Longitude+Latitude
proj4string(grid_map.sp) = CRS("+init=epsg:4326") #this references a specific geodetic coding system explained here: https://www.nceas.ucsb.edu/~frazier/RSpatialGuides/OverviewCoordinateReferenceSystems.pdf
gridded(grid_map.sp) = TRUE #promotes SpatialPointsDataFrame to SpatialGridDataFrame

grid_map.sp <- raster(grid_map.sp[, c("id")]) #changes SpatialGridDataFrame to raster


# subset IHME shapefile to countries missing at least 10% of their area
# sometimes it treats this variable as a factor, if so, here is a fix:
if(data.class(borders$location_id)=="factor"){
  borders$location_id <- as.numeric(levels(borders$loc_id))[borders$loc_id] 
}
borders <- subset(borders,location_id %in% missing$location_id)


gc()

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

# check for countries with grids
temp <- temp[!is.na(id)]
#make sure weights scale to 1 for each location
temp[,Weight:=Weight/sum(Weight),by="location_id"]

# merge on lat long
temp <- merge(temp, dt[, c("Latitude", "Longitude", "id"), with=F] %>% unique(), by=c("Latitude","Longitude"),all.x=T)

# bind on to ozone grid
ozone_grid <- rbind(ozone_grid,temp,use.names=T)

# Some countries are too small islands to pick up any grids. To correct for this,
# we take the average values for the rectangular area around islands + 1 (or more if necessary) degrees in any direction

# Identify missing countries
missing.countries <- missing$location_id

missing.countries.vals <- mclapply(missing.countries,
                                   estimateIslands,
                                   borders=borders,
                                   location_id.list=locs,
                                   mc.cores=1)


#population
pop <- fread(file.path(in.dir,"FILEPATH.csv"))


# Not interested in all columns
years <- c(1990:2020)

years_pop <- c(2000, 2005, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2020)
pop <- pop[, c("longitude","latitude","dust_frac",
               "pop_1990","pop_1995",
               paste0("pop", years_pop,"v4r10")),
           with=F]

#Change names
setnames(pop,c(paste0("pop",years_pop,"v4r10")),c(paste0("pop_",years_pop)))

#merge pop onto pollution
pollution <- merge(pollution,pop,by=c("latitude", "longitude"))


# Use dataset made for air_pm to match location_ids and weights 
# (proportion of area covered) to the pollution file

grids <- read.fst(file.path("FILEPATH",air_pm_version,"grid_map.fst")) %>% as.data.table()

#merge location_id, name, and weight on to pollution dt
pollution <- merge(pollution, grids[,.(location_id,location_name,weight,latitude,longitude)] , by=c("latitude", "longitude"), all.x=T)


#----MISSING COUNTRIES-----------------------------------------------------------------------------------------------------------


pollution <- rbind(pollution, rbindlist(missing.countries.vals))


# Apply weights to pop
pollution[, paste0("pop_", years) := lapply(years,
                                            function(year) get(paste0("pop_", year))*weight)]


pollution$weight<-NULL


#----RESHAPE---------------------------------------------------------------------------------------------------------------------

# Reshape long
pollution <- data.table(pollution) %>% setkey("location_id")
pollution <- melt(pollution, id.vars=c("location_id", "location_name", "longitude", "latitude", "perurban", "dust_frac"))

setkey(pollution, "variable")

pollution[, c("var", "year") := tstrsplit(variable, "_", fixed=TRUE)]
pollution[, variable := NULL]


gc()

#now output the global gridded file as an Rdata for parallelized saving
save(pollution,
     file=file.path(out.dir, "all_grids.Rdata"))


#----SAVE---------------------------------------------------------------------------------------------------------------

# Save by iso3 so that we can run code in parallel
setkeyv(pollution, c("location_id", "longitude", "latitude"))

system("export OMP_NUM_THREADS=1")

global.list <- mclapply(na.omit(unique(pollution$location_id)),
                        castAndSave,
                        global.dt = pollution,
                        mc.cores = cores.provided)
