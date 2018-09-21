#----HEADER----------------------------------------------------------------------------------------------------------------------

# Project: RF: air_pm/air_ozone
# Purpose: Take the global gridded shapefile and cut it up into different countries/subnationals using shapefiles
# This is an update of FILEPATH

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
  cores.provided <- 30 # on big jobs i like to ask for 80 slots, rule of thumb is 2 slots per core
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  cores.provided <- parallel::detectCores() - 1 # on my machine this will return 3 cores (quadcore)
  
}

# load packages, install if missing
pacman::p_load(data.table, ggplot2, parallel, magrittr, maptools, raster, reshape2, rgdal, rgeos, sp, splines)

# set options
prediction.method <- "spline" #suggested by NAME
prediction.method <- "aroc" 
location_set_version_id <- 149

# Versioning
version <- 1 # split ozone exposure into its own category, this is first run using AROC method to extrap
version <- 2 # rerunning with single core as it keeps hanging on the mclapply, this should match v #1
version <- 3 #GBD2016 first version, extrapolation continues
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(FILEPATH)
ubcov.function.dir <- file.path(FILEPATH)
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db (personal version)
file.path(central.function.dir, "db_tools.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "utilitybelt/db_tools.r") %>% source
# central functions
file.path(FILEPATH) %>% source
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

# this bash script will append all csvs to create a global file, then create national files for each subnational country
aggregateResults <- paste0("bash ", file.path(FILEPATH))

# this function is used to estimate pollution for islands or small countries that dont have large enough borders to pick up a grid
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
    temp <- pollution[which(pollution$x <= padded.max.x
                            & pollution$x >= padded.min.x
                            & pollution$y <= padded.max.y
                            & pollution$y >= padded.min.y), ]
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
  temp <- aggregate(temp, by=list(temp$one), FUN=mean)
  temp$Group.1 <- temp$one <- NULL
  
  # Prep to be added on to the dataset
  temp$location_id <- country
  temp$x <- round(mean(max.x, min.x) * 20) / 20 # All real grids are at .05 units of latitude/longitude.
  temp$y <- round(mean(max.y, min.y) * 20) / 20
  
  return(temp)
  
}

# this function is used to forecast pollution (since we only have it up to 2011 at this time)
# we fit splines to the data from 1990-2011 and then use them to predict 2012-2015

splinePred <- function(dt,
                       this.grid,
                       pred.vars,
                       start.year,
                       end.year) {
  
  #cat("~",this.grid); flush.console() #toggle for troubleshooting/monitoring loop status
  
  pred.length <- (end.year - start.year) + 1
  
  pred.dt <- dt[grid==this.grid & year %in% c(1990, 2000, 2010)] # these are the only values that haven't already been predicted using splines (IE real data)
  
  forecast <- dt[grid==this.grid, -c("year", pred.vars), with=F]
  forecast[1:pred.length, "year" := start.year:end.year]
  
  forecast[1:pred.length,
           c(pred.vars) := lapply(pred.vars,
                                  function(var)
                                    ifelse(rep(any(is.na(pred.dt[, var, with=F])), #test if any obv is NA, spline needs 4+ obvs to fit
                                               pred.length), # note i had to add the rep*pred.length because ifelse returns things in shape of test
                                           NA, #if so, return NA for the pred
                                           predict(lm(get(var) ~ ns(year), data=pred.dt),
                                                   newdata=data.frame(year=year)))),
           with=F]
  
  return(na.omit(forecast))
  
}

arocPred <- function(dt,
                     this.grid,
                     pred.vars,
                     start.year,
                     end.year) {
  
  #cat("~",this.grid); flush.console() #toggle for troubleshooting/monitoring loop status
  
  pred.length <- (end.year - start.year) + 1
  pred.dt <- dt[grid==this.grid]
  
  forecast <- dt[grid==this.grid, -c("year", pred.vars), with=F]
  forecast[1:pred.length, "year" := start.year:end.year]
  
  predictVar <- function(var, year) {
    
    #calculate the annualized rate of change from 2010 to 2011
    rate.of.change <- -log(pred.dt[year==2011, get(var)]/pred.dt[year==2010, get(var)])/(2011-2010)
    pred.dt[year==2010, get(var)] * exp(-rate.of.change * (year-2010))
    
  }
  
  forecast[1:pred.length,
           c(pred.vars) := lapply(pred.vars,
                                  predictVar,
                                  year = year),
           with=F]
  
  return(na.omit(forecast))
  
}

# this is a wrapper for splinePred that subsets to country, reshapes wide, runs splinePred, and then appends the forecasts and saves a csv
castAndSave <- function(global.dt,
                        country) {
  
  cat(country, "\n"); flush.console()
  
  #subset to country
  
  if(country != "EU") {
    temp <- global.dt[location_id==country, ]
  } else if (country == "EU") { #select the European Union iso3s
    temp <- global.dt[ihme_loc_id %in% c('AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU',
                                         'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT',
                                         "ROU", 'SVK', 'SVN', 'ESP', 'SWE', 'GBR')]
  }
  
  #reshape the dt wide
  temp <- dcast(temp,
                location_id + ihme_loc_id + x + y + perurban + year ~ var, #formula to cast over
                value.var="value") %>% as.data.table() 
  
  # create a grid ID variable, splines can only be done on a single grid at a time
  temp[, grid := as.numeric(as.factor(paste0(x,y)))]
  
  # ensure that year is a numeric, after cast it is becoming character and it needs to be a number to be used in the extrapolation formula
  temp[, year := as.numeric(year)]
  
  #wrapper function to supply which prediction function we want to use based on an argument at the top
  choosePred <- function(type) {
    
    switch(type,
           spline = splinePred,
           aroc = arocPred)
    
  }
  
  #generate forecasts using your spline prediction function
  forecasts <- lapply(unique(temp$grid),
                      choosePred(prediction.method),
                      dt = temp,
                      pred.vars = c("fus", "pop", "o3"),
                      start.year = 2012,
                      end.year = 2016)
  
  #add the forecasts to the dt
  temp <- rbind(rbindlist(forecasts), #first use rbindlist to turn the output of lapply into a dt
                temp) 
  
  #write the country CSV so we can run in parallel at later stages
  write.csv(temp,
            file.path(out.dir, paste0(country, ".csv")),
            row.names=F)
  
  #also return each country to a list in case we want to look at them interactively or do testing
  return(temp)
  
}

#********************************************************************************************************************************

#----IN/OUT----------------------------------------------------------------------------------------------------------------------
# Set directories and load files
###Input###
# Get the list of most detailed GBD locations
locs <- get_location_hierarchy(location_set_version_id)
locations <- locs[is_estimate==1, unique(location_id)]
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]

# Pull in the global shapefile (this was prepped by NAME - new mapping specialist)
shapefile.dir <- file.path(FILEPATH)
shapefile.version <- "GBD2016_analysis_final"
borders <- readOGR(shapefile.dir, layer = shapefile.version)
borders$location_id <- borders$loc_id # this variable is misnamed in the current official global shape, create it here

# directory holding the global gridded dataset we wish to split
gridded.dataset.dir <- file.path(FILEPATH)
gridded.dataset <- file.path(FILEPATH) 

###Output###
# where to output the split gridded files
out.dir <-  file.path(FILEPATH)
dir.create(paste0(out.dir), recursive=T, showWarnings=F)

#********************************************************************************************************************************

#----PREP GRID-------------------------------------------------------------------------------------------------------------------
# Set up pollution file
pollution <- fread(gridded.dataset, stringsAsFactors=FALSE)
names(pollution) <- tolower(names(pollution))
# Not interested in all columns
years <- c(1990, 1995, 2000, 2005, 2010, 2011)
pollution <- pollution[, c("x",
                           "y",
                           "perurban",
                           "dust_frac",
                           paste0("pop_", years),
                           paste0("fus_", years),
                           paste0("o3_", years)),
                       with=F]

#Create an id column
pollution[, id := 1:length(x)]

# Make a raster of the id column
pollution.sp <- pollution[, c("x", "y", "id"), with=F]
coordinates(pollution.sp) = ~x+y
proj4string(pollution.sp)=CRS("+init=epsg:4326")
gridded(pollution.sp) = TRUE

pollution.sp <- raster(pollution.sp[, c("id")])

#********************************************************************************************************************************

#----EXTRACT---------------------------------------------------------------------------------------------------------------------
# Use raster's extract to get a list of the raster ids that are in a given country
# The output is a list with one item per country in the borders file
beginCluster(n=cores.provided) #extract function can multicore, this initializes cluster (must specify n or it will take all cores)
raster.ids <- extract(pollution.sp, borders)
endCluster()

# Convert to dataframe with two columns
temp <- NULL
for (iii in 1:length(raster.ids)) {
  if (!is.null(raster.ids[[iii]])) {
    temp <- rbind(temp, data.frame(location_id=borders$location_id[iii], id=raster.ids[[iii]]))
  }
}

#Some are missing ids
temp <- temp[!is.na(temp$id), ]
# Some ids are in multiple countries. Create an indicator
temp$num_countries <- ave(temp$id, temp$id, FUN=length)
# Merge back on
pollution <- merge(temp, pollution, by="id")

# Reduce population by 1/(number of countries grid is in). 
pollution <- data.table(pollution)
pollution[, paste0("pop_", years) := lapply(years,
                                            function(year) get(paste0("pop_", year)) / num_countries),
          with=F]

#********************************************************************************************************************************

#----MISSING COUNTRIES-----------------------------------------------------------------------------------------------------------
# Some countries are too small of islands to pick up any grids. To correct for this,
# we will take the average values for the rectangular area around islands + 1 (or more if necessary) degrees in any direction
# Find out which ones
missing.countries <- unique(borders$location_id)[!(unique(borders$location_id) %in% unique(pollution$location_id))]

missing.countries.vals <- mclapply(missing.countries,
                                   estimateIslands,
                                   borders=borders,
                                   location_id.list=locs,
                                   mc.cores=1)

pollution <- rbind(pollution, rbindlist(missing.countries.vals))
pollution <- merge(pollution,
                   locs[, c("ihme_loc_id", "location_id"), with=F],
                   by="location_id",
                   all.x=T)

pollution$num_countries <- pollution$id <- NULL

#********************************************************************************************************************************

#----RESHAPE---------------------------------------------------------------------------------------------------------------------

# Reshape long
pollution <- data.table(pollution) %>% setkey("location_id")
pollution <- melt(pollution, id.vars=c("location_id", "ihme_loc_id", "x", "y", "perurban", "dust_frac"))

setkey(pollution, "variable")

pollution[, c("var", "year") := tstrsplit(variable, "_", fixed=TRUE)]
pollution[, variable := NULL]
gc()

#now output the global gridded file as an Rdata for parallelized saving
save(pollution,
     file=file.path(out.dir, "all_grids.Rdata"))

#********************************************************************************************************************************

#----FORECAST/SAVE---------------------------------------------------------------------------------------------------------------
# Workspace - need to find a new way to extrapolate to 2013/2015 (AROC overestimates in many cases)
# NAME's suggestion was to use a spline
# Save by iso3 so that we can run code in parallel
setkeyv(pollution, c("location_id", "x", "y"))

system("export OMP_NUM_THREADS=1") 

global.list <- mclapply(na.omit(unique(pollution$location_id)), 
                        castAndSave,
                        global.dt = pollution,
                        mc.cores = cores.provided)


system(aggregateResults) #run a bash script that will append all the results to create a global csv and also national csvs for each subnational

#********************************************************************************************************************************

#----SCRAP-----------------------------------------------------------------------------------------------------------------------