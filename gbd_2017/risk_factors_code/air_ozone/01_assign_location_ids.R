#----HEADER----------------------------------------------------------------------------------------------------------------------
# Project: RF: air_pm/air_ozone
# Purpose: Take the global gridded shapefile and cut it up into different countries/subnationals using shapefiles
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
  cores.provided <- 50 

} else {

  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  cores.provided <- parallel::detectCores() - 1 # on my machine this will return 3 cores (quadcore)

}

# load packages, install if missing
pacman::p_load(data.table, ggplot2, parallel, magrittr, maptools, raster, reshape2, rgdal, rgeos, sp, splines,ini)

# set options
prediction.method <- "spline"
prediction.method <- "aroc" #spline performed poorly, switching back to AROC
location_set_version_id <- 367
prep.grid <- T
air_pm_version <- 32  #version to use for mapping gridcells to shapefile

# Versioning
version <- 1 # split ozone exposure into its own category, this is first run using AROC method to extrap
version <- 2 # rerunning with single core as it keeps hanging on the mclapply, this should match v #1
version <- 3 #GBD2016 first version, extrapolation continues
version <- 4 #first run GBD 2017 with GBD 2016 data
version <- 5 #v4 was mysteriously missing some countries
version <- 6 #New GBD2017 exposure dataset
version <- 7 #caught model bug and reduces emphasis on measurement in Kenya
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- "FILEPATH"
ubcov.function.dir <- "FILEPATH"
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db (personal version)
file.path(central.function.dir, "db_tools.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH/db_tools.r") %>% source
# central functions
file.path("FILEPATH/get_population.R") %>% source
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

# this bash script will append all csvs to create a global file, then create national files for each subnational country
aggregateResults <- paste0("bash ", file.path("FILEPATH/01b_aggregate_results.sh")) 

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

  # must convert location IDs from factor in order to collapse them
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


# this is a wrapper that subsets to country, reshapes wide,  and saves a csv
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

  # create a grid ID variable, splines can only be done on a single grid at a time
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

#********************************************************************************************************************************

#----IN/OUT----------------------------------------------------------------------------------------------------------------------
# Set directories and load files
###Input###
# Get the list of most detailed GBD locations
locs <- get_location_hierarchy(location_set_version_id)
locations <- locs[is_estimate==1, unique(location_id)]
levels <- locs[,grep("level|location_id|location_name", names(locs)), with=F]

###Output###
# where to output the split gridded files
out.dir <-  file.path("FILEPATH", version)
dir.create(paste0(out.dir), recursive=T, showWarnings=F)

#********************************************************************************************************************************

#----PREP GRID-------------------------------------------------------------------------------------------------------------------
if(prep.grid==TRUE){
  
  # Pull in the global shapefile
  shapefile.dir <- "FILEPATH"
  shapefile.version <- "GBD2017_analysis_final"
  borders <- readOGR(shapefile.dir, layer = shapefile.version)
  borders$location_id <- borders$loc_id

  # directory holding the global gridded dataset we wish to split
  gridded.dataset.dir <- "FILEPATH"
  gridded.dataset <- "FILEPATH" # GBD2017 new exp
  
  # directory holding the pop dataset
  gridded.pop.dir <- "FILEPATH"
  gridded.pop <- "FILEPATH" # GBD2017 new pop
  

# Set up pollution file
load(gridded.dataset)
pollution<-as.data.table(ozGBD2017_06172018)
pollution<-na.omit(pollution)
# bring in pop file
load(gridded.pop)
pop <- as.data.table(data)

#clean up workspace
rm(ozGBD2017_06172018, data)


# Not interested in all columns
years <- c(1990, 1995, 2000, 2005, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
pollution <- pollution[, c("x",
                           "y",
                           "perurban",
                           paste0("DMA8_", years),
                           paste0("DMA8_", years,"_LCL"),
                           paste0("DMA8_", years,"_UCL")),
                       with=F]
#change names
setnames(pollution,c("x","y",
                     paste0("DMA8_", years),
                     paste0("DMA8_", years,"_LCL"),
                     paste0("DMA8_", years,"_UCL")),
                  c("longitude", "latitude",
                    paste0("o3_", years),
                    paste0("o3low_", years),
                    paste0("o3up_", years)))


years_pop <- c(2000, 2005, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
pop <- pop[, c("longitude","latitude","dust_frac",
                "pop_1990","pop_1995",
                           paste0("pop", years_pop,"v4r10")),
                           with=F]
#Change names
setnames(pop,c(paste0("pop",years_pop,"v4r10")),c(paste0("pop_",years_pop)))



#merge pop onto pollution
pollution <- merge(pollution,pop,by=c("latitude", "longitude"))




#********************************************************************************************************************************
# instead of re-extracting everything we will use our dataset we just made for air_pm to match location_ids and weights 
# (proportion of area covered) to the pollution file

grids <- read.fst("FILEPATH/grid_map.fst") %>% as.data.table()

pollution <- merge(pollution, grids[,.(location_id,location_name,weight,latitude,longitude)] , by=c("latitude", "longitude"), all.x=T)


#********************************************************************************************************************************

#----MISSING COUNTRIES-----------------------------------------------------------------------------------------------------------
# Some countries are too small of islands to pick up any grids. To correct for this,
# we will take the average values for the rectangular area around islands + 1 (or more if necessary) degrees in any direction
# Find out which ones
missing.countries <- unique(grids$location_id)[!(unique(grids$location_id) %in% unique(pollution$location_id))]

missing.countries.vals <- mclapply(missing.countries,
                                    estimateIslands,
                                   borders=borders,
                                   location_id.list=locs,
                                   mc.cores=1)

pollution <- rbind(pollution, rbindlist(missing.countries.vals))


#Apply weights to pop
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
    file=file.path("FILEPATH", "all_grids.Rdata"))
} else{
  load(file.path("FILEPATH", "all_grids.Rdata"))
}

#********************************************************************************************************************************

#----SAVE---------------------------------------------------------------------------------------------------------------

# Save by iso3 so that we can run code in parallel
setkeyv(pollution, c("location_id", "longitude", "latitude"))


system("export OMP_NUM_THREADS=1") 

global.list <- mclapply(na.omit(unique(pollution$location_id)), 
                        castAndSave,
                        global.dt = pollution,
                        mc.cores = cores.provided)


system(aggregateResults) #run a bash script that will append all the results to create a global csv and also national csvs for each subnational

#********************************************************************************************************************************
