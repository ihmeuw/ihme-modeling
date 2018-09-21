#----HEADER----------------------------------------------------------------------------------------------------------------------
# Project: RF -> air_pm
# Purpose: Moved the assign prep section to a new file in order to make easier control flow in master
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}
#********************************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#Air EXP functions#
exp.function.dir <- file.path(h_root, 'FILEPATH')
file.path(exp.function.dir, "assign_tools.R") %>% source

#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
#********************************************************************************************************************************

#----PREP GRID-------------------------------------------------------------------------------------------------------------------
# Pull in the global shapefile
shapefile.dir <- file.path(j_root, "FILEPATH")
shapefile.version <- "GBD2016_analysis_final"
borders <- readOGR(shapefile.dir, layer = shapefile.version)
borders$location_id <- borders$loc_id # rename variable

# import object "newdata" 
# also import population
getwd() %>% file.path("FILEPATH") %>% load(envir = globalenv(), verbose=TRUE)
getwd() %>% file.path("FILEPATH") %>% load(envir = globalenv(), verbose=TRUE)

# Set up pollution file
pollution <- as.data.table(GBD2016_PRED) #convert to DT
rm(GBD2016_PRED) #cleanup
names(pollution) <- tolower(names(pollution))

# set up pop file
popdata <- popdata_rural_urban[, c('idgridcell.x', 'urban', 'perurban', names(popdata_rural_urban)[names(popdata_rural_urban) %like% 'pop'])]
setnames(popdata, 'idgridcell.x', 'idgridcell')

#merge the pop file to the pollution file
pollution <- merge(pollution, popdata, by='idgridcell')

# Not interested in all columns
legacy.variables <- c('id', 'idgridcell', 'iso3', 'pop2020v4', 'pop2019v4', 'pop2018v4', 'pop2017v4',
                      'dust', 'dust_frac', 'region', 'idgridcell.x', 'country.x', 'country.y')
pollution <- pollution[, -legacy.variables, with=F]
setnames(pollution,
         c('pop_1990v3', 'pop_1995v3', 'pop2000v4', 'pop2005v4', 'pop2010v4', 'pop2011v4', 'pop2012v4', 'pop2013v4', 'pop2014v4', 'pop2015v4', 'pop2016v4'),
         c('pop_1990', 'pop_1995', 'pop_2000', 'pop_2005', 'pop_2010', 'pop_2011', 'pop_2012', 'pop_2013', 'pop_2014', 'pop_2015', 'pop_2016'))

#define years of interest
years <- c(1990, 1995, 2000, 2005, 2010, 2011, 2012, 2013, 2014, 2015, 2016)

#Create an id column
pollution[, id := .GRP, by = c('latitude', 'longitude')]

# Make a raster of the id column
pollution.sp <- pollution[, c("latitude", "longitude", "id"), with=F]
coordinates(pollution.sp) = ~longitude+latitude
proj4string(pollution.sp)=CRS("+init=epsg:4326")
gridded(pollution.sp) = TRUE

pollution.sp <- raster(pollution.sp[, c("id")])
#********************************************************************************************************************************

#----EXTRACT---------------------------------------------------------------------------------------------------------------------
# Use raster's extract to get a list of the raster ids that are in a given country
# The output is a list with one item per country in the borders file
beginCluster(n=max.cores) #extract function can multicore, this initializes cluster (must specify n or it will take all cores)
raster.ids <- extract(pollution.sp, borders)
endCluster()

# Convert to dataframe with two columns
temp <- NULL
for (iii in 1:length(raster.ids)) {
  if (!is.null(raster.ids[[iii]])) {
    
    message("binding locs in border #", iii)
    
    temp <- rbind(temp, data.frame(location_id=borders$location_id[iii],
                                   location_name=borders$loc_nm_sh[iii],
                                   id=raster.ids[[iii]]))
  }
}

#Some are missing ids
temp <- temp[!is.na(temp$id), ]
# Some ids are in multiple countries. Create an indicator
temp$num_countries <- ave(temp$id, temp$id, FUN=length)
# Merge back on
pollution <- merge(temp, pollution, by="id")


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
                                   mc.cores=max.cores)

pollution <- rbind(pollution, rbindlist(missing.countries.vals))
pollution <- merge(pollution,
                   locs[, c("ihme_loc_id", "location_id"), with=F],
                   by="location_id",
                   all.x=T)

pollution$num_countries <- pollution$id <- NULL
#********************************************************************************************************************************

#----RESHAPE---------------------------------------------------------------------------------------------------------------------
#drop the urban datapoints 
if (grid.version==22) {
  
  pollution <- pollution[urban < .5]
  
}

#no longer need the urban/perurban vars
pollution <- pollution[, -c('urban', 'perurban')]

# Reshape long
setkey(pollution, "location_id")
pollution <- melt(pollution, id.vars=c("location_id", "location_name", "ihme_loc_id", "country",
                                       "longitude", "latitude"))

#setkey for speed
setkey(pollution, "variable")

#cleanup the variable
pollution[, variable := str_replace_all(variable, "_pm2.5_", "_")]
pollution[, variable := str_replace_all(variable, "_logpm2.5_", "log_")]

#split out
pollution[, c("var", "year") := tstrsplit(variable, "_", fixed=TRUE)]
pollution[, variable := NULL]

#further cleanup
pollution[, var := str_replace_all(var, "stddevlog", "log_sd")]
pollution[, var := str_replace_all(var, "meanlog", "log_mean")]
setnames(pollution, c('longitude', 'latitude'), c('long', 'lat'))

#reshape the different variables out wide (mean/ci and pop)
pollution	<- dcast(pollution, location_id + location_name + ihme_loc_id + long + lat + year ~ var)	%>% as.data.table 

#save the reshaped all grid file
write.csv(pollution, file.path(out.dir, "all_grids.csv"))

#now output the global gridded file as an Rdata for parallelized saving
write.fst(pollution,path=file.path(exp.dir, "all_grids.fst"))

save(pollution,file=file.path(exp.dir, "all_grids.Rdata"))
#********************************************************************************************************************************