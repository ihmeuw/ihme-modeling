#----HEADER----------------------------------------------------------------------------------------------------------------------
# Project: air_pm
# Purpose: create a file that maps air_pm_exp lat long to GBD shapefile
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


#----PREP GRID-------------------------------------------------------------------------------------------------------------------
# Pull in the global shapefile 
shapefile.dir <- file.path(j_root, "FILEPATH")
shapefile.version <- "GBD2017_analysis_final"
borders <- readOGR(shapefile.dir, layer = shapefile.version)
borders$location_id <- borders$loc_id 
if(data.class(borders$location_id)=="factor"){
  borders$location_id <- as.numeric(levels(borders$loc_id))[borders$loc_id] 
}

#isolate border file to most detailed
most_detailed <- locs[most_detailed==1 & is_estimate==1,.(location_id)]
borders <- subset(borders,location_id %in% most_detailed$location_id)

#import one year of datasets to get lat long for mapping
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)
draw.tools.dir <- file.path(home.dir,"FILEPATH")
load(file.path(draw.tools.dir,'grid_dat.RData'))

# Set up grid_map file
grid_map <- as.data.table(grid_dat) #convert to DT
rm(grid_dat) #cleanup
names(grid_map) <- tolower(names(grid_map))

#only need lat and long
grid_map <- grid_map[,.(latitude,longitude)]

#Create an id column
grid_map[, id := .GRP, by = c('latitude', 'longitude')]

# Make a raster of the id column using sp package to create objects of class, "spatial points data frame"
grid_map.sp <- grid_map[, c("latitude", "longitude", "id"), with=F]
coordinates(grid_map.sp) = ~longitude+latitude
proj4string(grid_map.sp) = CRS("+init=epsg:4326") #this references a specific geodetic coding system explained here: https://www.nceas.ucsb.edu/~frazier/RSpatialGuides/OverviewCoordinateReferenceSystems.pdf
gridded(grid_map.sp) = TRUE #promotes SpatialPointsDataFrame to SpatialGridDataFrame

grid_map.sp <- raster(grid_map.sp[, c("id")]) #changes SpatialGridDataFrame to raster)
#********************************************************************************************************************************

#----EXTRACT---------------------------------------------------------------------------------------------------------------------
# Use raster's extract to get a list of the raster ids that are in a given country
# The output is a list with one item per country in the borders file, a matrix of cell ids and the approximate fraction of that cell covered by the polygon.
# This is used for splitting population between grids ocvering multiple locations and really tiny islands
beginCluster(n=max.cores) #extract function can multicore, this initializes cluster (must specify n or it will take all cores)
raster.ids <- extract(grid_map.sp, borders, small=TRUE, weights=TRUE, normalizeWeights=FALSE)
endCluster()

# Convert to dataframe with two columns
temp <- NULL
for (iii in 1:length(raster.ids)) {
  if (!is.null(raster.ids[[iii]])) {

    
    temp <- rbind(temp, data.frame(location_id=borders$location_id[iii],
                                   location_name=borders$loc_nm_sh[iii],
                                   id=raster.ids[[iii]][,1],
                                   weight=raster.ids[[iii]][,2]))
  }
}

#Some are missing ids
temp <- temp[!is.na(temp$id), ]

#indicator for number of countries each grid is in
temp$num_countries <- ave(temp$id, temp$id, FUN=length)

# Merge back on
grid_map <- merge(temp, grid_map, by="id")
grid_map <- data.table(grid_map)

#fix for borders with water and not other countries
grid_map[num_countries==1,weight:=1]


grid_map$id <- NULL
#********************************************************************************************************************************

#----Save---------------------------------------------------------------------------------------------------------------------

# save file that matches lat long to location_id, location_name, and weight corresponding to the proportion of a gridcell covered 
# by a border
grid_map <- grid_map[,.(location_id,location_name,weight,longitude,latitude)]
write.fst(grid_map, path=assign.output)

#********************************************************************************************************************************