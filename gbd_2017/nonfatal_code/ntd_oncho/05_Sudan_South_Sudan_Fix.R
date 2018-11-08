###############################################################
# Date: 3/8/18
# Purpose: Split Sudan draws into Sudan/South Sudan
################################################################

###############################################################
# Program Set-up
###############################################################
# load packages
library(data.table)
library(foreign)
library(maptools) # R package with useful map tools
library(rgeos) # "Geometry Engine- Open Source (GEOS)
library(rgdal) # "Geospatial Data Analysis Library (GDAL)
require(raster)
library(sp)
library(data.table)

# source central functions
source("FILEPATH/get_best_model_versions.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_location_metadata.R")

############################################################################
# Get Population at Risk Estimates for Sudan Pre Abu Hamed Focus Elimination
############################################################################

# load in maps
sudan_endem_pre_abu <- shapefile("FILEPATH/Sudan_Pre_Abu_Hamed_Elimination.shp")
years <- c(1990, 1995, 2000, 2005, 2010)
sudan_par_pre_elim <- data.table()

for (a in 1:length(years)) {
  year <- years[a]
  tot_pop <- raster(paste0("FILEPATH/worldpop_total_1y_", year,"_00_00.tif"))

  # Extract data from  raster for locations in eth_endem locations (spatialPolygon) and sum values
  sum_population <- extract(tot_pop, sudan_endem_pre_abu, df=TRUE)
  sum_population <- data.table(sum_population)
  sum_population <- na.omit(sum_population) # get rid of NA values
  final_sum_pop <- sum_population[,.(population_risk = sum(eval(as.name((paste0("worldpop_total_1y_", year, "_00_00")))))),by=ID] # sum of values for each ID (each subnational location)

  final_sum_pop <- data.table(location = "Sudan",
                              year_id = year,
                              population_risk = final_sum_pop$population_risk)

  sudan_par_pre_elim <- dplyr::bind_rows(sudan_par_pre_elim, final_sum_pop)

}


#############################################################################
# Get Population at Risk Estimates for Sudan Post Abu Hamed Focus Elimination
#############################################################################

# load in maps
sudan_endem_post_abu <- shapefile("FILEPATH/Final_Sudan_Post_Abu_Hamed_Elimination.shp")
tot_pop <- raster("FILEPATH/worldpop_total_1y_2017_00_00.tif")

# Extract data from  raster for locations in sudan_endem_post_abu locations (spatialPolygon) and sum values
sum_population <- extract(tot_pop, sudan_endem_post_abu, df=TRUE)
sum_population <- data.table(sum_population)
sum_population <- na.omit(sum_population) # get rid of NA values
sudan_par_post_elim <- sum_population[,.(population_risk = sum(worldpop_total_1y_2017_00_00)),by=ID] # sum of values for each ID (each location)

sudan_par_post_elim <- data.table(location = "Sudan",
                            year_id = 2017,
                            population_risk = sudan_par_post_elim$population_risk)

#############################################################################
# Get Population at Risk Estimates for South Sudan
#############################################################################
# load in maps
south_sudan_endem <- shapefile("FILEPATH/South_Sudan_Foci.shp")
years <- c(1990, 1995, 2000, 2005, 2010, 2017)
south_sudan_par <- data.table()

for (a in 1:length(years)) {
  year <- years[a]
  tot_pop <- raster(paste0("FILEPATH/worldpop_total_1y_", year,"_00_00.tif"))

  # Extract data from  raster for locations in eth_endem locations (spatialPolygon) and sum values
  sum_population <- extract(tot_pop, south_sudan_endem, df=TRUE)
  sum_population <- data.table(sum_population)
  sum_population <- na.omit(sum_population) # get rid of NA values
  final_sum_pop <- sum_population[,.(population_risk = sum(eval(as.name((paste0("worldpop_total_1y_", year, "_00_00")))))),by=ID] # sum of values for each ID (each subnational location)

  final_sum_pop <- data.table(location = "South Sudan",
                              year_id = year,
                              population_risk = final_sum_pop$population_risk)

  south_sudan_par <- dplyr::bind_rows(south_sudan_par, final_sum_pop)

}

# bind rows for all data sets now
par <- dplyr::bind_rows(sudan_par_pre_elim, sudan_par_post_elim)
par <- dplyr::bind_rows(par, south_sudan_par)

####################################################
# Now Calculate proportions and format
####################################################
par <- par[, total := sum(population_risk), by = "year_id"]
par <- par[, prop := population_risk/total]
par[, population_risk := NULL]
par[, total := NULL]

par[location == "Sudan", location_id := 522]
par[location == "South Sudan", location_id := 435]

##########################################################################
# Now split draws
# we are going to overwrite the copy of ethiopia national to subnationals
##########################################################################
# set-up GBD skeleton
demographics <- get_demographics(gbd_team = "ADDRESS")
gbdages <- demographics$age_group_id # gbd ages
gbdyears <- demographics$year_id # gbd years
gbdsexes <- demographics$sex_id # gbd sexes

# get population for Sudan and South Sudan
pop <- get_population(location_id = c(522, 435), sex_id = gbdsexes, age_group_id = gbdages, year_id = gbdyears)
pop <- pop[, run_id := NA]


locations <- c(522, 435)
meids <- c(1494, 1495, 2620, 2515, 2621, 1496, 1497, 1498, 1499)



for(a in 1:length(locations)) {
  for(b in 1:length(meids)) {
    print(a)
    print(b)

    loc <- locations[a]
    meid <- meids[b]

    # get draws
    temp_draws <- fread(paste0("FILEPATH", meid, "/435.csv"))
    # temp_draws <- get_draws("modelable_entity_id", meid, location_id=435, year_id=gbdyears, sex_id=gbdsexes, age_group_id=gbdages, source="ADDRESS", status="best")

    if (loc == 522) {
      temp_draws[, location_id := 522]
    }

    # merge proportions
    temp_draws <- merge(temp_draws, par, by = c("location_id", "year_id"))

    # merge national population
    temp_draws <- merge(temp_draws, pop, by = c("sex_id", "age_group_id", "year_id", "location_id"))
    setnames(temp_draws, "population", "national_population")


    # multiply by population of both South Sudan and Sudan
    temp_draws <- merge(temp_draws, pop[location_id == 435,], by = c("sex_id", "age_group_id", "year_id"))
    setnames(temp_draws, "location_id.x", "location_id")
    temp_draws[, location_id.y := NULL]


    draw_names <- grep("draw", names(temp_draws), value = TRUE)
    test <- temp_draws[, (draw_names) := lapply(.SD, function(x){(x * population * prop)/national_population}), .SDcols = draw_names]

    # format structure of output
    test <- test[, model_version_id := NULL]
    test <- test[, population := NULL]
    test <- test[, national_population := NULL]
    test <- test[, run_id.y := NULL]

    test <- test[, run_id.x := NULL]
    test <- test[, prop := NULL]
    test <- test[, metric_id := NULL]
    test <- test[, location := NULL]


    #order modelable_entity_id  measure_id location_id year_id age_group_id sex_id  draw*
    other_cols <- c("modelable_entity_id", "measure_id", "location_id", "year_id", "age_group_id", "sex_id")
    setcolorder(test, append(other_cols, draw_names))

    # Output
    if (file.exists(paste0("FILEPATH", meid, "/", loc, ".csv"))) {
      file.remove(paste0("FILEPATH", meid, "/", loc, ".csv"))
    }
      fwrite(test, paste0("FILEPATH", meid, "/", loc, ".csv"))

  }
}





# source("FILEPATH/save_results_epi.R")
# meids <- c(1494, 1495, 2620, 2515, 2621, 1496, 1497, 1498, 1499)
#
# for (c in 1:length(meids)) {
#   meid <- meids[c]
#   save_results_epi(paste0("FILEPATH", meid, "/"), "{location_id}.csv", modelable_entity_id = meid, description = "gbd2015 methods - eg draws extrapolation, added Ethiopia and Sudan/South Sudan fix", measure_id = 5)
# }
