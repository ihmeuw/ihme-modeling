###############################################################
# Date: 3/1/18
# Purpose: Clean REMO and Systematic Review Data for Ethiopia
#          and get proportions to split draws into subnationals
################################################################

##############################################################
# Program Set-up
##############################################################
# load packages
library(data.table)
library(foreign)
library(maptools) # R package with useful map tools
library(rgeos) # "Geometry Engine- Open Source (GEOS)
library(rgdal) # "Geospatial Data Analysis Library (GDAL)
require(raster)
library(sp)

# source central functions
source("FILEPATH/get_best_model_versions.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")

###############################################
# GET PROPORTIONS NOW TO SPLIT NATIONAL CASES
###############################################
# load data
remo <- fread("FILEPATH/KEVIN_ETH_REMO.csv")

# create cleaned REMO dataset
remo_clean <- data.table(location = remo$DIVISION1,
                         cases = remo$TOT_NOD,
                         sample.size = remo$TOT_EXAM)

remo_clean[, mean := cases/sample.size]

# standardize location spellings
remo_clean <- remo_clean[location == "AMHARA", location := "Amhara"]
remo_clean <- remo_clean[location == "OROMIYA", location := "Oromiya"]
remo_clean <- remo_clean[location == "BENISHANGUL", location := "Benishangul"]
remo_clean <- remo_clean[location == "GAMBELLA", location := "Gambella"]

# subset by region
remo_amhara <- remo_clean[location == "Amhara",] # 0.1045
remo_oromiya <- remo_clean[location == "Oromiya",] # 0.187
remo_benishangul <- remo_clean[location == "Benishangul",] # 0.1526
remo_gambella <- remo_clean[location == "Gambella",] # 0.1813
remo_tigray <- remo_clean[location == "Tigray",] # 0.0521
remo_snnpr <- remo_clean[location == "SNNPR",] # 0.1712

# population weighted averages now
amhara_mean <- weighted.mean(remo_amhara$mean, remo_amhara$sample.size)
oromiya_mean <- weighted.mean(remo_oromiya$mean, remo_oromiya$sample.size)
benishangul_mean <- weighted.mean(remo_benishangul$mean, remo_benishangul$sample.size)
gambella_mean <- weighted.mean(remo_gambella$mean, remo_gambella$sample.size)
tigray_mean <- weighted.mean(remo_tigray$mean, remo_tigray$sample.size)
SNNPR_mean <- weighted.mean(remo_snnpr$mean, remo_snnpr$sample.size)

########################################################
# NEED TO GET POPULATION AT RISK NOW IN EACH SUBNATIONAL
########################################################
# need to get population at risk so we can get proportions in case space

# load in maps
eth_endem <- shapefile("FILEPATH/ethiopia_shapefile.shp")
tot_pop_2015 <- raster("FILEPATH/worldpop_total_1y_2015_00_00.tif")

# Extract data from  raster for locations in eth_endem locations (spatialPolygon) and sum values
sum_population <- extract(tot_pop_2015, eth_endem, df=TRUE)
sum_population <- data.table(sum_population)
sum_population <- na.omit(sum_population) # get rid of NA values
final_sum_pop <- sum_population[,.(population_risk = sum(worldpop_total_1y_2015_00_00)),by=ID] # sum of values for each ID (each ETH subnational location)

# IDs are sequential so make data table to merge subnational locations back with sum
id_match <- data.table(location = eth_endem$ADM1_NAME,
                       ID = 1:5)

final_sum_pop <- merge(final_sum_pop, id_match, by = "ID")

############################################################################
# NEED TO MULTIPLY MEAN AND POPULATION AT RISK TO CONVERT TO CASE SPACE
############################################################################

case_amhara <- amhara_mean * final_sum_pop[location == "Amhara",]$population_risk
case_oromiya <- oromiya_mean * final_sum_pop[location == "Oromia",]$population_risk
case_benishangul <- benishangul_mean * final_sum_pop[location == "Beneshangul Gumu",]$population_risk
case_gambella <- gambella_mean * final_sum_pop[location == "Gambela",]$population_risk
case_tigray <- tigray_mean * 0 # according to endemicity map from 2015 article there is no population at risk in Ethiopia
case_SNNPR <- SNNPR_mean * final_sum_pop[location == "SNNPR",]$population_risk

############################################################################
# GET PROPORTIONS
############################################################################
total_cases <- case_amhara + case_benishangul + case_gambella + case_oromiya + case_SNNPR + case_tigray
prop_amhara <- case_amhara/total_cases
prop_oromiya <- case_oromiya/total_cases
prop_benishangul <- case_benishangul/total_cases
prop_gambella <- case_gambella/total_cases
prop_SNNPR <- case_SNNPR/total_cases

#############################################################
# SPLIT DRAWS
# we are going to overwrite the copy of ethiopia national to subnationals
#############################################################
# set-up GBD skeleton
demographics <- get_demographics(gbd_team = "ADDRESS")
gbdages <- demographics$age_group_id # gbd ages
gbdyears <- demographics$year_id # gbd years
gbdsexes <- demographics$sex_id # gbd sexes

# get list of subnational location ids for ETH
locs <- get_location_metadata(location_set_id = 35)
locs <- locs[ihme_loc_id %like% "ETH",]

# get population for ETH subnationals
eth_pop <- get_population(location_id = locs$location_id, sex_id = gbdsexes, age_group_id = gbdages, year_id = gbdyears)
eth_pop <- eth_pop[, run_id := NA]

#get population for ETH national
nat_eth_pop <- get_population(location_id = 179, sex_id = gbdsexes, age_group_id = gbdages, year_id = gbdyears)
nat_eth_pop <- data.table(sex_id = nat_eth_pop$sex_id,
                          age_group_id = nat_eth_pop$age_group_id,
                          year_id = nat_eth_pop$year_id,
                          population = nat_eth_pop$population)

# create data.table with proportions
location <- c("Addis Ababa", "Afar", "Amhara", "Benishangul-Gumuz", "Dire Dawa", "Gambella", "Harari", "Oromia", "Somali", "SNNP", "Tigray")
location_id <- c(44861, 44853, 44854, 44857, 44862, 44860, 44859, 44855, 44856, 44858, 44852)
proportions <- c(0, 0, prop_amhara, prop_benishangul, 0, prop_gambella, 0, prop_oromiya, 0, prop_SNNPR, 0)

proportion_table <- data.table(location_id = location_id,
                               proport = proportions)

locations <- locs[ihme_loc_id %like% "ETH_",]$location_id
meids <- c(1494, 1495, 2620, 2515, 2621, 1496, 1497, 1498, 1499)

for(a in 1:length(location_id)) {
  for(b in 1:length(meids)) {
    loc <- locations[a]
    meid <- meids[b]
    cat(paste0("Writing location id ", loc, " for meid ", meid, " -- ", Sys.time(), "\n"))
    # get draws
    # temp_draws <- get_draws("modelable_entity_id", meid, location_id=loc, year_id=gbdyears, sex_id=gbdsexes, age_group_id=gbdages, source="ADDRESS", status="best")
    temp_draws <- fread(paste0("FILEPATH", meid, "/179.csv"))
    temp_draws <- temp_draws[, location_id := loc]

    # merge subnational populations
    temp_draws <- merge(temp_draws, eth_pop, by = c("sex_id", "age_group_id", "year_id", "location_id"))
    setnames(temp_draws, "population", "subnational_population")

    # merge national populations
    temp_draws <- merge(temp_draws, nat_eth_pop, by = c("sex_id", "age_group_id", "year_id"))
    setnames(temp_draws, "population", "national_population")

    # merge proportions
    temp_draws <- merge(temp_draws, proportion_table, by = "location_id")

    draw_names <- grep("draw", names(temp_draws), value = TRUE)
    test <- temp_draws[, (draw_names) := lapply(.SD, function(x){(x * national_population * proport)/subnational_population}), .SDcols = draw_names]

    # format structure of output
    test <- test[, model_version_id := NULL]
    test <- test[, subnational_population := NULL]
    test <- test[, run_id := NULL]
    test <- test[, national_population := NULL]
    test <- test[, proport := NULL]
    test <- test[, metric_id := NULL]

    #order modelable_entity_id  measure_id location_id year_id age_group_id sex_id  draw*
    other_cols <- c("modelable_entity_id", "measure_id", "location_id", "year_id", "age_group_id", "sex_id")
    setcolorder(test, append(other_cols, draw_names))

    # write_csv from readr package ->

    # Output
    if (file.exists(paste0("FILEPATH", meid, "/", loc, ".csv"))) {
      file.remove(paste0("FILEPATH", meid, "/", loc, ".csv"))
    }
    fwrite(test, paste0("FILEPATH", meid, "/", loc, ".csv"))

  }
}
