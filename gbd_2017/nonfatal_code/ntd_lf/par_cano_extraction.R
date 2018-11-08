
# get rid of anything saved in workspace
rm(list=ls())

if (Sys.info()[1] == "Linux"){
  j <- "FILEPATH"
  h <- paste0("FILEPATH")
  package_lib <- paste0(j,'FILEPATH')
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  package_lib <- paste0(j,'FILEPATH')
}
.libPaths(package_lib)

library(maptools) # R package with useful map tools
library(rgeos) # "Geometry Engine- Open Source (GEOS)
library(rgdal) # "Geospatial Data Analysis Library (GDAL)
require(raster)
library(sp)
library(data.table)


# load in maps
envir_suit <- raster(paste0(j, "FILEPATH/lf_32bitv2.tif"))
tot_pop_2010 <- raster(paste0(j, "FILEPATH/worldpop_total_1y_2010_00_00.tif"))
# get gbd shapefile
worldshp = readOGR(paste0(j, 'FILEPATH'), 'GBD2017_analysis_final')
# get rid of the factor class
worldshp$loc_id = as.numeric(levels(worldshp$loc_id))[as.integer(worldshp$loc_id)]
# pull in geographic restrictions
excl_list = data.table(read.csv(paste0(j, "FILEPATH/geographic_exclusions_detailed_only.csv"), stringsAsFactors = F))
# keep if a location is endemic
excl_list = excl_list[year_id == 2017 & include == 1,]
#subset world shp to be only locations with da LF
worldshp = worldshp[worldshp$loc_id %in% excl_list[,location_id],]
# writeOGR(e, "FILEPATH", layer = "lf", driver="ESRI Shapefile", overwrite_layer=TRUE)


# tot_pop_2017 <- raster(paste0(j, "FILEPATH/worldpop_total_1y_2017_00_00.tif"))
# tot_pop_2005 <- raster(paste0(j, "FILEPATH/worldpop_total_1y_2005_00_00.tif"))
# tot_pop_2000 <- raster(paste0(j, "FILEPATH/worldpop_total_1y_2000_00_00.tif"))
# tot_pop_1995 <- raster(paste0(j, "FILEPATH/worldpop_total_1y_1995_00_00.tif"))
# tot_pop_1990 <- raster(paste0(j, "FILEPATH/worldpop_total_1y_1990_00_00.tif"))

# read in TAS shapefile, rasterize, and turn into a binary layer for masking
tas_pass <- readOGR(paste0(j, "FILEPATH"), layer = "e_analysis")
tas_raster <- rasterize(tas_pass, tot_pop_2017)
# need to reverse this -- replace values that are greater than 1 with 0
tas_raster[tas_raster>0 & tas_raster<=100000] <- 1
tas_raster[is.na(tas_raster)] <- 0
new_tas_raster <- projectRaster(tas_raster, envir_suit)
writeRaster(new_tas_raster, paste0(j, "FILEPATH"), format = "GTiff")

# extract over the lF geographies
iter = 1
isos <- unique(worldshp$ihme_lc_id)
rshell <- "FILEPATH/r_shell.sh"
code1 <- "FILEPATH/par_cano_extraction_parallel.R"
for (ihme_loc_id in isos) {
  jobname = paste0("lf_",ihme_loc_id)
  command = paste("qsub -pe multi_slot 6 -l mem_free=12g -P proj_custom_models -o FILEPATH -e FILEPATH -N",jobname, rshell, code1, ihme_loc_id)
  system(command)
  iter = iter +1
}



world_lf <- readOGR(paste0(j, "FILEPATH"), layer = "lf")
world_raster <- rasterize()


# make rasters have same projection
new_tot_pop_2017 <- projectRaster(tot_pop_2017, envir_suit)
new_tot_pop_2010 <- projectRaster(tot_pop_2010, envir_suit)
new_tot_pop_2005 <- projectRaster(tot_pop_2005, envir_suit)
new_tot_pop_2000 <- projectRaster(tot_pop_2000, envir_suit)
new_tot_pop_1995 <- projectRaster(tot_pop_1995, envir_suit)
new_tot_pop_1990 <- projectRaster(tot_pop_1990, envir_suit)

# multiply environmental suitability by population/pixel to get number of people at risk
pop_risk_2017 <- overlay(envir_suit, new_tot_pop_2017, fun=function(x, y){return(x*y)})
pop_risk_2017 <- overlay(pop_risk_2017, new_tas_raster, fun = function(x, y){return(x*y)})
pop_risk_2005 <- overlay(envir_suit, new_tot_pop_2005, fun=function(x, y){return(x*y)})
pop_risk_2000 <- overlay(envir_suit, new_tot_pop_2000, fun=function(x, y){return(x*y)})
pop_risk_1995 <- overlay(envir_suit, new_tot_pop_1995, fun=function(x, y){return(x*y)})
pop_risk_1990 <- overlay(envir_suit, new_tot_pop_1990, fun=function(x, y){return(x*y)})

zonal_PAR<-list()
PAR_2017<-list()
PAR_2017[[1]]<-pop_risk_2017
zonal_PAR[[1]]<-zonal(PAR_2017[[1]],
                      gbd_raster,
                      fun='sum',
                      na.rm=TRUE)


zonal_PAR_clean<-zonal_PAR[[1]]
for (i in 1:nrow(zonal_PAR_clean)){
  if (zonal_PAR_clean[i,2]<1){
    zonal_PAR_clean[i,2]<-0
  }
}

zonal_PAR[[1]]<-zonal_PAR_clean
colnames(zonal_PAR[[1]]) <- c('zone', 'par')


#calculate zonal totals
total_sums<-zonal(tot_pop_2017,
                  gbd_raster,
                  fun = 'sum',
                  na.rm = TRUE)
for (i in 1:nrow(total_sums)){
  if (total_sums[i,2]<=0){
    total_sums[i,2]<-NA
  }
}

# generate the total population to get proportion
prop_par <- cbind(total_sums, zonal_PAR[[1]])
prop_par <- as.data.table(prop_par)

prop_par[, prop:=par/sum]
prop_par <- prop_par[, c("zone", "prop")]

# Extract data from  raster for gbd locations (spatialPolygon) and sum values
sum_risks_2017 <- extract(pop_risk_2017, gbd_locs, df=TRUE)
sum_risks_2017 <- data.table(sum_risks_2017)
sum_risks_2017 <- na.omit(sum_risks_2017) # get rid of NA values
final_sum_risk_2017 <- sum_risks_2017[,.(layer.Sum = sum(layer)),by=ID] # sum of values for each ID (each gbd location)

sum_risks_2005 <- extract(pop_risk_2017, gbd_locs, df=TRUE)
sum_risks_2005 <- data.table(sum_risks_2005)
sum_risks_2005 <- na.omit(sum_risks_2005) # get rid of NA values
final_sum_risk_2005 <- sum_risks_2005[,.(layer.Sum = sum(layer)),by=ID] # sum of values for each ID (each gbd location)

sum_risks_2000 <- extract(pop_risk_2000, gbd_locs, df=TRUE)
sum_risks_2000 <- data.table(sum_risks_2000)
sum_risks_2000 <- na.omit(sum_risks_2000) # get rid of NA values
final_sum_risk_2000 <- sum_risks_2000[,.(layer.Sum = sum(layer)),by=ID] # sum of values for each ID (each gbd location)

sum_risks_1995 <- extract(pop_risk_1995, gbd_locs, df=TRUE)
sum_risks_1995 <- data.table(sum_risks_1995)
sum_risks_1995 <- na.omit(sum_risks_1995) # get rid of NA values
final_sum_risk_1995 <- sum_risks_1995[,.(layer.Sum = sum(layer)),by=ID] # sum of values for each ID (each gbd location)

sum_risks_1990 <- extract(pop_risk_1990, gbd_locs, df=TRUE)
sum_risks_1990 <- data.table(sum_risks_1990)
sum_risks_1990 <- na.omit(sum_risks_1990) # get rid of NA values
final_sum_risk_1990 <- sum_risks_1990[,.(layer.Sum = sum(layer)),by=ID] # sum of values for each ID (each gbd location)

# get total population per country for denom for proportion
tot_pop_country_2010 <- extract(new_tot_pop_2010, gbd_locs, df=TRUE)
tot_pop_country_2010 <- data.table(tot_pop_country_2010)
tot_pop_country_2010 <- na.omit(tot_pop_country_2010) # get rid of NA values
final_tot_pop_country_2010 <- tot_pop_country_2010[,.(layer.Sum = sum(worldpop_total_1y_2010_00_00)),by=ID] # sum of values for each ID (gbd location)

tot_pop_country_2017 <- extract(new_tot_pop_2017, gbd_locs, df=TRUE)
tot_pop_country_2017 <- data.table(tot_pop_country_2017)
tot_pop_country_2017 <- na.omit(tot_pop_country_2017) # get rid of NA values
final_tot_pop_country_2017 <- tot_pop_country_2017[,.(layer.Sum = sum(worldpop_total_1y_2017_00_00)),by=ID] # sum of values for each ID (gbd location)

tot_pop_country_2000 <- extract(new_tot_pop_2000, gbd_locs, df=TRUE)
tot_pop_country_2000 <- data.table(tot_pop_country_2000)
tot_pop_country_2000 <- na.omit(tot_pop_country_2000) # get rid of NA values
final_tot_pop_country_2000 <- tot_pop_country_2000[,.(layer.Sum = sum(worldpop_total_1y_2000_00_00)),by=ID] # sum of values for each ID (gbd location)

tot_pop_country_2005 <- extract(new_tot_pop_2005, gbd_locs, df=TRUE)
tot_pop_country_2005 <- data.table(tot_pop_country_2005)
tot_pop_country_2005 <- na.omit(tot_pop_country_2005) # get rid of NA values
final_tot_pop_country_2005 <- tot_pop_country_2005[,.(layer.Sum = sum(worldpop_total_1y_2005_00_00)),by=ID] # sum of values for each ID (gbd location)

tot_pop_country_1995 <- extract(new_tot_pop_1995, gbd_locs, df=TRUE)
tot_pop_country_1995 <- data.table(tot_pop_country_1995)
tot_pop_country_1995 <- na.omit(tot_pop_country_1995) # get rid of NA values
final_tot_pop_country_1995 <- tot_pop_country_1995[,.(layer.Sum = sum(worldpop_total_1y_1995_00_00)),by=ID] # sum of values for each ID (gbd location)

tot_pop_country_1990 <- extract(new_tot_pop_1990, gbd_locs, df=TRUE)
tot_pop_country_1990 <- data.table(tot_pop_country_1990)
tot_pop_country_1990 <- na.omit(tot_pop_country_1990) # get rid of NA values
final_tot_pop_country_1990 <- tot_pop_country_1990[,.(layer.Sum = sum(worldpop_total_1y_1990_00_00)),by=ID] # sum of values for each ID (gbd location)

# merge total population in each location with sum of peopele at risk per country
table_2017 <- merge(final_sum_risk_2017,final_tot_pop_country_2017, by="ID", all=TRUE)
table_2000 <- merge(final_sum_risk_2000,final_tot_pop_country_2000, by="ID", all=TRUE)
table_2005 <- merge(final_sum_risk_2005,final_tot_pop_country_2005, by="ID", all=TRUE)
table_1995 <- merge(final_sum_risk_1995,final_tot_pop_country_1995, by="ID", all=TRUE)
table_1990 <- merge(final_sum_risk_1990,final_tot_pop_country_1990, by="ID", all=TRUE)

# calculate proportion exposed and create new column "prop_exposed"
table_2017[,prop_exposed := layer.Sum.x/layer.Sum.y, by="ID"]
table_2005[,prop_exposed := layer.Sum.x/layer.Sum.y, by="ID"]
table_2000[,prop_exposed := layer.Sum.x/layer.Sum.y, by="ID"]
table_1995[,prop_exposed := layer.Sum.x/layer.Sum.y, by="ID"]
table_1990[,prop_exposed := layer.Sum.x/layer.Sum.y, by="ID"]


# read csv with FID and ihme_loc_ids
fid <- fread(paste0(j, "FILEPATH/gbd2017_fids.csv"))
fid[, ID := 1:882] # matching IDs so you can merge data.table of prop at risk with attribute table for map
prop_risk_2017 <- merge(table_2017, fid,by="ID", all = TRUE)
prop_risk_2010 <- merge(table_2010, fid,by="ID", all = TRUE)
prop_risk_2005 <- merge(table_2005, fid,by="ID", all = TRUE)
prop_risk_2000 <- merge(table_2000, fid,by="ID", all = TRUE)
prop_risk_1995 <- merge(table_1995, fid,by="ID", all = TRUE)
prop_risk_1990 <- merge(table_1990, fid,by="ID", all = TRUE)


# save results in csv for year specified
fwrite(prop_risk_2017, paste0(j, "FILEPATH/prop_risk_2017_tas.csv"))
fwrite(prop_risk_2010, paste0(j, "FILEPATH/prop_risk_2010.csv"))
fwrite(prop_risk_2005, paste0(j, "FILEPATH/prop_risk_2005.csv"))
fwrite(prop_risk_2000, paste0(j, "FILEPATH/prop_risk_2000.csv"))
fwrite(prop_risk_1995, paste0(j, "FILEPATH/prop_risk_1995.csv"))
fwrite(prop_risk_1990, paste0(j, "FILEPATH/prop_risk_1990.csv"))
