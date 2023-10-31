
rm(list=ls())

arg <- commandArgs(trailingOnly = T)  #[-(1:5)]  # First args are for unix use only
y <- arg[1]


library("tidyverse")
library("data.table")
library("zoo")
library("feather")

# set up directories
if (Sys.info()["sysname"]=="Windows") {
  h_drive <- "H:"
  j_drive <- "J:"
  k_drive <- "K:"
} else {
  h_drive <- "~"
  j_drive <- "/FILEPATH/"
  k_drive <- "/FILEPATH"
}



source(paste0(k_drive, "/FILEPATH/get_location_metadata.R"))
#source(paste0(k_drive, "/FILEPATH/get_population.R"))

### GET LOCATION METADATA ###
locMeta <- get_location_metadata(location_set_id = 35, release_id = 9)

#tempZone <- as.integer(10)

rrdir <- paste0(j_drive, "/FILEPATH/")
acause <- "cvd_ihd"
rr <- fread(paste0(rrdir, "/", acause, "/", acause, "_curve_samples.csv"))[, paste0("draw_", 0:999) := NULL]
setnames(rr, "annual_temperature", "meanTempCat")

rr[, dailyTempCat := as.integer(round(daily_temperature*10))][, daily_temperature := NULL]

tempLimits <- unique(rr[, c("meanTempCat", "dailyTempCat")])
tempLimits <- unique(tempLimits[, `:=` (minTemp = min(dailyTempCat), maxTemp = max(dailyTempCat)), by = c("meanTempCat")][, dailyTempCat := NULL])

rm(rr)




pull.temps <- function(year, location_id) {
  cat(paste0(location_id, " "))
  
  inYear <- year
  inLoc <- location_id
  
  
  #if (location_id==101) inLoc <- 101101
  #if (file.exists(paste0(j_drive, "/FILEPATH/", inYear, "/pop_", inYear, "_", location_id, ".feather"))) {
    
    temp <- as.data.table(read_feather(paste0("/FILEPATH/", year, "/era5_", year, "_", inLoc, ".feather")))
    
    # if (year==2020) {
    #  inYear <- 2019
    #  temp <- temp[as.numeric(date)<= as.numeric(as.Date("2020-12-31")), ]
    # }
    
    if ("variable" %in% names(temp)) temp[, variable := NULL]
    temp[, temp := temp - 273.15]
    
    #if (year==2019 & inYear==2018) temp[, date := date + 365]
    
    ann_mean <- as.data.table(read_feather(paste0("/FILEPATH/meanTemps_", location_id, ".feather")))
    #colnames(ann_mean) <- c("meanTempCat", "x", "y")
    if ("loc_id" %in% names(ann_mean)) ann_mean[, loc_id := NULL]
    #ann_mean[,meanTempCat := as.integer(round(meanTempCat - 273.15, digits = 0))]
 
    #pop <- as.data.table(read_feather(paste0(j_drive, "/FILEPATH/", inYear, "/pop_", inYear, "_", location_id, ".feather")))
    #pop[,':=' (x = round(x, digits = 2), y = round(y, digits = 2))]
 
    #temp <- merge(temp, pop, by = c("x","y"), all.x = TRUE)
  
    nmiss <- sum(is.na(temp$population))
  
    if (nmiss>0 & nmiss<nrow(temp)) {
      temp[, population := as.numeric(na.approx(population, na.rm=FALSE)), by="date"]  ### if there are missing population pixels fill them through interpolation
    }
    
    temp$population[is.na(temp$population)==T] <- 0  ### if some pixels are still missing replace them with zero
    if (sum(temp$population)==0) {
        temp[, population := 1] ### if all pixels are zero then replace with 1 
    }
    
    temp <- merge(temp, ann_mean, by = c("x", "y"), all.x = TRUE)
    temp[, year_id := year]
    temp[, location_id := location_id]
  
    return(temp)
  # } else {
  #   return(data.table(year_id = y, location_id = location_id))
  # }
}




#for (y in 1990:2020) {
  print(y)
		
  allLocs <- lapply(locMeta[most_detailed==1, location_id], function(l) {pull.temps(year = y, location_id = l)})
  allLocs <- rbindlist(allLocs, fill=TRUE)

  print("All locations loaded.  Begining zone-specific export. ")
  
  write_feather(allLocs, paste0("/FILEPATH/temp_", y, "_allZones.feather"))
  
  allLocs[meanTempCat < 6, meanTempCat := 6][meanTempCat >28, meanTempCat := 28]
  
  
  for (zone in 6:28) {
     cat(paste0(zone, " "))
     write_feather(allLocs[meanTempCat==zone,], paste0("/FILEPATH/temp_", y, "_", zone, ".feather"))
  }
  

  allLocs <- merge(allLocs, tempLimits, by = "meanTempCat", all.x = T)
  allLocs[, dailyTempCat := as.integer(round(temp * 10))]
  allLocs[dailyTempCat < minTemp, dailyTempCat := minTemp]
  allLocs[dailyTempCat > maxTemp, dailyTempCat := maxTemp]
  
  collapsed <- allLocs[, lapply(.SD, function(x) {sum(x, na.rm = T)}), by = c("meanTempCat", "dailyTempCat", "location_id", "year_id"), .SDcols = "population"]
  
  for (zone in 6:28) {
    cat(paste0(zone, " "))
    write_feather(collapsed[meanTempCat==zone,], paste0("/FILEPATH/tempCollapsed_", y, "_", zone, ".feather"))
  } 
  




