# Purpose: Prep temperature data at the Mexico or Brazil municipality level for link up with COD data
# Output should have average daily temperature by admin 2 level for each day, saved by year
# source('FILEPATH', echo = T)
rm(list=ls())
#set up smart sensing of os
if(Sys.info()[1]=='Windows'){
  j = "ADDRESS"
} else{
  j = 'ADDRESS'
}

# load packages, install if missing
pack_lib = 'FILEPATH'
.libPaths(pack_lib)
pacman::p_load(data.table, fst, sf, fasterize, ggplot2, parallel, magrittr, maptools, raster, rgdal, rgeos, sp, splines, stringr, RMySQL, snow, ncdf4, feather)

## set filepath objects
iso <- "gtm" #bra or mex or us or nza or gtm
max.cores <- 6
proj <- "era_interim"
data.dir = paste0(j,'FILEPATH', proj, '/')
out.dir = paste0(j,'FILEPATH', iso,'/', proj, '/')
shapefile.dir = paste0(j, "FILEPATH", iso, "/", iso)
map.dir = paste0(j,'FILEPATH')
pop.dir <- paste0(j, "FILEPATH")
code.dir = paste0('FILEPATH')

#source locations function
source(paste0(j, "FILEPATH"))
source(paste0(code.dir, 'FILEPATH'))

########Read in shapefile and extract##########
borders <- sf::st_read( paste0(shapefile.dir, "FILEPATH") )
df <- data.table(borders$adm2_name)

#create fake raster with dimensions of temp raster
b <- raster()
extent(b) <- extent(borders)
res(b) <- .5

#Loop through years
for(year in c(1989)){
     #read in file and crop to borders of specified country
  brik <- brick(paste0(data.dir, year, "FILEPATH"))
  brik <- rotate(brik) # b/c era raster coordinates is from 0 to 360 and needs to be -180 to 180
  brik <- resample(brik, b, method = "bilinear")


  #NEED TO PULL IN POPS AND EXTRACT IN ORDER TO POP WEIGHT
  #Read in and crop pop
  pop <- raster(paste0(pop.dir, 'FILEPATH',year,'_00_00', '.tif'))
  pop <- crop(pop, borders)
  pop_1 <- resample(pop, b, method = "bilinear")
  pop <- mask(crop(pop_1, brik), brik)
  pop.spdf <- as(pop_1, "SpatialPixelsDataFrame")
  pop.df <- as.data.frame(pop.spdf)



  #multiply temp times pop and resample to .05 x .05 resolution
  brik <- brik * pop

  ##extract weighted temperature and pop to the municipality
  print(paste0("extracting ", year))
  ugh <- extract(brik, coordinates(borders), fun = sum, na.rm = T, method='bilinear')
  pop_1 <- extract(pop, coordinates(borders), fun = sum, na.rm = T, method='bilinear')

  pop <- pop_1[,1]
  ##add on important metadata- muni codes
  bord <- cbind(as.numeric(as.character(borders@data$ADM1_CODE)), as.character(borders@data$ADM1_NAME)) %>% as.data.table
  setnames(bord, c("V1", "V2"), c("adm1_id", "adm1_name"))
  ugh <- cbind(ugh, bord)
  pop <- cbind(pop, bord)

  ##melt down by day
  dt <- melt(ugh, id = c("adm1_id", "adm1_name"))

  #merge on pops by admin2 and divide by them to finish pop weighting
  dt <- merge(dt, pop, by = c("adm1_id", "adm1_name"))


  #Extract for tiny municipalities that didn't extract correctly
  missing.munis <- dt[is.na(value), unique(adm1_id)]

  if (length(missing.munis)> 0){
    missing.countries.vals <- mclapply(missing.munis,
                                       estimateIslands,
                                       borders=borders,
                                       ras=brik,
                                       mc.cores=max.cores)
    missing.pop.vals <- mclapply(missing.munis,
                                 estimatePopIslands,
                                 borders=borders,
                                 ras=pop_2,
                                 mc.cores=max.cores)
    missing.dt <- rbindlist(missing.countries.vals)
    missing.pop <- rbindlist(missing.pop.vals)
    names(missing.dt) <- c("adm2_id", "adm2_name", "variable", "value")
    names(missing.pop) <- c("adm2_id", "adm2_name", "variable", "pop")
    missing.dt <- merge(missing.dt, missing.pop, by = c("adm2_id", "adm2_name", "variable"))
    #Bind together missing temp munis with bigger ones and covert to celsius
    dt <- dt[!is.na(value),]
    dt <- rbind(dt, missing.dt, fill = T)
  }

  dt[, temperature := value / pop]
  dt[, day := substring(as.character(variable), 2, 5)]
  dt <- dt[, lapply(.SD, mean, na.rm = T), .SDcols = "temperature", by = c("adm1_id", "adm1_name", "day")]
  dt[, temperature := temperature - 273.15]
  dt[, year_id := year]
  dt[, date := paste(day, year_id, sep = "/")]
  dt[, date := as.Date(date, "%j/%Y")]
  dt[, adm1_id := as.numeric(adm1_id)]

  #clean and save
  dt <- dt[, .(adm1_id, temperature, date, year_id)]
  write.csv(dt, paste0(out.dir, "municipality_temp_adm1_", year, ".csv"), row.names = F)
  print(paste0("saved ", year))

}
#end#


## combine all years and calculate all years mean for all admin 2s from 1990-2017
iso <- ISO
out.dir <- paste0(j,'FILEPATH', iso,'/', proj, '/')
all.dt <- data.table(year_id = as.numeric(), adm1_id = as.numeric(), temperature = as.numeric())
dir.create(paste0(out.dir, "annual"))
for(year in 1990:2017){
  temp.dt <- fread(paste0(out.dir, "municipality_temp_adm1_", year, ".csv"))
  temp.dt <- temp.dt[, lapply(.SD, mean), .SDcols = "temperature", by = c("year_id", "adm1_id")]
  all.dt <- rbind(all.dt, temp.dt)
  print(year)
}
all.dt <- all.dt[, lapply(.SD, mean), .SDcols = "temperature", by = c("adm1_id")]
write.csv(all.dt, paste0(out.dir, "FILEPATH"), row.names = F)
