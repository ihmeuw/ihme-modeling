# Run for a single mean map for each of the different species

if (Sys.info()[1] == "Linux"){
  j <- "FILEPATH"
  h <- paste0("FILEPATH",Sys.info()[6])
  package_lib <- paste0(j,'FILEPATH')
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  package_lib <- paste0(j,'FILEPATH')
}
.libPaths(package_lib)
#load necessary packages
library(dplyr)
library(maptools)
library(raster)
library(rgeos)
library(rgdal)
library(data.table)

repo <- paste0(j, 'FILEPATH')

source(paste0(repo, 'FILEPATH/econiche_qsub.R'))

# get world shapefile
worldshp = readOGR(paste0(j, 'FILEPATH'), 'GBD2017_analysis_final')
# get rid of the factor class
worldshp$loc_id = as.numeric(levels(worldshp$loc_id))[as.integer(worldshp$loc_id)]
# pull in geographic restrictions
# excl_list = data.table(read.csv(paste0(j, "/species_specific_exclusions.csv"), stringsAsFactors = F))
# # keep if a location is endemic
# excl_list = excl_list[japonicum == "" | mansoni == "" | haematobium =="" | other == "",]
# excl_list_mansoni = excl_list[mansoni == "" | location_id == 172,]
# excl_list_haematobium = excl_list[haematobium =="" | location_id == 215,]
# excl_list_japonicum = excl_list[japonicum == "" | location_id == 10 | location_id == 12,]
# #subset world shp to be only locations with da shishto
# worldshp = worldshp[worldshp$loc_id %in% excl_list[,location_id],]
# worldshp_mansoni = worldshp[worldshp$loc_id %in% excl_list_mansoni[,location_id],]
# worldshp_haematobium = worldshp[worldshp$loc_id %in% excl_list_haematobium[,location_id],]
# worldshp_japonicum = worldshp[worldshp$loc_id %in% excl_list_japonicum[,location_id],]

#nichemap_mansoni <- raster(paste0(FILEPATH/model_', date, i, '.tif'), band=1)

#ras_shp = rasterize(worldshp, nichemap, field = 'loc_id')

# # set date of run I want to use -- this was done in 2016, keeping for the info
# date_mansoni <- '2017_05_30_11_18_43'
# date_haematobium <- '2017_05_30_11_23_36'
# date_japonicum <- '2017_05_30_11_32_48'






##############
## Submit jobs
##############

# extract for japonicum at the species level -- there's no overlap between other species and japonicum, so it's ok that this is done on it's own
njobs <- 1000 #no. of bootstraps; determines number of model runs; trial with 1
parallel_script <- (paste0(repo,"FILEPATH/extract_par_species_parallel_temp.r"))
for(color in c("green", "pink", "red")) {
  species <- "japonicum"
  inpath <- paste0(j, 'FILEPATH')
  data_input <- paste0(j, 'FILEPATH/', species, '/dat_all.csv')
  outpath <- paste0(j, 'FILEPATH', species, "_", color, "_urbanmask")
  dir.create(outpath)
  dir.create(paste0(outpath, 'FILEPATH'))
  dir.create(paste0(outpath, 'FILEPATH'))
  for(jobnum in 1:njobs) {
    ras_shp <- paste0(j, "FILEPATH", color, ".tif")
    qsub(paste0(color, "_", jobnum), parallel_script, pass=list(jobnum, outpath, inpath, data_input, ras_shp), proj="proj_custom_models", log=T, slots=4, submit=T)
  }
}

# extract haematobium and mansoni together because they overlap

# set date of run to use
run_date <- '2018_06_19'

#define number of draws
outpath <- (paste0(j, FILEPATH))
dir.create(outpath)
dir.create(paste0(outpath, 'FILEPATH'))
dir.create(paste0(outpath, 'FILEPATH'))
root_path <- paste0(j, 'FILEPATH')

parallel_script <- (paste0(repo,"FILEPATH/extract_par_species_parallel.r"))

for(jobnum in 1:njobs) {
  qsub(paste0("schisto_", jobnum), parallel_script, pass=list(jobnum, root_path, outpath), proj="proj_custom_models", log=T, slots=4, submit=T)
}
