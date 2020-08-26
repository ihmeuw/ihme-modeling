 # Test run for a single mean map for each of the different species
rm(list=ls())

if (Sys.info()[1] == "Linux"){
  ADDRESS <- "FILEPATH"
  package_lib <- "FILEPATH"
} else {
  ADDRESS <- "FILEPATH"
  package_lib <- "FILEPATH"}

# #load necessary packages
 library(sp)
 library(dplyr)
 library(maptools)
 library(raster)
 library(rgeos)
 library(rgdal)
 library(data.table)

source('FILEPATH')
package_list <- c("dplyr", "maptools", "raster", "rgeos", "rgdal", "data.table", "sp")
load_R_packages(package_list)

model_path <- 'FILEPATH/ntd_schisto/extract_PAR_non_urban_mask'
params_path <- 'FILEPATH/ntd_schisto/extract_PAR_non_urban_mask'
run_paths <- levels(tail(as.data.table(read.csv('FILEPATH', header=TRUE)), -1)$run_folder_path)
run_path <- run_paths[length(run_paths)]
source('FILEPATH/econiche_qsub.R')


#functions from seegSDM
notMissingIdx <- function(raster) {
  # return an index for the non-missing cells in raster
  which(!is.na(getValues(raster)))
}

missingIdx <- function(raster) {
  # return an index for the missing cells in raster
  which(is.na(getValues(raster)))
}

nearestLand<-function (points, raster, max_distance) 
{
  nearest <- function(lis, raster) {
    neighbours <- matrix(lis[[1]], ncol = 2)
    point <- lis[[2]]
    land <- !is.na(neighbours[, 2])
    if (!any(land)) {
      return(c(NA, NA))
    }
    else {
      coords <- xyFromCell(raster, neighbours[land, 1])
      if (nrow(coords) == 1) {
        return(coords[1, ])
      }
      dists <- sqrt((coords[, 1] - point[1])^2 + (coords[, 
                                                         2] - point[2])^2)
      return(coords[which.min(dists), ])
    }
  }
  neighbour_list <- extract(raster, points, buffer = max_distance, 
                            cellnumbers = TRUE)
  neighbour_list <- lapply(1:nrow(points), function(i) {
    list(neighbours = neighbour_list[[i]], point = as.numeric(points[i, 
                                                                     ]))
  })
  return(t(sapply(neighbour_list, nearest, raster)))
}

#set random number seed for in-house replicability
set.seed(1)

# date
time_stamp <- TRUE 
run_date <- make_time_stamp(time_stamp)

# define species type
species <- 'all_schisto'


# NOTE Interface to niche_map codebase

# set date of run to use
date_mansoni <- 'yyyy_mm_dd_hh_mm_ss'
date_haematobium <- 'yyyy_mm_dd_hh_mm_ss'
date_japonicum <- 'yyyy_mm_dd_hh_mm_ss'

#define input filepath 
input_mansoni<-('FILEPATH/ntd_schisto/niche_map/data/mansoni.csv'))
input_haematobium<-('FILEPATH/ntd_schisto/niche_map/data/ntd_schisto/niche_map/data/haematobium.csv'))
input_japonicum<-('FILEPATH/ntd_schisto/niche_map/data/ntd_schisto/niche_map/data/japonicum.csv'))

#define number of draws
data_dir <- 'FILEPATH'
map_dir <- 'FILEPATH'
outpath <- 'FILEPATH'

dir.create('FILEPATH/prop'))
dir.create('FILEPATH/thresholds'))


##########################################################

##############
## Submit jobs
##############

#Using qsub calls
njobs <- 1000 #no. of bootstraps; determines number of model runs; trial with 1
parallel_script <- 'FILEPATH/extract_par_species_parallel.r'
for(jobnum in 1:njobs) {
  qsub(paste0("niche_", jobnum), parallel_script, pass=list(jobnum, data_dir, map_dir, outpath, package_lib, date_mansoni, date_haematobium, date_japonicum), proj="ADDRESS", log=T, submit=T)
}
