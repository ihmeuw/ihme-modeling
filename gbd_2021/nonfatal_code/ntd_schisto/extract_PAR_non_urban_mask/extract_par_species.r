 # Test run for a single mean map for each of the different species
rm(list=ls())

if (Sys.info()[1] == "Linux"){
  j <- "FILEPATH"
  code_root <-  paste0("FILEPATH/",Sys.info()[7])
  package_lib <- paste0(j,'FILEPATH/singularity_packages/3.5.0')
} else {
  drive1 <- "FILEPATH"
  drive2 <- "FILEPATH"
  package_lib <- paste0(drive1,'FILEPATH/packages_windows')
}

# libPaths(package_lib)
# load necessary packages
 library(sp)
 library(dplyr)
 library(maptools)
 library(raster)
 library(rgeos)
 library(rgdal)
 library(data.table)


source('FILEPATH/setup.R')
package_list <- c("dplyr", "maptools", "raster", "rgeos", "rgdal", "data.table", "sp")
load_R_packages(package_list)

model_path <- paste0(h, 'FILEPATH')
params_path <- 'FILEPATH'
run_paths <- levels(tail(as.data.table(read.csv('FILEPATH', header=TRUE)), -1)$run_folder_path)
run_path <- run_paths[length(run_paths)]
source(paste0(model_path, 'FILEPATH'))


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
#run_date <- make_time_stamp(time_stamp)
run_date <- 'DATE_green'

# define species type
species <- 'all_schisto'


# NOTE Interface to niche_map codebase
# ------------------------------------------------------------------------

# set date of run I want to use
date_mansoni <- 'date_mansoni'
date_haematobium <- 'date_haematobium'
date_japonicum <- 'date_japonicum'
#define input filepath
input_mansoni<-(paste0(drive1, 'FILEPATH'))
input_haematobium<-(paste0(drive1, 'FILEPATH'))
input_japonicum<-(paste0(drive1, 'FILEPATH'))

#define number of draws
data_dir <- 'FILEPATH'

map_dir <- 'FILEPATH'

outpath <- paste0(run_path, 'FILEPATH')

dir.create(paste0(outpath, 'FILEPATH'))
dir.create(paste0(outpath, 'FILEPATH'))


##############
## Submit jobs
##############

njobs <- 1000 #no. of bootstraps; determines number of model runs; trial with 1
parallel_script <- paste0(model_path, 'FILEPATH')
for(jobnum in 1:njobs) {
  submit(paste0("niche_", jobnum), parallel_script, pass=list(jobnum, data_dir, map_dir, outpath, package_lib, date_mansoni, date_haematobium, date_japonicum), proj="ADDRESS", log=T, submit=T)
}