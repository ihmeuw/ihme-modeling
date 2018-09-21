########################################################################################
#Summarizing the BRT ensemble 
#There are three helper functions to help us summarize the BRT ensemble: 
#1. getRelInf
#2. getEffectPlots
#3. combinePreds
########################################################################################
repo <-  commandArgs()[3]
print(repo)
outpath <- commandArgs()[4]
print(outpath)
data_loc <- commandArgs()[5]
print(data_loc)
run_date <-  commandArgs()[6]
print(run_date)
package_lib <- commandArgs()[7]
print(package_lib)
data_dir_model <- commandArgs()[8]
print(data_dir_model)
outold <- commandArgs()[9]
print(outold)
jobnum <- as.numeric(commandArgs()[10])
print(jobnum)



## Load libraries
print("set wd")
setwd(repo)

# Library for packages. Ensures that none of this code is dependent on the machine where the user runs the code.
.libPaths(package_lib)# Ensures packages look for dependencies here when called with library().

# Load packages
package_list <- c('stringr', 'reshape2', 'ggplot2', 'plyr', 'dplyr', 'rgeos', 'data.table','raster','rgdal', 'seegSDM','sp')
print("load packages")
for(package in package_list) {
  library(package, lib.loc = package_lib, character.only=TRUE)
}

# Load functions files
print("source repo")
source(paste0(repo, 'FILEPATH/functions.R'))                   

#########################################################################################
# get the subset of job numbers
file_start <- 10 * (jobnum - 1) + 1
file_end <- 10 * (jobnum)
#Bring in model and stats and make into 2 lists
print("get file names for 10 models")
model_filenames <- list.files(path = (outold))[file_start:file_end]
print(model_filenames)


print("merging rasters")
model_list <- lapply(paste0(outold, model_filenames), function(x) raster(x))
print("forming brick")
preds <- brick(model_list) #change file location

#Reading in files and making into lists--don't need this because we don't have .csv
#model_list <- lapply(paste0(data_dir_model, model_filenames), fread)
#stat_lis <- lapply(paste0(data_dir_stats, stat_filenames), fread)

#3. combinePreds: combines the prediction maps (on the probability scale) from multiple models and returns rasters giving the mean, median and quantiles
#                 of the ensemble predictions. unlike the previous two functions, combinePreds needs a RasterBrick or RasterStack object with each layers 
#                 giving a single prediction. So we need to create one of these before we can use combinePreds. Note that we can also run combinePreds in 
#                 parallel to save some time if the rasters are particularly large.
# Run combinePreds - could summarise the predictions in parallel
preds <- mean(preds)

# save the prediction summary
print("saving raster")
writeRaster(preds,
            file = paste0(outpath,
                          '/schisto_pred_final_', run_date, '_', jobnum),
            format = 'GTiff',
            overwrite = TRUE)

