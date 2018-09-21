########################################################################################
#Summarizing the BRT ensemble 
#There are three helper functions to help us summarize the BRT ensemble: 
#1. getRelInf
#2. getEffectPlots
#3. combinePreds
########################################################################################
repo <-  commandArgs()[3]
outpath <- commandArgs()[4]
data_loc <- commandArgs()[5]
run_date <-  commandArgs()[6]
package_lib <- commandArgs()[7]
data_dir_model <- commandArgs()[8]
data_dir_stats <- commandArgs()[9]
jobnum <- commandArgs()[10]

## Load libraries
setwd(repo)

# Library for packages. Ensures that none of this code is dependent on the machine where the user runs the code.
.libPaths(package_lib)# Ensures packages look for dependencies here when called with library().

# Load packages
package_list <- c('car', 'MASS', 'stringr', 'reshape2', 'ggplot2', 'plyr', 'dplyr', 'rgeos', 'data.table','raster','rgdal', 'seegSDM','sp')
for(package in package_list) {
  library(package, lib.loc = package_lib, character.only=TRUE)
}

# Load functions files
source(paste0(repo, '/FILEPATH/functions.R'))                   

#########################################################################################
# get the subset of job numbers
file_start <- 200 * (jobnum - 1) + 1
file_end <- 200 * (jobnum)
#Bring in model and stats and make into 2 lists
model_filenames <- list.files(path = (data_dir_model))[file_start:file_end]
stat_filenames <- list.files(path = (data_dir_stats))[file_start:file_end]


fetch_from_rdata = function(file_location, item_name, use_grep = F){
  load(file_location)
  if(use_grep){
    return(mget(grep(item_name, ls(), value=T)))
  }else{
    return(get(item_name))
  }
}
model_list <- lapply(paste0(data_dir_model, model_filenames), function(x) raster(x))
preds <- brick(model_list) #change file location
stat_list <- lapply(paste0(data_dir_stats, stat_filenames), function(x) fetch_from_rdata(x, 'stats', F))

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
writeRaster(preds,
            file = paste0(outpath,
                          '/schisto_pred_summary_', run_date, '_', jobnum),
            format = 'GTiff',
            overwrite = TRUE)

# convert the stats list into a matrix using the do.call function
stats <- do.call("rbind", stat_list) # need to fix this

# save them
write.csv(stats, paste0(outpath, '/stats_', run_date, '_', jobnum,".csv"))


#1. getRelInf: combines the models to get summaries of the relative influence of the covariates across all the models. 
#              It returns a matrix of means and quantiles, and optionally produces a boxplot.
relinf <- getRelInf(68, plot = TRUE)

#Save the relative influence scores
write.csv(relinf,
          file = paste0(FILENAME, '.csv'))
