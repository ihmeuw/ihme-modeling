######################################
#Code author:   USERNAME
#Date: DATE
#Code intent: Bernoulli niche model
#####################################

########################################################################################

# clear workspace
#rm(list = ls())

########################################################################################
#Setting up

# - Initially examining environment and indicating file paths dependent on OS - Linux vs Windows
# - Calling 'parallel' and specifying cores 
# - Setting data location, output path,  working directory and library
# - Sourcing external function scripts
# - Loading packages
# - Creating a time stamp for model run
########################################################################################

if (Sys.info()[1] == "Linux"){
  j <- "FILENAME"
  h <- paste0("FILENAME",Sys.info()[6])
  package_lib <- paste0(FILENAME)
} else {
  j <- "FILENAME"
  h <- "FILENAME"
  package_lib <- paste0(FILENAME)
}

# Set repo location
repo <- paste0(FILENAME)

# set species (haematobium, mansoni, japonicum, or all_schisto)
species <- "mekongi"

## Set data location
data_loc <- (paste0(FILENAME))

# Set output path
outpath <- (paste0(FILENAME))

## Load libraries
setwd(repo)

# Library for packages. Ensures that none of this code is dependent on the machine where the user runs the code.

.libPaths(package_lib)# Ensures packages look for dependencies here when called with library().

# Load functions files
source(paste0(repo, '/econiche_central/functions.R'))                   
source(paste0(repo, '/econiche_central/econiche_qsub.R'))  
source(paste0(repo, '/econiche_central/check_loc_results.R'))  

# Load packages ***** took out 'car' and 'MASS' and 'reshape2'
package_list <- c('stringr', 'ggplot2', 'plyr', 'dplyr', 'rgeos', 'data.table','raster','rgdal','sp', 'seegSDM')

for (package in package_list) {
  library(package, lib.loc = package_lib, character.only=TRUE)
}

time_stamp <- TRUE 
run_date <- make_time_stamp(time_stamp)

## buffer distance (in meters)
buff_dist <- "100km"

########################################################################################
########################################################################################

# Load covariate raster brick here 
covs <- brick(paste0(data_loc, "FILEPATH/schisto_covs1.grd"))
print('Loading covariate brick')

# Occurrence data - schisto point data; will need to change for each species, currently mansonia
occ <- read.csv(file = (paste0(data_loc, '/', species, '.csv')))
print('Loading occurrence data')
num_rows <- nrow(occ)

# Generate pseudo-absence data according to the aridity surface and suppress weighting (prob=FALSE) so as to not weight by aridity pixel values
buffer <- raster(paste0(data_loc, "/covariates/buffer", buff_dist, ".tif"))
print('Loading buffer grid for background point generation')

bg <- bgSample(buffer, # Weighting grid
               n = num_rows*2, 
               prob = TRUE, 
               replace = TRUE,
               spatial = FALSE)
print('Generating background data')

colnames(bg) <- c('longitude', 'latitude') 
bg <- data.frame(bg)
print('Making background data into a dataframe')

# Add an outbreak id to this
bg$outbreak_id <- 0

## add a weight value
# first get the sum of presence weights
total_weight <- sum(occ$prevalence)
bg$prevalence <- total_weight/num_rows*2

# Combine the occurrence and background records
dat <- rbind(cbind(PA = rep(1, nrow(occ)),
                   occ[, c('longitude', 'latitude', 'outbreak_id', 'prevalence')]),
             cbind(PA = rep(0, nrow(bg)),
                   bg))
print('Combining occurrence and background records')
 
#need to drop 'outbreak_id'
dat <- dat[,c(1:3, 5)]

# Get the covariate values for every data point - pseudo and actual
dat_covs <- extract(covs, dat[, 2:3]) 
print('Getting covariate values for every data point - background and observed')

# Then add them
dat_all <- cbind(dat, dat_covs)
print('Adding extracted covariate values to the occurrence and background records dataframe')

dat_all <- na.omit(dat_all)
print('Omitting all null values from dataframe')

write.csv(dat_all, file = (paste0(outpath, "/interim_output/dat_all", run_date, ".csv")))

#######################################################################################
#Parallelizing BRT model runs
#######################################################################################
data_dir <- paste0(outpath, '/', run_date)
dir.create(data_dir)
dir.create(paste0(data_dir,"/model_output"))
dir.create(paste0(data_dir,"/stats_output"))
dir.create(paste0(data_dir,"/data_output"))
dir.create(paste0(data_dir,"/final_output"))
dir.create(paste0(data_dir,"/relinf"))
dir.create(paste0(data_dir,"/data_input"))

#Using qsub calls
njobs <- 1000
parallel_script_1 <- (paste0(repo,"/FILEPATHl/brt_model.R"))
for(jobnum in 1:njobs) {
  qsub(paste0("brt_niche_", jobnum), parallel_script_1, pass=list(jobnum, repo, outpath, data_loc, run_date, package_lib, data_dir), proj="proj_custom_models", log=T, slots=4, submit=T)
}


## Check for results - makes sure models running and allows time for them to run
data_dir_model <- paste0(data_dir, '/model_output/')
data_dir_stats <- paste0(data_dir, '/stats_output/')
data_dir_data <- paste0(data_dir, '/data_output/')
  
Sys.sleep(600)
check_loc_results(c(1:njobs), data_dir_model, prefix="model_",postfix=".tif")

Sys.sleep(600)
check_loc_results(c(1:njobs), data_dir_stats, prefix="stats_",postfix=".Rdata")

########################################################################################
#Summarizing the BRT ensemble using qsubs, 10 at a time
########################################################################################
njobs <- 100
parallel_script_2 <- (paste0(repo,"/FILEPATH/summarize_ensemble_1.R"))
outpath <- paste0(data_dir, '/output_temp')
dir.create(outpath)
# this needs to be done 100 times
for(jobnum in 1:njobs) {
  qsub(paste0("summ_ensemble_", jobnum), parallel_script_2, pass=list(repo, outpath, data_loc, run_date, package_lib, data_dir_model, data_dir_stats, jobnum), proj="proj_custom_models", log=T, slots=10, submit=T)
}
  
########################################################################################
#Summarizing the BRT ensemble using qsubs, 10 at a time (second round)
########################################################################################
njobs <- 10
outold <- paste0(data_dir, '/output_temp/')
parallel_script_3 <- (paste0(repo,"/econiche_central/summarize_ensemble_2.R"))
outpath <- paste0(data_dir, '/outpath_temp_2')
dir.create(paste0(data_dir, '/outpath_temp_2'))

# this needs to be done 10 times
for(jobnum in 1:njobs) {
  qsub(paste0("summ2_ensemble_", jobnum), parallel_script_3, pass=list(repo, outpath, data_loc, run_date, package_lib, data_dir_model, outold, jobnum), proj="proj_custom_models", log=T, slots=10, submit=T)
}

########################################################################################
#Summarizing the BRT ensemble (last 10), no need to parallelize
########################################################################################
final_outpath <- paste0(data_dir, '/final_output')
dir.create(final_outpath)
model_filenames <- list.files(path = (outpath))[1:10]
# combining rasters
model_list <- lapply(paste0(outpath,'/', model_filenames), function(x) raster(x))
# bricking rasters
preds <- brick(model_list)
preds <- mean(preds)

# save the prediction summary
writeRaster(preds,
            file = paste0(final_outpath,
                          '/schisto_pred_final'),
            format = 'GTiff',
            overwrite = TRUE)

########################################################################################
#Summarizing stats
########################################################################################

# read in file names
stat_filenames <- list.files(path=paste0(data_dir, "/stats_output/"))
# reset outpath to original outpath
outpath <- (paste0(FILENAME))
numstats <- length(stat_filenames)
# set the empty list to collate stats
stat_list <- list()
for(i in 1:numstats) {
  load(paste0(data_dir, "/stats_output/", stat_filenames[i]))
  stat_list[[i]] <- stats
}
# stats directory
dir.create(paste0(data_dir, "/stats_final"))
stat_dir <- paste0(data_dir, "/stats_final")
# combine all stats
stat_all <- do.call(rbind, stat_list)
# save stats
write.csv(stat_all, paste0(stat_dir, "/stat_agg.csv"))

relinf_filenames <- list.files(path=paste0(data_dir, '/relinf'))
numrelinf <- length(relinf_filenames)
relinf_list <- list()
# create a list for all of the relative influence csvs
for(i in 1:numrelinf) {
  read.csv(paste0(data_dir,'/relinf/',relinf_filenames[i]))
  relinf_list[[i]] <- read.csv(paste0(data_dir,'/relinf/',relinf_filenames[i]))
}

# rewrite the getRelInf function so it fits with a list of files
adjusted_getRelInf <- function (models, plot = FALSE, quantiles = c(0.025, 0.975), ...)

{
  rel.infs <- t(sapply(models, function(x) x[, 3]))
  colnames(rel.infs) <- models[[1]][, 1]
  if (plot) {
    boxplot(rel.infs, ylab = 'relative influence', col = 'light grey', ...)
  }
  relinf <- cbind(mean = colMeans(rel.infs),
                  t(apply(rel.infs, 2, quantile, quantiles)))
  return (relinf)
}

relinf <- adjusted_getRelInf(relinf_list)
write.csv(relinf, paste0(stat_dir, "/relinf_agg.csv"))

