## set up parallized portion from qsub

jobnum <- commandArgs()[3]
repo <-  commandArgs()[4]
outpath <- commandArgs()[5]
data_loc <- commandArgs()[6]
run_date <-  commandArgs()[7]
package_lib <- commandArgs()[8]
data_dir <- commandArgs()[9]

## Load libraries
setwd(repo)

# Library for packages. Ensures that none of this code is dependent on the machine where the user runs the code.
.libPaths(package_lib)# Ensures packages look for dependencies here when called with library().

# Load packages
package_list <- c('stringr', 'ggplot2', 'plyr', 'dplyr', 'rgeos', 'data.table','raster','rgdal', 'seegSDM','sp')
for(package in package_list) {
  library(package, lib.loc = package_lib, character.only=TRUE)
}

# Load functions files
source(paste0(repo, '/FILEPATH/functions.R'))                   

#########################################################################################
covs <- brick(paste0(data_loc, "/FILEPATH/schisto_covs1.grd"))

# create a list with random permutations of dat_all, 
# This is like k-folds stuff in mbg; subsample custom function
dat_all <- read.csv((paste0(FILENAME, ".csv"))) 
dat_all <- dat_all[,(-1)]
dat_rows <- nrow(dat_all)
nsample <- 0.3*dat_rows
minsample <- nsample/6
minsample <- 12
nsample <- 50
# Set the RNG seed
set.seed(jobnum)

data_sample <- subsamplenew(dat_all,
                          n = nsample,
                          minimum = c(minsample, minsample), 
                          max_tries = 30)
                          #simplify = FALSE)

model <- runBRT(data_sample,
          gbm.x = 5:ncol(data_sample),
          gbm.y = 1,
          pred.raster = covs, #brick
          gbm.coords = 2:3,
          # method = "step",
          wt = function(PA) ifelse(PA == 1, 1, sum(PA) / sum(1 - PA)), 
          n.folds = 3)



write.csv(model$relinf, file = paste0(data_dir, '/relinf/relinf_', jobnum, '.csv'))

stats <- getStats(model, 
                  pwd = FALSE)

# Output model results
writeRaster(model$pred, filename = paste0(FILENAME,".tif"), overwrite = TRUE)
# save an .rdata of everything else
save(model, file = paste0(FILENAME,".Rdata"))
# save the data used in the sample
write.csv(data_sample, file = paste0(FILENAME,".csv"))

save(stats, file = paste0(FILENAME,".Rdata"))



