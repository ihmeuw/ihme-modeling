#----HEADER----------------------------------------------------------------------------------------------------------------------
# Author: NAME
# Date: 02/08/2016
# Purpose: Create a clean environment to calc ozone PAFs, sample distributions to preserve covariance across parallel
# source("FILEPATH.R", echo=T)

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS" 
  h_root <- "ADDRESS"
  arg <- commandArgs()[-(1:5)]  # First args are for unix use only
  if (length(arg)!=2) {
    arg <- c(1000,18) #toggle targetted run 
  }
} else { 
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  arg <- c(1000,18) #toggle targetted run 
}

# Set parameters
draws.required <- arg[1]
#years <- c(1990,1995,2000,2005,2010,2015,2017,2019)
output.version <- arg[2]
#out.rr.dir <- file.path(j_root, "FILEPATH", output.version)

# load packages, install if missing
pacman::p_load(data.table, gdata, magrittr,ini,meta,grid)

"%ni%" <- Negate("%in%")

set.seed(143) # reproducibility

#----DIRECTORIES-----------------------------------------------------------------------------------------------------------------

# clean environment with all necessary objects for the analysis
out.dir <- file.path(j_root, "FILEPATH",output.version)
dir.create(out.dir,showWarnings = F)
out.environment <- file.path(out.dir,"FILEPATH.Rdata")
#objects kept:
#tmrel - draws of the TMREL distribution
#rr.draws - draws of the RR distribution

#----PREP------------------------------------------------------------------------------------------------------------------------
# generate draws of tmrel
tmrel <- data.frame(tmrel=runif(draws.required, 29.1, 35.7))

#GBD2019 MR-BeRT model with no bias covariates or priors

rr <- fread("/FILEPATH.csv")
rr.draws <- as.vector(unlist(rr[1]))

#----SAVE------------------------------------------------------------------------------------------------------------------------
keep(tmrel, #draws the of the tmrel
     rr.draws,
     out.environment,
     sure=T) #draws of the RR

detach(package:gdata)

# output your clean, prepped environment for parallelized calculation files to run in
save(list=ls(), file=out.environment)