#----HEADER----------------------------------------------------------------------------------------------------------------------

# Date: 02/08/2016
# Purpose: Create a clean environment to calc ozone PAFs, sample distributions to preserve covariance across parallel

#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  arg <- c(1000) #toggle for targeted run
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- c(1000)
}

# Set parameters
draws.required <- arg[1]

# load packages, install if missing
pacman::p_load(data.table, gdata, magrittr)
#********************************************************************************************************************************

#----IN&OUT----------------------------------------------------------------------------------------------------------------------
###Input###
#USER

###Output###
# clean environment with all necessary objects for the analysis
out.environment <- file.path(FILEPATH)
#objects kept:
#tmred - draws of the TMRED distribution
#rr.draws - draws of the RR distribution
#********************************************************************************************************************************

#----PREP------------------------------------------------------------------------------------------------------------------------
# generate draws of tmred
tmred <- data.frame(tmred=runif(draws.required, 33.3, 41.9))

# generate draws of rr from study using mean/ci
rr.mean <- 1.029
rr.lower <- 1.010
rr.upper <- 1.048
rr.sd <- (log(rr.upper)-log(rr.lower))/(2*1.96)
rr.draws <- exp(rnorm(draws.required,log(rr.mean),rr.sd))
#********************************************************************************************************************************

#----SAVE------------------------------------------------------------------------------------------------------------------------
# clean up environment (removing intermediate steps: keep only objects necessary to running 02_calc.R)
keep(tmred, #draws the of the TMRED
     rr.draws,
     out.environment,
     sure=T) #draws of the RR

# detach the gdata function, as it is pesky and masks other functions that i may want to use later. i only need it for the above keep() call
detach(package:gdata)

# output your clean, prepped environment for parallelized calculation files to run in
save(list=ls(), file=out.environment)
#********************************************************************************************************************************