#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Create a clean environment to calc ozone PAFs, sample distributions to preserve covariance across parallel

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "~/"
  arg <- commandArgs()[-(1:5)]  # First args are for unix use only
  if (length(arg)!=2) {
    arg <- c(1000,20) #toggle targetted run 
  }
} else { 
  j_root <- "J:"
  h_root <- "H:"
  arg <- c(1000,20) #toggle targetted run 
}

# Set parameters
draws.required <- arg[1]
output.version <- arg[2]
model.version <- arg[3]

# load packages, install if missing
pacman::p_load(data.table, gdata, magrittr,ini,meta,grid)

"%ni%" <- Negate("%in%") # create a reverse %in% operator

set.seed(143) # reproducibility

#----IN&OUT----------------------------------------------------------------------------------------------------------------------

# clean environment with all necessary objects for the analysis
out.dir <- file.path(j_root, "FILEPATHS/",output.version)
dir.create(out.dir,showWarnings = F)
out.environment <- file.path(out.dir,"clean.Rdata")

#----PREP------------------------------------------------------------------------------------------------------------------------
# Generate TMREL
# Update for GBD 2017: 0 and 5% of (Aug-Sept) ozone from updated ACS II study, NID 259393
tmred <- data.frame(tmred=runif(draws.required, 29.1, 35.7))

# Load RR
# GBD2019 MR-BRT model with no bias covariates or priors
rr <- fread("/ihme/erf/GBD2019/air_ozone/rr/draws/",model.version,"/draws.csv")
rr <- exp(rr)
r.draws <- as.vector(unlist(rr[1]))


#----SAVE------------------------------------------------------------------------------------------------------------------------
# clean up environment (removing intermediate steps: keep only objects necessary to running 02_calc.R)
keep(tmred, #draws the of the TMRED
     rr.draws,
     out.environment,
     sure=T) #draws of the RR


# detach the gdata function, as it is pesky and masks other functions that I may want to use later. I only need it for the above keep() call
detach(package:gdata)

# output your clean, prepped environment for parallelized calculation files to run in
save(list=ls(), file=out.environment)
#********************************************************************************************************************************
