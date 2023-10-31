#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  
} else { 
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

# load packages, install if missing
pacman::p_load(data.table)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

##in/out##
#out
tmrel.dir <- file.path(home.dir, 'FILEPATH')

#set parameters
draws.required <- 1000
version <- "VERSION"

#set seed for draws
seed <- 2846702
set.seed(seed)


# GBD 2010 
if (version == "gbd2010") tmrel <- data.table(tmrel=runif(draws.required, 5.8, 8.8))

if (version == "gbd2013") tmrel <- data.table(tmrel=runif(draws.required, 5.9, 8.7))

if (version == "gbd2015") tmrel <- data.table(tmrel=runif(draws.required, 2.4, 5.9))

if (version == "gbd2017") tmrel <- data.table(tmrel=runif(draws.required, 2.4, 5.9))

write.csv(tmrel, paste0(tmrel.dir, "FILEPATH", version, ".csv"), row.names=F)

