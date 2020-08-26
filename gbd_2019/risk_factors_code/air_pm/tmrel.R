#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: NAME
# Date: 04/21/2016
# Project: RF: air_pm
# Purpose: Prep tmrel based on distribution from EG report
# update of source("FILEPATH.R")
# source("FILEPATH.R", echo=T)

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "ADDRESS" 
  h_root <- "ADDRESS"
  
} else { 
  
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  
}

# load packages, install if missing
pacman::p_load(data.table)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# in/out
# out
tmrel.dir <- file.path(home.dir, 'FILEPATH')

# set parameters
draws.required <- 1000
version <- "gbd2017"

# set seed for draws
seed <- 2846702
set.seed(seed)

# GBD 2010 
if (version == "gbd2010") tmrel <- data.table(tmrel=runif(draws.required, 5.8, 8.8))

# GBD 2013: Quote from EG report.
	# The GBD 2010 tmrel was based on Uniform uncertainty distribution with lower/upper bound equal to minimum and 5th percentile 
	# from the ACS study alone:  U(5.8, 8.8). For GBD 2013 we propose to use the same general approach but use similar information 
	# from additional cohort studies, besides the ACS, which are listed in Table 1.   However, we excluded three cohort studies for 
	# which their minimum exposure concentration was greater than the 5th percentile observed in the ACS of 8.85g/m3 since they did 
	# not provide information on risk at concentrations for which we a priori believed there existed an association.   We thus excluded 
	# AHSMOG (minimum=12.95g/m3), DSDC (minimum=23.05g/m3), and the Japanese cohort (minimum=16.85g/m3). 
	# We then averaged either the minimum or 5th percentile concentrations among the nine remaining studies resulting in average 
	# values of 5.9 and 8.7 respectively.
	# We thus define: tmrel ~ U(5.9, 8.7).
if (version == "gbd2013") tmrel <- data.table(tmrel=runif(draws.required, 5.9, 8.7))

# GBD 2015:
  #New criteria that a cohort's 5th percentile must be <= the 5th percentile of the ACS 
  #(which is now 8.2 ug/m3 based on the most recent pub)
  #the average of the min and 5th among the 7 cohorts meeting this criteria is TMREL~U(2.4, 5.9) which is below the 
  #GBD2013 TMREL~U(5.9, 8.7) 
if (version == "gbd2015") tmrel <- data.table(tmrel=runif(draws.required, 2.4, 5.9))

if (version == "gbd2017") tmrel <- data.table(tmrel=runif(draws.required, 2.4, 5.9))

write.csv(tmrel, paste0(tmrel.dir, "FILEPATH", version, ".csv"), row.names=F)