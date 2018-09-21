# ---HEADER-------------------------------------------------------------------------------------------------------------

# Project: OCC - OCC
# Purpose: Prep economic activities for modellling
#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# set toggles

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  # necessary to set this option in order to read in a non-english character shapefile on a linux system (cluster)
  Sys.setlocale(category = "LC_ALL", locale = "C")
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

# load packages
pacman::p_load(data.table, ggplot2, lme4, magrittr, parallel, stringr, readxl, zoo)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

#set values for project
location_set_version_id <- 149
year_start <- 1970
year_end <- 2016
by_sex <- 1
by_age <- 0
relevant.ages <- c(8:18) #only ages 15-69
emp.version <- 8 #check most recent version of worker.version from 3_squeeze code

##in##
turnover.dir <- file.path(home.dir, "FILEPATH")
turnover <- file.path(turnover.dir, 'turnover_01292015.csv') %>% fread
emp.dir <- file.path(home.dir, "FILEPATH", emp.version)

##out##
out.dir <- file.path(home.dir, "FILEPATH")
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db (personal version)
file.path(central.function.dir, "db_tools.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source
# central functions
file.path('FILEPATH') %>% source
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#files#
#specify the function to define RDS to hold emp.occ/industry draws
fileName <- function(dir,
                     loc,
                     value,
                     suffix) {
  
  file.path(dir, paste0(loc, '_', value, suffix)) %>% return
  
}
#***********************************************************************************************************************

# ---PREP WORKERS-------------------------------------------------------------------------------------------------------
# bring in levels
locs <- get_location_hierarchy(location_set_version_id)
locations <- unique(locs[is_estimate==1, location_id]) %>% sort
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]

#interpolate years for the turnover file
#first add missing datapoints for the years in question
ot.cols <- names(turnover)[names(turnover) %like% 'OT']

duplicateYear <- function(this.year, dt) {
  
  message('duplicating for year=', this.year)
  out <- copy(dt[year == 1990])
  out[, year := this.year]
  
  #replace columns with missing
  out[, (ot.cols) := NA]
  
  return(out)
  
}

years <- (year_start:year_end)[(year_start:year_end) %ni% unique(turnover$year)] #create a vector of missing years
dupes <- lapply(years, duplicateYear, dt=turnover) %>% rbindlist
turnover <- list(turnover, dupes) %>% rbindlist

#now interpolate using the zoo package na.spline
turnover <- turnover[order(location_name, sex, duration, year)]
turnover[, (ot.cols) := lapply(.SD, na.spline), .SDcols=ot.cols,
         by=list(location_name, sex, duration)]

#read employment ratio from outputs
#saved by country so needs to be appended
appendFiles <- function(country, dir, pfx, sfx) {
  
  message('reading ', country)
  
  dt <- fileName(dir, country, pfx, sfx) %>% fread
  
}

workers <- lapply(locations, appendFiles, dir=emp.dir, pfx='emp', sfx='_mean.csv') %>% rbindlist
workers[, V1 := NULL]

#merge pops to your data
dt <- merge(turnover, workers, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))
dt <- merge(dt, levels, by=c("location_id", 'ihme_loc_id'))

#***********************************************************************************************************************

# ---SCRAP--------------------------------------------------------------------------------------------------------------

#***********************************************************************************************************************