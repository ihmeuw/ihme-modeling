#----HEADER-------------------------------------------------------------------------------------------------------------

# Project: RF: air_pm
# Purpose: Calculate PAFs from air PM for a given country year

#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  
  if (length(arg)==0) {
    #toggle for targeted run on cluster
    arg <- c("GLOBAL", #location
             "16", #rr data version
             "power2_simsd_source", #rr model version
             "power2", #rr functional form
             "23", #exposure grid version
             23, #output version
             1000, #draws required
             5) #number of cores to provide to parallel functions
  }
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  arg <- c("MEX_4650", #location
           "14", #rr data version
           "power2_simsd_source", #rr model version
           "power2", #rr functional form
           "12", #exposure grid version
           5, #output version
           1000, #draws required
           1) #number of cores to provide to parallel functionss
  
}

# load packages, install if missing
pacman::p_load(data.table, fst, ggplot2, magrittr)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# set project values
location_set_version_id <- 149

# Set parameters from input args
country <- arg[1]
rr.data.version <- arg[2]
rr.model.version <- arg[3]
rr.functional.form <- arg[4]
exp.grid.version <- arg[5]
output.version <- arg[6]
draws.required <- as.numeric(arg[7])
cores.provided <- as.numeric(arg[8])
cores.provided <- ifelse(cores.provided>40, 40, cores.provided) #give the bigger jobs (50) some buffer memory

# Memory settings

if (country == "62") {
  
  cause.cores <- 5
  year.cores <- 1
  years <- c(1990, 1995, 2000, 2005, 2010, 2013, 2015, 2016) #removing unnecessary years

} else {
  
  years <- c(1990, 1995, 2000, 2005, 2010, 2011, 2013, 2014, 2015, 2016)
  cause.cores <- ifelse(cores.provided>1, cores.provided/length(years), 1) %>% as.integer
  cause.cores <- ifelse(cause.cores>1, cause.cores, 1)
  year.cores <- ifelse(cores.provided>1, length(years), 1)
  
}

message("splitting over #", year.cores, " for each year, and #", cause.cores, " for each age/cause")

#***********************************************************************************************************************

#----IN/OUT-------------------------------------------------------------------------------------------------------------
##in##
exp.grid.dir <- file.path("FILEPATH", exp.grid.version)
rr.dir <- file.path(home.dir, 'FILEPATH', paste0(rr.data.version, rr.model.version))
tmrel.dir <- file.path(home.dir, 'FILEPATH')

##out##
out.paf.tmp <-  file.path("FILEPATH", output.version)
out.exp.tmp <-  file.path("FILEPATH", output.version)
out.paf.dir <- file.path(home.dir, 'FILEPATH', output.version)
out.exp.dir <- file.path(home.dir, 'FILEPATH', output.version)

#Exposure
dir.create(file.path(out.exp.dir, "summary"), recursive = T)
dir.create(file.path(out.exp.tmp, "final_draws"), recursive = T)

#PAFs
dir.create(file.path(out.paf.dir, "summary"), recursive = T)
dir.create(file.path(out.paf.tmp, "draws"), recursive = T)
#***********************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#PAF functions#
paf.function.dir <- file.path(h_root, 'FILEPATH')
file.path(paf.function.dir, "paf_helpers.R") %>% source

#RR functions#
rr.function.dir <- file.path(h_root, 'FILEPATH')
file.path(rr.function.dir, "functional_forms.R") %>% source
fobject <- get(rr.functional.form)

#AiR PM functions#
air.function.dir <- file.path(h_root, 'FILEPATH')
# this pulls the miscellaneous helper functions for air pollution
file.path(air.function.dir, "misc.R") %>% source

#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# this pulls the current locations list
file.path(central.function.dir, "get_locations.R") %>% source

#ubcov functions#
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source

#***********************************************************************************************************************

#----PREP DATA----------------------------------------------------------------------------------------------------------
# Make a list of all cause-age pairs that we have.
age.cause <- ageCauseLister(full.age.range = T, gbd.version="GBD2016", lri.version="single")

#Get the list of most detailed GBD locations
locs <- get_location_hierarchy(location_set_version_id)
if (country != "EU" & country != "GLOBAL") {
  
  locs[location_id == country, ihme_loc_id]
  
} else {
  this.iso3 <- country
}


# Prep gridded exposure dataset
#load the Rdata, bring in an object "exp" with exposure draws

if (exp.grid.version < 22) {
  
  paste0(exp.grid.dir, "/", country, ".Rdata") %>%
    file.path %>%
    load(envir = globalenv())
  
} else if (country=="GLOBAL") {
  
  #combine all countries to create a global gridded dataset with draws
  all.files <- list.files(exp.grid.dir, full.names=T, include.dirs = F)
  
  readFx <- function(file, multi) {
    
    message(file)
    
    if(multi==TRUE) {
      
      size <- file.size(file) %>% humanReadable(units="Gib")
      
      if(size>4) {
        message(file, "=TOO BIG")
        dt <- file
      } else {
        
        dt <- read.fst(file, as.data.table = TRUE)
        dt <- unique(dt, by=c('long', 'lat', 'year'))
        
      }
      
    } else {
      
      size <- file.size(file) %>% humanReadable(units="Gib")
      
      if(size<4) {
        message(file, "=TOO small")
        dt <- file
      } else {
        
        dt <- read.fst(file, as.data.table = TRUE)
        dt <- unique(dt, by=c('long', 'lat', 'year'))
        
      }
      
    }
    
    return(dt)
    
  }
  
  exp <- mclapply(all.files, readFx, multi=TRUE, mc.cores=cores.provided)
  big <- sapply(exp, function(x) length(x)==1)
  big <- lapply(all.files, readFx)
  small <- sapply(exp, function(x) length(x)>1)
  dt <- list(big, small) %>% rbindlist
  stop()
  exp <- unique(exp, by=c('long', 'lat', 'year')) #remove duplicate grids
  
} else {
  
  exp <- paste0(exp.grid.dir, "/", country, ".fst") %>%
    file.path %>%
    read.fst(as.data.table=TRUE)
  
}

# Potential cleanup
exp <- exp[!is.na(pop) & pop > 0, ] # Get rid of grids that have missing/0 pop 

# Prep the RR curves into a single object, so that we can loop through different years without hitting the files extra times.
all.rr <- lapply(1:nrow(age.cause), prepRR, rr.dir=rr.dir)
#***********************************************************************************************************************

#----CALC and SAVE------------------------------------------------------------------------------------------------------
yearWrapper <- function(this.year,
                        ...) {
  
  #subset exposure
  this.exp <- exp[year==this.year]
  
  message("calculating PAF and saving results for the year ", this.year)
  
  # Calculate Mortality PAFS using custom function
  out.paf.mort <- mclapply(1:nrow(age.cause),
                           FUN=calculatePAFs,
                           exposure.object = this.exp,
                           rr.curves = all.rr,
                           metric.type = "yll",
                           function.cores = 1,
                           mc.cores = cause.cores)
  
  # Call to a custom function to do some final formatting and generate a lite summary file with mean/CI
  mortality.outputs <- formatAndSummPAF(out.paf.mort, "yll", draws.required)
  
  # Save Mortality PAFs/RRs
  write.csv(mortality.outputs[["summary"]],
            file.path(out.paf.dir, "summary",
                      paste0("paf_yll_", country, "_", this.year, ".csv")))
  # write.csv(mortality.outputs[["draws"]], mort.draw.file)
  
  # Calculate Morbidity PAFS using custom function
  out.paf.morb <- mclapply(1:nrow(age.cause),
                           FUN=calculatePAFs,
                           exposure.object = this.exp,
                           rr.curves = all.rr,
                           metric.type = "yld",
                           function.cores = 1,
                           draws.required,
                           mc.cores = cause.cores)
  
  
  # Call to a custom function to do some final formatting and generate a lite summary file with mean/CI40/
  morbidity.outputs <- formatAndSummPAF(out.paf.morb, "yld", draws.required)
  
  #Save Morbidity PAFs
  write.csv(morbidity.outputs[["summary"]],
            file.path(out.paf.dir, "summary",
                      paste0("paf_yld_", country, "_", this.year, ".csv")))
  # write.csv(morbidity.outputs[["draws"]], morb.draw.file)
  
  
  #combine the different pafs and then do prep/formatting for their dalynator run
  out.paf <- rbind(mortality.outputs[["draws"]],
                   morbidity.outputs[["draws"]])
  
  out.paf[, iso3 := this.iso3]
  out.paf[, location_id := country]
  out.paf[, year_id := this.year]
  out.paf[, measure_id := 18]
  out.paf[, risk := "air_pm"]
  out.paf[, acause := cause]
  
  # expand cvd_stroke to include relevant subcauses in order to prep for merge to YLDs, using your custom find/replace function
  # first supply the values you want to find/replace as vectors
  old.causes <- c('cvd_stroke')
  replacement.causes <- c('cvd_stroke_cerhem',
                          "cvd_stroke_isch")
  
  # then pass to your custom function
  out.paf <- findAndReplace(out.paf,
                            old.causes,
                            replacement.causes,
                            "acause",
                            "acause",
                            TRUE) #set this option to be true so that rows can be duplicated in the table join (expanding the rows)
  
  # now replace each cause with cause ID
  out.paf[, cause_id := acause] #create the variable
  # first supply the values you want to find/replace as vectors
  cause.codes <- c('cvd_ihd',
                   'cvd_stroke_cerhem',
                   "cvd_stroke_isch",
                   "lri",
                   'neo_lung',
                   'resp_copd')
  
  cause.ids <- c(493,
                 496,
                 495,
                 322,
                 426,
                 509)
  
  # then pass to your custom function
  out.paf <- findAndReplace(out.paf,
                            cause.codes,
                            cause.ids,
                            "cause_id",
                            "cause_id")
  
  out.paf <- out.paf[, c("risk",
                         'type',
                         "age_group_id",
                         "iso3",
                         "location_id",
                         "year_id",
                         "acause",
                         "cause_id",
                         c(paste0("paf_", 0:(draws.required-1)))),
                     with=F]
  
  for (sex.id in c(1,2)) {
    
    out.paf[, sex_id := sex.id]
    
    write.csv(out.paf[type=="yll"],
              paste0(out.paf.tmp, "/draws/paf_yll_",
                     country, "_", this.year, "_", sex.id, ".csv"))
    
    write.csv(out.paf[type=="yld"],
              paste0(out.paf.tmp, "/draws/paf_yld_",
                     country, "_", this.year, "_", sex.id, ".csv"))
    
  }
  
  return(dim(out.paf))
  
  gc()
  
}

temp <- mclapply(years,
                 yearWrapper,
                 mc.cores = year.cores)
#***********************************************************************************************************************

#----EXPOSURE-----------------------------------------------------------------------------------------------------------
##EXPOSURE##
# Save average PM2.5 at the country level
# Prep datasets
out.exp.summary <- as.data.frame(matrix(as.integer(NA), nrow=1, ncol=3))

# calculate population weighted draws
calib.draw.colnames <- c(paste0("draw_",1:draws.required))

out.exp <- exp[, lapply(.SD, weighted.mean, w=pop), .SDcols=calib.draw.colnames, by=year]

# calculate mean and CI for summary figures
out.exp[, exp_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=calib.draw.colnames]
out.exp[, exp_mean := apply(.SD, 1, mean), .SDcols=calib.draw.colnames]
out.exp[, exp_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=calib.draw.colnames]

#output pop-weighted draws
write.csv(out.exp, paste0(out.exp.tmp, "/final_draws/", this.iso3, ".csv"))

#also save version with just summary info (mean/ci)
write.csv(out.exp[, -calib.draw.colnames, with=F],
          paste0(out.exp.dir, "/summary/", this.iso3, ".csv"))
#***********************************************************************************************************************