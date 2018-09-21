#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Execute parallelized calculation of country RRs for SHS
#          Uses country/year SHS exposure from covariates db and the IER curve
# Run:     source("FILEPATH/calc_IER_RR.R", echo=T)
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

# settings from arguments
args <- commandArgs()[-(1:3)]
print(args)
this.location <- args[1]
print(this.location)
rr.functional.form <- args[2]
print(rr.functional.form)
output.version <- args[3]
print(output.version)
draws.required <- as.numeric(args[4])
print(draws.required)
cores.provided <- ifelse((as.numeric(args[5])/2)>0, 
                         (as.numeric(args[5])/2), 
                         1) 
print(cores.provided)

# other settings
years <- seq(1990, 2016)

# r configuration
options(scipen=10)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# load packages, install if missing
pacman::p_load(dplyr, data.table, magrittr, parallel, stringr, reshape2)
#***********************************************************************************************************************
  

#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
# PAF functions
paf.function.dir <- file.path(home.dir, "FILEPATH")
file.path(paf.function.dir, "paf_helpers.R") %>% source  

# RR functions
rr.function.dir <- file.path(home.dir, "FILEPATH")
file.path(rr.function.dir, "functional_forms.R") %>% source
fobject <- get(rr.functional.form)  

# SHS functions
shs.function.dir <- file.path(home.dir, "FILEPATH")
# pulls the miscellaneous helper functions
file.path(shs.function.dir, "_lib", "misc.R") %>% source

# general functions
central.function.dir <- file.path(home.dir, "FILEPATH")
# pulls the general misc helper functions
file.path(central.function.dir, "dependencies.R") %>% source
# locations
file.path(j_root, "FILEPATH/get_location_metadata.R") %>% source
# other tools for querying db
file.path(central.function.dir, "db_tools.R") %>% source
#***********************************************************************************************************************


#----CALC---------------------------------------------------------------------------------------------------------------
# LOAD IN THE CLEAN ENVIRONMENT HERE, THEN SUBSET TO COUNTRY/YEAR
output.dir <- file.path("FILEPATH", output.version)
exposure.dir <- file.path(home.dir, "FILEPATH")
out.environment <- file.path(home.dir, "FILEPATH", paste0(output.version, "_clean.Rdata")) #this file will be read in by each parallelized run in order to preserve draw covariance
#objects exported:
#SHS.global.exp = file containing HAP PM2.5 exposure estimates for all country years
#ot.genesis = 1000 draws of otitis media RR (comes from literature, not IER curve)
#age.cause - list of all age-cause pairs currently being calculated
#all.rr - compiled list of all the RR curves for the ages/causes of interest
load(out.environment)

# get the list of most detailed GBD locations
locations <- get_location_metadata(gbd_round_id=4, version_id=149)
# set iso3 using the passed location ID, for convenience
this.country <- locations[location_id==this.location, ihme_loc_id]

yearWrapper <- function(this.year) {
  message("working on ", this.year)
  SHS.country.exp <- SHS.global.exp[location_id==this.location & year_id==this.year]
  # Calculate Mortality PAFS using custom function
  RR.mort <- lapply(1:nrow(age.cause), 
                      FUN               = calculateRRs, 
                      exposure.object   = SHS.country.exp, 
                      metric.type       = "yll",
                      function.cores    = 1,
                      draws.required    = draws.required) %>%
    rbindlist %>% # Call to a custom function to do some final formatting and generate a lite summary file with mean/CI
    formatAndSummRR(metric.type         = "yll", 
                    ot.draws            = ot.genesis,
                    breast.cancer.draws = breast.cancer.draws,
                    diabetes.draws      = diabetes.draws)
  # Calculate Morbidity PAFS using custom function
  RR.morb <- lapply(1:nrow(age.cause), 
                      FUN               = calculateRRs, 
                      exposure.object   = SHS.country.exp, 
                      metric.type       = "yld",
                      function.cores    = 1,
                      draws.required    = draws.required) %>%
    rbindlist %>% # Call to a custom function to do some final formatting and generate a lite summary file with mean/CI
    formatAndSummRR(metric.type         = "yld", 
                    ot.draws            = ot.genesis,
                    breast.cancer.draws = breast.cancer.draws,
                    diabetes.draws      = diabetes.draws)
#***********************************************************************************************************************


#----FORMAT-------------------------------------------------------------------------------------------------------------
# final formatting in order to get RRs in shape for running save results and uploading for the DALYnator
# this is also where I will bring in the RRs that have been calculated from literature and save them as well
# these will be saved as a single file for each location
out.rr <- rbind(RR.mort[["draws"]], RR.morb[["draws"]], RR.mort[["tmrel"]], RR.morb[["tmrel"]])
  setkeyv(out.rr, c('cause', 'age'))
  
#note the metric in the specified manner
out.rr[, mortality := ifelse(metric=="yll", 1, 0)]
out.rr[, morbidity := ifelse(metric=="yld", 1, 0)]

# note other variables relative to this job
out.rr[, iso3 := this.country]
out.rr[, location_id := this.location]
out.rr[, year_id := this.year] 
out.rr[, risk := "smoking_shs"]
out.rr[, acause := cause]

# now fix the causes

# expand cvd_stroke to include relevant subcauses in order to prep for merge to YLDs, using your custom find/replace function
# first supply the values you want to find/replace as vectors
old.causes <- c("cvd_stroke")   
replacement.causes <- c("cvd_stroke_cerhem", 
                        "cvd_stroke_isch")

# then pass to your custom function
out.rr <- findAndReplace(out.rr,
                         old.causes,
                         replacement.causes,
                         "acause",
                         "acause",
                         TRUE) #set this option to be true so that rows can be duplicated in the table join (expanding the rows)

# now replace each cause with cause ID
out.rr[, cause_id := acause] #create the variable
# first supply the values you want to find/replace as vectors
cause.codes <- c('cvd_ihd',
                 'cvd_stroke_cerhem', 
                 "cvd_stroke_isch",
                 "lri",
                 'neo_lung',
                 'otitis',
                 'resp_copd',
                 "neo_breast",
                 "diabetes")
cause.ids <- c(493,
               496,
               495,
               322,
               426,
               329,
               509,
               429,
               587)

# then pass to custom function
out.rr <- findAndReplace(out.rr,
                        cause.codes,
                        cause.ids,
                        "cause_id",
                        "cause_id")

#cleanup
out.rr[, modelable_entity_id := 9027]
out.rr[, location_id := as.integer(location_id)]
out.rr <- out.rr[, c("modelable_entity_id",
                     "location_id",
                     "year_id",
                     "age_group_id",
                     "cause_id",
                     "mortality",
                     "morbidity",
                     "parameter",
                     c(paste0("rr_", 0:(draws.required - 1)))), with=FALSE]

#save one file for each sex
  for (sex.id in c(1,2)) {
    
    out.rr[, sex_id := sex.id]
    write.csv(out.rr, 
              paste0(output.dir, "FILEPATH/rr_", 
                     this.location, "_", this.year, "_", sex.id, ".csv"), row.names=FALSE)
    
  }

  return(out.rr)

}

#loop over years and calculate, then save summary/lite files for diagnostic use
all.output <- lapply(years, yearWrapper)
#***********************************************************************************************************************