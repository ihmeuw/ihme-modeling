#----HEADER----------------------------------------------------------------------------------------------------------------------
# Date: 07/10/2015
# Purpose: Execute parallelized calculation of country RRs for HAP 

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "" 
  h_root <- ""
  arg <- commandArgs()[-(1:3)]                  # First args are for unix use only
  #arg <- c("USA", "power2", "2", 1000, 6)      #toggle targetted run
  output.dir <- file.path("")
  
  
} else { 
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- c("AFG", "power2", "3", 1000, 1)
  output.dir <- file.path("")
}

print(arg)

# set working directories
home.dir <- file.path(j_root, "")
#setwd(home.dir)

this.country <- arg[1]
rr.functional.form <- arg[2]
output.version <- arg[3]
draws.required <- as.numeric(arg[4])
cores.provided <- as.numeric(arg[5])

message(this.country, rr.functional.form, output.version, draws.required, cores.provided)

# r configuration
options(scipen=10) # disable scientific notation because it annoys me, set to display ten digits

# load packages, install if missing
pacman::p_load(data.table, magrittr, parallel, stringr, reshape2)
#******************************************************************************************************************************** 
  
#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
##function lib##
#RR functions#
rr.function.dir <- file.path(h_root, '') 
file.path(rr.function.dir, "functional_forms.R") %>% source
fobject <- get(rr.functional.form)  

#AiR HAP functions#
hap.function.dir <- file.path(h_root, '')
# this pulls the miscellaneous helper functions for air pollution
file.path(hap.function.dir, "misc.R") %>% source()

#general functions#
get.locations.dir <- file.path(j_root, "")
file.path(get.locations.dir, "get_location_metadata.R") %>% source()
#***********************************************************************************************************************
  # LOAD IN THE CLEAN ENVIRONMENT HERE, THEN SUBSET TO COUNTRY/YEAR
  exposure.dir <- file.path(home.dir, "")
  out.environment <- paste0(exposure.dir, "/clean.Rdata") #this file will be read in by each parallelized run in order to preserve draw covariance

  ##Load in ambient exposure draws to subtract off of HAP exposure **Added 5/22/17** Decided to switch back to not subtracting off ambient concentrations due to negative HAP exposure issues
  # amb.dt <- paste0(amb.dir, "/", this.country, ".csv") %>% fread
  # cols <- paste0("draw_", seq(1, 1000, 1))
  # amb.dt <- amb.dt[, c("year", cols), with=F]
  # amb.dt <- melt(amb.dt, id=c("year"), variable.factor = F)
  # amb.dt[,c("sex","draw") := as.data.table(str_split_fixed(variable, fixed("_"), 2)[,1:2])]
  # amb.dt <- amb.dt[, c("year", "value", "draw"), with=F]
  # setnames(amb.dt, "year", "year_id") 
  
  #objects exported:
  #age.cause - list of all age-cause pairs currently being calculated
  #all.rr - compiled list of all the RR curves for the ages/causes of interest
  load(out.environment)
  HAP.country.exp <- HAP.global.exp[ihme_loc_id==this.country]
  #HAP.country.exp <- merge(HAP.country.exp, amb.dt, by= c("year_id", "draw"), all.x = T)
  #HAP.country.exp[, exposure := exposure - value]
  print("starting loop over years")
  
years <- unique(HAP.global.exp$year_id)
#years <- c(1990, 1995, 2000, 2005, 2010, 2013, 2015, 2016)
for(year in years) {
    HAP.country.year.exp <- HAP.country.exp[year_id==year]

  # Calculate Mortality PAFS using custom function
  RR.mort <- mclapply(1:nrow(age.cause),
                           FUN=calculateRRs,
                           exposure.object = HAP.country.year.exp,
                           metric.type = "yll",
                           sex.specific = T,
                           function.cores = 1,
                           draws.required = draws.required,
                           mc.cores = cores.provided) %>% rbindlist

  # Call to a custom function to do some final formatting and generate a lite summary file with mean/CI
  mortality.outputs <- formatAndSummRR(RR.mort, "yll", draws.required, year)

  # Save Mortality PAFs/RRs
  print(paste0("Saving:", "yll_", this.country, "- Year:", year))
  write.csv(mortality.outputs[["summary"]], paste0(output.dir, "/", output.version, "/summary/yll_", this.country, "_", year, ".csv"))
  write.csv(mortality.outputs[["lite"]], paste0(output.dir, "/", output.version, "/lite/yll_", this.country, "_", year, ".csv"))
  write.csv(mortality.outputs[["draws"]], paste0(output.dir, "/", output.version, "/draws/yll_", this.country, "_", year, ".csv"))
}

for(year in years) {
  HAP.country.year.exp <- HAP.country.exp[year_id==year]
  # Calculate Morbidity PAFS using custom function
  RR.morb <- mclapply(1:nrow(age.cause), 
                      FUN=calculateRRs, 
                      exposure.object = HAP.country.year.exp, 
                      metric.type = "yld", 
                      sex.specific = T,
                      function.cores = 1,
                      draws.required = draws.required,
                      mc.cores = cores.provided) %>% rbindlist
  
  # Call to a custom function to do some final formatting and generate a lite summary file with mean/CI
  morbidity.outputs <- formatAndSummRR(RR.morb, "yld", draws.required, year)
  
  # Save Mortality PAFs/RRs
  print(paste0("Saving:", "yld_", this.country, "- Year:", year))
  write.csv(morbidity.outputs[["summary"]], paste0(output.dir, "/", output.version, "/summary/yld_", this.country,  "_", year, ".csv")) 
  write.csv(morbidity.outputs[["lite"]], paste0(output.dir, "/", output.version, "/lite/yld_", this.country, "_", year, ".csv"))
  write.csv(morbidity.outputs[["draws"]], paste0(output.dir, "/", output.version, "/draws/yld_", this.country, "_", year, ".csv"))
}