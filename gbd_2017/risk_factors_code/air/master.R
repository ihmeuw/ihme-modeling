#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Launch the parallelized calculation of proportional PAFs for ambient, hap, and total PM by country year
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

user <- "name"

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {

  j_root <- "FILEPATH"
  h_root <- paste0("FILEPATH", user, "/")

} else {

  j_root <- "FILEPATH"
  h_root <- "FILEPATH"

}


  sge.output.dir <- paste0(" -o FILEPATH -e FILEPATH ")
  #sge.output.dir <- "" # toggle to run with no output files



# load packages, install if missing
#pacman::p_load(data.table, magrittr)
require(data.table)
require(magrittr)
require(ini)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)


# set project values
location_set_version_id <- 397

# Settings
rr.data.version <- 33
rr.model.version <- "power2_simsd_source_priors" #for diabetes use "power2_simsd_source_phaseII"
rr.functional.form <- "power2"     #for diabetes use "phaseII"
exp.grid.version <- 33
draws.required <- 1000
years <- c(1990:2017)

# version history
output.version <- 1 # first version of air PM PAFs, using the power2 model with updated 2015 data (rr data v2)
output.version <- 2 # second version of air PM PAFs, updated exposure shapefile to include india urb/rural
output.version <- 3 # should be the same as v2 but need a rerun due to error in a function
output.version <- 4 # new version, using an IER with power2 model with source specific uncertainty term and rr data v5
output.version <- 5 #new test version, should match v5 but also generate summ exposure
output.version <- 6 #new version using grid v13 which should has exp(log()) created draws
output.version <- 7 #rerun of version 4/5, summ exposure was messed up (used v11 exposure)
output.version <- 8 #rerun of v6, using exp v13 (some of them still arent saved so i use a try call to submit)
output.version <- 9 #new run with the updated v7 IER and v14 exposure (uses the logspace draws and new version from mike)
output.version <- 10 #run using v15 exposure (updated version from NAME but with draws in normal)
output.version <- 11 #logspace draws in parallelized version. should match v9
output.version <- 12 #should match GBD2015 final but include annual pafs
output.version <- 13 #GBD 2016 first run. using new exp data but old IER
output.version <- 14 #GBD 2016 second run. no change
output.version <- 15 #using location ids instead because of problem with china iso3s in version 171 (also testing new ages)
output.version <- 16 #trying just nigeria for review week w/ 2015 IER
output.version <- 17 #trying all
output.version <- 18 #trying just nigeria 2015 for review week comparison
output.version <- 19 #running just exp for covariate
output.version <- 20 #first PAF run for GBD2016, targetting DALYnator upload
output.version <- 21 #new data from NAME
output.version <- 22 #run using rural mask for HAP
output.version <- 23 #fixing a problem with TMREL for GBD2016 final
output.version <- 24 #rerunning due to file problems, should match 23
output.version <- 25 #NAME's first run GBD 2017 uses 2016 exp and IERs with 2017 shapefiles
output.version <- 26 #2016 exp and 2016 IERs with 2017 shapefiles
output.version <- 27 #2016 exp and IER with 2017 shapefile, fixed Noroway and Russia for cov upload
output.version <- 28 #New 2017 exposure and 2017 IER updates
output.version <- 29 #implementing proportional approach
output.version <- 30 #NAME's run of proportional approach (diabetes using 2-phase model and all others using old model, version 28)
output.version <- 31 #uses power 2 for all except diabetes which uses phase II (had to launch everything twice)
output.version <- 32  #same as above but fixing things for save results
output.version <- 33 #MORE save results errors
output.version <- 34 #Updated IER (shs data) and new exposure for first submission
output.version <- 35 #new air_pm exposure, first submission
output.version <- 36 #new exposure method for accurate U.I.s, PAFs at country level(not grid), new IERs with informative priors
output.version <- 37 #updated data from smoking team for IERs
output.version <- 38 #removed duplicates from IER


###in/out###
##in##
code.dir <- file.path(h_root, 'FILEPATH')
calc.script <- file.path(h_root,"FILEPATH/01_calc.R")
cataract.calc.script <- file.path(h_root,"FILEPATH/calc_cataract.R")
r.shell <- "FILEPATH"


##out##
out.dir <-  file.path("FILEPATH")

#********************************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(h_root, "FILEPATH/")
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source()
# this pulls the current locations list
file.path(central.function.dir, "get_locations.R") %>% source()
#ubcov functions#
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH/db_tools.r") %>% source()

#--------------Launch Cataract computation--------------------------------------------------------------------------------------
slots <- 20
mem <- slots*2

for(year in years){

  message(paste("launching cataracts PAF calc for year",year))

  # Launch jobs
  args <- paste(output.version,
                draws.required,
                year)

  # Prepare job settings
  jname <- paste0("air_paf_cataract_",year)

  # Create submission call
  sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")

  # Run
  paste(sys.sub, r.shell, cataract.calc.script, args) %>% system

}


#----LAUNCH CALC--------------------------------------------------------------------------------------------------------


# Get the list of most detailed GBD locations
locs <- get_location_hierarchy(location_set_version_id)
locations <- locs[most_detailed==1, location_id] %>% unique %>% sort

#see if files have already been saved
files <- list.files(path=paste0(out.dir,"/air"), pattern=".csv", full.names=T)

for(year in years){
for (country in locations) {

  if (length(grep(paste0("_",country,"_",year), files))<32) {
    slots <- 5
    mem <- slots*2
    cores.provided <- slots #try running again with a lot of memory but few slots. will be slow but should get it done

    message("launching PAF calc for loc ", country, " and year ", year, "\n --using ", slots, " slots and ", mem, "GB of mem")

    # Launch jobs
    args <- paste(country,
                  year,
                  rr.data.version,
                  rr.model.version,
                  rr.functional.form,
                  exp.grid.version,
                  output.version,
                  draws.required,
                  cores.provided)

    # Prepare job settings
    jname <- paste0("air_paf_", country, "_", year, "_", rr.functional.form)

    # Create submission call
    sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")

    # Run
    #paste(sys.sub, r.shell, conda.path, env.name, calc.script, args) %>% system
    paste(sys.sub, r.shell, calc.script, args) %>% system

  }
  }
}




#********************************************************************************************************************************
