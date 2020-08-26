#----HEADER----------------------------------------------------------------------------------------------------------------------
# Author: NAME
# Date: 01/08/2016
# Purpose: Launch the parallelized calculation of ozone exp/RR/PAF
# source("FILEPATH.R", echo=T)
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {

  j_root <- "ADDRESS"
  h_root <- "ADDRESS"

} else {

  j_root <- "ADDRESS"
  h_root <- "ADDRESS"

}

if (Sys.getenv('SGE_CLUSTER_NAME') == "ADDRESS" ) {

  project <- "-P ADDRESS "
  sge.output.dir <- "-o FILEPATH -e FILEPATH"
  #sge.output.dir <- "" # toggle to run with no output files

} else {

  project <- "-P ADDRESS "
  sge.output.dir <- "-o FILEPATH -e FILEPATH"
  #sge.output.dir <- "" # toggle to run with no output files

}

	# System settings
  rshell <- "FILEPATH.sh"
	prep.script <- paste0(h_root, "FILEPATH.R")
	rscript <- paste0(h_root, "FILEPATH.R")

	# Job settings.
	location_set_version_id <- 420
	exp.grid.version <- "11"
	draws.required <- 1000 #number of draws to create to show distribution, default is 1000 - do less for a faster run
	prep.environment <- F

	output.version <- 19 # updating to 3-year rolling mean

	# load packages, install if missing
	pacman::p_load(data.table, magrittr, ini)

	# load functions
	central.function.dir <- file.path(h_root, "FILEPATH")
	ubcov.function.dir <- file.path(j_root, 'FILEPATH')
	file.path(central.function.dir, "FILEPATH.R") %>% source
	file.path(central.function.dir, "FILEPATH.R") %>% source
	file.path(ubcov.function.dir, "FILEPATH.r") %>% source
	file.path(j_root, 'FILEPATH.R') %>% source

	"%ni%" <- Negate("%in%")

#----PREP------------------------------------------------------------------------------------------------------------------------
# Prep save directories
out.paf.dir <- file.path(j_root, "FILEPATH", output.version)
out.exp.dir <- file.path(j_root, "FILEPATH", output.version)
out.tmp <- file.path("FILEPATH", output.version)
out.rr.dir <- file.path(j_root, "FILEPATH", output.version)
directory.list <- c(out.paf.dir, out.tmp, out.exp.dir,out.rr.dir)

# Prep directories
for (directory in directory.list) {

  dir.create(paste0(directory, "FILEPATH"), recursive = TRUE)
  dir.create(paste0(directory, "FILEPATH"), recursive = TRUE)

}

# Get the list of most detailed GBD locations
locs <- get_location_hierarchy(location_set_version_id)
countries <- locs[is_estimate==1 & most_detailed==1, unique(location_id)]
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]
#********************************************************************************************************************************

#----LAUNCH LOAD-----------------------------------------------------------------------------------------------------------------
# Launch job to prep the clean environment if necessary
if (prep.environment != FALSE) {
  
  args <- paste(draws.required,output.version)
  mem <- "-l m_mem_free=0.5G"
  fthread <- "-l fthread=1"
  runtime <- "-l h_rt=00:05:00"
  archive <- "-l archive=TRUE" # or "" if no jdrive access needed
  name <- "load_clean_environment_ozone"
  jname <- paste0("-N ",name)
  
  system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,prep.script,args))

  # Prep hold structure
  hold.text <- paste0("-hold_jid ", name)

} else {

  hold.text <- ""

}

#----LAUNCH CALC-----------------------------------------------------------------------------------------------------------------
#Launch the jobs to calculate ozone PAFs and exp
#with a check to see if they have already been created
files <- list.files(paste0(out.tmp,"FILEPATH"))

for (country in countries) {
	if(paste0("FILEPATH",country,"FILEPATH.csv") %ni% files){
	  
	  threads <- 3
	  
	  args <- paste(country,
	                exp.grid.version,
	                output.version,
	                draws.required,
	                threads)
	  
	  big_countries <- c("23","71","44979","101","44973","36","97","524","38","139","171","519","152") #these need extra resources
	  
	  if(country %in% big_countries){
	    mem <- "-l m_mem_free=15G"
	    fthread <- paste0("-l fthread=",threads)
	    runtime <- "-l h_rt=02:00:00" # most take less than 30 m
	    archive <- "-l archive=TRUE" # or "" if no jdrive access needed
	    jname <- paste0("-N calc_paf_ozone_",country)
	  }else{
	    mem <- "-l m_mem_free=2G"
	    fthread <- paste0("-l fthread=",threads)
	    runtime <- "-l h_rt=00:20:00" # most take less than 1 m
	    archive <- "-l archive=TRUE" # or "" if no jdrive access needed
	    jname <- paste0("-N calc_paf_ozone_",country)
	  }
	 
	  
	  system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,hold.text,rshell,rscript,args))
	  

		}

	}