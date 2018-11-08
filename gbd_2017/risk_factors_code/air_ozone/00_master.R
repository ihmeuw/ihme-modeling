#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Launch the parallelized calculation of ozone exp/RR/PAF
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {

  j_root <- "FILEPATH"
  h_root <- "FILEPATH"

} else {

  j_root <- "FILEPATH"
  h_root <- "FILEPATH"

}

if (Sys.getenv('SGE_CLUSTER_NAME') == "prod" ) {


} else {

}

	# System settings
  cores.provided <- 6 #number of cores to request (this number x2 in order to request slots, which are a measure of computational time that roughly equal 1/2core)
  rshell <- "FILEPATH/shell_singularity.sh"
	prep.script <- "FILEPATH/01_load.R"
	rscript <- "FILEPATH/02_calculate.R"

	# Job settings.
	location_set_version_id <- 398
	exp.grid.version <- "7"
	draws.required <- 1000 #number of draws to create to show distribution, default is 1000 - do less for a faster run
	prep.environment <- T


	# Versioning
	#output.version <- 2 #test version for GBD2015 and to create exposures for GBD2013
	#output.version <- 3 #first run with exposure extrapolated to GBD2015
	#output.version <- 4 #first run with india urban/rural and AROC exposure
	output.version <- 5 #typo in ozone RR, rerunning with fix
	output.version <- 6  #first version for GBD2016
	output.version <- 7 #first run GBD2017, updated RR
	output.version <- 8 #some missing countries and years
	output.version <- 9 #still working on missing countries and years
	output.version <- 10 #fixing error with measure id for first gbd17 exp upload
	output.version <- 11 #new GBD 2017 model
	output.version <- 12 #new GBD 2017 exp with 2016 RR
	output.version <- 13 #new gbd 2017 exp with updated RR and tmrel based on meta-analysis and turner
	output.version <- 14 #updated exposure model 
	output.version <- 15 #re-running to save draws of exposure

	# load packages, install if missing
	pacman::p_load(data.table, magrittr, ini)

	##function lib##
	#general functions#
	central.function.dir <- "FILEPATH"
	ubcov.function.dir <- "FILEPATH"
	# this pulls the general misc helper functions
	file.path(central.function.dir, "misc.R") %>% source
	# other tools created by covs team for querying db (personal version)
	file.path(central.function.dir, "db_tools.R") %>% source
	# other tools created by covs team for querying db
	file.path(ubcov.function.dir, "FILEPATH/db_tools.r") %>% source
	# central functions
	file.path("FILEPATH/get_population.R") %>% source
	#custom fx
	"%ni%" <- Negate("%in%") # create a reverse %in% operator

#********************************************************************************************************************************

#----PREP------------------------------------------------------------------------------------------------------------------------
# Prep save directories
out.paf.dir <- file.path("FILEPATH", output.version)
out.exp.dir <- file.path("FILEPATH", output.version)
out.tmp <- file.path("FILEPATH", output.version)
out.rr.dir <- file.path("FILEPATH", output.version)
directory.list <- c(out.paf.dir, out.tmp, out.exp.dir,out.rr.dir)

# Prep directories
for (directory in directory.list) {

  dir.create(paste0(directory, "/draws"), recursive = TRUE)
  dir.create(paste0(directory, "/summary"), recursive = TRUE)

}

# Get the list of most detailed GBD locations
locs <- get_location_hierarchy(location_set_version_id)
countries <- locs[is_estimate==1 & most_detailed==1, unique(location_id)]
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]
#********************************************************************************************************************************

#----LAUNCH LOAD-----------------------------------------------------------------------------------------------------------------
# Launch job to prep the clean environment if necessary
if (prep.environment != FALSE) {

  # Launch job
  jname <- paste0("load_clean_environment")
  sys.sub <- paste0("qsub ", project, sge.output.dir, "-N ", jname, " -pe multi_slot ", 1, " -l mem_free=", 2, "G")
  args <- paste(draws.required)

  system(paste(sys.sub, rshell, prep.script, args))

  # Prep hold structure
  hold.text <- paste0(" -hold_jid ", jname)

} else {

  hold.text <- ""

}
#********************************************************************************************************************************

#----LAUNCH CALC-----------------------------------------------------------------------------------------------------------------
#Launch the jobs to calculate ozone PAFs and exp
#with a check to see if they have already been created
files <- list.files(paste0(out.tmp,"/draws"))

	for (country in countries) {
	if(paste0("paf_yll_",country,"_2017_2.csv") %ni% files){

		# Launch jobs
		jname <- paste0("air_ozone_paf_", country, "_", output.version)
		sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -pe multi_slot ", 2*cores.provided, " -l mem_free=", 4*cores.provided, "G", hold.text)
		args <- paste(country,
		              exp.grid.version,
		              output.version,
		              draws.required,
                  cores.provided)

		system(paste(sys.sub, rshell, rscript, args))
	 }

	}

#********************************************************************************************************************************
