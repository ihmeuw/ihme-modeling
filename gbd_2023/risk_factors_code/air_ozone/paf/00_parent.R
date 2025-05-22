#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Launch the parallelized calculation of ozone exp/RR/PAF

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {

  j_root <- "/home/j/"
  h_root <- "~/"

} else {

  j_root <- "J:"
  h_root <- "H:"

}

project <- "-A proj_erf "
sge.output.dir <- "-e ADDRESS/%x.e%j -o ADDRESS/%x.o%j "
#sge.output.dir <- "" # toggle to run with no output files


	# System settings
 	rshell <- "/ihme/singularity-images/rstudio/shells/execRscript.sh"
  prep.script <- paste0(h_root, "/air_pollution/air_ozone/paf/01_load.R")
	rscript <- paste0(h_root, "/air_pollution/air_ozone/paf/02_calculate.R")

	# Job settings.
	exp.grid.version <- "12"
	draws.required <- 1000 
	prep.environment <- T
	model.version <- 5 # RR model version


	# Versioning
	output.version <- 22 # calculating with Fisher's Information boost for RR draws

	# load packages, install if missing
	pacman::p_load(data.table, magrittr, ini)

	##function lib##
	# central functions
	file.path(j_root, 'temp/central_comp/libraries/current/r/get_population.R') %>% source
	file.path("/ihme/cc_resources/libraries/current/r/get_location_metadata.R") %>% source
	#custom fx
	"%ni%" <- Negate("%in%") # create a reverse %in% operator

#********************************************************************************************************************************

#----PREP------------------------------------------------------------------------------------------------------------------------
# Prep save directories
out.paf.dir <- file.path(j_root, "FILEPATHS/pafs", output.version)
out.exp.dir <- file.path(j_root, "FILEPATHS/exp", output.version)
out.tmp <- file.path("/FILEPATHS/paf", output.version)
out.rr.dir <- file.path(j_root, "FILEPATHS", output.version)
directory.list <- c(out.paf.dir, out.tmp, out.exp.dir,out.rr.dir)

# Prep directories
for (directory in directory.list) {

  dir.create(paste0(directory, "/draws"), recursive = TRUE)
  dir.create(paste0(directory, "/summary"), recursive = TRUE)

}

# Get the list of most detailed GBD locations
locs <- get_location_metadata(35)
countries <- locs[is_estimate==1 & most_detailed==1, unique(location_id)]
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]

#----LAUNCH CALC-----------------------------------------------------------------------------------------------------------------
#Launch the jobs to calculate ozone PAFs and exp
#with a check to see if they have already been created
files <- list.files(paste0(out.tmp,"/draws"))

memory <- 300
cores.provided <- 5

for (country in countries) {
	if(paste0("paf_yll_",country,"_2022.csv") %ni% files){ # check to see if we've already run it
	  
	  threads <- 3
	  
	  args <- paste(country,
	                exp.grid.version,
	                output.version,
	                draws.required,
	                cores.provided,
	                model.version)
	  
	  big_countries <- c("23","71","44979","101","44973","36","97","524","38","139","171","519","152") #these friends need xtra resources
	  
	  if(country %in% big_countries){
	    mem <- paste0("--mem=", memory, "G")
	    fthread <- paste0("-c ",cores.provided)
	    runtime <- "-t 02:00:00" # most take less than 30 m
	    archive <- "-C archive " # or "" if no jdrive access needed
	    jname <- paste0("-J calc_paf_ozone_",country)
	  }else{
	    mem <- paste0("--mem=", memory, "G")
	    fthread <- paste0("-c ",cores.provided)
	    runtime <- "-t 00:20:00" # most take less than 1 m
	    archive <- "-C archive " # or "" if no jdrive access needed
	    jname <- paste0("-J calc_paf_ozone_",country)
	  }
	 
	  
	  system(paste("sbatch",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,rscript,args))
	  

		}

}



#********************************************************************************************************************************





