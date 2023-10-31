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

project <- "-P PROJECT " 
sge.output.dir <- "-o FILEPATH -e /FILEPATH "
#sge.output.dir <- "" # toggle to run with no output files


	# System settings
  rshell <- "FILEPATH.sh"
	rscript <- paste0(h_root, "FILEPATH/01_calculate.R")

	# Job settings.
	exp.grid.version <- VERSION
	draws.required <- 1000 #number of draws to create to show distribution, default is 1000 - do less for a faster run
	prep.environment <- T
	model.version <- VERSION # RR model version

	output.version <- VERSION

	# load packages, install if missing
	pacman::p_load(data.table, magrittr, ini)

	##function lib##
	# central functions
	file.path(j_root, 'FILEPATH/get_population.R') %>% source
	file.path("FILEPATH/get_location_metadata.R") %>% source
	#custom fx
	"%ni%" <- Negate("%in%") # create a reverse %in% operator

#----PREP------------------------------------------------------------------------------------------------------------------------
# Prep save directories
out.paf.dir <- file.path(j_root, "WFILEPATH", output.version)
out.exp.dir <- file.path(j_root, "FILEPATH", output.version)
out.tmp <- file.path("FILEPATH", output.version)
out.rr.dir <- file.path(j_root, "FILEPATH", output.version)
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

for (country in countries) {
	if(paste0("paf_yll_",country,"_2022.csv") %ni% files){ # check to see if we've already run it
	  
	  threads <- 3
	  
	  args <- paste(country,
	                exp.grid.version,
	                output.version,
	                draws.required,
	                threads,
	                model.version)
	  
	  big_countries <- c("23","71","44979","101","44973","36","97","524","38","139","171","519","152")
	  
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
	 
	  
	  system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,rscript,args))
	  

		}

	}

#********************************************************************************************************************************





