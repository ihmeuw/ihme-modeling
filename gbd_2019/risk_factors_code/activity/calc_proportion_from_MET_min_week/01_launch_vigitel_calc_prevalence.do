
// AUTHOR: 
// DATE:
// PURPOSE: LAUNCH PARALLELIZING CODE TO EXTRACT PHYSICAL ACTIVITY DATA FROM VIGITEL AND COMPUTE PHYSICAL ACTIVITY PREVALENCE IN 5 YEAR AGE-SEX GROUPS FOR EACH YEAR 


// Set preferences for STATA
	// Clear memory and set memory and variable limits
		clear all
		set maxvar 32000
		capture restore, not
	// Set to run all selected code without pausing
		set more off
	// Define J drive (data) for cluster (UNIX) and Windows (Windows)
		if c(os) == "Unix" {
			global prefix "FILEPATH"
			set odbcmgr unixodbc
		}
		else if c(os) == "Windows" {
			global prefix "FILEPATH"
		}

// Local repo 
	local repo `1'

// Bring in 2013 iso3 code
	
	import excel "FILEPATH", firstrow clear
	
// Create locals for relevant files and strings
	levelsof city, local(cities)

	local code_dir "FILEPATH"

// Parallelize by Brazilian city 
	foreach city of local cities {
			di "CITY = `city'" 
			!qsub -N "`city'" -l mem_free=4G -pe multi_slot 4 ///
			"`code_dir'/FILEPATH" ///
			"`code_dir'/FILEPATH" ///
			"`city'"
		}
			
		
	
