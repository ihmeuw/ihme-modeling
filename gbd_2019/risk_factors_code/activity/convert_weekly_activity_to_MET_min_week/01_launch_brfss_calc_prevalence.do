
// AUTHOR: 
// DATE: 
// PURPOSE: LAUNCH PARALLELIZING CODE TO EXTRACT PHYSICAL ACTIVITY DATA FROM BRFSS AND COMPUTE PHYSICAL ACTIVITY PREVALENCE IN 5 YEAR AGE-SEX GROUPS FOR EACH YEAR 


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

// Set up local repo 
	//local repo `1'
	local shell_script "FILEPATH"
	
// Bring in 2013 iso3 code

	insheet using "FILEPATH", comma clear
	drop state 
	rename state_name us_state 
	
// Create locals for relevant files and strings
	levelsof us_state, local(us_states)

	local code_dir "FILEPATH"

// Parallelize by healthstate, national-level iso3, year and sex
	foreach us_state of local us_states {
			di "STATE = `us_state'" 
			!qsub -P proj_crossval -N "`us_state'" -pe multi_slot 4 -l mem_free=4G "`shell_script'" "FILEPATH" "`us_state'"
		}
			
		
	
