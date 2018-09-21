// *****************************************************************************
// Purpose:		launch script do file - parallelize calculation of population
//				attributable fraction of hepatitis C and hepatitis B prevalence
//				due to IV drug use, by country, sex and groups of 100 draws
** *****************************************************************************
** RUNTIME CONFIGURATION
** ****************************************************************************
// Set preferences for STATA
	// Clear memory and set memory and variable limits
		clear all
		set mem 12g
		set maxvar 32000
	// Set to run all selected code without pausing
		set more off
	// Set graph output color scheme
		set scheme s1color
	// Remove previous restores
		cap restore, not
	// Reset timer (?)
		timer clear	

	// Close previous logs
		cap log close

** Set version number (increases by 1 for each run)
	local ver 4

	local code_dir "FILEPATH"

// Save ISO3 with subnational location ids in a local for launching parallelized jobs
	run "FILEPATH/get_location_metadata.ado" 
	get_location_metadata, location_set_id(35) clear
	keep if is_estimate == 1 & most_detailed == 1

	keep ihme_loc_id location_id location_ascii_name super_region_name super_region_id region_name region_id
	
	rename ihme_loc_id iso3 
	tempfile country_codes
	save `country_codes', replace

	qui: levelsof location_id, local(locations)

// 2.) Parallelize on location

		foreach iso3 of local locations {
			
			!qsub -P proj_custom_models -N "ipv_pafs_`iso3'" -pe multi_slot 4 "FILEPATH/stata_shell.sh" "`code_dir'/04_format_paf.do" "`ver' `iso3'" 
					
		}
				
		
	
