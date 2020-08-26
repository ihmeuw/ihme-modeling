// AUTHOR: 
// DATE:
// PURPOSE: Append VIGITEL files for each Brazilian city for physical activity prevalence


// Set preferences for STATA
	// Clear memory and set memory and variable limits
		clear all
		set maxvar 32000
		capture restore, not
	// Set to run all selected code without pausing
		set more off
	// Define J drive (data) for cluster (UNIX) and Windows (Windows)
		if c(os) == "Unix" {
			global prefix FILEPATH
			set odbcmgr unixodbc
		}
		else if c(os) == "Windows" {
			global prefix FILEPATH
		}

// Get location information
	adopath + FILEPATH
	get_location_metadata, location_set_id(9) clear
	keep ihme_loc_id location_id location_ascii_name 
	
	rename ihme_loc_id iso3 
	rename location_ascii_name location_name
	duplicates drop location_name, force

	tempfile country_codes 
	save `country_codes', replace

// Set up locals 
	local codebook FILEPATH
	local out_dir FILEPATH



// Merge on with codebook 
	
	use "`codebook'/FILEPATH", clear
	tempfile compiled 
	save `compiled', replace 

	import excel "`codebook'/FILEPATH", firstrow clear 
	replace state = "Piaui" if state == "Piauí"
	merge 1:m city using `compiled', nogen keep(3) 
	rename state location_name

	// adjust the state names to not have accents in them
	replace location_name = "Amapa" if location_name == "Amapá"
	replace location_name = "Ceara" if location_name == "Ceará"
	replace location_name = "Espirito Santo" if location_name == "Espírito Santo"
	replace location_name = "Goias" if location_name == "Goiás"
	replace location_name = "Maranhao" if location_name == "Maranhão"
	replace location_name = "Parana" if location_name == "Paraná"
	replace location_name = "Paraiba" if location_name == "Paraíba"
	replace location_name = "Para" if location_name == "Pará"
	replace location_name = "Rondonia" if location_name == "Rondônia"
	replace location_name = "Sao Paulo" if location_name == "São Paulo"

	merge m:1 location_name using `country_codes', nogen assert(2 3) keep(3)


// Drop if sample size < 10
	drop if sample_size < 10 

// Clean up data 
	drop city city_number 

// Urbanicity / representativeness variable 
	
	gen urbanicity = 2 // just sampled in capital cities of the states 
	gen representative_name = 3 // not representative of that subnational location (state)

	destring location_id, replace

// Save

	save "`out_dir'/FILEPATH", replace
