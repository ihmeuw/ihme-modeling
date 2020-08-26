// AUTHOR: 
// DATE: January 4, 2016
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

	
// Prepare location names & demographics for 2015

	adopath + "$prefix/FILEPATH"
	get_location_metadata, location_set_id(9) clear
	keep if is_estimate == 1 & inlist(level, 3, 4)

	keep ihme_loc_id location_id location_ascii_name super_region_name super_region_id region_name region_id

	// drop duplicates 
	drop if location_ascii_name == "Distrito Federal" & regexm(ihme_loc_id, "MEX") 
	duplicates drop location_ascii_name, force

	tempfile countrycodes
	save `countrycodes', replace

// Set up locals 
	local codebook "$prefix/FILEPATH" 
	local out_dir "$prefix/FILEPATH"


	local data_dir "FILEPATH"
	local files: dir "`data_dir'" files "*.dta"


//  Append datasets for each extracted microdata survey series/country together 
		use "`data_dir'/pns_Acre.dta", clear
		foreach file of local files {
			if "`file'" != "pns_Acre.dta" {
				di in red "`file'" 
				append using "`data_dir'/`file'", force
			}
		}
		
		tempfile compiled 
		save `compiled', replace 

// Save 
	save "FILEPATH/pns_compiled_2.dta", replace



// Merge on with codebook 
	
	use "`codebook'/pns_compiled_2.dta", clear
	tempfile compiled 
	save `compiled', replace 

	import excel "`codebook'/pns_codebook.xlsx", firstrow clear 
	merge 1:m state using `compiled', nogen keep(3) 
	rename state location_ascii_name

	replace location_ascii_name = subinstr(location_ascii_name, "_", " ", .)

	merge m:1 location_ascii_name using `countrycodes', nogen keep(3)


// Drop if sample size < 10 because it will produce unstable estimates
	drop if sample_size < 10 

// Clean up data 
	drop uf_code region
	
// Urbanicity / representativeness variable 
	
	gen urbanicity = 1 // just sampled in capital cities of the states 
	gen representative_name = 1 // not representative of that subnational location (state)
	gen nid = 195010 

// Save

	save "`out_dir'/pns_prepped.dta", replace
/*
