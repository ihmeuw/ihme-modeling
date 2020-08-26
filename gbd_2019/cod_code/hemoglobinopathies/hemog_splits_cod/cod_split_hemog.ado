
cap program drop cod_split_hemog
program define cod_split_hemog
	syntax, parent_cause_id(integer) root_dir(string)

	// Set OS flexibility
	set more off
	if ("`c(os)'"=="Windows") {
		local j "J:"
		local h "H:"
	}
	else {
		local j "FILEPATH"
		local h "~"
		set odbcmgr unixodbc
	}
	sysdir set PLUS "`h'/ado/plus"
	
	// load in programs

	run "FILEPATH"

	/*
	// get data from cod
	get_cod_data, cause_id("618") gbd_round_id(5) clear
	keep if data_type == "Vital Registration"
	keep cause_id location_id year age_group_id sex rate
	rename (sex year) (sex_id year_id)
	tempfile causes
	save `causes', replace


	
	// merge on location metadata
	get_location_metadata, location_set_id(35) gbd_round_id(5) clear
	merge 1:m location_id using `causes', keep(3)
	keep if developed == "1"
	drop developed

	// genereate aggregate 
	collapse (mean) rate, by(year_id age_group_id sex_id)
	reshape wide rate, i(age_group_id sex_id) j(year_id)
	// Iceland is the only 2016 VR data. Copy 2015 data which is more comprehensive
	drop rate2016
	gen rate2016 = rate2015
	reshape long
	gen cause_id = 618
	
	*/

	use "FILEPATH", clear 

	// make temporary space
	/*
	
	// save to temporary space
	forvalues i = 0/999 {
		gen draw_`i' = rate
	}
	drop rate
	save "/ihme/scratch/users/jab0412/hemog/tmp/618_csmr.dta", replace

	// create folders for results
	levelsof cause_id, local(child_cause_ids) clean
	foreach cause of local child_cause_ids {
	}
	
	local sex_ids 1 2
	
	foreach sex of local sex_ids {
		foreach cause of local child_cause_ids {
				}
		}
	}	
*/

// loop through demographics
	get_demographics, gbd_team("cod") gbd_round_id(6) clear
	local location_ids = r(location_id) 


	// this line tests just one location (for filepath change test) 
	//local location_ids 44710




	// submit jobs by location/year
	local jobs = ""
	foreach loc of local location_ids {
		local jobs = "`jobs'" + ",hemog_`loc'"
		!qsub -N "hemog_`loc'" -l m_mem_free=15G -l fthread=2 -q long.q -P "proj_nch" "FILEPATH" "root_dir(`root_dir') parent_cause_id(`parent_cause_id') location_id(`loc')"
		
		
	}
	local jobs = subinstr("`jobs'",",","",1)

/*

// upload males first to prevent database lock
local sex_id 1
local jobs2 = ""
foreach cause of local child_cause_ids{
}
// upload females holding for male uploads to finish
local sex_id 2
}
*/
end

// keep parent_cause_id argument the same (613), but change root_dir to your repo location
cod_split_hemog, parent_cause_id(613) root_dir("FILEPATH")
