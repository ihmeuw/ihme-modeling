
cap program drop cod_split_hemog
program define cod_split_hemog
	syntax, parent_cause_id(integer) root_dir(string)

	// Set OS flexibility
	set more off
	if ("`c(os)'"=="Windows") {
		local j "{FILEPATH}"
		local h "{FILEPATH}"
	}
	else {
		local j "{FILEPATH}"
		local h "{FILEPATH}"
		set odbcmgr unixodbc
	}
	sysdir set PLUS "`h'/ado/plus"
	
	// load in programs

	run "{FILEPATH}/get_demographics.ado"
	run "{FILEPATH}/get_location_metadata.ado"
	run "{FILEPATH}/get_cod_data.ado"


	// get data from cod
	get_cod_data, cause_id({CAUSE ID}) clear
	keep if data_type == "Vital Registration"
	keep cause_id location_id year age_group_id sex rate
	rename (sex year) (sex_id year_id)
	tempfile causes
	save `causes', replace
	
	// merge on location metadata
	get_location_metadata, location_set_id({LOCATION SET ID}) clear
	merge 1:m location_id using `causes', keep(3)
	keep if developed == {DEVLEOPED}
	drop developed

	// genereate aggregate 
	collapse (mean) rate, by(year_id age_group_id sex_id)
	reshape wide rate, i(age_group_id sex_id) j(year_id)
	reshape long
	gen cause_id = {CAUSE ID}
	
	// make temporary space
	cap mkdir "`root_dir'/tmp"
	if _rc {
		!rm -rf "`root_dir'/tmp"
		mkdir "`root_dir'/tmp"
	}
	mkdir "`root_dir'/tmp/scratch"
	
	// save to temporary space
	forvalues i = 0/999 {
		gen draw_`i' = rate
	}
	drop rate
	save "`root_dir'/tmp/{CAUSE ID}_csmr.dta", replace

	// create folders for results
	import delimited using "`root_dir'/input_map.csv", clear
	levelsof cause_id, local(child_cause_ids) clean
	foreach cause of local child_cause_ids {
		mkdir "`root_dir'/tmp/`cause'"
	}
	local sex_ids {SEX IDS}
	foreach sex of local sex_ids {
		foreach cause of local child_cause_ids {
			cap mkdir "`root_dir'/tmp/`cause'/`sex'"
				if _rc {
					!rm -rf "`root_dir'/tmp/`cause'/`sex'"
					mkdir "`root_dir'/tmp/`cause'/`sex'"
				}
		}
	}	

	// loop through demographics
	get_demographics, gbd_team({GBD TEAM}) clear
	local location_ids = r(location_ids)

	// submit jobs by location/year
	local jobs = ""
	foreach loc of local location_ids {
		local jobs = "`jobs'" + ",hemog_`loc'"
		!qsub -N "hemog_`loc'" -l mem_free=8 -pe multi_slot 4 -P {PROJECT} -o {FILEPATH} "{FILEPATH}/stata_shell.sh" "`root_dir'/split_loc_year.do" "root_dir(`root_dir') parent_cause_id(`parent_cause_id') location_id(`loc')"
		
		
	}
	local jobs = subinstr("`jobs'",",","",1)


local sex_id {SEX ID}
local jobs2 = ""
foreach cause of local child_cause_ids{
	local jobs2 = "`jobs'" + ",save_`cause'_`sex_id'"
	!qsub -hold_jid `jobs' -N "save_`cause'_`sex_id'" -l mem_free=40 -pe multi_slot 40 -P {PROJECT} -o {FILEPATH} -e {FILEPATH}  "{FILEPATH}/stata_shell.sh" "`root_dir'/save.do" "in_dir(`root_dir'/tmp/`cause'/`sex_id') cause_id(`cause') mark_best(yes) description(hemog split) sex_id(`sex_id')"
}

local sex_id {SEX ID}
foreach cause of local child_cause_ids{
	!qsub -hold_jid `jobs2' -N "save_`cause'_`sex_id'" -l mem_free=40 -pe multi_slot 40 -P {PROJECT} -o {FILEPATH} -e {FILEPATH}  "{FILEPATH}/stata_shell.sh" "`root_dir'/save.do" "in_dir(`root_dir'/tmp/`cause'/`sex_id') cause_id(`cause') mark_best(yes) description(hemog split) sex_id(`sex_id')"
}

end

cod_split_hemog, parent_cause_id({CAUSE ID}) root_dir({FILEPATH}/cod_split_hemog)
