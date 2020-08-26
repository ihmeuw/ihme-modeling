
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
		local j "/home/j"
		local h "~"
		set odbcmgr unixodbc
	}
	sysdir set PLUS "`h'/ado/plus"
	
	// load in programs

	run "`j'/temp/central_comp/libraries/current/stata/get_demographics.ado"
	run "`j'/temp/central_comp/libraries/current/stata/get_location_metadata.ado"
	run "`j'/temp/central_comp/libraries/current/stata/get_cod_data.ado"


	/*

	// get data from cod
	//use "`j'/temp/chikeda/2018/180110_Hemog/cod_data_2017_updated.dta", clear
	get_cod_data, cause_id("618") gbd_round_id(5) clear
	keep if data_type == "Vital Registration"
	keep cause_id location_id year age_group_id sex rate
	rename (sex year) (sex_id year_id)
	tempfile causes
	save `causes', replace

	//run "/home/j/temp/central_comp/libraries/current/stata/get_cod_data.ado"

	
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

	use "`j'/temp/thomakat/cod_data_618.dta", clear

	// make temporary space
	cap mkdir "/ihme/scratch/users/chikeda/hemog/tmp"
	if _rc {
		!rm -rf "/ihme/scratch/users/chikeda/hemog/tmp"
		mkdir "/ihme/scratch/users/chikeda/hemog/tmp"
	}
	mkdir "/ihme/scratch/users/chikeda/hemog/tmp/scratch" 
	
	
	// save to temporary space
	forvalues i = 0/999 {
		gen draw_`i' = rate
	}
	drop rate
	save "/ihme/scratch/users/chikeda/hemog/tmp/618_csmr.dta", replace

	// create folders for results
	import delimited using "`root_dir'/input_map.csv", clear
	levelsof cause_id, local(child_cause_ids) clean
	foreach cause of local child_cause_ids {
		mkdir "/ihme/scratch/users/chikeda/hemog/tmp/`cause'"
	}
	
	local sex_ids 1 2
	
	foreach sex of local sex_ids {
		foreach cause of local child_cause_ids {
			cap mkdir "/ihme/scratch/users/chikeda/hemog/tmp/`cause'/`sex'"
				if _rc {
					!rm -rf "/ihme/scratch/users/chikeda/hemog/tmp/`cause'/`sex'"
					mkdir "/ihme/scratch/users/chikeda/hemog/tmp/`cause'/`sex'"
				}
		}
	}	


// loop through demographics
	get_demographics, gbd_team("cod") gbd_round_id(5) clear
	//local location_ids = r(location_id) 

	//local location_ids 44704 44711 44710 44712 44706 44684
	//local location_ids 44704 44711 44712 44706 44684

	// this line tests just one location (for filepath change test) 
	//local location_ids 44710

	///this is for when jobs fail, can resubmit with list of loc-ids here
	local location_ids 493


	// submit jobs by location/year
	local jobs = ""
	foreach loc of local location_ids {
		local jobs = "`jobs'" + ",hemog_`loc'"
		!qsub -N "hemog_`loc'" -l mem_free=8 -pe multi_slot 4 -P "proj_custom_models" -o "/ihme/scratch/users/chikeda/output" -e "/ihme/scratch/users/chikeda/errors" "`j'/WORK/10_gbd/00_library/functions/utils/stata_shell.sh" "`root_dir'/split_loc_year_test618.do" "root_dir(`root_dir') parent_cause_id(`parent_cause_id') location_id(`loc')"
		
		
	}
	local jobs = subinstr("`jobs'",",","",1)


/*


// upload males first to prevent database lock
local sex_id 1
local jobs2 = ""
foreach cause of local child_cause_ids{
	local jobs2 = "`jobs'" + ",save_`cause'_`sex_id'"
	!qsub -hold_jid `jobs' -N "save_`cause'_`sex_id'" -l mem_free=40 -pe multi_slot 40 -P "proj_custom_models" -o "/home/j/temp/chikeda/do/171204_Hemog/outputs/save_males" -e "/home/j/temp/chikeda/do/171204_Hemog/errors/save_males"  "`j'/WORK/10_gbd/00_library/functions/utils/stata_shell.sh" "`root_dir'/save.do" "in_dir(`root_dir'/tmp/`cause') cause_id(`cause') mark_best(yes) description(hemog split) sex_id(`sex_id')"
}
// upload females holding for male uploads to finish
local sex_id 2
foreach cause of local child_cause_ids{
	!qsub -hold_jid `jobs2' -N "save_`cause'_`sex_id'" -l mem_free=40 -pe multi_slot 40 -P "proj_custom_models" -o "/home/j/temp/chikeda/do/171204_Hemog/outputs/save_females" -e "/home/j/temp/chikeda/do/171204_Hemog/errors/save_females"  "`j'/WORK/10_gbd/00_library/functions/utils/stata_shell.sh" "`root_dir'/save.do" "in_dir(`root_dir'/tmp/`cause') cause_id(`cause') mark_best(yes) description(hemog split) sex_id(`sex_id')"
}
*/
end

cod_split_hemog, parent_cause_id(613) root_dir("/homes/chikeda/repos/hemog/hemog_splits_cod")
