
cap program drop cod_split_data
program define cod_split_data
	syntax, parent_cause_id(integer) child_cause_ids(string) [group_by(string)] root_dir(string)

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
	run "`root_dir'/cod_data_prop.ado"
	run "GET_DEMOGRAPHIC SHARED FUNCTION"

	// calculate data proportions
	cod_data_prop, cause_ids(`child_cause_ids') group_by(`group_by') clear
	drop if age_group_id < 7  // only for greater than 10 year old. the rest are from syphilis_nhm
	
	// set sex_ids
	local sex_ids 1 2

	// make temporary space
	cap mkdir "`root_dir'/tmp"
	if _rc {
		!rm -rf "`root_dir'/tmp"
		mkdir "`root_dir'/tmp"
	}

	// save to temporary space and make necssary folders
	save "`root_dir'/tmp/props.dta", replace
	
	foreach cause of local child_cause_ids{
		cap mkdir "`root_dir'/tmp/`cause'"
				if _rc {
					!rm -rf "`root_dir'/tmp/`cause'"
					mkdir "`root_dir'/tmp/`cause'"
				}
	}
	foreach sex of local sex_ids {
		foreach cause of local child_cause_ids {
			cap mkdir "`root_dir'/tmp/`cause'/`sex'"
				if _rc {
					!rm -rf "`root_dir'/tmp/`cause'/`sex'"
					mkdir "`root_dir'/tmp/`cause'/`sex'"
				}
		}
	}

	get_demographics, gbd_team(cod) clear
	local location_ids = r(location_ids)

	// submit jobs by location
	local jobs = ""
	foreach loc of local location_ids {

		local jobs = "`jobs'" + ",sti_`loc'"

		!qsub -N "sti_`loc'" -l mem_free=8 -pe multi_slot 4 -P "proj_custom_models" "SHELL" "`root_dir'/split_loc_year.do" "root_dir(`root_dir') parent_cause_id(`parent_cause_id') prop_index(`group_by') location_id(`loc')"
		
	}
	local jobs = subinstr("`jobs'",",","",1)
*/
// upload males first to avoid database lock
local sex_id 1
local child_cause_ids 394
foreach cause of local child_cause_ids {
	local jobs2 = "`jobs'" + ",save_`cause'_`sex_id'"
	!qsub -hold_jid `jobs' -N "save_`cause'_`sex_id'" -l mem_free=80 -pe multi_slot 40 -P "proj_custom_models" "SHELL" "`root_dir'/save.do" "in_dir(`root_dir'/tmp/`cause'/`sex_id') cause_id(`cause') mark_best(yes) description(sti split) sex_id(`sex_id')"
}

// upload females holding for male uploads to finish
local sex_id 2
foreach cause of local child_cause_ids{
	!qsub -hold_jid `jobs2' -N "save_`cause'_`sex_id'" -l mem_free=80 -pe multi_slot 40 -P "proj_custom_models" "SHELL" "`root_dir'/save.do" "in_dir(`root_dir'/tmp/`cause'/`sex_id') cause_id(`cause') mark_best(yes) description(sti split) sex_id(`sex_id')"
}

end

cod_split_data, parent_cause_id(393) child_cause_ids(394 395 396 399) group_by(sex_id age_group_id) root_dir(ROOT_DIR)
