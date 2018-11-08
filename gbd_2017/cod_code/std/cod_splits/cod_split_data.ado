***************************************************************
** RUNS AND SOURCES ALL CODE
***************************************************************

***************************************************************
** Setup Stata
***************************************************************

clear all
set more off
set maxvar 32000
// OS flexibility setup 
// ***
// ***
sysdir set PLUS "`h'/ado/plus"

*****************************************************************

cap program drop cod_split_data
program define cod_split_data
	syntax, parent_cause_id(integer) child_cause_ids(string) [group_by(string)] root_dir(string) out_dir(string)

	// Set OS flexibility
	set more off
	
	sysdir set PLUS "FILEPATH/ado/plus"

	***************************************************************
	** Calculate CoD proportion splits for each of the 4 child causes
	** for each age group and sex pair
	***************************************************************

	// load in programs
	run "`root_dir'/cod_data_prop.ado"
	run "FILEPATH/get_demographics.ado"
	run "FILEPATH/get_location_metadata.ado"

	// calculate data proportions
	cod_data_prop, cause_ids(`child_cause_ids') group_by(`group_by') clear
	drop if age_group_id < 7  // only for greater than 10 year old. the rest are from congenital syphilis

	// set sex_ids
	local sex_ids 1 2

	// make temporary space
	cap mkdir FILEPATH
	if _rc {
		!rm -rf FILEPATH
		mkdir FILEPATH
	}

	// save to temporary space and make more folders
	save FILEPATH, replace 

	***************************************************************
	** Make/Clear temporary space for saving .csv files
	***************************************************************
	
// sequentially goes through each item in child_cause_ids
// local sexes = "male_1 female_2"

	foreach cause of local child_cause_ids{
		cap mkdir FILEPATH
				if _rc {
					!rm -rf FILEPATH
					mkdir FILEPATH
				}
				foreach sex of local sex_ids {
					cap mkdir FILEPATH
						if _rc {
							!rm -rf FILEPATH
							mkdir FILEPATH
						}
				}
	}

	***************************************************************
	** Get proportion splits for every cause, country, year, and sex
	** and save individually as .csv with split_loc_year.do
	***************************************************************

	get_location_metadata, location_set_id(35) clear
	levelsof location_id, local(locations)

	// submit jobs by location/year into split_loc_year
	local jobs = ""
	foreach loc of local locations {
		local jobs = "`jobs'" + ",sti_`loc'"
		!qsub -N "sti_`loc'" -l mem_free=8 -pe multi_slot 4 -P "proj_custom_models" -o "FILEPATH" -e "FILEPATH" "FILEPATH/stata_shell.sh" "`root_dir'/split_loc_year.do" "root_dir(`out_dir') parent_cause_id(`parent_cause_id') prop_index(`group_by') location_id(`loc')"
	}
	
	***************************************************************
	** Save results using save.do
	***************************************************************
	
	local jobs = subinstr("`jobs'",",","",1)
	local description = "MODEL DESCRIPTION"

	// upload males first to avoid database lock
	local sex_id 1
	foreach cause of local child_cause_ids {
		local jobs2 = "`jobs'" + ",save__t2_`cause'_`sex_id'" 
		local input_dir = "FILEPATH"
		
		!qsub -hold_jid `jobs' -N "save_t2_`cause'_`sex_id'" -l mem_free=80 -pe multi_slot 40 -P "proj_custom_models" -o "FILEPATH" -e "FILEPATH" "FILEPATH/stata_shell.sh" "`root_dir'/save.do" "input_dir(`input_dir') cause_id(`cause') mark_best(yes) description(`description') sex_id(`sex_id')"
	}

	// upload females holding for male uploads to finish
	local sex_id 2
	foreach cause of local child_cause_ids {
		local input_dir = "FILEPATH"
		
		!qsub -hold_jid `jobs2' -N "save_t2_`cause'_`sex_id'" -l mem_free=80 -pe multi_slot 40 -P "proj_custom_models" -o "FILEPATH" -e "FILEPATH" "FILEPATH/stata_shell.sh" "`root_dir'/save.do" "input_dir(`FILEPATH') cause_id(`cause') mark_best(yes) description(`description') sex_id(`sex_id')"
	}

end

***************************************************************
** Run function defined above: cod_split_data
***************************************************************

local root_dir = "ROOT_DIR"
local out_dir = "OUT_DIR" 
local std_parent_id = 393 // Sexually trasmitted diseases exclusing HIV

// In order: syphilis, chlamydia, gonococcal, other stds 
local std_child_ids = "394 395 396 399" 
local group_by = "sex_id age_group_id"

cod_split_data, parent_cause_id(`std_parent_id') child_cause_ids(`std_child_ids') group_by(`group_by') root_dir(`root_dir') out_dir(`out_dir')
