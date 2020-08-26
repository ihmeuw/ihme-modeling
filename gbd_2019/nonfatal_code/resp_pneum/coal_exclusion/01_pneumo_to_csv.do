// write csvs with draws set to 0 for locations that are excluded

disp("Starting Pneumo to CSV Script ------------------------------")
	clear
	set more off
// Set to run all selected code without pausing
	set more off
// Remove previous restores
	cap restore, not
// Define J drive (data) for cluster (UNIX) and Windows (Windows)
	if c(os) == "Unix" {
		local prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else{
		local prefix "J:/"
	}
	adopath + `prefix'FILEPATH
	qui{
		do "FILEPATH/get_draws.ado"
	}

disp("Args Location ID ------------------------------")
//	args location_id output ver_desc excluded
local location_id `1'
local output `2'
local ver_desc `3'
local excluded `4'
	di "`location_id' `output' `ver_desc' `excluded'"


	di "location(`location_id') output(`output') ver_desc(`ver_desc') excluded(`excluded')"
	local coal_workers_ac 24658 // 10/4/2019 changed to EMR bundle, previously 1893
	local coal_workers_end 3052

	di "`location_id'"
	di "`coal_workers_ac'"
	disp("Starting Get Draws ----------------------------")
	*// get_draws, gbd_id_type(modelable_entity_id) gbd_id(24658) location_id(2) measure_id(5 6) sex_id(1 2) source(epi) status(best) gbd_round_id(6) decomp_step(step4) clear
	get_draws, gbd_id_type(modelable_entity_id) gbd_id(`coal_workers_ac') location_id(`location_id') measure_id(5 6) sex_id(1 2) source(epi) status(best) gbd_round_id(6) decomp_step(step4) clear

disp("Line 52 --------------------------")
di "excluded(`excluded')"
	if ("`excluded'" == "1") {
		di "exclusion!"
		foreach draw of varlist draw*{
			qui replace `draw' =0
		}
	}


	local years 1990 1995 2000 2005 2010 2015 2017 2019
	levelsof sex_id, local(sexes)

	local counter = 0

	disp("Set measure and sex, location, exclusion line 67 ----------------------------------")
	forvalues i = 5/6{
		//set measure
		local measure_id = `i'

		foreach year of local years{
			foreach s of local sexes{
				di "`loc' `year' `s' `i'"
				//set exclusion
				preserve
					local counter = `counter' +1
					keep age_group_id draw_* sex_id location_id year_id measure_id
					keep if sex_id == `s' & location_id == `location_id' & year_id == `year' & measure_id == `i'
					count

					export delim "`output'/`measure_id'_`location_id'_`year'_`s'.csv", replace
				restore

			}
		}
	}
