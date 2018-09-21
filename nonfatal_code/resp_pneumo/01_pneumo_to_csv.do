// write csvs with draws set to 0 for locations that are excluded 

	clear
	set more off
// Set to run all selected code without pausing
	set more off
// Remove previous restores
	cap restore, not

	adopath + "FILEPATH" // load in shared function get_draws

	args location_id output ver_desc excluded
	di "`location_id' `output' `ver_desc' `excluded'"
	local coal_workers_ac 1893
	local coal_workers_end 3052
	
	//load the draws
	get_draws, gbd_id_field(modelable_entity_id) gbd_id(`coal_workers_ac') location_ids(`location_id') measure_ids(5 6) sex_ids(1 2) source(epi) clear

	if `excluded' == 1{
		di "exclusion!"
		foreach draw of varlist draw*{
			qui replace `draw' =0
		}
	}


	local years 1990 1995 2000 2005 2010 2016
	levelsof sex_id, local(sexes)
	
	local counter = 0
	
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
					
					export delim "FILEPATH.csv", replace
				restore
				
			}
		}
	}
