
****************************
**** 	1. Set Up		****
****************************

clear all
set more off
set maxvar 32000
cap log close


	if c(os) == "Unix" {
		local prefix "/home/j"
		set more off
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local prefix "J:"
	}

// Locals
	// Taking Parameters (location_id)
		local loc_id `1'
		local version `2'

**pull in each of the four modeled prevalences from dismod
adopath + "`prefix'/FILEPATH"

**call in super-region coding
get_location_metadata, location_set_id(22) clear

	keep location_id super_region_name
	duplicates drop

	tempfile super_region_name
	save `super_region_name', replace

local healthstates "modhighactive lowmodhighactive inactive lowactive modactive highactive"
local data_types "gpaq ipaq"

di "`data_types'"
di "`healthstates'"


foreach healthstate of local healthstates {
	foreach data_type of local data_types {

	di in green "Now on `data_type' data from `healthstate'"

	// Moderately and Highly Active
	if "`healthstate'" == "modhighactive" {

		local me_id = 9361

		if "`data_type'" == "gpaq" {
			local mv_id "315611"
		}
		if "`data_type'" == "ipaq" {
			local mv_id "315581"
		}
	}
	// Low, Moderately and Highly Active
	if "`healthstate'" == "lowmodhighactive" {

		local me_id = 9360

		if "`data_type'" == "gpaq" {
			local mv_id "315608"
		}
		if "`data_type'" == "ipaq" {
			local mv_id "316559"
		}
	}
	// Inactive
	if "`healthstate'" == "inactive" {

		local me_id = 9356

		if "`data_type'" == "gpaq" {
			local mv_id "315596"
		}
		if "`data_type'" == "ipaq" {
			local mv_id "315638"
		}
	}
	// Low active
	if "`healthstate'" == "lowactive" {

		local me_id = 9357

		if "`data_type'" == "gpaq" {
			local mv_id "315599"
		}
		if "`data_type'" == "ipaq" {
			local mv_id "315569"
		}
	}
	// Moderately active
	if "`healthstate'" == "modactive" {

		local me_id = 9358

		if "`data_type'" == "gpaq" {
			local mv_id "315602"
		}
		if "`data_type'" == "ipaq" {
			local mv_id "315593"
		}
	}
	// Highly active
	if "`healthstate'" == "highactive" {

		local me_id = 9359

		if "`data_type'" == "gpaq" {
			local mv_id "317120"
			*local mv_id "315575"
		}
		if "`data_type'" == "ipaq" {
			local mv_id "315575"
		}
	}

	di in green "This means the me_id = `me_id' & mv_id = `mv_id'"

if "`data_types'" == "gpaq ipaq" {
get_draws, gbd_id_type("modelable_entity_id") gbd_id(`me_id') location_id(`loc_id') year_id(1990 1995 2000 2005 2010 2017) source(epi) version_id(`mv_id') clear
}

	forvalues x=0/999 {
		rename draw_`x' mean_`healthstate'_`x'
	}

	keep location_id year_id age_group_id sex_id mean_`healthstate'_*

        duplicates drop location_id year_id sex_id age_group_id, force

    replace age_group_id = 21 if age_group_id == 30

	drop if age_group_id > 30
	drop if age_group_id < 10

	cap mkdir "FILEPATH"
	cap mkdir "FILEPATH"
	cap mkdir "FILEPATH"
	cap mkdir "FILEPATH"
	export delimited "FILEPATH/`loc_id'.csv", replace

	tempfile `healthstate'_`data_type'
	save ``healthstate'_`data_type'', replace
} // end data_type
} // end healthstate

**merge GPAQ and IPAQ and take mean of draws
if "`data_types'" == "best" {
	use `highactive_best', clear 

	// Combine draw files for inactive, lowactive, modactive, highactive
	merge 1:1 location_id year_id age_group_id sex_id using `inactive_best', nogen keep(3)
	merge 1:1 location_id year_id age_group_id sex_id using `lowactive_best', nogen keep(3)
	merge 1:1 location_id year_id age_group_id sex_id using `modactive_best', nogen keep(3)
	merge 1:1 location_id year_id age_group_id sex_id using `modhighactive_best', nogen keep(3)
	merge 1:1 location_id year_id age_group_id sex_id using `lowmodhighactive_best', nogen keep(3)
}

if "`data_types'" == "gpaq ipaq" {
	foreach data_type of local data_types {
		import delimited "FILEPATH/`loc_id'.csv", clear
			tempfile inactive_`data_type'
			save `inactive_`data_type'', replace
		import delimited "FILEPATH/`loc_id'.csv", clear
			tempfile lowactive_`data_type'
			save `lowactive_`data_type'', replace
		import delimited "FILEPATH/`loc_id'.csv", clear
			tempfile modactive_`data_type'
			save `modactive_`data_type'', replace
		import delimited "FILEPATH/`loc_id'.csv", clear
			tempfile modhighactive_`data_type'
			save `modhighactive_`data_type'', replace
		import delimited "FILEPATH/`loc_id'.csv", clear
			tempfile lowmodhighactive_`data_type'
			save `lowmodhighactive_`data_type'', replace


		import delimited "FILEPATH/`loc_id'.csv", clear
		*use `highactive_`data_type'', clear
			merge 1:1 location_id year_id age_group_id sex_id using `inactive_`data_type'', nogen keep(3)
			merge 1:1 location_id year_id age_group_id sex_id using `lowactive_`data_type'', nogen keep(3)
			merge 1:1 location_id year_id age_group_id sex_id using `modactive_`data_type'', nogen keep(3)
			merge 1:1 location_id year_id age_group_id sex_id using `modhighactive_`data_type'', nogen keep(3)
			merge 1:1 location_id year_id age_group_id sex_id using `lowmodhighactive_`data_type'', nogen keep(3)

			if "`data_type'" == "gpaq" {
				gen gpaq = 1
			}
			if "`data_type'" == "ipaq" {
				gen gpaq = 0
			}

			tempfile pre_append_`data_type'
			save `pre_append_`data_type'', replace
	}

	use `pre_append_gpaq', clear
		append using `pre_append_ipaq'
}

// Need to squeeze each of the categories to sum to 1
forvalues draw = 0/999 {

		// Step1: Rescale inactive model and low+moderate+high model so that the sum is equal to 1
			gen sum = mean_inactive_`draw' + mean_lowmodhighactive_`draw'
			gen scaler1 = 1 / sum
			replace mean_inactive_`draw' = mean_inactive_`draw' * scaler1 // this is inactive draw rescaled
			replace mean_lowmodhighactive_`draw' = mean_lowmodhighactive_`draw' * scaler1 // this is lowmodhighactive draw rescaled
		
		// Step2: Rescale low activity exposure and moderate+high exposure so that sum is equal to 
		// [rescaled version of] low+moderate+high exposure
			gen sum_lowmodhigh = mean_lowactive_`draw' + mean_modhighactive_`draw'
			gen scaler2 = mean_lowmodhighactive_`draw' / sum_lowmodhigh
			replace mean_lowactive_`draw' = mean_lowactive_`draw' * scaler2 // this is lowactive draw rescaled
			replace mean_modhighactive_`draw' = mean_modhighactive_`draw' * scaler2	// this is modhighactive draw rescaled
			
		// Step3: Rescale moderate activity exposure and high activity exposure so that sum is equal 
		//to [rescaled version of] moderate+high exposure
			gen sum_modhigh = mean_modactive_`draw' + mean_highactive_`draw'
			gen scaler3 = mean_modhighactive_`draw' / sum_modhigh
			replace mean_modactive_`draw' = mean_modactive_`draw' * scaler3 //this is modactive draw rescaled
			replace mean_highactive_`draw' = mean_highactive_`draw' * scaler3 //this is highactive draw rescaled
			
		drop sum* scaler*
		
		** Check that rescale worked within reasonable tolerance
		assert abs(mean_inactive_`draw' + mean_lowactive_`draw' + mean_modactive_`draw' + mean_highactive_`draw' - 1) < 0.001
}

**associate super_region_name to location_id
merge m:1 location_id using `super_region_name', keep(3) nogen

gen measure_id = 19

if "`data_types'" == "gpaq ipaq" {

keep location_id year_id sex_id age_group_id gpaq mean_inactive_* mean_lowactive_* mean_modactive_* mean_highactive_*


** Save a unique dataset for each location
}

