
clear all
set graphics off
set more off
set maxvar 32000


/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */ 


// discover root 
	if c(os) == "Windows" {
		local j /*FILEPATH*/
		// Load the PDF appending application
		quietly do /*FILEPATH*/
	}
	if c(os) == "Unix" {
		local j /*FILEPATH*/
		ssc install estout, replace 
		ssc install metan, replace
	} 
	
// arguments
local healthstate `1'

run /*FILEPATH*/

// directories 	
local working_dir = /*FILEPATH*/
local data_dir = /*FILEPATH*/
local split_dir = /*FILEPATH*/
local out_dir = /*FILEPATH*/

************************************************************

// format for save_results
import delimited using "`split_dir'", clear 

di "getting target_me_id"
keep if healthstate == "`healthstate'"
local target_me_id = target_me_id
di "`healthstate' results will be saved under me_id `target_me_id'"

di "bringing in data"
use /*FILEPATH*/, clear
tempfile data
save `data'

di "generating locals"
levelsof location_id, local(location_id_list)
levelsof year_id, local(year_id_list)
levelsof sex_id, local(sex_id_list)

//GBD2016

di "looping"
foreach location_id of local location_id_list {
	foreach year_id of local year_id_list {
		foreach sex_id of local sex_id_list {
			di "location_id `location_id' year_id `year_id' sex_id `sex_id'"
			keep if location_id == `location_id' & year_id == `year_id' & sex_id == `sex_id'
			
			expand 24, generate (age_group_id)
			replace age_group_id = _n + 1
			replace age_group_id =  30 if age_group_id == 21
			replace age_group_id =  31 if age_group_id == 22
			replace age_group_id =  32 if age_group_id == 23
			replace age_group_id =  164 if age_group_id == 24
			replace age_group_id = 235 if age_group_id == 25

			generate modelable_entity_id = `target_me_id'
			generate measure_id = 5

			export delimited /*FILEPATH*/, replace 

			use `data', clear
		}
	}
}

// save results
save_results, modelable_entity_id(`target_me_id') description(orig bp - `me_id') in_dir(`out_dir') metrics(prevalence) skip_calc(yes) mark_best(yes)

//test 

