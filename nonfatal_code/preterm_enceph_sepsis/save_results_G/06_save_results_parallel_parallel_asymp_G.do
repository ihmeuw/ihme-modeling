/*******************************************

Description: This is the third lower-level script submitted by 
FILEPATH, and the second submitted by FILEPATH. 
FILEPATH formats asymp prevalence data and runs 
save_results. We do not run a second DisMod model for asymptomatic because 
we assume no excess mortality associated with asymptomatic. Therefore there 
is no need to stream out in DisMod to get our final results. 
*******************************************/

clear all 
set more off
set maxvar 30000
version 13.0

// priming the working environment
if c(os) == "Windows" {
			local j /*FILEPATH*/
			local working_dir = /*FILEPATH*/
		}
		if c(os) == "Unix" {
			local j /*FILEPATH*/
			ssc install estout, replace 
			ssc install metan, replace
			local working_dir = /*FILEPATH*/ 
		} 

// arguments
local acause `1'
local me_id `2'

// directories
local in_dir /*FILEPATH*/
local out_dir /*FILEPATH*/

// functions

adopath + /*FILEPATH*/

run /*FILEPATH*/


************************************************************************************

di "getting gest age, target_me_id"
	if `me_id' == 1557 {
		local gest_age "ga1_"
		local target_me_id 9978
	}
	if `me_id' == 1558 {
		local gest_age "ga2_"
		local target_me_id 9979
	}
	if `me_id' == 1559 {
		local gest_age "ga3_"
		local target_me_id 9980
	}
	if `me_id' == 2525 {
		local gest_age ""
		local target_me_id 9981
	}
	if `me_id' == 1594 {
		local gest_age ""
		local target_me_id 9982
	}


di "Me_id is `me_id' and target_me_id is `target_me_id'"
import delimited /*FILEPATH*/, clear

gen measure_id = 5
gen garbage = 1
generate modelable_entity_id = `target_me_id'
rename year year_id
rename sex sex_id

di "dropping for all but the most granular locations"
drop if draw_0 == .

tempfile data
save `data'

clear
set obs 20 
gen garbage = 1
gen age_group_id = _n + 1

joinby garbage using `data'
drop garbage
cap drop _merge

//Set draws to 0 where age_group_id>4
forval i=0/999 {
	replace draw_`i' = 0 if age_group_id > 4
} 
save `data', replace

levelsof location_id, local(location_ids)
levelsof year_id, local(year_ids)
levelsof sex_id, local(sex_ids)

foreach location_id of local location_ids {
	foreach year_id of local year_ids {
		foreach sex_id of local sex_ids {
			keep if location_id == `location_id' & year_id == `year_id' & sex_id == `sex_id'
			export delimited /*FILEPATH*/, replace
			use `data', clear
		}
	}
}

// save_results
save_results, modelable_entity_id(`target_me_id') description(Bprev asymp `me_id') in_dir(`out_dir'/asymp/) metrics(prevalence) mark_best(yes)
