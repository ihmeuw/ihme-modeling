
// priming the working environment
clear 
set more off
set maxvar 30000
version 13.0


// discover root 
		if c(os) == "Windows" {
			local j /*FILEPATH*/
			// Load the PDF appending application
			quietly do /*FILEPATH*/
			local working_dir = /*FILEPATH*/ 
		}
		if c(os) == "Unix" {
			local j /*FILEPATH*/
			local working_dir = /*FILEPATH*/
		} 

// arguments
local me_id `1'
local acause `2'

adopath + /*FILEPATH*/

run /*FILEPATH*/


// directories
local data_dir /*FILEPATH*/

**********************************************************************************************


// launch interpolation jobs

get_location_metadata, location_set_id(9) gbd_round_id(4) clear
levelsof location_id, local(location_ids)
local year_ids 1990 1995 2000 2005 2010 2016 
local sex_ids 1 2 

foreach location_id of local location_ids {
	foreach year_id of local year_ids{
		foreach sex_id of local sex_ids {
					
			/* QSUB TO NEXT SCRIPT */

		}
	}
}


// wait until interpolation jobs are done
cap mkdir /*FILEPATH*/
cd /*FILEPATH*/
!qstat -u /*USERNAME*/ |grep inter* |wc -l > /*FILEPATH*/
import delimited /*FILEPATH*/, clear 
local job_count v1
while `job_count' > 0 {
	di "`job_count' jobs are not finished. Take a nap."
	!rm *csv
	sleep 10000
	!qstat -u /*USERNAME*/ |grep inter* |wc -l > /*FILEPATH*/
	import delimited /*FILEPATH*/, clear
	local job_count v1	
}
if `job_count' == 0 {
	di "Jobs are all done!"
}

// check to see if all the files are present



foreach location_id of local location_ids {
	foreach year_id of local year_ids{
		foreach sex_id of local sex_ids {
			capture noisily confirm file /*FILEPATH*/
			if _rc!=0 {
				di "/*FILEPATH*/ is missing."
				local failed_locations `failed_locations' `location_id'
				local failed_years `failed_years' `year_id'
				local failed_sexes `failed_sex' `sex_id'
			}
			else if _rc==0 {
				di "File /*FILEPATH*/found!"
			}
			
		}
	}
}

// check again to see if everything's done
foreach location_id of local location_ids {
	foreach year_id of local year_ids{
		foreach sex_id of local sex_ids {
			capture noisily confirm file "/*FILEPATH*/"
			while _rc!=0 {
				di "File /*FILEPATH*/ not found"
				sleep 60000
				capture noisily confirm file /*FILEPATH*/
			}
			if _rc==0 {
				di "File /*FILEPATH*/ found!"
			}
			
		}
	}
}

di "Done checking!"


// generate appended 28 day draws file for use in future steps

cd /*FILEPATH*/
!cat *csv > /*FILEPATH*/
import delimited /*FILEPATH*/, varnames(1) clear
capture noisily drop if age_group_id == "age_group_id"
quietly ds 
local _all = "`r(varlist)'"
foreach var of varlist _all {
	di "Var is `var'"
	gen `var'_copy = real(`var')
}
keep *copy
foreach var of local _all {
	di "Var is `var'"
	rename `var'_copy `var'
}
save /*FILEPATH*/, replace

// generate appended at birth draws file for use in future steps
cd /*FILEPATH*/
!cat *csv > /*FILEPATH*/
import delimited /*FILEPATH*/, varnames(1) clear
capture noisily drop if age_group_id == "age_group_id"
quietly ds 
local _all = "`r(varlist)'"
foreach var of varlist _all {
	di "Var is `var'"
	gen `var'_copy = real(`var')
}
keep *copy
foreach var of local _all {
	di "Var is `var'"
	rename `var'_copy `var'
}
save /*FILEPATH*/, replace

// generate appended 0-6 draws file for use in future steps
cd /*FILEPATH*/
!cat *csv > /*FILEPATH*/
import delimited /*FILEPATH*/, varnames(1) clear
capture noisily drop if age_group_id == "age_group_id"
quietly ds 
local _all = "`r(varlist)'"
foreach var of varlist _all {
	di "Var is `var'"
	gen `var'_copy = real(`var')
}
keep *copy
foreach var of local _all {
	di "Var is `var'"
	rename `var'_copy `var'
}
save /*FILEPATH*/, replace

// generate appended 7-27 draws file for use in future steps
cd /*FILEPATH*/
!cat *csv > /*FILEPATH*/
import delimited /*FILEPATH*/, varnames(1) clear
capture noisily drop if age_group_id == "age_group_id"
quietly ds 
local _all = "`r(varlist)'"
foreach var of varlist _all {
	di "Var is `var'"
	gen `var'_copy = real(`var')
}
keep *copy
foreach var of local _all {
	di "Var is `var'"
	rename `var'_copy `var'
}
save /*FILEPATH*/, replace

// save results 

	// get target me_id - where the results will be saved
	if `me_id' == 1557 | `me_id' == 1558 | `me_id' == 1559 {
		di "Me_id is `me_id'"
		local target_me_id = `me_id' + 7058
	}

	if `me_id' == 2525 {
		di "Me_id is `me_id'"
		local target_me_id = 3961
	}

	if `me_id' == 1594 {
		di "Me_id is `me_id'"
		local target_me_id = 3963
	}

