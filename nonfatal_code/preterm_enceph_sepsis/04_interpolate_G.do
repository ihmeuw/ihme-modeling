
	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix /*FILEPATH*/
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix /*FILEPATH*/
	}
	
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir /*FILEPATH*/ dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file /*FILEPATH*/
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

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
			ssc install estout, replace 
			ssc install metan, replace
			local working_dir = /*FILEPATH*/ 
		} 
		

local me_ids 1557 1558 1559 2525 1594

adopath + /*FILEPATH*/

// directories
local data_dir /*FILEPATH*/



// Submit jobs
foreach me_id of local me_ids {

	// get acause
	if `me_id' == 1557 | `me_id' == 1558 | `me_id' == 1559 {
		di "Me_id is `me_id'"
		local acause "neonatal_preterm"
	}

	if `me_id' == 2525 {
		di "Me_id is `me_id'"
		local acause "neonatal_enceph"
	}

	if `me_id' == 1594 {
		di "Me_id is `me_id'"
		local acause "neonatal_sepsis"
	}

	// delete any old interpolation results
	di "Me_id is `me_id' and acause is `acause'"
	cd /*FILEPATH*/
	!ls
	!pwd
	!rm *csv
	!rm *dta
	cd /*FILEPATH*/
	!ls
	!pwd
	!rm *csv
	!rm *dta
	cd /*FILEPATH*/
	!ls
	!pwd
	!rm *csv
	!rm *dta
	cd /*FILEPATH*/
	!ls
	!pwd
	!rm *csv
	!rm *dta
	

	/* QSUB TO NEXT SCRIPT */	
}


// wait for final results files to finish 
foreach me_id of local me_ids {

	// get acause
	if `me_id' == 1557 | `me_id' == 1558 | `me_id' == 1559 {
		di "Me_id is `me_id'"
		local acause "neonatal_preterm"
	}

	if `me_id' == 2525 {
		di "Me_id is `me_id'"
		local acause "neonatal_enceph"
	}

	if `me_id' == 1594 {
		di "Me_id is `me_id'"
		local acause "neonatal_sepsis"
	}

	capture noisily confirm file /*FILEPATH*/

	while _rc!=0 {
		di "File /*FILEPATH*/ for `acause' `me_id' not found :["
		sleep 60000
		capture noisily confirm file /*FILEPATH*/
	}
	if _rc==0 {
		di "File /*FILEPATH*/ for `acause' `me_id' found!"
	}		

}
