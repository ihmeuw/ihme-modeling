clear all 
set more off
set maxvar 30000
version 13.0

// priming the working environment
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

// locals
local acause_list " "neonatal_preterm" "neonatal_enceph" "neonatal_sepsis" "

// directories
local out_dir /*FILEPATH*/

*******************************************************

// first remove all previous files
foreach acause of local acause_list {

	di "finding target me_id"
	if "`acause'" == "neonatal_preterm" {
		local target_me_ids 8621 8622 8623
	}
	if "`acause'" == "neonatal_enceph" {
		local target_me_ids 8653
	}
	if "`acause'" == "neonatal_sepsis" {
		local target_me_ids 8674
	}

}



// submit jobs that format and save_results
foreach acause of local acause_list {

	// find me_ids
	if "`acause'" == "neonatal_preterm" {
		local me_id_list 1557 1558 1559 
	}

	if "`acause'" == "neonatal_enceph" {
		local me_id_list 2525
	}
	
	if "`acause'" == "neonatal_sepsis" {
		local me_id_list 1594
	}

	// submit jobs
	foreach me_id of local me_id_list {
		/* QSUB TO NEXT SCRIPT */
	}
}	


// wait until results files have been created
foreach acause of local acause_list {

	di "finding target me_id"
	if "`acause'" == "neonatal_preterm" {
		local target_me_ids 8621 8622 8623
	}
	if "`acause'" == "neonatal_enceph" {
		local target_me_ids 8653
	}
	if "`acause'" == "neonatal_sepsis" {
		local target_me_ids 8674
	}

	foreach target_me_id of local target_me_ids {

		capture noisily confirm file /*FILEPATH*/
		while _rc!=0 {
			di "File /*FILEPATH*/ not found :["
			sleep 60000
			capture noisily confirm file  /*FILEPATH*/
		}
		if _rc==0 {
			di "File /*FILEPATH*/ found!"
		}	
	}
}
	
