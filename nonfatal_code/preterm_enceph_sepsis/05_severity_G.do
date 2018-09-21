
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
	local working_dir = /*FILEPATH*/
} 


// directories
local out_dir /*FILEPATH*/

// locals
local acause_list " "neonatal_preterm" "neonatal_enceph" "neonatal_sepsis" "


****************************************************************

// first, remove all previous files 
foreach acause of local acause_list {

	di "get me_id_list"
	if "`acause'" == "neonatal_preterm" {
		local me_id_list 1557 1558 1559

	}
	if "`acause'" == "neonatal_enceph" {
		local me_id_list 2525
	}
	if "`acause'" == "neonatal_sepsis" {
		local me_id_list 1594
	}

	foreach me_id of local me_id_list {
		di "removing old files"
		cd /*FILEPATH*/
		local files: dir . files "*.csv"
		foreach file of local files {
			erase `file'
		}
	}
} 

// qsub the upper-level severity script (and custom shell)
/* QSUB TO NEXT SCRIPT */

// wait until results files have been generated
foreach acause of local acause_list {

	di "get me_id_list"
	if "`acause'" == "neonatal_preterm" {
		local me_id_list 1557 1558 1559 
	}
	if "`acause'" == "neonatal_enceph" {
		local me_id_list 2525
	}
	if "`acause'" == "neonatal_sepsis" {
		//local me_id_list 9793
		local me_id_list 1594
	}

	foreach me_id of local me_id_list {
		di "checking for mild_prev_final_prev"
		capture noisily confirm file /*FILEPATH*/
		while _rc!=0 {
			di "File /*FILEPATH*/ for `acause' not found :["
			sleep 60000
			capture noisily confirm file /*FILEPATH*/
		}
		if _rc==0 {
			di "File /*FILEPATH*/ for `acause' found!"
		}

		di "checking for mild_count_scaled_check"
		capture noisily confirm file /*FILEPATH*/
		while _rc!=0 {
			di "File /*FILEPATH*/ for `acause' not found :["
			sleep 60000
			capture noisily confirm file /*FILEPATH*/
		}
		if _rc==0 {
			di "File /*FILEPATH*/ for `acause' found!"
		}

		di "checking for modsev_prev_final_prev"
		capture noisily confirm file /*FILEPATH*/
		while _rc!=0 {
			di "/*FILEPATH*/ for `acause' not found :["
			sleep 60000
			capture noisily confirm file "/*FILEPATH*/"
		}
		if _rc==0 {
			di "File /*FILEPATH*/ for `acause' found!"
		}

		di "checking for modsev_count_scaled_check"
		capture noisily confirm file /*FILEPATH*/
		while _rc!=0 {
			di "File /*FILEPATH*/ for `acause' not found :["
			sleep 60000
			capture noisily confirm file /*FILEPATH*/
		}
		if _rc==0 {
			di "File /*FILEPATH*/ for `acause' found!"
		}	
		
		di "checking for asymp_prev_final_prev"
		capture noisily confirm file /*FILEPATH*/
		while _rc!=0 {
			di "File /*FILEPATH*/ for `acause' not found :["
			sleep 60000
			capture noisily confirm file /*FILEPATH*/
		}
		if _rc==0 {
			di "File /*FILEPATH*/ for `acause' found!"
		}
	}
}
