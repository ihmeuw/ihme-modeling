*********************************************
** Description: This .do file deletes the results of previous runs
** of this step in the modeling process, submits a python script 
** that does the actual computation, and then checks for completed 
** results files. 
*********************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE

clear all
set more off
set maxvar 30000
version 13.0

local working_dir `c(pwd)'
local j "FILEPATH"

// directories
local out_dir "FILEPATH"

// locals
local acause_list "neonatal_sepsis"

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
		cd "`out_dir'/`acause'/`me_id'"
		local files: dir . files "*.csv"
		foreach file of local files {
			erase `file'
		}
	}
} 

cd "`working_dir'"


!qsub -l fthread=5 -l m_mem_free=2G -l h_rt=01:00:00 -l archive -q long.q -N "severity_split" -P "proj_custom_models" -e FILEPATH -o FILEPATH -cwd "FILEPATH/python_shell.sh" "FILEPATH/start_neonatal_sepsis.py"  
 
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
		di "checking for mild_prev_final_prev"
		capture noisily confirm file "FILEPATH/mild_prev_final_prev.csv"
		while _rc!=0 {
			di "File mild_prev_final_prev.csv for `acause' not found :["
			sleep 60000
			capture noisily confirm file "FILEPATH/mild_prev_final_prev.csv"

		}
		if _rc==0 {
			di "File mild_prev_final_prev.csv for `acause' found!"
		}

		di "checking for mild_count_scaled_check"
		capture noisily confirm file "FILEPATH/mild_count_scaled_check.csv"
		while _rc!=0 {
			di "File mild_count_scaled_check.csv for `acause' not found :["
			sleep 60000
			capture noisily confirm file "FILEPATH/mild_count_scaled_check.csv"
		}
		if _rc==0 {
			di "File mild_count_scaled_check.csv for `acause' found!"
		}

		di "checking for modsev_prev_final_prev"
		capture noisily confirm file "FILEPATH/modsev_prev_final_prev.csv"
		while _rc!=0 {
			di "File modsev_count_final_prev.csv for `acause' not found :["
			sleep 60000
			capture noisily confirm file "FILEPATH/modsev_prev_final_prev.csv"
		}
		if _rc==0 {
			di "File modsev_prev_final_prev.csv for `acause' found!"
		}

		di "checking for modsev_count_scaled_check"
		capture noisily confirm file "FILEPATH/modsev_count_scaled_check.csv"
		while _rc!=0 {
			di "File modsev_count_scaled_check.csv for `acause' not found :["
			sleep 60000
			capture noisily confirm file "FILEPATH/modsev_count_scaled_check.csv"
		}
		if _rc==0 {
			di "File modsev_count_scaled_check.csv for `acause' found!"
		}	
		
		di "checking for asymp_prev_final_prev"
		capture noisily confirm file "FILEPATH/asymp_prev_final_prev.csv"
		while _rc!=0 {
			di "File asymp_prev_final_prev.csv for `acause' not found :["
			sleep 60000
			capture noisily confirm file "FILEPATH/asymp_prev_final_prev.csv"
		}
		if _rc==0 {
			di "File asymp_prev_final_prev.csv for `acause' found!"
		}
	}
}
