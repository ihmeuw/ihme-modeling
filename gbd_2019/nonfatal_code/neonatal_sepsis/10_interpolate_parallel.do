************************************************************
** Description: This is the parallelized script submitted by 04_interpolate.do.
** It saves files for each me_id for preterm, enceph and sepsis in the proper 
** format and saves them using save_results. 
************************************************************


// priming the working environment
clear 
set more off
set maxvar 30000
version 13.0


// root 
local j "FILEPATH"


// arguments
//local me_id `1'
//local acause `2'
local working_dir `c(pwd)'

// test
local me_id 1594
local acause "neonatal_sepsis"

adopath + "FILEPATH"

run "`j'/FILEPATH/get_location_metadata.ado"

local data_dir "FILEPATH"

**********************************************************************************************


// launch interpolation jobs

get_location_metadata, location_set_id(9) gbd_round_id(6) clear
levelsof location_id, local(location_ids)
local year_ids 1990 1995 2000 2005 2010 2015 2017 2019
local sex_ids 1 2 

foreach location_id of local location_ids {
	foreach year_id of local year_ids	{
		foreach sex_id of local sex_ids {
					
			if  "`acause'" == "neonatal_sepsis"  {
				!qsub -l fthread=1 -l m_mem_free=4G -l h_rt=00:15:00 -l archive -q all.q -N interpolate_`me_id'_`location_id'_`year_id'_`sex_id' -P proj_neonatal -e FILEPATH -o FILEPATH -cwd "FILEPATH/stata_shell.sh" "FILEPATH/04_interpolate_sepsis_parallel_parallel.do" "`me_id' `acause' `location_id' `year_id' `sex_id'"
			}

		}
	}
}


// wait until interpolation jobs are done
cap mkdir "FILEPATH"
cd "FILEPATH"
!qstat -u `c(username)' |grep inter* |wc -l > job_counts.csv
import delimited "job_counts.csv", clear 
local job_count v1
while `job_count' > 0 {
	di "`job_count' jobs are not finished."
	!rm *csv
	sleep 10000
	!qstat -u `c(username)' |grep inter* |wc -l > job_counts.csv
	import delimited "job_counts.csv", clear
	local job_count v1	
}
if `job_count' == 0 {
	di "Jobs are all done!"
}

di "Please run 10b_interpolate_check.R"

// check again to see if everything's done
foreach location_id of local location_ids {
	foreach year_id of local year_ids{
		foreach sex_id of local sex_ids {
			capture noisily confirm file "FILEPATH/5_`location_id'_`year_id'_`sex_id'.csv"
			while _rc!=0 {
				di "File 5_`location_id'_`year_id'_`sex_id'.csv not found :["
				sleep 60000
				capture noisily confirm file "FILEPATH/5_`location_id'_`year_id'_`sex_id'.csv"
			}
			if _rc==0 {
				di "File 5_`location_id'_`year_id'_`sex_id'.csv found!"
			}
			
		}
	}
}

di "Done checking!"


// generate appended 28 day draws file for use in future steps
cd "FILEPATH"
!cat *csv > all_draws.csv
import delimited "FILEPATH/all_draws.csv", varnames(1) clear
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
save "FILEPATH/all_draws.dta", replace

// generate appended at birth draws file for use in future steps
cd "FILEPATH"
!cat *csv > all_draws.csv
import delimited "FILEPATH/all_draws.csv", varnames(1) clear
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
save "FILEPATH/all_draws.dta", replace

// generate appended 0-6 draws file for use in future steps
cd "FILEPATH"
!cat *csv > all_draws.csv
import delimited "FILEPATH/all_draws.csv", varnames(1) clear
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
save "FILEPATH/all_draws.dta", replace

// generate appended 7-27 draws file for use in future steps
cd "FILEPATH"
!cat *csv > all_draws.csv
import delimited "FILEPATH/all_draws.csv", varnames(1) clear
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
save "FILEPATH/all_draws.dta", replace
