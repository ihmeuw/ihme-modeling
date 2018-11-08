/*====================================================================
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2017
Output:           Submit script to extrapolate GBD2013 ONCHO prevalence draws
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "FILEPATH"
	}


* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2017"
	*model step
	local step 02
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories
		*set up base directories on shared
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"
		capture shell mkdir "FILEPATH"

	/*	*clear tempfiles, logs and progress files
		foreach dir in log progress tmp {
			capture cd ``dir'_dir'
			capture shell rm *.*
		}

	*clear temp files and output files created by this script
	local clear_output_dirs 1494 1495 2620 2515 2621 1496 1497 1498 1499
	foreach output_meid_dir in `clear_output_dirs' {
		capture cd `output_meid_dir'_dir
		capture shell rm *
	}
*/

*Directory for standard code files
	adopath + "FILEPATH"


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	capture mkdir "`log_dir'/`date'_`time'"
	*log using "`log_dir'/`date'_`time/02b_submit_extrapolate_log.smcl"
	***********************

	*print macros in log
	macro dir


/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Demographics

	get_demographics, gbd_team("ADDRESS") clear
		local gbdages `r(age_group_id)'
		local gbdyears `r(year_id)'
		local gbdsexes `r(sex_id)'

*--------------------1.2: Location Metadata

	get_location_metadata, location_set_id(35) clear
		keep if most_detailed==1
		levelsof location_id,local(gbdlocs)clean
		save "`tmp_dir'/metadata.dta", replace

*--------------------1.3: GBD Skeleton

	get_population, location_id("`gbdlocs'") sex_id("`gbdsexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
		save "`tmp_dir'/skeleton.dta", replace


/*====================================================================
                        2: Submit Extrapolation Script to Qsub
====================================================================*/


*--------------------2.2: Make Task Directory

use "`in_dir'/ocp_GBD2016_PreppedData.dta", clear
gen set = "ocp"
append using "`in_dir'/apoc_GBD2016_PreppedData.dta"
replace set = "apoc" if set == ""
keep location_id set
duplicates drop
merge 1:1 location_id using  "`tmp_dir'/metadata.dta"
replace set = "nonendemic" if set==""
keep location_id set
gen id = _n
*keep if set != "nonendemic"
*keep if location_id == 169
save "`tmp_dir'/task_directory.dta", replace

quietly sum id
local num_tasks `r(max)'

*--------------------2.3: Submit Array Job


	*remove spaces from locals to pass through qsub
		local gbdyears = subinstr("`gbdyears'", " ", "_", .)
		local gbdages = subinstr("`gbdages'", " ", "_", .)
		local gbdsexes = subinstr("`gbdsexes'", " ", "_", .)

	local jobname Oncho_extrap
	! qsub -N `jobname' -pe multi_slot 10 -t 1:`num_tasks' -P proj_tb -m aes -o "FILEPATH" "FILEPATH/stata_shell.sh"  "FILEPATH/extrapolate_gbd2017.do" "`gbdyears' `gbdages' `gbdsexes' `date' `time'"


/*

*--------------------2.2: ENDEMIC (OCP + APOC): Submit Extrapolation Scropt

	*Loop over ocp and apoc datasets
		foreach set in ocp apoc {

				di "`set' ." _continue

			*Pull in the dataset and get local list of locations
				use "`in_dir'/`set'_GBD2016_PreppedData.dta", clear
				levelsof location_id,local(locations_`set') clean

			*Loop over each location to create temp draws file and submit extrapolation script to qsub
				foreach location in `locations_`set''{

					di "`location' ." _continue

					*save temp file
						preserve
							quietly keep if location_id==`location'
							quietly save `tmp_dir'/`set'_preextrapolated_draws_`location'.dta, replace
						restore
						quietly drop if location_id == `location'

					*submit qsub
						local jobname location`location'_Oncho_extrapolation
						*! qsub -N `jobname' -pe multi_slot 4 -l mem_free=8 -P proj_custom_models -m aes "FILEPATH/stata_shell.sh" "`code_dir'/extrapolate_gbd2017.do" "`location'" "`set'"
						!qsub -N `jobname' -pe multi_slot 4 -l mem_free=8 -P proj_custom_models -m aes "FILEPATH/stata_shell.sh" "`code_dir'/extrapolate_gbd2017.do" "`gbdyears' `gbdages' `gbdsexes' `location' `set'"

				}
		}

*--------------------2.3: NON-ENDEMIC (ALL ELSE): Submit Zeroes Script


	*Compile local lists of endemic and nonendemic locations
		local endemic `locations_ocp' `locations_apoc'
		local nonendemic: list gbdlocs - endemic
		local set nonendemic

	*Loop over each location to create temp zeroes file for all output meids and submit zeroes script to qsub
		foreach location in `nonendemic' {

			*submit qsub
				local jobname locyear`locyear'_Oncho_nonendemic
				*! qsub -N `jobname' -pe multi_slot 4 -l mem_free=8 -P proj_custom_models -m aes "FILEPATH/stata_shell.sh" "`code_dir'/extrapolate_gbd2017.do" "`location'" "`set'"
				!qsub -N `jobname' -pe multi_slot 4 -l mem_free=8 -P proj_custom_models -m aes "FILEPATH/stata_shell.sh" "`code_dir'/extrapolate_gbd2017.do" "`gbdyears' `gbdages' `gbdsexes' `location' `set'"

		}

*/

/*====================================================================
                        4: Monitor Submission
====================================================================*/

/*

*--------------------6.1: CREATE DATASET OF locations TO MARK COMPLETION STATUS

	    clear
	    set obs `=wordcount("`gbdlocs'")'
	    generate location_id = .

	    forvalues i = 1 / `=wordcount("`gbdlocs'")' {
	                    quietly replace location_id = `=word("`gbdlocs'", `i')' in `i'
	                    }

	    generate complete = 0

*--------------------6.2: GET READY TO CHECK IF ALL locations ARE COMPLETE

	    local pause 2
	    local complete 0
	    local incompleteLocations `gbdlocs'

	    display _n "Checking to ensure all locations are complete" _n


*--------------------6.3: ITERATIVELY LOOP THROUGH ALL locations TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE

		while `complete'==0 {

		*Are all locations complete?
			foreach location of local incompleteLocations {
			            capture confirm file /`progress_dir'/`location'.txt
			            if _rc == 0 quietly replace complete = 1 if location_id==`location'
			            }

			quietly count if complete==0

		*If all locations are complete submit save results jobs
			if `r(N)'==0 {
				display "All locations complete!!!"
				local complete 1
			}


         *If all locations are not complete, inform the user and pause before checking again
			else {
				quietly levelsof location_id if complete==0, local(incompleteLocations) clean
					display "The following locations remain incomplete:" _n _col(3) "`incompleteLocations'" _n "Pausing for `pause' minutes" _continue

					forvalues sleep = 1/`=`pause'*6' {
						sleep 10000
						if mod(`sleep',6)==0 di "`=`sleep'/6'" _continue
						else di "." _continue
					}

					di _n

				}
			}

*/

/*====================================================================
                        5: Save Results
====================================================================*/


*SAVE RESULTS
exit
run "FILEPATH"

foreach meid in 1494 1495 2620 2515 2621 1496 1497 1498 1499 {

*local draws_dir "FILEPATH"
*local description "gbd2015 methods - eg draws extrapolation, added Ethiopia and Sudan/South Sudan fix"
*local filepattern "{location_id}.csv"

save_results_epi, input_dir("FILEPATH") input_file_pattern("{location_id}.csv") modelable_entity_id("`meid'") description("Adopted methods from gbd2015. New extrapolation method using a decaying functions (prev space). Incorporated subnational data for Ethiopia, and fixed Yemen") measure_id(5) clear
}





*log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.
