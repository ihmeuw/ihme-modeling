/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER      
Output:           Submit script to calculate asymptomatic cases
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "J:"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step 03
	*local root
	local localRoot "FILEPATH"
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

	*make all directories
	local make_dirs code in tmp local_tmp out log progress_dir
	foreach dir in `make_dirs' {
		capture mkdir `dir'_dir
	}
	*clear tempfiles, logs and progress files
	foreach dir in log progress tmp {
		capture cd `dir'_dir
		capture shell rm *
	}
	*clear temp files and output files created by this script
	local clear_output_dirs 3107
	foreach output_meid_dir in `clear_output_dirs' {
		capture cd `output_meid_dir'_dir
		capture shell rm *
	}
	

*Directory for standard code files
	adopath + FILEPATH


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/03_submit_asymptomatic_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir



/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Demographics

	get_demographics, gbd_team("epi") clear
	local gbdages="`r(age_group_ids)'"
	local gbdyears="`r(year_ids)'"
	local gbdsexes="`r(sex_ids)'"

*--------------------1.2: Location Metadata

	get_location_metadata,location_set_id(35) clear
	keep if most_detailed==1
	levelsof location_id,local(gbdlocs) clean

*--------------------1.3: GBD Skeleton/Population

	get_population, location_id("`gbdlocs'") sex_id("`gbdsexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
	save "`local_tmp_dir'/skeleton.dta", replace


/*====================================================================
                        2: Submit Asymptomatic Calculation Script to Qsub 
====================================================================*/


*--------------------2.1: Set MEIDs

	local squeezedmeids 2957 2958 3611
	local parent 1494
	local unsqueezedmeids 1495 2620 1496 2515 2621


*--------------------2.2: Submit script to qsub

	foreach location in `gbdlocs'{
		
		*Use gbd skeleton
			use "`local_tmp_dir'/skeleton.dta", clear
		
		*Save tmp file for each location
			preserve
				keep if location_id==`location'
				save `tmp_dir'/skeleton_`location'.dta, replace
			restore
			drop if location_id == `location'
		
		*Submit qsub
			! qsub -o FILEPATH -e FILEPATH -P proj_custom_models -pe multi_slot 8 -N onchoNFaSx_`location' "`localRoot'/submit_asymptomatic.sh" "`location'"
		
	}


/*====================================================================
                        3: Monitor Submission
====================================================================*/



*--------------------3.1: CREATE DATASET OF locations TO MARK COMPLETION STATUS

	    clear
	    set obs `=wordcount("`gbdlocs'")'
	    generate location_id = .
	    
	    forvalues i = 1 / `=wordcount("`gbdlocs'")' {
	                    quietly replace location_id = `=word("`gbdlocs'", `i')' in `i'
	                    }
	                    
	    generate complete = 0

*--------------------3.2: GET READY TO CHECK IF ALL locations ARE COMPLETE

	    local pause 2
	    local complete 0
	    local incompleteLocations `gbdlocs'
	    
	    display _n "Checking to ensure all locations are complete" _n

                
*--------------------3.3: ITERATIVELY LOOP THROUGH ALL locations TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE              
       
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
        


/*====================================================================
                        4: Save Results
====================================================================*/

/*

	local meid 3107
	local mark_best = "no"
	local env = "prod"
	local description "Africa unc adjustment, new americas v4 w/PAR #22"
	local in_dir = "FILEPATH"
	local file_pat = "{location_id}.csv"

	run FILEPATH/save_results.do

	save_results, modelable_entity_id(`meid') mark_best(`mark_best') env(`env') file_pattern(`file_pat') description(`description') in_dir(`in_dir')

*/




log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:


