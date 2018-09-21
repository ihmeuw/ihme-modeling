/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER    
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	clear mata
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
	local gbd = "gbd2016"
	*model step
	local step 04
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
	*directory for this step only
	capture mkdir "`out_dir'/trich"

*Directory for standard code files
	adopath + FILEPATH


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/04_submit_trich_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir



/*====================================================================
                        1: Setup
====================================================================*/


*--------------------1.1: GBD Location Metadata

	get_location_metadata, location_set_id(35)	
	keep if is_estimate==1
	levelsof location_id, local(locations) clean

*--------------------1.2: Set Locals

	local meid 3001
	local csmr none
	local description Trich outlier fix model add Theos extras
	local cause trich


/*====================================================================
                        2: Submit Adjustment Script to Qsub
====================================================================*/


*--------------------2.1: Submit Qsub

	foreach location of local locations {
		
		/*
		quietly {
			preserve
			keep if location_id==`location'
			save `tmp_dir'/`step'_`location'.dta, replace
			restore
		}
		*/

	! qsub -N `cause'_`location' -P proj_custom_models -pe multi_slot 2  "FILEPATH.sh" "`location'"
				
	}



/*====================================================================
                        3: Monitor Results
====================================================================*/


*--------------------3.1: Monitor and Submit Save Results 

	*CREATE DATASET OF LOCATIONS TO MARK COMPLETION STATUS ***	
		keep location_id 
		duplicates drop
		generate complete = 0
		levelsof location_id, local(locations) clean 
	   
	*GET READY TO CHECK IF ALL LOCATIONS ARE COMPLETE ***   
	    local pause 2
		local complete 0
		local incompleteLocations `locations'
		
		display _n "Checking to ensure all locations are complete" _n

		
	*ITERATIVELY LOOP THROUGH ALL LOCATIONS TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE ***			
		while `complete' == 0 {
		
		foreach location of local incompleteLocations {
			capture confirm file `rootDir'/progress/`location'.txt 
			if _rc == 0 quietly replace complete = 1 if location_id == `location'
		}

		quietly count if complete==0

		*If all locations are complete submit save results jobs 
		if `r(N)' == 0 {
			display "All locations complete for cause `cause'." _n "Submitting save_results. " 
			local complete 1

			local mark_best no

			run FILEPATH/save_results.do
			save_results, modelable_entity_id(`meid') mark_best(`mark_best') file_pattern({location_id}.csv) description("`description'") in_dir(`out_dir'/trichfix) env("prod")
		}


		*If all locations are not complete, inform the user and pause before checking again
		else {
			quietly levelsof location_id if complete==0, local(incompleteLocations) clean
			display "The following locations remain incomplete for cause `cause':" _n _col(3) "`incompleteLocations'" _n "Pausing for `pause' minutes" _continue

			forvalues sleep = 1/`=`pause'*6' {
				sleep 10000
				if mod(`sleep',6)==0 di "`=`sleep'/6'" _continue
					else di "." _continue
				}
			di _n
			}

		}
		
	





log close
exit
/* End of do-file */
