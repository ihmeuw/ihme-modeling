* do FILENAME/01a_par_adjustment_xxx.do

*** BOILERPLATE ***
	clear all
	set more off
  
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		local j "J:"
		}


	

*** LOAD SHARED FUNCTIONS ***  
	adopath + FILEPATH
	run FILEPATH.ado
	run FILEPATH.ado
	run FILEPATH.ado
	
	
	local outDir FILEPATH


	// get demographics
    get_location_metadata, location_set_id(9) clear
    keep if most_detailed == 1 & is_estimate == 1

    // set locations for parallelization
    levelsof location_id, local(locations)
    keep ihme_loc_id location_id location_name region_name

 local locations 206



	foreach location of local locations {	
		! qsub -P proj_custom_models -pe multi_slot 8 -N schisto_par_`location' "FILEPATH.sh" "`location'" 
		sleep 100
		}


		
/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/ 	


*** CREATE DATASET OF LOCATIONS TO MARK COMPLETION STATUS ***	
	keep location_id 
	duplicates drop
	generate complete = 0
	levelsof location_id, local(locations) clean 
   
*** GET READY TO CHECK IF ALL LOCATIONS ARE COMPLETE ***   
    local pause 2
	local complete 0
	local incompleteLocations `locations'
	
	display _n "Checking to ensure all locations are complete" _n

	
*** ITERATIVELY LOOP THROUGH ALL LOCATIONS TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE ***	
	while `complete'==0 {
	
	* Are all locations complete?
	  foreach location of local incompleteLocations {
		capture confirm file FILEPATH/`location'.txt 
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  * If all locations are complete submit save results jobs 
	  if `r(N)'==0 {
	    display "All locations complete." _n "Submitting save_results. " 
		local complete 1
		
		local mark_best no
		
		run /home/j/temp/central_comp/libraries/current/save_results.do
		save_results, modelable_entity_id(2797) mark_best(`mark_best') env("prod") file_pattern({location_id}.csv) description("All-species PAR adjustment `date', dm-173441 with updated CHN PAR") in_dir(`outDir'/2797)
		}
	

	  * If all locations are not complete, inform the user and pause before checking again
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
	
	