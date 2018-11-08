
  clear all
  set more off

  local codCorrectVersion 84
  
* ENSURE THAT OUTPUT DIRECTORY EXISTS *

  run FILEPATH/get_demographics.ado

  get_demographics, gbd_team(ADDRESS) clear
  local locations `r(location_id)'
  local years `r(year_id)'
  local ages `r(age_group_id)'
  local currentYear = max(`=subinstr("`years'", " ", ",", .)')
  
  
  capture mkdir FILEPATH
  capture mkdir FILEPATH
  capture mkdir FILEPATH

  !rm -rf FILEPATH
  capture mkdir FILEPATH
	

   
* SUBMIT BASH FILES *
  foreach location_id of local locations {
    ! qsub  -P proj_custom_models -pe multi_slot 8 -N d2c_`location_id' "FILEPATH/submit_deaths2cases.sh" "`location_id'" "`=subinstr("`years'", " ", "_", .)'" "`=subinstr("`ages'", " ", "_", .)'"
  	sleep 1000
	}
	

	
/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/ 	


*** CREATE DATASET OF LOCATIONS TO MARK COMPLETION STATUS ***	
	clear
	set obs `=wordcount("`locations'")'
	generate location_id = .
	
	forvalues i = 1 / `=wordcount("`locations'")' {
		quietly replace location_id = `=word("`locations'", `i')' in `i'
		}
		
	generate complete = 0

   
*** GET READY TO CHECK IF ALL LOCATIONS ARE COMPLETE ***   
    local pause 2
	local complete 0
	local incompleteLocations `locations'
	
	display _n "Checking to ensure all locations are complete" _n

	
*** ITERATIVELY LOOP THROUGH ALL LOCATIONS TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE ***	
	while `complete'==0 {
	
	* Are all locations complete?
	  foreach location of local incompleteLocations {
		capture confirm file /ihme/scratch/users/stanaway/rabies/gbd`currentYear'/progress/`location'.txt 
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  * If all locations are complete submit save results jobs 
	  if `r(N)'==0 {
	    display "All locations complete." _n "Submitting save_results. " 
		local complete 1
		
		local mark_best True
	
		run FILEPATH/save_results_epi.ado

		save_results_epi, modelable_entity_id(1512) mark_best(`mark_best') input_file_pattern({location_id}.csv) description("Rabies (dervied from CodCorrect #`codCorrectVersion')") input_dir(FILEPATH/inf_sev) clear
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
	