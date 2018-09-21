
 
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
		
    adopath + FILEPATH

	local cause // SET CAUSE HERE
	
			
	else if "`cause'"=="lf" {
		local meid 1492
		local meid_prev 1491
		local csmr none
		local description Test of `cause' incidence model
		local inFile FILEPATH/master.dta
		local shellFile FILEPATH/submitIncidenceParallel.sh
		}
		
	else if "`cause'"=="schisto" {
		local meid 1468
		local meid_prev 2797
		local csmr 351
		local description Test of `cause' incidence model
		local inFile FILEPATH/master.dta
		local shellFile FILEPATH/submitIncidenceParallel.sh
		}
		
	else if "`cause'"=="oncho" {
		local meid 1495
		local meid_prev 1494
		local csmr none
		local description Test of `cause' incidence model
		local inFile FILEPATH/master.dta
		local shellFile FILEPATH/submitIncidenceParallel.sh
		}
	
	
*** ENSURE THAT OUTPUT DIRECTORIES EXIST ***
	local rootDir FILEPATH
	local logDir FILEPATH


	local path
	tokenize "`rootDir'", parse(/)
	while "`1'"!="" {
		local path `path'`1'`2'
		capture mkdir `path'
		macro shift 2
		}
		
	local path   
	tokenize "`logDir'", parse(/)
	while "`1'"!="" {
		local path `path'`1'`2'
		capture mkdir `path'
		macro shift 2
		}
	
	
	foreach subDir in draws temp progress {
		sleep 1000
		!rm -rf `rootDir'/`subDir'
		capture mkdir `rootDir'/`subDir'
		}
 
 
*** LOAD MASTER FILE ***		
	use `inFile', clear
	keep if is_estimate==1
	
	levelsof location_id, local(locations) clean


*** SUBMIT BASH FILES ***
	foreach location of local locations {
		quietly {
			preserve
			keep if location_id==`location'
			save FILEPATH/`location'.dta, replace
			restore
			}
		
		! qsub -P proj_custom_models -pe multi_slot 8 -N `cause'_inc_`location' "`shellFile'" "`location'" "`meid'" "`rootDir'" "`logDir'" "`csmr'" "`meid_prev'"
		
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
	    display "All locations complete for cause `cause'." _n "Submitting save_results. " 
		local complete 1
		
		local mark_best no
		
		run FILEPATH/save_results.do
		save_results, modelable_entity_id(`meid') mark_best(`mark_best') file_pattern({location_id}.csv) description("`description'") in_dir(FILEPATH) env("prod")
		}
	

	  * If all locations are not complete, inform the user and pause before checking again
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
	
	
