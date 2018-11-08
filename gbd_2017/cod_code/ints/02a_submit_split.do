 
*** BOILERPLATE ***
	clear all
	set more off, perm

	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
		}
		
	else if c(os) == "Windows" {
		local j "FILEPATH"
		}


	run "FILEPATH"


	
	
*** ENSURE THAT OUTPUT DIRECTORIES EXIST ***
	local states death death_hiv inf_sev
	local modelMeid 20291
	
	capture mkdir "FILEPATH"
	
	local rootDir "FILEPATH"
		

	!rm -rf `rootDir'

	capture mkdir `rootDir'
	capture mkdir `rootDir'/temp
	capture mkdir `rootDir'/parent
	capture mkdir `rootDir'/logs
	capture mkdir `rootDir'/logs/progress

	foreach state of local states {
		capture mkdir `rootDir'/`state'
		}

*/


		
*** GET LIST OF LOCATIONS ***	
	get_location_metadata, location_set_id(35) clear
	keep if is_estimate==1
	levelsof location_id, local(locations) clean
	
	tempfile locMeta
	save `locMeta'

	
	

*** PREP CASE FATALITY & COINFECTION DRAWS *** 
	cp "FILEPATH"/cfrDrawDeviations.txt `rootDir'/temp/cfrDrawDeviations.txt, replace
	import delimited using ""FILEPATH"/cfrEstimates.csv", clear case(preserve)
	reshape wide logitPred logitPredSeSm, i(location_id year_id age_group_id) j(estPrHiv)

	merge 1:m location_id year_id age_group_id using ""FILEPATH"/hivRRs.dta", assert(3) nogenerate
				   
	
    
*** SUBMIT BASH FILES ***
	foreach location of local locations {
		preserve
		keep if location_id==`location'
		save `rootDir'/temp/cfr_`location'.dta, replace
		restore
		
		drop if location_id==`location'
		
		! qsub -P proj_custom_models -pe multi_slot 2 -N split_`location' ""FILEPATH"/02b_submit_split.sh" "`location'" "`modelMeid'"
		sleep 100
		}

			
	use `locMeta', clear
	
	

/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/ 	

*** GET DISMOD MODEL IDS FOR DESCRIPTIONS ***
	preserve
	run "FILEPATH"/get_best_model_versions.ado
	get_best_model_versions, entity(modelable_entity) ids(`modelMeid') clear
	levelsof model_version_id, sep(,) clean local(dm)
	restore

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
		capture confirm file `rootDir'/logs/progress/`location'.txt 
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  * If all locations are complete submit save results jobs 
	  if `r(N)'==0 {
	    display "All locations complete." _n "Submitting save_results. " 
		local complete 1
		
		local mark_best False
		
		run "FILEPATH"/save_results_epi.ado
		run "FILEPATH"/save_results_cod.ado

		save_results_cod, cause_id(959) mark_best("`mark_best'") description("Natural history model using first estimates of CFR and HIV coinfection (dm models `dm')") input_dir("FILEPATH") input_file_pattern({location_id}.csv) metric_id(3) clear
		save_results_cod, cause_id(959) mark_best("False") description("Natural history model of HIV attributed iNTS DEATHS - DO NOT BEST!!! (dm models `dm')") input_dir("FILEPATH") input_file_pattern({location_id}.csv) metric_id(3) clear
				
		save_results_epi, modelable_entity_id(19680) mark_best("`mark_best'") input_file_pattern({location_id}.csv) description("Severe iNTS (dm models `dm')") input_dir("FILEPATH") clear

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
	
	
  
