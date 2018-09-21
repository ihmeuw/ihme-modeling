
* BOILERPLATE *
  clear all
  set maxvar 12000
  set more off
   
  if c(os) == "Unix" {
    local j "FILEPATH"
    set odbcmgr unixodbc
    }
  else if c(os) == "Windows" {
    local j "FILEPATH"
    }
	
  
  adopath + FILEPATH
  run FILEPATH/get_location_metadata.ado	

  
  tempfile endemic
  

* CREATE DIRECTORIES * 
  local outDir FILEPATH
  tokenize "`outDir'", parse(/)
  while "`1'"!="" {
	local path `path'`1'`2'
	capture mkdir `path'
	macro shift 2
	}

  !rm -rf `outDir'/progress
  
  foreach subDir in inputs progress 1451 1452 1453 1454 2413 3077 10141 {
	!rm -rf `outDir'/`subDir'
    sleep 2000
	capture mkdir `outDir'/`subDir'
	}

  capture mkdir FILEPATH
 
  local meid_acute 1451
  local meid_afib  1452
  local meid_hf    2413
  local meid_asymp 3077
  local meid_total 10141
  local meid_digest_mild 1453
  local meid_digest_mod  1454 

 
* BUILD LIST OF ENDEMIC LOCATIONS * 
  get_location_metadata, location_set_id(8) clear
  keep if is_estimate==1 
  
  generate endemic =(strmatch(lower(region_name), "*latin america*") | inlist(ihme_loc_id, "BLZ", "GUY", "SUR"))
  keep location_id endemic


  merge 1:m location_id using FILEPATH/latinAmericanMigrants.dta, nogenerate
  
 
  levelsof location_id if endemic==1, local(endemicLocations) clean
  levelsof location_id if missing(draw_0) & endemic!=1, local(zeroLocations) clean
  levelsof location_id if !missing(draw_0) & endemic!=1,  local(nonZeroLocations) clean
  
  
 
  
  preserve
  use FILEPATH/chronicSeqPrDraws.dta, clear
  foreach location in `endemicLocations' `nonZeroLocations' `zeroLocations' {
    quietly save `outDir'/inputs/chronicPr_`location'.dta, replace
	di "." _continue
	}
  restore
  

 foreach location in `endemicLocations' {
   ! qsub -P proj_custom_models -pe multi_slot 8 -N chagas_`location' "FILEPATH/04b_submitFirstSplit.sh" "`location'" "1"
   sleep 100
   }
  

  
  foreach location in `nonZeroLocations' {
   preserve
   keep if location_id==`location'
   save `outDir'/inputs/migrantPrev_`location'.dta, replace
   restore
   
   ! qsub -P proj_custom_models -pe multi_slot 8 -N chagas_`location' "FILEPATH/04b_submitFirstSplit.sh" "`location'" "0"
   }
 
 
  foreach location of local zeroLocations {
   ! qsub -P proj_custom_models -pe multi_slot 8 -N chagas_`location' "FILEPATH/04e_submitZeros.sh" "`location'" 
   sleep 100
   }
 
 
 	
/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/ 	

*** GET DISMOD MODEL IDS FOR DESCRIPTIONS ***
	preserve
	get_best_model_versions, entity(modelable_entity) ids(1450) clear
	levelsof model_version_id, clean local(dm)
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
		capture confirm file `outDir'/progress/`location'.txt 
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  * If all locations are complete submit save results jobs 
	  if `r(N)'==0 {
	    display "All locations complete." _n "Submitting save_results. " 
		local complete 1
		
		local mark_best yes
		
		run /home/j/temp/central_comp/libraries/current/save_results.do
		save_results, modelable_entity_id(3077) mark_best(`mark_best') file_pattern({location_id}.csv) description("Asymptomatic Chagas infections (dm models `dm')") in_dir(`outDir'/3077) env("prod")
		save_results, modelable_entity_id(1451) mark_best(`mark_best') file_pattern({location_id}.csv) description("Acute Chagas (dm models `dm')") in_dir(`outDir'/1451) env("prod")
		save_results, modelable_entity_id(1452) mark_best(`mark_best') file_pattern({location_id}.csv) description("Afib due to Chagas (dm models `dm')") in_dir(`outDir'/1452) env("prod")
		save_results, modelable_entity_id(1453) mark_best(`mark_best') file_pattern({location_id}.csv) description("Mild Chagas megaviscera (dm models `dm')") in_dir(`outDir'/1453) env("prod")
		save_results, modelable_entity_id(1454) mark_best(`mark_best') file_pattern({location_id}.csv) description("Moderate Chagas megaviscera (dm models `dm')") in_dir(`outDir'/1454) env("prod")
		save_results, modelable_entity_id(2413) mark_best(`mark_best') file_pattern({location_id}.csv) description("Heart failure due to Chagas (dm models `dm')") in_dir(`outDir'/2413) env("prod")
		save_results, modelable_entity_id(10141) mark_best(`mark_best') file_pattern({location_id}.csv) description("All Chagas infections (elimiation adjusted) (dm models `dm')") in_dir(`outDir'/10141) env("prod")
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
	
 
 