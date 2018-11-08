*** BOILERPLATE ***
	clear all
	set more off 
	
	capture log close
	log using FILEPATH/birthPrevSubmission.smcl, replace

	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
		}
		
	else if c(os) == "Windows" {
		local j "FILEPATH"
		}


	adopath + FILEPATH
	
	
*** SET ENVIRONMENTAL LOCALS (PATHS, FILENAMES MODEL NUBMERS, ETC) ***
	tempfile locMeta pregTemp  

	local meid_acute 1451
	local outDir FILEPATH
 
	
	!rm -rf `outDir'/progress
	mkdir `outDir'/progress
	
	sleep 2000
	!rm -rf `outDir'/`meid_acute'b
	mkdir `outDir'/`meid_acute'b


	
	use "FILEPATH/prPreg.dta", clear
	replace year_id = year_id + 1
	drop ihme_loc_id
	keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2017)
	save `pregTemp'

	get_location_metadata, location_set_id(35) clear
	keep if inlist(location_id, 4727, 4728, 4734, 4762, 4764, 4944, 35429) // patch to rerun failed locations -- remove before next run
	levelsof location_id if is_estimate==1, local(locations) clean
	save `locMeta'

	get_draws, gbd_id_type(modelable_entity_id) gbd_id(10141) location_id(`locations') sex_id(2) measure_id(5) source(ADDRESS) status(best) clear

	merge 1:1 location_id year_id age_group_id using `pregTemp', gen(prPregMerge)
	merge m:1 location_id using `locMeta', assert(2 3) nogenerate

	generate endemic = (strmatch(lower(region_name), "*latin america*") | inlist(ihme_loc_id, "BLZ", "GUY", "SUR"))
	drop if inlist(age_group_id, 27, 33, 164) | is_estimate==0
  
 
  

*** DERIVE PARAMETERS OF BETA DISTRIBUTION FOR RATE OF VERTICAL TRANSMISSION ***
	local mu    = 0.047  // mean and SD here are from meta-analysis by Howard et al (doi: 10.1111/1471-0528.12396)
	local sigma = (0.056 - 0.039) / (invnormal(0.975) * 2)
	local alpha = `mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2 
	local beta  = `alpha' * (1 - `mu') / `mu' 

	

	  
	forvalues i = 0/999 {
		local vertical    = rbeta(`alpha', `beta')  
		quietly replace draw_`i' = nPreg * draw_`i' * `vertical'
		}
	
	fastcollapse nPreg draw_*, by(location_id ihme_loc_id location_name year_id) type(sum)
	
	forvalues i = 0/999 {
		quietly replace draw_`i' = draw_`i' / nPreg
		}
		
	keep location_id year_id draw_*
		
	expand 2, gen(sex_id)
	replace sex_id = sex_id + 1
	
	generate age_group_id = 164
	generate measure_id = 6
	generate modelable_entity_id = 1451
	
	
	save FILEPATH/birthPrevDraws_2017.dta, replace
	
	
	use FILEPATH/birthPrevDraws_2017.dta, clear
	levelsof location_id, local(locations) clean
	
 
  foreach location of local locations {
	preserve
	keep if location_id==`location'
	save FILEPATH/birthPrev_`location'.dta, replace
	restore
   
	! qsub -P proj_custom_models -pe multi_slot 8 -N chagas_`location' "FILEPATH/04h_submitBirthPrev.sh" "`location'" 
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
		capture confirm file FILEPATH/`location'.txt 
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  * If all locations are complete submit save results jobs 
	  if `r(N)'==0 {
	    display "All locations complete." _n "Submitting save_results. " 
		local complete 1
		
		local mark_best True
		
		run FILEPATH/save_results_epi.ado
		save_results_epi, modelable_entity_id(1451)  mark_best(`mark_best') birth_prevalence(True) input_file_pattern({location_id}.csv) measure_id(5 6) description("Acute Chagas (dm models `dm')") input_dir(FILEPATH/1451b) clear
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

log close	  
