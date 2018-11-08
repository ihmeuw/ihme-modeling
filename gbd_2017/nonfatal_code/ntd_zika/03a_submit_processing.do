
/******************************************************************************\
           SET UP ENVIRONMENT WITH NECESSARY SETTINGS AND LOCALS
\******************************************************************************/

* BOILERPLATE *
  clear all
  set maxvar 10000
  set more off
  
  adopath + FILEPATH
  adopath + FILEPATH
  run FILEPATH/get_demographics.ado
 
 
* SETUP DIRECTORIES * 
  local inputDir FILEPATH
  local codeDir FILEPATH
  local outDir FILEPATH 
  
  
  capture mkdir `outDir'
  
  foreach subDir in logs temp fatalities total inf_mod _asymp gbs congenital preg cases {
  	!rm -rf `outDir'/`subDir'
	sleep 2000
    capture mkdir `outDir'/`subDir'
	} 
 
  !rm -rf `outDir'/logs/progress
  capture mkdir `outDir'/logs/progress
  */

  local allAge FILEPATH/allAgeEstimateTemp.dta
  local ageSpecific FILEPATH/ageSpecificTemp.dta
  local outcomes `inputDir'/outcomes.dta
  local prBirthsBySex `inputDir'/prBirthsBySex.dta
  
* GET LIST OF ESTIMATION LOCATIONS *
  get_demographics, gbd_team(epi) clear
  local locations `r(location_ids)'
  
  
* LOAD ESTIMATE TEMP FILES AND PROCESS BY LOCATION * 

  foreach file in allAge ageSpecific outcomes prBirthsBySex {
	di _n "Processing locations for `file' file"
	use ``file'', replace
	if "`file'" == "allAge" quietly levelsof location_id, local(locations) clean  

	local i 1
	
	foreach location of local locations {
		quietly {
			if "`file'"=="outcomes"  {
				! cp `outcomes' FILEPATH/`file'_`location'.dta
				}
			else {
				preserve
				keep if location_id==`location' 
				save FILEPATH/`file'_`location'.dta , replace 
				restore
				drop if location_id==`location' 
				}
			}
			di "." _continue
			if mod(`i', 100)==0 di ""
			local ++i
		}
		di _n	
	
	}


	foreach location of local locations {
		! qsub  -P proj_custom_models -pe multi_slot 8 -N zika_`location' "FILEPATH/03b_submit_processing.sh" "`location'"
		sleep 500
		}
		
	
/******************************************************************************\
                         SAVE THE RESULTS (OPTIONAL)
\******************************************************************************/ 	

*** ESTABLISH LOCALS WITH CAUSE AND ME IDS ***
	local outcomes fatalities _asymp inf_mod gbs congential
	local id_fatalities 935
	local id__asymp 10400
	local id_inf_mod 10401
	local id_gbs 10402
	local id_congenital 10403

*** CREATE DATASET OF LOCATIONS TO MARK COMPLETION STATUS ***	
	clear
	set obs `=wordcount("`locations'")'
	generate location_id = .
	local i 1
	foreach location of local locations {
		quietly replace location_id = `location' in `i'
		local ++i
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
		capture confirm file FILEPATH/`location'.txt 
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  * If all locations are complete submit save results jobs 
	  if `r(N)'==0 {
	    display "All locations complete." _n "Submitting save_results. " 
		local complete 1
		
		* Submit a job for each outcome (meid or cause_id)
		local mark_best no
		local description Test run of zika `outcome'
			
		run FILEPATH/save_results.do
		save_results, modelable_entity_id(10400) description("All Zika infections") in_dir(FILEPATH) file_pattern({location_id}.csv) mark_best(yes) env("prod")	
		save_results, modelable_entity_id(10401) description("Moderate acute Zika case") in_dir(FILEPATH) file_pattern({location_id}.csv) mark_best(yes) env("prod")	
		save_results, modelable_entity_id(10402) description("Zika-associated Guillian-Barre") in_dir(FILEPATH) file_pattern({location_id}.csv) mark_best(yes) env("prod")
		save_results, modelable_entity_id(11028) description("Asymptomatic Zika infection") in_dir(FILEPATH) file_pattern({location_id}.csv) mark_best(yes) env("prod")
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
	
	
  
   