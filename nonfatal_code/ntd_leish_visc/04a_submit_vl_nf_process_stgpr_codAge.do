
*** BOILERPLATE ***
	clear all
	set more off
  
	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		local j "FILEPATH"
		}


	local ageModel   97035
	

*** LOAD SHARED FUNCTIONS ***  
	adopath + FILEPATH
	run FILEPATH/get_location_metadata.ado
	run FILEPATH/get_demographics.ado
	run FILEPATH/get_population.ado

	
*** CREATE DIRECTORIES *** 

	local outDir FILEPATH
	
	tokenize "`outDir'", parse(/)
	while "`1'"!="" {
		local path `path'`1'`2'
		capture mkdir `path'
		macro shift 2
		}

  
	foreach subDir in temp progress 1458 1459 1460 {
		sleep 2000
		!rm -rf `outDir'/`subDir'
		capture mkdir `outDir'/`subDir'
		}

	capture mkdir FILEPATH
	
  
	tempfile data appendTemp endemic parents pop locMeta



*** GET ALL-AGE POPULATION ESTIMATES ***
	use FILEPATH/bestCombined.dta
	levelsof location_id, local(locations) clean
	levelsof year_id, local(years) clean
	local maxYear = max(`=subinstr("`years'", " ", ",", .)')

	get_population, location_id(`locations') year_id(`years') age_group_id(22) sex_id(3) clear
	keep if inrange(year_id, 1980, `maxYear')
	keep location_id year_id population

	
	
*** LOAD ALL-AGE INCIDENCE ESTIMATES FROM ST-GPR ***
	merge 1:1 location_id year_id using FILEPATH/bestCombined.dta, keep(3) nogenerate
	keep if is_estimate==1
	
	tsset location_id year_id
	foreach var in mean lower upper {
		tssmooth ma temp = `var', window(1 1 1)
		replace `var' = temp
		drop temp
		}
		
	
	generate sigma = ((10/population) + upper - lower) / (2 * invnormal(0.975))
	generate alpha = mean * (mean - mean^2 - sigma^2) / sigma^2 
	generate beta  = alpha * (1 - mean) / mean 
	
	replace alpha = alpha + 1e-4 if endemic==1
	replace beta  = (alpha / mean) - alpha if endemic==1
	
	keep location_id year_id endemic alpha beta
	
	
*** SUBMIT LOCATION-SPECIFIC JOBS ***
	keep if inrange(year_id, 1990, `maxYear)
	levelsof location_id, local(locations) clean
	
	
	foreach location of local locations {	
		
		quietly {
			preserve
			keep if location_id==`location'
			save `outDir'/temp/processing_`location'.dta, replace
			restore
			}
		
		! qsub -P proj_custom_models -pe multi_slot 8 -N vl_nf_`location' "FILEPATH/04b_submit_vl_nf_process_stgpr_codAge.sh" "`location'" "`ageModel'"
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
		capture confirm file `outDir'/progress/`location'.txt 
		if _rc == 0 quietly replace complete = 1 if location_id==`location'
		}
		
	  quietly count if complete==0
	  
	  * If all locations are complete submit save results jobs 
	  if `r(N)'==0 {
	    display "All locations complete for vl." _n "Submitting save_results. " 
		local complete 1
		
		local mark_best no
		
		run FILEPATH/save_results.do
		
		save_results, modelable_entity_id(10827) mark_best(`mark_best') file_pattern({location_id}.csv) description("All VL (ST-GPR model `stGprModel' with cod age pattern)") in_dir(`outDir'/1458) env("prod")
		save_results, modelable_entity_id(1459) mark_best(`mark_best') file_pattern({location_id}.csv) description("Moderate VL (ST-GPR model `stGprModel' with cod age pattern)") in_dir(`outDir'/1459) env("prod")
		save_results, modelable_entity_id(1460) mark_best(`mark_best') file_pattern({location_id}.csv) description("Severe VL (ST-GPR model `stGprModel' with cod age pattern)") in_dir(`outDir'/1460) env("prod")

		}
	

	  * If all locations are not complete, inform the user and pause before checking again
	  else {
	    quietly levelsof location_id if complete==0, local(incompleteLocations) clean
	    display "The following locations remain incomplete for vl:" _n _col(3) "`incompleteLocations'" _n "Pausing for `pause' minutes" _continue
		
		forvalues sleep = 1/`=`pause'*6' {
		  sleep 10000
		  if mod(`sleep',6)==0 di "`=`sleep'/6'" _continue
		  else di "." _continue
		  }
		di _n
		
		}
	  }
	
 
 