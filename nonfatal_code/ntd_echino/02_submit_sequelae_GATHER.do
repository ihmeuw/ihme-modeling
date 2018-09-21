/*====================================================================
project:       GBD2016
Organization:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER         
Output:           Submit script to estimate sequelae prevalence for CE
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
	*local envir (dev or prod)
	local envir = "prod"
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH/gbd2016"
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

	*directory for standard code files
	adopath + FILEPATH



	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/submit_sequelae_`date'_`time'.smcl", replace
	***********************	


/*====================================================================
                        1: Get GBD Info
====================================================================*/

*--------------------1.1: Demographics

	get_demographics, gbd_team("epi") clear
		local gbdages `r(age_group_ids)'
		local gbdyears `r(year_ids)'
		local gbdsexes `r(sex_ids)'

*--------------------1.2: Location Metadata
		
	get_location_metadata, location_set_id(35) clear
		keep if most_detailed==1
		levelsof location_id,local(gbdlocs)clean

*--------------------1.3: GBD Skeleton

	get_population, location_id("`gbdlocs'") sex_id("`gbdsexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
		*gen model_version_id=`model_parent'
		save "`local_tmp_dir'/pop_env.dta", replace
			

/*====================================================================
          2: Create Structures for Submission to Qsub Estimation
====================================================================*/

*--------------------2.1: Create Zeroes File

	use "`local_tmp_dir'/pop_env.dta", clear
		
	forval x=0/999{
		display in red ". `x' " _continue 
		quietly gen draw_`x'=0
	}

	save "`tmp_dir'/zeroes/zeroes.dta", replace

*--------------------2.2:Get Draws from All-Cases Prevalence Model

	*Get best model id's
		get_best_model_versions, entity(modelable_entity) ids(1484) clear
			local model_parent = model_version_id
				di in red "`Parent Model Version: `model_parent'"
			local gbdround 4
			local metrics 5 6
	
	* Get draws
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(1484) measure_ids(`metrics') location_ids(`gbdlocs') year_ids(`gbdyears') sex_ids(`gbdsexes') source(epi) model_version_id(`model_parent') clear
			save "`tmp_dir'/parent_draws/alldraws.dta", replace


/*====================================================================
                        3: Submit Estimation Script to Qsub
====================================================================*/


*--------------------3.1: Clear Progress Monitoring Files
	
	*Clear progress files & logs for locations to be q-subbed
		foreach folder in `clusterRoot'/progress `clusterRoot'/logs{
			cd `folder'
			capture shell rm *
		}


*--------------------3.2: Identify geographically restricted location-years

	*Rule out geographic restrictions - get locals of restricted location-years
		import delimited "FILEPATH/ce_cc.csv", clear 
		
		gen restricted=1
		merge 1:m location_id year_id using "`tmp_dir'/parent_draws/alldraws.dta", nogen keep(matched using)
			*Location-years that do not merge are endemic (not geographically restricted)
		replace restricted=0 if restricted==.
				

		save "`local_tmp_dir'/draws_GR.dta", replace
		

*--------------------3.3: Submit Qsub
		
	*QSUB + save parent draws for endemic loc-years/save zeroes file for restricted loc-years
		use "`local_tmp_dir'/draws_GR.dta", clear
		
		egen locyear=concat(location_id year_id),p(_)
		levelsof locyear,local(locyears) clean
	
		foreach locyear in `locyears' {
		
			di in red ". `locyear' " _continue
			
			quietly{
			preserve
				keep if locyear=="`locyear'"
				local restrict=restrict
			
				if `restrict'==1 {
					noisily di in red "Location is Restricted"
					local location=location_id
					local year=year_id
					
					use "`tmp_dir'/zeroes/zeroes.dta", clear
					keep if location_id==`location'
					keep if year_id==`year'
					save `tmp_dir'/zeroes/zeroes_`locyear'.dta, replace	
				}
				
				if `restrict'==0 {
					noisily di in red "Location is Endemic"
					save `tmp_dir'/parent_draws/draws1484_`locyear'.dta, replace
				}
				
				sleep 100
				
				
				! qsub -N echinoSEQ_`locyear' -pe multi_slot 2 -l mem_free=4 -P proj_custom_models "FILEPATH/submit_sequelae.sh" "`locyear'" "`restrict'" "`model_parent'"
		
		
			quietly restore
			quietly drop if locyear == "`locyear'"
			
			}
		}
	


/*====================================================================
                        4: Monitor Submission
====================================================================*/

*--------------------4.1: CREATE DATASET OF locyears TO MARK COMPLETION STATUS

	    clear
	    set obs `=wordcount("`locyears'")'
	    generate locyear = .
	    
	    forvalues i = 1 / `=wordcount("`locyears'")' {
	                    quietly replace locyear = `=word("`locyear'", `i')' in `i'
	                    }
	                    
	    generate complete = 0

*--------------------4.2: GET READY TO CHECK IF ALL locyears ARE COMPLETE

	    local pause 2
	    local complete 0
	    local incompleteLocyears `locyears'
	    
	    display _n "Checking to ensure all locyears are complete" _n

                
*--------------------4.3: ITERATIVELY LOOP THROUGH ALL locyears TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE              
       
		while `complete'==0 {
        
		*Are all locyears complete?
			foreach locyear of local incompleteLocyears {
			            capture confirm file /`progress_dir'/`locyear'.txt 
			            if _rc == 0 quietly replace complete = 1 if locyear==`locyear'
			            }
			            
			quietly count if complete==0
          
		*If all locyears are complete submit save results jobs 
			if `r(N)'==0 {
				display "All locyears complete!!!"
				local complete 1
			}
        

         *If all locyears are not complete, inform the user and pause before checking again
			else {
				quietly levelsof locyear if complete==0, local(incompleteLocyears) clean
					display "The following locyears remain incomplete:" _n _col(3) "`incompleteLocyears'" _n "Pausing for `pause' minutes" _continue

					forvalues sleep = 1/`=`pause'*6' {
						sleep 10000
						if mod(`sleep',6)==0 di "`=`sleep'/6'" _continue
						else di "." _continue
					}
					
					di _n

				}
			}
        

/*====================================================================
                        5: Save Results
====================================================================*/

/*
	*save the results to the database

		foreach meid in 1485 1486 2796
		
		run "FILEPATH/save_results.do"
		
			save_results, modelable_entity_id(1485) description("Abdominopelvic disease due to echino, derived from estimated clinical cases (custom#`model_parent') and proportion of people with abdominopelvic localization of cysts (incl other localizations such as bone, skin, and muscle)") in_dir("`out_dir'/1485_prev") metrics(prevalence incidence) mark_best(no) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)
		
			save_results, modelable_entity_id(1486) description("Respiratory disease due to echinococcosis, derived from echino envelope (custom #`model_parent') and proportion of people with thoracic localization of cysts") in_dir("`out_dir'/1486_prev") metrics(prevalence incidence) mark_best(yes) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)
		
			save_results, modelable_entity_id(2796) description("Epilepsy due to echinococcosis, derived from echino envelope (custom #`model_parent') and proportion of people with cerebral localization of cysts") in_dir("`out_dir'/2796_prev") metrics(prevalence incidence) mark_best(yes) env(prod) file_pattern({measure_id}_{location_id}_{year_id}_{sex_id}.csv)

	
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


