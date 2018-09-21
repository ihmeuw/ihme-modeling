/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER  
Output:           Submit script to estimate prevalence of NCC with epilepsy from NCC among epileptics
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
	*model step
	local step 01
	*local envir (dev or prod)
	local envir = "prod"
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

	*directory for standard code files
	adopath + FILEPATH


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/submitlog_`date'_`time'.smcl", replace
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
		save "`local_tmp_dir'/metadata.dta", replace

*--------------------1.3: GBD Skeleton

	get_population, location_id("`gbdlocs'") sex_id("`gbdsexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
		save "`local_tmp_dir'/skeleton.dta", replace
			

/*====================================================================
                        2: Set MEIDs and Model Version IDs
====================================================================*/


*--------------------2.1: Set MEIDs

	local meid 1479
	local epilepsymeid 2403
	
*--------------------2.2: Get Best Model Version - NCC

	local nccmodelid 175205
	*get_best_model_versions, entity(modelable_entity) ids(`meid') clear
	*local nccmodelid = model_version_id

*--------------------2.3: Get Best Model Version - Epilepsy
	
	*local epilepsymodelid 175718
	get_best_model_versions, entity(modelable_entity) ids(`epilepsymeid') clear
	local epilepsymodelid = model_version_id



/*====================================================================
                        3: Pull Data to Define Population Not At Risk (PNAR)
====================================================================*/
** PNAR = proportion non-Muslim without access to sanitation

*--------------------3.1: Religion = Muslim (continuous; proportion)

	*Pull in file - this cov is
		insheet using "`in_dir'/prop_muslim_gbd2016.csv", clear double
		tempfile muslim_raw
		save `muslim_raw', replace

	*Merge with population envelope
		merge 1:1 location_id using `metadata', nogen
		drop if level<3
		bysort region_id : egen region_mean=mean(prop_muslim)
		replace prop_muslim=region_mean if prop_muslim==.

	*Replace missing locations: American Samoa (298), Greenland (349), Guam(351), Northern Mariana Islands (376), Us Virgin Islands (422) per CIA World Factbook
		replace prop_muslim=0 if inlist(location_id,298,349,351,376,422)
				
	*Format for merge with cod database
		keep location_id prop_muslim

	*Save
		save "`local_tmp_dir'/muslimcov.dta", replace


*--------------------3.2: Proportion of People with Access to Sanitation (covariate_id = 142)

	*Get estimates
		get_covariate_estimates,covariate_id(142) location_id(`alllocs') year_id(`gbdyears') clear
		save "`local_tmp_dir'/sanitation.dta", replace

		local name=covariate_name_short
	
	*Format
		foreach measure in mean lower upper{
			rename `measure'_value `name'_`measure'
		}
		keep covariate_id location_id year_id age_group_id sex_id `name'*
	
	*Wilson's Continuity Correction - Apply to get draws
		
		*Calculate value for n for binomial distribution using each of the bounds - then round them to whole numbers
			gen w=.
			gen p=`name'_mean
			
			replace w=`name'_lower
			gen n_lower_wilson = ((49*sqrt((-1250*p*(w^2))+(1250*p*w)+(2401*(w^4))-(3552*(w^3)) + (1151*(w^2))))+(625*p)-(2401*(w^2))+(1776*w))/(1250*((p^2)-(2*p*w)+(w^2)))
			
			replace w=`name'_upper
			gen n_upper_wilson = ((49*sqrt((1250*p*(w^2))-(1250*p*w)+(2401*(w^4))-(6052*(w^3)) + (3651*(w^2))))-(625*p)-(2401*(w^2))+(3026*w))/(1250*((p^2)-(2*p*w)+(w^2)))

			gen n_ul = round(n_upper_wilson)
			gen n_ll = round(n_lower_wilson)

		*get draws using these n's	
			forval x=0/499 {
				gen `name'_`x'= rbinomial(n_ul,`name'_mean)
				replace `name'_`x'=`name'_`x'/n_ul
			}
			forval x=500/999{
				gen `name'_`x'= rbinomial(n_ll,`name'_mean)
				replace `name'_`x'=`name'_`x'/n_ll
			}

		*Format
			drop covariate_id `name'_lower `name'_upper `name'_mean n_lower_wilson n_upper_wilson n_ul n_ll w p

		*Save
			save "`local_tmp_dir'/sanitation.dta", replace

*--------------------3.3: Merge and Save

	*Merge covs into one table for use
		merge m:1 location_id using `muslim',nogen keep(matched master)
		save "`local_tmp_dir'/covstable.dta", replace


/*====================================================================
                        4: Calculate Population Not At Risk (PNAR)
====================================================================*/


*--------------------4.1: Calculate PNAR (GBD2015 equation)

	forval x=0/999{
			generate double notPAR_`x' = 1 - (1 - prop_muslim) * (1 - sanitation_prop_`x')
		}


*--------------------4.2: Format and save

	keep location_id year notPAR*
	save "`local_tmp_dir'/not_at_risk.dta", replace



/*====================================================================
                        5: Submit Estimation Script to Qsub 
====================================================================*/


*--------------------5.1: Create a Zeroes File
	
	use "`local_tmp_dir'/skeleton.dta", clear

	gen modelable_entity_id=2656
	gen measure_id=5
	forval x=0/999{
		gen draw_`x'=0
	}

	save "`local_tmp_dir'/zeroes.dta", replace
	
*--------------------5.2: Clear Progress Monitoring Files

	*Clear progress files & logs for locations to be q-subbed
		foreach folder in `clusterRoot'/progress{
			cd `folder'
			capture shell rm standard*
		}

*--------------------5.3: Identify geographically restricted location-years

	*Rule out geographic restrictions - get locals of restricted location-years
		import delimited "FILEPATH/cysticerc_cc.csv", clear 
		keep if inlist(year_id,1990,1995,2000,2005,2010,2016)
		gen restricted=1
		
		merge 1:m location_id year_id using `not_at_risk', nogen
		replace restricted=0 if restricted==.
		egen locyear=concat(location_id year_id),p(_)
	
	*Save
		save "`local_tmp_dir'/not_at_risk.dta", replace 

*--------------------5.4: Submit Qsub

		use "`local_tmp_dir'/not_at_risk.dta", clear
		levelsof locyear,local(locyears) clean
		
		foreach locyear of local locyears {
		
			preserve
				keep if locyear=="`locyear'"
				local location=location_id
				local year=year_id
				local restrict=restrict
				
				*Endemic location-years: Save PNAR for location-year and submit prevalence estimation script
				if `restrict' != 1 {
				
					use "`local_tmp_dir'/zeroes.dta", clear
					replace location_id=`location'
					keep if year_id==`year'
					save `tmp_dir'/zeroes_`locyear'.dta, replace
					
					! qsub -P proj_custom_models -pe multi_slot 8 -N prev_`locyear' "`localRoot'/submit_draw_prev.sh" "`meid'" "`epilepsymeid'" "`nccmodelid'" "`epilepsymodelid'" "`locyear'" "`restrict'"
						
				}
				
				*Restricted location-years: Save Zeroes File
				else if `restrict' == 1 {

					save `tmp_dir'/not_at_risk_`locyear'.dta, replace

				}
				
			restore

			*Drop location-year so that loop speeds up over time
			drop if locyear == "`locyear'"

		}
	

/*====================================================================
                        6: Monitor Submission
====================================================================*/


*--------------------6.1: CREATE DATASET OF locyears TO MARK COMPLETION STATUS

	    clear
	    set obs `=wordcount("`locyears'")'
	    generate locyear = .
	    
	    forvalues i = 1 / `=wordcount("`locyears'")' {
	                    quietly replace locyear = `=word("`locyear'", `i')' in `i'
	                    }
	                    
	    generate complete = 0

*--------------------6.2: GET READY TO CHECK IF ALL locyears ARE COMPLETE

	    local pause 2
	    local complete 0
	    local incompleteLocyears `locyears'
	    
	    display _n "Checking to ensure all locyears are complete" _n

                
*--------------------6.3: ITERATIVELY LOOP THROUGH ALL locyears TO ASSESS PROGRESS UNTIL ALL ARE COMPLETE              
       
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
                        7: Save Results
====================================================================*/

/*
	*save the results to the database
		
		local meid 2656
		local mark_best = "no"
		local env = "prod"
		local description "Results - NCC `nccmodelid' & Epilepsy `epilepsymodelid' - sanit w/wlisons cc"
		local in_dir = "`cluster_root'/2656_prev/"
		local file_pat = "{metric}_{location_id}_{year_id}_{sex}.csv"

		run FILEPATH/save_results.do

		save_results, modelable_entity_id(`meid') mark_best(`mark_best') env(`env') file_pattern(`file_pat') description(`description') in_dir(`in_dir')

*/


log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

References:
1. CIA World Factbook online accessed 3/2/17: https://www.cia.gov/library/publications/the-world-factbook/fields/2122.html
2.
3.


