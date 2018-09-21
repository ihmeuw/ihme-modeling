capture log close
log using "FILEPATH/draw_prev_clean.txt", replace text
/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER        
Output:           Estimate prevalence of NCC with epilepsy
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

	** Set locals from arguments passed on to job
	local meid "`1'"
	local epilepsymeid "`2'"
	local nccmodelid "`3'"
	local epilepsymodelid "`4'"
	local locyear "`5'"
	local restrict "`6'"

	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/drawprev_`locyear'_`date'_`time'.smcl", replace
	***********************	 
	
	*print macros in log
	macro dir

/*====================================================================
                        1: RESTRICTED LOCYEARS
====================================================================*/

** Zeroes file saved in submit script

/*====================================================================
                        2: ENDEMIC LOCYEARS
====================================================================*/


*--------------------2.1: Prep Files and Locals

	*Load PNAR file for this location-year
		use `tmp_dir'/not_at_risk_`locyear'.dta, clear
			
	*Define location and year locals
		levelsof year_id,local(year) clean
		levelsof location_id,local(location) clean
			
	*Use get_demographics to get list of ages/year/sexes needed for epi estimate output
		get_demographics, gbd_team(epi) clear
			local gbdages `r(age_group_ids)'
			local gbdyears `r(year_ids)'
			local gbdsexes `r(sex_ids)'


*--------------------2.2: Get draws for NCC among epileptics

	*Load NCC prevalence among epileptics at risk
		quietly run FILEPATH/get_draws.ado
		quietly get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid') source(epi) measure_ids(5) model_version_id(`nccmodelid') location_ids(`location') age_group_ids(`gbdages')  clear
		quietly keep if year_id==`year'

		*checkpoint
		quietly count
		di in red "NCC Prevalence Obs = `r(N)'"

	*Format
		forvalues i=0/999 {
			quietly rename draw_`i' ncc_prev_`i'
		}
		
	*Save
		tempfile ncc_prev_draws
		save "`ncc_prev_draws'", replace


*--------------------2.3: Get draws of Epilepsy Envelope
	
	*Load Epilepsy prevalence
		quietly run FILEPATH/get_draws.ado
		quietly get_draws, gbd_id_field(modelable_entity_id) gbd_id(`epilepsymeid') measure_ids(5) source(epi) model_version_id(`epilepsymodelid') location_ids(`location') age_group_ids(`gbdages') clear
		quietly keep if year_id==`year'

		*checkpoint
		quietly count
		di in red "Epilepsy Prevalence Obs = `r(N)'"

	*Format
		forvalues i = 0/999 {
			quietly rename draw_`i' epilepsy_`i'
		}

	*Save
		tempfile epilepsy_draws
		save "`epilepsy_draws'", replace
	
*--------------------2.4: Merge NCC Draws + Epilepsy Draws + PNAR

	*Merge NCC Among epileptics + Epilepsy
		merge 1:1 location_id year_id sex_id age_group_id using "`ncc_prev_draws'"

		*checkpoint
		quietly sum _merge
		if `r(min)'<3{
		di in red "Merge Issue Between NCC and Epilepsy Draws"
		}
		drop _merge

	*Merge PNAR
		merge m:1 location_id year_id using `not_at_risk', keep(master match)

		*checkpoint
		quietly sum _merge
		if `r(min)'<3{
		di in red "Merge Issue Between NCC/Epilepsy Draws and PAR"
		}
		drop _merge


/*====================================================================
                        3: Calculate NCC with Epilepsy
====================================================================*/

** Multiply NCC prevalence among all epileptics with epilepsy envelope, correcting for population not at risk
*Calculate prevalence of epilepsy due to NCC as: P * (NM-N) / (NM-1), where

*--------------------3.1:

		forvalues i = 0/999 {
			gen draw_`i' = epilepsy_`i' * (ncc_prev_`i' * notPAR_`i' - ncc_prev_`i') / (ncc_prev_`i' * notPAR_`i' - 1)
		}
 
		keep draw_* age_group_id year_id sex_id location_id

		gen modelable_entity_id=2656
		gen measure_id=5
			
		foreach sex in 1 2 {
			preserve
		
			keep if sex_id==`sex'
			local metric 5
			
				*checkpoint
				quietly count
				di in red "Final obs = `r(N)'"			
							
			keep modelable_entity_id measure_id location_id year_id age_group_id sex_id draw_*
			order modelable_entity_id measure_id location_id year_id age_group_id sex_id draw_*
			outsheet using "`out_dir'/2656_prev/`metric'_`location'_`year'_`sex'.csv", comma replace
			
			sleep 200
			
			restore
		}

	




***************************

file open progress using `clusterRoot'/progress/`locyear'.txt, text write replace
file write progress "complete"
file close progress



log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:


