// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
** Purpose:		This sub-step template is for parallelized jobs submitted from main step code
** Description:	split up severity/etiology-specific estimates with proportion that has ringing (tinnitus) and split the meningitis estimates into pneumococcal (pneumo), H influenzea type B (hib), other bacterial (other), and Meningococcal infection
** Run: do "FILEPATH"

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
local username "`c(username)'"
local decomp "step4"

** do "FILEPATH"
	local cluster_check 0
	if `cluster_check' == 1 {
		local 1		"FILEPATH"
		local 2		"FILEPATH"
		local 3		"DATE"
		local 4		"06"
		local 5		"tinnitus_split"
		local 6		"FILEPATH"
		local 7		"43939"
		}
	
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	set type double, perm
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		local cluster 1 
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		local cluster 0
	}
	// directory for standard code files
		adopath + "FILEPATH"


	//If running locally, manually set locals (USER: I deleted or commented out ones I think will be unnecessary for running locally...if needed refer to full list below)
	if `cluster' == 0 {

		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
        local date = subinstr("`date'", " " , "_", .)
		local step_num "06"
		local step_name "split_by_severity"
		local code_dir "FILEPATH"
		local in_dir "FILEPATH"
		local root_j_dir "FILEPATH"
		local root_tmp_dir "FILEPATH"
		local location_id "58"

		}


	//If running on cluster, use locals passed in by model_custom's qsub
	else if `cluster' == 1 {
		// base directory on share scratch 
		local root_j_dir `1'
		// base directory on clustertmp
		local root_tmp_dir `2'
		// timestamp of current run (i.e. 2014_01_17) 
		local date `3'
		// step number of this step (i.e. 01a)
		local step_num `4'
		// name of current step (i.e. first_step_name)
		local step_name `5'
		// directory for steps code
		local code_dir `6'
		local location_id `7'

		}
	
	**Define directories 
		// directory for external inputs 
		local in_dir "FILEPATH"
		cap mkdir "`root_j_dir'"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		local out_dir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		local tmp_dir "FILEPATH"
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
 
//Loop over every year and sex for given location 
	//year_ids
		local year_ids "1990 1995 2000 2005 2010 2015 2017 2019"
		//local year_ids "2017 2019"
	//sex_ids
		local sex_ids "1 2"

		insheet using "FILEPATH", comma clear
		keep if outlier_type_id == 0
		collapse (sum) numerator denominator, by(severity_start)
		gen mean = numerator / denominator
		tempfile tin_bundle
		save `tin_bundle', replace


	foreach year_id in `year_ids' {
		foreach sex_id in `sex_ids' {

		** Bring in prevalence by etiology and severity 
		forvalues sev=20(15)95 {
			use "FILEPATH", clear 
			drop draw_* //drop vars saved for diagnostics
			tempfile working_sev`sev'
				save `working_sev`sev'', replace
			}

		** Now go by etiology group and add tinnitus proportions to get with and without ringing. 
			use `tin_bundle', clear


				// Get standard error from sample size, replace 0's with max SE
					gen standard_error = sqrt(mean*(1-mean)/denominator)
					sum standard_error if standard_error != 0
					replace standard_error = r(max) if standard_error == 0 
					keep if mean != .
					tempfile ring
					save `ring', replace
				
				// save info as locals
					** local sev 35
					forvalues sev=20(15)95 {
						use `ring', clear
						keep if severity_start==`sev'
						local mu mean
						local sigma standard_error
						local alpha_`sev' = `mu' * (`mu' - `mu' ^ 2 - `sigma' ^2) / `sigma' ^2 
						local beta_`sev'  = `alpha_`sev'' * (1 - `mu') / `mu'
					}

			
		// go through each etiology separately and make tempfiles for each.
			// note: r= ringing and nr= no ringing	
		cap restore, not

		**local cause meng_pneumo
		**local sev 20

		foreach cause in cong_other otitis other meng {
			forvalues sev=20(15)95 {
				use `working_sev`sev'', clear
				keep age `cause'*
				
				local draw 0
			
				forvalues draw = 0/999 {
					di in red "Draw `draw'! `cause'!! `step_name'"
					
						// to propogate uncertainty, use beta distribution for proportion due to tinnitus
						gen ring_sev`sev'_`draw' = rbeta(`alpha_`sev'', `beta_`sev'')

						gen r`cause'_sev`sev'_draw_`draw'=`cause'_sev`sev'_draw_`draw'*ring_sev`sev'_`draw'
						gen nr`cause'_sev`sev'_draw_`draw'=`cause'_sev`sev'_draw_`draw'*(1-ring_sev`sev'_`draw')
						drop ring_sev`sev' `cause'_sev`sev'_draw_`draw'
				}
				
				// save ringing and not ringing in separate tempfiles so we can then save all the separate outputs.	
				preserve 
					keep age r`cause'_sev`sev'_*
					rename r`cause'_sev`sev'_* *
					tempfile r`cause'_sev`sev'
					save `r`cause'_sev`sev'', replace
				restore
				preserve 
					keep age nr`cause'_sev`sev'*
					rename nr`cause'_sev`sev'_* *
					tempfile nr`cause'_sev`sev'
					save `nr`cause'_sev`sev'', replace
				restore	
			
			
			}
		}



***************************************************
** Save files into special place in special format
***************************************************

		**local cause otitis
		**local sev 20
		**local ring r
		//foreach cause in otitis cong_other other meng_pneumo meng_hib meng_meningo meng_other {
		foreach cause in otitis cong_other other meng {
			forvalues sev=20(15)95 {
				foreach ring in nr r {
					di in red "SAVING `cause' `sev' `ring'"
					
					import delimited "FILEPATH", clear
					levelsof modelable_entity_id if variable_name == "`ring'`cause'_sev`sev'", local(meid)
					cap noisily mkdir "`out_dir'/03_outputs/01_draws/`meid'"

					use ``ring'`cause'_sev`sev'', clear

						*ds draw* 
						*foreach d in `r(varlist)' {
							*replace `d' = 0 if `d' < 0
							*}


					format *draw* %16.0g
					outsheet using "FILEPATH", c replace

				}
			}
		}



		//Next sex	
		}
	//Next year 
	}
*********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// 
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

cap log close  

	// write check file to indicate sub-step has finished
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		file open finished using "FILEPATH", replace write
		file close finished

