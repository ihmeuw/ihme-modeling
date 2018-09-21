// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	
// Description:	Parallelization of 04b_outcome_prev_womort

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}

// define locals from qsub command
	local date 			`1'
	local step_num 		`2'
	local step_name		`3'
	local location 		`4'
	local code_dir 		`5'
	local in_dir 		`6'
	local out_dir 		`7'
	local tmp_dir 		`8'
	local root_tmp_dir 	`9'
	local root_j_dir 	`10'

// define other locals
	// directory for standard code files
	adopath + "SHARED FUNCTIONS"
	// functional
	local functional "meningitis"
	// etiologies
	local etiologies "meningitis_pneumo meningitis_hib meningitis_meningo meningitis_other"
	// grouping
	local grouping "long_mild _vision _hearing"

	// directory for pulling files from previous step
	local pull_dir_03b "`root_tmp_dir'/03_steps/`date'/03b_outcome_split/03_outputs/01_draws"

	// get locals from demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	
	// test run
	//local years 2000 2005
	//local sexes 1

	// set locals for etiology meids
	local _hearing_meningitis_pneumo = 2924
	local _hearing_meningitis_hib = 2918
	local _hearing_meningitis_meningo = 2920
	local _hearing_meningitis_other = 2922
	local _vision_meningitis_pneumo = 9429
	local _vision_meningitis_hib = 9432
	local _vision_meningitis_meningo = 9435
	local _vision_meningitis_other = 9438

	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
foreach year of local years {
	foreach sex of local sexes {
		foreach etiology of local etiologies {
			foreach group of local grouping {
			
				local file "`pull_dir_03b'/`etiology'_`group'_`location'_`year'_`sex'.dta"
			
				** Pull in the etiology-outcome file
				use "`file'", clear

				foreach n of numlist 1/3 {
					// age_group_ids 2 through 4
					count
					gen row = r(N) // 20
					/*
					count
					assert r(N) > 0
					*/
					expand 2 in `n'
					gene seqnum = _n // used to mark newly generated values
					foreach d of numlist 0/999 {
						qui replace draw_`d' = draw_`d' * (1/52) if row < seqnum & `n' == 1 // one week old neonate
						qui replace draw_`d' = draw_`d' * (3/52) if row < seqnum & `n' == 2 // 3 week old neonate
						qui replace draw_`d' = draw_`d' * (48/52) if row < seqnum & `n' == 3 // under one year neonate
					}
					qui replace age_group_id = 100 if row < seqnum // temporary label
					qui drop row seqnum
				}
				collapse (sum) draw_*, by(modelable_entity_id measure_id grouping location_id year_id age_group_id etiology sex_id) fast
				drop in 1/3 // deletes age_group_ids 2/4, combining all of the adjusted versions into age_group_id 100

				// change age_group_id for all neonates
				qui replace age_group_id = 0 if age_group_id == 100
				// "5/20 30/33 235"
				foreach n of numlist 5/20 {
					qui expand 4 if age_group_id == `n' & `n' == 5
					qui expand 5 if age_group_id == `n' & `n' >= 6 & `n' <= 20
				}
				foreach n of numlist 30/32{
					qui expand 5 if age_group_id == `n'
				}
							
				sort age_group_id	
				gene seqnum = _n -1

				/* declaring the data are time-series */
				tsset location_id seqnum

				** Do math based on what theo sent:
				forvalues i = 0/999 {
					** set up prevalence as half-year incidence 
					qui gen prev_`i' = draw_`i'/2 if seqnum == 0
					** set up exact prevalence
					qui gen exact_`i' = 0 if seqnum == 0
					** replace the exact = last year's exact prevalence + this year's incidence*the fraction vulnerable
					forvalues seqnum = 1/99 {
						qui replace exact_`i' = L1.exact_`i' + L1.draw_`i'*(1-L1.exact_`i') if seqnum == `seqnum'
					}
					forvalues seqnum = 1/99 {
						qui replace prev_`i' = exact_`i' + draw_`i'*.5*(1-exact_`i') if seqnum == `seqnum'
					}
				}

				drop seqnum exact_* draw_*
				// set measure_id for prevalence
				qui replace measure_id = 5
				qui rename prev_* draw_*
				
				** Return to the original age groups
				// collapse back into age groups
				collapse (mean) draw_*, by(modelable_entity_id measure_id grouping location_id age_group_id year_id etiology sex_id) fast
				qui expand 3 if age_group_id == 0
				qui sort age_group_id
				qui replace age_group_id = 2 in 1
				qui replace age_group_id = 3 in 2
				qui replace age_group_id = 4 in 3
				foreach d of numlist 0/999 {
					qui replace draw_`d' = ((draw_`d'*2)/52)*(0+(1/2)) in 1 // return to 1 week neonate (prevalence)
					qui replace draw_`d' = ((draw_`d'*2)/52)*(1+(3/2)) in 2 // return to 3 week neonate (prevalence)
					qui replace draw_`d' = ((draw_`d'*2)/52)*(4+(48/2)) in 3 // return to 48 week neonate (prevalence)
				}	

				if "`group'" == "_hearing" | "`group'" == "_vision" {
					cap mkdir "`tmp_dir'/03_outputs/01_draws/`etiology'"
					cap mkdir "`tmp_dir'/03_outputs/01_draws/`etiology'/`group'"
					cap mkdir "`tmp_dir'/03_outputs/01_draws/`etiology'/`group'/_unsqueezed"
					// these are saved using save_results in parent file
					drop measure_id modelable_entity_id etiology grouping
					outsheet using "`tmp_dir'/03_outputs/01_draws/`etiology'/`group'/_unsqueezed/5_`location'_`year'_`sex'.csv", comma replace // measure_id for prevalence
				}
				else {
					// do not want to save long_mild with save_results because needs further splitting
					save "`tmp_dir'/03_outputs/01_draws/`etiology'_`group'_`location'_`year'_`sex'.dta", replace
				}
			}
		}
	}
}

// write check here
	file open finished using "`tmp_dir'/02_temp/01_code/checks/finished_loc`location'.txt", replace write
	file close finished

// close logs
	if `close' log close
	clear
	