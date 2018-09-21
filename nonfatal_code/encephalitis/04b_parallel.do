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
	local functional "encephalitis"
	// grouping
	local grouping "long_mild _vision"
	// get locals from demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_ids)
	local sexes = r(sex_ids)
	local age_group_ids = r(age_group_ids)

	// directory for pulling files from previous step
	local pull_dir_03b "`root_tmp_dir'/03_steps/`date'/03b_outcome_split/03_outputs/01_draws"
	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE

//test run
//local years 2000 2005
//local sexes 1

foreach group of local grouping {
	foreach year of local years {
		foreach sex of local sexes {

			local file "`pull_dir_03b'/`functional'_`group'_`location'_`year'_`sex'.dta"

			** Pull in the outcome file
			use "`file'", clear

			foreach n of numlist 1/3 {
				// age_group_ids 2 through 4
				count
				gen row = r(N) // 20
				expand 2 in `n'
				gene seqnum = _n // used to mark newly generated values
				foreach d of numlist 0/999 {
					qui replace draw_`d' = draw_`d' * (1/52) if row < seqnum & `n' == 1 // one week old neonate
					qui replace draw_`d' = draw_`d' * (3/52) if row < seqnum & `n' == 2 // 3 week old neonate
					qui replace draw_`d' = draw_`d' * (48/52) if row < seqnum & `n' == 3 // under one year neonate
				}
				qui replace age_group_id = 100 if row < seqnum // temporarily label
				qui drop row seqnum
			}
			collapse (sum) draw_*, by(modelable_entity_id measure_id grouping location_id year_id age_group_id sex_id)
			drop in 1/3 // deletes age groups 2-4, combining all of the adjusted versions into age group id 100 (temp)

			// change age_group_ids for all neonates
			qui replace age_group_id = 0 if age_group_id == 100

			foreach n of numlist 5/20 {
				qui expand 4 if age_group_id == `n' & `n' == 5
				qui expand 5 if age_group_id == `n' & `n' >= 6 & `n' <= 20
			}
			foreach n of numlist 30/32{
				qui expand 5 if age_group_id == `n'
			}

			sort age_group_id
			gene seqnum = _n -1

			// declaring the data are time-series 
			tsset location_id seqnum

			forvalues i = 0/999 {
				** set up prevalence as half-year incidence
				qui gen prev_`i' = draw_`i'/2 if seqnum == 0
				** set up exact prevalence
				qui gen exact_`i' = 0 if seqnum == 0
				** preplace the exact = last year's exact prevalence + this year's incidence*the fraction vulnerable
				forvalues seqnum = 1/99 {
					qui replace exact_`i' = L1.exact_`i' + L1.draw_`i'*(1-L1.exact_`i') if seqnum == `seqnum'
				}
				forvalues seqnum = 1/99 { 
					qui replace prev_`i' = exact_`i' + draw_`i'*0.5*(1-exact_`i') if seqnum == `seqnum'
				}
			}

			drop seqnum exact_* draw_*
			// set measure_id for prevalence
			qui replace measure_id = 5
			qui rename prev_* draw_*

			** return to the original age group ids
			// collapse back into age groups
			collapse (mean) draw_*, by(modelable_entity_id measure_id grouping location_id age_group_id year_id sex_id) fast
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

			if "`group'" == "_vision" { 
				cap mkdir "`tmp_dir'/03_outputs/01_draws/`group'"
				cap mkdir "`tmp_dir'/03_outputs/01_draws/`group'/_unsqueezed"
				drop measure_id modelable_entity_id grouping
				// these are saved using save_results in parent file
				outsheet using "`tmp_dir'/03_outputs/01_draws/`group'/_unsqueezed/5_`location'_`year'_`sex'.csv", comma replace // measure id for prevalence
			}
			else {
				// do not want to save long_mild with save_results because needs further splitting
				save "`tmp_dir'/03_outputs/01_draws/`functional'_`group'_`location'_`year'_`sex'.dta", replace
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
	