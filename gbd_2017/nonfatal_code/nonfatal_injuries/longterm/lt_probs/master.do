// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Master Script to submit mixed model regression

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	if "`1'" == "" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "04a"
		local 5 prob_long_term

		local 8 "FILEPATH"
	}
	// base directory on FILEPATH 
	local root_j_dir `1'
	// base directory on FILEPATH
	local root_tmp_dir `2'
	// timestamp of current run (i.e. 2014_01_17)
	local date `3'
	// step number of this step (i.e. 01a)
	local step_num `4'
	// name of current step (i.e. first_step_name)
	local step_name `5'
	// step numbers of immediately anterior parent step (i.e. for step 2: 01a 01b 01c)
	local hold_steps `6'
	// step numbers for final steps that you are running in the current run (i.e. 11a 11b 11c)
	local last_steps `7'
    // directory where the code lives
    local code_dir `8'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	// directory for standard code files
	adopath + "FILEPATH"

	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/FILEPATH.smcl", replace name(step_parent)
	if !_rc local close_log 1
	else local close_log 0

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Set globals for steps
global run_regressions 0
global summary 0
global redo_compiled 0
global draws 1

local pct_treated 0

// If no check global passed from master, assume not a test run
	if missing("$check") global check 0
	
	local tester = 0

// Settings
	local subnational "yes"
	local type "epi"
	set type double, perm
	
	** debugging?
	local debug 99
	
	** make any stochastic aspects repeatable
	set seed 0
	
	** codes leading to no long-term injury. Setting the effect of these to 0
	global no_lt 		N30	N31	N32	N47
	global no_lt_sev	9	9	9	9
	
	** ncodes that have 100% long-term outcomes and can take out of regression. all_lt_hosp == 9
	** indicates both inpatient and "other" cases are set to 100% long-term. all_lt_hosp == 1 indicates inpatient only.
	global all_lt 		N1	N2	N4	N5	N7	N3	N6
	global all_lt_sev	9	9	9	9	9	1	1

	
// filepaths
	local inj_dir "FILEPATH"
	local data_dir "`tmp_dir'/FILEPATH"
	local n_hs_map "`inj_dir'/FILEPATH.csv"

	local gbd_ado "FILEPATH"
	local diag_dir "`out_dir'/FILEPATH"
	local check_file_dir "`data_dir'/FILEPATH"
	local log_dir "`out_dir'/FILEPATH"
	
	** ALTERING ADOPATH
	adopath + "`code_dir'/ado"
	run "`code_dir'/FILEPATH.ado"

	start_timer, dir("`diag_dir'") name("`step_name'")
	
	** get rid of compound quotes so that adopath global can be passed to sub-step parallel worker scripts
	global S_ADO = subinstr(subinstr(subinstr(`"$S_ADO"',"`","",.),"'","",.),`"""',"",.)
	
	** getting locations of hierarchies and pooled followup-study data
	foreach i in hierarchies pooled_followup {
		get_step_num, name("`i'") stepfile("`code_dir'/FILEPATH.xlsx")
		local `i'_name ${step_num_`i'}_`i'
		local `i'_input_dir "FILEPATH"
		local `i'_input_dir "$prefix/``i'_input_dir'"
	}

	
// Get DW draws
	get_ncode_dw_map, prefix("$prefix") out_dir("`data_dir'") category("lt_t")
	local dw_file "`data_dir'/FILEPATH.csv"

// get percent treatment to use for all jobs
if `pct_treated' == 1 {
	get_pct_treated, prefix("$prefix") code_dir("`code_dir'")
	save "`out_dir'/FILEPATH.dta", replace
}
	
// Import hierarchies
	local h_loc "`hierarchies_input_dir'/FILEPATH.xls"
	foreach pf in inp noninp {
		import excel rank=A ncode=B using "`h_loc'", sheet("`pf'") cellrange(A2) clear
		tempfile h_`pf'
		save `h_`pf'', replace
	}
	
	
// Import pooled followup studies
	import delimited using "`pooled_followup_input_dir'/FILEPATH.csv", delim(",") clear case(preserve)
	
// Merge on hierarchy
	tempfile both_pf
	save `both_pf'

	forvalues x = 0/1 {
		use `both_pf', clear
		
		if `x' keep if inlist(inpatient,1,2)
		else keep if inpatient == 0
		
		gen best_ncode = ""
		gen best_rank = 999
		gen best_ix = 0
		foreach var of varlist INJ_* {
			local n_code = subinstr("`var'","INJ_","",.)
			gen ncode = ""
			replace ncode = "`n_code'" if `var' > 0
			merge m:1 ncode using `h_inp', keep(match master) nogen
			replace best_ncode = ncode if rank < best_rank
			replace best_ix = `var' if rank < best_rank
			replace best_rank = rank if rank < best_rank
			drop ncode rank `var'
		}
		drop best_rank
		
		** drop patients with a probabilistically mapped top-ranked N-code
		drop if best_ix < 1
		drop best_ix
		
		** save data
		tempfile inpatient_`x'
		save `inpatient_`x''
	}

	use `both_pf', clear
	keep if inpatient == .

	forvalues x = 0/1 {
		append using `inpatient_`x''
	}
	rename best_ncode n_code
	
// Save resulting dataset
	gen n_code_num = subinstr(n_code,"N","",.)
	replace n_code_num = "0" if n_code == ""
	destring n_code_num, replace
	keep id iso3 sex age_gr inpatient never_injured dataset n_code n_code_num logit_dw
	save "`data_dir'/appended.dta", replace
	
if $run_regressions == 1 {
// Run regression and save draws of long-term probabilities by inpatient/outpatient/pooled
	** set holds macro and make directory for draws and check files
	local holds
	cap mkdir "`data_dir'/00_dw_draws"
	cap mkdir "`data_dir'/01_prob_draws"
	
	cap mkdir "`check_file_dir'"
	local datafiles: dir "`check_file_dir'" files "*.txt"
	foreach datafile of local datafiles {
			rm "`check_file_dir'/`datafile'"
		}	

	forvalues inp = 0/2 {
	// create locals for initial no-LT info so that we can reset them with each iteration of the model
	// also create locals for all-LT info that applies to this particular platform (inpatient, outpatient, pooled)
		foreach i in no all {
			local `i'_lt
			local num_`i'_lt = wordcount("${`i'_lt}")
			forvalues j = 1/`num_`i'_lt' {
				local this_n_code = word("${`i'_lt}",`j')
				if inlist(word("${`i'_lt_sev}",`j'),"9","`inp'") local `i'_lt ``i'_lt' `this_n_code'
			}
			local `i'_lt_tot
		}
	
	// submit job to run regression
		local slots 2
		local mem 4
		local jobname W_AGE_inj_`step_num'_`inp'
		!rm -r "`check_file_dir'"
		mkdir "`check_file_dir'"
		!qsub  -P proj_injuries -N `jobname' -pe multi_slot `slots' -l mem_free=`mem' "`code_dir'/FILEPATH.sh" "`code_dir'/`step_name'/FILEPATH.do" "`inp' `tmp_dir' `data_dir' \"`no_lt'\" \"`all_lt'\" `data_dir' `code_dir' `dw_file' `gbd_ado' `diag_dir' `jobname' `slots'"
		if "`holds'" == "" local holds `jobname'
		else local holds `holds',`jobname'
		
	}

	local alljobs = 3
	local i = 0
	while `i' == 0 {
		local checks : dir "`check_file_dir'" files "*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `alljobs' regression jobs finished"
		if (`count' == `alljobs') continue, break
		else sleep 300000
	}
// End loop to run regressions
}

if $redo_compiled == 1 {

// using summary stats, correct for instances in which we don't have follow-up data, or have small sample sizes, or strange results (i.e. inpatient < outpatient probabilities)
	import delimited using "`in_dir'/FILEPATH.csv", delim(",") varnames(1) clear
	gen drop = 1
	tempfile dropns
	save `dropns', replace
	
	import delimited using "`data_dir'/FILEPATH.csv", delim(",") clear 
	merge m:1 n_code using `dropns'
	foreach var in mean_otp ll_otp ul_otp n_otp {
		replace `var' = . if _m == 3
	}
	drop _m drop
	
	levelsof n_code, local(ncodes)
	foreach code of local ncodes {
		preserve
		keep if n_code == "`code'"
		if (mean_otp > mean_inp & mean_otp != .) {
			di in red "`code'"
			local use_pool_`code' = 1
		}
		else {
			local use_pool_`code' = 0
		}
		restore
	}
// make locals for manual changes to change at the draw level 
	** for inpatient burns, we will use the severe disability weight for both severe and moderate (N8 and N9) if the moderate is greater than the severe
	local burn_indic 0
	preserve
	keep if n_code == "N8" | n_code == "N9"
	sort n_code
	gen burn = mean_inp[1]/mean_inp[2]
	gen burn_indic = 0
	replace burn_indic = 1 if burn > 1
	local burn_indic = burn_indic
	restore
	** for lower respiratory burns N10, use higher of N8 and N9 for inpatient (no outpatient)
	local lowerburn 0
	preserve 
	keep if n_code == "N10"
	if (mean_inp == .) {
			local lowerburn = 1
	}	
	restore
	local thumb_indic 0
	** for outpatient thumb amputation, use outpatient finger amputation because we have no cases
	preserve 
	keep if n_code == "N6" 
	if (mean_otp == .) local thumb_indic = 1
	restore
	local submersion_indic 0
	** we are assuming pooled for inpatient and outpatient because we don't have outpatients for non-fatal submersion N35
	preserve
	keep if n_code == "N35"
	if (mean_otp == .) local submersion_indic = 1
	restore
	local asphyx_indic 0
	** use nonfatal submersion as a proxy for asphyxiation because we have no data on it, N36
	preserve
	keep if n_code == "N36"
	if (mean_otp == .) local asphyx_indic = 1
	restore
	
// import draws and make these changes to draws- append inp, out, pooled together (make sure they are indicated), and make changes above- outpatient =0, inpatient=1, pooled=2
	tempfile allplat_draws
	local count = 0
	forvalues i = 0/2 {
	local count = `count' + 1
	use "`data_dir'/01_prob_draws/`i'_squeeze_WITH_AGE.dta", clear
	gen platform = `i'
	if (`count' > 1) append using `allplat_draws'
	save `allplat_draws', replace
	}
	
	drop _merge
	rename draw* draw*_
	reshape wide draw_*_ n_, i(n_code age) j(platform)
	gen code = subinstr(n_code,"N" ,"",.)
	destring code, replace
	
	** create observation for N10, which had no data at all, so didn't carry through, set equal to the higher of N8 and N9 (mod, severe burns), of note- only inpatient
	** also in here we'll create a blank entry for N36 to get its value filled later by N35
	tempfile pre_fix
	save `pre_fix', replace
	
	local addsomens 0
	preserve
	keep if n_code == "N10" | n_code == "N36"
	if (_N == 0) local addsomens = 1
	restore
	
** add N10 and N36 if they don't exist at the draw level	(don't come as result of regressions- no data)
	if (`addsomens' == 1) {
	keep if n_code == "N8"
	expand 3
	replace n_code = "N10" if _n == 2
	replace code = 10 if _n == 2
	replace n_code = "N36" if _n == 3
	replace code = 36 if _n == 3
	sort code
	forvalues i = 0/999 {
		forvalues j = 0/2 {
			replace draw_`i'_`j' = . if _n == 3 | _n == 2
		}
	}
	}

	
	// merge 1:1 n_code using `pre_fix'
	// drop _m
	
	sort code
	// isid code
	
	** lower respiratory burns should be higher of N8 and N9 if no data, burn_indic == 1 means N8 > N9
	cap if (`lowerburn' == 1) {
		if (`burn_indic' == 1) {
			forvalues i = 0/999 {
				replace draw_`i'_1 = draw_`i'_1[_n-2] if n_code == "N10"
			}
		}
		else {
			forvalues i = 0/999 {
				replace draw_`i'_1 = draw_`i'_1[_n-1] if n_code == "N10"
			}
		}
	}
	
	** fix burns- if moderate is higher than severe, set both to severe (for inpatient only- N9 is inpatient only)
	cap if (`burn_indic' == 1) {
		forvalues i = 0/999 {
			replace draw_`i'_1 = draw_`i'_1[_n+1] if n_code == "N8"
		}
	}
	
	** fix thumb amputation- if outpatient missing, set to finger amputation (outpatient probably partial amputations)- thumb N6, finger N3
	cap if (`thumb_indic' == 1) {
		forvalues i = 0/999 {
			replace draw_`i'_0 = draw_`i'_0[_n-3] if n_code == "N6"
		}
	}
	
	** make sure that since we have 0 outpatients for non-fatal submersion, that we are assuming no long-term outcomes
	cap if (`submersion_indic' == 1) {
		forvalues i = 0/999 {
				replace draw_`i'_0 = 0 if n_code == "N35"
		}
	}
	
	** use nonfatal submersion as a proxy for asphyxiation because we have no data on it
	cap if (`asphyx_indic' == 1) {
		forvalues i = 0/999 {
			forvalues j = 0/2 {
				replace draw_`i'_`j' = draw_`i'_`j'[_n-1] if n_code == "N36"
			}
		}
	}
	
	** if we're replacing inpatient N8 with N9, then outpatient N8 is higher than inpatient N8
	cap if (`burn_indic' == 1) {
		forvalues i = 0/999 {
			replace draw_`i'_0 = draw_`i'_1 if n_code == "N8"
		}
	}
	
	** verify N5 inpatient and outpatient set to 1
	forvalues i = 0/999 {
		forvalues j = 0/2 {
			replace draw_`i'_`j' = 1 if n_code == "N5"
		}
	}
	
	foreach code of local ncodes {
		if (`use_pool_`code'' == 1) {
			di in red "`code'"
			forvalues i = 0/1 {
				forvalues j = 0/999 {
					replace draw_`j'_`i' = draw_`j'_2 if n_code == "`code'"
				}
			}
		}
	}
	
	** save input dataset for the parallelization
	drop n_1 n_2 n_0 code
	preserve 
	keep n_code age draw_*_0
	rename draw_*_0 draw_*
	gen inpatient = 0
	tempfile outhalf
	save `outhalf', replace
	restore
	keep n_code age draw_*_1
	rename draw_*_1 draw_*
	gen inpatient = 1
	append using `outhalf'
	order n_code age inpatient
	** drop ncodes with zero long term probability
	drop if n_code == "N30" | n_code == "N31" | n_code == "N32" | n_code == "N47"
	save `outhalf', replace
	
	import delimited using "`in_dir'/FILEPATH.csv", delim(",") varnames(1) clear
	merge 1:m n_code using `outhalf'
	keep if _m != 3 | inpatient == 1
	drop _m
	
	// Set spinal lesion lt probabilities to 98.7% manually. This was derived from a single study on the long-term consequences of spinal injuries (Marino et al., 1999) because our regression method was giving implausibly low probabilities at all ages (~10%), which is likely a data problem.
		forvalues i = 0/999 {
			replace draw_`i' = rnormal(0.987, 0.004) if n_code == "N33" | n_code == "N34"
			replace draw_`i' = 1 if draw_`i' > 1 & (n_code == "N33" | n_code == "N34")
		}
	// Set medical complications to 0% manually.  We only have 20 cases from MEPS and they're all extremely high DWs, leaving us with 100% long-term probability under our current analysis.  The initial raw incidence data is also extremely high, especially in the US, so giving this a high lt prob is exploding the YLDs.  We've decided we are forced to assume 0% until a better solution is created or better data is collected.
		forvalues i = 0/999 {
			replace draw_`i' = 0 if n_code == "N46"
		}
	// Set outpatient lt probabilities to 0 for contusions and poisonings.  These are high in the data but don't make sense to go long-term from outpatient.
		forvalues i = 0/999 {
			replace draw_`i' = 0 if (n_code == "N44" | n_code == "N41") & inpatient == 0 
		}
	export delimited using "`tmp_dir'/FILEPATH.csv", delim(",") replace
	
	}

	if $draws == 1 {
// Output draws
	local code = "prob_long_term/FILEPATH.do"
	local type = "epi"
	
	** submit jobs
		local n 0
		get_demographics, gbd_team(epi) clear
		global location_ids `r(location_ids)'

		if `tester' == 1 {
			global location_ids 44550
		}


		foreach location_id of global location_ids {
		// Create check file upon submission and then delete when job is complete
			local name _`step_num'_`location_id'
			! qsub -o FILEPATH -P proj_injuries -N `name' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FILEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id'"
			local n = `n' + 1
		}
	}
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

