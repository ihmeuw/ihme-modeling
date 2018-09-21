// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author:		USERNAME
// Date:			DATE
// Description:	This code applies the EN matrices to the Ecode-platform incidence data FOR SHOCK E-CODES THAT ARE NOT THE USUAL GBD YEARS to get Ecode-Ncode-platform-level incidence data
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
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
	if "`1'"=="" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "04b"
		local 5 scaled_short_term_en_inc_by_platform
		local 6 "FILEPATH"
		local 7 101
		local 8 1
		local 9 otp
	}
	forvalues i = 1/10 {
		di "``i''"
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
    // directory where the code lives
    local code_dir `6'
    // iso3
	local location_id `7'
	// sex
	local sex `8'
	// platform
	local platform `9'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the FILEPATH drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	
	// write log if running in parallel and log is not already open
	log using "`out_dir'/FILEPATH.smcl", replace name(worker)
	
	// start the timer for this substep
	adopath + "FILEPATH"
	** start_timer
	local diag_dir "`tmp_dir'/FILEPATH"
	local old_date "2017_03_06"

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// SETTINGS
	
// Filepaths
	local diag_dir "`out_dir'/FILEPATH"
	local slots 8
	
// Import functions
	adopath + "`code_dir'/ado"

	start_timer, dir("`diag_dir'") name("shock_`location_id'_`platform'_`sex'") slots(`slots')
	
	if "`platform'"=="otp" {
		local platform_num 0
	}
		
	if "`platform'"=="inp" {
		local platform_num 1
	}
** set locals for the location of the non-shock incidence results, the shock incidence results and the EN matrices based on the "steps" files
import excel using "`code_dir'/FILEPATH.xlsx", firstrow clear
** shock incidence
preserve
keep if name == "impute_short_term_shock_inc"
local this_step=step in 1
local shock_inc_dir = "`root_tmp_dir'/FILEPATH"
restore
** EN Matrices results
preserve
keep if name == "EN_matrices"
local this_step=step in 1
local this_step=step in 1
local EN_mat_dir = "`root_tmp_dir'/FILEPATH"
restore

** pull the list of iso3 codes that the results are saved at- written as an ado function and get the income level of the location - do this outside of the parallelized code now and saves to inputs file
use if location_id==`location_id' using "`out_dir'/FILEPATH.dta", clear
local income_level=high_income in 1

** need to change this if in the future you have multiple age categories for EN matrices. In GBD 2013 all under 1 ncodes were aggergated to age=0
global collapsed_under1=1

local counter 0
** get the years with shock data for this iso3/ type
foreach shock in inj_war_warterror inj_disaster inj_war_execution {
	
	** grab the EN matrix for this cause and store the n-codes in memory
	import delimited "`EN_mat_dir'/FILEPATH.csv", delim(",") varnames(1) asdouble clear
	keep if high_income==`income_level'
	keep if sex == `sex'
	rename draw* n_draw*
	preserve
	keep n_code
	duplicates drop
	levelsof n_code, local(`shock'_ns) clean			
	clear mata
	putmata n_code
	count
	local n_count=`r(N)'
	restore
	
	tempfile en_matrix
	save `en_matrix', replace
		
	import delimited "`shock_inc_dir'/FILEPATH.csv", delim(",") varnames(1) asdouble clear
	levelsof ecode
	count if ecode=="`shock'"
	if `r(N)'==0 {
		di "there are no `shock' incidence for `location_id' `platform' `sex'"
	}
	** if there is are results for this e-code, then we want to apply the EN matrix
	else {
		keep if ecode=="`shock'"
		rename draw* inc_draw*
		capture generate inpatient = 1
		replace inpatient=0 if "`platform'"=="otp"
		
		** expand to the number of n_codes that have EN data for this E-code, generate a new variable for "N code" which will make age-n_code combination a unique identifier
		expand `n_count'
		bysort age year: gen n=_n
		gen n_code=""
		forvalues i=1/`n_count' {
			mata: st_local("n_code",n_code[`i'])
			replace n_code="`n_code'" if n==`i'
		}
		
		preserve
			insheet using "`code_dir'/FILEPATH.csv", comma names clear
			tempfile age_codes
			save `age_codes', replace
		restore
		merge m:1 age_group_id using `age_codes', keep(3) nogen
		drop age_group_id
		rename age_start age
	
		** depending on what ages we have e-n data for, we create a new age variable for merging on the EN matrix
		rename age true_age
		gen age = true_age
		if $collapsed_under1 {
			replace age = round(age, 1)
		}
		
		** merge on the E-N matrix and generate the proportions of this e-code that get allocated to each n-code
		merge m:1 age n_code using `en_matrix', keep(3) nogen
			
		** generate proportion of each incidence number alloted to each n-code
			forvalues j=0(1)999 {
				quietly gen draw_`j'=n_draw_`j'*inc_draw_`j'
				drop n_draw_`j' inc_draw_`j'
			}
		drop age
		rename true_age age 
		keep age n_code ecode year inpatient draw_*
		fastrowmean draw_*, mean_var_name(mean_)
		fastpctile draw_*, pct(2.5 97.5) names(ll ul)
		format mean ul ll draw_* %16.0g			
		local ++counter
		
		if `counter'==1 {
			tempfile all_appended
			save `all_appended', replace
		}
		if `counter'>1 {
			append using `all_appended'
			save `all_appended', replace
		}
	}
	** end loop confirming there are results for this e-code
}
** end shock code loop

	use `all_appended', clear
	levelsof age
	
	** keep only the EN combinations that have nonzero draws
	if "`platform'"=="otp" {
		foreach n of global inp_only_ncodes {
			drop if n_code=="`n'"
		}
	}
	egen double dropthisn=rowtotal(draw_*) 
	egen dropit = sum(dropthisn), by(year_id ecode inpatient n_code)
	drop if dropit==0

	count
	if `r(N)' > 0 {
		bysort ecode n_code inpatient : egen ensum = sum(dropthisn)
		drop if ensum==0
	}			

	// if there are no observations, then drop and don't save a file
	// if not, proceed as usual
	count

	if `r(N)' > 0 {

		capture mkdir "`tmp_dir'/FILEPATH"
		rename n_code ncode
		sort ecode ncode inpatient year age
		preserve
		keep ecode ncode inpatient year age draw_*
		save "`tmp_dir'/FILEPATH.dta", replace	
		restore
		
		rename *_ *
		capture mkdir "`tmp_dir'/FILEPATH"
		keep ecode ncode inpatient year age mean ll ul
		save "`tmp_dir'/FILEPATH.dta", replace
	}
	else {
		capture mkdir "`tmp_dir'/FILEPATH"
		file open noobs using "`tmp_dir'/FILEPATH.txt", replace write
		file close noobs
	}
	
	end_timer, dir("`diag_dir'") name(shock_`location_id'_`platform'_`sex')

// Erase log if successful
	log close worker
	erase "`out_dir'/FILEPATH.smcl"	
