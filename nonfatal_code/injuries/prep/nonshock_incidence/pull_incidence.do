** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** Purpose:		This sub-step template is for parallelized jobs submitted from main step code
** Author:		USERNAME
** Description:	This code is submitted by the raw_short_term_ecode_inc_by_platform.do file to grab dismod short-term incidence results for e-codes, as well as the study-level covariate for transforming into outpatient results
** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 4g
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
		local 4 "02a"
		local 5 raw_nonshock_short_term_ecode_inc_by_platform

		local 6 "FILEPATH"
		local 7 8
		local 8 2016
		local 9 1
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
    // location id
    local location_id `7'
    // year id
    local year_id `8'
    // sex id 
    local sex_id `9'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the FILEPATH drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on FILEPATH
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	
	// write log if running in parallel and log is not already open

	cap log using "`out_dir'/FILEPATH.smcl", replace 
	if !_rc local close_log 1
	else local close_log 0

	// check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "`root_j_dir'/FILEPATH" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "`root_j_dir'/FILEPATH.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

	di in red "TROUBLESHOOTING"
	di "root_j_dir: `root_j_dir'"
	di "root_tmp_dir `root_tmp_dir'"
	di "date `date'"
	di "step_num `step_num'"
	di "step_name `step_name'"
	di "Code dir: `code_dir'"

** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** SETTINGS
	// Set covariate we are using to adjust final estimates (inpatient) to inpatient+outpatient
	local covariate "beta_incidence_x_s_outpatient"

	set type double, perm
** how many slots were used to run this script
	local slots 4

	local testing 1
	local metric incidence
	
** Filepaths
	local gbd_ado "FILEPATH"
	local diag_dir "`out_dir'/FILEPATH"
	local summ_dir "`tmp_dir'/FILEPATH"
	local draw_dir "`tmp_dir'/FILEPATH"
	
** Import GBD functions
	adopath + "`gbd_ado'"
	adopath + `code_dir'/ado
	adopath + "FILEPATH"

	start_timer, dir("`diag_dir'") name("`location_id'_`year_id'_`sex_id'") slots(`slots')

** Load injury parameters
	
	load_params
	
	get_demographics , gbd_team(epi) clear

	local year_ids `r(year_ids)'
	local age_group_ids `r(age_group_ids)'
	local location_ids `r(location_ids)'
	local sex_ids `r(sex_ids)'

** Pull the list of e-codes that are modeled by DisMod that we are going to transform
	insheet using "`code_dir'/FILEPATH.csv", comma names clear
		keep if injury_metric == "Adjusted data"
		drop if e_code == "inj_war" | e_code == "inj_disaster"
		levelsof modelable_entity_id, l(me_ids)
		keep modelable_entity_id e_code
		tempfile mes 
		save `mes', replace
	
		tempfile appended
		tempfile ecode

	foreach me_id of local me_ids {
		// Pull acause associated with this ME id
		use `mes', clear
			keep if modelable_entity_id == `me_id'
			local e_code = e_code 

		// Get measure_id for incidence 
		get_ids, table(measure) clear
			preserve 
				keep if measure_name == "Incidence"
				local inc_id = measure_id 
			restore
			preserve
				keep if measure_name == "Remission"
				local remission_id = measure_id
			restore
			preserve
				keep if measure_name == "Excess mortality rate"
				local emr_id = measure_id
			restore			

		di "Pulling draws... me_id:`me_id', year_id:`year_id', sex_id:`sex_id', location_id:`location_id'"
		get_draws, gbd_id_field(modelable_entity_id) gbd_id(`me_id') location_ids(`location_id') year_ids(`year_id') sex_ids(`sex_id') age_group_ids($age_group_ids) status(best) source(dismod) clear
		
		drop if inlist(age_group_id, 27, 33, 164)

		keep if measure_id == `inc_id' | measure_id == `remission_id' | measure_id == `emr_id'
		tostring measure_id, replace
		replace measure_id = "inc" if measure_id == "`inc_id'"
		replace measure_id = "remission" if measure_id == "`remission_id'"
		replace measure_id = "emr" if measure_id == "`emr_id'"
		reshape wide draw_*, i(location_id year_id age_group_id sex_id modelable_entity_id model_version_id) j(measure_id, string)
		gen inpatient = 1

		forvalues i = 0/999 {
			replace draw_`i'inc = (exp(draw_`i'inc * (1/52)) - 1) * 52 if age == 0
			replace draw_`i'inc = (exp(draw_`i'inc * (3/52)) - 1) * (52/3) if age == .01
			replace draw_`i'inc = (exp(draw_`i'inc * (48/52)) - 1) * (52/48) if age == .1
		}
		
		// Correct incidence results for mortality
		forvalues i = 0/999 {
			gen draw_`i' = draw_`i'inc * exp( -( draw_`i'emr / draw_`i'remission ) )
		}

		// Save diagnostics ratios 

		preserve 
			forvalues i = 0/999 {
				gen inc_ratio_`i' = draw_`i' / draw_`i'inc 
			}
			egen mean_inc_ratio = rowmean(inc_ratio_*)
			keep location_id year_id age_group_id sex_id modelable_entity_id mean_inc_ratio
			cap mkdir "FILEPATH"
			cap mkdir "FILEPATH"
			save "FILEPATH.dta", replace
		restore

		drop draw_*inc draw_*emr draw_*remission

		** save these as inpatient draws
		gen ecode = "`e_code'"
		save `ecode', replace
		
	** merge on and create outpatient draws
		rename draw* inp_draw*
		merge m:1 inpatient using "FILEPATH.dta", nogen

		** multiply inpatient incidence by the exponentiated outpatient covariate to get all outpatient care incidence
		forvalues j=0/999 {
			generate otp_draw_`j'=inp_draw_`j' * outpatient`j'
			drop outpatient`j' inp_draw_`j'
		}
		
		capture drop iso3 year
		generate year = `year_id'
		generate iso3 = "`iso3'"
		
		** now subtract off inpatient incidence to get just outpatient numbers
		merge 1:1 age inpatient using `ecode'

		drop draw*
		rename otp_draw* draw*
		replace inpatient=0
		
		append using `ecode'
		keep ecode inpatient age draw*
		
	** Append to other e-codes
		cap confirm file `appended'
		if !_rc append using `appended'
		save `appended', replace
	}
		
** Save draws
	format draw* %16.0g
	
	order ecode inpatient age draw*
	sort ecode inpatient age
	export delimited using "`draw_dir'/FILEPATH.csv", replace
		
** Save summary files
	fastrowmean draw*, mean_var_name("mean")
	fastpctile draw*, pct(2.5 97.5) names(ll ul)
	drop draw*
	format mean ul ll %16.0g
	export delimited "`summ_dir'/FILEPATH", replace

** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** CHECK FILES (NO NEED TO EDIT THIS SECTION)
	
** End timer
	end_timer, dir("`diag_dir'") name("`location_id'_`year_id'_`sex_id'")
	
** Close log
	log close 

** write check file to indicate sub-step has finished
	file open finished using "`tmp_dir'/FILEPATH.txt", replace write
	file close finished
	
** Erase log if completed successfully
	erase "`out_dir'/FILEPATH.smcl"
		
