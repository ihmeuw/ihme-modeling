// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author:		USERNAME
// Purpose: 	Saves shock mortality to be pulled in later when we apply the ratios
// DON'T EDIT - prep stata

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
		global prefix "FILEPATH:"
	}
	
// Global can't be passed from master when called in parallel
	if "`1'" == "" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "03a"
		local 5 impute_short_term_shock_inc
		local 6 "FILEPATH"
		local 7 555
		local 8 1
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
    // country
    local location_id `7'
    // sex 
    local sex_id `8'
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/FILEPATH"
	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/FILEPATH.smcl", replace name(worker)
	if !_rc local close_log 1
	else local close_log 0

	// check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "`root_j_dir'/FILEPATH/`date'" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "`root_j_dir'/FILEPATHd.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}

	
// *******************************************************************************************************************************************************************

// Import GBD functions
local gbd_ado "FILEPATH"
local gbd_ado_2 "FILEPATH"
local diag_dir "`out_dir'/FILEPATH"
adopath + `gbd_ado'
adopath + `gbd_ado_2'
adopath + "`code_dir'/ado"

	
// Start step timer
	local timer_name "`location_id'_`sex_id'"
	local slots 2
	di "`timer_name'"
	start_timer, dir("`diag_dir'") name("`timer_name'") slots(`slots')
	

// Load GBD parameters
load_params

// Load map of ME ids 
insheet using "`code_dir'/FILEPATH.csv", comma names clear
keep if injury_metric == "Adjusted data"
tempfile me_map
save `me_map', replace

** store all the years we have modeled data for in the cod and epi databases
foreach dbase in cod epi {
	get_demographics , gbd_team(`dbase') clear

	global year_ids `r(year_ids)'
	global age_group_ids `r(age_group_ids)'
	global location_ids `r(location_ids)'
	global sex_ids `r(sex_ids)'

	local `dbase'_years = ""

	foreach year_id of global year_ids {
		local `dbase'_years = "``dbase'_years' "+"`year_id'"
		}

	insheet using "`code_dir'/FILEPATH.csv", comma names clear

	rename year_ids year_id
	rename sex_ids sex_id
	rename location_ids location_id
	rename age_group_ids age_group_id

	keep year_id
	rename year_id years
	summ years
	local `dbase'_years_min = r(min)	
	tempfile `dbase'_years_map
	save ``dbase'_years_map', replace
}
di "`epi_years'"
di "`cod_years'"


insheet using "`in_dir'/FILEPATH.csv", comma names clear

levelsof imputed_ecode, local(imputed_ecodes) clean

capture ssc install sxpose
sxpose, firstnames clear
tempfile relationships
save `relationships', replace
	
get_demographics, gbd_team(cod) clear

global year_ids `r(year_ids)'
global age_group_ids `r(age_group_ids)'
global location_ids `r(location_ids)'
global sex_ids `r(sex_ids)'
	

// Get relevant ids for draw calls
	get_ids, table(measure) clear
		keep if measure_name == "Deaths"
		local cod_measure_id = measure_id
	get_ids, table(metric) clear
		keep if metric_name == "Number"
		local cod_metric_id = metric_id

local noshock_counter=0
local save_counter=0
foreach shock_e_code of local imputed_ecodes {

	di "in loop for `shock_e_code'"
	if "`shock_e_code'"=="inj_war_warterror" {
		local shock_type war_warterror
		local cause_id 945
	}
	if "`shock_e_code'"=="inj_war_execution" {
		local shock_type war_execution
		local cause_id 854
	}
	if "`shock_e_code'"=="inj_disaster" {
		local shock_type disaster
		local cause_id 729
	}
	
	use `relationships', clear
	
	get_demographics, gbd_team(cod) clear

	global year_ids `r(year_ids)'
	global age_group_ids `r(age_group_ids)'
	global location_ids `r(location_ids)'
	global sex_ids `r(sex_ids)'

	get_best_model_versions, entity(cause) ids(`cause_id') status(latest) clear
		keep if sex_id == `sex_id'
		local best_model_version = model_version_id

	get_draws, gbd_id_field(cause_id) gbd_id(`cause_id') source(codem) location_ids(`location_id') sex_ids(`sex_id') year_ids($year_ids) gbd_round_id(4) age_group_ids($age_group_ids) status(best) clear

	* it is a restricted model so it shouldn't be there for neonates

	if "`shock_e_code'" == "inj_war_execution" {
		expand 2 if age_group_id == 4, gen(dupe_2)
		replace age_group_id = 2 if dupe_2 == 1

		expand 2 if age_group_id == 4, gen(dupe_3)
		replace age_group_id = 3 if dupe_3 == 1

		drop dupe_*

		foreach i of numlist 0/999 {
			replace draw_`i' = 0 if age_group_id == 2 | age_group_id == 3
		}
	}

	keep location_id sex_id age_group_id year_id draw_* pop 
	di "got shock results for `location_id' `shock_type'"

	preserve
	describe, replace clear
	keep if regexm(name, "draw")
	clear mata
	putmata name, replace
	restore
	quietly {
		forvalues j=0(1)999 {
			local i=`j'+1
			mata: st_local("name",name[`i'])
			rename `name' shock_draw_`j'
		}
	}
	
	
	di "saving shocks data"
	count
	if `r(N)'==0 {
		di "there is no shock mortality data for this sex/location for `shock_type'"
		local ++noshock_counter
	}
	
	else {
		
		forvalues j=0(1)999 {
		quietly replace shock_draw_`j'=shock_draw_`j'/pop
		}
		drop pop
		
		rename shock_draw_* draw_*
		format draw* %16.0g
		
		capture gen ecode = "`shock_e_code'"
		local ++save_counter
		
		if `save_counter'==1 {
			tempfile all_appended
			save `all_appended', replace
		}
		if `save_counter'>1 {
			append using `all_appended'
			save `all_appended', replace
		}
		
	}
	** end loop for sex/location with shock data
}
** end shock_e_code loop

di `noshock_counter'
if `noshock_counter'<3 {
	use `all_appended', clear

	isid age_group_id ecode year_id

	sort ecode year_id age_group_id

	levelsof ecode, local(shocks) clean

	foreach code of local shocks {
		preserve
		keep if ecode == "`code'"
		export delimited using "`tmp_dir'/FILEPATH.csv", delim(",") replace

		fastrowmean draw*, mean_var_name("mean")
		fastpctile draw*, pct(2.5 97.5) names(ll ul)
		drop draw*
		format mean ul ll %16.0g

		export delimited using "`tmp_dir'/FILEPATH.csv", delim(",") replace
		restore
	}
}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

	// End step timer
		end_timer, dir("`diag_dir'") name("`timer_name'")

	// write check file to indicate sub-step has finished
		file open finished using "`tmp_dir'/FILEPATH.txt", replace write
		file close finished
	
	log close worker
	erase "`out_dir'/FILEPATH.smcl"
	
	
