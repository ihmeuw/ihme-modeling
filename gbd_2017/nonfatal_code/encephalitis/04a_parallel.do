// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		
// Last updated:	10:04 AM 8:31 AM 10/21/2014
// Description:	Parallelization of 04a_dismod_prep_wmort

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
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
	adopath + "FILEPATH"
	// functional
	local functional "encephalitis"
	// grouping
	local grouping "long_modsev epilepsy"
	// directory for pulling files from previous step
	local pull_dir_03b "FILEPATH"

	// get locals from demographics
	get_demographics, gbd_team(epi) clear
	local years = r(year_id)
	local sexes = r(sex_id)
	local ages = r(age_group_id)

	// set locals for encephalitis meids
	local epilepsy_encephalitis = 2821
	local long_modsev_encephalitis = 2815

	set seed 2
	
	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/02_temp/02_logs/`step_num'_`group'_`location'.smcl", replace
	if !_rc local close 1
	else local close 0
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE
run /home/j/temp/central_comp/libraries/current/stata/get_draws.ado
	// get estimates from epilepsy (emr) - new for ODE solver
	di "pulling epilepsy excess mortality estimates"

	get_draws, gbd_id_type(modelable_entity_id) gbd_id(2403) source(epi) measure_id(9) location_id(`location') age_group_id(`ages') gbd_round_id(5) clear 

	egen meas_value = rowmean(draw_*)
	egen meas_stdev = rowsd(draw_*)
	gen measure = "mtexcess"
	drop draw_*
	drop measure_id model_version_id modelable_entity_id
	preserve

	foreach year of local years {
		foreach sex of local sexes {
			keep if sex_id == `sex' & year_id == `year'
			save "FILEPATH", replace
			restore, preserve
		}
	}

	restore
	clear

	//get estimates from epilepsy (remission) - new for ODE solver 
	di "pulling epilepsy remission estimates"

	get_draws, gbd_id_type(modelable_entity_id) gbd_id(2403) source(epi) measure_id(7) location_id(`location') age_group_id(`ages') gbd_round_id(5) clear 

	egen meas_value = rowmean(draw_*)
	egen meas_stdev = rowsd(draw_*)
	gen measure = "remission"
	drop draw_*
	drop measure_id model_version_id modelable_entity_id
	preserve

	foreach year of local years {
		foreach sex of local sexes {
			keep if sex_id == `sex' & year_id == `year'
			save "FILEPATH", replace
			restore, preserve
		}
	}

	restore
	clear

	//check these files, keep only years you want, count your merges 
	di "pulling mortality deaths data"
	import delimited "FILEPATH", delim(",") varnames(1) clear
	drop if age_group_id == 22
	tempfile mort
	save `mort'

	// pull in population data and merge
	di "pulling in pop data"
	//changed the age_group_ids for getting poulations...definitely need to update the mortality lifetables
	get_population, year_id(`years') location_id(`location') sex_id(`sexes') age_group_id(2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235) clear
	merge 1:1 year_id sex_id age_group_id using `mort', keep(3) nogen

	di "calculate mortality rate"
	forvalues x = 0/999 {
		rename draw_`x' deaths_`x'
		gen mort_`x' = deaths_`x'/population
	}
	drop deaths*

	gen age = "old"
	replace age = "young" if age_group_id < 9
	save `mort', replace

	// input code for smr-->excessmortality calculation here
	di "pulling in SMR data"
	// prepare SMR file of neonatal encephalopathy to be attached to all long_modsev iso3/year/sex
	use "FILEPATH", clear
	keep if cause == "NE" // neonatal encephalopathy
	drop cause parameter mean_og
	forvalues x = 0/999 {
		gen smr_`x' = rnormal(mean, se)
	}

	// format and merge SMR to mortality data
	gen age = "young"
	replace age = "old" if age_start == 20

	merge 1:m age using `mort', keep(3) nogen

	// generating EMR draws
	forvalues x = 0/999 {
		gen emr_`x' = mort_`x' * (smr_`x' - 1)
	}

	// regenerate mean and CIs for DisMod
	drop mean
	egen meas_value = rowmean(emr_*)
	egen meas_stdev = rowsd(emr_*)
	fastpctile emr_*, pct(2.5 97.5) names(lower upper)
	keep location_id sex_id age_group_id year_id meas_value meas_stdev upper lower
	gen measure = "mtexcess"
	gen etiology = ""
	gen modelable_entity_id = .
	gen modelable_entity_name = ""

	order modelable_entity_name modelable_entity_id location_id year_id sex_id age_group_id meas_value meas_stdev lower upper
	foreach year of local years {
		foreach sex of local sexes {
			preserve
			keep if year_id == `year' & sex_id == `sex'
			tempfile smr_`location'_`year'_`sex'
			save `smr_`location'_`year'_`sex''
			restore
		}
	}

	clear
				
// THIS IS NEW
	cap mkdir "FILEPATH"

	foreach year of local years {
		foreach sex of local sexes {
			foreach group of local grouping {
				use "FILEPATH", clear

				** create DisMod input parameters
				egen meas_value = rowmean(draw_*)
				egen meas_stdev = rowsd(draw_*)
				egen lower = rowpctile(draw_*), p(2.5)
				egen upper = rowpctile(draw_*), p(97.5)
				drop draw_*
				gen measure = "incidence"
				drop measure_id
				gen modelable_entity_name = "`functional'"
				order modelable_entity_name modelable_entity_id location_id year_id sex_id age_group_id meas_value meas_stdev lower upper 

				if "`group'" == "epilepsy" {
					append using "FILEPATH"
					append using "FILEPATH"
				}
				if "`group'" == "long_modsev" {
					append using `smr_`location'_`year'_`sex''
				}

				gen age_start = 0
				replace age_start = 0 if age_group_id == 2
				replace age_start = 0.02 if age_group_id == 3
				replace age_start = 0.08 if age_group_id == 4
				replace age_start = 1 if age_group_id == 5
				replace age_start = (age_group_id - 5)*5 if (age_group_id > 5 & age_group_id <=20)
				replace age_start = 80 if age_group_id == 30
				replace age_start = 85 if age_group_id == 31
				replace age_start = 90 if age_group_id == 32
				replace age_start = 95 if age_group_id == 235
				gen age_end = 0.02
				replace age_end = 0.08 if age_group_id == 3
				replace age_end = 1 if age_group_id == 4
				replace age_end = 4 if age_group_id == 5
				replace age_end = age_start + 4 if (age_group_id > 5 & age_group_id <=20)
				replace age_end = 84 if age_group_id == 30
				replace age_end = 89 if age_group_id == 31
				replace age_end = 94 if age_group_id == 32
				replace age_end = 99 if age_start == 95 
				drop age_group_id

				replace modelable_entity_name = "`functional'" if modelable_entity_name == ""
				replace location_id = `location' if location_id == .
				replace year_id = `year' if year_id == .
				gen note_modeler = "`group'"

				rename year_id year_start
				gen year_end = year_start
				gen sex = "Female"
				replace sex = "Male" if `sex' == 1

				replace modelable_entity_id = ``group'_`functional''
				if "`group'" == "epilepsy" {
					replace modelable_entity_name = "Epilepsy due to encephalitis"
				}
				else if "`group'" == "long_modsev" {
					replace modelable_entity_name = "Moderate to severe impairment due to encephalitis"
				}
				
				// dtsoi - added next lines to change to ODE solver format 

				drop modelable_entity_name modelable_entity_id location_id year_start year_end note_modeler sex sex_id lower upper grouping
				rename age_start age_lower
				rename age_end age_upper
				rename measure integrand
				gen subreg = "none"
				gen region = "none"
				gen super = "none"
				gen x_ones = 1

				order integrand meas_value meas_std age_lower age_upper subreg region super x_ones
				
				export delimited "FILEPATH", replace
			}
		}
	}

// write check here
	file open finished using "FILEPATH", replace write
	file close finished

// close logs
	if `close' log close
	clear
