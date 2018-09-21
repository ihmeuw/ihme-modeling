	
** **************************************************************************
** PREP STATA
** **************************************************************************
	
	// prep stata
	clear all
	set more off
	set maxvar 32000

	// Set OS flexibility 
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
		local h "~"
	}
	else if c(os) == "Windows" {
		local j "J:"
		local h "H:"
	}
	sysdir set PLUS "`h'/ado/plus"

	// parse incoming syntax elements
	cap program drop parse_syntax
	program define parse_syntax
		syntax, root_dir(string) date(string) out_dir(string)
		c_local out_dir "`out_dir'"
		c_local date "`date'"
		c_local root_dir "`root_dir'"
	end
	parse_syntax, `0'
	
	run "GET_DEMOGRAPHICS SHARED FUNCTION"
	run "GET_COVARIATE_ESTIMATES SHARED FUNCTION"
	run "GET_POPULATION SHARED FUNCTION"
	run "INTERPOLATE SHARED FUNCTION"

** **************************************************************************
** RUN PARALLEL PROCESSES
** **************************************************************************
	
	// make scratch dir for interpolated results
	cap mkdir "`out_dir'/scratch"
	if _rc {
		!rm -rf "`out_dir'/scratch"
		mkdir "`out_dir'/scratch"
	}
	cap mkdir "`out_dir'/tmp"
	if _rc {
		!rm -rf "`out_dir'/tmp"
		mkdir "`out_dir'/tmp"
	}

	// calculate live births
	get_covariate_estimates, covariate_name_short(ASFR) clear
	drop if age_group_id < 6
	drop if age_group_id > 16
	keep if sex_id == 2
	tempfile asfr
	save `asfr', replace
	levelsof year_id, local(years)
	levelsof location_id, local(locs)
	get_population, year_id(`years') location_id(`locs') sex_id(2) age_group_id(6 7 8 9 10 11 12 13 14 15 16) clear
	merge 1:1 age_group_id year_id location_id using `asfr'
	drop _merge
	gen live_births = mean_value * population
	keep live_births location_id age_group_id year_id
	save "`out_dir'/scratch/live_births.dta", replace
	keep year_id age_group_id live_births location_id
	tempfile livebirths
	save `livebirths', replace

	get_demographics, gbd_team(cod) gbd_round_id(4) clear
	local location_ids = r(location_ids)

	//interpolate early syphilis prevalence for females across all locations, age groups, starting from 19890
	interpolate, gbd_id_field(modelable_entity_id) gbd_id(3948) measure_ids(5) reporting_year_start(1980) reporting_year_end(2016) sex_ids(2) source(epi) gbd_round_id(4) clear
	drop sex_id measure_id
	
	merge 1:1 year_id age_group_id location_id using "`livebirths'", keep(3) nogen
	forvalues i = 0/999 {
		replace draw_`i' = draw_`i' * live_births
	}
	collapse (sum) draw* , by(year_id location_id)
	rename draw* syph_moms_draw*

	// merge on visit proportions 
	joinby location_id year_id using "`out_dir'/models/anc_visit_props.dta"

	rename draw* anc_visit_prop_draw*
	preserve
		// no visits mean no treatment
		keep if visits == "_0"
		drop visits
		forvalues i = 0/999 {
			gen draw_`i' = syph_moms_draw_`i' * anc_visit_prop_draw_`i'
		}
		keep year_id location_id draw* 
		gen treatment = 0 // no treatment in model_nn_death_by_treatment
		tempfile notreat 
		save `notreat', replace
	restore
	drop if visits == "_0"

	// merge on antenatal syphilis testing
	merge 1:1 location_id year_id visits using "`out_dir'/models/anc_syph_test.dta", keep(1 3) nogen

	// merge on antenatal syphilis treatment
	merge 1:1 location_id year_id visits using "`out_dir'/models/anc_syph_treatment.dta", nogen keep(1 3)

	// find number by treatment type
	//this is only those who have 1-3 visits
	forvalues i = 0/999 {
		gen adetreat_draw_`i' = syph_moms_draw_`i' * anc_visit_prop_draw_`i' * anc_syph_test_`i' * anc_syph_treat_`i'
		gen inadetreat_draw_`i' = syph_moms_draw_`i' * anc_visit_prop_draw_`i' * anc_syph_test_`i' * (1 - anc_syph_treat_`i')
		gen notreat_draw_`i' = syph_moms_draw_`i' * anc_visit_prop_draw_`i' - adetreat_draw_`i' - inadetreat_draw_`i'
	}
	keep year_id location_id inadetreat* notreat*

	// reshape manually because fast
	preserve 
		keep year_id location_id inadetreat*
		rename inadetreat_* *
		gen treatment = 1 // inadequate treatment in model_nn_death_by_treatment
		//this data set gives you the proportion inadequately treated 
		append using `notreat'
		//After you append, this data set gives you the proportion with 1-3 visits that are inadequately treated plus those with 0 visits
		save `notreat', replace
	restore
	keep year_id location_id notreat*
	rename notreat_* *
	gen treatment = 0 // no treatment in model_nn_death_by_treatment
	//this data gives yo uthe proportion untreated 
	append using `notreat'
	//After you append, this data set gives you the proportion with 1-3 visits that are inadequately treated, plus those that received no treatment 
	//and those that had 0 visits. The latter 2 both get a treatment = 0 designation.

	// apply death rates by treatment
	merge m:1 treatment using "`out_dir'/models/nn_death_by_treatment.dta"
	forvalues i = 0/999 {
		replace draw_`i' = draw_`i' * nn_dth_rate`i'
	}
	drop nn_dth_rate*

	// collapse into years
	collapse (sum) draw* , by(year_id location_id)
	
	// apply age sex pattern
	cross using "`out_dir'/models/cod_data_prop.dta"
	forvalues i = 0/999 {
		replace draw_`i' = draw_`i' * prop
	}
	drop prop
	gen measure_id = 1
	
	//save it on the cluster
	save "saved_file", replace

	//local location_ids 7
	// save to temp folder
	local a = 0
	foreach location of local location_ids{
		!qsub -N "syphilis_`location'" -pe multi_slot 10 -P "proj_custom_models" "SHELL" "`root_dir'/create_files.do" "out_dir(`out_dir') location(`location') date(`date')"
		local ++ a
	}

	//check to make sure all files are written
	local i = 0
	while `i' == 0{
		local checks : dir "`out_dir'/checks" files "finished_loc*.txt", respectcase
		local count: word count `checks'
		di "checking `c(current_time)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 60000
	}
	
	local best "no"
	local description "congenital syphilis using early syphilis model 146552 with data on date: `date'"
	foreach cause in 393 394 {
		!qsub -N "save_syph_`cause'" -l mem_free=40 -pe multi_slot 20 -P "proj_custom_models" "SHELL" "`root_dir'/save.do" "in_dir(`out_dir'/tmp) cause_id(`cause') mark_best(`best') description(`description')"
	}
	

	