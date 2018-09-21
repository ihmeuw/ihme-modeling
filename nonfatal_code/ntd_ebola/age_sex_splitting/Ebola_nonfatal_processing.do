// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USERNAME
// Description:	Compile all the inputs to produce incidence and prevalence for both Ebola sequelae
// FILEPATH

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix FILEPATH
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix FILEPATH
	}
	if "`1'" != "" {
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
	}
	else if "`1'" == "" {
		// base directory on FILEPATH 
		local root_j_dir FILEPATH
		// base directory on FILEPATH
		local root_tmp_dir FILEPATH
		// timestamp of current run (i.e. 2014_01_17)
		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
		local date = subinstr("`date'"," ","_",.)
		// step number of this step (i.e. 01a)
		local step_num "01"
		// name of current step (i.e. first_step_name)
		local step_name "acute_episode_to_chronic_fatigue"
		// step numbers of immediately anterior parent step (i.e. for step 2: 01a 01b 01c)
		local hold_steps ""
		// step numbers for final steps that you are running in the current run (i.e. 11a 11b 11c)
		local last_steps ""
		// directory where the code lives
		local code_dir FILEPATH
	}
	// directory for external inputs
	local in_dir FILEPATH
	// directory for output on the FILEPATH
	local out_dir FILEPATH
	// directory for output on FILEPATH
	capture mkdir FILEPATH
	local tmp_dir FILEPATH
	capture mkdir FILEPATH
	capture mkdir FILEPATH
	capture mkdir FILEPATH
	capture mkdir FILEPATH
	
	// set adopath
	adopath + FILEPATH

	// set the shell file
    local shell_file FILEPATH

	// write log if running in parallel and log is not already open
	cap log using FILEPATH, replace
	if !_rc local close_log 1
	else local close_log 0
	
	// check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir FILEPATH, respectcase
			** remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file FILEPATH
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Import functions
	set seed 15243
	** get demographics
	get_demographics, gbd_team(cod) clear
	local location_ids `r(location_ids)'
	local year_ids `r(year_ids)'
	local age_group_ids `r(age_group_ids)'
	local sex_ids `r(sex_ids)'
	** get_populations
	get_population, year_id(`year_ids') location_id(`location_ids') sex_id(`sex_ids') age_group_id(`age_group_ids') clear
	save FILEPATH, replace
	** location data
	get_location_metadata, location_set_id(35) clear
	replace location_name = "Republic of Congo" if location_name == "Congo"
	replace location_name = "USA" if location_name == "United States"
	replace location_name = "Democratic Republic of Congo" if location_name == "Democratic Republic of the Congo"
	save FILEPATH, replace
	
/*
Outline:
	1) Get incident cases for acute episode
		- Use historical dataset for past cases
		- For recent W SSA outbreak: get deaths from CoD, counter-act scaling done on that data
		- Age-split based on W SSA age patterns
		- Apply underreporting factor
		- Add imported cases from recent outbreak
	2) Get prevalence of acute episode
		- Parse out deaths and survivors
		- Assign duration based on outcome
	3) Get incidence of post-Ebola chronic fatigue syndrome
		- Acute incidence with remission isolated (deaths removed)
	4) Get prevalence of post-Ebola chronic fatigue syndrome
		- Apply duration
*/

** *********************************
// Get parameters
	**
	** Case fatality rates by age group
	**
	clear
	gen group = ""
	tempfile cfrs
	save `cfrs', replace
	import excel using FILEPATH, firstrow sheet("CFR") clear
	levelsof Grouping if !inlist(Grouping,"All cases","Male", "Female"), local(Gs)
	foreach G of local Gs {
		preserve
			levelsof CFR_mean if Grouping == "`G'", local(M) c
			levelsof CFR_LCI if Grouping == "`G'", local(L) c
			levelsof CFR_UCI if Grouping == "`G'", local(U) c
			local SE = (`U'-`L')/(2*1.96)
			clear
			set obs 1000
			gen cfr_ = rnormal(`M',`SE')
			gen num = _n-1
			gen group = "`G'"
			append using `cfrs'
			save `cfrs', replace
		restore
	}
	use `cfrs', clear
	replace cfr_ = 99.99 if cfr >= 100
	replace cfr_ = 0.01 if cfr <= 0
	reshape wide cfr_, i(group) j(num)
	save `cfrs', replace
	keep group
	gen age_group_id = .
	expand 4 if group == "<15 yrs"
	bysort group : replace age_group_id = _n + 3 if group == "<15 yrs"
	expand 6 if group == "15-44 yrs"
	bysort group : replace age_group_id = _n + 7 if group == "15-44 yrs"
	expand 11 if group == ">=45 yrs"
	bysort group : replace age_group_id = _n + 13 if group == ">=45 yrs"
	replace age_group_id = age_group_id + 9 if age_group_id > 20
	replace age_group_id = 235 if age_group_id == 33
	tempfile cfr_group_map
	save `cfr_group_map', replace
	**
	** Underreporting factor
	**
	clear
	gen factor_type = ""
	tempfile ufs
	save `ufs', replace
	import excel using FILEPATH, firstrow sheet("corr_factor") clear
	levelsof Correctionfactor, local(CFs)
	foreach CF of local CFs {
	preserve
		levelsof Mean if Correctionfactor == "`CF'", local(M) c
		levelsof Lower if Correctionfactor == "`CF'", local(L) c
		levelsof Upper if Correctionfactor == "`CF'", local(U) c
		local SE = (`U'-`L')/(2*1.96)
		clear
		set obs 1000
		gen factor_type = lower("`CF'")
		gen uf_ = rnormal(`M',`SE')
		gen num = _n-1
		append using `ufs'
		save `ufs', replace
	restore
	}
	use `ufs', clear
	reshape wide uf_, i(factor_type) j(num)
	tempfile ufs
	save `ufs', replace
	**
	** Acute durations
	**
	clear
	gen group = ""
	tempfile durs
	save `durs', replace
	import excel using FILEPATH, firstrow sheet("duration") clear
	levelsof Grouping, local(Gs)
	foreach G of local Gs {
		preserve
			levelsof mean if Grouping == "`G'", local(M) c
			levelsof mean_LCI if Grouping == "`G'", local(L) c
			levelsof mean_UCI if Grouping == "`G'", local(U) c
			local SE = (`U'-`L')/(2*1.96)
			clear
			set obs 1000
			gen dur_ = rnormal(`M',`SE')
			gen num = _n-1
			gen group = "`G'"
			append using `durs'
			save `durs', replace
		restore
	}
	use `durs', clear
	replace dur_ = dur_ / 365
	reshape wide dur_, i(group) j(num)
	gen outcome = "death" if group == "onset_to_death"
	replace outcome = "remission" if group == "onset_to_discharge"
	order outcome dur*
	keep outcome dur*
	save `durs', replace
	**
	** Chronic duration
	**
	import excel using FILEPATH, firstrow sheet("ersatz") clear
	levelsof Mean, local(M) c
	levelsof LCI95, local(L) c
	levelsof HCI95, local(U) c
	local SE = (`U'-`L')/(2*1.96)
	clear
	set obs 1000
	gen dur_ = rnormal(`M',`SE')
	gen num = _n-1
	gen outcome = "post"
	reshape wide dur_, i(outcome) j(num)
	// Assign durations > 1 to next year
	forval d = 0/999 {
		gen dur_carryover_`d' = dur_`d' - 1
		replace dur_carryover_`d' = 0 if dur_carryover_`d' < 0
		replace dur_`d' = dur_`d' - dur_carryover_`d'
	}
	tempfile post_dur
	save `post_dur', replace

** *********************************
// Get deaths
	use FILEPATH if year >= 1989 & year <= 2016, clear
	merge m:1 location_id using FILEPATH, keepusing(location_name) nogen keep(3)
	keep location_name year age sex best upper lower
	rename best deaths
	rename sex sex_id
	rename year year_id
	rename age age_group_id
	replace sex_id = "1" if sex_id == "male"
	replace sex_id = "2" if sex_id == "female"
	destring sex_id, replace
	preserve
		** take the distribution of the redistributed deaths and create draws (took out the death correction factor here)
		gen se = (upper - lower)/(2*1.96)
		forvalues i = 0/999 {
			gen deaths_`i' = rnormal(deaths, se)
		}
		drop deaths
		tempfile all_year_deaths
		save `all_year_deaths', replace
	restore
	
// Convert deaths to cases, save 2014 and 2015 to add to other dataset (only deaths for most recent epidemic)
	merge m:1 age_group_id using `cfr_group_map', assert(2 3) keep(2 3) nogen
	merge m:1 group using `cfrs', assert(3) nogen
	gen factor_type = "cases"
	merge m:1 factor_type using `ufs', assert(2 3) keep(3) nogen
	forval x = 0/999 {
		gen cases_`x' = (deaths / (cfr_`x'/100)) * uf_`x'
	}
	drop deaths group cfr* factor_type uf*
	preserve
		keep if inlist(year_id, 2014, 2015, 2016, .)
		** already have these country-years
		drop if year == 2014 & location_name == "Democratic Republic of Congo" 
		drop if year == 2014 & location_name == "Mali" 
		drop if year == 2014 & location_name == "Nigeria"
		drop if year == 2016 & location_name == "Sierra Leone"
		drop if year == 2016 & location_name == "Guinea"
		drop if location_name == "Texas"
		
		expand 2 if inlist(location_name, "Liberia", "Guinea") & year == 2014 & age_group_id == 235, gen(new)
		replace sex_id = 1 if new == 1
		forvalues i = 0/999 {
			replace cases_`i' = 0 if new == 1
		}
		drop new
		
		expand 4 if location_name == "Guinea" & year == 2015 & age_group_id == 31, gen(new)
		bysort new: gen count = _n
		replace count  = 0 if new == 0
		replace sex_id = 1 if count == 1
		replace age_group_id = 32 if count > 1 & count < 4
		replace sex_id = 1 if count == 2
		forvalues i = 0/999 {
			replace cases_`i' = 0 if new == 1
		}
		drop new count
		
		expand 2 if location_name == "Liberia" & year == 2015 & age_group_id < 30, gen(new)
		drop if new == 1 & age_group_id == 11
		replace sex_id = 1 if new == 1
		forvalues i = 0/999 {
			replace cases_`i' = 0 if new == 1
		}
		drop new
		expand 5 if location_name == "Liberia" & year == 2015 & age_group_id == 20, gen(new)
		bysort sex_id new: gen count = _n
		replace count = 0 if new == 0
		replace age_group_id = 29+count if new == 1
		replace age_group_id = 235 if age_group_id == 33 & new == 1
		forvalues i = 0/999 {
			replace cases_`i' = 0 if new == 1
		}
		drop new count

		drop if year == . | sex_id == .
		forval x = 0/999 {
			replace cases_`x' = 0 if cases_`x' == .
		}
		tempfile W_SSA_2014_2015
		save `W_SSA_2014_2015', replace
	restore
	
// Get age-split proportions
	collapse (sum) cases*, by(age_group_id sex_id) fast
	forval x = 0/999 {
		egen tot_cases_`x' = sum(cases_`x')
		gen prop_`x' = cases_`x' / tot_cases_`x'
		drop cases_`x' tot_cases_`x'
	}
	rename sex_id rep_sex_id
	rename age_group_id rep_age_group_id
	gen sex_id = 3
	gen age_group_id = 22
	tempfile split_dist
	save `split_dist', replace
	
** *********************************
// Get cases
	** Imported
	import excel using FILEPATH, firstrow sheet("Confirmed Cases") clear
	** Both sexes
	rename Both casesboth__NO_AGE
	** Males
	foreach var of varlist Male-V {
		local varname = `var'[1]
		local varname = subinstr("`varname'"," ","_",.)
		local varname = subinstr("`varname'","-","_",.)
		local varname = subinstr("`varname'","+","",.)
		rename `var' casesmale__`varname'
	}
	rename casesmale__Missing casesmale__NO_AGE
	** Females
	foreach var of varlist Female-AO {
		local varname = `var'[1]
		local varname = subinstr("`varname'"," ","_",.)
		local varname = subinstr("`varname'","-","_",.)
		local varname = subinstr("`varname'","+","",.)
		rename `var' casesfemale__`varname'
	}
	rename casesfemale__Missing casesfemale__NO_AGE
	gen origin = "imported"
	rename A Country
	gen Note = "No age or sex" if casesboth__NO_AGE != "0"
	tempfile imported_cases
	save `imported_cases', replace
	
	** Native
	import excel using FILEPATH, firstrow clear
	** Both sexes
	foreach var of varlist All-Y {
		local varname = `var'[1]
		local varname = subinstr("`varname'"," ","_",.)
		local varname = subinstr("`varname'","-","_",.)
		local varname = subinstr("`varname'","+","",.)
		rename `var' casesboth__`varname'
	}
	** Males
	foreach var of varlist Male-AU {
		local varname = `var'[1]
		local varname = subinstr("`varname'"," ","_",.)
		local varname = subinstr("`varname'","-","_",.)
		local varname = subinstr("`varname'","+","",.)
		rename `var' casesmale__`varname'
	}
	** Females
	foreach var of varlist Female-BQ {
		local varname = `var'[1]
		local varname = subinstr("`varname'"," ","_",.)
		local varname = subinstr("`varname'","-","_",.)
		local varname = subinstr("`varname'","+","",.)
		rename `var' casesfemale__`varname'
	}
	gen origin = "native"
	append using `imported_cases'
	drop if Country == "Raw deaths" | Country == ""
	
	** Format
	destring cases*, replace force
	keep if Year >= 1989 & Year <= 2016
	reshape long cases, i(Country Year Note origin) j(sex_age) string
	split sex_age, p("__")
	rename sex_age1 sex
	rename sex_age2 age
	drop sex_age
	replace Note = "No age or sex" if Country == "Uganda" & Year == 2007
	drop if (sex != "both" | age != "NO_AGE") & (Note == "No age or sex" | Note == "No age or gender")
	drop if (sex == "both" | age != "NO_AGE") & Note == "No age"
	drop if (sex == "both" | age == "NO_AGE") & Note == ""
	rename Country location_name
	rename Year year_id
	gen age_group_id = .
	replace age_group_id = 4 if age == "0"
    replace age_group_id = 5 if age == "1_4"
    replace age_group_id = 6 if age == "5_9"
    replace age_group_id = 7 if age == "10_14"
    replace age_group_id = 8 if age == "15_19"
    replace age_group_id = 9 if age == "20_24"
    replace age_group_id = 10 if age == "25_29"
    replace age_group_id = 11 if age == "30_34"
    replace age_group_id = 12 if age == "35_39"
    replace age_group_id = 13 if age == "40_44"
    replace age_group_id = 14 if age == "45_49"
    replace age_group_id = 15 if age == "50_54"
    replace age_group_id = 16 if age == "55_59"
    replace age_group_id = 17 if age == "60_64"
    replace age_group_id = 18 if age == "65_69"
    replace age_group_id = 19 if age == "70_74"
    replace age_group_id = 20 if age == "75_79"
    replace age_group_id = 30 if age == "80_84"
    replace age_group_id = 31 if age == "85_89"
    replace age_group_id = 32 if age == "90_94"
    replace age_group_id = 235 if age == "95"
	replace age_group_id = 22 if age == "NO_AGE"
	gen sex_id = .
	replace sex_id = 1 if sex == "male"
	replace sex_id = 2 if sex == "female"
	replace sex_id = 3 if sex == "both"
	drop age sex Note
	gen factor_type = "cases"
	merge m:1 factor_type using `ufs', assert(2 3) keep(3) nogen
	forval x = 0/999 {
		gen cases_`x' = cases * uf_`x' if origin == "native"
		replace cases_`x' = cases if origin == "imported"
	}
	drop uf* factor_type cases origin
	joinby age_group_id sex_id using `split_dist', unmatched(master)
	forval x = 0/999 {
		replace cases_`x' = cases_`x' * prop_`x' if _m == 3
	}
	replace age_group_id = rep_age_group_id if _m == 3
	replace sex_id = rep_sex_id if _m == 3
	drop rep* prop* _m
	tempfile historic_cases
	save `historic_cases', replace

** *********************************
// Combine cases across all years, parse outcome for prevalence
	use `historic_cases', clear
	append using `W_SSA_2014_2015'
	renpfix cases draw
	forvalues i = 0/999 {
		replace draw_`i' = 0 if draw_`i' == .
	}
	drop if age_group_id == .
	expand 2 if sex_id == ., gen(new)
	replace sex_id = 1 if new == 1
	replace sex_id = 2 if sex_id == .
	drop new
	gen measure_id = 6
	duplicates tag location_name year_id age_group_id sex_id, gen(dup)
	drop if dup == 1 & draw_0 == 0
	drop dup
	duplicates drop location_name year_id age_group_id sex_id, force
	save FILEPATH, replace
	preserve
		** Save list for final check
		keep location_name year_id
		duplicates drop
		save FILEPATH, replace
	restore
	
	** Divide based on outcome
	merge 1:1 location_name year_id age_group_id sex_id using `all_year_deaths'
	forval x = 0/999 {
		replace deaths_`x' = 0 if deaths_`x' == .
		replace draw_`x' = deaths_`x' + (deaths_`x'*.01) if deaths_`x' > draw_`x'
		gen remission_`x' = draw_`x' - deaths_`x'
		rename deaths_`x' death_`x'
	}
	foreach outcome in remission death {
		preserve
			keep location_name year_id age_group_id sex_id `outcome'*
			renpfix `outcome' draw
			gen outcome = "`outcome'"
			tempfile cases_`outcome'
			save `cases_`outcome'', replace
		restore
	}
	use `cases_remission', clear
	append using `cases_death'
	merge m:1 outcome using `durs', assert(3) nogen
	forval x = 0/999 {
		replace draw_`x' = draw_`x' * dur_`x'
	}
	drop dur* outcome
	collapse (sum) draw*, by(location_name year_id age_group_id sex_id) fast
	gen measure_id = 5
	save FILEPATH, replace
	
** *********************************
// Get chronic fatigue syndrome
	use `cases_remission', clear
	drop if age_group_id == .
	drop outcome
	gen measure_id = 6
	save FILEPATH, replace
	gen outcome = "post"
	merge m:1 outcome using `post_dur', assert(3) nogen
	forval x = 0/999 {
		replace draw_`x' = draw_`x' * dur_`x'
	}
	preserve
		replace year = year + 1
		forval x = 0/999 {
			replace draw_`x' = draw_`x' * dur_carryover_`x'
		}
		tempfile carryover
		save `carryover', replace
	restore
	append using `carryover'
	collapse (sum) draw*, by(location_name year_id age_group_id sex_id measure_id) fast
	replace measure_id = 5

	save FILEPATH, replace
	// save chronic location years
	egen draw_mean = rowmean(draw_*)
	drop if draw_mean == 0
	keep location_name year_id
	duplicates drop
	save FILEPATH, replace
	
** *********************************
// By location, check file for endemicity, save 0s if not
	cap mkdir FILEPATH
	foreach location_id of local location_ids {
		FILEPATH
		
		sleep 100
	}
	
// Check for full set of results
	get_demographics, gbd_team(epi) clear
	local location_ids `r(location_ids)'
	foreach location_id of local location_ids {
		capture confirm file FILEPATH
		if _rc == 601 noisily display "Searching for `location_id'  -- `c(current_time)'"
		while _rc == 601 {
			capture confirm file FILEPATH
			sleep 1000
		}
		if _rc == 0 {
			noisily display "`location_id' FOUND!"
		}
	}

// Upload
  local saveyears 1990
  forval x = 1991/2016 {
	local saveyears `saveyears' `x'
  }
 	// run save_results
	qui run FILEPATH
	save_results, modelable_entity_id(9668) description("Incidence and prevalence of acute Ebola episode") env("prod") years(`saveyears') in_dir(FILEPATH) file_pattern({location_id}.csv) mark_best("yes")

    save_results, modelable_entity_id(9669) description("Incidence and prevalence of chronic fatigue following acute Ebola episode") years(`saveyears') env("prod") in_dir(FILEPATH) file_pattern({location_id}.csv) mark_best("yes")

