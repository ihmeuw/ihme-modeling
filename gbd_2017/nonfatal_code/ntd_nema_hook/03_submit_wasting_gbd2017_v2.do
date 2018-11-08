/*====================================================================
project:       GBD2017
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:   GBD2017
Output:            Submit script to estimate wasting from the wasting envelope using a weight-for-height z-score
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	clear mata
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "FILEPATH"
	}


* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2017"
	*model step
	local step 03
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*directory for output of draws
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories
	*set up base directories on shared
	capture shell mkdir "FILEPATH"
	capture shell mkdir "FILEPATH"
	capture shell mkdir "FILEPATH"
	capture shell mkdir "FILEPATH"
	capture shell mkdir "FILEPATH"
	capture shell mkdir "FILEPATH"
	capture shell mkdir "FILEPATH"

	*make all directories
	foreach dir in tmp in out log progress {
		capture shell mkdir ``dir'_dir'
	}

*Directory for standard code files
	adopath + "FILEPATH"


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "FILEPATH.smcl", replace
	***********************

	*print macros in log
	macro dir

*Code Testing
	local testing
	*set equal to "*" if not testing

/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Demographics

	get_demographics, gbd_team("ADDRESS") clear
		local gbdages="`r(age_group_id)'"
		local gbdyears="`r(year_id)'"
		local gbdsexes="`r(sex_id)'"

		local wasting_ages 2 3 4 5

*--------------------1.2: Location Metadata

	get_location_metadata, location_set_id(35) clear
		keep if most_detailed==1
		levelsof location_id,local(gbdlocs) clean
		keep location_id ihme_loc_id parent_id path_to_top_parent level location_name
		save "`tmp_dir'/metadataestimates.dta", replace


*--------------------1.3: GBD Skeleton

	get_population, location_id("`gbdlocs'") sex_id("`gbdsexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
		save "`tmp_dir'/skeleton.dta", replace

		*Create Zeroes File for Ages 5+
			drop year_id location_id population
			drop if inlist(age_group_id,2,3,4,5)
			duplicates drop

			forval d = 0/999 {
				gen draw_`d' = 0
			}

			save "`tmp_dir'/age5plus_zeroes.dta", replace

*--------------------1.4: Define meids to use to get draws for STH and wasting

	local heavyinfmeids 1513 1516 1519
	local wastingmeids 1607 1608
	local wormwastingmeids 1515 1518 1521

/*====================================================================
                        2: Prepare STH Heavy Infestation Draws
====================================================================*/


*--------------------2.1: Load Heavy Infestation Draws for Each Worm


	*prep loop
		tempfile heavy_dir_temp
		local stack 1

	*loop through all the heavy infestation meids and get draws
		foreach cause in `heavyinfmeids' {

			di in red "Getting heavy infestation estimates for: `cause'"

			*identify best model version
				quietly get_best_model_versions, entity(modelable_entity) ids(`cause') clear
					local `cause'_mvid = model_version_id
					di in red "using model version ``cause'_mvid'"

			*get draws from that model
				local obs 0
				local counter 1
				while `obs'==0 & `counter'<=10 {
					quietly get_draws, gbd_id_type(modelable_entity_id) gbd_id(`cause') measure_id(5) source(ADDRESS) version_id(``cause'_mvid') location_id(`gbdlocs') age_group_id(`wasting_ages') clear
						quietly des
						local obs = `r(N)'
						di in red "obtained `r(N)' obs"
						local ++counter
				}

				gen meid = `cause'

			*append draws from multipe meids
				if `stack'>1 append using `heavy_dir_temp'
				save `heavy_dir_temp', replace
				local ++ stack
		}

	*Save
		save "`tmp_dir'/all_heavyinf.dta", replace

*--------------------2.2: Calculate proportion of total STH prevalence of heavy worm infestation attributable to each worm

	*Sum prevalences of all worms by Location-Year-Age-Sex
		quietly fastcollapse draw*, by(age_group_id sex_id year_id location_id) type(sum)

		forvalues i = 0/999 {
			quietly replace draw_`i' = 1 if draw_`i' > 1
		}

		rename draw* sumsth*

	*Save
		`testing' save "`tmp_dir'/heavyinf_sum.dta", replace

	*Calculate proportions for later split up of total wasting due to heavy infestation

		merge 1:m age_group_id sex_id year_id location_id using "`tmp_dir'/all_heavyinf.dta", nogen
			*1:m because the latter has 3 meid

		forvalues i = 0/999 {
			quietly replace draw_`i' = draw_`i' / sumsth_`i'
			quietly replace draw_`i' = 0 if sumsth_`i' == 0
				*cant divide by 0 - fill in just so there is no missing
		}

		rename draw_* prop_*

	*Save
		save "`tmp_dir'/propSTHprevperworm.dta", replace



/*====================================================================
                        3: Prepare Wasting Envelope Draws
====================================================================*/


*--------------------3.1: Load Wasting Envelope Draws

	*prep loop
		tempfile wasting_draws
		local a 0

	*loop through all the heavy infestation meids and get draws
		foreach meid in `wastingmeids'{
			get_best_model_versions, entity(modelable_entity) ids(`meid') status(latest) clear
					local `meid'_mvid = model_version_id
					di in red "using model version ``meid'_mvid'"

		*get draws from that model
			local obs 0
			local counter 1
			while `obs'==0 & `counter'<=10 {
				get_draws, gbd_id_type(modelable_entity_id) gbd_id(`meid') measure_id(5) source(ADDRESS) version_id(``meid'_mvid') location_id(`gbdlocs') age_group_id(`wasting_ages') clear
					quietly des
					local obs = `r(N)'
					di in red "obtained `r(N)' obs"
					local ++counter
			}

		*append draws from multipe meids
			if `a'>0 append using `wasting_draws'
			save `wasting_draws', replace
		}

	*Append draws for wasting
		`testing' save "`tmp_dir'/wastingdraws.dta", replace


*--------------------3.2: Sum Prevalence of Wasting Across Types (1607 + 1608)

	*Merge with population and calculate cases
		`testing' use "`tmp_dir'/wastingdraws.dta", clear
		merge m:1 location_id year_id age_group_id sex_id using "`tmp_dir'/skeleton.dta", nogen keep(matched)

		local y 1
		forval x = 0/999 {
			if `y' == 1 di in red "Converting to case space to sum across wasting types"
			display in red ". `y' " _continue
			local ++y

			quietly replace draw_`x' = draw_`x' * population
		}

	*Collapse to sum across 2 different wasting meids to get total # wasting cases
		fastcollapse draw*, type(sum) by(location_id year_id age_group_id sex_id)

	*Merge with population
		merge 1:1 location_id year_id age_group_id sex_id using "`tmp_dir'/skeleton.dta", nogen keep(matched)


		local y 1
		forval x = 0/999 {
			if `y' == 1 di in red "Converting back to rate space for total wasting"
			display in red ". `y' " _continue
			local ++y

			quietly replace draw_`x' = draw_`x'/population
		}

	*Format
		format %16.0g age_group_id draw*
		rename draw* wast*
		keep location_id year_id sex_id age_group_id wast_*

	*Save
		save "`tmp_dir'/wast_draws.dta", replace


/*====================================================================
                        4: Assemble Full Dataset
====================================================================*/

*--------------------4.2: Load Geographic Restrictions

	*load Geographic Restrictions for 3 Worms
		local g 1
		tempfile allGR

		foreach worm in ascariasis trichuriasis hookworm {

			*import geographic restrictions in central comp format
				quietly import delimited "FILEPATH", clear
				gen restrict = 1

			*generate meid variable spcific to file being brought in
				if "`worm'" == "ascariasis" {
					gen modelable_entity_id = 1513
				}
				if "`worm'" == "trichuriasis" {
					gen modelable_entity_id = 1516
				}
				if "`worm'" == "hookworm" {
					gen modelable_entity_id = 1519
				}

			*append all files
				if `g'>1 append using `allGR'
				save `allGR', replace
				local ++g

		}

	*save
		`testing' save "`tmp_dir'/allGR.dta", replace

*--------------------4.2: Merge with wasting draws, sth draw sum, worm proportions

	*Variables: sum of sth prevalence; proportion of heavy inf attributable to each worm
		merge 1:m location_id year_id modelable_entity_id using "`tmp_dir'/propSTHprevperworm.dta", keep(matched using) nogen

	*Merge with wasting draws
		merge m:1 location_id year_id age_group_id sex_id using "`tmp_dir'/wast_draws.dta", nogen

	*Make locyear variable
		quietly egen locyear = concat(location_id year_id), p("_")
		levelsof locyear, local(locyears) c

	*replace meids for heavy infections to meids for wasintg
		replace modelable_entity_id = 1515 if modelable_entity_id == 1513
		replace modelable_entity_id = 1518 if modelable_entity_id == 1516
		replace modelable_entity_id = 1521 if modelable_entity_id == 1519


	*Save
		`testing' save "`tmp_dir'/prepped_wasting_inputs.dta", replace


/*====================================================================
                        4: Submit Wasting Estimation Script to Qsub
====================================================================*/


*--------------------4.1: Loop Over Locations to Submit Wasting Estimation Script to Qsub


	*remove spaces from locals to pass through qsub
		local gbdyears = subinstr("`gbdyears'", " ", "_", .)
		local gbdages = subinstr("`gbdages'", " ", "_", .)
		local gbdsexes = subinstr("`gbdsexes'", " ", "_", .)
		local wasting_ages = subinstr("`wasting_ages'", " ", "_", .)

	 *make reference file
		`testing' use "`tmp_dir'/prepped_wasting_inputs.dta", clear
		preserve
			keep locyear restrict
			duplicates drop
			*expand = `num_meids'
			sort locyear
			egen modelable_entity_id = fill(`wormwastingmeids' `wormwastingmeids')
			gen id=_n
			*save "`tmp_dir'/new_task_directory.dta", replace
			save "`tmp_dir'/task_directory.dta", replace

			*get number of tasks
			quietly sum id
			local num_tasks `r(max)'
			di in red "# tasks: `num_tasks'"
		restore

	*write one file per meid
		`testing' use "`tmp_dir'/prepped_wasting_inputs.dta", clear
		keep if restrict != 1
		foreach meid in `wormwastingmeids'{
			preserve
			keep if modelable_entity_id == `meid'
			save "`tmp_dir'/wasting_prepped_`meid'_all.dta", replace
			restore
			drop if modelable_entity_id == `meid'
		}


	*qsub array job
		local jobname sth_OLDwast
		! qsub -P proj_tb -pe multi_slot 5 -t 1:`num_tasks' -N `jobname' "FILEPATH" "`code_dir'/wasting_gbd2017.do" "`gbdages' `gbdsexes' `gbdyears'"


/*====================================================================
                        6: Save Results
====================================================================*/

/*
*SAVE RESULTS

run "FILEPATH.ado"


*1515 - Ascariasis
local meid 1515
local draws_dir "FILEPATH"
local description ""
local filepattern "{location_id}_{year_id}.csv"
save_results_epi, modelable_entity_id(`meid') description("`description'") input_dir("`draws_dir'") db_env(prod) input_file_pattern("`filepattern'") measure_id(5) clear


*1518 - Trichuriasis
local meid 1518
local draws_dir "FILEPATH"
local description ""
local filepattern "{location_id}_{year_id}.csv"
save_results_epi, modelable_entity_id(`meid') description("`description'") input_dir("`draws_dir'") db_env(prod) input_file_pattern("`filepattern'") measure_id(5) mark_best(True) clear


*1521 - Hookworm
local meid 1521
local draws_dir "FILEPATH"
local description ""
local filepattern "{location_id}_{year_id}.csv"
save_results_epi, modelable_entity_id(`meid') description("`description'") input_dir("`draws_dir'") db_env(prod) input_file_pattern("`filepattern'") measure_id(5) mark_best(True) clear


*/




log close
exit
/* End of do-file */

><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><

Notes:
1.
2.
3.


Version Control:
