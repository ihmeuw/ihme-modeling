/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:   GBD2016 GATHER  
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
	local gbd = "gbd2016"
	*model step
	local step 03
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draws
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories

	*make all directories
	local make_dirs code in tmp local_tmp out log progress_dir
	foreach dir in `make_dirs' {
		capture shell mkdir ``dir'_dir'
	}
	*clear tempfiles, logs and progress files
	foreach dir in log progress tmp {
		capture cd ``dir'_dir'
		capture shell rm *
	}

*Directory for standard code files
	adopath + FILEPATH


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/03_submit_wasting_`date'_`time'.smcl", replace
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

	get_demographics, gbd_team("epi") clear
		local gbdages="`r(age_group_ids)'"
		local gbdyears="`r(year_ids)'"
		local gbdsexes="`r(sex_ids)'"

*--------------------1.2: Location Metadata

	get_location_metadata, location_set_id(35) clear
		keep if most_detailed==1
		levelsof location_id,local(gbdlocs) clean
		keep location_id ihme_loc_id parent_id path_to_top_parent level location_name
		save "`local_tmp_dir'/`step'_metadataestimates.dta", replace


*--------------------1.3: GBD Skeleton

	get_population, location_id("`gbdlocs'") sex_id("`gbdsexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
		save "`local_tmp_dir'/`step'_skeleton.dta", replace
			

/*====================================================================
                        2: Prepare STH Heavy Infestation Draws
====================================================================*/


*--------------------2.1: Load Heavy Infestation Draws for Each Worm

	*Loop through all the heavy infestation meids and get draws
		local heavyinfmeids 1513 1516 1519
	
		tempfile heavy_dir_temp
		local stack 1
		
		foreach cause in `heavyinfmeids' {
			
			di in red "Getting heavy infestation estimates for: `cause'"
			quietly{
				get_best_model_versions, entity(modelable_entity) ids(`cause') clear
					local `cause'_mvid = model_version_id
				
				get_draws, gbd_id_field(modelable_entity_id) gbd_id(`cause') measure_ids(5) source(epi) model_version_id(``cause'_mvid') location_ids(`gbdlocs') age_group_ids(2 3 4 5) clear
				
				gen meid = `cause'
				
				if `stack'>1 append using `heavy_dir_temp'
				save `heavy_dir_temp', replace
				local ++ stack 
			}

		}

	*Save
		`testing' save "`local_tmp_dir'/`step'_all_heavyinf.dta", replace

*--------------------2.2: Calculate proportion of total STH prevalence of heavy worm infestation attributable to each worm

	*Sum prevalences of all worms by Location-Year-Age-Sex
		quietly fastcollapse draw*, by(age_group_id sex_id year_id location_id) type(sum)

		forvalues i = 0/999 {
			quietly replace draw_`i' = 1 if draw_`i' > 1
		}

		rename draw* sumsth*

		 `testing' save "`local_tmp_dir'/`step'_heavyinf_sum.dta", replace

	*Calculate proportions for later split up of total wasting due to heavy infestation
		merge 1:m age_group_id sex_id year_id location_id using "`local_tmp_dir'/`step'_all_heavyinf.dta", nogen
			
		forvalues i = 0/999 {
			replace draw_`i' = draw_`i' / sumsth_`i'
			replace draw_`i' = 0 if sumsth_`i' == 0
		}

		rename draw_* prop_*
		
		save "`local_tmp_dir'/`step'_propSTHprevperworm.dta", replace



/*====================================================================
                        3: Prepare Wasting Envelope Draws
====================================================================*/


*--------------------3.1: Load Wasting Envelope Draws

	*Loop through all the heavy infestation meids and get draws
		local wastingmeids 1607 1608

		foreach meid in `wastingmeids'{
			get_best_model_versions, entity(modelable_entity) ids(`meid') clear
					local `meid'_mvid = model_version_id
				
			get_draws, gbd_id_field(modelable_entity_id) gbd_id(`meid') measure_ids(5) source(epi) model_version_id(``meid'_mvid') location_ids(`gbdlocs') age_group_ids(2 3 4 5) clear
			tempfile `meid'_prev
			save ``meid'_prev', replace
		}
		
	*Append draws for wasting
		use `1607_prev', clear
		append using `1608_prev'
		`testing' save "`local_tmp_dir'/`step'_wastingdraws.dta", replace
		

*--------------------3.2: Sum Prevalence of Wasting Across Types (1607 + 1608)

	*Merge with population and calculate cases
		merge m:1 location_id year_id age_group_id sex_id using "`local_tmp_dir'/`step'_skeleton.dta", nogen keep(matched)
		
		local y 1
		forval x=0/999{
			if `y' == 1 di in red "Converting to case space to sum across wasting types"
			display in red ". `y' " _continue
			local ++y
			quietly replace draw_`x' = draw_`x'*population
		}

	*Collapse to sum across 2 different wasting meids to get total # wasting cases
		fastcollapse draw* ,type(sum) by(location_id year_id age_group_id sex_id)
		
	*Merge with population again to get back to rate space
		merge 1:1 location_id year_id age_group_id sex_id using "`local_tmp_dir'/`step'_skeleton.dta", nogen keep(matched)
	
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
		save "`local_tmp_dir'/`step'_wast_draws.dta", replace


/*====================================================================
                        4: Assemble Full Dataset 
====================================================================*/

*--------------------4.2: Load Geographic Restrictions

	*Load Geographic Restrictions for 3 Worms
		local g 1		
		tempfile allGR

		foreach worm in ascariasis trichuriasis hookworm {
			quietly import delimited `j'/Project/NTDS/geographic_restrictions/central_comp/`worm'_cc.csv, clear 
			gen restrict = 1

			if "`worm'" == "ascariasis" {
				gen modelable_entity_id = 1513
			}
			if "`worm'" == "trichuriasis" {
				gen modelable_entity_id = 1516
			}
			if "`worm'" == "hookworm" {
				gen modelable_entity_id = 1519
			}

			if `g'>1 append using `allGR'
			save `allGR', replace
			local ++g

		}

		`testing' save "`local_tmp_dir'/`step'_allGR.dta", replace

*--------------------4.2: Merge with wasting draws, sth draw sum, worm proportions

	*Variables: sum of sth prevalence; proportion of heavy inf attributable to each worm
		merge 1:m location_id year_id modelable_entity_id using "`local_tmp_dir'/`step'_propSTHprevperworm.dta", keep(matched using) nogen

	*Merge with wasting draws
		merge m:1 location_id year_id age_group_id sex_id using "`local_tmp_dir'/`step'_wast_draws.dta", nogen

	*Make locyear variable
		quietly egen locyear = concat(location_id year_id), p("_")
		levelsof locyear, local(locyears) c
		
	*Save
		tempfile dataset
		save `dataset', replace
	

/*====================================================================
                        4: Submit Wasting Estimation Script to Qsub 
====================================================================*/


*--------------------4.1: Create Zeroes Files

	*Create Zeroes File for Geographically Restricted Location-Years
		use "`local_tmp_dir'/`step'_skeleton.dta", clear

		preserve
			forval d=0/999{
				quietly gen draw_`d' = 0
			}	
			save "`local_tmp_dir'/`step'_gr_zeroes.dta", replace
		restore

	*Create Zeroes File for Ages 5+
		drop year_id location_id population process_*
		duplicates drop
		drop if inlist(age_group_id,2,3,4,5)
			
		save "`local_tmp_dir'/`step'_age5plus_zeroes.dta", replace
	

*--------------------4.2: Loop Over Locations to Submit Wasting Estimation Script to Qsub
	

	*Loop through worms to assign geographic restrictions
		use `dataset', clear

		foreach meid in 1513 1516 1519{
			use `dataset', clear
			keep if modelable_entity_id == `meid'

			foreach locyear in `locyears' {
			
				preserve
				
				*Determing if location-year is geographically restricted
					keep if locyear == "`locyear'"
					local restrict = restrict

				*If locyear is restricted
					if `restrict' == 1 {
						use "`local_tmp_dir'/`step'_gr_zeroes.dta", clear
						gen locyear = "`locyear'"
						save "`tmp_dir'/wasting_zeroes_`meid'_`locyear'.dta", replace
					}

				*If locyear is not restricted
					else if `restrict' != 1 {
						append using "`local_tmp_dir'/`step'_age5plus_zeroes.dta"
						save "`tmp_dir'/wasting_prepped_`meid'_`locyear'.dta", replace
					}

				*Drop completed locyear to speed up loop
					restore
					drop if locyear == `locyear'
		

				*QSUB
					! qsub -P proj_custom_models -pe multi_slot 8 -N wasting_`locyear' "`localRoot'/submit_wasting.sh" "`locyear' `meid' `restrict'" 		
			}
		}




/*====================================================================
                        5: Save Results
====================================================================*/

/*
*SAVE RESULTS

run "FILEPATH/save_results.do"

	*1515 - Ascariasis
	save_results, modelable_entity_id(1515) description("Wasting due to ascariasis; ascar heavy model `1513_mvid' ; wasting models `1607_mvid' & `1608_mvid' ") in_dir("`out_dir'/1515_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({location_id}_{year_id}.csv)
  
	*1518 - Trichuriasis
	save_results, modelable_entity_id(1518) description("Wasting due to trichuriasis; trichur heavy model `1516_mvid' ; wasting models `1607_mvid' & `1608_mvid' ") in_dir("`out_dir'/1518_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({location_id}_{year_id}.csv)
 
	*1821 - Hookworm
	save_results, modelable_entity_id(1521) description("Wasting due to PEM based on updated worm and wasting estimates; hook heavy model `1519_mvid' ; wasting models `1607_mvid' & `1608_mvid' ") in_dir("`out_dir'/1521_prev") metrics(prevalence) mark_best(yes) env(prod) file_pattern({location_id}_{year_id}.csv)


*/




log close
exit
/* End of do-file */
