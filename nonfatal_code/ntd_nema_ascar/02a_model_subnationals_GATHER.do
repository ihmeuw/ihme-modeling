/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:   GBD2016 GATHER     
Output:            Model subnationals and submit script to extrapolate to new GBD years
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
	local step 02a
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
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories

	*make all directories
	local make_dirs code in tmp local_tmp out log progress_dir
	foreach dir in `make_dirs' {
		capture mkdir ``dir'_dir'
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
	log using "`log_dir'/02a_model_subnationals_log_`date'_`time'.smcl", replace
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
		local gbdages = "`r(age_group_ids)'"
		local gbdyears = "`r(year_ids)'"
		local gbdsexes = "`r(sex_ids)'"

*--------------------1.2: Location Metadata

	get_location_metadata,location_set_id(35) clear
		levelsof location_id, local(gbdlocs) clean
		save "`local_tmp_dir'/`step'_metadata.dta", replace

*--------------------1.2: Population
	
	get_population, location_id("`gbdlocs'") sex_id("`gbdsexes'") age_group_id("`gbdages'") year_id("`gbdyears'") clear
		save "`local_tmp_dir'/`step'_population.dta", replace

/*====================================================================
                        2: Build Database for Regression
====================================================================*/


*--------------------2.1: Get Subnational Information
	
	*Get info on locations with subnationals
		`testing' use "`local_tmp_dir'/`step'_metadata.dta", clear

	*Get just the most detailed locations
		keep if most_detailed == 1
		keep if level>3
		keep location_id parent_id path_to_top_parent level location_name ihme_loc_id
		gen subnat = 1

		tempfile locs
		save `locs'

	*Get location ids for the parent locations
		split path_to_top_parent, p(,) destring
		levelsof path_to_top_parent4, local(parent_locids_list) sep(,) clean
	
	
*--------------------2.2: Get Empty Rows for Subnationals to Be Filled


	*Make empty draws skeleton
		use "`in_dir'/01a_preprocessed_EGdraws.dta", clear
		keep year_id sex_id age_group_id helminth_type intensity	
		duplicates drop
	
	*Cross with subnational locations table to get full table with all helminth-intensity-yr-age-sex values for each subnational location
		cross using `locs'

	*Append back to dataset without these subnationals
		append using "`in_dir'/01a_preprocessed_EGdraws.dta"
		levelsof location_id,local(sthlocs) c

	*Save (testing mode)
		`testing' save "`local_tmp_dir'/`step'_subnationaltemplate.dta", replace


*--------------------2.3: Add Covariates

	*Open (testing mode)
		`testing' use "`local_tmp_dir'/`step'_subnationaltemplate.dta", clear

	*Get covariates needed for regression and merge with data
		tempfile unfilleddata
		save `unfilleddata', replace
		foreach c in 881 1099 45 57 33 1087 142 160 3 130 118 119{
			
			di in red "Getting covariate: `c'"
			quietly{
				*Get covariate estimate from database
					get_covariate_estimates,covariate_id(`c') location_id(-1) sex_id(-1) age_group_id(-1) year_id(`gbdyears') clear
					levelsof covariate_name_short,local(covname) clean
					rename mean_value c_`covname'
					keep location_id age_group_id sex_id year_id c_`covname'
					tempfile cov_`c'
					save `cov_`c'', replace
				
				*Determine if covariate is all-age or all-sex					
					quietly sum age_group_id
					local allage `r(max)' 
					quietly sum sex_id
					local allsex `r(max)' 
				
				*Merge covariates with data
					if `allage'==22 & `allsex'==3{
						drop age_group_id sex_id
						merge 1:m location_id year_id using `unfilleddata', nogen keep(matched using)
					}
					
					else if `allage'==22 & `allsex'!=3{
						drop age_group_id
						merge 1:m location_id year_id sex_id using `unfilleddata', nogen keep(matched using)
					}
					
					else if `allage'!=22 & `allsex'==3{
						drop sex_id
						merge 1:m location_id year_id age_group_id using `unfilleddata', nogen keep(matched using)
					}
					
					else{
						merge 1:m location_id year_id sex_id age_group_id using `unfilleddata', nogen keep(matched using)
					}
			
			*Save
				save `unfilleddata', replace

			}
			di in red "Covariate Merge = SUCCESS [allage = `allage'; allsex = `allsex']"	
		}

		save "`local_tmp_dir'/`step'_unfilleddata.dta", replace
		
	*Calculate draw mean
		egen drawmean = rowmean(draw_*)
		drop draw_*
		
	*Merge with regional & super-regional data for reg/SR terms
		merge m:1 location_id using "`local_tmp_dir'/`step'_metadata.dta", keep(matched) nogen
		
	*Save (testing mode)
		`testing' save "`local_tmp_dir'/`step'_modelsubnat_dataset.dta", replace
		

/*====================================================================
                        3: Model Subnational Prevalence
====================================================================*/

	*Open (testing mode)
		`testing' use "`local_tmp_dir'/`step'_modelsubnat_dataset.dta", clear

*--------------------3.1: Run Seperate GLM Model for Each Worm
	
								
	*Code categorical variable(s)
		encode intensity, generate(intensity_code)

	*Create prediction variables to fill
		gen predmean = .
		gen predse = .

	*Run regression on mean of the draws with covariates - run seperate regressions for each worm/intensity level (reference below)
	
		*Model ASCARIASIS
			glm drawmean i.region_id i.age_group_id i.sex_id i.intensity_code c_LDI_pc c_rainfall_quint_4_5_prop c_haqi if helminth_type == "ascar", family(binomial) link(logit) robust
							
			*Predict the mean and standard error out
				predict tmp_predmean_ascar
				predict tmp_predse_ascar, stdp

				replace predmean = tmp_predmean_ascar if helminth_type == "ascar"
				replace predse   = tmp_predse_ascar   if helminth_type == "ascar"
							
		*Model TRICHURIASIS
			glm drawmean i.region_id i.age_group_id i.sex_id i.intensity_code c_rainfall_quint_4_5_prop c_abs_latitude c_haqi c_LDI_pc if helminth_type == "trich", family(binomial) link(logit) robust
											
			*Predict the mean and standard error out
				predict tmp_predmean_trichur
				predict tmp_predse_trichur, stdp
				
				replace predmean = tmp_predmean_trichur if helminth_type == "trich"
				replace predse   = tmp_predse_trichur   if helminth_type == "trich"
										
		*Model HOOKWORM
			glm drawmean i.region_id i.age_group_id i.sex_id i.intensity_code c_haqi c_rainfall_quint_4_5_prop c_abs_latitude c_sanitation_prop c_prop_pop_agg if helminth_type == "hook", family(binomial) link(logit) robust
			
			*Predict the mean and standard error out
				predict tmp_predmean_hook
				predict tmp_predse_hook, stdp
				
				replace predmean = tmp_predmean_hook if helminth_type == "hook"
				replace predse   = tmp_predse_hook   if helminth_type == "hook"
		
		
	*Keep relevant variables and save
		keep helminth_type intensity location_id subnat path_to_top_parent level year_id age_group_id sex_id predmean predse 

	*Save (testing mode)
		`testing' save "`local_tmp_dir'/`step'_modelsubnationals.dta", replace

/*====================================================================
                        4: Process Predictions
*====================================================================*/

	*Open (testing mode)
		`testing' use "`local_tmp_dir'/`step'_modelsubnationals.dta", clear

*--------------------4.1: Use the predicted mean and standard error to fill in subnational prevalence for each draw 
		
	*Generate 1000 draws of the predicted mean/se and log each one - use log space to avoid negative values
		forval d = 0/999 {
			quietly gen draw_`d' = ln(rnormal(predmean,predse)) if subnat == 1
		}
	
	*Get the mean,se of each distribution(row) in log space
		egen logdrawmean = rowmean(draw_*) if subnat == 1
		egen logdrawse = rowsd(draw_*) if subnat == 1
		
	*Replace each draw with the exponentiated value draw from the log-space distribution
		forval d = 0/999 {
			quietly replace draw_`d' = exp(rnormal(logdrawmean,logdrawse)) if subnat == 1
		}
		
	*Save (testing mode)
		`testing' save "`local_tmp_dir'/`step'_rawsubnatpredictions.dta", replace


*--------------------4.3: Rake Subnational Estimates to National Estimates		
*Prep Raking
**Foreach draw, get the national total # cases (national total to rake to) & total of subnationals cases (to rake from)
**Then rake sum of subnational values to the national value for # cases for each draw individually

	*Open (testing mode)
		`testing' use "`local_tmp_dir'/`step'_rawsubnatpredictions.dta", clear

	*Merge with population
		merge m:1 location_id age_group_id year_id sex_id using "`local_tmp_dir'/`step'_population.dta", keep(matched) nogen

	*Get state id variable
		split path_to_top_parent,p(,) destring
		rename path_to_top_parent4 country_code

	*Limit dataset to just places with subnationals
		keep if inlist(location_id,`parent_locids_list') | subnat == 1
		
	*Loop through draws rake subnational draws to national total
		di in red "Beginning to Rake Subnational Estimates"
		
		sort helminth_type intensity year_id age_group_id sex_id country_code subnat level
		
		forval d = 0/999 {
		
			di in red "`d' ." _continue
			quietly {
				*Convert draw to cases
					replace  draw_`d' = draw_`d' * population
											
				*Define population to rake to, create temp variable with value of total for that level
					capture drop rake*
					gen raketemp = draw_`d' if level == 3

				*Spread that total to all rows that have same value of year and parent
					by helminth_type intensity year_id age_group_id sex_id country_code: egen raketo_`d' = total(raketemp)

				*Create variable equal to total number of cases for subnat
					by helminth_type intensity year_id age_group_id sex_id country_code subnat level: egen rakefrom_`d' = total(draw_`d')

				*Calculate raking factor
					gen rakefactor_`d'= raketo_`d' / rakefrom_`d' if level > 3 & subnat == 1

				*Adjust # predicted cases
					gen rakedcases_`d'= draw_`d' * rakefactor_`d' if rakefactor_`d' != .

				*Replace cases in draws for subnationals with new adjusted cases
					replace draw_`d'= rakedcases_`d' if level > 3 & subnat == 1
					
				*Convert back to case space
					replace draw_`d'= draw_`d' / population	
			
				*Drop all temp raking variables & cases variable that is now replaced with prevalence
					drop rake*
			
			}
		}
		
		save "`local_tmp_dir'/`step'_subnationalsraked.dta", replace

	*Append to estimates for locations that didn not require raking (no subnationals)
		use "`local_tmp_dir'/`step'_rawsubnatpredictions.dta", clear
		drop if inlist(location_id,`parent_locids_list') | subnat == 1
		append using "`local_tmp_dir'/`step'_subnationalsraked.dta"

	*Save (testing mode)
		`testng' save "`local_tmp_dir'/`step'_raked1.dta", replace


*--------------------4.3: Scale India Estimates Using India Prevalence Mapping Survey

	*Open (testing mode)
		`testing' use "`local_tmp_dir'/`step'_raked1.dta", clear

	*Limit dataset to just India
		keep if country_code == 163

	*Rake India Estimates
		merge m:1 location_id helminth_type using "`in_dir'/01c_indiaprevalencemappingsurvey.dta", keep(matched) nogen
		split location_name, p(", ")
		rename location_name2 urbanicity_type
		capture rename STATE_surveytotal surveytotal_STATE
		capture rename NATIONAL_surveytotal surveytotal_NATIONAL
		capture rename STATE_surveyprop surveyprop_STATE
				
		forval r = 0/999 {
			
			di in red "Raking India Draw `r'"
			quietly{
				*Convert estimated prevalence draws to cases
					gen cases_`r' = draw_`r' * population
					drop draw_`r'
						
				*Sum the total estimated cases by state
					bysort state_name year_id helminth_type intensity :egen estimatetotal_STATE = total(cases_`r')
				
				*Calculate the proportion of cases in each state by urban/rural
					bysort location_name year_id helminth_type intensity: egen estimatetotal_URBANICITY = total(cases_`r')
					gen estimateprop_URBAN = estimatetotal_URBANICITY / estimatetotal_STATE if urbanicity_type=="Urban"
					gen estimateprop_RURAL = estimatetotal_URBANICITY / estimatetotal_STATE if urbanicity_type=="Rural"
				
				*Sum total accross india
					bysort year_id helminth_type intensity: egen estimatetotal_NATIONAL = total(cases_`r')
				
				*Break out cases proportionally in same proportions as the 2016 survey
					replace cases_`r' = estimatetotal_NATIONAL * surveytotal_STATE * estimateprop_URBAN if urbanicity_type=="Urban"
					replace cases_`r' = estimatetotal_NATIONAL * surveytotal_STATE * estimateprop_RURAL if urbanicity_type=="Rural"
							
				*Convert back to draws
					gen draw_`r' = cases_`r' / population
					drop estimate* cases_`r'	
			}
		}
		
		save "`local_tmp_dir'/`step'_indiascaled.dta", replace

	*Append back with all other location estimates
		use "`local_tmp_dir'/`step'_raked1.dta", clear
		drop if country_code == 163
		append using "`local_tmp_dir'/`step'_indiascaled.dta"

		save "`local_tmp_dir'/`step'_allraked.dta"


*--------------------2.7: Sudan

		keep if location_id==522
		replace location_id=435
		append using "`local_tmp_dir'/`step'_allraked.dta"

*--------------------2.8: Append All Subnational Estimates and Save

	*Save
		save "`in_dir'/`step'_draws_with_subnationals.dta", replace
		





log close
exit
/* End of do-file */

