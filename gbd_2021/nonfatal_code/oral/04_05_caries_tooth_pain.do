// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 	NAME
// Date: 		04 August 2014
// Purpose:	Split deciduous and permanent caries to make tooth pain child
// do "FILEPATH"

// PREP STATA
	clear
	set more off
	set maxvar 3200
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		set odbcmgr unixodbc
		set mem 2g
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
		set mem 2g
	}

// Temp directory
	local tmp_dir "`1'"

// Parent ME_id
	local parent_id `2'

// ME_id of symptomatic results
	local child_id `3'

// ME_id of asymptomatic results
	local asymp_id `4'

// location_id
	local loc `5'

// data-rich or data-poor?
	local dev_stat "`6'"


// ****************************************************************************
// Log work
	capture log close
	log using "`tmp_dir'/`child_id'/00_logs/`loc'_draws.smcl", replace

	// Load in necessary function
run "FILEPATH"

// Get symptomatic and asymptomatic prevalence and incidence, which is different based on dev stat (data-rich or data-poor) of the particular location passed in
	foreach year in 1990 1995 2000 2005 2010 2015 2019 2020 2021 2022{
	**foreach year in 2015 2019 {
		foreach sex in 1 2 {
			foreach met in 5 6 {
				// pull in draws of the parent model
				get_draws, gbd_id_type("modelable_entity_id") gbd_id(`parent_id') source("epi") measure_id(`met') location_id(`loc') year_id(`year') sex_id(`sex') age_group_id(2 3 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 34 235 238 388 389) status(best) gbd_round_id(7) decomp_step("iterative") clear
								drop modelable_entity_id

				gen metric = "prevalence_incidence"
				// merge on the symptomatic proportion for the particular dev stat, that was created in 00_master
				merge m:1 metric using "`tmp_dir'/`child_id'/tooth_pain_split_`dev_stat'.dta", assert(3) nogen
				// calculate symptomatic by multiplying by the symptomatic proportion
				forval y = 0/999 {
					gen symp_`y' = draw_`y'*prop_`y'
				}
				drop prop* metric
				// calculate asymptomatic by subtracting the symptomatic from the total
				preserve
					forval y = 0/999 {
						replace draw_`y' = draw_`y' - symp_`y'
					}
					drop symp*
					outsheet using "`tmp_dir'/`asymp_id'/01_draws/`met'_`loc'_`year'_`sex'.csv", comma names replace
				restore
				drop draw*
				renpfix symp draw
				outsheet using "`tmp_dir'/`child_id'/01_draws/`met'_`loc'_`year'_`sex'.csv", comma names replace
			}
		}
	}


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
