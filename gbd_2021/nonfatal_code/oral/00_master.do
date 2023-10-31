// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: 	NAME
// Date: 		08 August 2014
// Update: by NAME for GBD 2015, on 2 February 2016
// Purpose:	Perform post-DisMod modifications to oral conditions
// do "FILEPATH"

/*
Oral model tags :
oral_edent			2337	Edentulism and severe tooth loss
oral_edent			2584	Difficulty eating due to edentulism and severe tooth loss
oral_perio			2336	Chronic periodontal diseases
oral_permcaries		2335	Permanent caries
oral_permcaries		2583	Tooth pain due to permanent caries
oral_decidcaries	2334	Deciduous caries
oral_decidcaries	2582	Tooth pain due to deciduous caries

STEPS BY CAUSE:
	oral_edent
		1) split parent [2337] by 0.444(0.438 � 0.451) to get difficulty eating [2584]
	oral_perio
		2) multiply model by *(1-prev_2337) at the CYAS level
	oral_permcaries
		3) multiply parent model by *(1-prev_2337) at the CYAS level
		4) split into tooth pain...
			- Data-rich = 0.0595 (0.0506 - 0.0685)
			- Data-poor = 0.0997 (0.0847 - 0.1146)
	oral_decidcaries
		5 ) split into tooth pain...
			- Data-rich = 0.0233 (0.0198 - 0.0268)
			- Data-poor = 0.0380 (0.0323 - 0.0437)
*/

// PREP STATA
	clear
	set more off
	pause on
	set maxvar 3200
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		set odbcmgr unixodbc
		set maxvar 32767
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
		set maxvar 32767
	}

	// make connection string
	/*run "FILEPATH"
	create_connection_string, database(epi) server(modeling-epi-db)

	run "FILEPATH"


// ****************************************************************************
// Manually defined macros

	get_location_metadata, location_set_id(35) gbd_round_id(7) decomp_step('iterative') clear
	keep if most_detailed == 1
	levelsof location_id, local(locations)
	** User
	//local username USERNAME

	** Steps to run (1 2 3 4 5 from above)
	** If you run steps 2 and 3, you MUST also run the severity split of 4 and 5. Run one at a time.
	local run_steps 1 2 3 4 5

	** Modelable_entity_ids to upload (2335 2336 2582 2583 2584 3091 3092 3093)
	local me_uploads 2335 2336 2582 2583 2584 3091 3092 3093
	**local me_uploads 2335

	** Sweep directory
	local sweep 0

	** Where is your local repo for this code?
	local prog_dir "FILEPATH"
// ****************************************************************************
// Directory macros
	local tmp_dir "FILEPATH"
	capture mkdir "`tmp_dir'"

// ****************************************************************************
// Load current best models
	local edent_id 2337
	local perio_id 2336
	local permcaries_id 2335
	local decidcaries_id 2334




// ****************************************************************************
foreach run_step of local run_steps {
// 1) oral_edent --> split parent [2337] by 0.444(0.438 � 0.451) to get difficulty eating [2584]
	if `run_step' == 1 {
		** Set up results directory
		local child_id 2584
		local asymp_id 3093
		capture mkdir "`tmp_dir'/`child_id'"
		capture mkdir "`tmp_dir'/`child_id'/00_logs"
		capture mkdir "`tmp_dir'/`child_id'/01_draws"
		capture mkdir "`tmp_dir'/`asymp_id'"
		capture mkdir "`tmp_dir'/`asymp_id'/01_draws"
		** Bootstrap
		clear
		local M = 0.444
		local L = 0.438
		local U = 0.451
		local SE = (`U'-`L')/(2*1.96)
		drawnorm prop_, n(1000) means(`M') sds(`SE')
		gen num = _n
		replace num = num-1
		gen metric = "prevalence_incidence"
		reshape wide prop_, i(metric) j(num)
		saveold "`tmp_dir'/`child_id'/tooth_loss_split.dta", replace
		foreach loc of local locations {
			!qsub -P "proj_nch" -l m_mem_free=4G -l fthread=1 -l h_rt=06:00:00 -q long.q -N "tooth_loss_split_`loc'" -o "FILEPATH" -e "FILEPATH" "FILEPATH" "FILEPATH" "`tmp_dir' `edent_id' `child_id' `asymp_id' `loc'"
		}
	}

// 2) oral_perio --> multiply model by *(1-prev_2337) at the CYAS level
	if `run_step' == 2 {
		** Set up results directory
		local perio_id 2336
		capture mkdir "`tmp_dir'/`perio_id'"
		capture mkdir "`tmp_dir'/`perio_id'/00_logs"
		capture mkdir "`tmp_dir'/`perio_id'/01_draws"
		foreach loc of local locations {
			!qsub -P "proj_nch" -l m_mem_free=4G -l fthread=1 -l h_rt=06:00:00 -q long.q -N "perio_split_`loc'" -o "FILEPATH" -e "FILEPATH" "FILEPATH" "FILEPATH" "`tmp_dir' `edent_id' `perio_id' `loc'"
		}
	}

// 3) oral_permcaries --> multiply parent model by *(1-prev_2337) at the CYAS level
	if `run_step' == 3 {
		** Set up results directory
		local permcaries_id 2335
		capture mkdir "`tmp_dir'/`permcaries_id'"
		capture mkdir "`tmp_dir'/`permcaries_id'/00_logs"
		capture mkdir "`tmp_dir'/`permcaries_id'/01_draws"
		foreach loc of local locations {
			!qsub -P "ADDRESS" -l m_mem_free=4G -l fthread=1 -l h_rt=06:00:00 -q long.q -N "permcaries_split_`loc'" -o "FILEPATH" -e "FILEPATH" "FILEPATH" "FILEPATH" "`tmp_dir' `edent_id' `permcaries_id' `loc'"
		}
	}

// 4) oral_permcaries --> split parent into tooth pain [Data Rich = 0.0595 (0.0506 - 0.0685) // Data Poor = 0.0997 (0.0847 - 0.1146)]
	if `run_step' == 4 {
		** Set up results directory
		local child_id 2583
		local asymp_id 3092
		capture mkdir "`tmp_dir'/`child_id'"
		capture mkdir "`tmp_dir'/`child_id'/00_logs"
		capture mkdir "`tmp_dir'/`child_id'/01_draws"
		capture mkdir "`tmp_dir'/`asymp_id'"
		capture mkdir "`tmp_dir'/`asymp_id'/01_draws"
		** Bootstrap - DATA-RICH
		clear
		local M = 0.0595
		local L = 0.0506
		local U = 0.0685
		local SE = (`U'-`L')/(2*1.96)
		drawnorm prop_, n(1000) means(`M') sds(`SE')
		gen num = _n
		replace num = num-1
		gen metric = "prevalence_incidence"
		reshape wide prop_, i(metric) j(num)
		saveold "`tmp_dir'/`child_id'/tooth_pain_split_D1.dta", replace
		** Bootstrap - DATA-POOR
		clear
		local M = 0.0997
		local L = 0.0847
		local U = 0.1146
		local SE = (`U'-`L')/(2*1.96)
		drawnorm prop_, n(1000) means(`M') sds(`SE')
		gen num = _n
		replace num = num-1
		gen metric = "prevalence_incidence"
		reshape wide prop_, i(metric) j(num)
		saveold "`tmp_dir'/`child_id'/tooth_pain_split_D0.dta", replace
				get_location_metadata, location_set_id(43) gbd_round_id(6) clear 
			//use "FILEPATH", clear
		gen dev = "D0" if parent_id == 44640
		replace dev = "D1" if parent_id == 44641
		foreach loc of local locations {
			**levelsof dev if location_id == `loc', local(dev_stat) c
			local dev_stat "D0"
			!qsub -P "ADDRESS" -l m_mem_free=4G -l fthread=1 -l h_rt=04:00:00 -q long.q -N "perm_tooth_pain_`loc'" -hold_jid "permcaries_split_`loc'" -o "FILEPATH" -e "FILEPATH" "FILEPATH" "FILEPATH" "`tmp_dir' `permcaries_id' `child_id' `asymp_id' `loc' `dev_stat'"
		}
	}

// 5) oral_decidcaries --> split parent into tooth pain [Data Rich = 0.0233 (0.0198 - 0.0268) // Data Poor = 0.0380 (0.0323 - 0.0437)]
	if `run_step' == 5 {
		** Set up results directory
		local child_id 2582
		local asymp_id 3091
		capture mkdir "`tmp_dir'/`child_id'"
		capture mkdir "`tmp_dir'/`child_id'/00_logs"
		capture mkdir "`tmp_dir'/`child_id'/01_draws"
		capture mkdir "`tmp_dir'/`asymp_id'"
		capture mkdir "`tmp_dir'/`asymp_id'/01_draws"
		** Bootstrap - DEVELOPED
		clear
		local M = 0.0233
		local L = 0.0198
		local U = 0.0268
		local SE = (`U'-`L')/(2*1.96)
		drawnorm prop_, n(1000) means(`M') sds(`SE')
		gen num = _n
		replace num = num-1
		gen metric = "prevalence_incidence"
		reshape wide prop_, i(metric) j(num)
		saveold "`tmp_dir'/`child_id'/tooth_pain_split_D1.dta", replace
		** Bootstrap - DEVELOPING
		clear
		local M = 0.0380
		local L = 0.0323
		local U = 0.0437
		local SE = (`U'-`L')/(2*1.96)
		drawnorm prop_, n(1000) means(`M') sds(`SE')
		gen num = _n
		replace num = num-1
		gen metric = "prevalence_incidence"
		reshape wide prop_, i(metric) j(num)
		saveold "`tmp_dir'/`child_id'/tooth_pain_split_D0.dta", replace
			get_location_metadata, location_set_id(43) gbd_round_id(6) clear
		gen dev = "D0" if parent_id == 44640
		replace dev = "D1" if parent_id == 44641
		foreach loc of local locations {
			**levelsof dev if location_id == `loc', local(dev_stat) c
			local dev_stat "D0"
			!qsub -P "ADDRESS" -l m_mem_free=4G -l fthread=1 -l h_rt=04:00:00 -q long.q -N "decid_tooth_pain_`loc'" -o "FILEPATH" -e "FILEPATH" "FILEPATH" "FILEPATH" "`tmp_dir' `decidcaries_id' `child_id' `asymp_id' `loc' `dev_stat'"
		}
	}

}
