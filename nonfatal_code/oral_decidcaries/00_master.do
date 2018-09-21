// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

/*
STEPS BY CAUSE:
	oral_edent
		1) split parent by 0.444(0.438 – 0.451) to get difficulty eating
	oral_perio
		2) multiply model by *(1-prev parent) at the CYAS level
	oral_permcaries
		3) multiply parent model by *(1-prev parent) at the CYAS level
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
		global prefix "{PREFIX}"
		set odbcmgr unixodbc
		set maxvar 32767
	}
	else if c(os) == "Windows" {
		global prefix "{PREFIX}"
		set maxvar 32767
	}

	// make connection string
	run "{FILEPATH}/create_connection_string.ado"
	create_connection_string, database({DATABASE}) server({SERVER})
	local epi_str = r(conn_string)

// ****************************************************************************
// Manually defined macros
	** User
	local username {USERNAME}

	** Steps to run (1 2 3 4 5 from above)
	** If you run steps 2 and 3, you MUST also run the severity split of 4 and 5. Run one at a time.
	local run_steps 1 2 3 4 5

	** Modelable_entity_ids to upload (2335 2336 2582 2583 2584 3091 3092 3093)
	local me_uploads 2335 2336 2582 2583 2584 3091 3092 3093

	** Sweep directory
	local sweep 0

	** Where is your local repo for this code?
	local prog_dir "{FILEPATH}"
// ****************************************************************************
// Directory macros
	local tmp_dir "{FILEPATH}"
	capture mkdir "`tmp_dir'"

// ****************************************************************************
// Load current best models
	local edent_id {MODELABLE ENTITY ID}
	local perio_id {MODELABLE ENTITY ID}
	local permcaries_id {MODELABLE ENTITY ID}
	local decidcaries_id {MODELABLE ENTITY ID}
	odbc load, exec("SELECT model_version_id, modelable_entity_id, best_user FROM {DATABASE} WHERE is_best = {IS BEST} and modelable_entity_id IN(`edent_id', `perio_id', `permcaries_id', 'decidcaries_id')") `epi_str' clear
	count if (best_user != "{USERNAME}") & (best_user != "{USERNAME}") & (modelable_entity_id == `perio_id' | modelable_entity_id == `permcaries_id')
	if `r(N)' > 0 {
		quietly {
			noisily di "Are you running periodontal or the permanent caries parent (steps 2 or 3)?"
			noisily di "Split models are already marked as best."
			noisily di `"Type "q" to continue..."'
			pause
			sleep 1000
			noisily di "CONTINUING"
		}
	}


// Load countries
	odbc load, exec("SELECT location_id FROM {DATABASE} INNER JOIN {DATABASE} USING(location_id) WHERE location_set_version_id = (SELECT location_set_version_id FROM {DATABASE} lsv WHERE location_set_id = {LOCATION_SET_ID} AND gbd_round = {GBD ROUND} AND end_date IS NULL) AND most_detailed = {MOST DETAILED} ORDER BY sort_order") `epi_str' clear
	levelsof location_id, local(locations)

// ****************************************************************************
foreach run_step of local run_steps {
// 1) oral_edent --> split parent by 0.444(0.438 – 0.451) to get difficulty eating
	if `run_step' == 1 {
		** Set up results directory
		local child_id {MODELABLE ENTITY ID}
		local asymp_id {MODELABLE ENTITY ID}
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
			!qsub -P proj_custom_models -pe multi_slot 4 -l mem_free=8g -N "tooth_loss_split_`loc'" "`prog_dir'/stata_shell.sh" "`prog_dir'/01_edent_tooth_loss.do" "`tmp_dir' `edent_id' `child_id' `asymp_id' `loc'"
		}
	}

// 2) oral_perio --> multiply model by *(1-prev parent) at the CYAS level
	if `run_step' == 2 {
		** Set up results directory
		local perio_id {MODELABLE ENTITY ID}
		capture mkdir "`tmp_dir'/`perio_id'"
		capture mkdir "`tmp_dir'/`perio_id'/00_logs"
		capture mkdir "`tmp_dir'/`perio_id'/01_draws"
		foreach loc of local locations {
			!qsub -P proj_custom_models -pe multi_slot 4 -l mem_free=8g -N "perio_split_`loc'" "`prog_dir'/stata_shell.sh" "`prog_dir'/02_03_edent_split_perio_permcaries.do" "`tmp_dir' `edent_id' `perio_id' `loc'"
		}
	}

// 3) oral_permcaries --> multiply parent model by *(1-prev parent) at the CYAS level
	if `run_step' == 3 {
		** Set up results directory
		local permcaries_id {MODELABLE ENTITY ID}
		capture mkdir "`tmp_dir'/`permcaries_id'"
		capture mkdir "`tmp_dir'/`permcaries_id'/00_logs"
		capture mkdir "`tmp_dir'/`permcaries_id'/01_draws"
		foreach loc of local locations {
			!qsub -P proj_custom_models -pe multi_slot 4 -l mem_free=8g -N "permcaries_split_`loc'" "`prog_dir'/stata_shell.sh" "`prog_dir'/02_03_edent_split_perio_permcaries.do" "`tmp_dir' `edent_id' `permcaries_id' `loc'"
		}
	}

// 4) oral_permcaries --> split parent into tooth pain [Data Rich = 0.0595 (0.0506 - 0.0685) // Data Poor = 0.0997 (0.0847 - 0.1146)]
	if `run_step' == 4 {
		** Set up results directory
		local child_id {MODELABLE ENTITY ID}
		local asymp_id {MODELABLE ENTITY ID}
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
				odbc load, exec("SELECT location_id, parent_id FROM {DATABASE} WHERE location_set_version_id=(SELECT location_set_version_id FROM {DATABASE} WHERE location_set_id = {LOCATION SET ID} AND end_date IS NULL)") `epi_str' clear
		gen dev = "D0" if parent_id == {MODELABLE ENTITY ID}
		replace dev = "D1" if parent_id == {MODELABLE ENTITY ID}
		foreach loc of local locations {
			levelsof dev if location_id == `loc', local(dev_stat) c
			!qsub -P proj_custom_models -pe multi_slot 4 -l mem_free=8g -N "perm_tooth_pain_`loc'" -hold_jid "permcaries_split_`loc'" "`prog_dir'/stata_shell.sh" "`prog_dir'/04_05_caries_tooth_pain.do" "`tmp_dir' `permcaries_id' `child_id' `asymp_id' `loc' `dev_stat'"
		}
	}

// 5) oral_decidcaries --> split parent into tooth pain [Data Rich = 0.0233 (0.0198 - 0.0268) // Data Poor = 0.0380 (0.0323 - 0.0437)]
	if `run_step' == 5 {
		** Set up results directory
		local child_id {MODELABLE ENTITY ID}
		local asymp_id {MODELABLE ENTITY ID}
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
		odbc load, exec("SELECT location_id, parent_id FROM {DATABASE} WHERE location_set_version_id=(SELECT location_set_version_id FROM {DATABASE} WHERE location_set_id = {LOCATION SET ID} AND end_date IS NULL)") `epi_str' clear
		gen dev = "D0" if parent_id == {MODELABLE ENTITY ID}
		replace dev = "D1" if parent_id == {MODELABLE ENTITY ID}
		foreach loc of local locations {
			levelsof dev if location_id == `loc', local(dev_stat) c
			!qsub -P proj_custom_models -pe multi_slot 4 -l mem_free=8g -N "decid_tooth_pain_`loc'" "`prog_dir'/stata_shell.sh" "`prog_dir'/04_05_caries_tooth_pain.do" "`tmp_dir' `decidcaries_id' `child_id' `asymp_id' `loc' `dev_stat'"
		}
	}

}
// ****************************************************************************
local loc_len : word count `locations'
local year_len 6
local measure_len 2
local need_files = `loc_len' * `year_len' * `measure_len'

foreach me_upload of local me_uploads {
// Upload?
	if `me_upload' != . & `me_upload' != 0 {
		// check if files are ready to be uploaded
		local ready 0
		while `ready' == 0 {
			local list_dir: dir "`tmp_dir'/`me_upload'/01_draws" files "*2.csv"
			local exist_files : word count `list_dir'
			if (`exist_files' < `need_files') sleep 60000
			else local ready 1
			if `ready' == 1 di "Ready to upload `me_upload'!"
		}
		quietly {
			local me_upload_use_`me_upload' = `me_upload'
			local me_upload_use_{MODELABLE ENTITY ID} = {MODELABLE ENTITY ID}
			local me_upload_use_{MODELABLE ENTITY ID} = {MODELABLE ENTITY ID}
			local {MODELABLE ENTITY ID}_comment "non edentulous population applied"
			local {MODELABLE ENTITY ID}_comment "non edentulous population applied"
			local {MODELABLE ENTITY ID}_comment "data-rich proportion: 0.0233 (0.0198 to 0.0268); data-poor proportion: 0.0380 (0.0323 to 0.0437)"
			local {MODELABLE ENTITY ID}_comment "data-rich proportion: 0.0595 (0.0506 to 0.0685); data-poor proportion: 0.0997 (0.0847 to 0.1146)"
			local {MODELABLE ENTITY ID}_comment "global proportion: 0.444 (0.438 to 0.451)"
			local {MODELABLE ENTITY ID}_comment "remainder of parent attributed to asymptomatic"
			local {MODELABLE ENTITY ID}_comment "remainder of parent attributed to asymptomatic"
			local {MODELABLE ENTITY ID}_comment "remainder of parent attributed to asymptomatic"
			noisily di "ID: `me_upload'... ``me_upload'_comment'"
			qui do "{FILEPATH}/save_results.do"
			save_results, modelable_entity_id(`me_upload_use_`me_upload'') metrics("incidence prevalence") description("``me_upload'_comment'") in_dir("`tmp_dir'/`me_upload'/01_draws") mark_best("yes")
			noisily di "UPLOADED -> " c(current_time)
		}
	}
}
// ****************************************************************************
// Clear draws?
	if `sweep' == 1 {
		!rm -rf "`tmp_dir'"
	}

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
