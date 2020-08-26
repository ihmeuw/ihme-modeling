// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

/*
Oral model tags :
oral_edent			2337	Edentulism and severe tooth loss
oral_edent			2584	Difficulty eating due to edentulism and severe tooth loss
oral_perio			2336	Chronic periodontal diseases
oral_decidcaries	2334	Deciduous caries
oral_decidcaries	2582	Tooth pain due to deciduous caries

STEPS BY CAUSE:
	oral_edent
		1) split parent [2337] by 0.444(0.438 – 0.451) to get difficulty eating [2584]
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
		global prefix "USERNAME"
		set odbcmgr unixodbc
		set maxvar 32767
	}
	else if c(os) == "Windows" {
		global prefix "J:"
		set maxvar 32767
	}


	run "FILEPATH"


// ****************************************************************************
// Manually defined macros

	get_location_metadata, location_set_id(35) gbd_round_id(6) clear
	keep if most_detailed == 1
	levelsof location_id, local(locations)
	** User
	//local username chikeda

	** Steps to run (1 2 3 4 5 from above)
	** If you run steps 2 and 3, you MUST also run the severity split of 4 and 5. Run one at a time.
	local run_steps 2 
	
	**** This section for resubmitting jobs for failed locations

	**local run_steps 1
	local locations 44786


	**local run_steps 2
	**local locations 25323	25327	25338	35443	35453	35494	35512	35636	35643	44676	44688	44699	44725	44770	44790	44850	44869	44930	44961	44983	4648	4664	4671	4716	4724	4727	4730	4740	4751	53558	53580	53591	53598	53616	562	566	573

	**local run_steps 3
	**local locations 110	122	151	156	25319	25329	25347	25351	35433	35453	35459	35495	35620	35641	35655	35659	43874	43886	43890	43900	43917	43924	44649	44665	44669	44689	44714	44720	44774	44856	44864	44869	44872	44878	44892	44912	44916	44920	44931	44939	44964	4660	4663	4674	4736	4922	53577	53613	53665	536	542	560	564	569	71

	**local run_steps 4
	**local locations 25338	25351	25	35448	35459	35498	35631	43898	43905	43921	44703	44964	4672	532	53616	53672	559	60	7

	**local run_steps 5
	**local locations 12	154	200	25334	25339	35449	35629	35660	43875	43887	43929	44677	44707	44766	44774	44782	44789	44859	44868	44880	44890	44919	4653	4666	4763	53533	53672	545	562	569	82	

	** Modelable_entity_ids to upload (2335 2336 2582 2583 2584 3091 3092 3093)
	local me_uploads 2335 2336 2582 2583 2584 3091 3092 3093
	**local me_uploads 2335

	** Sweep directory
	local sweep 0

	** Where is your local repo for this code?
	local prog_dir "USERNAME"
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
	/*
	count if (best_user != "nickjk") & (best_user != "agcsmith") & (modelable_entity_id == `perio_id' | modelable_entity_id == `permcaries_id')
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
*/









// ****************************************************************************
foreach run_step of local run_steps {
// 1) oral_edent --> split parent [2337] by 0.444(0.438 – 0.451) to get difficulty eating [2584]
	if `run_step' == 1 {
		** Set up results directory
		local child_id 2584
		local asymp_id 3093
		capture mkdir "`tmp_dir'/`child_id'"
		capture mkdir "`tmp_dir'/`child_id'/FILEPATH"
		capture mkdir "`tmp_dir'/`child_id'/FILEPATH"
		capture mkdir "`tmp_dir'/`asymp_id'"
		capture mkdir "`tmp_dir'/`asymp_id'/FILEPATH"
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
		saveold "`tmp_dir'/`child_id'/FILEPATH", replace
		foreach loc of local locations {
			!qsub -P "proj_custom_models" -l m_mem_free=4G -l fthread=1 -l h_rt=06:00:00 -q long.q -N "tooth_loss_split_`loc'" -o "FILEPATH" "`tmp_dir' `edent_id' `child_id' `asymp_id' `loc'"
		}
	}

// 2) oral_perio --> multiply model by *(1-prev_2337) at the CYAS level
	if `run_step' == 2 {
		** Set up results directory
		local perio_id 2336
		capture mkdir "`tmp_dir'/`perio_id'"
		capture mkdir "`tmp_dir'/`perio_id'/FILEPATH"
		capture mkdir "`tmp_dir'/`perio_id'/FILEPATH"
		foreach loc of local locations {
			!qsub -P "proj_custom_models" -l m_mem_free=4G -l fthread=1 -l h_rt=06:00:00 -q long.q -N "perio_split_`loc'" -o "FILEPATH" "`tmp_dir' `edent_id' `perio_id' `loc'"
		}
	}

// 3) oral_permcaries --> multiply parent model by *(1-prev_2337) at the CYAS level
	if `run_step' == 3 {
		** Set up results directory
		local permcaries_id 2335
		capture mkdir "`tmp_dir'/`permcaries_id'"
		capture mkdir "`tmp_dir'/`permcaries_id'/FILEPATH"
		capture mkdir "`tmp_dir'/`permcaries_id'/FILEPATH"
		foreach loc of local locations {
			!qsub -P "proj_custom_models" -l m_mem_free=4G -l fthread=1 -l h_rt=06:00:00 -q long.q -N "permcaries_split_`loc'" -o "FILEPATH" "`tmp_dir' `edent_id' `permcaries_id' `loc'"
		}
	}

// 4) oral_permcaries --> split parent into tooth pain [Data Rich = 0.0595 (0.0506 - 0.0685) // Data Poor = 0.0997 (0.0847 - 0.1146)]
	if `run_step' == 4 {
		** Set up results directory
		local child_id 2583
		local asymp_id 3092
		capture mkdir "`tmp_dir'/`child_id'"
		capture mkdir "`tmp_dir'/`child_id'/FILEPATH"
		capture mkdir "`tmp_dir'/`child_id'/FILEPATH"
		capture mkdir "`tmp_dir'/`asymp_id'"
		capture mkdir "`tmp_dir'/`asymp_id'/FILEPATH"
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
		saveold "`tmp_dir'/`child_id'/FILEPATH", replace
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
		saveold "`tmp_dir'/`child_id'/FILEPATH", replace
				get_location_metadata, location_set_id(43) gbd_round_id(6) clear
		gen dev = "D0" if parent_id == 44640
		replace dev = "D1" if parent_id == 44641
		foreach loc of local locations {
			local dev_stat "D0"
			!qsub -P "proj_custom_models" -l m_mem_free=4G -l fthread=1 -l h_rt=04:00:00 -q long.q -N "perm_tooth_pain_`loc'" -hold_jid "permcaries_split_`loc'" -o "FILEPATH" "`tmp_dir' `permcaries_id' `child_id' `asymp_id' `loc' `dev_stat'"
		}
	}

// 5) oral_decidcaries --> split parent into tooth pain [Data Rich = 0.0233 (0.0198 - 0.0268) // Data Poor = 0.0380 (0.0323 - 0.0437)]
	if `run_step' == 5 {
		** Set up results directory
		local child_id 2582
		local asymp_id 3091
		capture mkdir "`tmp_dir'/`child_id'"
		capture mkdir "`tmp_dir'/`child_id'/FILEPATH"
		capture mkdir "`tmp_dir'/`child_id'/FILEPATH"
		capture mkdir "`tmp_dir'/`asymp_id'"
		capture mkdir "`tmp_dir'/`asymp_id'/FILEPATH"
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
		saveold "`tmp_dir'/`child_id'/FILEPATH", replace
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
		saveold "`tmp_dir'/`child_id'/FILEPATH", replace
			get_location_metadata, location_set_id(43) gbd_round_id(6) clear
		gen dev = "D0" if parent_id == 44640
		replace dev = "D1" if parent_id == 44641
		foreach loc of local locations {
			**levelsof dev if location_id == `loc', local(dev_stat) c
			local dev_stat "D0"
			!qsub -P "proj_custom_models" -l m_mem_free=4G -l fthread=1 -l h_rt=04:00:00 -q long.q -N "decid_tooth_pain_`loc'" -o "FILEPATH" "`tmp_dir' `decidcaries_id' `child_id' `asymp_id' `loc' `dev_stat'"
		}
	}

}
// ****************************************************************************
// ****************************************************************************
	*/



// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************


*done
