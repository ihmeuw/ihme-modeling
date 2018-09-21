// *************************************************************************************************
// *************************************************************************************************
// Purpose:		Launches central PAF calculator jobs and prep for burdenator
// *************************************************************************************************
// *************************************************************************************************

** *************************************************************************************************
** STATA
** *************************************************************************************************

	clear all
	set more off
	set maxvar 32000
	pause on

** *************************************************************************************************
** WORKSPACE
** *************************************************************************************************

	cap ssc install rsource

// set arguments manually
	local launch_pafs 1
	local save_results 1
	local gbd_round_id 4
	local n_draws 1000
	numlist "1990/2016"
	local year_ids `r(numlist)' 
	local year_ids : subinstr local year_ids " " "_", all
	local launch_prep_for_burdenator = 1
	local risk_version = 0
	local nuke_risk_version = 0

	local out_dir "FILEPATH"		
	local check_jobs_dir "FILEPATH"
	local code_dir "FILEPATH"

// load functions
	adopath + "FILEPATH"
	adopath + "`code_dir'/helpers"

	create_connection_string, server(ADDRESS)
    local epi_string = r(conn_string)

** *************************************************************************************************
** FIND RISKS THAT NEED TO RUN
** *************************************************************************************************

if `launch_pafs' {

	// Pull current best risk MEs in the DB (PAF, exposure, RR)
	# delim ;
	odbc load, exec("
		SELECT model_version_id, modelable_entity_id, modelable_entity_name, description,
			model_version_status_id, gbd_round_id, best_start, best_end, rei_id, rei, 
			dtype.modelable_entity_metadata_value as draw_type
		FROM epi.model_version mv
		LEFT JOIN epi.modelable_entity me USING (modelable_entity_id) 
		LEFT JOIN epi.modelable_entity_rei mer USING (modelable_entity_id) 
		JOIN (SELECT rei_id, rei 
			FROM shared.rei_hierarchy_history
			WHERE rei_set_version_id = shared.active_rei_set_version(2,`gbd_round_id')) r USING (rei_id) 
		JOIN
			(SELECT
			modelable_entity_metadata_value, modelable_entity_id
			FROM
			epi.modelable_entity_metadata mem
			JOIN
			epi.modelable_entity_metadata_type using (modelable_entity_metadata_type_id)
			WHERE 
			modelable_entity_metadata_type = 'gbd_2016' and mem.last_updated_action != 'DELETE') round using (modelable_entity_id)
		JOIN
			(SELECT
			modelable_entity_metadata_value, modelable_entity_id
			FROM
			epi.modelable_entity_metadata mem
			JOIN
			epi.modelable_entity_metadata_type using (modelable_entity_metadata_type_id)
			WHERE 
			modelable_entity_metadata_type = 'draw_type' and mem.last_updated_action != 'DELETE') dtype using (modelable_entity_id)
		WHERE model_version_status_id = 1 and gbd_round_id = `gbd_round_id'") `epi_string' clear;
	# delim cr
	levelsof rei, local(my_risks) c
	tempfile list
	save `list', replace

	// Make list of risks that have had exp or RR updated since last PAF calc
	clear
	gen rei = ""
	gen recalc = 0
	gen rr_recalc = 0
	gen exp_recalc = 0
	tempfile BUILD_LIST
	save `BUILD_LIST', replace
	foreach risk of local my_risks {
		use `list', clear
		keep if rei == "`risk'"
		levelsof best_start if draw_type == "paf", local(paf_start) c
		levelsof best_start if draw_type == "exposure", local(exposure_start) c
		levelsof best_start if draw_type == "rr", local(rr_start) c
		clear
		set obs 1
		gen rei = "`risk'"
		gen recalc = 0
		gen rr_recalc = 0
		gen exp_recalc = 0
		if "`paf_start'" != "" & "`exposure_start'" != "" {	
			foreach time of local exposure_start {
				if `paf_start' < `time' replace exp_recalc = 1
			}
		}
		if "`paf_start'" == "" & "`exposure_start'" != "" replace exp_recalc = 1
		if "`paf_start'"!="" & "`rr_start'"!="" {	
			foreach time of local rr_start {
				if `paf_start' < `time' replace rr_recalc = 1
			}
		}
		if exp_recalc == 1 | rr_recalc == 1 {
			replace recalc = 1
		} 
		append using `BUILD_LIST'
		save `BUILD_LIST', replace
	}
	keep if recalc == 1
	save `BUILD_LIST', replace

	// Check how many risks need to be updated.
	// If there are risks to run, give user option to edit list before submiting
	count
	if `r(N)'==0 {
		di as error "ALL PAFS are up to date! Double check COMO (smoking injuries, BMI, BMD, lead blood) and CoDCorrect (SIR, smoking injuries, BMD)"
		error(999)
	} 
	else {
		noi list
		noi di _newline(2) "Looks like we have risks we should run! Are you sure you want to run all of these? Hit enter and in the next line make any edits to the list, type end to launch, or BREAK to not launch" ///
			_newline(2) _request(initok)
		pause
		levelsof rei, local(to_calc) c
		tempfile to_calc_risks
		save `to_calc_risks', replace
	}

***********************************************************************************************
** EXPOSURE MAX AND MIN VALUE CALCULATION 
***********************************************************************************************

	import excel using "FILEPATH/risk_variables.xlsx", firstrow clear
	levelsof risk if risk_type == 2, local(exp_calc) c
	use `to_calc_risks', clear
	levelsof rei if exp_recalc == 1, local(exp_calc_risks) c

	// only run if exposure has been updated since last PAF calc
	local exp_calc : list exp_calc & exp_calc_risks
	foreach risk of local exp_calc {
		cap mkdir "`out_dir'/`risk'"
		cap mkdir "`out_dir'/`risk'/exposure"
		cap rm "`out_dir'/`risk'/exposure/exp_max_min.dta"
		// find risk type and exposure mvid
		risk_info, risk(`risk') gbd_round_id(`gbd_round_id') draw_type("exposure") clear
		levelsof risk_id, local(rei_id) c
		! qsub -P proj_rfprep -N "EXP_maxmin_`risk'" -pe multi_slot 40 -l mem_free=80 ///
			-l hosttype=intel -e FILEPATH  ///
			"FILEPATH/stata_shell.sh" "`code_dir'/core/calc_max_exp_continuous_sample_200.do" ///
			"`risk' `rei_id' `gbd_round_id' `code_dir' `out_dir'"
	}

	// wait till all exposure jobs are done
	! qstat -xml | tr '\n' ' ' | sed 's#<job_list[^>]*>#\n#g' | sed 's#<[^>]*>##g' | grep " " | column -t > "`check_jobs_dir'/exp_maxmin_grep.csv"
	local check = 1
	while `check' > 0 {
		sleep 20000
		cap import delimited using "`check_jobs_dir'/exp_maxmin_grep.csv", clear
		cap gen v1=""
		keep if regexm(v1,"EXP_maxmin_")
		count 
		if `r(N)' == 0 local check = 0
		noi di c(current_time) + ": `r(N)' jobs are still running"
		! qstat -xml | tr '\n' ' ' | sed 's#<job_list[^>]*>#\n#g' | sed 's#<[^>]*>##g' | grep " " | column -t > "`check_jobs_dir'/exp_maxmin_grep.csv"
	}

	// confirm that file was actually written
	foreach risk of local exp_calc {
		cap confirm file "`out_dir'/`risk'/exposure/exp_max_min.dta"
		if _rc {
			di as error "`out_dir'/`risk'/exposure/exp_max_min.dta does not exist. Something went wrong... "
			error(999)
		}
	}

***********************************************************************************************
** LAUNCH ACTUAL PAF CALC
***********************************************************************************************

	// set local for three rounds of launching
	local LAUNCHING = 0
	while `LAUNCHING' <= 3  & !missing("`to_calc'") {
		noi di c(current_time) + ": launching PAF jobs per location/sex, round `LAUNCHING'"

		// launch PAF calc
		foreach risk of local to_calc {

			// delete everything in the intermediate folder
			cap mkdir "`out_dir'/`risk'"
			if `LAUNCHING'==0 {
				! find "`out_dir'/`risk'" -maxdepth 1 -type f -delete
			}

			// find rei_id
			risk_info, risk(`risk') gbd_round_id(`gbd_round_id') clear
			levelsof risk_id, local(rei_id) c

			// pull locations & sexes to loop over
			get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
			local location_ids `r(location_ids)'
			local sex_ids `r(sex_ids)'
			** specific risks are just male or female so avoid submitting those jobs
			if inlist("`risk'","abuse_ipv_exp","abuse_csa_female","nutrition_iron") local sex_ids 2
			else if inlist("`risk'","abuse_csa_male") local sex_ids 1

			** submit PAF calc jobs
			foreach location_id of local location_ids {
				foreach sex_id of local sex_ids {

					// custom calc scripts
					if inlist("`risk'","drugs_illicit_suicide","nutrition_lbw_preterm","envir_lead_blood", ///
						"envir_lead_bone","metab_bmd","metab_ikf","nutrition_iron","occ_hearing", ///
						"smoking_direct_sir") | "`risk'" == "wash_water" {
						capture confirm file "`out_dir'/`risk'/paf_yll_`location_id'_`sex_id'.csv" 
						if _rc {
							! qsub -P proj_rfprep -N "PAF_calc_`risk'_`location_id'_`sex_id'" -pe multi_slot 12 ///
								-l mem_free=24 -e FILEPATH "FILEPATH/stata_shell.sh" ///
								"`code_dir'/core/custom_calc/custom_calc_`risk'.do" ///
								"`risk' `rei_id' `location_id' `sex_id' `year_ids' `gbd_round_id' `n_draws' `code_dir' `out_dir'"
						}
					}
					// normal calc script
					else {
						capture confirm file "`out_dir'/`risk'/paf_yll_`location_id'_`sex_id'.csv"
						if _rc {
							! qsub -P proj_rfprep -N "PAF_calc_`risk'_`location_id'_`sex_id'" -pe multi_slot 12 ///
								-l mem_free=24 -e FILEPATH "FILEPATH/stata_shell.sh" ///
								"`code_dir'/core/paf_calc.do" ///
								"`risk' `rei_id' `location_id' `sex_id' `year_ids' `gbd_round_id' `n_draws' `code_dir' `out_dir'"
						}
					}

				}
			}

		}  // end foreach risk loop of job sumbission by risk/location/sex

		// wait until all PAF jobs are complete 
		! qstat -xml | tr '\n' ' ' | sed 's#<job_list[^>]*>#\n#g' | sed 's#<[^>]*>##g' | grep " " | column -t > "`check_jobs_dir'/paf_calc_grep.csv"
		local check = 1
		while `check' > 0 {
			sleep 20000
			cap import delimited using "`check_jobs_dir'/paf_calc_grep.csv", clear
			cap gen v1=""
			keep if regexm(v1,"PAF_calc")
			count 
			if `r(N)' == 0 local check = 0
			noi di c(current_time) + ": `r(N)' PAF calc jobs are still running - round `LAUNCHING'"
			! qstat -xml | tr '\n' ' ' | sed 's#<job_list[^>]*>#\n#g' | sed 's#<[^>]*>##g' | grep " " | column -t > "`check_jobs_dir'/paf_calc_grep.csv"
		}

		// go thorugh this loop 3 times to confirm all files (ie OOM, etc)
		local LAUNCHING = `LAUNCHING' + 1

	}

} // end loop of launch pafs

***********************************************************************************************
** SUBMIT SAVE RESULTS JOBS
***********************************************************************************************

if `save_results' {
	foreach risk of local to_calc {
		! qsub -P proj_rfprep -N "save_results_PAF_`risk'" -pe multi_slot 20 -l mem_free=40 ///
			-e FILEPATH "FILEPATH/stata_shell.sh" "`code_dir'/core/submit_save_results.do" ///
			"`risk' `year_ids' `gbd_round_id' `code_dir' `out_dir'"
	}
}

***********************************************************************************************
** LAUNCH PREP FOR BURDENATOR
***********************************************************************************************

if `launch_prep_for_burdenator' {
	
	local final_dir "FILEPATH/pafs/`risk_version'"
	if `nuke_risk_version' {
		! find "`final_dir'" -maxdepth 1 -type f -delete
	}
	cap mkdir "`final_dir'"

	# delim ;
	odbc load, exec("
		SELECT model_version_id, modelable_entity_id, modelable_entity_name, description, best_start, rei
		FROM epi.model_version 
		LEFT JOIN epi.modelable_entity USING (modelable_entity_id) 
		LEFT JOIN epi.modelable_entity_rei USING (modelable_entity_id) 
        LEFT JOIN shared.rei USING (rei_id) 
		WHERE model_version_status_id = 1 and gbd_round_id = `gbd_round_id' AND modelable_entity_type_id IN (2,8) 
			AND modelable_entity_name like '%PAF'") `epi_string' clear;
	# delim cr
	levelsof model_version_id, local(mvids) sep(";") c

	// pull location and years to loop over
	get_demographics, gbd_team("epi") gbd_round_id(`gbd_round_id') clear
	local location_ids `r(location_ids)'
	foreach location_id of local location_ids {
		capture confirm file "`final_dir'/`location_id'_2016.dta"
		if _rc {
			! qsub -P proj_rfprep -N "PAF_compile_`location_id'" -pe multi_slot 8 -l hosttype=intel ///
				-l mem_free=16 -o FILEPATH -e FILEPATH "FILEPATH/stata_shell.sh" ///
				"`code_dir'/core/prep_for_burdenator.do" ///
				"`location_id' `year_ids' `risk_version' `mvids' `gbd_round_id' `code_dir' `out_dir' `n_draws'"
		}
	}

	// wait until jobs are done and make sure all files are present
	sleep 120000
	! qstat -xml | tr '\n' ' ' | sed 's#<job_list[^>]*>#\n#g' | sed 's#<[^>]*>##g' | grep " " | column -t > "`check_jobs_dir'/prep_for_burdenator_paf_grep.csv"
	local check = 1
	while `check' > 0 {
		sleep 10000
		import delimited using "`check_jobs_dir'/prep_for_burdenator_paf_grep.csv", clear
		cap gen v1=""
		keep if regexm(v1,"PAF_compile")
		count 
		if `r(N)' == 0 local check = 0
		noi di c(current_time) + ": `r(N)' jobs are still running"
		! qstat -xml | tr '\n' ' ' | sed 's#<job_list[^>]*>#\n#g' | sed 's#<[^>]*>##g' | grep " " | column -t > "`check_jobs_dir'/prep_for_burdenator_paf_grep.csv"
	}

	noi di c(current_time) + ": checking correct number of files exist"
	local n = 0
	local year_ids subinstr("`year_ids'","_"," ",.)
    mata: check_files()
    noi di "Expecting `expected_num_files' draw files. `n' files are missing."
    if `n' > 0 {
       di as error "Example: `fname'"
       error(999)
    }

} // end launch prep for burdenator loop

// END
