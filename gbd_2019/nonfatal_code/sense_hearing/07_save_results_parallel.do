// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This sub-step template is for parallelized jobs submitted from main step code
// Author:		USER
//Updated 		USER Jan 2016
// Description:	Parallelization of 07_save_results

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// set a local that contains the uwnetid of the person running the code
// Reference -- answer by Chamara, available from: reference: ADDRESS
local username "`c(username)'"

//Running interactively on cluster
** do "FILEPATH"
** do "FILEPATH" 

	local cluster_check 0
	if `cluster_check' == 1 {
		local 1		"FILEPATH"
		local 2		"FILEPATH"
		local 3		"2019_10_06"
		local 4		"07"
		local 5		"save_results"
		local 6		"FILEPATH"
		local 7		"2312"
		}
	
// LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION)
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	set type double, perm
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		local cluster 1 
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		local cluster 0
	}
	// directory for standard code files
		adopath + "FILEPATH"

	//If running locally, manually set locals (USER: I deleted or commented out ones I think will be unnecessary for running locally...if needed refer to full list below)
	if `cluster' == 0 {

		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
        local date = subinstr("`date'", " " , "_", .)
		local step_num "07"
		local step_name "save_results"
		local code_dir "FILEPATH"
		local in_dir "FILEPATH"
		local root_j_dir "FILEPATH"
		local root_tmp_dir "FILEPATH"
		local meid "2323"

		}

	//If running on cluster, use locals passed in by model_custom's qsub
	else if `cluster' == 1 {
		// base directory on share scratch
		local root_j_dir `1'
		// base directory on clustertmp
		local root_tmp_dir `2'
		// timestamp of current run (i.e. 2014_01_17) 
		local date `3'
		// step number of this step (i.e. 01a)
		local step_num `4'
		// name of current step (i.e. first_step_name)
		local step_name `5'
		// directory for steps code
		local code_dir `6'
		local meid `7'

		}
	
	**Define directories 
		// directory for external inputs 
			//USER: hardcoded for now... model_custom doesn't pass it, rather by default tells us to 
			//make an input folder within root_j_dir, but I don't want to have separate input folders
			//in both FILEPATH and FILEPATH, so I'll use the input folder structure from 2013 
		local in_dir "FILEPATH"
		// directory for output on the J drive
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		local out_dir "FILEPATH"
		// directory for output on clustertmp
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		local tmp_dir "FILEPATH"
	
	//Note USER: right now I have qsub set to only write outputs and errors if in diagnostic mode to reduce files (if re-adding, may need to close log at end)
		/*
		// write log if running in parallel and log is not already open
		cap log using "FILEPATH", replace
		if !_rc local close_log 1
		else local close_log 0
		*/

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// WRITE CODE HERE


// USER HACK 6/1 - make sure there are no negative draws
/*
	cd "FILEPATH"
	local files: dir . files "*.csv"

	foreach file in `files' {
		qui insheet using `file', comma clear
		qui ds draw* 
		foreach d in `r(varlist)' {
			qui count if `d' < 0
			* no di "DRAW `d' IS NEGATIVE FOR `file'"
			qui replace `d' = 0 if `d' < 0
			}
		qui export delim using `file', replace 
	}
*/

//Custom outputs: for list, see "FILEPATH"

run "FILEPATH"
	local mark_best = "False"
	local db_env = "fair"
	local description = "model results"
	local input_file_pattern = "FILEPATH"
	local measure_id 5
	local decomp = "iterative"
	local gbd_round_id=6

		local input_dir "FILEPATH"
		di "save_results_epi, modelable_entity_id(`meid') mark_best(`mark_best') input_file_pattern(`input_file_pattern') description(`description') input_dir(`input_dir') measure_id(`measure_id') decomp_step(`decomp') gbd_round_id(`gbd_round_id') clear"
		    save_results_epi, modelable_entity_id(`meid') mark_best(`mark_best') input_file_pattern(`input_file_pattern') description(`description') input_dir(`input_dir') measure_id(`measure_id') decomp_step("step4") gbd_round_id(`gbd_round_id') clear

// CHECK TO MAKE SURE IT ACTUALLY UPLOADED - save_results won't break if it fails (say, for negative draws or too many model versions)
/*
	create_connection_string, database(epi) server(modeling-epi-db)
	local epi_str = r(conn_string)
	get_best_model_versions, entity(modelable_entity) ids(`meid') decomp_step("`decomp'") clear
	levelsof model_version_id, local(version) c
	odbc load, exec("SELECT description FROM epi.model_version WHERE model_version_id = `version'") `epi_str' clear
	count if regexm(description, "`date'")
	if `r(N)' == 0 {
	    BREAK
	}
	*/



// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

cap log close //Note USER: this is superfluous if I'm not actually writing logs in parallel jobs 

	// write check file to indicate sub-step has finished
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		cap mkdir "FILEPATH"
		file open finished using "FILEPATH", replace write
		file close finished

