// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		SUBMITS THE CLEANING PREP AND APPENDING STEPS FOR THE E-N MATRICES
// Author:		USERNAME

** *********************************************
// DON'T EDIT - prep stata
** *********************************************

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
		local hold_steps ""
		local last_steps ""
		local step_name "EN_matrices"
		local step_num "03b"
	}
		local check = 99
	if `check'==1 {
		local 1 _inj
		local 2 gbd2016
		local 3 DATE
		local 4 "03b"
		local 5 EN_matrices

		local 8 "FILEPATH"
	}
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
	// directory for external inputs
	local in_dir "`root_j_dir'/FILEPATH"
	// directory for output on the J drive
	local out_dir "`root_j_dir'/FILEPATH"
	// directory for output on clustertmp
	local tmp_dir "`root_tmp_dir'/FILEPATH"

	// write log if running in parallel and log is not already open
	cap log using "`out_dir'/FILEPATH.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "`root_j_dir'/FILEPATH/`date'" dirs "`step'_*", respectcase
			// remove extra quotation marks
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "`root_j_dir'/FILEPATH.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

** *********************************************
// WRITE CODE HERE
** *********************************************
	
// Settings
	** which sub-steps to run (leave blank if all)
	local sub_steps  "02"
	** max standard error to allow for a given model before moving to fewer covariates
	local max_se 1.5
	** what age groups are we going to aggregate to? 
	local ages 0 1 5 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 90 95
	** how many slots to run EN matrix creation code with
	local make_matrix_slots 1
	** are you debugging? If `debug'==0 then the code will delete the contents of the FILEPATH folder
	local debug=1
	
// Filepaths
	local homedir `root_j_dir'
	local data "`tmp_dir'/FILEPATH"
	local logs "`out_dir'/FILEPATH"
	local cleaned_dir "`data'/FILEPATH"
	local prepped_dir "`data'/FILEPATH"
	local gbd_ado "FILEPATH"
	local diag_dir "`out_dir'/FILEPATH"
	local summ_dir "`tmp_dir'/FILEPATH"

// Load ado-files
	adopath + `code_dir'/ado
	adopath + `gbd_ado'
	
	start_timer, dir("`diag_dir'") name("`step_name'")

// Load injuries parameters
	load_params
	
// Set-up filestructure
	foreach folder in 00_cleaned 01_prepped 03_modeled {
		cap mkdir "`data'/`folder'"
	}

// 00) Clean data
	if regexm("`sub_steps'","00") | "`sub_steps'" == "" do "`code_dir'/FILEPATH.do" $prefix `homedir' `code_dir' `step_name' `cleaned_dir' `prepped_dir' "`ages'" `data' `logs' `gbd_ado' `diag_dir'


// 01) Prep data
	if regexm("`sub_steps'","01") | "`sub_steps'" == "" {
		local args $prefix `date' `step_num' `step_name' `code_dir' `cleaned_dir' `prepped_dir' `in_dir' `root_j_dir'
		foreach ds in chinese_niss ihme_data chinese_icss hdr nld_iss argentina ihme_data_2016 gbr {
			di `""`code_dir'/FILEPATH.do" `args' `ds'"'
			!qsub -e FILEPATH -o FILEPATH -P proj_injuries -N prep_`ds' -pe multi_slot 40 "`code_dir'/FILEPATH.sh" "`code_dir'/FILEPATH.do" "`args' `ds'"
		}
	}

// 02) Append datasets and further prep
	if regexm("`sub_steps'","02") | "`sub_steps'" == "" do "`code_dir'/FILEPATH.do" $prefix `prepped_dir' `data' `logs'	
	