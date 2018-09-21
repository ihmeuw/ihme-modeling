// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This master file runs the steps involved in cod/epi custom modeling for a functional group, using the steps spreadsheet as a template for launching the code
//				This master file should be used for submitting all steps or selecting one or more steps to run in "steps" global
// Author:		
// Last updated:	
// Description:	Master file for encephalitis: see specific step files for descriptions


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// prep stata //
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	cap log close
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}

// SET GLOBALS

	// define the steps to run as space-separated list: 01 02 03a 03b (blank for all)
	local steps = "04d 05a 05b 09"
	if "`steps'" == "" local steps "_all"
	
	// define the sequence of your steps (1=run parallelized on the cluster, 0=run in series to check intermediate results)
	local parallel 1

	// define directory that contains steps code
	if `parallel' == 0 local code_dir "CODE_DIR"
	if `parallel' == 1 local code_dir "CODE_DIR"
	// define directory that will contain results -- this is actually the root_j_dir, not step-specific out_dir
	local out_dir = "OUT_DIR"
	// define directory on clustertmp that holds intermediate files
	local tmp_dir = "TMP_DIR"
	// define the date of the run in format YYYY_MM_DD: 2014_01_09
	local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
	local date = subinstr(`"`date'"'," ","_",.)

	// temporary date
	local date = "2017_06_12"


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// RUN MODEL (NO NEED TO EDIT BELOW THIS LINE)

// run model_custom 
	run "MODEL_CUSTOM"
	
// run model
	model_custom, code_dir("`code_dir'") out_dir("`out_dir'") tmp_dir("`tmp_dir'") date("`date'") steps("`steps'") parallel(`parallel')
