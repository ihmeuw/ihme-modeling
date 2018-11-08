// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This master file runs the steps involved in cod/epi custom modeling for a functional group, using the steps spreadsheet as a template for launching the code
// Author:		
// Last updated: 10/5/2017
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

	// define the steps to run as space-separated list (all steps: first set and second set 02a 02b 03a 03b 04a // 04b 05a 05b 09)
	//local steps = "04b 05a 05b"
	//local steps = "05b"
	//local steps = "02a 02b 03a 03b 04a"
	local steps "09"
	if "`steps'" == "" local steps "_all"
	
	// define the sequence of your steps (1=run parallelized on the cluster, 0=run in series to check intermediate results)
	local parallel 1
	if `parallel' == 1 local code_dir "FILEPATH"

	// define the date of the run in format YYYY_MM_DD: 2014_01_09
	local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
	local date = subinstr(`"`date'"'," ","_",.)
	// Change local date if 02 and 03 not run on same day
	local date = "2018_07_13"

	// define directory on clustertmp that holds intermediate draws, and ODE inputs/outputs
	local enceph_dir = "FILEPATH"
	cap mkdir `enceph_dir'
		
	// define directory on clustertmp that holds intermediate draws, and ODE inputs/outputs
	local tmp_dir = "`enceph_dir'/draws"
	cap mkdir `tmp_dir'
	
	// define directory that will contain log files
	local out_dir = "`enceph_dir'/logs"
	cap mkdir `out_dir'
	
	// manually put in files here 
	local in_dir = "FILEPATH"
	cap mkdir `in_dir'


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// RUN MODEL (NO NEED TO EDIT BELOW THIS LINE)

// run model_custom 
	run "`code_dir'/model_custom.ado"
	
// run model
	model_custom, code_dir("`code_dir'") out_dir("`out_dir'") tmp_dir("`tmp_dir'") date("`date'") steps("`steps'") parallel(`parallel') in_dir(`in_dir')
