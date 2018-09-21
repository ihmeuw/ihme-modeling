// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This master file runs the steps involved in cod/nonfatal custom modeling for a modelable entity, using the steps spreadsheet as a template for launching the code
//				This master file should be used for submitting all steps or selecting one or more steps to run in "steps" global
// Author:		USERNAME
// Description:	See specific step files for descriptions.


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// SET GLOBALS

   // define directory that contains steps code
      local code_dir = "FILEPATH"
   // define directory that will contain results
      local out_dir = "FILEPATH"
   // define directory on clustertmp that holds intermediate files
      local tmp_dir = "FILEPATH"
   // define the date of the run in format YYYY_MM_DD
	  local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
	// define the steps to run as space-separated list: 01 02 03a 03b (blank for all)
		local steps = "13"
	// define the sequence of your steps (1=run parallelized on the cluster, 0=run in series to check intermediate results)
		local parallel 0

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// RUN MODEL (NO NEED TO EDIT BELOW THIS LINE)

// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	cap log close
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	if "`steps'" == "" local steps "_all"
   qui run "FILEPATH.ado"

	// Follow-up studies
	if regexm("`steps'", "02b") == 1 {
		cap mkdir "`out_dir'/FILEPATH"
		cap mkdir "`out_dir'/FILEPATH"
		cap mkdir "`out_dir'/FILEPATH"
		import delimited "FILEPATH.csv", delim(",") varn(1) case(preserve) clear
		export delimited "FILEPATH.csv", delim(",") replace
		file open finished using "FILEPATH.txt", replace write
		file close finished
	}

// run model
	model_custom, code_dir("`code_dir'") out_dir("`out_dir'") tmp_dir("`tmp_dir'") date("`date'") steps("`steps'") parallel(`parallel')
	
