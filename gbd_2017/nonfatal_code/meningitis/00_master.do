// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This master file runs the steps involved in cod/epi custom modeling for a functional group, using the steps spreadsheet as a template for launching the code
//				This master file should be used for submitting all steps or selecting one or more steps to run in "steps" global
// Author:		
// Description:	Master ifle for meningitis: see specific step files for descriptions.

// important notes

	// MUST HAVE meningitis_steps.xlsx closed when running 00_master or it wont locate file
	// On first run, you must migrate input files into the in_dir (after it is created) 
	// change local date according to when the original run date (must be manually input on ODE prep and ODE run codes)
	// 1) RUN STEPS 02-04a!
	// 2) Launch ODE solver prep code in python
	// 3) Launch ODE solver run code in python
	// 4) Run 04b save results to upload hearing, vision, epilepsy (requires competion of 04b and ODE)
	//		note: this is annoyingly ran from 04b again because I dont want to add more scripts, make sure code portion is commented out
	// Regenerate bac_viral_ratio for each new set of hospital data, change path in step 08
	// 5) Run 05a-08
	// 6) Run 09 to check and save reults for acute, viral, long_modsev, long_mild 

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// quick run 
    ** do "FILEPATH"

// ***************************************** Manual inputs **************************************************************
	// define the steps to run as space-separated list: "02a 02b 02c 03a 03b 04a // 04b 05a 05b 07 08 09" (blank for all)
	//local steps "02b 02c 03a 03b 04a"
	//local steps "04b 05a 05b 07 08"
	local steps "09"
	if "`steps'" == "" local steps "_all"

	// define the date of the run in format YYYY_MM_DD: 2014_01_09
	local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
	local date = subinstr(`"`date'"'," ","_",.)
	// Change local date if 02 and 03 not run on same day
	local date = "2018_07_13"
// *********************************************************************************************************************

// prep stata
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


	//set parallelization 
	local parallel 1 
	if `parallel' == 1 local code_dir "FILEPATH"
	
	// define directory that will contain flat file inputs and log files 
	// define directory on clustertmp that holds intermediate draws, and ODE inputs/outputs
	local men_dir = "FILEPATH"
	cap mkdir `men_dir'
		
	// define directory on clustertmp that holds intermediate draws, and ODE inputs/outputs
	local tmp_dir = "`men_dir'/draws"
	cap mkdir `tmp_dir'
	
	// define directory that will contain log files
	local out_dir = "`men_dir'/logs"
	cap mkdir `out_dir'
	
	// manually put in files here 
	local in_dir = "FILEPATH"
	cap mkdir `in_dir'


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// RUN MODEL (NO NEED TO EDIT BELOW THIS LINE)

// run model_custom 
	run "FILEPATH"
	
// run model
	model_custom, code_dir("`code_dir'") out_dir("`out_dir'") tmp_dir("`tmp_dir'") date("`date'") steps("`steps'") parallel(`parallel') in_dir("`in_dir'")
