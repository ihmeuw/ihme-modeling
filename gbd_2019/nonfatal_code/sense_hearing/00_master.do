//run ** *********************************************************************************************************************************************************************
//** *********************************************************************************************************************************************************************
//** Purpose:		This master file runs the steps involved in cod/epi custom modeling for a functional group, using the steps spreadsheet as a template for launching the code
//**				This master file should be used for submitting all steps or selecting one or more steps to run in "steps" global
//** Author:		USER
// Updated: 	USER Dec 2015 
//** Description:	run _hearing custom code

//To run master code:
	** qlogin
	** cd FILEPATH
	** git checkout develop 
	** git pull ADDRESS develop
	** stata-mp
	** do "FILEPATH"

**Three ways to run individual step code: 
		**NOTE: unless you are running all steps from model_custom, you need make sure your directories exist (there are date-stamped out_dir and tmp_dir folders in 03_steps). 
			//Or you use an old output and tmp folder that you rename to today's date 
		*1) locally - Edit cluster_0 settings 
		*2) On cluster, interactively or non-parallelized - Set cluster_check 1 and manually set locals 
		*3) On cluster from master file - Set cluster_check 0 
//aorji: changed file paths to my name/directories for gbd 2019

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// set a local that contains the uwnetid of the person running the code
// Reference -- answer by USER, available from: reference: ADDRESS
local username "`c(username)'"


** SET GLOBALS
	

		// define the sequence of your steps (1=run parallelized on the cluster, 0=run in series to check intermediate results)
		local parallel 0
		// define the steps to run as space-separated list: 01 02 03a 03b (blank for all)
		local steps = "02" //"02 03 04 05 06 07 99"
		


	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32767
	cap log close
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}

	** define directory that contains steps code
		local code_dir = "FILEPATH"
	** define directory that will contain results
		//Note: while named "out_dir", this actually is the root j directory, in which output directories are created by model_custom
			//for final runs, change the out_dir from FILEPATH to FILEPATH 
     	local out_dir = "FILEPATH"
    ** define directory on clustertmp that holds intermediate files (this is passed into step files as "root_tmp_dir" not "tmp_dir")
    	//local tmp_dir = "FILEPATH" 
    	local tmp_dir = "FILEPATH"
 	** define the date of the run in format YYYY_MM_DD: 2014_01_09
 		//local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
     	//local date = subinstr(`"`date'"'," ","_",.)
     	local date = "2019_10_06"

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// RUN MODEL (NO NEED TO EDIT BELOW THIS LINE)


	if "`steps'" == "" local steps "_all"	//saying that if steps is blank, run a new local for all, in a variable called steps as well 
	
  run "FILEPATH"

   	

// run model
	model_custom, code_dir("`code_dir'") out_dir("`out_dir'") tmp_dir("`tmp_dir'") date("`date'") steps("`steps'") parallel(`parallel')
	
