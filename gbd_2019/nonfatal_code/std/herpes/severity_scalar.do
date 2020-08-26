// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:	Apply proportion
// do "FILEPATH/01_apply_scalar.do"

// PREP STATA
	clear
	set more off
	//set maxvar 3200
	if c(os) == "Unix" {
		global prefix "/home/j/"
		set odbcmgr unixodbc
		set mem 2g
	}
	else if c(os) == "Windows" {
		global prefix "J:"
		set mem 2g
	}


//Launch scalar jobs: 0 for no, 1 for yes

local tmp_dir "`1'" // Temp directory
local parent_model `2' // model_version_id
local loc `3' //location to run script on 


capture log close
log using "FILEPATH/`loc'_scaling.smcl", replace
// ****************************************************************************
// Log work
	
// ****************************************************************************
// Manually defined macros
	** User
	local username USERNAME

	** Steps to run (0/1)
	local apply_scalar 0
	local upload 1
	local sweep 0 //always leave this as 0, you're going to want to keep draws from past runs

	** Where is your local repo for this code?
	local prog_dir "FILEPATH/herpes/"
	local decomp "step4"

// ****************************************************************************
// Automated macros

	local tmp_dir FILEPATH
	capture mkdir "`tmp_dir'/02_newlogs"
	capture mkdir "`tmp_dir'/01_draws"
	capture mkdir "`tmp_dir'/errors"
	capture mkdir "`tmp_dir'/outputs"

	// make connection string
	run "FILEPATH/get_best_model_versions.ado"
	run "FILEPATH/get_location_metadata.ado"
// ****************************************************************************
// Load original models

	local me_id 1642
	get_best_model_versions, entity("modelable_entity") ids(`me_id') decomp_step("`decomp'") clear
	local parent_model 420671 //check for updated parent model id number



// ****************************************************************************
// Get country list
	

// ****************************************************************************
// Modify draws to apply undiagnosed scalar?
	if `apply_scalar' == 1 {
		//create the undiagnosed scalar draws
		clear
		local M = 0.01007
		local SE = 0.0055
		local N = `M'*(1-`M')/`SE'^2
		local a = `M'*`N'
		local b = (1-`M')*`N'
		clear
		set obs 1000
		gen prop_ = rbeta(`a',`b')
		gen num = _n-1
		gen mvar = 1
		reshape wide prop_, i(mvar) j(num)
		saveold "`tmp_dir'/prop_draws.dta", replace

		get_location_metadata, location_set_id(35) clear
		levelsof location_id if most_detailed==1, local(locations) //(abbreviate locations passed in as needed)

		foreach loc of local locations {
			!qsub -N "herpes_scalar_`loc'" -e "FILEPATH/errors" -o "FILEPATH/outputs" -l m_mem_free=10G -l fthread=5 -l h_rt=02:00:00 -l archive=TRUE -q all.q -P "proj_custom_models" "FILEPATH/stata_shell.sh" "`prog_dir'/01_apply_scalar.do"  " `tmp_dir' `parent_model' `loc' "	
			 
		}
	}

// Load in necessary function
run "FILEPATH/get_draws.ado"


// Load Herpes incidence and produce symptomatic 

	// Location_id


foreach year in 1990 1995 2000 2005 2010 2015 2017 2019{
	foreach sex in 1 2 {
		get_draws, gbd_id_type(modelable_entity_id) gbd_id(1642) source("epi") measure_id(6) location_id(`loc') year_id(`year') sex_id(`sex') age_group_id(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235) version_id(`parent_model') decomp_step("step4") clear
		drop modelable_entity_id
		gen mvar = 1
			//don't do anything to incidence
		outsheet using "`tmp_dir'/FILEPATH/6_`loc'_`year'_`sex'.csv", comma names replace
			// merge the undiagnosed scalar 
		merge m:1 mvar using "`tmp_dir'/prop_draws.dta", assert(3) nogen
			// multiply incidence by the scalar to get only the diagnosed prevalent cases
		forval t = 0/999 {
			replace draw_`t' = draw_`t'*prop_`t'
		}
		drop prop*
		replace measure_id = 5
		outsheet using "`tmp_dir'/FILEPATH/5_`loc'_`year'_`sex'.csv", comma names replace
	}
}

	


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************



