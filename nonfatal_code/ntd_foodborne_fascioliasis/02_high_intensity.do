// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: USERNAME
// Purpose:	Limit countries

// PREP STATA
	clear
	set more off
	set maxvar 3200
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	
// Program directory
	local prog_dir "`1'"
	
// Temp directory
	local tmp_dir "`2'"
	
// Sequela label
	local location "`3'"

// other file paths
    local code_dir "`prog_dir'/01_code"
    local in_dir "`prog_dir'/02_inputs"
    
// set adopath
    adopath + "FILEPATH"

// years
    local years 1990 1995 2000 2005 2010 2016
// sexes
    local sexes 1 2

	// set up file paths for checks
	capture mkdir "`tmp_dir'/checks"
    capture mkdir "`tmp_dir'/checks/high_intensity"
	
// ****************************************************************************
// Use get_draws
    run "FILEPATH/get_draws.ado"

// Load sequela list and produce symptomatic
	use "`tmp_dir'/me_list.dta", clear
	levelsof child_id, local(me_ids)
	foreach me_id of local me_ids {
		preserve
			levelsof parent_id if child_id == `me_id', local(parent_id) c
			insheet using "FILEPATH/`location'.csv", comma names clear
			drop if age_group_id == 22 | age_group_id == 27 | age_group_id == 33
			recast double age_group_id
			tempfile parent_`parent_id'
			save `parent_`parent_id'', replace
			merge m:1 age_group_id sex_id using "FILEPATH/high_intensity_proportions.dta", assert(2 3) keep(3) nogen
			forval t = 0/999 {
				replace draw_`t' = draw_`t'*prop_`t'
			}
			replace modelable_entity_id = `me_id'
			drop prop*
			tempfile child_`me_id'
			save `child_`me_id'', replace
			outsheet using "FILEPATH/`location'.csv", comma names replace
		restore
	}
	levelsof asymp_id, local(asymp_ids)
	foreach asymp_id of local asymp_ids {
		preserve
			levelsof parent_id if asymp_id == `asymp_id', local(parent_id) c
			levelsof child_id if asymp_id == `asymp_id', local(child_ids) c
			use `parent_`parent_id'', clear
			renpfix draw parent
			foreach child_id of local child_ids {
				merge 1:1 age_group_id sex_id measure_id year_id using `child_`child_id'', assert(3) nogen
				forval t = 0/999 {
					replace parent_`t' = parent_`t'-draw_`t'
				}
				drop draw*
			}
			replace modelable_entity_id = `asymp_id'
			renpfix parent draw
			outsheet using "FILEPATH/`location'.csv", comma names replace
		restore
	}
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// write check here
    file open finished using "FILEPATH/finished_`location'.txt", replace write
    file close finished