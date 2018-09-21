// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Author: NAME
// Purpose:	Adjusting FBT model to (1) limit the country list and (2) adjust for 
// heavy infection/cerebral
// do "/home/j/WORK/12_bundle/ntd_foodborne/gbd2016/01_code/00_master.do"

{
/*
Clonorchiasis due to food-borne trematodiases all countries (1525)
Fascioliasis due to food-borne trematodiases all countries (1526)
Intestinal fluke infection due to food-borne trematodiases all countries (1527)
Opisthorchiasis due to food-borne trematodiases all countries (1528)
Paragonimiasis due to food-borne trematodiases all countries (1529)
*/
}

// PREP STATA
	clear all
	set more off
	set maxvar 3200
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	
// ****************************************************************************
// Manually defined macros
	** User
	local username "USERNAME"
	
	** Steps to run (0/1)
	local country_limit 1
	local heavy_infection_cerebral 1
	local upload_parent 1
	local upload_child 1
	local sweep 0
	
// ****************************************************************************
// Automated macros

   // define the date of the run in format YYYY_MM_DD: 2014_01_09
  local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
  local date = subinstr("`date'"," ","_",.)

  ** local date "2017_03_13"


	// set adopath
	adopath + FILEPATH
	
	local prog_dir "FILEPATH"
	local code_dir "`prog_dir'/01_code"
	local in_dir "`prog_dir'/02_inputs"
	local tmp_dir "FILEPATH"
	capture mkdir "`tmp_dir'"
	capture mkdir "`tmp_dir'/_split_logs"
	
	adopath + "FILEPATH"

	// set the shell file
    local shell_file "FILEPATH/stata_shell.sh"

	
// ****************************************************************************
// Load original parent models
	"SQL QUERY"
	tempfile parent_me_list
	save `parent_me_list', replace
	
// ****************************************************************************
// Get country list
	get_location_metadata, location_set_id(35) clear
	keep if is_estimate == 1 & most_detailed == 1
	levelsof location_id, local(locations)

// ****************************************************************************
// Save draws with limited country lists
	if `country_limit' == 1 {
		insheet using "FILEPATH/parent_map.csv", comma names clear
		tempfile map
		save `map'
		use `parent_me_list', clear
		merge 1:1 modelable_entity_id using `map', assert(3) keep(3) keepusing(grouping) nogen
		levelsof grouping, local(groupings)
		local a = 0
		foreach group of local groupings {
			di "`group'"
			levelsof modelable_entity_id if grouping == "`group'" & strmatch(modelable_entity_name, "*countries*"), local(parent_me) c
			di "parent `parent_me'"
			levelsof modelable_entity_id if grouping == "`group'" & strmatch(modelable_entity_name, "Symptomatic*"), local(limit_me) c
			di "limit `limit_me'"
			levelsof model_version_id if grouping == "`group'" & strmatch(modelable_entity_name, "*countries*"), local(model) c
			capture mkdir "`tmp_dir'/checks"
			capture mkdir "`tmp_dir'/checks/country_limit"
			! qsub -P proj_custom_models -N "`group'_country_exclusions" -pe multi_slot 4 -l mem_free=8 "`shell_file'" "FILEPATH/01_country_limit.do" "`prog_dir' `tmp_dir' `parent_me' `limit_me' `model'"
			local ++ a
			sleep 100	
		}
			
			// wait for jobs to finish ebefore passing execution back to main step file
		local b = 0
		while `b' == 0 {
			local checks : dir "FILEPATH" files "finished_*.txt", respectcase
			local count : word count `checks'
			di "checking `c(current_time)': `count' of `a' jobs finished"
			if (`count' == `a') continue, break
			else sleep 60000
		}
	}
	


// ****************************************************************************
// Modify draws to contain proportion of cases with heavy infection/cerebral
	if `heavy_infection_cerebral' == 1 {
		clear
		gen group = . 
		tempfile props
		save `props', replace
		clear
		gen child_id = . 
		tempfile me_list
		save `me_list', replace
		insheet using "`in_dir'/fbt_high_intensity_infection_proportions.csv", comma names clear
		egen group = group(age_group_id sex_id grouping)
		preserve
			keep age_group_id sex_id grouping group
			tempfile tag
			save `tag', replace
		restore
		keep group mean lower upper
		levelsof group, local(gs)
		foreach g of local gs {
			preserve
				levelsof mean if group == `g', local(M) c
				levelsof lower if group == `g', local(L) c
				levelsof upper if group == `g', local(U) c
				local SE = (`U'-`L')/(2*1.96) 
				local N = `M'*(1-`M')/`SE'^2
				local a = `M'*`N'
				local b = (1-`M')*`N'
				clear
				set obs 1000
				gen prop_ = rbeta(`a',`b')
				gen num = _n-1
				gen group = `g'
				append using `props'
				save `props', replace
			restore
		}
		use `props', clear
		merge m:1 group using `tag', assert(3) nogen
		drop group
		** Make draws by sex_id
		reshape wide prop_, i(num age_group_id grouping) j(sex_id)
		replace prop_1 = prop_3 if prop_1 == . & prop_3 != .
		replace prop_2 = prop_3 if prop_2 == . & prop_3 != .
		drop prop_3
		reshape long prop_, i(num age_group_id grouping) j(sex_id)
		** Make age draws
		reshape wide prop_, i(num sex_id grouping) j(age_group_id)
		foreach af of numlist 164 2(1)20 30(1)32 235 {
			capture gen prop_`af' = .
		}
		foreach a in 3 4 5 6 {
			replace prop_`a' = prop_2 if prop_`a' == . & prop_2 != .
		}
		foreach ag of numlist 7(2)17 {
			local mid = `ag'+1
			replace prop_`mid' = prop_`ag' if prop_`mid' == . & prop_`ag' != .
		}
		foreach a of numlist 18(1)20 30(1)32 235 {
			replace prop_`a' = prop_17 if prop_`a' == . & prop_17 != .
		}
		foreach a of numlist 2(1)20 30(1)32 235 {
			replace prop_`a' = prop_22 if prop_`a' == . & prop_22 != .
		}
		reshape long prop_, i(num sex_id grouping) j(age_group_id)
		reshape wide prop_, i(age_group_id sex_id grouping) j(num)
		replace grouping = "intestinal fluke" if grouping == "fluke"
		levelsof grouping, local(grouping_names)
		foreach g_name of local grouping_names {
			preserve
				** Heavy
				if "`g_name'" != "cerebral" {
					odbc load, exec("SQL QUERY") `epi_str' clear
				}
				else if "`g_name'" == "cerebral" {
					odbc load, exec("SQL QUERY") `epi_str' clear
					}
				levelsof modelable_entity_id, local(id) c
				capture mkdir "`tmp_dir'/`id'"
				capture mkdir "`tmp_dir'/`id'/01_child_draws"
				** Asymp
				if "`g_name'" != "cerebral" {
					odbc load, exec("SQL QUERY") `epi_str' clear
				}
				else if "`g_name'" == "cerebral" {
					odbc load, exec("SQL QUERY") `epi_str' clear
				}
				levelsof modelable_entity_id, local(asymp_id) c
				capture mkdir "`tmp_dir'/`asymp_id'"
				capture mkdir "`tmp_dir'/`asymp_id'/01_child_draws"
				** Parent
				if "`g_name'" != "cerebral" {
					odbc load, exec("SQL QUERY") `epi_str' clear
				}
				else if "`g_name'" == "cerebral" {
					odbc load, exec("SQL QUERY") `epi_str' clear
				}
				gen child_id = `id'
				gen asymp_id = `asymp_id'
				append using `me_list'
				save `me_list', replace
			restore, preserve
				keep if grouping == "`g_name'"
				drop grouping
				saveold "FILEPATH/high_intensity_proportions.dta", replace
			restore
		}
		use `me_list', replace
		order child_id asymp_id
		saveold "`tmp_dir'/me_list.dta", replace
		sleep 5000
		local a = 0
		foreach location of local locations {
			!qsub -pe multi_slot 4 -l mem_free=8 -P "proj_custom_models" -N "Heavy_Infection_and_Cerebral_`location'" "`shell_file'" "`code_dir'/02_high_intensity.do" "`prog_dir' `tmp_dir' `location'"
			local ++ a
			sleep 100		
		}

		// wait for jobs to finish ebefore passing execution back to main step file
		local b = 0
		while `b' == 0 {
			local checks : dir "`tmp_dir'/checks/high_intensity" files "finished_*.txt", respectcase
			local count : word count `checks'
			di "checking `c(current_time)': `count' of `a' jobs finished"
			if (`count' == `a') continue, break
			else sleep 60000
		}

	}

// ****************************************************************************
// Upload
	// run save_results
	qui run "FILEPATH/save_results.do"

	if `upload_parent' == 1 {
		insheet using "`in_dir'/parent_map.csv", comma names clear
		tempfile parent_map
		save `parent_map'
		use `parent_me_list', clear
		merge 1:1 modelable_entity_id using `parent_map', assert(3) keep(3) keepusing(grouping) nogen
		levelsof grouping, local(fbt_groups)
		foreach fbt_group of local fbt_groups {
			quietly {
				noisily di "Uploading parent `fbt_group'..."
				levelsof modelable_entity_id if grouping == "`fbt_group'" & strmatch(modelable_entity_name, "Symptomatic*"), local(parent_upload) c
				levelsof model_version_id if grouping == "`fbt_group'" & strmatch(modelable_entity_name, "Symptomatic*"), local(model) c
				preserve
					save_results, modelable_entity_id(`parent_upload') env("prod") file_pattern({location_id}.csv) description("country limit applied to model `model'") in_dir("FILEPATH/01_country_limit") mark_best("yes")
				restore
				noisily di "UPLOADED -> " c(current_time)
			}
		}
	}
	if `upload_child' == 1 {
		use "`tmp_dir'/me_list.dta", clear
		levelsof child_id, local(upload_ids)
		foreach me_upload of local upload_ids {
			preserve
				quietly {
					levelsof parent_model if child_id == `me_upload', local(parent_id) c
					noisily di "ID: `me_upload'..."
					save_results, modelable_entity_id(`me_upload') env("prod") file_pattern({location_id}.csv) description("high intensity proportion split from model `parent_id'") in_dir("FILEPATH/01_child_draws") mark_best("yes")
					noisily di "UPLOADED -> " c(current_time)				
				}
			restore
		}
		levelsof asymp_id, local(upload_ids)
		foreach me_upload of local upload_ids {
			preserve
				quietly {
					levelsof parent_model if asymp_id == `me_upload', local(parent_id) c
					noisily di "ID: `me_upload'..."
					save_results, modelable_entity_id(`me_upload') env("prod") file_pattern({location_id}.csv) description("asymptomatic remainder from model `parent_id'") in_dir("FILEPATH") mark_best("yes")
					noisily di "UPLOADED -> " c(current_time)				
				}
			restore
		}
	}
	
// ****************************************************************************
// Clear draws
	if `sweep' == 1 {
		!rm -rf "`tmp_dir'"
	}
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
