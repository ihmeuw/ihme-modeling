// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:	 USERNAME
// Description:	apply probability of long-term outcomes to short-term incidence to get incidence of long-term outcomes by ecode-ncode-platform
//                       then run dismod engine to get non-shock prevalence and custom ODE solver to get shock prevalence
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	set type double, perm
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
// Global can't be passed from master when called in parallel
	if "`1'" == "" {
		local 1 FILEPATH
		local 2 FILEPATH
		local 3 DATE
		local 4 "05b"
		local 5 long_term_inc_to_raw_prev

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

// Filepaths
// Filepath to where pyHME function library is located
	local func_dir "`code_dir'"
	local diag_dir "`tmp_dir'/FILEPATH"
	local pops_dir "`tmp_dir'/FILEPATH"
	local rundir "`root_tmp_dir'/FILEPATH"
	local checkfile_dir "`tmp_dir'/FILEPATH"
	local ode_checkfile_dir "`tmp_dir'/FILEPATH"
	local prev_results_dir "`tmp_dir'/FILEPATH"
	
// Import functions
	adopath + "`code_dir'/ado"
	adopath + "`func_dir'"
	adopath + "FILEPATH"
	
	local slots 1
	start_timer, dir("`diag_dir'") name("`step_name'") slots(`slots')

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************

// use these to turn on the different blocks of code
	global make_pops 0
	global update_pops 0
	// temporarily save the long-term incidence numbers in this file structure
	global prep_ltinc 0
	// Run shock ODE solver
	global shock 0
	// create rate_in value_in plain_in data_in effect_in
	global prep_dm_input 0
	// run nonshock dismod code to create estimates of long term prevalence for the nonshock e-codes
	global nonshock 0
	// Append results to the country-year-sex level
	global append_results 0
	global append_results_annual 1

	global value_in 0

	global shock_run 0
	
	// are you just testing this code and don't wait to erase/zip any previous files at the end?
	local debug 1
	local tester 1

// Settings
	** two ways of sending excess mortality data to dismod "SMR" or "chi"; try both ways
	global SMR_or_chi "chi"
	** how much memore does the prep_ltinc step need
	local prep_ltinc_mem 2
	** how much memory does the prep_ltinc step need for shocks
	local prep_ltinc_mem_shocks 4
	** how much memory does the data_in/rate_in generation script need
	local data_rate_mem 4
	** how much memory does creating the value_in files need
	local value_in_mem 2
	** how much memory does appending the results take
	local append_results_mem 2
	
// start_timer
	local diag_dir "`tmp_dir'/FILEPATH"
	start_timer, dir("`diag_dir'") name("`step_name'")
	
// Load parameters
	load_params

	local platforms inp otp

// Write file to signify whether we are using SMR or excess mort (chi in dismod terms)
	capture erase "`tmp_dir'/FILEPATH.txt"
	capture erase "`tmp_dir'/FILEPATH.txt"
	
	if "$SMR_or_chi"== "SMR" {
		file open check using "`tmp_dir'/FILEPATH.txt", replace write
	}
		
	if "$SMR_or_chi"== "chi" {
		file open check using "`tmp_dir'/FILEPATH.txt", replace write
		local has_mort =1
	}

// set type for pulling different years (cod/epi); this is used for what parellel jobs to submit based on cod/epi estimation demographics, not necessarily what inputs/outputs you use
	local type "epi"
// set subnational=no (drops subnationals) or subnational=yes (drops national CHN/IND/MEX/GBR)
	local subnational "yes"
	
// Step 1: Save all major inputs that don't need to be parallelized: single-year populations, GBD age group populations
	** get single-year and group-year age pops for other code to pull.
	if $make_pops {
		get_population, year_id($year_ids) location_id($location_ids) sex_id($sex_ids) age_group_id($age_group_ids) clear
			save "`pops_dir'/FILEPATH.dta", replace

		get_demographics, gbd_team(cod) clear
			global cod_years `r(year_ids)'

		* pull single-year-age-group populations
		* for those we can pull single-year pops for
		numlist "49/142"
		global sy_ages `r(numlist)'

		get_population, year_id($cod_years) location_id($location_ids) sex_id($sex_ids) age_group_id($sy_ages) single_year_age(1) clear
			tempfile sy_populations
			save `sy_populations'

		* pull non-single-year-pops for neonates and 95+
		get_population, year_id($cod_years) location_id($location_ids) sex_id($sex_ids) age_group_id(2 3 4 235) single_year_age(0) clear
			append using "`sy_populations'"

			tempfile temp_pops
			save `temp_pops'

		* pull age group ids so that they are in age form
		get_ids, table(age_group) clear
			merge 1:m age_group_id using `temp_pops', keep(3) nogen

		* change ages so that they are numeric
			replace age_group_name = "95" if age_group_name == "95 plus"

			tempfile grp_pops
			save `grp_pops'

			replace age_group_name = "0" if age_group_name == "Early Neonatal" | age_group_name == "Late Neonatal" | age_group_name == "Post Neonatal"
			drop process_version_map_id
		
			drop age_group_id
			rename age_group_name age

			fastcollapse population, type(sum) by(location_id year_id age sex_id)
			tempfile temp_pops
			save `temp_pops', replace

		* pull location metadata to merge on ISO3's
		get_location_metadata, location_set_id(35) clear
			keep location_id ihme_loc_id
			rename ihme_loc_id iso3
			tempfile metaloc
			save `metaloc'

			merge 1:m location_id using `temp_pops', keep(3) nogen

			drop location_id
			rename year_id year
			rename population pop

			gen sex = "male" if sex_id == 1
			replace sex = "female" if sex_id == 2
			drop sex_id

			save "`pops_dir'/FILEPATH.dta", replace
			export delimited "`pops_dir'/FILEPATH.csv", delim(",") replace

		insheet using "`code_dir'/FILEPATH.csv", comma names clear
			rename age_group age_group_name

			tempfile convert_ages
			save `convert_ages'

		use `grp_pops', clear
			merge m:1 age_group_name using `convert_ages', keep(3) nogen
			rename age_start age

			fastcollapse population, type(sum) by(location_id year_id age sex_id)
			tostring age, force replace
			merge m:1 location_id using `metaloc', keep(3) nogen

			drop location_id
			rename population pop
			rename year_id year

			gen sex = "male" if sex_id == 1
			replace sex = "female" if sex_id == 2
			drop sex_id

			export delimited "`pops_dir'/FILEPATH.csv", delim(",") replace
	}
	
	** get the filepath to the excess mortality draws and incidence draws
	import excel using "`code_dir'/FILEPATH.xlsx", firstrow clear
	** non-shock incidence
	preserve
	keep if name == "SMR_to_excessmort"
	local this_step=step in 1
	local SMR_dir = "`root_tmp_dir'/FILEPATH"
	restore

// Step 2B: prep the value-in files for the latest dismod run for each E-codes
// this wasn't done last year, so this is a new part of the code
if $value_in {
	insheet using "`code_dir'/FILEPATH.csv", comma names clear
		keep if injury_metric == "Adjusted data"
		levelsof e_code, l(ecodes) clean

	if `tester' == 1 {
		local ecodes "inj_animal_nonven"
	}

	local value_code "FILEPATH.do"

	foreach ecode of local ecodes {
		! qsub -P proj_injuries -N value_in_`ecode' -pe multi_slot 2 "`code_dir'/FILEPATH.sh" "`code_dir'/`value_code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `ecode'"
	}
}

* Step 3: submit shock e-codes to custom ODE solver
if $shock_run {
	if $shock {
	* directory of GBD age group characteristics (created earlier)
		local ages_dir "`in_dir'/FILEPATH"
	* filepath to csv that contains cleaned single-year-age-group populations
		local pop_s_path "`pops_dir'/FILEPATH.csv"
		if $update_pops {
			insheet using "`out_dir'/FILEPATH.csv", comma names clear
			outsheet using "`pop_s_path'", comma names replace
		}
	* filepath to csv that contains cleaned GBD-age-group populations	
		local pop_grp_path "`pops_dir'/FILEPATH.csv"
		if $update_pops {	
			insheet using "`out_dir'/FILEPATH.csv", comma names clear
			outsheet using "`pop_grp_path'", comma names replace	
		}

	* Keep count of how many shock jobs have been submitted
		local shock_jobs 0
	
	* Loop over platforms
		foreach platform of local platforms {
			
		* Set excess mortality directory, if exists for this ncode-platform
			if "`platform'"=="inp" local mort_dir "`SMR_dir'/FILEPATH"
			else local mort_dir ""
		* directory of long-term shock incidence estimates
			local shockinc_dir = "`tmp_dir'/FILEPATH"
			
		* Loop over sex
			foreach sex_string in male  {
			
			* Loop over countries
				*foreach iso3 of global iso3s {
				foreach location_id of global location_ids {
					capture mkdir "`ode_checkfile_dir'"
					
				* Set path for timer-file to be created
				
					* Increment count of how many jobs have been submitted (to check for checkfiles)
					local ++shock_jobs
					
					* Path of check file for this platform-iso3-sex group
					
					use `metaloc', clear
					keep if location_id == `location_id'
					local iso3 = ihme_loc_id

					local checkfile_path "`ode_checkfile_dir'/FILEPATH.txt"
					local time_path "`diag_dir'/FILEPATH.csv"

					local output_dir "`prev_results_dir'/FILEPATH"

				* Submit ODE solver
					local slots 4
					local mem 8
					di "`checkfile_path'"
					confirm file "`code_dir'/FILEPATH.sh"
					confirm file "`code_dir'/`step_name'/FILEPATH.py"
					* SAVE ANNUAL PREVALENCE 
						local output_dir "`prev_results_dir'/FILEPATH"
						!qsub -P proj_injuries -N shock_ode_`platform'_`iso3'_`sex_string' -pe multi_slot `slots' -l mem_free=`mem' -p -2 "`code_dir'/FILEPATH.sh" "`code_dir'/FLIEPATH.py" `iso3' `sex_string' `ages_dir' `func_dir' `code_dir' `pop_s_path' `pop_grp_path' `shockinc_dir' `output_dir' `time_path' `checkfile_path' `mort_dir'
				}
			}
		}
	}
}
	
// Step 4: prep input files for dismod runs wait for all the long-term incidence jobs to finish before continuing (occurs in the parallelize function)
	if $prep_dm_input {
	
		** need a file path for the file with the model version ids pulled from dismod 
		import excel using "`code_dir'/FILEPATH.xlsx", firstrow clear
		// where are the short term incidence results by EN combination saved
		keep if name == "raw_nonshock_short_term_ecode_inc_by_platform"
		local pull_step = step in 1
		local modnum_dir = "`root_j_dir'/FILEPATH"	
		
		local dminput_dir "`tmp_dir'/FILEPATH"
		capture mkdir "`dminput_dir'"
		
		** make effect_in.csv; same for all models
		import delimited "`in_dir'/FILEPATH.csv", delim(",") asdouble varnames(1) clear
		gen x_ones=1 // 
		export delimited using "`dminput_dir'/FILEPATH.csv", delim(",") replace
		
		** make draw_in.csv; same for all models
		import excel using "`in_dir'/FILEPATH.xlsx", sheet("Sheet1") firstrow clear
		if ("$SMR_or_chi"=="chi") drop if integrand=="mtstandard"
		expand 2 if integrand == "incidence", gen(mtexcess)
		replace integrand = "mtexcess" if mtexcess == 1
		export delimited using "`dminput_dir'/FILEPATH.csv", delim(",") replace
		
		** make the folder where the FILEPATH`FILEPATH'.csv files will be made
		local plainin_dir "`dminput_dir'/FILEPATH"			
		capture mkdir "`plainin_dir'"			
		** make the folder where the `ncode'/FILEPATH`ecode'.csv files will be made
		local  ratein_dir "`dminput_dir'/FILEPATH"			
		capture mkdir "`ratein_dir'"
		
		local datain_dir "`dminput_dir'/FILEPATH"
		capture mkdir "`datain_dir'"
		local ecode_count=0
		
		// PARALLELIZE data_in and rate_in BY ISO3/YEAR/SEX				
		local code "/`step_name'/FILEPATH.do"

		local platforms inp otp

		if `tester' == 1 {
			global location_ids 35652
			global sex_ids 1
			global year_ids 1995
		}
		di "`date'"

		foreach location_id of global location_ids {
			foreach year_id of global year_ids {
				foreach sex_id of global sex_ids {
					! qsub -P proj_injuries -N ode_REDO_`location_id'_`year_id'_`sex_id' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FLIEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year_id' `sex_id'"
				}
			}
		}	
	}
	** end prep dm_input block

// Step 5: Append non-shock and shock E-codes to one file
	** make sure shock step has finished
	if $shock {
		local i = 0
		while `i' == 0 {
			local checks : dir "`ode_checkfile_dir'" files "shocks_*.txt", respectcase
			local count : word count `checks'
			di "checking `c(current_time)': `count' of `shock_jobs' jobs finished"
			if (`count' == `shock_jobs') continue, break
			else sleep 60000
		}
	}
	
	** run parallelized code to append results
	if $append_results {
		local code "`step_name'/FILEPATH.do"

		get_demographics, gbd_team(epi) clear
		global location_ids `r(location_ids)'
		global year_ids `r(year_ids)'
		global sex_ids `r(sex_ids)'

		if `tester' == 1 {
			global location_ids 44661
			global sex_ids 2
		}

		foreach location_id of global location_ids {
			foreach year_id of global year_ids {
				foreach sex_id of global sex_ids {
					! qsub -P proj_injuries -N append_`location_id'_`year_id'_`sex_id' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FLIEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year_id' `sex_id'"
				}
			}
		}
	}

	if $append_results_annual {
		local code "`step_name'/FILEPATH.do"

		get_demographics, gbd_team(epi) clear
		global location_ids `r(location_ids)'
		global year_ids `r(year_ids)'
		global sex_ids `r(sex_ids)'

		if `tester' == 1 {
			global location_ids 35643
			global sex_ids 1
			global year_ids 1990
		}

		foreach location_id of global location_ids {
			foreach year_id of global year_ids {
				foreach sex_id of global sex_ids {
					! qsub FILEPATH -P proj_injuries_2 -N append_annual_`location_id'_`year_id'_`sex_id' -pe multi_slot 4 -l mem_free=8 "`code_dir'/FLIEPATH.sh" "`code_dir'/`code'" "`root_j_dir' `root_tmp_dir' `date' `step_num' `step_name' `code_dir' `location_id' `year_id' `sex_id'"
				}
			}
		}
	}	
	
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
