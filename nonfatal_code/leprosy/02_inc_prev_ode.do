// **********************************************************************
// Purpose:        This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Author:          USERNAME
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILENAME"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILENAME"
	}
	if "`1'" != "" {
		// base directory on J 
		local root_j_dir `1'
		// base directory on share
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
	}

	// directory for external inputs
	local in_dir "FILENAME"
	// directory for output on the J drive
	local out_dir "FILENAME"
	// directory for output on share
	capture mkdir "FILENAME"
	local tmp_dir "FILENAME"
	capture mkdir "FILENAME"
	capture mkdir "FILENAME"

	// set adopath
	adopath + "FILENAME"

	// set shell file
	local shell_file "FILENAME/stata_shell.sh"

	// write log if running in parallel and log is not already open
	cap log using "FILEPATH/`step_num'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0
	
	// check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "`FILEPATH/`date'" dirs "`step'_*", respectcase
			local dir = subinstr(`"`dir'"',`"""',"",.)
			capture confirm file "FILEPATH/finished.txt"
			if _rc {
				di "`dir' failed"
				BREAK
			}
		}
	}	

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
 // Import functions
	qui run "FILENAME/save_results.do"
  
// Make subfolders in`tmp_dir' on clustertmp 
	capture mkdir `tmp_dir'/sy_pop
	capture mkdir `tmp_dir'/draws
	capture mkdir `tmp_dir'/draws/upload
	capture mkdir `tmp_dir'/draws/upload/1662
	capture mkdir `tmp_dir'/draws/upload/1663
	capture mkdir `tmp_dir'/draws/upload/1664
	capture mkdir `tmp_dir'/draws/cases
	capture mkdir `tmp_dir'/draws/cases/age_pattern_interpolated
	capture mkdir `tmp_dir'/draws/cases/age_pattern_interpolated/interpolated
	capture mkdir `tmp_dir'/draws/cases/inc_annual
	capture mkdir `tmp_dir'/draws/cases/prev_initial
	capture mkdir `tmp_dir'/draws/cases/final
	capture mkdir `tmp_dir'/draws/disfigure_1
	capture mkdir `tmp_dir'/draws/disfigure_1/final
	capture mkdir `tmp_dir'/draws/disfigure_2
	capture mkdir `tmp_dir'/draws/disfigure_2/prev_initial
	capture mkdir `tmp_dir'/draws/disfigure_2/final
	capture mkdir `tmp_dir'/02_temp
	capture mkdir `tmp_dir'/02_temp/01_code
	capture mkdir `tmp_dir'/02_temp/01_code/checks
	capture mkdir `tmp_dir'/02_temp/02_logs
  
// Prep data on cases reported by WHO in WER 
  import delimited "`in_dir'/leprosy_cases_wer_2014_04_11.csv", clear
  
  replace mean = numerator / denominator
  
  duplicates list iso3 year_start
  
  // Create iso variable to allow national figures to merge onto subnational figures
    rename iso3 iso
    
	keep iso location_id sex year_start year_end mean
    tempfile lit_data_clean
    save `lit_data_clean', replace
    
    quietly summarize year_start
    local min_year = `r(min)'
    
// Save pops
	get_demographics, gbd_team(cod) clear
	local location_ids `r(location_ids)'
	local year_ids `r(year_ids)'
	local age_group_ids `r(age_group_ids)'
	local sex_ids `r(sex_ids)'
	get_population, year_id(`year_ids') location_id(`location_ids') sex_id(3) age_group_id(22) clear
	tempfile aggpops
	save `aggpops', replace
	get_population, year_id(`year_ids') location_id(`location_ids') sex_id(`sex_ids') age_group_id(`age_group_ids') clear
	rename population mean_pop
	tempfile pops
	save `pops', replace
    save "`tmp_dir'/pops.dta", replace
  
// Create template for country-years back to the earliest year for which data is available
  get_location_metadata, location_set_id(35) clear
  tempfile locs
  save `locs', replace
  gen iso = substr(ihme_loc_id,1,3)
  replace iso = ihme_loc_id if inlist(ihme_loc_id,"CHN_354","CHN_361")
  keep if most_detailed == 1
  keep location_id iso
  
  // Expand years back to earliest year for which data is reported
    local iter = 2016 - `min_year' + 1
    expand `iter'
    bysort location_id: generate int year_start = `min_year' + _n - 1
    
  tempfile iso_template
  save `iso_template', replace
  
  // Prep for later use
	use `locs', clear
	keep if most_detailed == 1 & is_estimate == 1
	keep location_id ihme_loc_id
	rename ihme_loc_id iso3
	levelsof location_id, local(all_locs)
	tempfile loc_iso3
	save `loc_iso3', replace
    save "`tmp_dir'/loc_iso3.dta", replace
  
// Merge data and template and prepare for merging onto age-specific data
  use `iso_template', replace
  merge m:1 iso year_start using `lit_data_clean', keep(1 3) nogen
  rename year_start year_id
  
  tempfile data_template
  save `data_template', replace


    generate has_year = year_id if !missing(mean)
    bysort location_id: egen min_year = min(has_year)
    bysort location_id: egen max_year = max(has_year)
    drop has_year
    

    bysort location_id: egen has_mean = sum(mean)
    replace mean = 0 if has_mean == 0
    drop has_mean
    

    generate has_data = !missing(mean)
    bysort location_id has_data (year_id): generate double roc = (mean[_n+1]/mean)^(1/(year_id[_n+1]-year_id))
    bysort location_id (year_id): replace roc = roc[_n-1] if missing(roc)
    bysort location_id (year_id): replace mean = mean[_n-1] * roc[_n-1] if missing(mean) & year_id < max_year & year_id > min_year
    replace mean = 0 if missing(mean) & year_id < max_year & year_id > min_year
    
    bysort location_id: egen one_obs = count(mean)
      replace one_obs = 0 if one_obs > 1
    
    replace mean = 0 if year_id > max_year & one_obs == 1
    replace mean = 0 if year_id < min_year & one_obs == 1
    

      preserve
        drop if missing(mean)
        bysort location_id (year_id): generate obs = _n
        keep if obs <= 5
        
        collapse (mean) mean, by(location_id)
        rename mean mu_first
        tempfile mu_first
        save `mu_first', replace
      restore
      
      preserve
        drop if missing(mean)
        bysort location_id (year_id): generate obs = _N - _n + 1
        keep if obs <= 3
        
        collapse (mean) mean, by(location_id)
        rename mean mu_last
        tempfile mu_last
        save `mu_last', replace
      restore
  
  
    merge m:1 location_id using `mu_first', keepusing(mu_first) nogen
    merge m:1 location_id using `mu_last', keepusing(mu_last) nogen

    replace mean = mu_last if year_id > max_year & one_obs == 0
    replace mean = mu_first if year_id < min_year & one_obs == 0
  
    generate age_group_id = 22
    generate sex_id = 3
    
    merge 1:1 location_id year_id age_group_id sex_id using `aggpops', keepusing(population) keep(master match) nogen
    rename population total_pop
    
  keep location_id year_id mean total_pop
  
  tempfile data_filled
  save `data_filled', replace
  save "`tmp_dir'/data_filled.dta", replace
	insheet using "`in_dir'/single_year_ages_to_age_group_id.csv", comma names clear
	levelsof age_group_id, local(sy_ages)
	tempfile sy_age_map
	save `sy_age_map', replace
    save "`tmp_dir'/sy_age_map.dta", replace
	
	insheet using "`in_dir'/ages_to_age_group_id.csv", comma names clear
	recast double age
	replace age = 0.01 if age > 0.009 & age < 0.011
	replace age = 0.1 if age > 0.09 & age < 0.11
	tempfile age_map
	save `age_map', replace
    save "`tmp_dir'/age_map.dta", replace
	
** ************************************
** ************************************
** GET POPULATIONS

capture confirm file "`tmp_dir'/sy_pop.csv"
if _rc {
	local a = 0
	// erase and make directory for finished checks
	! mkdir "`tmp_dir'/02_temp/01_code/checks"
	local datafiles: dir "`tmp_dir'/02_temp/01_code/checks" files "finished_loc*.txt"
	foreach datafile of local datafiles {
		rm "`tmp_dir'/02_temp/01_code/checks/`datafile'"
	}	

	foreach location of local all_locs {
		** submit job
		local job_name "loc`location'_`step_num'"
		di "submitting `job_name'"
		local slots = 4
		local mem = `slots' * 2

		! qsub -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" "`code_dir'/02_sy_pop.do" "`date' `step_num' `step_name' `location' `code_dir' `in_dir' `out_dir' `tmp_dir' `root_tmp_dir' `root_j_dir'"

		local ++ a
		sleep 50
	}

	local b = 0
	while `b' == 0 {
		local checks : dir "`tmp_dir'/02_temp/01_code/checks" files "finished_loc*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 60000
	}

// append together single year-age population files
	clear
	** set up empty file to append
	tempfile all_files
	save `all_files', replace emptyok

	** append each .dta in the tmp file
	local files: dir "`tmp_dir'/sy_pop" files "sy_pop_*.csv"
	foreach file of local files {
		import delimited "`tmp_dir'/sy_pop/`file'", clear
		append using `all_files'
		save `all_files', replace
	}

	keep age year pop iso3 sex
	destring year pop, replace
	tostring age, replace
	export delimited "`tmp_dir'/sy_pop.csv", replace
}

	capture confirm file "`tmp_dir'/grp_pop.csv"
	if _rc {
		numlist "1979/2016"
		local pop_years = "`r(numlist)'"
		get_population, year_id("`pop_years'") location_id(`all_locs') sex_id(1 2) age_group_id(`age_group_ids') clear
		merge m:1 age_group_id using `age_map', assert(3) keep(3) nogen
		merge m:1 location_id using `loc_iso3', assert(3) keep(3) nogen
		gen sex = "male" if sex_id == 1
		replace sex = "female" if sex_id == 2
		rename population pop
		rename year_id year
		merge m:1 location_id using `loc_iso3', nogen keep(3)

		keep age year pop iso3 sex
		export delimited "`tmp_dir'/grp_pop.csv", replace

	}
 
// ******************************************************************************************************
// ******************************************************************************************************


  	local a = 0
	// erase and make directory for finished checks
	! mkdir "`tmp_dir'/02_temp/01_code/checks"
	local datafiles: dir "`tmp_dir'/02_temp/01_code/checks" files "finished_loc*.txt"
	foreach datafile of local datafiles {
		rm "`tmp_dir'/02_temp/01_code/checks/`datafile'"
	}	

    foreach location_id of local all_locs {	
		!qsub -e FILENAME -o FILENAME -N "Leprosy_custom_model_lid_`location_id'" -P proj_custom_models -pe multi_slot 5 -l mem_free=10 "`shell_file'" "`code_dir'/`step_num'_`step_name'_1662_parallel.do" "`location_id' `min_year' `tmp_dir'"
		
		sleep 50
		local ++ a

    }

	local b = 0
	while `b' == 0 {
		local checks : dir "`tmp_dir'/02_temp/01_code/checks" files "finished_loc*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 60000
	}

	import excel "FILENAME/india_phfi_splits.xlsx", firstrow clear
	** get locations
	levelsof location_id, local(india_locs)
	** decide the locations we need to pull draws for
	use `locs', clear
	keep if is_estimate == 1 & most_detailed == 1
	gen keep = 0
	foreach loc of local india_locs {
		replace keep = 1 if location_id == `loc' | parent_id == `loc'
	}

	keep if keep == 1
	levelsof location_id, local(india_detailed)

	cap mkdir "`tmp_dir'/02_temp/02_logs"
	! mkdir "`tmp_dir'/02_temp/01_code/checks"
	local datafiles: dir "`tmp_dir'/02_temp/01_code/checks" files "finished_*.txt"
	foreach datafile of local datafiles {
		rm "`tmp_dir'/02_temp/01_code/checks/`datafile'"
	}	

	local a = 0
	foreach location of local india_detailed {
		** submit job
		local job_name "india_adj_`location'"
		di "submitting `job_name'"
		local slots = 4
		local mem = `slots' * 2

		! qsub -e FILENAME -o FILENAME/output -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" "`code_dir'/india_adjustment_parallel.do" "`location' `tmp_dir'"

		local ++ a
		sleep 100		

	}

	local b = 0
	while `b' == 0 {
		local checks : dir "`tmp_dir'/02_temp/01_code/checks" files "finished_*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 60000
	}

  // Run ODE solver (Python) by country-sex
    local python_dir "FILENAME"
      
    // Setup of arguments for ODE solver
      local has_mort 0
      local has_prev0 1
      local ages_dir "FILENAME"
      local code_dir_pyth "FILENAME"
      local pop_s_path "`tmp_dir'/sy_pop.csv"
      local pop_grp_path "`tmp_dir'/grp_pop.csv"
      local out_dir_prev "`tmp_dir'/draws/cases/final"
      local inc_dir "`tmp_dir'/draws/cases/inc_annual"
      local prev0_path "`tmp_dir'/draws/cases/prev_initial"
      local code "`python_dir'/inc_to_prev_annual.py"



	// Wait for results (check for the last file saved), then submit ODE script
	use `loc_iso3', clear
    foreach location_id of local all_locs {
		foreach sex in male female {
			quietly levelsof iso3 if location_id == `location_id', local(iso) c
				! qsub -e FILENAME/errors -o FILENAME/output -N leprosy_ODE_`iso'_`sex' -pe multi_slot 5 -l mem_free=10 -P proj_custom_models "`shell_file'" "`code_dir'/ode_solver_parallel.do" "`code' `iso' `sex' `has_mort' `has_prev0' `ages_dir' `code_dir_pyth' `pop_s_path' `pop_grp_path' `out_dir_prev' `inc_dir' `prev0_path'/prevalence_`iso'_1987_`sex'.csv"
				sleep 100

			 		}
	}
	
	// Now wait for those results (check for the last file saved)
	use `loc_iso3', clear
	foreach location_id of local all_locs {
		foreach sex in male female {
			quietly levelsof iso3 if location_id == `location_id', local(iso) c
			capture confirm file "`out_dir_prev'/prevalence_`iso'_2016_`sex'.csv"
			if _rc == 601 noisily display "Searching for `iso' -- `c(current_time)'"
			while _rc == 601 {
				capture confirm file "`out_dir_prev'/prevalence_`iso'_2016_`sex'.csv"
				sleep 100
			}
			if _rc == 0 {
				noisily display "`iso' FOUND!"
			}
		}
	}


*/
// ******************************************************************************************************
// ******************************************************************************************************

  insheet using "`in_dir'/leprosy_disability_2014_05_22.csv", double clear
  drop if inlist(data_status, "excluded", "issues")
  
  duplicates list healthstate iso3 sex year_start age_start
    
  keep healthstate iso3 year_start sex age_start age_end mean numerator denominator
  
  format %16.0g mean
  replace mean = numerator / denominator

  tempfile disability_data_clean
  save `disability_data_clean', replace
  
  
    use `disability_data_clean', replace
    keep if iso3 == "BRA"
    
    preserve
      drop mean
      reshape wide numerator, i(iso3 year_start sex age_start age_end denominator) j(healthstate) string
      generate healthstate = "disfigure_0"
      generate numerator = denominator - numeratordisfigure_1 - numeratordisfigure_2
      drop numeratordisfigure_*
      generate double mean = .
      format %16.0g mean
      replace mean = numerator / denominator
      tempfile gd0
      save `gd0', replace
    restore
    append using `gd0'
    assert mean < 1
    
    generate int disability = .
    replace disability = 0 if healthstate == "disfigure_0"
    replace disability = 1 if healthstate == "disfigure_1"
    replace disability = 2 if healthstate == "disfigure_2"
    
      tabulate age_start, gen(ac)
      tabulate sex, gen(sc)
      tabulate year, gen(yc)
      drop ac1 sc1 yc1
    
    
      keep if year >= 2001 & year <= 2006
      ologit disability ac* sc* yc* [fweight = numerator]
      ologit disability ac* sc* [fweight = numerator]
    
      net from FILENAME
      net install omodel
      
      omodel logit disability ac* sc* yc* [fweight = numerator]
      omodel logit disability ac* sc* [fweight = numerator]
    
    
      net from FILENAME
      net install gologit2
 
      gologit2 disability ac* sc* yc* [fweight = numerator], autofit force
      gologit2 disability ac* sc* [fweight = numerator], autofit force
      
    // Store regression results and produce thousand draws of proportions.
        matrix B = e(b)'
        matrix S = e(V)
        local covars: rownames B
        local num_covars: word count `covars'
        local betas
        forvalues j = 1/`num_covars' {
          local this_covar: word `j' of `covars'
          if `j' <= `num_covars' / 2 {
            local betas `betas' b0_`this_covar'
          }
          else {
            local betas `betas' b1_`this_covar'
          }
        }
        
        levelsof age_start, local(ages_pred)
        local num_ages_pred: word count `ages_pred'
        
        local num_ages: word count `age_group_ids'
        
        // Generate age covariate
		  clear
          set obs `num_ages'
          generate double age_pred = .
          generate double age_group_id = .

          forvalues i = 1/`num_ages' {
            local this_age: word `i' of `age_group_ids'
            quietly replace age_group_id = `this_age' if _n == `i'
          }
		  merge 1:1 age_group_id using "`tmp_dir'/age_map.dta", assert(2 3) keep(3) nogen
          recast double age_pred age
		  replace age = 0.1 if age > 0.09 & age < 0.11
          forvalues i = 1/`num_ages_pred' {
            local this_age: word `i' of `ages_pred'
            quietly replace age_pred = `this_age' if age == `this_age'
          }
          
          replace age_pred = age_pred[_n - 1] if missing(age_pred)
        
        // Generate sex covariate
          generate int sex = 1
          expand 2, generate(copy)
          replace sex = 2 if copy == 1
          drop copy
        
        // Create dummy variables
          tabulate age_pred, gen(ac)
          tabulate sex, gen(sc)
          drop ac1 sc1
        
        // Predict point-estimates (probabilities)
          predict mu0, outcome(0)
          predict mu1, outcome(1)
          predict mu2, outcome(2)
        
        // Create a thousand draws of predicted probabilities
          set obs 1000
          generate draw = _n - 1
          
          drawnorm `betas', means(B) cov(S)
          
          local counter 0
          forvalues j = 1/1000 {
          
            ** display in red `counter'
          
            forvalues o = 0/1 {  
              
              quietly generate double xb`o'_d`j' = 0
              format %16.0g xb`o'_d`j'
              
              quietly replace xb`o'_d`j' = xb`o'_d`j' + b`o'__cons[`j']

              foreach var of varlist ac* sc* {
                quietly replace xb`o'_d`j' = xb`o'_d`j' + `var' * b`o'_`var'[`j']
              }
              
            }  
          
            forvalues o = 0/1 {
              local gd = `o' + 1
              quietly replace xb`o'_d`j' = 1 / (1 + exp(-xb`o'_d`j'))
              quietly rename xb`o'_d`j' draw_gd`gd'_`counter'
            }
            
            forvalues gd = 1/2 {
              quietly replace draw_gd`gd'_`counter' = 0 if age < 0.05
            }
            
            quietly replace draw_gd1_`counter' = draw_gd1_`counter' - draw_gd2_`counter'
            
            local counter = `counter' + 1
            
          }
          
          fastrowmean draw_gd1_*, mean_var_name(mean_gd1)
          fastrowmean draw_gd2_*, mean_var_name(mean_gd2)
          tabstat mu* mean_gd*, by(age)
          

          forvalues j = 0/999 {
            quietly replace draw_gd1_`j' = draw_gd1_`j' / (1 - draw_gd2_`j')
            quietly label variable draw_gd1_`j' "G1D prev among new leprosy patients without G2D"
            quietly label variable draw_gd2_`j' "G2D prev among all new leprosy patients"
          }
          
        keep if !missing(age)
        drop age age_pred ac* sc* draw b* mu* mean_gd*
		rename sex sex_id
        
        tempfile gd_prop_draws
        save `gd_prop_draws', replace
        save "`tmp_dir'/gd_prop_draws.dta", replace
   
// ******************************************************************************************************
// ******************************************************************************************************
// Split incident leprosy cases into incident cases of grade 1 and 2 disability
    insheet using "`in_dir'/leprosy_disability_2014_05_22.csv", double clear
    keep if healthstate == "disfigure_2"
    keep if age_start < 0.11 & age_start > 0.09 & age_end == 99 & sex == 3
    keep iso3 year_start sex age_start age_end mean numerator denominator
	
    duplicates list iso3 year_start
    
    rename iso3 iso
    
    tempfile g2d_data_clean
    save `g2d_data_clean', replace
    
    use `iso_template', clear
    merge m:1 iso year_start using `g2d_data_clean', nogen
    sort location_id year_start
    
    keep location_id year_start mean numerator denominator
    rename year_start year_id
    
    save `g2d_data_clean', replace
    
      use `g2d_data_clean', clear
    
        replace mean = . if denominator == 0 | (missing(numerator) & missing(mean))
        replace numerator = . if denominator == 0 | (missing(numerator) & missing(mean))
        replace denominator = . if denominator == 0 | (missing(numerator) & missing(mean))
      
        generate has_year = year_id if !missing(mean)
        bysort location_id: egen min_year = min(has_year)
        bysort location_id: egen max_year = max(has_year)
        drop has_year
        

        generate has_data = !missing(mean)
        bysort location_id has_data (year_id): generate double roc = (mean[_n+1]/mean)^(1/(year_id[_n+1]-year_id))
        bysort location_id (year_id): replace roc = roc[_n-1] if missing(roc)
        bysort location_id (year_id): replace mean = mean[_n-1] * roc[_n-1] if missing(mean) & year_id < max_year & year_id > min_year
        replace mean = 0 if missing(mean) & year_id < max_year & year_id > min_year
        gsort location_id -year_id
        bysort location_id: replace denominator = denominator[_n-1] if missing(denominator) & year_id < max_year & year_id > min_year
        sort location_id year_id
        replace numerator = denominator * mean if missing(numerator)
        drop roc
        
          preserve
            drop if missing(mean)
            bysort location_id (year_id): generate obs = _n
            keep if obs <= 5
            
            collapse (mean) numerator denominator, by(location_id)
            generate double mu_first = numerator / denominator
            rename numerator num_first
            rename denominator denom_first
            tempfile mu_first
            save `mu_first', replace
          restore
          
          preserve
            drop if missing(mean)
            bysort location_id (year_id): generate obs = _N - _n + 1
            keep if obs <= 3
            
            collapse (mean) numerator denominator, by(location_id)
            generate double mu_last = numerator / denominator
            rename numerator num_last
            rename denominator denom_last
            tempfile mu_last
            save `mu_last', replace
          restore
      
      
        merge m:1 location_id using `mu_first', keepusing(mu_first num_first denom_first) nogen
        merge m:1 location_id using `mu_last', keepusing(mu_last num_last denom_last) nogen

        replace mean = mu_last if year_id > max_year
        replace numerator = num_last if year_id > max_year
        replace denominator = denom_last if year_id > max_year
        
        replace mean = mu_first if year_id < min_year
        replace numerator = num_first if year_id < min_year
        replace denominator = denom_first if year_id < min_year
      

        quietly summarize numerator
        replace numerator = r(mean) if missing(numerator)
        quietly summarize denominator
        replace denominator = r(mean) if missing(denominator)
        
        replace mean = numerator / denominator if missing(mean)
      

        quietly generate a = .
        quietly generate b = .
        forvalues i = 0/999 {
          quietly replace a = rgamma((numerator + 0.5),1) if denominator > 0
          quietly replace b = rgamma((denominator - numerator + 0.5),1) if denominator > 0
          quietly generate double env_`i' = a / (a + b) if denominator > 0

        }
        drop a b
      
      keep location_id year_id env* 
      save "`tmp_dir'/g2d_env_draws.dta", replace
      *****************************



    
	use `loc_iso3', clear
    foreach location_id of local all_locs {
		levelsof iso3 if location_id == `location_id', local(iso) c
			!qsub -N "Leprosy_custom_model_step2_lid_`location_id'" -e FILENAME -o FILENAME -P proj_custom_models -pe multi_slot 5 -l mem_free=10 "`shell_file'" "`code_dir'/`step_num'_`step_name'_1663_1664_parallel.do" "`location_id' `min_year' `tmp_dir'"
			sleep 50
    }
    
// ******************************************************************************************************
// ******************************************************************************************************
  // Run ODE solver (Python) by country-sex
    local python_dir "FILENAME"
      
    // Setup of arguments for ODE solver
      local has_mort 0
      local has_prev0 1
      local ages_dir "FILENAME"
      local code_dir_pyth "FILENAME"
      local pop_s_path "`tmp_dir'/sy_pop.csv"
      local pop_grp_path "`tmp_dir'/grp_pop.csv"
      local out_dir_prev "`tmp_dir'/draws/disfigure_2/final"
      local inc_dir "`tmp_dir'/draws/disfigure_2/final"
      local prev0_path "`tmp_dir'/draws/disfigure_2/prev_initial"
      local code "`python_dir'/inc_to_prev_annual.py"
	
	// Wait for results (check for the last file saved), then submit ODE script
	use `loc_iso3', clear
    foreach location_id of local all_locs {
		foreach sex in male female {
			quietly levelsof iso3 if location_id == `location_id', local(iso) c
			capture confirm file "`inc_dir'/incidence_`iso'_2016_`sex'.csv"
			if _rc == 601 noisily display "Searching for `iso' -- `c(current_time)'"
			while _rc == 601 {
				capture confirm file "`inc_dir'/incidence_`iso'_2016_`sex'.csv"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "`iso' FOUND!"
			}		
			** LAUNCH ODE SOLVER
			  capture confirm file  "`out_dir_prev'/prevalence_`iso'_2016_`sex'.csv"
			  if _rc {
				! qsub -e FILENAME -o FILENAME -N G2D_`iso'_`sex' -pe multi_slot 5 -l mem_free=10 -P proj_custom_models "`shell_file'" "`code_dir'/ode_solver_parallel.do" "`code' `iso' `sex' `has_mort' `has_prev0' `ages_dir' `code_dir_pyth' `pop_s_path' `pop_grp_path' `out_dir_prev' `inc_dir' `prev0_path'/prevalence_`iso'_`min_year'_`sex'.csv"
				
				sleep 50
			  }
			  
		}
	}
    
	// Now wait for those results (check for the last file saved)
	use `loc_iso3', clear
	foreach location_id of local all_locs {
		foreach sex in male female {
			quietly levelsof iso3 if location_id == `location_id', local(iso) c
			capture confirm file "`out_dir_prev'/prevalence_`iso'_2016_`sex'.csv"
			if _rc == 601 noisily display "Searching for `iso' -- `c(current_time)'"
			while _rc == 601 {
				capture confirm file "`out_dir_prev'/prevalence_`iso'_2016_`sex'.csv"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "`iso' FOUND!"
			}
		}
	}

  
// Format draws and compile in appropriate directories
    foreach location_id of local all_locs {	
		capture confirm file "`tmp_dir'/draws/upload/1664/5_`location_id'_2016_2.csv"
		if _rc {
			!qsub -N "Leprosy_custom_model_format_lid_`location_id'" -P proj_custom_models -pe multi_slot 2 -l mem_free=4 "`shell_file'" "`code_dir'/`step_num'_`step_name'_format_parallel.do" "`location_id' `min_year' `tmp_dir'"
			
			sleep 50
		}
    }
  
** Check for final output
	foreach location_id of local all_locs {
		foreach meid in 1662 1663 1664 {
			capture confirm file "`tmp_dir'/draws/upload/`meid'/`location_id'.csv"
			if _rc == 601 noisily display "Searching for /`meid'/`location_id'.csv  -- `c(current_time)'"
			while _rc == 601 {
				capture confirm file "`tmp_dir'/draws/upload/`meid'/`location_id'.csv"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "`location_id' FOUND!"
			}
		}
	}
  
	qui run "FILENAME/save_results.do"

local model 154862

** Upload
  save_results, modelable_entity_id(1662) description("Prevalence and incidence of cases who ever had leprosy, based on dismod model `model'") in_dir("`tmp_dir'/draws/upload/1662") mark_best("yes") env("prod") file_pattern({location_id}.csv)
  
  save_results, modelable_entity_id(1663) description("Prevalence and incidence of grade 1 disability, based on dismod model `model'") in_dir("`tmp_dir'/draws/upload/1663") mark_best("yes") env("prod") file_pattern({location_id}.csv)

  save_results, modelable_entity_id(1664) description("Prevalence and incidence of grade 2 disability, based on dismod model `model'") in_dir("`tmp_dir'/draws/upload/1664") mark_best("yes") env("prod") file_pattern({location_id}.csv)


// **********************************************************************


	// write check file to indicate step has finished
		file open finished using "`out_dir'/finished.txt", replace write
		file close finished
		
	// if step is last step, write finished.txt file
		local i_last_step 0
		foreach i of local last_steps {
			if "`i'" == "`this_step'" local i_last_step 1
		}
		
		// only write this file if this is one of the last steps
		if `i_last_step' {
		
			// account for the fact that last steps may be parallel and don't want to write file before all steps are done
			local num_last_steps = wordcount("`last_steps'")
			
			// if only one last step
			local write_file 1
			
			// if parallel last steps
			if `num_last_steps' > 1 {
				foreach i of local last_steps {
					local dir: dir "FILENAME" dirs "`i'_*", respectcase
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file "FILENAME/finished.txt"
					if _rc local write_file 0
				}
			}
			
			// write file if all steps finished
			if `write_file' {
				file open all_finished using "FILENAME/finished.txt", replace write
				file close all_finished
			}
		}
		
	// close log if open
		if `close_log' log close
	