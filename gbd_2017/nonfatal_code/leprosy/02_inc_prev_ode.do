// **********************************************************************
// Purpose:        This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
// Description:   Calculate incidence of leprosy by country-year-age-sex, using cases reported to WHO and
//                      age-patterns from dismod. Produce incidence for every year in 1890-2016, and sweep forward
//                      with ODE to arrive at prevalence predictions.
// FILEPATH/02_inc_prev_ode.do
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION)

	// prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
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
	else if "`1'" == "" {
		// base directory on J
		local root_j_dir "FILEPATH"
		// base directory on share
		local root_tmp_dir "FILEPATH"
		// timestamp of current run (i.e. 2014_01_17)
		local date: display %tdCCYY_NN_DD date(c(current_date), "DMY")
		local date = subinstr("`date'"," ","_",.)
		// step number of this step (i.e. 01a)
		local step_num "02"
		// name of current step (i.e. first_step_name)
		local step_name "inc_prev_ode"
		// step numbers of immediately anterior parent step (i.e. for step 2: 01a 01b 01c)
		local hold_steps ""
		// step numbers for final steps that you are running in the current run (i.e. 11a 11b 11c)
		local last_steps ""
		// directory where the code lives
		local code_dir "FILEPATH"
	}
	// directory for external inputs
	local in_dir "FILEPATH"
	// directory for output on the J drive
	local out_dir "FILEPATH"
	// directory for output on share
	capture mkdir "FILEPATH"
	local tmp_dir "FILEPATH"
	capture mkdir "FILEPATH"
	capture mkdir "FILEPATH"

	// set adopath
	adopath + "FILEPATH"

	// set shell file
	local shell_file "FILEPATH/stata_shell.sh"

	// write log if running in parallel and log is not already open
	cap log using "FILEPATH/`step_num'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// check for finished.txt produced by previous step
	if "`hold_steps'" != "" {
		foreach step of local hold_steps {
			local dir: dir "FILEPATH" dirs "`step'_*", respectcase
			// remove extra quotation marks
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
	**qui run "FILEPATH/save_results.do"

// Prep data on cases reported by WHO in WER (this is a manually selected subset of the
// data that went into DisMod, ie data that pertains to total number of cases per country-year).
  import delimited "`in_dir'/leprosy_cases_wer_2017_10_06.csv", clear
  * pull in population estimates from this round of population
  levelsof location_id, local(locs)
  levelsof year_start, local(years)
  preserve
  get_population, location_id(`locs') sex_id(3) year_id(`years') clear
  drop age_group_id sex_id run_id
  rename year_id year_start
  tempfile pops
  save `pops', replace
  restore
  merge 1:1 location_id year_start using `pops', keep(3) nogen
  replace denominator = population


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
	get_demographics, gbd_team(ADDRESS) clear
	local location_ids `r(location_id)'
	local year_ids `r(year_id)'
	local age_group_ids `r(age_group_id)'
	local sex_ids `r(sex_id)'
	get_population, year_id(`year_ids') location_id(`location_ids') sex_id(3) age_group_id(22) clear
	tempfile aggpops
	save `aggpops', replace
	get_population, year_id(`year_ids') location_id(`location_ids') sex_id(`sex_ids') age_group_id(`age_group_ids') clear
	rename population mean_pop
	tempfile pops
	save `pops', replace
    save "`tmp_dir'/pops.dta", replace

// Create template for country-years back to the earliest year for which data is available
** is there a reason why this is in iso instead of ihme_loc_id?
  get_location_metadata, location_set_id(35) clear
  tempfile locs
  save `locs', replace
  gen iso = substr(ihme_loc_id,1,3)
  replace iso = ihme_loc_id if inlist(ihme_loc_id,"CHN_354","CHN_361")
  keep if most_detailed == 1
  keep location_id iso

  // Expand years back to earliest year for which data is reported
    local iter = 2017 - `min_year' + 1
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


// Fill blanks in data
  // By country, determine first and last year for which observations are available
    generate has_year = year_id if !missing(mean)
    bysort location_id: egen min_year = min(has_year)
    bysort location_id: egen max_year = max(has_year)
    drop has_year

  // If no cases reported for any years, set incidence to zero
    bysort location_id: egen has_mean = sum(mean)
    replace mean = 0 if has_mean == 0
    drop has_mean

  // For countries with blanks between non-missing observations, apply exponential interpolation
  // (if any of two consecutive data points is zero, any gaps for years in between will remain missing
  // after exponential interpolation; we assume those data points are zeroes)
    generate has_data = !missing(mean)
    bysort location_id has_data (year_id): generate double roc = (mean[_n+1]/mean)^(1/(year_id[_n+1]-year_id))
    bysort location_id (year_id): replace roc = roc[_n-1] if missing(roc)
    bysort location_id (year_id): replace mean = mean[_n-1] * roc[_n-1] if missing(mean) & year_id < max_year & year_id > min_year
    replace mean = 0 if missing(mean) & year_id < max_year & year_id > min_year

  // For countries with single observations, assume zero incidence for other years
    bysort location_id: egen one_obs = count(mean)
      replace one_obs = 0 if one_obs > 1
    ** bysort location_id: egen mu_only = max(mean) if one_obs == 1  // in case this needs to be carried forward or backward

    replace mean = 0 if year_id > max_year & one_obs == 1
    replace mean = 0 if year_id < min_year & one_obs == 1

  // For countries with more than one observation, project earliest incidence backward into time
  // and latest incidence forward into time.
    // Calculate mean incidence over earliest five years with data/interpolations
      preserve
        drop if missing(mean)
        bysort location_id (year_id): generate obs = _n
        keep if obs <= 5

        collapse (mean) mean, by(location_id)
        rename mean mu_first
        tempfile mu_first
        save `mu_first', replace
      restore

    // Calculate mean incidence over latest three years with data/interpolations
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

  // Fill denominators
    generate age_group_id = 22
    generate sex_id = 3

    merge 1:1 location_id year_id age_group_id sex_id using `aggpops', keepusing(population) keep(master match) nogen
    rename population total_pop

  keep location_id year_id mean total_pop

  tempfile data_filled
  save `data_filled', replace
  save "`tmp_dir'/data_filled.dta", replace
// Prep ages so we can use ODE
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
    ** check how this file was created last year, need to remove string portions

** ************************************
** ************************************
** GET POPULATIONS

** see if we already have the appended single year population file to save time
capture confirm file "`tmp_dir'/sy_pop.csv"
if _rc {
** going to paralellize this part to speed things up
	local a = 0
	** erase and make directory for finished checks
	! mkdir "FILEPATH"
	local datafiles: dir "FILEPATH" files "finished_loc*.txt"
	foreach datafile of local datafiles {
		rm "FILEPATH"
	}

	numlist "1970/2017"
	local years "`r(numlist)'"
	local sexes 1 2
	foreach location of local all_locs {
		** submit job
		local job_name "loc`location'_`step_num'"
		di "submitting `job_name'"
		** di in red "location is `location'" // for tests
		local slots = 4
		local mem = `slots' * 2

			di "submitting `location'"
			! qsub -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" "`code_dir'/02_sy_pop.do" "`date' `step_num' `step_name' `location' `code_dir' `in_dir' `out_dir' `tmp_dir' `root_tmp_dir' `root_j_dir'"

			local ++ a
			sleep 50

	}

** wait for jobs to finish ebefore passing execution back to main step file
	local b = 0
	while `b' == 0 {
		local checks : dir "FILEPATH" files "finished_loc*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 60000
	}

** append together single year-age population files
	clear
	** set up empty file to append
	tempfile all_files
	save `all_files', replace emptyok

	** append each .dta in the tmp file
	local files: dir "FILEPATH" files "sy_pop_*.csv"
	foreach file of local files {
		import delimited "FILEPATH", clear
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
		numlist "1979/2017"
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
// Merge incidence pattern with annually reported total incidence and population envelope and calculate year-specific incidences by scaling
// by [absolute reported numbers per year] / [sum of mean of all draws by country-year]. Save files for 1987-2016

// Calculate prevalent cases that have ever had leprosy
  // Get Incidence and min year prevalence

  	local a = 0
	// erase and make directory for finished checks
	! mkdir "FILEPATH"
	local datafiles: dir "FILEPATH" files "finished_loc*.txt"
	foreach datafile of local datafiles {
		rm "FILEPATH"
	}

	** use `loc_iso3', clear
    foreach location_id of local all_locs {
    	use "`tmp_dir'/loc_iso3.dta", clear
  		keep if location_id == `location_id'
 		levelsof iso3, local(iso) c
 		local iso "`iso'"

    	*capture confirm file "FILEPATH/prevalence_`iso'_1987_female.csv"
    	*if _rc {
    		!qsub -e FILEPATH -o FILEPATH -N "Leprosy_custom_model_lid_`location_id'" -P proj_custom_models -pe multi_slot 5 -l mem_free=10 "`shell_file'" "`code_dir'/`step_num'_`step_name'_1662_parallel.do" "`location_id' `min_year' `tmp_dir'"

			sleep 50
			local ++ a
    	*}
    }

 	// wait for jobs to finish before passing execution back to main step file
	local b = 0
	while `b' == 0 {
		local checks : dir "FILEPATH" files "finished_loc*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 60000
	}

	** INDIA UPDATE HERE
	/*
	** pull in data
	import excel "FILEPATH/india_phfi_splits.xlsx", firstrow clear
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

	cap mkdir "FILEPATH"
	! mkdir "FILEPATH"
	local datafiles: dir "FILEPATH" files "finished_*.txt"
	foreach datafile of local datafiles {
		rm "FILEPATH"
	}

	local a = 0
	foreach location of local india_detailed {
		** submit job
		local job_name "india_adj_`location'"
		di "submitting `job_name'"
		** di in red "location is `location'" // for tests
		local slots = 4
		local mem = `slots' * 2

		! qsub -e FILEPATH -o FILEPATH -P proj_custom_models -N "`job_name'" -pe multi_slot `slots' -l mem_free=`mem' "`shell_file'" "`code_dir'/india_adjustment_parallel.do" "`location' `tmp_dir'"

		local ++ a
		sleep 100

	}

	local b = 0
	while `b' == 0 {
		local checks : dir "FILEPATH" files "finished_*.txt", respectcase
		local count : word count `checks'
		di "checking `c(current_time)': `count' of `a' jobs finished"
		if (`count' == `a') continue, break
		else sleep 60000
	}
*/
  // Run ODE solver (Python) by country-sex
    local python_dir "FILEPATH"

    // Setup of arguments for ODE solver
      local has_mort 0
      local has_prev0 1
      local ages_dir "FILEPATH"
      local code_dir_pyth "FILEPATH"
      local pop_s_path "`tmp_dir'/sy_pop.csv"
      local pop_grp_path "`tmp_dir'/grp_pop.csv"
      local out_dir_prev "`tmp_dir'FILEPATH"
      local inc_dir "`tmp_dir'FILEPATH"
      local prev0_path "`tmp_dir'FILEPATH"
      local code "`python_dir'/inc_to_prev_annual.py"

	// Wait for results (check for the last file saved), then submit ODE script
	use `loc_iso3', clear
    foreach location_id of local all_locs {
		foreach sex in male female {
			quietly levelsof iso3 if location_id == `location_id', local(iso) c
			capture confirm file "`out_dir_prev'/prevalence_`iso'_2017_`sex'.csv"
			if _rc {
				! qsub -e FILEPATH -o FILEPATH -N leprosy_ODE_`iso'_`sex' -pe multi_slot 5 -l mem_free=10 -P proj_custom_models "`shell_file'" "`code_dir'/ode_solver_parallel.do" "`code' `iso' `sex' `has_mort' `has_prev0' `ages_dir' `code_dir_pyth' `pop_s_path' `pop_grp_path' `out_dir_prev' `inc_dir' `prev0_path'/prevalence_`iso'_1987_`sex'.csv"
				sleep 100
			}
		}
	}

	// Now wait for those results (check for the last file saved)
	use `loc_iso3', clear
	foreach location_id of local all_locs {
		foreach sex in male female {
			quietly levelsof iso3 if location_id == `location_id', local(iso) c
			capture confirm file "`out_dir_prev'/prevalence_`iso'_2017_`sex'.csv"
			if _rc == 601 noisily display "Searching for `iso' -- `c(current_time)'"
			while _rc == 601 {
				capture confirm file "`out_dir_prev'/prevalence_`iso'_2017_`sex'.csv"
				sleep 100
			}
			if _rc == 0 {
				noisily display "`iso' Found."
			}
		}
	}

*/
// ******************************************************************************************************
// ******************************************************************************************************

// Model prevalence of grade 1 and grade 2 disability due to leprosy in Brazil
  insheet using "`in_dir'/leprosy_disability_2014_05_22.csv", double clear
  drop if inlist(data_status, "excluded", "issues")

  duplicates list healthstate iso3 sex year_start age_start

  keep healthstate iso3 year_start sex age_start age_end mean numerator denominator

  format %16.0g mean
  replace mean = numerator / denominator

  tempfile disability_data_clean
  save `disability_data_clean', replace


  // Regression of prevalence of grade 1 and 2 disability among incidence leprosy cases in Brazil, by age and sex
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

    // Create dummy variables (i. prefix not allowed) and drop dummies for reference categories
      tabulate age_start, gen(ac)
      tabulate sex, gen(sc)
      tabulate year, gen(yc)
      drop ac1 sc1 yc1


    // Ordered logistic regression
    	** years for which we have data for all age categories
      keep if year >= 2001 & year <= 2006
      ologit disability ac* sc* yc* [fweight = numerator]
      ologit disability ac* sc* [fweight = numerator]

    // Show that proportional odds assumption is violated (likelihood-ratio test = significant)
      net from http://fmwww.bc.edu/RePEc/bocode/o
      net install omodel

      omodel logit disability ac* sc* yc* [fweight = numerator]
      omodel logit disability ac* sc* [fweight = numerator]


    // Generalized ordered logistic regression (cut-offs may vary with independent variables)
      net from http://fmwww.bc.edu/RePEc/bocode/g
      net install gologit2

      gologit2 disability ac* sc* yc* [fweight = numerator], autofit force
      gologit2 disability ac* sc* [fweight = numerator], autofit force

    // Store regression results and produce thousand draws of proportions.
    // Assume that proportions are homogeneous within age categories
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

          ** For each level of the outcome, create a linear predictor
            forvalues o = 0/1 {

              quietly generate double xb`o'_d`j' = 0
              format %16.0g xb`o'_d`j'

              quietly replace xb`o'_d`j' = xb`o'_d`j' + b`o'__cons[`j']

              foreach var of varlist ac* sc* {
                quietly replace xb`o'_d`j' = xb`o'_d`j' + `var' * b`o'_`var'[`j']
              }

            }

          ** Translate linear predictors to prevalence of grade 1 and 2 disability among incident leprosy cases
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

        // Translate overall prevalence of grade 1 disability to prevalence of grade 1 disability among persons
        // without grade 2 disability (so we make sure that later, the sum of G2D, G1D and asymptomatic
        // cases does not exceed one).
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
  // Prep data of overall prevalence of G2D among incident leprosy cases
    insheet using "`in_dir'/leprosy_disability_2017_11_02.csv", double clear
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

    // Fill blanks in data and create 1K draws of the envelope for G2D prevalence among incidence leprosy patients
      use `g2d_data_clean', clear

      // If no leprosy cases reported for any years or a proportion or numerator is missing,
      // set all data to missing (there is no information)
        replace mean = . if denominator == 0 | (missing(numerator) & missing(mean))
        replace numerator = . if denominator == 0 | (missing(numerator) & missing(mean))
        replace denominator = . if denominator == 0 | (missing(numerator) & missing(mean))

      // By country, determine first and last year for which observations are available
        generate has_year = year_id if !missing(mean)
        bysort location_id: egen min_year = min(has_year)
        bysort location_id: egen max_year = max(has_year)
        drop has_year

      // For countries with blanks between non-missing observations, apply exponential interpolation
      // (if any of two consecutive data points is zero, any gaps for years in between will remain missing
      // after exponential interpolation; we assume those data points are zeroes and carry backward the
      // denominator)
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

      // Project earliest and latest proportions backward and forward into time.
        // Calculate mean incidence over earliest five years with data/interpolations
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

        // Calculate mean incidence over latest three years with data/interpolations
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

      // In countries with no data at all (and most likely, very few cases), assume that the prevalence
      // of G2D among incident leprosy cases is the average of the global pattern.
        quietly summarize numerator
        replace numerator = r(mean) if missing(numerator)
        quietly summarize denominator
        replace denominator = r(mean) if missing(denominator)

        replace mean = numerator / denominator if missing(mean)

      // Create 1K draws of proportions, based on analytical solution of the probability of proportions, given the data
      // and a prior for the unknown proportion (Beta(0.5,0.5)). Solution = Beta(positives+0.5, negatives+0.5).
        quietly generate a = .
        quietly generate b = .
        forvalues i = 0/999 {
          quietly replace a = rgamma((numerator + 0.5),1) if denominator > 0
          quietly replace b = rgamma((denominator - numerator + 0.5),1) if denominator > 0
          quietly generate double env_`i' = a / (a + b) if denominator > 0
          ** The above is the same as using Stata's random number generator for the beta distribution, but allows larger
          ** values of parameters (rbeta returns missing values for India because of large number of cases):
          ** quietly generate double env_`i' = rbeta(numerator+0.5,(denominator-numerator)+0.5) if denominator > 0
        }
        drop a b

      keep location_id year_id env*
      save "`tmp_dir'/g2d_env_draws.dta", replace
      *****************************

  // By country-year (1987-2015), take leprosy incidence draws and split of the proportion of incident cases of
  // G2D and G1D, taking into account uncertainty in the age-sex-split, as well as the uncertainty in the overall proportion
  // of G2D cases among incident leprosy cases (i.e. the G2D envelope). G1D is assigned only to cases without G2D.


  // Serial submission
	use `loc_iso3', clear
    foreach location_id of local all_locs {
		levelsof iso3 if location_id == `location_id', local(iso) c
		** check to see if file already exists
		capture confirm file "FILEPATH/incidence_`iso'_2017_female.csv"
		if _rc == 601 {
			!qsub -N "Leprosy_custom_model_step2_lid_`location_id'" -e FILEPATH -o FILEPATH -P proj_custom_models -pe multi_slot 5 -l mem_free=10 "`shell_file'" "`code_dir'/`step_num'_`step_name'_1663_1664_parallel.do" "`location_id' `min_year' `tmp_dir'"
			sleep 50
		}
		else {
			noisily display "`iso' Found."
		}
    }

// ******************************************************************************************************
// ******************************************************************************************************
  // Run ODE solver (Python) by country-sex
    local python_dir "FILEPATH"

    // Setup of arguments for ODE solver
      local has_mort 0
      local has_prev0 1
      local ages_dir "FILEPATH"
      local code_dir_pyth "FILEPATH"
      local pop_s_path "`tmp_dir'/sy_pop.csv"
      local pop_grp_path "`tmp_dir'/grp_pop.csv"
      local out_dir_prev "FILEPATH/disfigure_2/final"
      local inc_dir "FILEPATH/disfigure_2/final"
      local prev0_path "FILEPATH/disfigure_2/prev_initial"
      local code "`python_dir'/inc_to_prev_annual.py"

	// Wait for results (check for the last file saved), then submit ODE script
	use `loc_iso3', clear
    foreach location_id of local all_locs {
		foreach sex in male female {
			quietly levelsof iso3 if location_id == `location_id', local(iso) c
			capture confirm file "`inc_dir'/incidence_`iso'_2017_`sex'.csv"
			if _rc == 601 noisily display "Searching for `iso' -- `c(current_time)'"
			while _rc == 601 {
				capture confirm file "`inc_dir'/incidence_`iso'_2017_`sex'.csv"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "`iso' FOUND!"
			}
			** LAUNCH ODE SOLVER
			  capture confirm file  "`out_dir_prev'/prevalence_`iso'_2017_`sex'.csv"
			  if _rc {
				! qsub -e FILEPATH -o FILEPATH -N G2D_`iso'_`sex' -pe multi_slot 5 -l mem_free=10 -P proj_custom_models "`shell_file'" "`code_dir'/ode_solver_parallel.do" "`code' `iso' `sex' `has_mort' `has_prev0' `ages_dir' `code_dir_pyth' `pop_s_path' `pop_grp_path' `out_dir_prev' `inc_dir' `prev0_path'/prevalence_`iso'_`min_year'_`sex'.csv"

				sleep 50
			  }
			  ** !FILEPATH `python_dir'/inc_to_prev.py `iso' `sex' `has_mort' `has_prev0' `ages_dir' `code_dir_pyth' `pop_s_path' `pop_grp_path' `out_dir_prev' `inc_dir' `prev0_path'/prevalence_`iso'_`min_year'_`sex'.csv

		}
	}

	// Now wait for those results (check for the last file saved)
	use `loc_iso3', clear
	foreach location_id of local all_locs {
		foreach sex in male female {
			quietly levelsof iso3 if location_id == `location_id', local(iso) c
			capture confirm file "`out_dir_prev'/prevalence_`iso'_2017_`sex'.csv"
			if _rc == 601 noisily display "Searching for `iso' -- `c(current_time)'"
			while _rc == 601 {
				capture confirm file "`out_dir_prev'/prevalence_`iso'_2017_`sex'.csv"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "`iso' Found."
			}
		}
	}


// Format draws and compile in appropriate directories
    foreach location_id of local all_locs {
		capture confirm file "FILEPATH/`location_id'.csv"
		if _rc {
			!qsub -N "Leprosy_custom_model_format_lid_`location_id'" -P proj_custom_models -pe multi_slot 2 -l mem_free=4 "`shell_file'" "`code_dir'/`step_num'_`step_name'_format_parallel.do" "`location_id' `min_year' `tmp_dir'"

			sleep 50
		}
    }

** Check for final output
	foreach location_id of local all_locs {
		foreach meid in 1662 1663 1664 {
			capture confirm file "FILEPATH/`location_id'.csv"
			if _rc == 601 noisily display "Searching for FILEPATH/`location_id'.csv  -- `c(current_time)'"
			while _rc == 601 {
				capture confirm file "FILEPATH/`location_id'.csv"
				sleep 1000
			}
			if _rc == 0 {
				noisily display "`location_id' Found."
			}
		}
	}

	** qui run "FILEPATH/save_results_epi.do"

local model 325769

** Upload
  save_results_epi, modelable_entity_id(1662) description("Prevalence and incidence of cases who ever had leprosy, based on dismod model `model'") input_dir("`tmp_dir'/draws/upload/1662") mark_best(TRUE) db_env("prod") input_file_pattern({location_id}.csv) clear measure_id(5 6)

  save_results_epi, modelable_entity_id(1663) description("Prevalence and incidence of grade 1 disability, based on dismod model `model'") input_dir("`tmp_dir'/draws/upload/1663") mark_best(TRUE) db_env("prod") input_file_pattern({location_id}.csv) clear measure_id(5 6)

  save_results_epi, modelable_entity_id(1664) description("Prevalence and incidence of grade 2 disability, based on dismod model `model'") input_dir("`tmp_dir'/draws/upload/1664") mark_best(TRUE) db_env("prod") input_file_pattern({location_id}.csv) clear measure_id(5 6)


// **********************************************************************
// CHECK FILES (NO NEED TO EDIT THIS SECTION)

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
					local dir: dir "FILEPATH
					local dir = subinstr(`"`dir'"',`"""',"",.)
					cap confirm file "FILEPATH/finished.txt"
					if _rc local write_file 0
				}
			}

			// write file if all steps finished
			if `write_file' {
				file open all_finished using "FILEPATH/finished.txt", replace write
				file close all_finished
			}
		}

	// close log if open
		if `close_log' log close
