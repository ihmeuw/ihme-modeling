/// AUTHOR NAME
/// November 2017
/// GBD 2017: negative binomial regression for iodine deficiency


// Settings
			// Clear memory and set memory and variable limits
				clear all
				set mem 5G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define drive for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "FILEPATH"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "FILEPATH"
				}
			
			// Close any open log file
				cap log close

** ********************************************************************************************************
// function needed
adopath + "$prefix/FILEPATH"
adopath + "$prefix/FILEPATH/create_connection_string.ado"
qui do "$prefix/FILEPATH/save_results_cod.ado"
qui do "$prefix/FILEPATH/save_results_epi.ado"

// locals
	
	local gbd_round GBD2017
	local acause nutrition_iodine
	local age_group_list 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235
	local sex_list 1 2
	local custom_version VERSION NUMBER 
	/* run model using national level data only, drop cf>99.9th pctile*/
	
** *********************************************************************************************************

// define filepaths
	local outdir "$prefix/FILEPATH"
	
// pull data for regression and save
			
		create_connection_string, server("ADDRESS") database("DATABASE") user("USERNAME") password("PASSWORD")
			local conn_string = r(conn_string)
			#delimit;
			odbc load, exec("SELECT location_id, location_name, age_group_id, year_id AS year, sex_id AS sex, acause, sample_size, cf_corr, cf_rd, nid FROM DATABASE INNER JOIN DATABASE using (data_version_id) INNER JOIN DATABASE using (location_id) INNER JOIN DATABASE using (cause_id) WHERE status = 1 and acause IN ('nutrition_iodine') GROUP BY location_id, location_name, year_id, sex_id, acause, sample_size, cf_corr, cf_rd, nid") `conn_string' clear
			#delimit cr
	
		export delim using "$prefix/FILEPATH/`acause'_`gbd_round'.csv", replace


	//generate year list 
clear 	
set obs 38
gen year = _n+1979
levelsof year, local(year_list)
	
	//Pull info from the envelope

get_envelope, age_group_id("`age_group_list'") year_id("`year_list'") sex_id("`sex_list'") location_id ("-1") location_set_id("35") clear

keep year_id location_id sex_id age_group_id mean
	//rename
	rename mean envelope
	
	tempfile pop_env
	save `pop_env', replace
	

// Get raw data
	import delimited using "$prefix/FILEPATH/`acause'_`gbd_round'.csv", clear
	rename sex sex_id 
	rename year year_id 
	rename cf_corr cf
	drop if year<1980
	drop if cf==.
		drop if sample_size==0
		gen criteria = inlist(age_group_id,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)
		keep if criteria == 1
		drop criteria
	keep location_id location_name year sex age_group_id cf sample_size 
    tempfile raw_data
	save `raw_data', replace

// Get covariate
      get_covariate_estimates, covariate_id(46) clear
	  //year_id	
		drop if year_id < 1980
		keep location_id year_id mean_value
		duplicates drop location_id year_id, force
		gen iodized_salt=mean_value
		tempfile covs
		save `covs', replace
	
		use `raw_data', clear
		merge m:1 location_id year_id using `covs', keep(3) nogen


// want death numbers in order to do count model of negative binomial (cf model)
   gen deaths=cf*sample_size
   drop if sample_size<1

save "`outdir'/raw_input_data.dta", replace	

// less than half a partial death doesn't make sense; replace as zero	
	replace deaths=0 if deaths<.5
	drop if year<1980 

// create female covariate
	gen female=.
	replace female=1 if sex_id==2
	replace female=0 if sex_id==1


// drop if cause fraciton is greater than the 99.9th percentile value
	
	centile cf, centile (99.9)
	gen pc_99th=r(c_1)
	drop if cf > pc_99th


// drop subnational data: look to see if there are overlaps and run without this at first potentially
	merge m:1 location_id using "$prefix/FILEPATH/location_id_parent.dta", keep(3)nogen
	
	drop if parent=="Brazil" | parent=="China" | parent=="India" | parent=="Japan" | parent=="Kenya" | parent=="Mexico" | parent=="Saudi Arabia" | parent=="Sweden" | parent=="United Kingdom" | parent=="United States" | parent=="South Africa"

	save "`outdir'/data_for_nbreg.dta", replace
** *********************************************************************************************************

	log using "`outdir'/nbr_log.smcl", replace
	
** *********************************************************************************************************
** NEGATIVE BINOMIAL REGRESSION - EDIT IN YOUR REGRESSION HERE 				
** *********************************************************************************************************

// run negative binomial regression with fixed effects on year, age, super_region. 
					
		di in red "CF MODEL"
			nbreg deaths iodized_salt i.age_group_id female, exposure(sample_size)
** **********************************************************************************************************
	cap log close 

	
// predict out for all country-year-age-sex
	use `pop_env', clear
	merge m:1 location_id year_id using `covs', nogen
	drop if year_id<1980

	gen female=.
	replace female=1 if sex_id==2
	replace female=0 if sex_id==1
// 1000 DRAWS 
// Your regression stores the means coefficient values and covariance matrix in the ereturn list as e(b) and e(V) respectively
// Create draws from these matrices to get parameter uncertainty (for each coefficient and the constant)
	// create a columnar matrix (rather than default, which is row) by using the apostrophe
		matrix m = e(b)'
	// create a local that corresponds to the variable name for each parameter
		local covars: rownames m
	// create a local that corresponds to total number of parameters
		local num_covars: word count `covars'
	// create an empty local that you will fill with the name of each beta (for each parameter)
		local betas
	// fill in this local
		forvalues j = 1/`num_covars' {
			local this_covar: word `j' of `covars'
			local covar_fix=subinstr("`this_covar'","b.","",.)
			local covar_rename=subinstr("`covar_fix'",".","",.)
			
	// Rename dispersion coefficient (is also called _const, like intercept) 
        if `j' == `num_covars' {
          local covar_rename = "alpha"
        }
			local betas `betas' b_`covar_rename'
		}
	// store the covariance matrix
		matrix C = e(V)
	// use the "drawnorm" function to create draws using the mean and standard deviations from your covariance matrix
		drawnorm `betas', means(m) cov(C)
	// you should now have as many new variables in your dataset as betas
	// they will be filled in, with diff value from range of possibilities of coefficients, in every row of your dataset
	
	// Generate draws of the prediction
		levelsof age_group_id, local(ages)
		levelsof year_id, local(year)
		local counter=0
		generate alpha = exp(b_alpha)
		forvalues j = 1/1000 {
			local counter = `counter' + 1
			di in red `counter'
			quietly generate xb_d`j' = 0
			quietly replace xb_d`j'=xb_d`j'+b__cons[`j']
			quietly replace xb_d`j'=xb_d`j'+iodized_salt*b_iodized_salt[`j']
			quietly replace xb_d`j'=xb_d`j'+female*b_female[`j']
** ****************************************************************************************************************
** ******* IF YOU HAVE MORE COVARIATES:														
** ******* Add them into the prediction now													 
** ******* for a covariate "COVAR" NOT using a reference value: quietly replace xb_d`j'=xb_d`j'+COVAR*b_COVAR[`j']	
** ******* for a covariate "COVAR" USING a reference value "4": quietly replace xb_d`j'=xb_d`j'+4*b_COVAR[`j']		
** ****************************************************************************************************************

			foreach a of local ages {
					quietly replace xb_d`j'=xb_d`j'+b_`a'age_group_id[`j'] if age_group_id==`a'
					}
					
					
					
			// rename
			quietly rename xb_d`j' draw_`counter'
			// get our predition in the right form
				// nbr as run above stores the prediction as ln_cf, which we multiply by env to get deaths
			quietly replace draw_`counter' = exp(draw_`counter') * env
			  // If dispersion is modeled as function of mean (default in Stata nbreg; NB2):
            quietly replace draw_`counter' = rgamma(1/alpha[`j'],alpha[`j']*draw_`counter')
         
		}

	
	
		// Rename draws
			forvalues i = 1/1000 {
				local i1 = `i' - 1
				rename draw_`i' draw_`i1'
			}
			
			forvalues j=0/999 {
				replace draw_`j' = 0 if draw_`j' < 0
			}

			
// saving death draws
	sort location_id year age_group_id
	drop if draw_0==.
	drop if year<1980 
	
	
	// add cause_id
	gen cause_id=388
	
	keep age_group_id location_id year_id sex_id cause_id draw_*
	
	levelsof(location_id), local(ids) clean
	
	// takes about 1.5 hours on 12 slots 
	foreach location_id of local ids {
			qui outsheet if location_id==`location_id' using "/FILEPATH/death_`location_id'.csv", comma replace
	}
	
	save "/FILEPATH/death_draws.dta", replace
	
	local file_pattern "death_{location_id}.csv"
	save_results_cod, cause_id(388) description(`acause' custom `custom_version') input_dir(FILEPATH) input_file_pattern(`file_pattern') clear

** ********************************************************************************************************************************************
