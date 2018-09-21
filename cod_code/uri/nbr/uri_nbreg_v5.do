// Preparing a negative binomial regression for input into codcorrect

// Settings
			// Clear memory and set memory and variable limits
				clear all
				set mem 5G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "ADDRESS"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "ADDRESS"
				}
			
			// Close any open log file
				cap log close

** ********************************************************************************************************
// locals
	local acause uri
	/* local age_start=2   /* age_group_id 2 corresponds to age_name "early neonatal" */
	local age_end=21    /* age_group_id 21 corresponds to age_name 80+ yrs */                  */
	local custom_version v5  

// Make folders
	capture mkdir "ADDRESS"
	capture mkdir "ADDRESS"
** *********************************************************************************************************

// define filepaths
	cap mkdir "ADDRESS"
	cap mkdir "ADDRESS"
	
	local outdir "ADDRESS"


adopath + "ADDRESS"
get_envelope, location_id(-1) year_id(-1) sex_id("1 2") age_group_id(-1) location_set_id(22) clear
keep if (age_group_id>=2 & age_group_id<=20) | (age_group_id>=30 & age_group_id<=32) | age_group_id==235
drop if sex_id==3

//rename
	rename mean envelope	
		
	// drop uncessary age groups
	/* drop if age_group_id <`age_start' | age_group_id >`age_end' */
	drop if year_id <1980
	tempfile pop_env
	save `pop_env', replace
	

// Get raw data
	use "FILEPATH", clear
	
	rename sex sex_id 
	rename year year_id 
	rename cf_corr cf 
	rename age age_group_id
	drop if year_id<1980
	drop if cf==.
	drop if sample_size==0
	/* drop if age_group_id<`age_start' | age_group_id>`age_end'  */
	 
	/* keep location_id location_name year_id sex_id age_group_id cf sample_size  */ 
    tempfile raw_data
	save `raw_data', replace

	
// get covariates
    clear all
    get_covariate_estimates, covariate_name_short(LDI_pc)
	drop if year_id<1980
	keep location_id year_id mean
	duplicates drop location_id year_id, force
	gen ln_LDI=ln(mean)
	tempfile covs
	save `covs', replace
		
	use `raw_data', clear
	merge m:1 location_id year_id using `covs', keep(3) nogen
	

// want death numbers in order to do count model of negative binomial 
   gen deaths=cf*sample_size
   drop if sample_size<1
   save "FILEPATH", replace	
   
// less than half a partial death doesn't make sense; replace as zero	
	replace deaths=0 if deaths<.5
	drop if year<1980 


// drop national level data for countries with subnationals /* the model does not perform well when including national level data */ 
	drop if inlist(location_id,130,95,6,11,102,135,163,180,67,93,196,152)  

		
// drop if cause fraciton is greater than the 99th percentile value
	
	egen p99 = pctile(cf), p(99)
	drop if cf > p99
	
	save "ADDRESS", replace

** *********************************************************************************************************
/*
// run negative binomial regression  
	// exposure of sample_size takes ln(sample_size) & forces coeff of 1, making it defacto cf model. 
	if "`type'"=="cf" {
		local exposure = "sample_size"
		}
	// exposure of pop takes ln(pop) & forces coefficient of 1, making it defacto rate model
	if "`type'"=="rate" {
		local exposure = "pop"
		}
*/	
	log using "FILEPATH", replace
	
	di in red "CF MODEL"
			nbreg deaths ln_LDI i.age_group_id, exposure(sample_size)

	cap log close 
** **********************************************************************************************************

// predict out for all country-year-age-sex
	use `pop_env', clear
	merge m:1 location_id year_id using `covs', nogen
    drop if year_id<1980
	
// 1000 DRAWS 
// Your regression stores the means coefficient values and covariance matrix in the ereturn list as e(b) and e(V) respectively
// Create draws from these matrices to get parameter uncertainty (for each coefficient and the constant)
	// create a columnar matrix (rather than dUSERt, which is row) by using the apostrophe
		matrix m = e(b)'
	/*
	// with gnbr, the output also includes a dispersion parameter which we don't need to include in the prediction
	// subtract 1 row to get rid of the coefficient on the dispersion parameter
		matrix m = m[1..(rowsof(m)-1),1]
		*/
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
		/* matrix C = C[1..(colsof(C)-1), 1..(rowsof(C)-1)] */
	// use the "drawnorm" function to create draws using the mean and standard deviations from your covariance matrix
		drawnorm `betas', means(m) cov(C)
	// you should now have as many new variables in your dataset as betas
	// they will be filled in, with diff value from range of possibilities of coefficients, in every row of your dataset
	
	// Generate draws of the prediction
		levelsof age_group_id, local(ages)
		local counter=0
		
		    gen alpha = exp(b_alpha)
		   	forvalues j = 1/1000 {
			local counter = `counter' + 1
			di in red `counter'
			quietly generate xb_d`j' = 0
			quietly replace xb_d`j'=xb_d`j'+b__cons[`j']
		// add in any addtional covariates here in the form:
			** quietly replace xb_d`j'=xb_d`j'+covariate*b_covariate[`j']
			
			quietly replace xb_d`j'=xb_d`j'+ln_LDI*b_ln_LDI[`j']
			foreach a of local ages {
					quietly replace xb_d`j'=xb_d`j'+b_`a'age_group_id[`j'] if age_group_id==`a'
					}			
		 	/*
			foreach sr of local superreg {
					quietly replace xb_d`j'=xb_d`j'+b_`sr'super_region[`j'] if super_region==`sr'
					}
			*/		
			// rename
			quietly rename xb_d`j' draw_`counter'
			
// multiply by envelope to get deaths		
		
		quietly replace draw_`counter' = exp(draw_`counter') * envelope
        quietly replace draw_`counter' = rgamma(1/alpha[`j'],alpha[`j']*draw_`counter')
		}

		
		// Rename draws
			forvalues i = 1/1000 {
				local i1 = `i' - 1
				rename draw_`i' draw_`i1'
			}
			
// saving death draws
	
	sort location_id year_id age_group_id sex_id
	keep location_id year_id age_group_id sex_id draw_*
	drop if year_id<1980 
	drop if draw_0==.
	/* tempfile death_draws
	save `death_draws', replace */
	
	//add cause_id
	gen cause_id=328
	
	outsheet using FILEPATH, comma names replace



// save results

run FILEPATH

save_results, cause_id(328) description(`acause' custom `custom_version') mark_best(yes) in_dir(ADDRESS)


