  /* **************************************************************************
a. We have data on the proportion of children who are not firstborns for some country-years.
b. Regress on these values with Total Fertility Rate (TFR) and year fixed effects and superregion, region, and country-level random effects:

	notfirst_birth_prop = b_0 + b_1*TFR + b_2*year + g_superregion + g_region + g_country

This gives us the proportion of children who are not firstborn.
	
******************************************************************************/

clear all
set more off
set graphics off
set maxvar 32000

/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */ 

	// priming the working environment 
	if c(os) == "Windows" {
		local j "J:"
	}
	if c(os) == "Unix" {
		local j "FILEPATH/j"
		ssc install estout, replace 
		ssc install metan, replace
	} 
	di in red "J drive is `j'"

	// locals
	local me_id 8659 // "Proportion who are not first-born"
	
	// load functions 
	adopath + "FILEPATH/stata_functions/"
	
	local out_dir "FILEPATH"
	
/* ///////////////////////////////////////////////////////////
// BIRTH ORDER REGRESSION
///////////////////////////////////////////////////////////// */
	
	/* /////////////////////////
	//1. Covariates & Templates
	/////////////////////////// */
		
		//TFR
		get_covariate_estimates, covariate_id(149) gbd_round_id(6) decomp_step(step4) clear
		drop age* sex* model_version_id covariate_id
		keep if year_id >=1980
		rename year_id year 
		rename mean_value tfr
		duplicates drop
		tempfile tfr
		save `tfr'

		// get templates ready for regression
		get_location_metadata, location_set_id(9) gbd_round_id(6) clear
		expand 40
		bysort location_id: gen year = _n
		replace year = year + 1979
		keep if level > 2 // keeping only country-level and lower 

		tempfile template
		save `template', replace
	
	/* /////////////
	2. Birth order regression
	//////////////// */
	di "importing data"
	get_bundle_data, bundle_id(498) decomp_step(step1) clear
	di "got epi data from bundle 498"
	
		//format
		gen year = floor((year_start+year_end)/2)
		rename mean notfirst_birth_prev_data
		keep location_id location_name year notfirst_birth_prev_data 
		
		merge 1:m location_id year using `template', keep(2 3) nogen
		merge m:1 location_id year using `tfr', keep(1 3) nogen

		gen lt_notfirst_birth_prev_data = logit(notfirst_birth_prev_data)
	
		// Get covariates and random effects set up:
	
		// 1. fixed effects
		local cov_list tfr year
		local cov_count: word count `cov_list'
		
		// 2. random effects
		// format ihme_loc_id 
		replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
		local re_levels super_region_id region_id ihme_loc_id
		local re_count: word count `re_levels'
		local re_name
		if "`re_levels'" != ""{
			foreach re of local re_levels {
				local re_name `re_name' || `re':
			}
		}
	
	di in red "random effects are `re_name'"

	// the regression itself
	xtmixed lt_notfirst_birth_prev_data `cov_list' `re_name'
	
	///////////////
	//3. Predictions
	///////////////
	
		/////////////////////////////////////////////////////////////////////
		/// Predict for fixed effects:
		/// 1. Take the covariates corresponding to the fixed effects, and the 
		/// 	covariance of those fixed effects, from the beta and covariance matrices
		/// 	(stored by default in the e(b) and e(V) matrices, respectively).  
		///		Note that the intercept will be at the end of this list, not the beginning.
		/// 2.  Make a list of locals whose names correspond to the entries in the matrices,
		/// 	in order (remember to but the intercept last!)
		/// 3. 	Use the 'drawnorm' function to generate a new dataset that contains a 
		/// 	column for each fixed effect, with a thousand draws (long) for each value.
		///		Save this in a temp file for later.
		///////////////////////////////////////////////////////////////////// */
		
			di in red "predicting for fixed effects"
			matrix betas = e(b)
			local endbeta = `cov_count' + 1
			//grab all the fixed effect betas
			matrix fe_betas = betas[1, 1..`endbeta']
			//grab all the fixed effect covariates
			matrix covars = e(V)
			matrix fe_covars = covars[1..`endbeta', 1..`endbeta']
			
			//we now predict a thousand draws for these betas using the 
			// 'drawnorm' function. This will create a new dataset with 
			// columns equal to our beta values (named in 'betalist') and a
			// thousand rows, one for each draw.  
			local betalist 
			forvalues i = 1/`cov_count' {
				local betalist `betalist' b_`i'
			}
			local betalist `betalist' b_0
			di in red "beta list is `betalist'"
			
			preserve
				drawnorm `betalist', n(1000) means(fe_betas) cov(fe_covars) clear
				gen sim = _n
				tempfile fe_sims
				save `fe_sims', replace
			restore
		
		
		/* /////////////////////////////////////////////////////////////////////
		/// Predict for random effects:
		/// 1.  Use the 'predict' function to get a random effect estimate for 
		/// 	every line in the dataset.  
		///
		///	2.  Unfortunately, this doesn't give us
		///		everything we want: any country whose superregion had no data will
		/// 	not receive a random-effects estimate.  We fix this by looping through
		///		each geography and filling in missing values with mean 0 and SE equal
		///		to the global SE.
		///
		///	3. Keep in mind also that we want to wind up with a thousand draws for
		///		each random-effect level that we have.  But since the higher-level geographies
		/// 	map to many lower-level geographies, if we were to predict for them all at 
		///		once we would wind up with many draw_1's for superregion for each draw_1 
		///		for iso3.  To prevent this, while we loop through each random effect level
		///		we collapse the dataset down to contain one line for every element of that 
		///		geography (i.e. 7 lines for the 'superregion' dataset, 21 for the 'region', etc.)
		///	
		///4.  We take a thousand draws from each collapsed dataset, and save it in a temp file.
		/// 
		///////////////////////////////////////////////////////////////////// */
	
			//1. Use 'predict' function
				di in red "predicting for random effects"
				predict re_* , reffects
				predict re_se_* , reses
		
			//2-3: Loop through each geography level 
				forvalues re_idx = 1/`re_count'{
					di in red "predicting for random effect `re_idx' of `re_count'"
					preserve
						//keep only the values corresponding to the random effect of interest
						//and the random effect one level above it (so you can merge on)
						local re_name_`re_idx' : word `re_idx' of `re_levels'
						//get a list of the previous re level so you have a common key to merge on
						local past_re_name
						if `re_idx'!=1{
							local past_re_idx = `re_idx'-1
							local past_re_name : word `past_re_idx' of `re_levels'
						}
						
						//reduce down so we have a single row for each geography at this level
						keep `re_name_`re_idx'' `past_re_name' re_*`re_idx'
						
						//check: if there is data for some (but not all) values of a level 
						// (i.e. a given superregion_id has values for some rows but not others),
						// drop the missing values.
						// if there are values of a level for which every entry is missing, (i.e. a region
						// where there is no data), do not drop missing values.
							levelsof `re_name_`re_idx'', local(full_level_list)
							levelsof `re_name_`re_idx'' if re_`re_idx'!=., local(notmissing_level_list)
							local full_len: word count `full_level_list'
							local notmissing_len: word count `notmissing_level_list'

							if ("`full_level_list'" == "`notmissing_level_list'") & (`full_len'==`notmissing_len'){
								di in red "all locations represented! dropping missing values"
								drop if re_`re_idx' == .
							}

						//now, return to reducing values down.
						sort `re_name_`re_idx'' re_`re_idx'
						bysort `re_name_`re_idx'': gen count=_n
						drop if count!=1
						drop count
						
						//we don't have data for every region, so: fill those in
						// with the global values: mean 0, SE equal to the SE 
						// of the entire regression (from the betas matrix)
						local mat_val = `endbeta' + `re_idx'
						local re_se = exp(betas[1, `mat_val'])
						di in red "using common se `re_se' for missing values"
						replace re_`re_idx' = 0 if re_`re_idx' == .
						replace re_se_`re_idx' = `re_se' if re_se_`re_idx' == .
						
						
						//4. get 1000 long draws
						expand 1000
						bysort `re_name_`re_idx'': gen sim = _n
						gen g_`re_idx' = rnormal(re_`re_idx', re_se_`re_idx')
						drop re_*
						sort `re_name_`re_idx'' sim
						
						tempfile random_effect_`re_idx'
						save `random_effect_`re_idx'', replace
					restore
				}
		
		/* /////////////////////////////////////////////////////////////////////
		/// Merge, reshape, and calculate final predicted values.
		///
		/// 1. Merge the fixed effect dataset and each random effect dataset
		///		together.
		///
		///	2. Reshape wide.
		///
		///	3. 	Merge this new dataset back onto your original dataset with the 
		///		covariates and data.
		/// 4. For each draw, do the math to generate a predicted value: 
		///		y_draw_i = b_0 + B*X + G, where B is the vector of fixed effects,
		///		X is the matrix of covariates, and G is the vector of random effects.
		///		We transform the draws out of logit space as we go along.
		///
		///////////////////////////////////////////////////////////////////// */
		
			//1. Merge fixed/random effects
				di in red "merging fixed and random effects"
				preserve
					use `fe_sims', clear
					gen summed_constants = b_0
					forvalues re_idx = 1/`re_count'{
						local re_name_`re_idx' : word `re_idx' of `re_levels'
						di in red "merging re `re_name_`re_idx''"
						//define keys to merge on
						if `re_idx'==1{
							local merge_on sim
						}
						else{
							local past_re_idx= `re_idx' -1
							local past_re_name: word `past_re_idx' of `re_levels'
							
							local merge_on sim `past_re_name'
						}
						di in red "merging on variables `merge_on'"
						merge 1:m `merge_on' using `random_effect_`re_idx''
						count if _merge!=3
						if `r(N)' > 0{
								di in red "merge on sims not entirely successful!"
								BREAK
							}
						drop _merge
						
						//add the constant terms (b_0 and random effects) together
						replace summed_constants = summed_constants + g_`re_idx'
					}
					
					//drop b_0 and the random effects: you've already captured them in summed_constants
					drop b_0 g_*
					
					//2. Reshape wide
						di in red "reshaping"
						rename summed_constants summed_draw_
						if `cov_count'!=0{
							rename b_* b_*_draw_
							reshape wide b_* summed_draw_, i(`re_levels') j(sim)
						}
						else{
							reshape wide summed_draw_, i(`re_levels') j(sim) 
						}

						tempfile reshaped_covariates
						save `reshaped_covariates', replace
				restore
			
			//3. Merge back onto post-regression dataset. Merge should be perfect
				di in red "merging covariates onto parent"
				merge m:1 `re_levels' using `reshaped_covariates'

				count if _merge!=3
				if `r(N)' > 0{
					di in red "merge on random effects not entirely successful!"
					BREAK
				}
				drop _merge
							
			//4. Do arithmetic on draw level, transform from logit to real space.
				di in red "calculating predicted value!"
				quietly{
					forvalues i=1/1000{
					
						if mod(`i', 100)==0{
							di in red "working on number `i'"
						}
					
						rename summed_draw_`i' lt_hat_draw_`i'
						
						//will only do anything if cov_count!=0
						forvalues j=1/`cov_count'{
							local cov: word `j' of `cov_list'
							replace lt_hat_draw_`i' = lt_hat_draw_`i' + b_`j'_draw_`i' * `cov'
							drop b_`j'_draw_`i'
						}
						
						gen draw_`i' = invlogit(lt_hat_draw_`i')
						drop lt_hat_draw_`i'
					}
				}
				
				//drop unnecessary columns
				drop re_* lt_notfirst_birth_prev_data

		//save all draws
			rename draw_1000 draw_0
			preserve
				//all rh- draws 
				keep ihme_loc_id location_id year draw_* 
				outsheet using "`out_dir'/notfirst_birth_prev_all_draws.csv", comma replace 
				save "`out_dir'/notfirst_birth_prev_all_draws.dta", replace 
			restore
		
		// summary stats
			egen mean = rowmean(draw*)
			fastpctile draw*, pct(2.5 97.5) names(lower upper)
			drop draw*

			sort ihme_loc_id year

			outsheet using "`out_dir'/notfirst_birth_prev_summary_stats.csv", comma replace 
			save "`out_dir'/notfirst_birth_prev_summary_stats.dta", replace
	
