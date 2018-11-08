/* **************************************************************************
NEONATAL HEMOLYTIC MODELING
PART 1: RH DISEASE
Part C: Birth order regression
	
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
		local j FILEPATH
		// Load the PDF appending application
		quietly do FILEPATH
	}
	if c(os) == "Unix" {
		local j FILEPATH
		ssc install estout, replace 
		ssc install metan, replace
	} 


	local me_id 8659 
	

	adopath + FILEPATH


	// set directories 
	local working_dir = FILEPATH
	local data_dir FILEPATH
	local out_dir FILEPATH
	local plot_dir FILEPATH
	
	// Create timestamp for logs2
    local c_date = c(current_date)
    local c_time = c(current_time)
    local c_time_date = "`c_date'"+"_" +"`c_time'"
    display "`c_time_date'"
    local time_string = subinstr("`c_time_date'", ":", "_", .)
    local timestamp = subinstr("`time_string'", " ", "_", .)
    display "`timestamp'"
	
	
	
/* ///////////////////////////////////////////////////////////
// BIRTH ORDER REGRESSION
///////////////////////////////////////////////////////////// */
	
	local plot_props=0
	
	
	/* /////////////////////////
	//1. Covariates & Templates
	/////////////////////////// */
		
		get_covariate_estimates, covariate_id(149) clear
		drop age* sex* model_version_id covariate_id
		keep if year_id >=1980
		rename year_id year 
		rename mean_value tfr
		duplicates drop
		tempfile tfr
		save `tfr'


		get_location_metadata, location_set_id(9) gbd_round_id(5) clear
		expand 38
		bysort location_id: gen year = _n
		replace year = year + 1979
		keep if level > 2 

		tempfile template
		save `template', replace
	
	/* /////////////
	2. Birth order regression
	//////////////// */
	di "importing data"
	cd "`data_dir'"
	get_epi_data, bundle_id(498) clear
	di "got epi data from bundle 498"
	
	
		//format
		gen year = floor((year_start+year_end)/2)
		rename mean notfirst_birth_prev_data
		keep location_id location_name year notfirst_birth_prev_data 
		
		merge 1:m location_id year using `template', keep(2 3) nogen
		merge m:1 location_id year using `tfr', keep(1 3) nogen

		gen lt_notfirst_birth_prev_data = logit(notfirst_birth_prev_data)
	
		// Get covariates and random effects set up:
	

		local cov_list tfr year
		local cov_count: word count `cov_list'
		

		replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
		local re_levels super_region_id region_id ihme_loc_id
		local re_count: word count `re_levels'
		local re_name
		if "`re_levels'" != ""{
			foreach re of local re_levels {
				local re_name `re_name' || `re':
			}
		}



	xtmixed lt_notfirst_birth_prev_data `cov_list' `re_name'
	
	
	///////////////
	//3. Predictions
	///////////////
	
		
		
			di in red "predicting for fixed effects"
			matrix betas = e(b)
			local endbeta = `cov_count' + 1

			matrix fe_betas = betas[1, 1..`endbeta']

			matrix covars = e(V)
			matrix fe_covars = covars[1..`endbeta', 1..`endbeta']

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
		///////////////////////////////////////////////////////////////////// */
	

				di in red "predicting for random effects"
				predict re_* , reffects
				predict re_se_* , reses
		

				forvalues re_idx = 1/`re_count'{
					di in red "predicting for random effect `re_idx' of `re_count'"
					preserve

						local re_name_`re_idx' : word `re_idx' of `re_levels'

						local past_re_name
						if `re_idx'!=1{
							local past_re_idx = `re_idx'-1
							local past_re_name : word `past_re_idx' of `re_levels'
						}
						

						keep `re_name_`re_idx'' `past_re_name' re_*`re_idx'
						

							levelsof `re_name_`re_idx'', local(full_level_list)
							levelsof `re_name_`re_idx'' if re_`re_idx'!=., local(notmissing_level_list)
							local full_len: word count `full_level_list'
							local notmissing_len: word count `notmissing_level_list'

							if ("`full_level_list'" == "`notmissing_level_list'") & (`full_len'==`notmissing_len'){
								drop if re_`re_idx' == .
							}

						sort `re_name_`re_idx'' re_`re_idx'
						bysort `re_name_`re_idx'': gen count=_n
						drop if count!=1
						drop count

						local mat_val = `endbeta' + `re_idx'
						local re_se = exp(betas[1, `mat_val'])
						di in red "using common se `re_se' for missing values"
						replace re_`re_idx' = 0 if re_`re_idx' == .
						replace re_se_`re_idx' = `re_se' if re_se_`re_idx' == .
						

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
		///////////////////////////////////////////////////////////////////// */
		
			//1. Merge fixed/random effects
				di in red "merging fixed and random effects"
				preserve
					use `fe_sims', clear
					gen summed_constants = b_0
					forvalues re_idx = 1/`re_count'{
						local re_name_`re_idx' : word `re_idx' of `re_levels'
						di in red "merging re `re_name_`re_idx''"

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
								BREAK
							}
						drop _merge
						

						replace summed_constants = summed_constants + g_`re_idx'
					}
					

					drop b_0 g_*
					

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
			

				di in red "merging covariates onto parent"
				merge m:1 `re_levels' using `reshaped_covariates'

				count if _merge!=3
				if `r(N)' > 0{
					BREAK
				}
				drop _merge
							

				di in red "calculating predicted value!"
				quietly{
					forvalues i=1/1000{
					
						if mod(`i', 100)==0{
							di in red "working on number `i'"
						}
					
						rename summed_draw_`i' lt_hat_draw_`i'
						

						forvalues j=1/`cov_count'{
							local cov: word `j' of `cov_list'
							replace lt_hat_draw_`i' = lt_hat_draw_`i' + b_`j'_draw_`i' * `cov'
							drop b_`j'_draw_`i'
						}
						
						gen draw_`i' = invlogit(lt_hat_draw_`i')
						drop lt_hat_draw_`i'
					}
				}
				

				drop re_* lt_notfirst_birth_prev_data

		//save all draws
			rename draw_1000 draw_0
			preserve

				keep ihme_loc_id location_id year draw_* 
				outsheet using "FILEPATH/notfirst_birth_prev_all_draws.csv", comma replace 
				save "FILEPATH/notfirst_birth_prev_all_draws.dta", replace 
			restore
		
		// summary stats
			egen mean = rowmean(draw*)
			fastpctile draw*, pct(2.5 97.5) names(lower upper)
			drop draw*

			sort ihme_loc_id year

			outsheet using "FILEPATH/notfirst_birth_prev_summary_stats.csv", comma replace 
			save "FILEPATH/notfirst_birth_prev_summary_stats.dta", replace
	
