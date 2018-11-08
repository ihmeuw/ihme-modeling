/* **************************************************************************
NEONATAL HEMOLYTIC MODELING
PART 1: RH DISEASE
Part A: Prevalence of Rh negativity

The hemolytic modeling process is described in detail in the README in the 
hemolytic code folder.  This script completes the first step of modeling 
kernicterus due to Rh Disease: calculating Rh- prevalence in the population, 
and taking (Rh_neg_prev) * (Rh_pos_prev) to get the prevalence of Rh-incompatible
pregnancies for every country-year.

1. Prevalence of Rh incompatibility (Rh+ baby born to Rh- mother)
	a. We have data on the proportion of Rh negativity for some country-years
	b. Regress on these values with only an intercept fixed effect, and superregion and region random effects:

		rh_negative_prop = b_0 + g_superregion + g_region

		This should give a full set of estimates for the proportion of Rh negativity in each country-year.

	c. The proportion of babies at risk of Rh disease (ignoring Rhogam and birth order for a moment) are those Rh+ babies who are born to Rh- women. Statistically, this works out to be equal to the proportion of Rh negativity in the population times the proportion of Rh positivity in the population (that is, 1-Rh_negativity). So, we do:

		pos_to_neg_prop = rh_negative_prop * (1-rh_negative_prop)

	d. Multiply this proportion by births to get a birth count:

		pos_to_neg_count = pos_to_neg_prop*births

	This gives us the proportion, and the number, of babies with Rh incompatibility.
		
******************************************************************************/

clear all
set graphics off
set more off
set maxvar 32000


if c(os) == "Windows" {
	local j "J:"
}
if c(os) == "Unix" {
	local j "FILEPATH"
	ssc install estout, replace 
	ssc install metan, replace
} 
di in red "J drive is `j'"

adopath + "FILEPATH"

local me_id 2768 
local bundle_id 389 

local data_dir "FILEPATH"
local out_dir "FILEPATH"
local plot_dir "FILEPATH"

local c_date = c(current_date)
local c_time = c(current_time)
local c_time_date = "`c_date'"+"_" +"`c_time'"
display "`c_time_date'"
local time_string = subinstr("`c_time_date'", ":", "_", .)
local timestamp = subinstr("`time_string'", " ", "_", .)
display "`timestamp'"

	
/* ///////////////////////////////////////////////
 Rh- Prevalence 
	Run a regression with a fixed intercept and region
	and superregion random effects to get prevalence of 
	rh-negativity for each country
//////////////////////////////////////////////////// */
	

	local regress=1

	local get_incompatible_prev =1

	local get_incompatible_counts = 1

	
		get_location_metadata, location_set_id(9) gbd_round_id(5) clear
		expand 68
		bysort location_id: gen year = _n
		replace year = year + 1949

		tempfile nosex_template
		save `nosex_template', replace
	
	
	/* //////////////////////////////////////
	// Run Regression
	//////////////////////////////////// */
		
		 if `regress' == 1 {
		

			di "importing data"
			cd "`data_dir'"

			get_epi_data, bundle_id(389) clear
		
			gen year = floor((year_start+year_end)/2)
			
			merge m:1 location_id year using `nosex_template', keep(2 3) nogen force

			keep location_id super_region_id region_id location_name year mean  
			drop if super_region_id == . 
		
			rename mean rh_neg_data
			
			gen lt_rh_neg_data = logit(rh_neg_data)
			
			local cov_list 

			local cov_count = 0
			

			local re_levels super_region_id region_id
			local re_count: word count `re_levels'
			local re_name
			if "`re_levels'" != ""{
				foreach re of local re_levels {
					local re_name `re_name' || `re':
				}
			}
			
			di in red "random effects are `re_name'"

			xtmixed lt_rh_neg_data `cov_list' `re_name'
			
			/////////////////////////////////////////////////////////////////////
			/// Predict for fixed effects:
			///////////////////////////////////////////////////////////////////// */
				
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
							
							di in red "re_name_`re_idx' is `re_name_`re_idx'', re_idx is `re_idx', past_re_idx is `past_re_idx', past_re_name is `past_re_name'"
							
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
							

							replace summed_constants = summed_constants + g_`re_idx'
						}
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


							replace region_id = 9999 if region_id == .
							tempfile reshaped_covariates
							save `reshaped_covariates', replace
					restore

				

					di in red "merging covariates onto parent"

					replace region_id = 9999 if region_id == . 
					merge m:1 `re_levels' using `reshaped_covariates'

								
				//4. Do arithmetic on draw level, transform from logit to real space.
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
			

					drop re_* lt_rh_neg_data

				rename draw_1000 draw_0
			
				preserve

					keep location_id year draw_* 
					drop if year<1980
					export delimited using "FILEPATH", replace 
					save "FILEPATH", replace 
				restore
			

				egen mean = rowmean(draw*)
				fastpctile draw*, pct(2.5 97.5) names(lower upper)
				drop draw*

				sort location_id year

				export delimited using "FILEPATH", replace 
				save "FILEPATH", replace
			

		
		
		
/* //////////////////////////////////////////
//Find proportion Rh-incompatible pregnancies
////////////////////////////////////////// */
		
		if `get_incompatible_prev'==1{
		

			use "`out_dir'/rh_neg_prev_all_draws.dta", clear
			

			quietly {
				forvalues i = 0/999{
					if mod(`i', 100)==0{
						di in red "`i'"
					} 
					replace draw_`i' = draw_`i' * (1-draw_`i')
				}
			}
			export delimited using "FILEPATH",  replace 
			save "FILEPATH", replace
			
			//summary stats
			preserve
				egen mean = rowmean(draw*)
				fastpctile draw*, pct(2.5 97.5) names(lower upper)
				drop draw* 
				
				export delimited using "FILEPATH", replace
				save "FILEPATH", replace
			restore
			
		}
		
	/* //////////////////////////////////////////////////////////////////////
	//C. Multiply by births to get counts of Rh-incompatible pregnancies
	///////////////////////////////////////////////////////////////////// */
		
		if `get_incompatible_counts'==1 {
		
			di in red "converting from prevalence to birth count!"
			
			
			get_covariate_estimates, covariate_id(1106) sex_id(1 2) gbd_round_id(5) clear
			keep location_id year_id sex_id mean_value
			rename sex_id sex 
			rename year_id year
			rename mean_value births
	
			tempfile sex_specific_births
			save `sex_specific_births', replace

			collapse(sum) births, by (location_id year)
			gen sex = 3

			append using `sex_specific_births'

			tempfile births
			save `births', replace
			

			use "FILEPATH", clear
			

			expand 2, gen(sex)
			replace sex=2 if sex==0
			expand 2 if sex==1, gen(both_indic)
			replace sex=3 if both_indic==1
			drop both_indic
			

			merge 1:1 location_id year sex using `births', keep(3) nogen
			
			
			di in red "multiplying to get birth counts"
			quietly{
				forvalues i = 0/999{
					if mod(`i', 100)==0{
						di in red "`i'"
					} 
					replace draw_`i' = draw_`i' * births
					
				}
			}
				
				

			preserve
				export delimited using "FILEPATH", replace 
				save "FILEPATH", replace
			restore
			

				egen mean = rowmean(draw*)
				fastpctile draw*, pct(2.5 97.5) names(lower upper)
				drop draw*

			export delimited using "FILEPATH", replace
			save "FILEPATH", replace
			
			
			
		}
	
	
	
		
	
		
