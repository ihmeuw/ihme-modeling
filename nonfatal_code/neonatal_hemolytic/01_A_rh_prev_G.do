/* **************************************************************************
NEONATAL HEMOLYTIC MODELING
PART 1: RH DISEASE
Part A: Prevalence of Rh negativity

The hemolytic modeling process is described in detail in the README in the 
hemolytic code folder.  This script completes the first step of modeling 
kernicterus due to Rh Disease: calculating Rh- prevalence in the population, 
and taking (Rh_neg_prev) * (Rh_pos_prev) to get the prevalence of Rh-incompatible
pregnancies for every country-year.

Copied from the README:
1. Prevalence of Rh incompatibility (Rh+ baby born to Rh- mother)
	a. We have data on the proportion of Rh negativity for some country-years
	b. Regress on these values with only an intercept fixed effect, and superregion and region random effects:

		rh_negative_prop = b_0 + g_superregion + g_region

		This should give a full set of estimates for the proportion of Rh negativity in each country-year.

	c. The proportion of bUSERs at risk of Rh disease (ignoring Rhogam and birth order for a moment) are those Rh+ bUSERs who are born to Rh- women. Statistically, this works out to be equal to the proportion of Rh negativity in the population times the proportion of Rh positivity in the population (that is, 1-Rh_negativity). So, we do:

		pos_to_neg_prop = rh_negative_prop * (1-rh_negative_prop)

	d. Multiply this proportion by births to get a birth count:

		pos_to_neg_count = pos_to_neg_prop*births

	This gives us the proportion, and the number, of bUSERs with Rh incompatibility.
		
******************************************************************************/

clear all
set graphics off
set more off
set maxvar 32000


// priming the working environemnt 
if c(os) == "Windows" {
	local j /*FILEPATH*/
	// Load the PDF appending application
	quietly do /*FILEPATH*/
}
if c(os) == "Unix" {
	local j /*FILEPATH*/
	ssc install estout, replace 
	ssc install metan, replace
} 
di in red "J drive is `j'"

// functions
quietly do /*FILEPATH*/

adopath + /*FILEPATH*/

// set locals
local me_id 2768 // "Hemolytic disease and other neonatal jaundice population prevalence Rh negative"

// set directories
local data_dir /*FILEPATH*/
local out_dir /*FILEPATH*/
local plot_dir /*FILEPATH*/
	
// Create timestamp for logs
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
	
	//steps:
	//i. Regress to find prev of Rh- 
	local regress=1
	local plot_rh_prev=0
	//ii. Algebra to get prev of Rh-incompatible pregnancies
	local get_incompatible_prev =1
	//iii. multiply by births to go from counts to proportions
	local get_incompatible_counts = 1
	local plot_final_births = 0
	
	// get templates ready for regression
		get_location_metadata, location_set_id(9) gbd_round_id(4) clear
		expand 67
		bysort location_id: gen year = _n
		replace year = year + 1949

		tempfile nosex_template
		save `nosex_template', replace
	
	
	/* //////////////////////////////////////
	// Run Regression
	//////////////////////////////////// */
		
		 if `regress' == 1 {
		
			//bring most recent file in data_dir 
			di "importing data"
			cd "`data_dir'"
			local files: dir . files "me_`me_id'*.xlsx"
	  		local files: list sort files 
	  		import excel using `=word(`"`files'"', wordcount(`"`files'"'))', firstrow clear
		
			gen year = floor((year_start+year_end)/2)
			
			merge m:1 location_id year using `nosex_template', keep(2 3) nogen force

			keep location_id super_region_id region_id location_name year mean  
			drop if super_region_id == . // global location 
		
			rename mean rh_neg_data
			
			// we're regressing on a prevalence, so we run the analysis in 
			// logit space to make sure our values stay between 0 and 1
			gen lt_rh_neg_data = logit(rh_neg_data)
			
			// generate a local for covariates, in case you ever run this with more variables
			local cov_list 
			// switch to: 
			// local cov_count: word count cov_list 
			// if cov_list is nonempty
			local cov_count = 0
			
			// determine which random effects you want; put them into the proper 
			// format for the regression
			local re_levels super_region_id region_id
			local re_count: word count `re_levels'
			local re_name
			if "`re_levels'" != ""{
				foreach re of local re_levels {
					local re_name `re_name' || `re':
				}
			}
			
			di in red "random effects are `re_name'"
			
			//actual regression is here! Consider taking screenshots of this output.
			xtmixed lt_rh_neg_data `cov_list' `re_name'
			
			/////////////////////////////////////////////////////////////////////
			/// Predict for fixed effects:
			/// This is relatively straightforward.
			/// 1. Take the covariates corresponding to the fixed effects, and the 
			/// 	covariance of those fixed effects, from the beta and covariance matrices
			/// 	(stored by dUSERt in the e(b) and e(V) matrices, respectively).  
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
							
							di in red "re_name_`re_idx' is `re_name_`re_idx'', re_idx is `re_idx', past_re_idx is `past_re_idx', past_re_name is `past_re_name'"
							
							//reduce down so we have a single row for each geography at this level
							keep `re_name_`re_idx'' `past_re_name' re_*`re_idx'
							
								levelsof `re_name_`re_idx'', local(full_level_list)
								levelsof `re_name_`re_idx'' if re_`re_idx'!=., local(notmissing_level_list)
								local full_len: word count `full_level_list'
								local notmissing_len: word count `notmissing_level_list'
								//stata deals with lists of strings and lists of numbers differently, this if statement
								// accounts for them both
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
				

			/////////////////////////////////////////////////////////////////////
			/// Merge, reshape, and calculate final predicted values.

			
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

							// if region_id values are missing (data is only)
							// at super_region level, replace with placeholder
							// value to ease merge
							replace region_id = 9999 if region_id == .
							tempfile reshaped_covariates
							save `reshaped_covariates', replace
					restore

				
				//3. Merge back onto post-regression dataset. Merge should be perfect
					di in red "merging covariates onto parent"

					// if region_id values are missing (data is only)
					// at super_region level, replace with placeholder
					// value to ease merge
					replace region_id = 9999 if region_id == . 
					merge m:1 `re_levels' using `reshaped_covariates'

					* count if _merge!=3
					* if `r(N)' > 0{
					* 	di in red "merge on random effects not entirely successful!"
					* 	BREAK
					* }
					* drop _merge

								
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
			
					//drop unnecessary things
					drop re_* lt_rh_neg_data

			//save all draws
				rename draw_1000 draw_0
			
				preserve
					//all rh- draws 
					keep location_id year draw_* 
					drop if year<1980
					export delimited using /*FILEPATH*/, replace 
					save /*FILEPATH*/, replace 
				restore
			
			// rh - summary stats
				egen mean = rowmean(draw*)
				fastpctile draw*, pct(2.5 97.5) names(lower upper)
				drop draw*

				sort location_id year

				//keep iso3 year rh_prev* mean
				export delimited using /*FILEPATH*/, replace 
				save /*FILEPATH*/, replace
			

		
		////////////////////
		//plot rh-negativity
		////////////////////
			
			if `plot_rh_prev'==1{
			
				import delimited using /*FILEPATH*/, clear
				sort location_id year
			
				//for ylabeling 
				qui sum mean
				local param_max = r(max)
				qui sum rh_neg_data
				local data_max = r(max)
				local max_val = max(`param_max', `data_max')
			
				pdfstart using /*FILEPATH*/
				levelsof location_id if rh_neg_data != ., local(location_id_list)
				
				foreach location_id of local location_id_list{
					
					di in red "plotting for `location_id'"
					
					twoway (line mean year if location_id == `location_id', lcolor(purple) lwidth(*1.75)  ) ///
						   (line  lower year if location_id == `location_id', lcolor(purple) lpattern(dash) lwidth(*1.75)  ) ///
							(line  upper year if location_id == `location_id', lcolor(purple) lpattern(dash) lwidth(*1.75)  ) ///
							(scatter rh_neg_data year if location_id == `location_id'), ///
							name(rh_prev_`location_id', replace) ///
							title("Rh- Prevalence, `location_id'") ///
							legend(order(1 2 4) label(1 "Prevalence Estimate") label(2 "95%CI") label(4 "Data") col(3)) ///
							ylabel(0(0.05)`max_val') ///
							xlabel(1950(10)2015)
					pdfappend
	
				}
				
				pdffinish, view
			
			}
		
		
		
/* //////////////////////////////////////////
//Find proportion Rh-incompatible pregnancies
////////////////////////////////////////// */
		
		if `get_incompatible_prev'==1{
		
			di in red "finding rh incompatibility prevalence!"
		
			di in red "uploading rh- prevalence draws"
			use /*FILEPATH*/, clear
			
			// calculate prop rh positive bUSERs to rh negative mothers:
			// prev_problem_pregs = prev_rg_neg (1-prev_rh_neg)
			di in red "generating 'problem baby' proportion"
			quietly {
				forvalues i = 0/999{
					if mod(`i', 100)==0{
						di in red "`i'"
					} 
					replace draw_`i' = draw_`i' * (1-draw_`i')
				}
			}
			export delimited using /*FILEPATH*/,  replace 
			save /*FILEPATH*/, replace
			
			//summary stats
			preserve
				egen mean = rowmean(draw*)
				fastpctile draw*, pct(2.5 97.5) names(lower upper)
				drop draw* 
				
				export delimited using /*FILEPATH*/, replace
				save /*FILEPATH*/, replace
			restore
			
		}
		
	/* //////////////////////////////////////////////////////////////////////
	//C. Multiply by births to get counts of Rh-incompatible pregnancies
	///////////////////////////////////////////////////////////////////// */
		
		if `get_incompatible_counts'==1 {
		
			di in red "converting from prevalence to birth count!"
			
			//prep births file to merge on when we convert prevalence to counts.

			local births_dir /*FILEPATH*/
			use location_id year sex_id births using "`births_dir'", clear
			rename sex_id sex 
			
			tempfile births
			save `births', replace
			
			//import rh incompatibility prevalence data
			use /*FILEPATH*/, clear
			
			//Duplicate data for each sex 
			expand 2, gen(sex)
			replace sex=2 if sex==0
			expand 2 if sex==1, gen(both_indic)
			replace sex=3 if both_indic==1
			drop both_indic
			
			//  _merge ==2 are locations we're not interested in and pre-1980
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
				
				
			//save draws
			preserve
				export delimited using /*FILEPATH*/, replace 
				save /*FILEPATH*/, replace
			restore
			
			//summary stats
				egen mean = rowmean(draw*)
				fastpctile draw*, pct(2.5 97.5) names(lower upper)
				drop draw*

			export delimited using /*FILEPATH*/, replace
			save /*FILEPATH*/, replace
			
			
			if `plot_final_births'==1{
			
				import delimited using /*FILEPATH*/, clear
				sort location_id year
				drop if sex==3
			 
				qui sum mean
				local max_val = r(max)

				pdfstart using /*FILEPATH*/
				levelsof location_id, local(location_id_list)
				
				foreach location_id of local location_id_list{
					
					di in red "plotting for `location_id'"
					
					twoway line mean year if location_id == `location_id', lcolor(purple) lwidth(*1.75) ||
						   line lower year if location_id == `location_id', lcolor(purple) lpattern(dash) lwidth(*1.75) ||
						   line upper year if location_id == `location_id', lcolor(purple) lpattern(dash) lwidth(*1.75) name(birth_counts_`location_id', replace) by(sex) title("Rh-Incompatible Pregs, `location_id'") legend(order(1 2) label(1 "Rh-Incompatible Pregnancies") label(2 "95%CI"))
					pdfappend
				}
				
				pdffinish, view
			
			}
		}
	
	
	
		
	
		
