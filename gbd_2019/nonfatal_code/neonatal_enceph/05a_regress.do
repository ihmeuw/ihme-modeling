***************************************************************
*** Regression for CFR (case-fatality rate) for enceph

***************************************************************

*********************
** General Prep
*********************

clear all
set more off
set maxvar 32000

if c(os) == "Windows" {
	local j "J:"
}
if c(os) == "Unix" {
	local j "FILEPATH/j"		
} 
	
adopath + "FILEPATH/stata_functions"	

local acause "neonatal_enceph"
local grouping "cfr"
local covariates "haqi"
local random_effects "super_region_id"
local out_dir "FILEPATH/`acause'_`grouping'_prepped_haqi.csv"
local timestamp "09_11_19" 

di in red "acause is `acause'"
di in red "grouping is `grouping'"
di in red "covariates are `covariates'"
di in red "random effects are `random_effects'"
di in red "output dir is `out_dir'"
di in red "timestamp is `timestamp'" 

*********************
** Importing data & transform mean into log space
*********************

di in red "importing data"
import delimited "`out_dir'", clear 

gen lt_mean = logit(mean)

local cov_count: word count `covariates'
local re_count: word count `random_effects'
local re_name 
foreach re of local random_effects{
	local re_name `re_name' || `re':
}

*********************
** Mixed effects Model: xtmixed lt_mean ln_NMR || super_region_id:
*********************

/* /////////////////////////////////////////////////////////////////////
/// Run the mixed effects model.  Note that we have transformed the data into
/// logit space to ensure that our predictions stay in the domain of 
/// prevalences/proportions [0,1].
///////////////////////////////////////////////////////////////////// */

xtmixed lt_mean `covariates' `re_name'


/////////////////////////////////////////////////////////////////////
/// Predict for fixed effects:
/// 1. Take the covariates corresponding to the fixed effects, and the 
/// 	covariance of those fixed effects, from the beta and covariance matrices
/// 	(stored by default in the e(b) and e(V) matrices, respectively).  
///		Note that the intercept will be at the end of this list, not the beginning.
/// 2.  Make a list of locals whose names correspond to the entries in the matrices,
/// 	in order (remember to put the intercept last!)
/// 3. 	Use the 'drawnorm' function to generate a new dataset that contains a 
/// 	column for each fixed effect, with a thousand draws (long) for each value.
///		Save this in a temp file for later.
///////////////////////////////////////////////////////////////////// */

//1. Extract fixed effects from matrices
matrix betas = e(b)
local endbeta = `cov_count' + 1
matrix fe_betas = betas[1, 1..`endbeta']
matrix list fe_betas
matrix covars = e(V)
matrix fe_covars = covars[1..`endbeta', 1..`endbeta']
matrix list fe_covars

//2. Generate list of locals that will become column names in the new dataset
local betalist 
forvalues i = 1/`cov_count' {
	local betalist `betalist' b_`i'
}
local betalist `betalist' b_0
di in red "beta list is `betalist'"

//3. Run 'drawnorm' to predict for fixed effects.  
di in red "predicting for fixed effects"
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
		local re_name_`re_idx' : word `re_idx' of `random_effects'
		local past_re_name
		if `re_idx'!=1{
			local past_re_idx = `re_idx'-1
			local past_re_name : word `past_re_idx' of `random_effects'
		}
		
		//reduce down so we have a single row for each geography at this level
		keep `re_name_`re_idx'' `past_re_name' re_*`re_idx'
		//ADD TO NEXT ROUND:
		//drop if re_`re_idx'==.
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
///////////////////////////////////////////////////////////////////// */

//1. Merge fixed/random effects
di in red "merging fixed and random effects"
preserve
	// `fe_sims' = your fixed effects dataset
	use `fe_sims', clear	

	forvalues re_idx = 1/`re_count'{ 
		local re_name_`re_idx' : word `re_idx' of `random_effects'
		di in red "merging re `re_name_`re_idx''"
		if `re_idx'==1{
			local merge_on sim
		}
		else{
			local past_re_idx= `re_idx' -1
			local past_re_name: word `past_re_idx' of `random_effects'
			
			local merge_on sim `past_re_name'
		}
		di in red "merging on variables `merge_on'"
		cap drop _merge
		merge 1:m `merge_on' using `random_effect_`re_idx''
		count if _merge!=3
		if `r(N)' > 0{
				di in red "merge on sims not entirely successful!"
				BREAK
			}
	}
	
	drop _merge
	
	//2. Reshape wide
	di in red "reshaping"
	rename b_* b_*_draw_
	rename g_* g_*_draw_
	reshape wide b_* g_*, i(`random_effects') j(sim) 

	tempfile reshaped_covariates
	save `reshaped_covariates', replace
restore

//3. Merge back onto post-regression dataset. Merge should be perfect
di in red "merging covariates onto parent"
drop _merge
merge m:1 `random_effects' using `reshaped_covariates'

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
	
		gen lt_hat_draw_`i' = b_0_draw_`i'
		
		forvalues j=1/`cov_count'{
			local cov: word `j' of `covariates'
			replace lt_hat_draw_`i' = lt_hat_draw_`i' + b_`j'_draw_`i' * `cov'
		}
		drop b_*_draw_`i'
		
		forvalues k=1/`re_count'{
			replace lt_hat_draw_`i' = lt_hat_draw_`i' + g_`k'_draw_`i'
		}
		drop g_*_draw_`i'
		
		gen draw_`i' = invlogit(lt_hat_draw_`i')
		drop lt_hat_draw_`i'
	}
}

drop re_* lt_mean

// if you haven't run a sex-specific regression, expand the dataset to include both sexes
local sexval = sex
if `sexval'==3{
	//confirm that all sex values are equal to 3
	preserve
		//what you collapse on doesn't matter, all that matters is the 'by(sex)'.
		// we're trying to get a dataset with one observation for each sex, that's all
		collapse(mean) year, by(sex)
		count
		if `r(N)'>1{
			di in red "YOU RAN A REGRESSION WITH BOTH SEX-SPECIFIC AND NON-SPECIFIC VALUES."
			BREAK
		}
	restore
	drop sex
	expand 2, gen(sex)
	replace sex=2 if sex==0
}


/* /////////////////////////
///Save everything
///////////////////////// */

//save draws
di in red "saving all draws!"
local out_dir "FILEPATH"
preserve
	keep location_id year sex draw*
	save "`out_dir'/`acause'_`grouping'_draws_haqi.dta", replace
	outsheet using "`out_dir'/`acause'_`grouping'_draws_haqi.csv", comma replace
	di in red "`out_dir'/`acause'_`grouping'_draws_haqi.csv"
restore
	
//save summary stats
rename mean data_val
egen mean = rowmean(draw*)
fastpctile draw*, pct(2.5 97.5) names(lower upper)
drop draw*
	
save "`out_dir'/`acause'_`grouping'_summary_haqi.dta", replace
export delimited using "`out_dir'/`acause'_`grouping'_summary_haqi.csv", replace

di in red "regression complete!"	

