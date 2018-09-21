/* **************************************************************************
NEONATAL REGRESSIONS: META-ANALYSIS
*/

clear all
set more off
set maxvar 32000
//ssc install estout, replace 

/*  //////////////////////////////////////////////
		WORKING DIRECTORY
////////////////////////////////////////////// */

		//root dir
	if c(os) == "Windows" {
		local j /*FILEPATH*/
		// Load the PDF appending application
		quietly do /*FILEPATH*/
	}
	if c(os) == "Unix" {
		local j /*FILEPATH*/
		
	} 
	
	di in red /*FILEPATH*/

/* /////////////////////////
///Prep: Pass , parameters,
/// set up logs, etc.
///////////////////////// */

adopath + /*FILEPATH*/

 //actual passed parameters

local acause "`1'"
local grouping "`2'"
local covariates "`3'"
local random_effects "`4'"
local parent_dir "`5'"
local in_dir "`6'"
local timestamp "`7'"


di in red "acause is `acause'"
di in red "grouping is `grouping'"
di in red "covariates are `covariates'"
di in red "random effects are `random_effects'"
di in red "parent dir is `parent_dir'"
di in red "in_dir is `in_dir'"
di in red "timestamp is `timestamp'" 

//make output directories and archive files
local out_dir /*FILEPATH*/
capture mkdir "`out_dir'"
local dirs_to_make draws summary
foreach dirname of local dirs_to_make{
	local `dirname'_out_dir /*FILEPATH*/
	local archive_`dirname'_out_dir /*FILEPATH*/
	capture mkdir /*FILEPATH*/
	capture mkdir /*FILEPATH*/
}

while regexm("`covariates'", "__")==1{
	local covariates = regexr("`covariates'", "__", " ")
}
di in red "covariates are `covariates'"

while regexm("`random_effects'", "__")==1{
	local random_effects = regexr("`random_effects'", "__", " ")
}
di in red "random_effects are `random_effects'"


/* /////////////////////////
/// Import data prepared in 
/// step 01_dataprep
///////////////////////// */

di in red "importing data"
use "`in_dir'", clear


gen lt_mean = logit(mean)

local cov_count: word count `covariates'
local re_count: word count `random_effects'
local re_name 
foreach re of local random_effects{
	local re_name `re_name' || `re':
}

/* /////////////////////////////////////////////////////////////////////
/// Run the mixed effects model.  Note that we have transformed the data into
/// logit space to ensure that our predictions stay in the domain of 
/// prevalences/proportions [0,1].///////////////////////////////////// */

xtmixed lt_mean `covariates' `re_name'


/////////////////////////////////////////////////////////////////////
/// Predict for fixed effects:
///////////////////////////////////////////////////////////////////// 

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
////////////////////////////////////////////////////////////////////// */

//1. Merge fixed/random effects
di in red "merging fixed and random effects"
preserve
	use `fe_sims', clear 
	forvalues re_idx = 1/`re_count'{ // re_count = Number of random effects 
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

		preserve
		collapse(mean) year, by(sex)
		count
		if `r(N)'>1{
			di in red "ERROR: REGRESSION WITH BOTH SEX-SPECIFIC AND NON-SPECIFIC VALUES."
			BREAK
		}
	restore
	drop sex
	expand 2, gen(sex)
	replace sex=2 if sex==0
}


//save draws
di in red "saving all draws!"
preserve
	keep location_id year sex draw*
	save /*FILEPATH*/, replace
	outsheet using /*FILEPATH*/, comma replace
	save /*FILEPATH*/, replace
	outsheet using /*FILEPATH*/, comma replace
restore
	
//save summary stats
rename mean data_val
egen mean = rowmean(draw*)
fastpctile draw*, pct(2.5 97.5) names(lower upper)
drop draw*
	
save /*FILEPATH*/, replace
export delimited using /*FILEPATH*/, replace 
export delimited using /*FILEPATH*/, replace
save /*FILEPATH*/, replace

