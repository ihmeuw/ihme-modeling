// NAME
// February 2014
// Regress logit conditional mortality on age and years since seroconversion with a random effect on study ID.  Predict out to capture uncertainty.

**************************************************************
** SET UP
**************************************************************
clear all
set more off
cap log close
cap restore, not

// Initialize pdfmaker
if c(os) == "Windows" {
	global prefix "ADDRESS"
	do "FILEPATH"
}
if c(os) == "Unix" {
	global prefix "ADDRESS"
	do "FILEPATH"
	set odbcmgr unixodbc
}

local data_dir "FILEPATH"
local log_dir "FILEPATH"
local figure_dir "FILEPATH"

set seed 4040

// Toggles
local visuals = 1
local save_draws_graph = 1
local median_survival = 1
local save_draws_comp_model = 1

**************************************************************
** PREP DATA FOR REGRESSION
**************************************************************
use "`data_dir'/FILEPATH/hiv_specific_mort.dta", clear

label define age_labs 1 "15_25" 2 "25_35" 3 "35_45" 4 "45_100"
label values age_cat age_labs

// Mortality
drop death_rate_adj cum_mort_adj
rename cond_prob_death_adj hazard_death_adj
generate hazard_surv_adj = 1-hazard_death_adj
generate surv = 1 - mort

// Logit transformation - deal with zeroes
replace hazard_death_adj = 0.005 if hazard_death_adj < 0.005
generate logit_hazard_death = logit(hazard_death_adj)

tempfile prepped
save `prepped', replace

**************************************************************
** REGRESSION
**************************************************************
log using "`log_dir'/re_study_logit_hazard.log", replace
mixed logit_hazard ib0.age_cat i.yr_since_sc || pubmed_id:
log close

**************************************************************
** PREDICTION PREP
**************************************************************

// Prediction
	// Recover beta point estimates and variance-covariance matrix
	matrix v = e(V)
	matrix var_covar = v[1..18, 1..18]
	matrix resid_covar = v[19..20, 19..20]
	matrix b = e(b)
	matrix betas = b[1, 1..18]
	
	// RMSE for residuals and random effects
	matrix x = b[1, 19..20]
	local rese = exp(b[1, 19])
	local rmse = exp(b[1, 20])
	
	// Create list of variable names for drawnorm to populate.  Stata can't handle variable names that start with 1 or have periods in them
	local covars: rownames var_covar
	local beta_names ""
	foreach var of local covars {
		local x: subinstr local var "." ""
		local beta_names = "`beta_names'"+" "+"b_"+"`x'"
	}
	
	
************************************************
** DATASET WITH ALL POSSIBLE STRATA FOR PREDICTIONS
************************************************

// Rectangularize
local vars "age_cat yr_since_sc"
keep `vars'
duplicates drop
fillin `vars'
drop if age_cat == 0
foreach var of local vars {
	cap drop if `var' == .
	cap drop if `var' == ""
}
drop _fillin
tempfile pred
save `pred', replace

// Create space for 1000 draws from regression.  In this case, we're going to expand to 2000 draws and keep 1000 later after we see which ones converge in the compartmental model.  Also, we are only going to draw 2000 draws total, not 2000 for each age group.  For each draw, we're going to predict each age group, so draw 1 is the same for each age group.
expand 2000
order age_cat yr_since_sc
bysort age_cat yr_since_sc: generate draw = _n

// Draw
drawnorm `beta_names', means(betas) cov(var_covar)

// We want to predict all years for each draw, whereas the previous command had separate draws for each year
foreach var of varlist b_* {
	replace `var' = . if age_cat != 1 | yr_since_sc != 1
	sort draw yr_since_sc age_cat
	by draw: replace `var' = `var'[_n-1] if `var'==.
}

// Add the random effects
drawnorm re, means(0) sds(`rese')
replace re = . if yr_since_sc != 1 | age_cat != 1
sort draw yr_since_sc age_cat
by draw: replace re = re[_n-1] if re == .

************************************************
** PREDICTIONS
************************************************
// Fixed effect reference categories need to be dropped 
drop b_0bage_cat
replace b_1byr_since_sc = 0


// Need to deal with the fixed effects - set to zero if it doesn't correspond to the variable
levelsof age_cat, local(ages)
levelsof yr_since_sc, local(yrs)

foreach a of local ages {
	replace b_`a'age_cat = 0 if age_cat != `a'
}

foreach y of local yrs {
	if `y' != 1 {
		replace b_`y'yr_since_sc = 0 if yr_since_sc != `y'
	}
}

// Predicted y in logit space
egen logit_y = rowtotal(b_1age_cat-re)
generate cond_y = exp(logit_y)/(1+exp(logit_y))

tempfile preds
save `preds', replace

// Convert back to cumulative mortality - NOTE THAT THIS IS FRAME-SHIFTED FORWARD
generate cum_y = 0 if yr_since_sc == 1
replace yr_since_sc = 0 if yr_since_sc == 1
keep if yr_since_sc == 0
append using `preds'

keep age_cat draw yr_since_sc cum_y cond_y

// Convert to cumulative probabilities -
sort age_cat draw yr_since_sc
bysort age_cat draw: replace cond_y = cond_y[_n+1]
bysort age_cat draw: replace cum_y = cum_y[_n-1] + cond_y[_n-1] - cum_y[_n-1]*cond_y[_n-1] if yr_since_sc != 0

tempfile draw_predictions
save `draw_predictions', replace

tempfile median_survival_data
save `median_survival_data', replace

rename cum_y y	
drop cond_y

tempfile predictions
save `predictions', replace
	
**************************************************************
** SAVE FOR GRAPHING IN R
**************************************************************
if `save_draws_graph' == 1 {
	outsheet using "`data_dir'/FILEPATH/logit_hazard_draws_re_study.csv", replace comma names
}

**************************************************************
** SAVE FOR COMPARTMENTAL MODEL
**************************************************************	
use `predictions', clear

generate surv_prob = 1-y
keep age_cat yr_since_sc draw surv_prob
generate age_group = "15_25" if age_cat == 1
replace age_group = "25_35" if age_cat == 2
replace age_group = "35_45" if age_cat == 3
replace age_group = "45_100" if age_cat == 4
drop age_cat

rename draw row

tempfile all
save `all', replace

levelsof age_group, local(ages)

foreach a of local ages {
			
	// local s "male"
			
	preserve
	keep if age_group == "`a'"
	reshape wide surv_prob, i(row age_group) j(yr_since_sc)
	drop surv_prob0
	outsheet using "`data_dir'/FILEPATH/`a'.csv", replace comma names
	restore
			
			
}

**************************************************************
** MEDIAN SURVIVAL AT DRAW LEVEL
**************************************************************
if `median_survival' == 1 {
preserve
	use `median_survival_data', clear

	rename cum_y cum_mort
	rename cond_y cond_mort
	generate cum_surv = 1-cum_mort
	generate cond_surv = 1-cond_mort

	// Predict survival out to 30 years
	reshape wide cum_mort cond_mort cum_surv cond_surv, i(age_cat draw) j(yr_since_sc)

	replace cond_mort12 = cond_mort11
	replace cond_surv12 = 1-cond_mort12

	forvalues i = 13/30 {
		foreach var in cond_mort cond_surv cum_mort cum_surv {
			generate `var'`i' = .
		}
		replace cond_mort`i' = cond_mort12
		replace cond_surv`i' = 1-cond_mort`i'
	}

	reshape long cond_mort cond_surv cum_mort cum_surv, i(age_cat draw) j(yr_since_sc)
	bysort age_cat draw: replace cum_mort = cond_mort[_n-1] + cum_mort[_n-1] - cond_mort[_n-1]*cum_mort[_n-1] if yr_since_sc != 1 & cum_mort == .
	replace cum_surv = 1 - cum_mort if cum_surv == .
			
	// Calculate median survival
	generate before_surv_half = 0 if cum_surv > 0.5 & cum_surv != .
	replace before_surv_half = 1 if cum_surv <= 0.5
	sort age_cat draw yr_since_sc
	bysort age_cat draw: generate midpoint = (cum_surv + cum_surv[_n-1])/2
	bysort age_cat draw: generate x = yr_since_sc[_n-1] + midpoint if before_surv_half == 1 & before_surv_half[_n-1] == 0
	bysort age_cat draw: egen median_survival = max(x)
	drop x	

	tabstat median_survival, by(age_cat) stat(mean min max)
	
	// 95% confidence intervals
	keep age_cat draw median_survival
	duplicates drop
	sort age_cat median_survival
	bysort age_cat: generate median_surv_lower = median_survival[25]
	bysort age_cat: generate median_surv_upper = median_survival[975]

	
	
	tabstat median_survival median_surv_lower median_surv_upper, by(age_cat) stat(p50)
	
restore
}


**************************************************************
** MODEL FIT GRAPHS - currently only does fixed effects w/ or w/o weights
**************************************************************
if `visuals' == 1 {

// Graph draw-level predicted uncertainty in R
preserve
	use `draw_predictions', clear
	drop cond_y
	generate cum_surv_draw = 1-cum_y
	drop cum_y
	sort age_cat yr_since_sc cum_surv_draw
	bysort age_cat yr_since_sc: egen cum_surv_lower = pctile(cum_surv_draw),p(2.5)
	bysort age_cat yr_since_sc: egen cum_surv_upper = pctile(cum_surv_draw),p(97.5)
	bysort age_cat yr_since_sc: egen cum_surv_med = median(cum_surv_draw)
	bysort age_cat yr_since_sc: egen cum_surv_mean = mean(cum_surv_draw)
	keep age_cat yr_since_sc cum_surv_lower cum_surv_upper cum_surv_mean
	duplicates drop
	
	// Save for graphing in R
	outsheet using "`data_dir'/FILEPATH/draw_uncertainty.csv", replace comma names
restore
	
}
