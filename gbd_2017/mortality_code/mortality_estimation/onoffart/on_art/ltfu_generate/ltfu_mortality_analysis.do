// NAME
// January 2014
// Estimating mortality in those LTFU

clear all
set more off
cap log close
cap restore, not

set seed 1092

local graph_dir "FILEPATH"
local in_dir "FILEPATH"
local new_dir "FILEPATH"
local data_dir "FILEPATH"

insheet using "`in_dir'/ltfu_mortality_studies.csv", clear comma names
tempfile temp_2013
save `temp_2013'

insheet using "`new_dir'/ltfu_mortality_studies_gbd2015.csv", comma clear names
append using `temp_2013'

scatter prop_traced_dead prop_ltfu
graph export "`graph_dir'/simple_scatter.png", replace

** // OLS
** log using ./ols.log
** regress prop_traced_dead prop_ltfu
** log close

// Logged
generate log_prop_traced_dead = log(prop_traced_dead)
generate log_prop_ltfu = log(prop_ltfu)
scatter prop_traced_dead log_prop_ltfu

*************
** FINAL MODELS
*************
// Drop outliers
drop if first_author == "NAME"

// Outsheet
outsheet using "`data_dir'/ltfu_scatter_for_graphing_in_r.csv", replace comma names
// Scatter
twoway scatter prop_traced_dead prop_ltfu || ///
	lfit prop_traced_dead prop_ltfu
graph export "`graph_dir'/simple_scatter_no_outlier.png", replace

// OLS
log using "`data_dir'/ols.log", replace
regress prop_traced_dead prop_ltfu
log close

// Logit space: logit(x) = ln(x/(1-x))
generate logit_prop_traced_dead = logit(prop_traced_dead)
generate logit_prop_ltfu = logit(prop_ltfu)
scatter logit_prop_traced_dead logit_prop_ltfu
graph export "`graph_dir'/simple_scatter_no_outlier_logit.png", replace

// Logit-logit regression
log using "`data_dir'/logit-logit.log", replace
regress logit_prop_traced_dead logit_prop_ltfu
predict logit_preds
generate preds = 1/(1+1/exp(logit_preds))
log close

// Logit line on scatter: logit(y) = -0.6978372*logit(x)-1.418175
sort prop_ltfu
twoway scatter prop_traced_dead prop_ltfu || ///
	scatter preds prop_ltfu, mcolor(black) connect(direct)
graph export "`graph_dir'/scatter_logit_logit_fit.png", replace

// Save data so model can be used later
preserve
keep first_author study_period location logit_prop_ltfu logit_prop_traced logit_preds preds
save "`data_dir'/logit_logit_model_data.dta", replace
outsheet using "`data_dir'/logit_logit_model_data.csv", replace comma names
restore

