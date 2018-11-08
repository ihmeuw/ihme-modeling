// NAME
// January 2014
// Adjusting survival estimates based on LTFU adjusted 
// This is code that's only used for vetting changes -- 01a in on_art takes the functional parts of this and applies it to the on_art results

clear all
set more off
cap log close
cap restore, not

local km_dir "FILEPATH"
local ltfu_dir "FILEPATH"
local graph_dir "FILEPATH"

import excel using "`km_dir'/HIV_extract_KM_2015.xlsx", clear firstrow 

// Predict LTFU prop dead
	generate x = 1
	preserve
	use "`ltfu_dir'/logit_logit_model_data.dta", clear
	regress logit_prop_traced_dead logit_prop_ltfu
	matrix b = e(b)
	generate beta0 = b[1,2]
	generate beta1 = b[1,1]
	keep beta0 beta1
	generate x = 1
	tempfile reg_betas
	save `reg_betas', replace
	restore
	
	merge m:m x using `reg_betas', nogen
	drop x

	// Logit space variables
	generate logit_ltfu_prop = logit(ltfu_prop)
	generate logit_ltfu_prop_dead = beta0 + beta1*logit_ltfu_prop
	generate ltfu_prop_dead = exp(logit_ltfu_prop_dead)/(1+exp(logit_ltfu_prop_dead))

// Logit-logit model for adjusting survival
generate dead_prop_adj = dead_prop+ltfu_prop*ltfu_prop_dead

// Look at what changes this made
hist dead_prop
hist dead_prop_adj
scatter dead_prop dead_prop_adj

generate dead_prop_diff = dead_prop_adj - dead_prop
hist dead_prop_diff
graph export "`graph_dir'/mort_diff_hist.png", replace
tabstat dead_prop_diff, stat(count mean max min p25 p50 p75)


preserve
keep if dead_prop_adj != .
drop if inlist(iso3, "BRB", "CHE", "DOM", "JAM", "HTI", "THA", "TTO")

bysort treat_mo_end: egen mean_dead_prop = mean(dead_prop)
drop if treat_mo_end == 3 | treat_mo_end == 36
bysort treat_mo_end: egen mean_dead_prop_adj = mean(dead_prop_adj)

hist dead_prop_diff
graph export "`graph_dir'/mort_diff_hist_ssa_only.png", replace
tabstat dead_prop_diff, stat(count mean max min p25 p50 p75)

twoway scatter mean_dead_prop treat_mo_end, mcolor(blue) || ///
		scatter mean_dead_prop_adj treat_mo_end, mcolor(green)
graph export "`graph_dir'/mort_adj_compare.png", replace

generate cd4_under_200 = 1 if cd4_end <= 200
replace cd4_under_200 = 0 if cd4_end > 200 & cd4_end != .

bysort cd4_under_200: egen mean_dead_prop_cd4 = mean(dead_prop)
bysort cd4_under_200: egen mean_dead_prop__cd4adj = mean(dead_prop_adj)

twoway scatter mean_dead_prop_cd4 treat_mo_end if cd4_under_200 == 1, mcolor(blue) || ///
		scatter mean_dead_prop__cd4adj treat_mo_end if cd4_under_200 == 1, mcolor(green)
	
twoway scatter mean_dead_prop_cd4 treat_mo_end if cd4_under_200 == 0, mcolor(blue) || ///
	scatter mean_dead_prop__cd4adj treat_mo_end if cd4_under_200 == 0, mcolor(green)
	
restore

// For studies that did their own correction, we'll keep their estimates
replace dead_prop_adj = dead_prop if ltfu_prop == 0
// Save
// outsheet using .FILEPATH, replace comma names


// We kept 18 studies (so far) - which ones are they?
preserve
keep if dead_prop_adj != .
keep pubmed_id
duplicates drop

// outsheet using .FILEPATH, replace comma names
restore

