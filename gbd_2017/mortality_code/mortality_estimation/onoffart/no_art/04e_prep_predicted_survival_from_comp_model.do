// NAME
// 24 Feb 2014
// Format predicted survival curves from compartmental model for graphing in R

**************************************************************
** SET UP
**************************************************************
clear all
set maxvar 20000
set more off
cap restore, not

if (c(os)=="Unix") {
	global root "ADDRESS"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
}

local ages "15_25 25_35 35_45 45_100"
local count = 1

set seed 100

local comp_dir "FILEPATH"

**************************************************************
** COMPILE SURVIVAL PREDICTIONS
**************************************************************

foreach a of local ages {

	
	
	insheet using "`comp_dir'/FILEPATH/`a'_sample_survival_predictions.csv", clear comma names
	
	forvalues i = 1/32 {
		local j = `i'-1
		rename v`i' surv`j'
		cap replace surv`j' = "" if surv`j' == "NA"
		cap destring surv`j', replace
	}
	generate draw = _n
	reshape long surv, i(draw) j(year)
	generate age = "`a'"
	
	
	tempfile surv
	save `surv', replace
	
	insheet using "`comp_dir'/FILEPATH/`a'_sample_mortality.csv", clear comma names
	generate draw = _n
	keep draw no_converge
	
	merge 1:m draw using `surv'
	keep if _m == 3
	drop _m
	
	if `count' == 1 {
		tempfile surv_pred
		save `surv_pred', replace
	}
	else {
		append using `surv_pred'
		save `surv_pred', replace
	}
	
	local count = `count' + 1
	
}

tempfile surv_preds
save `surv_preds', replace

**************************************************************
** SAVE FOR MEDIAN SURVIVAL ANALYSIS
**************************************************************
save "`comp_dir'/FILEPATH/surv_draws.dta", replace

**************************************************************
** RESAMPLE FROM CONVERGED DRAWS
**************************************************************
// Deal with convergence - note that this really should be coordinated with the parameter sampling....
// replace surv = . if inlist(draw, 43, 398, 1019, 1329, 1538)
// replace surv = . if inlist(draw, 429, 527, 539, 567, 600, 670, 814, 1235, 1561)
*marking surv curves above 1 for resampling
generate above_1 = 0 
replace above_1 = 1 if surv>1 & surv!= .  
bysort draw: egen drop_draw_above = total(above_1)
bysort draw: egen drop_draw_no_converge = total(no_converge) 
generate drop_draw = drop_draw_above+drop_draw_no_converge
replace surv = . if drop_draw != 0

drop drop_draw no_converge drop_draw_above drop_draw_no_converge above_1

forvalues i = 1/4 {
	bysort draw: generate replacement_draw`i' = floor(1001*runiform() + 1000) if surv == . & year == 1 & age == "15_25" & draw <= 1000
	bysort draw: egen rep_draw`i' = max(replacement_draw`i')
	drop replacement_draw`i'
}


levelsof rep_draw1, local(reps1)
levelsof rep_draw2, local(reps2)
levelsof rep_draw3, local(reps3)
levelsof rep_draw4, local(reps4)

reshape wide surv rep_draw1 rep_draw2 rep_draw3 rep_draw4, i(age year) j(draw)

forvalues i = 1/1000 {
	di as text "`i' trying replacement draw 1"
	foreach j of local reps1 {
		qui replace surv`i' = surv`j' if rep_draw1`i' == `j'
		cap assert surv`i' != .
		
		if _rc != 0 {
			di in red "`i' trying replacement draw 2"
			foreach k of local reps2 {
				qui replace surv`i' = surv`k' if rep_draw2`i' == `k' & surv`i' == .
			}
			cap assert surv`i' != .
		}
		if _rc != 0 {
			di in red "`i' trying replacement draw 3"
			foreach l of local reps3 {
				qui replace surv`i' = surv`l' if rep_draw3`i' == `l' & surv`i' == .
			}
			cap assert surv`i' != .
		}	
		if _rc != 0 {
			di in red "`i' trying replacement draw 4"
			foreach m of local reps4 {
				qui replace surv`i' = surv`m' if rep_draw4`i' == `m' & surv`i' == .
			}
			assert surv`i' != .
		}				
	}
}
	
	
**************************************************************
** FORMAT FOR GRAPHING
**************************************************************
drop rep_draw*
reshape long surv, i(age year) j(draw)
rename year yr_since_sc
keep if draw <= 1000

**************************************************************
** SAVE DRAW-LEVEL SURVIVAL PREDICTIONS
**************************************************************
outsheet using "`comp_dir'/FILEPATH/survival_curve_preds.csv", replace comma names

// 95% confidence intervals
preserve
	sort age yr_since_sc surv
	by age yr_since_sc: generate surv_lower = surv[25]
	by age yr_since_sc: generate surv_upper = surv[975]
	by age yr_since_sc: egen surv_mean = mean(surv)
	drop draw surv
	duplicates drop 
	keep if yr_since_sc <= 12
	outsheet using "`comp_dir'/FILEPATH/survival_uncertainty.csv", replace comma names
restore

**************************************************************
** MEDIAN SURVIVAL CALCULATIONS
**************************************************************
preserve
	// Calculate median survival
	generate before_surv_half = 0 if surv > 0.5 & surv != .
	replace before_surv_half = 1 if surv <= 0.5
	sort age draw yr_since_sc
	bysort age draw: generate midpoint = (surv + surv[_n-1])/2
	bysort age draw: generate x = yr_since_sc[_n-1] + midpoint if before_surv_half == 1 & before_surv_half[_n-1] == 0
	bysort age draw: egen median_survival = max(x)
	drop x	
	
	// 95% confidence intervals
	keep age draw median_survival
	duplicates drop
	sort age median_survival
	bysort age: generate median_surv_lower = median_survival[25]
	bysort age: generate median_surv_upper = median_survival[975]
	bysort age: egen median_surv_mean = mean(median_survival)
		
	outsheet using "`comp_dir'/FILEPATH/median_survival.csv", replace comma names
	
	tabstat median_survival median_surv_lower median_surv_upper, by(age) stat(mean)
restore



