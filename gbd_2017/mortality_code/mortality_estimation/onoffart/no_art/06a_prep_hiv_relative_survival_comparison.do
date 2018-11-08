// NAME
// February 2014
// Prep parameter HIV-specific survival comparison

**************************************************************
** SET UP
**************************************************************
clear all
set more off
cap restore, not

if (c(os)=="Unix") {
	global root "ADDRESS"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
}

local data_dir "FILEPATH"
local extract_dir "FILEPATH"

**************************************************************
** PREP EACH COMPONENT TO BE GRAPHED
**************************************************************

// Statistical model
insheet using "FILEPATH/output/draw_uncertainty.csv", clear comma names
rename cum_surv_lower surv_lower
rename cum_surv_upper surv_upper
rename cum_surv_mean surv_mean
rename age_cat age

generate source = "IHME Statistical Model"
tempfile stats
save `stats', replace

// Compartmental model
insheet using "FILEPATH/survival_uncertainty.csv", clear comma names

generate source = "IHME Compartmental Model"
tempfile cm
save `cm', replace

// UNAIDS compartmental model
insheet using "`FILEPATH/all_unaids_hiv_specific_prepped.csv", clear comma names
drop mort
rename surv surv_mean
generate source = "UNAIDS Compartmental Model"
drop if yr_since_sc > 12
tempfile cm_unaids
save `cm_unaids', replace

// Raw data with background mortality subtracted
insheet using "FILEPATH/hiv_specific_weibull_alpha_zaf.csv", clear comma names
generate source = "UNAIDS Weibull" if iso3 == "WEIBULL"
replace source = "ALPHA East Africa/ZAF Miners" if inlist(iso3, "ALPHA", "ZAF")
drop iso3
drop if yr_since_sc > 12

**************************************************************
** COMBINE
**************************************************************
append using `cm_unaids'
append using `cm'
append using `stats'

outsheet using "FILEPATH/compare_models.csv", replace comma names
