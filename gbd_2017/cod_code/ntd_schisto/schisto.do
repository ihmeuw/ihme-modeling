

*** BOILERPLATE ***
	clear all
	set more off

	if c(os) == "Unix" {
		local j "FILEPATH"
		}
	else if c(os) == "Windows" {
		local j "FILEPATH"
		}

	run FILEPATH/build_cod_dataset.ado
	run FILEPATH/select_xforms.ado
	run FILEPATH/run_best_model.ado
	run FILEPATH/process_predictions.ado


/*

1. last year's prevalence covariate
2. Try SDI or HAQI as a covariate
3. Do an interaction term (c.Prevvarname##SDIvarname)

*/
local date "2018_06_07"

cap log using "FILEPATH/`date'.smcl", replace
if !_rc local close_log 1
else local close_log 0


*** CREATE THE DATASET ***
	local cause_id 351
	// add HAQI covariate and cumulative treatment covariate
	local covariate_ids 57 118 142 160 1099 1110
	local get_model_results gbd_team(ADDRESS) model_version_id(206510) measure_id(5)
	local saveto FILEPATH/schisto.dta
	local minAge 5

	// use this if want new covariates or if there's been a data refresh
	build_cod_dataset `cause_id', covariate_ids(`covariate_ids') get_model_results(`get_model_results') saveto(`saveto', replace) clear


	adopath + "FILEPATH"
	get_location_metadata, location_set_id(8) clear
	preserve
	keep if parent_id == 196
	levelsof location_id, local(sa_subs)
	restore
	keep if parent_id == 135
	levelsof location_id, local(brazil_subs)
*** FIND BEST VARIABLE TRANSFORMATIONS ***
	use FILEPATH/schisto.dta, clear

*** RUN BEST MODEL AS MIXED EFFECTS ***

// variable management

	generate lnPrev_22 = ln(prevalence_206510_22 + 0.000001)
    generate logitWater = logit(water_prop)
    generate logLDI = ln(LDI_pc)
    gen lnPrev = ln(prevalence_206510 + 0.000001)

    ** generate cases
    gen cases = prevalence_206510 * population
    *generate hsa_impr_water = hsa * impr_water

   	** create lag variables
   	* FIND R^2 AND RMSE AND SAVE A FILE WITH ALL OF THEM
   	forvalues i = 5(5)25 {
   		preserve
	   	keep lnPrev lnPrev_22 location_id age_group_id sex_id year_id
	   	replace year_id = year_id + `i'
	   	drop if year_id > 2017
	   	local expand = `i' + 1
	   	expand `expand' if year_id == 1980 + `i'
	   	bysort year_id location_id sex_id age_group_id: replace year_id = year_id - _n + 1 if year_id == 1980 + `i'
	   	rename lnPrev lnPrev_lag`i'
	   	rename lnPrev_22 lnPrev_22_lag`i'
	   	tempfile lag`i'
	   	save `lag`i'', replace
	   	restore
	   	merge m:m year_id location_id sex_id age_group_id using `lag`i'', nogen keep(1 3)
   	}

   	** create the correct lag
   	local lag = 15
   	local i 15


   	** create a South Africa covariate
    gen south_africa = 0
    foreach loc of local sa_subs {
    	replace south_africa = 1 if location_id == `loc'
    }

	//run model with robust standard errors:
	*poisson study_deaths year_egypt_1 year_egypt_2 year_china_1 year_china_2 lnPrev i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult


	*run_best_model poisson study_deaths year_egypt_1 year_egypt_2 year_china_1 year_china_2 lnPrev i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult


	// create sample_size numbers for new rate and case fatality rate models

	// change exposure to necessary numbers for cf model and rate model
	// if running poisson, put vce(robust) to make it a quasi-poisson model
	// model with extra covariates
	*run_best_model nbreg study_deaths lnPrev logitWater haqi logLDI i.age_group_id i.sex_id yearS*, exposure(sample_size) vce(robust) difficult
	// model with only ln prevalence,
	// try without splines and without year, try with i.super_region_id, , modelable
	// now no longer has year spline
	*run_best_model nbreg study_deaths year_egypt_1 year_egypt_2 year_china_1 year_china_2 lnPrev i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult

	** pull in geographic restrictions
	preserve

	import delimited "FILEPATH/geo_restrict_schisto.csv", clear
	keep if pre1980 == "pp" | pre1980 == "pa"
	rename pre1980 endemicity
	replace endemicity = "1" if endemicity == "pp"
	replace endemicity = "0" if endemicity == "pa"
	destring endemicity, replace
	rename ihme_lc_id ihme_loc_id
	rename loc_id location_id
	rename loc_name location_name

	keep ihme_loc_id location_name location_id endemicity
	tempfile endemic
	save `endemic', replace
	restore
	** merge on to current dataset
	merge m:1 location_id using `endemic', keepusing(endemic) nogen
	replace endemic = 0 if endemic == .
	** create an endemic Brazil subnational covariate
	gen brazil_sub = 0
	foreach loc of local brazil_subs {
		replace brazil_sub = 1 if location_id == `loc' & endemic == 1
	}


** DIAGNOSTICS **
/*
	preserve
	keep if age_group_id == 22
	local cov_og "lnPrev lnPrev_22"
	local covs "`cov_og'"
	forvalues i = 5/15 {
		foreach cov of local cov_og {
			local covs "`covs' `cov'_lag`i'"
		}
	}

	local covs "`covs' haqi water_prop_22 water_prop_27 LDI_pc_22 LDI_pc LDI_pc_27 pop_dens_over_1000_psqkm_pct sanitation_prop lnPrev logitWater logLDI"

	foreach cov of local covs {
		scatter `cov' rate, saving(FILEPATH)
	}

	graph combine `graphnames', saving(FILEPATH)
*/

	// now trying with brazil subnational indicator variable, and without China year spline
	** this works
	/*
	nbreg study_deaths lnPrev i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult
	nbreg study_deaths lnPrev_lag15 i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult
	nbreg study_deaths lnPrev_lag6 logitWater i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult
	nbreg study_deaths lnPrev_lag5 i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult
	** trying out mixed effects model
	menbreg  study_deaths i.age_group_id i.sex_id lnPrev_lag5 year_id logitWater if endemic == 1, exposure(sample_size) || location_id: R.age_group_id
	menbreg  deaths i.age_group_id i.sex_id lnPrev_lag5 year_id logitWater, exposure(population) || location_id: R.age_group_id
*/

	// set local i, loop through 5/15
	// set up empty file, for age: 0 is age-specific, 22 is all-age

	/*
	preserve
	clear
	gen age = .
	gen z_score = .
	gen r_2 = .
	gen lag = .
	gen aic = .
	gen bic = .
	set obs 1
	tempfile output
	save `output', replace
	tempfile results
	save `results', replace
	restore

	forvalues i = 5/15 {

	nbreg study_deaths lnPrev_lag`i' i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult
	estat ic
	matrix a = r(S)
	local aic = a[1,5]
	local bic = a[1,6]
	local z_score = _b[lnPrev_lag`i']/_se[lnPrev_lag`i']
	local r_2 = `e(r2_p)'
	preserve
	use `output', clear
	replace aic = `aic'
	replace bic = `bic'
	replace lag = `i'
	replace z_score = `z_score'
	replace r_2 = `r_2'
	replace age = 0
	append using `results'
	save `results', replace
	restore

	nbreg study_deaths lnPrev_22_lag`i' i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult
	estat ic
	matrix a = r(S)
	local aic = a[1,5]
	local bic = a[1,6]
	local z_score = _b[lnPrev_22_lag`i']/_se[lnPrev_22_lag`i']
	local r_2 = `e(r2_p)'
	preserve
	use `output', clear
	replace aic = `aic'
	replace bic = `bic'
	replace lag = `i'
	replace z_score = `z_score'
	replace r_2 = `r_2'
	replace age = 1
	append using `results'
	save `results', replace
	restore

}
*/

** test runs
** run_best_model nbreg study_deaths lnPrev_lag5 south_africa logitWater i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult
**run_best_model nbreg study_deaths lnPrev_lag10 south_africa brazil_sub i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult
**run_best_model nbreg study_deaths lnPrev_lag12 south_africa brazil_sub LDI_pc_27 haqi i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult

* 27 degrees of freedom
forvalues i = 5(5)35 {
	preserve
	run_best_model nbreg study_deaths south_africa brazil_sub c.lnPrev_lag`i'##c.haqi i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult

	predict yhat
	gen diffsquared = (study_deaths-yhat)^2
	sum(diffsquared)
	display r(sum)
	local rmse = sqrt(`r(sum)'/27)
	di `rmse'

	restore

}
** 10



run_best_model nbreg study_deaths south_africa brazil_sub c.lnPrev_lag15##c.haqi i.age_group_id i.sex_id year_id , exposure(sample_size) vce(robust) difficult
tempfile model
save `model', replace
* getting r^2
estat ic
matrix a = r(S)
local aic = a[1,5]
local bic = a[1,6]
local z_score = _b[lnPrev_22_lag`i']/_se[lnPrev_22_lag`i']
local r_2 = `e(r2_p)'



test [lndelta]_cons = 1
test [lnalpha]_cons = 1

** run_best_model menbreg deaths i.age_group_id i.sex_id lnPrev_lag5 year_id logitWater, exposure(cases_lag5) difficult reffects(|| location_id: R.age_group_id)

/*
** fixed effects negative binomial cause fraction model
	run_best_model nbreg study_deaths lnPrev_lag5 i.age_group_id i.sex_id year_id, exposure(sample_size) vce(robust) difficult
** mixed effects negative binomial cause fraction model
	run_best_model menbreg study_deaths i.age_group_id i.sex_id lnPrev_lag5 year_id logitWater if endemic == 1, exposure(sample_size) difficult reffects(|| location_id: R.age_group_id)
	run_best_model menbreg study_deaths i.age_group_id i.sex_id lnPrev_lag5 year_id logitWater, exposure(sample_size) difficult reffects(|| location_id: R.age_group_id)
** mixed effects negative binomial rate space model
	run_best_model menbreg deaths i.age_group_id i.sex_id lnPrev_lag5 year_id logitWater, exposure(population) difficult reffects(|| location_id: R.age_group_id)

	*run_best_model menbreg study_deaths lnPrev i.age_group_id i.sex_id yearS*, exposure(sample_size) vce(robust) difficult || super_region_id:
	// create a variable equal to the location_id for brazil subnationals, and put that variable as an indicator (i.) variable
	*run_best_model poisson study_deaths c.ageS*##i.super_region_id c.yearS*##i.super_region_id i.sex_id##i.super_region_id lnPrev logitWater haqi logLDI, exposure(sample_size) vce(robust) difficult

	* Save this file in case something breaks!
	*save FILEPATH/model_fit.dta
	*/

*** SUBMIT JOBS TO CREATE DRAWS AND SAVE RESULTS ***
	process_predictions `cause_id', link(logit) random(no) min_age(5) multiplier(envelope) endemic(endemic) project("proj_custom_models") description("15 year lag, ethiopia still outliered")

**	process_predictions `cause_id', link(ln) random(yes) min_age(5) multiplier(cases_lag5) description("with all outliers in Africa, mixed effects model, exposure: cases")
** cause fraction: sample_size --> envelope, outcome: study_deaths
** rate space: population --> population, outcome: deaths -- severely underestimated
** cases --> cases (case fatality), outcome: deaths
