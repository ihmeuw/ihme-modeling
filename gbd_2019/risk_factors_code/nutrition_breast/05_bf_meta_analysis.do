** Project: RF: Suboptimal Breastfeeding
** Purpose: Conduct meta-regresion to calculate RR's for breastfeeding

// Setup
clear all
set maxvar 10000
set more off

// Set locals
	local in_dir "FILEPATH"
	adopath + "FILEPATH"
	local out_dir "FILEPATH"
	local save_results 0

***************************************************************
***********************NON-EXCLUSIVE***************************
***************************************************************
set seed 12345
import delimited "FILEPATH", clear
gen standard_error = (upperlimitofratio95ci - ratio)/1.96
sum standard_error, detail
replace standard_error = `r(p90)' if standard_error == . | standard_error < 0
replace outcome_clean = "lri" if outcome == "respiratory infection" & outcome_clean == ""
replace outcome_clean = "lri" if outcome == "respiratory infections" & outcome_clean == ""
drop if exposure_clean == ""
tempfile master
save `master', replace

// subset dataset appropriately
	use `master', clear
	rename (year country) (year_id location_name)
	keep if age_lower_years <= .5

// subsetting the dataset on outcome and run separate meta-analysis for LRI and diarrhea
	gen log_rr = log(ratio)
	replace outcome_clean = "lri" if outcome_clean == "resp"
	replace outcomemeasure ="Morbidity" if outcomemeasure != "Mortality" & outcomemeasure != ""
	gen variance_log = standard_error^2  * (1/ratio)^2
	gen se_log = sqrt(variance_log)
	tempfile both
	save `both', replace

	//meta-regression by outcome
	foreach outcome in lri diarrhea {
		use `both', clear
		keep if outcome_clean == "`outcome'"
		xi: metareg log_rr i.exposure_clean, wsse(se_log) eform

		**save beta coefficient
		matrix m = e(b)
		matrix C = e(V)

		local beta
		capture set obs 1000
		drawnorm beta_part beta_pred beta_int, n(1000) means(m) cov(C) cstorage(full) clear
		gen n = _n
		gen reference = 0
		reshape wide beta_part beta_pred beta_int, i(reference) j(n)
		tempfile beta
		save `beta', replace

		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_pred`n')
		}
		keep rr_*
		gen cat = "pred_`outcome'"
		tempfile rr
		save `rr', replace
	// Predominant, outcome, and morbidity RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_part`n')
		}
		keep rr_*
		gen cat = "part_`outcome'"
		append using `rr'
		save `rr', replace
	// Predominant, outcome, and mortality RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n')
		}
		keep rr_*
		gen cat = "none_`outcome'"
		append using `rr'
		save `rr', replace

	// Check means and 95% intervals
		fastrowmean rr_*, mean_var_name(mean)
		fastpctile rr_*, pct(2.5 97.5) names(lower upper)
		order cat mean lower upper
		tempfile `outcome'
		save ``outcome'', replace
}
	append using `lri'
	tempfile rr
	save `rr', replace

// Transform and conduct regression
	gen log_rr = log(ratio)
	replace outcome_clean = "lri" if outcome_clean == "resp"
	replace outcomemeasure ="Morbidity" if outcomemeasure != "Mortality" & outcomemeasure != ""
	gen variance_log = standard_error^2  * (1/ratio)^2
	gen se_log = sqrt(variance_log)
	xi: metareg log_rr i.exposure_clean i.outcome_clean i.outcomemeasure, wsse(se_log) eform

**save beta coefficient
	matrix m = e(b)
	matrix m = m[1,1..5]

**save variance covariance matrix
	matrix C = e(V)
	matrix C = C[1..5,1..5]
	
	local beta
	capture set obs 1000
	drawnorm beta_part beta_pred beta_lri beta_mort beta_int, n(1000) means(m) cov(C) cstorage(full) clear
	gen n = _n
	gen reference = 0
	reshape wide beta_part beta_pred beta_lri beta_mort beta_int, i(reference) j(n)
	tempfile beta
	save `beta', replace

**use beta/intercept draws to predict out values
	// Predominant, lri, and mortality RR
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_pred`n' + beta_lri`n' + beta_mort`n')
		}
		keep rr_*
		gen cat = "pred_lri_mort"
		tempfile rr
		save `rr', replace
	// Predominant, lri, and morbidity RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_pred`n' + beta_lri`n')
		}
		keep rr_*
		gen cat = "pred_lri_morb"
		append using `rr'
		save `rr', replace

	// Predominant, diarrhea, and mortality RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_pred`n' + beta_mort`n')
		}
		keep rr_*
		gen cat = "pred_diar_mort"
		append using `rr'
		save `rr', replace

	// Predominant, diarrhea, and morbidity RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_pred`n')
		}
		keep rr_*
		gen cat = "pred_diar_morb"
		append using `rr'
		save `rr', replace
	
	// Partial, lri, and mortality RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_part`n' + beta_lri`n' + beta_mort`n')
		}
		keep rr_*
		gen cat = "part_lri_mort"
		append using `rr'
		save `rr', replace

	// Partial, lri, and morbidity RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_part`n' + beta_lri`n')
		}
		keep rr_*
		gen cat = "part_lri_morb"
		append using `rr'
		save `rr', replace

	// Partial, diar, and mortality RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_part`n' + beta_mort`n')
		}
		keep rr_*
		gen cat = "part_diar_mort"
		append using `rr'
		save `rr', replace
	
	// Partial, diar, and morbidity RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_part`n')
		}
		keep rr_*
		gen cat = "part_diar_morb"
		append using `rr'
		save `rr', replace

	// None, lri, and mortality RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_lri`n' + beta_mort`n')
		}
		keep rr_*
		gen cat = "none_lri_mort"
		append using `rr'
		save `rr', replace
	
	// None, lri, and morbidity RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_lri`n')
		}
		keep rr_*
		gen cat = "none_lri_morb"
		append using `rr'
		save `rr', replace
	
	// None, diarrhea, and mortality RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_mort`n')
		}
		keep rr_*
		gen cat = "none_diar_mort"
		append using `rr'
		save `rr', replace
	
	// None, diarrhea, and morbidity RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n')
		}
		keep rr_*
		gen cat = "none_diar_morb"
		append using `rr'
		save `rr', replace

	// Check means and 95% intervals
	fastrowmean rr_*, mean_var_name(mean)
	fastpctile rr_*, pct(2.5 97.5) names(lower upper)
	order cat mean lower upper

****************************************
********save_results Prep***************
****************************************
	// Set up template for counterfactual category (exclusively breastfed)
		clear
		set obs 2
		forvalues x = 1/1000 {
			gen rr_`x' = 1
		}
		gen cat = "lri" if _n == 1
		replace cat = "diar" if _n == 2
		gen cause_id = 322 if _n == 1
		replace cause_id = 302 if _n == 2
		gen mortality = 1
		gen morbidity = 1
		gen parameter = "cat4"
		tempfile exclusive
		save `exclusive', replace

	// Tempfile each outcome
		use `rr', replace
		append using `exclusive'
		tempfile full_rr
		save `full_rr', replace
	
	// Loop through each outcome and generate necessary specifications for save_results
		foreach outcome in diar lri {
			use `full_rr', clear
			keep if regexm(cat, "`outcome'")
				replace cause_id = 322 if "`outcome'" == "lri"
				replace cause_id = 302 if "`outcome'" == "diar"
				replace mortality = 1
				replace morbidity = 1
				replace parameter = "cat1" if regexm(cat, "none")
				replace parameter = "cat2" if regexm(cat, "part")
				replace parameter = "cat3" if regexm(cat, "pred")
			tempfile `outcome'
			save ``outcome'', replace
		}

	append using `diar'
	rename rr_1000 rr_0
	gen age_group_id = .

// duplicate out for each age_group_id
	foreach x in 3 4 {
		replace age_group_id = `x'
		tempfile temp_`x'
		save `temp_`x'', replace
	}

	use `temp_3', clear
	append using `temp_4'

	** Save mean/lower/upper 
	gen sex_id = .
	foreach sex in 1 2 {
		replace sex_id = `sex'
		export delimited "FILEPATH", replace
	}


*****************************************************************************
***************DISCONTINUED**************************************************
*****************************************************************************
use `master', clear
drop if age_lower_years <= .5 | exclude == 1

// Transform and conduct regression
	gen log_rr = log(ratio)
	keep if outcome_clean == "diarrhea"
	replace outcomemeasure ="Morbidity" if outcomemeasure != "Mortality" & outcomemeasure != ""
	gen variance_log = standard_error^2  * (1/ratio)^2
	gen se_log = sqrt(variance_log)
	metan log_rr se_log, eform random
	
	local rr_mean  = 1.31
	local upper = 1.552 
	local lower = 1.103
	local sd = ((ln(`upper')) - (ln(`lower'))) / (2*invnormal(.975))
	forvalues draw = 0/999 {
		gen rr_`draw' = exp(rnormal(ln(`rr_mean'), `sd'))  
	}
	fastrowmean rr_*, mean_var_name(mean)
	fastpctile rr_*, pct(2.5 97.5) names(lower upper)

**save beta coefficient
	matrix m = e(b)
	matrix m = m[1,1..2]

**save variance covariance matrix
	matrix C = e(V)
	matrix C = C[1..2,1..2]
	
	local beta
	capture set obs 1000
	drawnorm beta_mort beta_int, n(1000) means(m) cov(C) cstorage(full) clear
	gen n = _n
	gen reference = 0
	reshape wide beta_mort beta_int, i(reference) j(n)
	tempfile beta
	save `beta', replace

**use beta/intercept draws to predict out values
	// Discontinued, diarrhea, and mortality RR
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n' + beta_mort`n')
		}
		keep rr_*
		gen cat = "dis_diar_mort"
		tempfile rr
		save `rr', replace
	// Discontinued, diarrhea, and morbidity RR
		use `beta', clear
		forvalues n = 1/1000 {
			gen rr_`n' = exp(beta_int`n')
		}
		keep rr_*
		gen cat = "dis_diar_morb"
		append using `rr'
		save `rr', replace

	// Check means and 95% intervals
		fastrowmean rr_*, mean_var_name(mean)
		fastpctile rr_*, pct(2.5 97.5) names(lower upper)
		order cat mean lower upper

****************************************
********save_results Prep***************
****************************************
	// Set up template for counterfactual category (continually breastfed)
		clear
		set obs 1
		forvalues x = 1/1000 {
			gen rr_`x' = 1
		}
		gen parameter = "cat2"
		tempfile continued
		save `continued', replace

	// Tempfile each outcome
		use `rr', replace
		append using `continued'
		tempfile full_rr
		save `full_rr', replace
		gen cause_id = 302
		gen mortality = 1 if !regexm(cat, "morb")
		replace mortality = 0 if mortality == .
		gen morbidity = 1 if !regexm(cat, "mort")
		replace morbidity = 0 if morbidity == .
		replace parameter = "cat1" if parameter == ""


	rename rr_1000 rr_0
	gen age_group_id = .

// duplicate out for each age_group_id
	foreach x in 4 5 {
		replace age_group_id = `x'
		tempfile temp_`x'
		save `temp_`x'', replace
	}

	use `temp_4', clear
	append using `temp_5'

	** Save mean/lower/upper 
	gen sex_id = .
	foreach sex in 1 2 {
		replace sex_id = `sex'
		export delimited "FILEPATH", replace
	}
