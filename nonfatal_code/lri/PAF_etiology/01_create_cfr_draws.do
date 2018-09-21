// Prep LRI case fatality scalars
	set more off
	set seed 3057114
	** Prep case fatlity rate scalar for LRIs
	insheet using "FILEPATH/cfr_scalars.csv", clear
	keep age_lower pred_lower pred_median pred_upper
	drop if age_lower == .
	rename age_lower age
	
	gen ln_mean = log(pred_median) // Log normal dist median close enough to mean? (For now yes...)
	gen ln_sd = ((ln(pred_upper)) - (ln(pred_lower))) / (2*invnormal(.975))
	drop pred*
	
	forvalues draw = 0/999 {
		gen scalarCFR_`draw' = 1 / exp(rnormal(ln_mean, ln_sd))
	}
	drop ln_mean ln_sd

	// Don't do this anymore! //
/*
	// Collapse down to 80+
	replace age = 80 if age > 80
	collapse (mean) scalarCFR*, by(age)
*/	
	// Get 0.1 and 0.01
	expand 3 if age == 0
	bysort age: gen id = _n if age == 0
	replace age = .01 if id == 1
	replace age = .1 if id == 2
	drop id
	gen age_group_id = _n + 1
	replace age_group_id = _n+10 if _n>=20
	replace age_group_id = 235 if age_group_id == 33
	drop if age==100
	
saveold "FILEPATH/cfr_scalar_draws.dta", replace

// Get summary for table //
egen mean_cfr = rowmean(scalarCFR_*)
egen lower_cfr = rowpctile(scalarCFR_*), p(2.5)
egen upper_cfr = rowpctile(scalarCFR_*), p(97.5)
