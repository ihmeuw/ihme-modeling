
// Predict LTFU prop dead
	generate x = 1
	preserve

	insheet using "FILEPATH/logit_logit_model_data.csv", clear comma names
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

// For studies that did their own correction, we'll keep their estimates
// We also keep the dead prop if the region is High-Income
replace dead_prop_adj = dead_prop if ltfu_prop == 0 | super == "high"


