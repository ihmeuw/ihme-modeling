
// Predict LTFU prop dead
	generate x = 1
	preserve
	// insheet using "FILEPATH", clear comma names  
	
	if $add_new_ltfu { 
		insheet using "FILEPATH/logit_logit_model_data2.csv", clear 
		*recalculate these to make sure they are accurate
		replace logit_prop_traced_dead = logit((traced_dead/traced)) if !missing(traced) & !missing(traced_dead) 
		replace logit_prop_ltfu = logit((ltfu/enrolled)) if !missing(ltfu) & !missing(enrolled) 
	}
	else{	
		insheet using "FILEPATH/logit_logit_model_data.csv", clear comma names 
	} 
	
	if $glmm{ 
		
		gen u = _n 
		meqrlogit traced_dead year logit_prop_ltfu || u :, binomial(traced) 
		matrix b = e(b) 
		generate beta0 = b[1,3]
		generate beta1 = b[1,2]
		generate beta2 = b[1,1] 
		keep beta0 beta1 beta2 
		generate x = 1
		tempfile reg_betas
		save `reg_betas', replace
		restore
		
		merge m:m x using `reg_betas', nogen
		drop x
		
		// Logit space variables
		gen myear = (year_start+year_end)/2
		replace myear = 2003 if myear <= 2003 
		replace myear = 2010.5 if myear >= 2010.5
		generate logit_ltfu_prop = logit(ltfu_prop)
		generate logit_ltfu_prop_dead = beta0 + beta1*logit_ltfu_prop + beta2*myear
		generate ltfu_prop_dead = exp(logit_ltfu_prop_dead)/(1+exp(logit_ltfu_prop_dead))

		// Logit-logit model for adjusting survival
		if $hai_corr{
			generate dead_prop_adj = dead_prop+ltfu_prop*(ltfu_prop_dead - dead_prop)
		} 
		else{ 
			generate dead_prop_adj = dead_prop+ltfu_prop*ltfu_prop_dead
		}

		// For studies that did their own correction, we'll keep their estimates
		// We also keep the dead prop if the region is High-Income, because we don't want to mix those with LTFU assumption of 0 with those that are adjusted
		// When they both might have LTFU issues
		// Also because the analysis is based on developing-country data
		replace dead_prop_adj = dead_prop if ltfu_prop == 0 | super == "high"
	
	}
	else{

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
		// We also keep the dead prop if the region is High-Income, because we don't want to mix those with LTFU assumption of 0 with those that are adjusted
		// When they both might have LTFU issues
		// Also because the analysis is based on developing-country data
		replace dead_prop_adj = dead_prop if ltfu_prop == 0 | super == "high"
	}

