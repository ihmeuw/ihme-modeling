***************************************************************
*** NEONATAL PRETERM-ENCEPH-SEPSIS FRAMEWORK
*** Regression for CFR (case-fatality rate), for preterm (ga1, ga2, ga3), enceph, and sepsis


*********************
** General Prep
*********************

clear all
set more off
set maxvar 32000


if c(os) == "Windows" {
	local j FILEPATH

	quietly do FILEPATH
}
if c(os) == "Unix" {
	local j FILEPATH
		
} 
	


adopath + FILEPATH	


local acause "`1'"
local grouping "`2'"
local covariates "`3'"
local random_effects "`4'"
local parent_dir "`5'"
local in_dir "`6'"
local timestamp "`7'"

 


di in red "importing data"
use "`in_dir'", clear

gen lt_mean = logit(mean)

local cov_count: word count `covariates'
local re_count: word count `random_effects'
local re_name 
foreach re of local random_effects{
	local re_name `re_name' || `re':
}

 
xtmixed lt_mean `covariates' `re_name'

 
matrix betas = e(b)
local endbeta = `cov_count' + 1
matrix fe_betas = betas[1, 1..`endbeta']
matrix list fe_betas
matrix covars = e(V)
matrix fe_covars = covars[1..`endbeta', 1..`endbeta']
matrix list fe_covars
 
local betalist 
forvalues i = 1/`cov_count' {
	local betalist `betalist' b_`i'
}
local betalist `betalist' b_0
di in red "beta list is `betalist'"
 
di in red "predicting for fixed effects"
preserve
	drawnorm `betalist', n(1000) means(fe_betas) cov(fe_covars) clear
	gen sim = _n
	tempfile fe_sims
	save `fe_sims', replace
restore

 
di in red "predicting for random effects"
predict re_* , reffects
predict re_se_* , reses
 
forvalues re_idx = 1/`re_count'{
	di in red "predicting for random effect `re_idx' of `re_count'"
	preserve
		 
		local re_name_`re_idx' : word `re_idx' of `random_effects'
		local past_re_name
		if `re_idx'!=1{
			local past_re_idx = `re_idx'-1
			local past_re_name : word `past_re_idx' of `random_effects'
		}
		
		 
		keep `re_name_`re_idx'' `past_re_name' re_*`re_idx'
 
		bysort `re_name_`re_idx'': gen count=_n
		drop if count!=1
		drop count
	 
		local mat_val = `endbeta' + `re_idx'
		local re_se = exp(betas[1, `mat_val'])
		di in red "using common se `re_se' for missing values"
		replace re_`re_idx' = 0 if re_`re_idx' == .
		replace re_se_`re_idx' = `re_se' if re_se_`re_idx' == .
 
		expand 1000
		bysort `re_name_`re_idx'': gen sim = _n
		gen g_`re_idx' = rnormal(re_`re_idx', re_se_`re_idx')
		drop re_*
		sort `re_name_`re_idx'' sim
		
		tempfile random_effect_`re_idx'
		save `random_effect_`re_idx'', replace
	restore
	
}

 
 
preserve
	use `fe_sims', clear  
	forvalues re_idx = 1/`re_count'{  
		local re_name_`re_idx' : word `re_idx' of `random_effects'
 
		if `re_idx'==1{
			local merge_on sim
		}
		else{
			local past_re_idx= `re_idx' -1
			local past_re_name: word `past_re_idx' of `random_effects'
			
			local merge_on sim `past_re_name'
		}
 
		cap drop _merge
		merge 1:m `merge_on' using `random_effect_`re_idx''
		count if _merge!=3
		if `r(N)' > 0{
 
				BREAK
			}
	}
	
	drop _merge
	 
 
	rename b_* b_*_draw_
	rename g_* g_*_draw_
	reshape wide b_* g_*, i(`random_effects') j(sim) 

	tempfile reshaped_covariates
	save `reshaped_covariates', replace
restore

 
 
drop _merge
merge m:1 `random_effects' using `reshaped_covariates'

count if _merge!=3
if `r(N)' > 0{
 
	BREAK
}
drop _merge
 
 
quietly{
	forvalues i=1/1000{
	
		if mod(`i', 100)==0{
			di in red "working on number `i'"
		}
	
		gen lt_hat_draw_`i' = b_0_draw_`i'
		
		forvalues j=1/`cov_count'{
			local cov: word `j' of `covariates'
			replace lt_hat_draw_`i' = lt_hat_draw_`i' + b_`j'_draw_`i' * `cov'
		}
		drop b_*_draw_`i'
		
		forvalues k=1/`re_count'{
			replace lt_hat_draw_`i' = lt_hat_draw_`i' + g_`k'_draw_`i'
		}
		drop g_*_draw_`i'
		
		gen draw_`i' = invlogit(lt_hat_draw_`i')
		drop lt_hat_draw_`i'
	}
}

drop re_* lt_mean

 
local sexval = sex
if `sexval'==3{
 
	preserve

		collapse(mean) year, by(sex)
		count
		if `r(N)'>1{
 
			BREAK
		}
	restore
	drop sex
	expand 2, gen(sex)
	replace sex=2 if sex==0
}

 

local draws_out_dir FILEPATH
local summary_out_dir FILEPATH
preserve
	keep location_id year sex draw*
	save "`draws_out_dir'/`acause'_`grouping'_draws.dta", replace
	outsheet using "`draws_out_dir'/`acause'_`grouping'_draws.csv", comma replace
	di in red "`draws_out_dir'/`acause'_`grouping'_draws.csv"
	save "`archive_draws_out_dir'/`acause'_`grouping'_draws_`timestamp'.dta", replace
	outsheet using "`archive_draws_out_dir'/`acause'_`grouping'_draws_`timestamp'.csv", comma replace
restore
	
 
rename mean data_val
egen mean = rowmean(draw*)
fastpctile draw*, pct(2.5 97.5) names(lower upper)
drop draw*
	
save "`summary_out_dir'/`acause'_`grouping'_summary.dta", replace
export delimited using "`archive_summary_out_dir'/`acause'_`grouping'_summary_`timestamp'.csv", replace 
export delimited using "`summary_out_dir'/`acause'_`grouping'_summary.csv", replace
save "`archive_summary_out_dir'/`acause'_`grouping'_summary_`timestamp'.dta", replace

 

