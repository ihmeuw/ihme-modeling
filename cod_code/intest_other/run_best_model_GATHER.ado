capture program drop run_best_model
program define run_best_model, rclass

*syntax anything
syntax anything [if] [in] [, reffects(string) *]

*replace sample_size = 1 if missing(sample_size)
*noisily di "`anything', `options' || region_id:"

*noisily `anything'
noisily `anything' `if' `in', `options' `reffects'


if "`e(cmd)'"=="mixed" {
	predict randomMean*, reffects
	predict randomSe*, reses
	
	foreach rVar of varlist randomMean* {
		sum `rVar' if e(sample)
		replace `rVar' = `r(mean)' if missing(`rVar')
		local seTemp = `r(sd)'
		sum `=subinstr("`rVar'", "Mean", "Se", .)' if e(sample)
		replace `=subinstr("`rVar'", "Mean", "Se", .)' = sqrt(`r(mean)'^2 + `seTemp'^2) if missing(`=subinstr("`rVar'", "Mean", "Se", .)')
		}	
	
	
	local random yes
	}

else if strmatch("`e(cmd)'", "me*") {
	predict randomMean, reffects reses(randomSe) nooffset
	
	foreach rVar of varlist randomMean* {
		sum `rVar' if e(sample)
		replace `rVar' = `r(mean)' if missing(`rVar')
		sum `=subinstr("`rVar'", "Mean", "Se", .)' if e(sample)
		replace `=subinstr("`rVar'", "Mean", "Se", .)' = `r(mean)' if missing(`=subinstr("`rVar'", "Mean", "Se", .)')
		}	
	
	local random yes
	}
else {
	local random no
	}

   		
matrix m = e(b)'
matrix C = e(V)
matrix V = vecdiag(C)'

if "`e(dispers)'"=="mean" | "`e(dispers)'"=="constant" generate dispersion = "`e(dispers)'"
else if "`e(dispersion)'"=="mean" | "`e(dispersion)'"=="constant" generate dispersion = "`e(dispersion)'"
else generate dispersion = ""

local covars: rowfullnames m
local dVar `e(depvar)'
local betas  

capture drop covarTemp_*
capture drop beta_covarTemp_*
capture drop deltaTemp
capture drop _merge
capture drop mergeIndex
capture drop draw*

local count 0

forvalues i = 1 / `=wordcount("`covars'")' {
	local covar: word `i' of `covars'
	
	if !strmatch("`covar'", "*:_cons") {
		local covar = subinstr("`covar'", "`dVar':", "", .)
		local covar = subinstr("`covar'", "co.", "c.", .)
		local covar = subinstr("`covar'", "c.", "", .)
		local covar = subinstr("`covar'", "#", " ", .)

		local covarClean 1
	
		foreach token of local covar {
			if strmatch("`token'", "*.*")==1 {	
				local token = subinstr("`token'", "b.", ".", .)
				local token = subinstr("`token'", "o.", ".", .)
				local token = subinstr("`token'", "b.", ".", .)
				local token = subinstr("`token'", ".",  " ", .)
				gettoken cat var: token
				local token = trim("`var'") + "==" + trim("`cat'")
				}
			local covarClean `covarClean' * (`token')
			}
		
		di "generating covarTemp_`i' = `covarClean'"
		quietly generate covarTemp_`i' = `covarClean'
		local betas `betas' beta_covarTemp_`i'
		local ++count
		}
	else {
		if strmatch("`covar'", "`dVar':*")==1 {	
			di "generating covarTemp_`i' = 1 [intercept]"
			quietly generate covarTemp_`i' = 1
			local betas `betas' beta_covarTemp_`i'
			local ++count
			}
		else if "`covar'"=="lndelta:_cons" | "`covar'"=="lnalpha:_cons" {
			di "generating dispersionTemp = 0 [dispersion parameter]"
			quietly generate dispersionTemp = 0
			local betas `betas' beta_dispersionTemp
			local ++count
			}
		}
	}

matrix m =  m[1..`count',1]
matrix C =  C[1..`count',1..`count']
	
quietly {
	bysort location_id (year_id sex_id age_group_id): gen mergeIndex = _n
	tempfile covarMerge
	save `covarMerge'
	}

noisily di _n "generating beta draw variables"

quietly {
	clear
	set obs 1000
	generate mergeIndex = _n
	drawnorm `betas', means(m) cov(C)
	foreach var of varlist beta_covarTemp_* {
		local betaTest = m[`=subinstr("`var'", "beta_covarTemp_", "", .)',1]
		local varTest  = V[`=subinstr("`var'", "beta_covarTemp_", "", .)',1]
		if `betaTest'==0 & `varTest'==0 quietly replace `var' = 0
		}

	merge 1:m mergeIndex using `covarMerge', assert(2 3) nogenerate
	}
	
	
return local random `random'

end
