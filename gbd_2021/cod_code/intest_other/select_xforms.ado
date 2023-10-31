/******************************************************************************\

Author: name

Date created:  28 December 2016
Last modified:  7 January  2017

Purpose:

\******************************************************************************/



capture program drop select_xforms
program define select_xforms, rclass
syntax varname [if] [in], covariates(string) model_type(string) [*]

marksample touse
local outcomeVar `varlist'

if c(os) == "Unix" {
	local j "FILEPATH"
	local user : env USER
	local h "/FILEPATH"
	local graph off
	}
else if c(os) == "Windows" {
	local j "FILEPATH"
	local h "FILEPATH"
	local graph on 
	}
		
		
quietly {
*noisily {

local eta 0.010
local max_iterations 5
local max_knots 5
if `max_knots' > 7 {
	noisily display "max_knots cannot exceed 7; resetting max_knots to 7"
	local `max_knots' 7
	}
local knotList `max_knots'

local xforms x ln(x) logit(x) 1/x x^2 x^3 sqrt(x)


capture drop cvTmp*
capture drop *_aic
capture drop *_bic
capture drop *Rank
capture drop *_dir
capture drop *_pred
capture drop if testing==1
capture drop testing
capture drop xformTemp*
capture drop noDirectionSorter
capture drop age_mid
capture drop ageSex*Curve
local icvs  
local lcvs  
local ccvs  

generate xformTemp1 = .
generate xformTemp2 = .



*** CREATE ROWS FOR PREDICTION TESTING ***	
	generate testing = 0
	set obs `=_N + 100'
	replace testing = 1 if missing(testing)
	generate noDirectionSorter = 1



*** PREP COVARIATES ***
	noisily display _n "{hline}" _n "Prepping covariates and testing basic transformations"
	
	
	tokenize "`covariates'", parse("|")
	local i 1

	while "`1'" != "" {

		parseCovars `1'
		local covar `r(covar)'
		local cvTmp_`i'_direction `r(direction)'
		if `r(nknots)' == -999 | `r(nknots)'> 7 local cvTmp_`i'_max_knots `max_knots'
		else local cvTmp_`i'_max_knots `r(nknots)'
		
		local knotList `knotList', `cvTmp_`i'_max_knots'

		if strmatch("`covar'", "*.*") & substr("`covar'", 1, 1)=="i" {
			generate cvTmp_`i' = `=word(subinstr("`covar'", ".", " ", 1), 2)'
			local icvs `icvs' `=word(subinstr("`covar'", ".", " ", 1), 1)'.cvTmp_`i'
			}

		else {	
			noisily display _n "Testing transformations of `covar':"
			
			sum `covar' if testing!=1
			bysort testing: replace `covar' = `r(min)' + (_n-1)*(`r(max)'-`r(min)')/99 if testing==1
			
			replace xformTemp1 = (`covar' - `r(min)') / (`r(max)' - `r(min)')
			replace xformTemp1 = xformTemp1 + ((0.5 - xformTemp1) * 0.000002)
	
			count if missing(`covar') & testing!=1
			local rawMissing `r(N)'
			count if missing(xformTemp1) & testing!=1
			if `r(N)'!= `rawMissing' {
				di "Problem with standardizing `covar' to >0 to <1! Proceeding with untransformed variable"
				local best_xform `covar'
				}
			else {	
				foreach xform in `=subinstr("`xforms'", "x", "xformTemp1", .)' {
					replace xformTemp2 = `xform' 
					
					count if missing(xformTemp2) & testing!=1
					if `r(N)'!= `rawMissing' {
						di ". Problem with `=subinstr("`xform'", "xformTemp1", "`covar'", .)' transformation. Skipping to next option."
						}
					else {
						capture `model_type' `outcomeVar' xformTemp2 if `touse' & aaIndex==1, iterate(1000) `options'
						if _rc != 0 {
							di ". Problem with model convergence. Skipping to next option." 
							}
						else if `e(converged)' == 0 {
							di ". Problem with model convergence. Skipping to next option." 
							}
						else {	
							local r2 `e(r2_p)'
						
							noisily di ". `=subinstr("`xform'", "xformTemp1", "`covar'", .)'" _col(45) round(`r2', 0.001)
			
							if "`xform'"=="`=word("`=subinstr("`xforms'", "x", "xformTemp1", .)'", 1)'" {
								local best_r2 `r2'
								local best_xform `xform'
								}
							else if `r2' > `best_r2' {
								local best_r2 `r2'
								local best_xform `xform'
								}
							}
						}
					}
				noisily display "`=subinstr("`best_xform'", "xformTemp1", "`covar'", .)' is best"
				}
						
		
				replace xformTemp2 = `best_xform'
				sum xformTemp2
				generate cvTmp_`i' = (ln((normal((xformTemp2 - `r(mean)') / `r(sd)') + `eta') / (1 - normal((xformTemp2 - `r(mean)') / `r(sd)') + `eta')) + (ln((1 + `eta') / `eta'))) / (2*(ln((1 + `eta') / `eta')))
		
				local ccvs `ccvs' cvTmp_`i'
				}
		
		local cvTmp_`i'Name `covar'	
	
		local ++i
		macro shift 2
		}
		
	local max_knots = max(`knotList')
	
	
	
*** DETERMINE THE RANK ORDER OF THE COVARIATE VALUES (USED LATER TO DETERMINE DIRECTION OF ASSOCIATION) ***
	preserve
	drop if testing==1	
	tempfile master
	save `master'
	restore

	keep if testing==1
	local covar = word("`ccvs'", 1)			
	generate revRankTemp = ``covar'Name' * -1 if testing==1
	bysort testing (``covar'Name'):  gen covarRank = _n if testing==1
	bysort testing (revRankTemp): gen covarRevRank = _n if testing==1
	drop revRankTemp

	append using `master'


	
	
*** TEST SPLINES ***	

foreach covar of local ccvs {
	local `covar'_knots 2
	}
	
local iteration 1	
local convergence 1

while `iteration' <= `max_iterations' & `convergence' > 0 {
	foreach covar of local ccvs {
		if ``covar'_knots'==1 {
			local `covar'_list   
			}
		else if ``covar'_knots'==2 {
			local `covar'_list  `covar'  
			}
		else {
			local `covar'_list   
			forvalues knots = 1/`=``covar'_knots'-1' {
				local `covar'_list ``covar'_list' `covar'S`knots'
				}
			}
		}
		
		
	foreach tempvar in aic bic direction pred best {
		capture drop *_`tempvar'
		}
		
	noisily display _n "{hline}" _n _n "Testing for best number of knots: Iteration #`iteration'"
	
	foreach target of local ccvs {		
		
		local nontargets: list ccvs - target

		noisily display _n "``target'Name':"
		
		local nontarget_list   
		foreach nontarget of local nontargets {
			local nontarget_list `nontarget_list' ``nontarget'_list'
			}
		
		
		sum `target'
		local range `r(min)' `r(max)'
	
		forvalues nk = 1/`max_knots' {
	
			local failure 0 
		
			if `nk' == 1 {
				generate `target'_aic = .
				generate `target'_bic = .
				generate `target'_direction = ``target'_direction' if covarRank==`nk'
			
				noisily display ". Variable excluded:" _continue
				
				capture `model_type' `outcomeVar' `nontarget_list' `icvs' if `touse' & aaIndex==1, iterate(1000) `options'
				if _rc != 0 {
					local failure 2
					noisily display _col(40) "model failed."
					}
				else if `e(converged)' == 0 {
					local failure 2
					noisily display _col(40) "model failed."
					}
				else {
					generate `target'_`nk'_pred = 0 if testing==1
					}
				}
	
			else if `nk' == 2 {
		
				noisily display ". 2 knots (i.e. linear):" _continue

				capture `model_type' `outcomeVar' `target' `nontarget_list' `icvs' if `touse' & aaIndex==1, iterate(1000) `options'
				if _rc != 0 {
					local failure 2
					noisily display _col(40) "model failed."
					local `target'_max_knots `nk'
					}
				else if `e(converged)' == 0 {
					local failure 2
					noisily display _col(40) "model failed."
					local `target'_max_knots `nk'
					}
				else {
					generate `target'_`nk'_pred = exp(`target' * _b[`target']) if testing==1
					}
				}
	
			else if `nk' <= ``target'_max_knots' {
	
				noisily display ". `nk' knots:" _continue
		
				capture drop `target'S* 
				capture mkspline `target'S = `target', cubic nknots(`nk')
				if _rc != 0 {
					local failure 1
					noisily display _col(40) "spline creation failed."
					}
				else {
					capture `model_type' `outcomeVar' `target'S* `nontarget_list' `icvs' if `touse' & aaIndex==1, iterate(1000) `options'
					if _rc != 0 {
						local failure 2
						noisily display _col(40) "model failed."
						}
					else if `e(converged)' == 0 {
						local failure 2
						noisily display _col(40) "model failed."
						}
					else {
						generate `target'_`nk'_pred = 1 if testing==1
						foreach var of varlist `target'S* {
							replace `target'_`nk'_pred = exp(`var' * _b[`var'])  if testing==1
							}
						}
					}
				}
	
			if `failure'==0 & `nk' <= ``target'_max_knots' {
				estat ic
				matrix aic = r(S)
				replace `target'_aic = aic[1,5] if covarRank==`nk'
				replace `target'_bic = aic[1,6] if covarRank==`nk'
				local `target'_bic = aic[1,6] 
				local bicTitle (BIC = `=round(``target'_bic', 0.1)')
			
				
				local direction .
		
				bysort testing (`target'_`nk'_pred covarRank): gen targetRank = _n if testing==1
				capture assert targetRank == covarRank

				if _rc==0 {
					local direction 1
					}
			
				else {
					drop targetRank
					bysort testing (`target'_`nk'_pred covarRevRank): gen targetRank = _n if testing==1
					capture assert targetRank == covarRevRank 
			
					if _rc==0 {
						local direction -1
						}
					
					else {
						local direction 0
						}
					}
				if `nk' == 1 noisily display _col(40) "BIC = " round(``target'_bic', 0.1) 
				else noisily display _col(40) "BIC = " round(``target'_bic', 0.1)  _col(55) "Direction = `direction'"
				}
			
			else {
				local direction .
				local bicTitle   
				generate `target'_`nk'_pred = 0 if testing==1
				}
			
			
			capture drop targetRank
			replace `target'_direction = `direction' if covarRank==`nk'
			
			
			if `nk' > 1 & "`graph'"=="on" {
				sort testing ``target'Name'
				line `target'_`nk'_pred ``target'Name' if testing==1, title("`direction' `bicTitle'") name(`=subinstr(abbrev("``target'Name'", 29), "~", "", .)'_`nk', replace)
				local graphs `graphs' `=subinstr(abbrev("``target'Name'", 29), "~", "", .)'_`nk'
				}
			}
		}
		
	

		preserve
		drop if testing==1 & inrange(covarRank, 2, `max_knots')
		save `master', replace
		restore
	
		keep if testing==1 & inrange(covarRank, 2, `max_knots')

		noisily display _n "Current best covariate configuration:"
		
		local convergence 0

		
		foreach target of local ccvs {	
			if ``target'_direction'==0 {
				bysort noDirectionSorter (`target'_bic): generate `target'_best = _n==1
				}
			else {
				bysort `target'_direction (`target'_bic): generate `target'_best = _n==1 & `target'_direction==``target'_direction'
				}
			
			local `target'_knots_previous ``target'_knots_best'
			levelsof covarRank if `target'_best==1, clean local(`target'_knots_best)
			levelsof `target'_bic if `target'_best==1, clean local(`target'_bic_best)
			
			
			if "``target'_knots_best'" != "``target'_knots_previous'" local ++convergence
			
			noisily display "``target'Name':" _col(40)  "Knots = ``target'_knots' (previous = ``target'_knots_previous')" _col(55) "BIC = " round(``target'_bic_best', 0.1) 
			
			}
		
		append using `master'
		

		if "`graph'"=="on" {
			graph combine `graphs', colfirst rows(`=`max_knots'-1') name(iteration_`iteration', replace)
			graph drop `graphs'
			}
		
		local ++iteration
		}
	
	
	
	
	
* COLLECT BEST SPLINE COMBINATION *
	noisily display _n "{hline}" _n "Processing results of spline and transformation tests"
	capture drop cvTmp*S*
	capture drop cvTmp_*_*
	
	local ccv_list  
	
	foreach covar of local ccvs {
		if ``covar'_knots'==1 {
			local ccv_list `ccv_list'
			}
		else if ``covar'_knots'==2 {
			local ccv_list `ccv_list' `covar'  
			}
		else {
			capture mkspline `covar'S = `covar', cubic nknots(``covar'_knots') 
			local ccv_list `ccv_list' `covar'S* 
			}
		}

	`model_type' `outcomeVar' `ccv_list' `icvs' if `touse', `options'
	predict covarCurve, xb nooffset
		
		
	
*** DETERMINE KNOTS FOR YEAR ***
	noisily display _n "{hline}" _n "Modelling trends by super-region"
	forvalues nk = 2/5 {
		capture drop yearS*
		if `nk' == 2 {
			generate yearS = year_id
			noisily display ". 2 knots (i.e. linear):" _continue
			}
		else {
			mkspline yearS = year_id, cubic nknots(`nk')
			noisily display ". `nk' knots:" _continue
			}
		
		noisily `model_type' `outcomeVar' covarCurve c.yearS*##super_region_id if `touse' & aaIndex==1, `options' difficult interate(1000)
		
		estat ic
		local bic = aic[1,6] 
						
		noisily display _col(40) "BIC = " round(``target'_bic', 0.1) 		
		
		if `nk' == 2 {
			local best_bic = `bic'
			local best_nk = `nk'
			predict fullCovarCurve, xb nooffset
			}
		else if `bic' < `best_bic' {
			local best_bic = `bic'
			local best_nk = `nk'
			capture drop fullCovarCurve
			predict fullCovarCurve, xb nooffset
			}
		}

	capture drop yearS*
	if `best_nk' == 2  local xform_covars `ccv_list' `icvs'  c.year_id##super_region_id
	else {
		mkspline yearS = year_id, cubic nknots(`best_nk')
		local xform_covars `ccv_list' `icvs'  c.yearS*##super_region_id
		}
	
}	

/*	
*** MODEL AGE/SEX PATTERNS ***
	noisily display _n "{hline}" _n "Modelling age/sex patterns by super-region"
	egen age_mid = rowmean(age_group_years_start age_group_years_end)
	preserve

	collapse (mean) cf, by(age_mid)
	drop if missing(cf) | age_mid>65 

	generate delta = cf - cf[_n-1]
	generate abs_delta = abs(delta)
	generate delta2 = delta - delta[_n-1]
	generate abs_delta2 = abs(delta2)
	
	sort age_mid
	gen age_tag = (_n==1) | (_n==_N)

	sort cf
	gen cf_tag = (_n==1) | (_n==_N)

	gsort -abs_delta
	gen max_delta = _n==1	
	gsort -abs_delta2
	gen max_delta2 = _n==1	

	egen knot = rowtotal(*_tag max_*)
	levelsof age_mid if knot>=1, local(ageKnots)

	restore

	capture drop ageS*
	mkspline ageS = age_mid, cubic knots(`ageKnots')
	`model_type' `outcomeVar' fullCovarCurve c.ageS*##i.super_region_id i.sex_id##i.super_region_id if `touse', `options'
	predict ageSexCurve, xb nooffset


*/

drop if testing==1
	drop testing	



end

	
	
*** DEFINE FUNCTION TO PROCESS COVARIATE LIST ***
	capture program drop parseCovars
	program define parseCovars, rclass
	syntax varname(fv ts) [, NKnots(integer -999) DIRection(integer 0)] 
		
	return local covar `varlist'
	return local direction `direction'
	return local nknots `nknots'

	end
