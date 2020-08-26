// SEE MEPS ANALYSIS FOR DETAILED COMMENTS

cap restore, not
clear all
set more off
set mem 1G


use "./FILEPATH/2a_ahs_prepped_to_crosswalk.dta", clear
drop if key == -999
tempfile data
save `data' 

// get data
use "./FILEPATH/2b_ahs_lowess_r_interpolation.dta", clear
drop if key == -999
merge 1:1 key using "`data'"

drop if key == -999
drop _merge
replace dw_hat = .999 if dw_hat>=1
replace dw_hat = .001 if dw_hat<=0

	
gen logit_dw = logit(dw_hat)

tempfile pre
save `pre', replace


// do this for 1 and 12 month mental diagnoses
foreach mo of numlist 1 12 {
	use `pre', clear

	if `mo' == 1  local opp 12
	if `mo' == 12 local opp 1

	label drop _all
	drop Idepression`opp' Ianxiety`opp' Idysthymia`opp'  Ialcohol_depend`opp' Idrug_depend`opp'

	tempfile pre`mo'
	save `pre`mo'', replace

	// run on 1000 bootstrapped samples
	forvalues x = 1/1000 {

		mata: COMO 	 		 		= J(1500, 1, "")
		mata: AGE 		 			= J(1500, 1, .)
		mata: DEPENDENT				= J(1500, 1, "")
		mata: DW_T					= J(1500, 1, .)
		mata: DW_S					= J(1500, 1, .)
		mata: DW_O					= J(1500, 1, .)
		mata: N						= J(1500, 1, .)
		local c = 1


		use `pre`mo'', clear
		di in red "`mo' mo diagnosis. Bootstrap draw `x'"
		bsample // statas seeds are the same every time, should be replicable

		reg logit_dw I* 

		foreach como of varlist I* {
			preserve
			
			di in red "`como'"
			keep if  `como' == 1 
				predict dw_obs
				replace dw_obs = dw_obs 
				replace dw_obs = invlogit(dw_obs) 				
			replace `como' = 0	
				predict dw_s
				replace dw_s = dw_s 			
				replace dw_s = invlogit(dw_s)		
				
				count
		
				
			sum dw_s
				if `r(N)' > 0 local mean_s = `r(mean)'
				else local mean_dw_s = .
			sum dw_obs
				if `r(N)' > 0 local mean_o = `r(mean)'
				else local mean_dw_o = .
				
			gen dw_t = (1 - ((1-dw_obs)/(1-dw_s)))			
			summ dw_t			
				if `r(N)' != 0 local mean_dw_tnoreplace = `r(mean)'
				else local mean_dw_tnoreplace = .
				
			// save bootstrap dataset		
			cap mkdir "./FILEPATH"	
			cap mkdir "./FILEPATH//`x'"			
			save	  "./FILEPATH//`x'//`como'", replace
			
			count
			local N = `r(N)'
				
			mata: COMO[`c', 1]  	= "`como'"		
			mata: DEPENDENT[`c', 1] = "logit" 
			mata: DW_T[`c', 1] 		= `mean_dw_tnoreplace'
			mata: DW_S[`c', 1] 		= `mean_s'
			mata: DW_O[`c', 1]		= `mean_o'
			mata: N[`c', 1]     	= `N'
			restore
			local c = `c' + 1
		}		

		clear 
		getmata COMO DW_T DW_S DW_O N
		drop if COMO == ""
		drop if DW_T == .

		rename COMO como
		rename DW_T dw_t
		rename DW_S dw_s
		rename DW_O dw_o
		rename N n
		keep como dw_t
		rename dw_t dw_t`x'
		
		cap mkdir "./FILEPATH"	
		save	  "./FILEPATH//`x'.dta", replace
				
	}
}

