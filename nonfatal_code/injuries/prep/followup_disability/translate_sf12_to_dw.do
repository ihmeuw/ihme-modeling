// USERNAME
// DATE
// Bring together the 4 prepped injury datasets. 

	cap restore, not
clear all
set mem 800m
set more off

** set OS
if c(os) == "Windows"   global j "FILEPATH"
if c(os) == "Unix" 		global j "FILEPATH"

local files: dir "$prepped" files "1)*.dta", respectcase


// append and prep 4 datasets for analysis
foreach f of local files {
	di in red "appending `f'"
	append using "${prepped}//`f'"
}

// mark datasets
gen dataset = 1 if meps == 1
replace dataset = 2 if nscot == 1
replace dataset = 3 if tbi == 1
replace dataset = 4 if dutch == 1
drop meps nscot tbi dutch
label define d 1 "MEPS" 2 "NSCOT" 3 "TBI" 4 "DUTCH_LIS"
label values dataset d


// general prep
rename time TIME
replace TIME = -999 if TIME == .



// N19 is rare, and fixing an N27 miscode. 
replace tN26 = 1 if tN27 == 1
replace tN19 = 0 if tN19 == .
drop tN27


// get everything into DW Space.
rename pcs_r pcs // this name for meps and nscot
rename mcs_r mcs
replace pcs = PCS_hat_log if dataset == 4 // DUTCH LIS
replace mcs = MCS_hat_log if dataset == 4
replace pcs = PCS12 if dataset == 3 // TBI
replace mcs = MCS12 if dataset == 3
	
// drop any missing values for mcs/pcs -- drop a lot of our obs here.	
drop if mcs == . | pcs == .
drop age // using age_gr 
gen composite = pcs + mcs
*gen key = _n
	
// Make sure to use the same key as before, this will be important for replicating the bootstrap random samples later on
replace round = 99 if round == .
merge 1:1 id dataset TIME round using "${int}//2) KEYSORT A.dta", nogen
replace round = . if round == 99
	
	// Write the R file which will be used to run the loess model for crosswalking
	file open R_file using "${code}//2) FILEPATH.r", write replace text
	
	// RUN THE CROSSWALK
	preserve
	rename composite predict
	keep key predict
	append using "${int}//2) a) FILEPATH.dta"
	replace key = -999 if key == .
	save "${int}//2) b) FILEPATH.dta", replace
	if "`c(os)'"=="Windows" {
		!"FILEPATH/R" <"${code}//2) FILEPATH.r" --no-save 
	}
	else {
		!"FILEPATH/R" <"${code}//2) FILEPATH.r" --no-save
	}
	restore

	merge 1:m key using "${int}//2) c) Crosswalked"
	drop if key == -999 
	drop _merge

// prepare dw for logit space, limit range // 2.27% of obs change.
replace dw_hat = .999 if dw_hat >= 1
replace dw_hat = .001 if dw_hat <= 0	
gen logit_dw = logit(dw_hat)

// make severity index for comorbidities by dataset, to make them more comparable.
	// split appart injury causes
	foreach inj of varlist  tN28 tN26 tN25 tN23 tN22 tN21 tN20 tN19 tN14 tN12 tN11 tN10 tN9 tN8 tN7 tN6 tN5 tN4 tN3 tN2 tN1 tGS tGC {
			di in red "ya: `inj'" 
			local newname = "INJ_" + substr("`inj'", 2,.)
			rename `inj' `newname'		
	}	

tempfile pre
save `pre', replace


// get severity for each comorbidity.
// essentially here we are running the MEPS DW analysis on each dataset in order to bin the comorbid conditions based on severity. 
// the bins become a standardized como corrections for the full injuries analysis, which comes next. 

forvalues ds = 1/4 {
	use `pre', clear
	mata: COMO				= J(1500, 1, "")
	mata: DW_T				= J(1500, 1, .)
	local c = 1

	keep if dataset == `ds'	
		
		foreach var of varlist t* {
			count if  `var' != .
			if `r(N)' != 0 rename `var' X_`var'
		}
		foreach var of varlist INJ* {
			summ `var'
			if `r(N)' == 0 drop `var'
		}
		regress logit_dw X_t* INJ* if dataset == `ds'
		
		foreach como of varlist X_t* {
			di in red "`ds' `como'"
			preserve
			keep if  `como' == 1 
				predict dw_obs
				replace dw_obs = invlogit(dw_obs) 				

			replace `como' = 0	
				predict dw_s
				replace dw_s = invlogit(dw_s)		
			
			gen dw_t = (1 - ((1-dw_obs)/(1-dw_s)))		
			
				count	
				
				if `r(N)' != 0 {				
					summ dw_t
					if `r(N)' != 0 local mean_dw_tnoreplace = `r(mean)' 
					else local mean_dw_tnoreplace = .
				}
				else local mean_dw_tnoreplace = .
			
			mata: COMO[`c', 1] = "`como'"		
			mata: DW_T[`c', 1] = `mean_dw_tnoreplace'
			
			restore
			local c = `c' + 1	
		}
			
		clear 
		getmata COMO DW_T
		drop if DW_T == .
		
		// split up effects of comorbidities into 4 even quantiles. --  maybe do this with everything pooled?
			sort DW_T
			gen n = _n
			xtile quan`ds' = n, nq(5)
			keep COMO quan`ds'
			
			gen dataset = `ds'
			reshape wide quan`ds', i(dataset) j(COMO) string
			
		// put the value in the name of the variable and turn the variable into a dummy 
			foreach var of varlist quan* {
				local val = `var'
				rename `var' `var'`val'
				replace `var'`val' = 1 
			}
	
			tempfile quantiles`ds'
			save `quantiles`ds'', replace
			
}			

// merge on the ranked quantiles for comos from each dataset
	use `pre', clear
	forvalues i = 1/4 {
		merge m:1 dataset using `quantiles`i''
		drop _merge
	}

// create the 5 indicator variables that are now comparable 
	forvalues ds = 1/4 {
		foreach cond of varlist quan`ds'* {
			local condvar = subinstr("`cond'","quan`ds'X_","",.)
			local l = length("`condvar'") - 1
			local condvar = substr("`condvar'",1,`l')
			replace `cond' = `cond'*`condvar' if dataset == `ds'
		}
	}
	
	forvalues quant = 1/5 {
		egen cond`quant' = rowtotal(quan*`quant')
		replace cond`quant' = 1 if cond`quant' > 1
	}

		foreach inj of varlist  INJ* {
			di in red "ya: `inj'" 
			replace `inj' = 0 if `inj' == . // didnt pick any up that dataset, needs to be zero for regression analysis. 	
	}	
	
egen INJ_Garb = rowmax(INJ_G*)
drop INJ_GS INJ_GC
drop if id == ""
order id sex age dw_hat logit_dw hospital TIME cond* INJ* t*	

// Make sure to use the same key as before, this will be important for replicating the bootstrap random samples later on
merge 1:1 key using "${int}//2) KEYSORT B.dta", nogen
sort keysort
drop keysort

save "${prepped}//2) Prepped for Analysis.dta", replace
save "${int}//2) Prepped for Analysis.dta", replace

	