// Purpose: Take age hazard ratio output from bradmod, and manipulate it to generate mean and 95% confidence intervals for HRs referenced to the median age from our studies

clear all
set more off

if (c(os)=="Unix") {
	global root FILEPATH
}

if (c(os)=="Windows") {
	global root FILEPATH
}


local prep_data=1

local hr_dir FILEPATH
local dismod_dir FILEPATH

if `prep_data' {
// Bring in raw data and save
foreach sup in ssa other high {
	insheet using "`dismod_dir'/HIV_HR_`sup'/data_in.csv", comma clear
	drop if "`sup'"=="other" & super=="ssa"
	replace super="`sup'"
	tempfile t`sup' 
	save `t`sup'', replace	
}

	append using `tssa' `tother'
	drop if region=="none"
	
	tempfile data
	save `data', replace

// Bring in dismod output, save last 1000 draws, reference to study medians, and reshape
foreach sup in ssa other high {
	insheet using "`dismod_dir'/HIV_HR_`sup'/model_draw2.csv", comma clear
	gen row=_n
	// Keep last 1000 draws
	keep if _n>4000 & _n<=5000
		

	// Divide the  HR from each draw by the value for the specified 'reference' point (from the same draw). 
		foreach grp in 15_25 25_35 35_45 45_55 55_100 {
				gen age_hr_`sup'_`grp'=`sup'_`grp'/`sup'_med
				gen raw_hr_`sup'_`grp'=`sup'_`grp'
			}
			tempfile tmp`sup'
			save `tmp`sup'', replace
	}
	
	use `tmphigh', clear
	merge 1:1 row using `tmpssa' 
	drop _m
	merge 1:1 row using `tmpother'
	drop _m row


	// Reshape to put data in 'wide' format
		keep age_hr_* raw_*
		gen id=_n
		reshape long age_hr_ raw_hr_, i(id) j(sup_age) string
		reshape wide age_hr_ raw_hr_, i(sup_age) j(id)
		
		split sup_age, parse(_)
		egen age_group=concat(sup_age2 sup_age3), p(_)
		rename sup_age1 super
		drop sup_age2 sup_age3 sup_age
		
		order super age_group
		rename age_group age

		gen merge_super=super
		
	save "`hr_dir'/age_hazard_ratios.dta", replace
	outsheet using "`hr_dir'/age_hazard_ratios.csv", comma names replace
}	
	
	