// Author: NAME
// Created: Jan 24, 2014
// Purpose: Take age hazard ratio output from bradmod, and manipulate it to 
// generate mean and 95% confidence intervals for HRs referenced to the median 
// age from our studies

// Last updated by: NAME
// Last updated on: Jan 25, 2014

clear all
set more off
if (c(os)=="Unix") {
	global root "ADDRESS"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
}

// Initialize pdfmaker
if 0==0 {
	if c(os) == "Windows" {
		global prefix "$root"
		do "FILEPATH"
	}
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		do "FILEPATH"
		set odbcmgr unixodbc
	}
}


local prep_data=1
local graph=1

local hr_dir "FILEPATH"
local dismod_dir "FILEPATH"
local graph_dir "FILEPATH"

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

// Bring in dismod output, save last 100 draws, reference to study medians, and reshape
foreach sup in ssa other high {
	insheet using "`dismod_dir'/HIV_HR_`sup'/model_draw2.csv", comma clear 
	
	//need a list of extra rows 
	ds
	local rows = r(varlist)
	local specrows  `sup'_15_25 `sup'_25_35 `sup'_35_45 `sup'_45_55 `sup'_55_100 `sup'_med
	local erows : list rows-specrows
	
	gen row=_n
	// Keep last 1000 draws
	keep if _n>4000 & _n<=5000
		

	// Divide the  HR from each draw by the value for the specified 'reference' point (from the same draw). 
		foreach grp in 15_25 25_35 35_45 45_55 55_100 {
				gen age_hr_`sup'_`grp'=`sup'_`grp'/`sup'_med
				gen raw_hr_`sup'_`grp'=`sup'_`grp'
			}
			foreach x of local erows{ 
					foreach grp in 15_25 25_35 35_45 45_55 55_100 {
					gen age_hr_`x'_`grp' = `sup'_`grp'/`x'
				}
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
		
		//variable holding baselines 
		gen baseline = "med" if missing(sup_age4) 
		replace baseline = age_group if !missing(sup_age4) 
		replace age_group = sup_age4 + "_" + sup_age5 if !missing(sup_age4)
		drop sup_age4 sup_age5
		
		order super age_group baseline
		rename age_group age

		gen merge_super=super
		
	save "`hr_dir'/age_hazard_ratios.dta", replace
	outsheet using "`hr_dir'/age_hazard_ratios.csv", comma names replace
}	
	
	
if `graph' {	
// GRAPH
	// use "`hr_dir'/age_hazard_ratios.dta", clear
	// FOR TABLES (NOT GENERATING NEW DATA)	
	egen mean=rowmean(age_hr*)
	egen low=rowpctile(age_hr*), p(2.5)
	egen high=rowpctile(age_hr*), p(97.5)
	
	egen mean_raw=rowmean(raw*)
	egen low_raw=rowpctile(raw*), p(2.5)
	egen high_raw=rowpctile(raw*), p(97.5)
	
	
	// ESTIMATES WITH SD (After dividing by HR for regional mean age)
	gen se=(high-mean)/1.96
	
	encode age, gen(age_enc)
	
	pdfstart using "`graph_dir'/hr_estimates_separate.pdf"
	foreach super in high other ssa {
		if "`super'" == "high" local title = "High Income"
		if "`super'" == "other" local title = "Other"
		if "`super'" == "ssa" local title = "Sub-Saharan Africa"
		serrbar mean se age_enc if super=="`super'",  scale(1.96) xlabel(1 "15-25" 2 "25-35" 3 "35-45" 4 "45-55" 5 "55-100") xtitle("age") ytitle("HR") title("`title'")
		pdfappend
	}
	pdffinish
	
	
	// ESTIMATES VS DATA: before dividing by HR for the regional mean
	append using `data'
	
	egen age_midpoint=rowmean(age_lower age_upper)
	split age, parse(_)
	destring age1 age2, replace
	egen predicted_age_midpoint=rowmean(age1 age2)
	
	pdfstart using "`graph_dir'/estimates_data_separate.pdf"
	foreach sup in ssa high other {
		preserve
		keep if super=="`sup'"
		twoway rarea low_raw high_raw predicted_age_midpoint, fcolor(blue*.2) lcolor(blue*.2)  ///
			|| line  mean_raw predicted_age_midpoint, lcolor(blue) ///
			|| rcap   age_lower age_upper meas_value, lwidth(vthin) color(red) horizontal ///
			title("`sup'") subtitle("separate dismod models by super region") legend(lab(1 "95% CI") lab(2 "Dismod predicted HR") lab(3 "Raw data")) ///
			ytitle("HR") xtitle("age")
		// graph export "`graph_dir'/`sup'_data_SEPARATE.eps", replace
		pdfappend
		restore
	}
	pdffinish

}
