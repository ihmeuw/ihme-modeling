clear all
set more off
global user "`c(username)'"

if (c(os)=="Unix") {
	global root "ADDRESS"
	global ado_dir "FILEPATH"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
	global ado_dir "FILEPATH"
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


// Set up
	cap restore, not
	local graphs "FILEPATH"
	local extract_file "FILEPATH"
	local pct_dir "FILEPATH"
	local hr_dir "FILEPATH"
	adopath + "FILEPATH"
	
// Bring in data
	import excel using "`extract_file'", firstrow clear

// Adjust reference groups so for sex reference is female, and for age reference is youngest age group

// Only keep those that are adjusted
	keep if include_sex==1 
	drop if baseline==1

// Classify the regions/iso3 into 3 categories: high income, sub-saharan africa, other (if its mostly sub-saharan africa such as LINC, treat at sub-saharan africa)
	gen super="high" if inlist(gbd_region, "Australasia", "High Income", "High income", "High-Income", "High-income North America", "Western Europe")

	replace super="other" if inlist(iso3, "CHN", "ETH", "THA")
	replace super="other" if inlist(gbd_region, "East Asia","Latin America/Carribean", "Central Asia", "Latin America and Caribbean","Latin America and the Carribean", "North Africa and Middle East", "South Asia", "Southeast Asia")
	
	replace super="ssa" if inlist(iso3, "ZAF", "UGA", "ZMB", "SEN", "KEN", "TZA")	
	replace super="ssa" if cohort=="ART-LINC"
	replace super="ssa" if regexm(gbd_region, "Sahar")
	replace super="ssa" if gbd_region=="Sub-Saharan Africa" 
	
// want to save for sourceing  
outsheet using "`hr_dir'/sex_hr_sources.csv", comma replace

	
// Transform data so it fits metan command
	gen ln_hr=ln(std_hr_mort)
	gen ln_lower=ln(std_hr_lo)
	gen ln_upper=ln(std_hr_hi)
	
	
// Run meta analyses: use 'metan' command in log space to conduct meta analysis. based on the standard error caulcated from this, then create 1000 draws. 
// we actually want them exponentiated to be put into the other parts of the broader analysis. so exponentiate them.
		
	foreach sup in high ssa other {
		preserve
			keep if  super=="`sup'" & ln_hr!=.
			tostring nid, replace
			metan ln_hr ln_lower ln_upper, random effect("Hazard Ratio") eform label(namevar=nid)
			
			if c(os) == "Windows" graph export "`graphs'/sex_`sup'.pdf", replace // End graphs should be run locally -- otherwise the output gets cutoff because of metan graph syntax
 			// if c(os) == "Unix" graph export "`graphs'/sex_`sup'.eps", replace // can't use pdfappend because the graphs are variable sizes and get cut off
			
			metan ln_hr ln_lower ln_upper, random effect("Hazard Ratio")
			drawnorm ln_sex_hr, n(1000) m(`r(ES)') sd(`r(seES)') clear
			gen sex_hr=exp(ln_sex_hr)
			drop ln_sex_hr
			gen super="`sup'"

			tempfile `sup'
			save ``sup'', replace		
		restore
	}
	
	use `high', clear
	append using `ssa' `other'

	bysort super: gen id=_n
	reshape wide sex_hr, i(super) j(id)
	order super 

// Add on the info about proportion male from each region
	tempfile sex
	save `sex', replace

	insheet using "`pct_dir'/pct_male_med_age.csv", comma names clear
	drop age_med
	
	merge 1:m super using `sex'
	drop if _m!=3
	drop _m

	save "`hr_dir'/sex_hazard_ratios.dta", replace
	outsheet using "`hr_dir'/sex_hazard_ratios.csv", comma names replace
		
