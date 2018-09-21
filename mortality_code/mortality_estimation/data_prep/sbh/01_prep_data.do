/*
Description: example for prepping SBH data. 
Note: 
	- variable names must be what are listed below
	- the code outline below assumes you are starting with microdata, 
	  it's also possible to use data from report tabulations. 
*/ 

	clear all
	capture cleartmp 
	set more off 
		
	cd "strSurveyDirectory"

// Format the data
	/* 
	Format SBH data such that each line is one women and you have these variables:
	iso3 - proper iso3 code
	ihme_loc_id - iso3 code modified to include subnational locations if using subnational data. 
	svdate - exact surveydate, in years (not cmc). This can be either the
			 midpoint of the span over which the survey was conducted or, if
			 you have a variable for when each woman was surveyd, the mean
			 survey date. 
	age - women's age 
	timefb - number of years since a women's first birth (note that this will 
	         be missing if a woman has 0 ceb) 
	ceb - number of children ever born 
	cd - number of children died 
	sample_weight - the sample weight, if appropriate
	*/	
	
// EXAMPLE using the 1999 Belize Reproductive Health Survey
	use "strBelizeDataFile.dta", clear
	
	// generate standard SBH variables 
	gen iso3 = "BLZ"
	gen sample_weight = Q010
	gen svdate1 = DATE_Y + (DATE_M-1)/12
	egen svdate = mean(svdate1)
	
	gen ceb = Q216LIB
	egen cd = rowtotal(Q209SON Q209DAU)
	replace cd = 0 if Q208 != 1
	replace ceb = 0 if (Q200 == 1 & Q201 == 1) | (Q202 == 2)
	replace cd = 0 if ceb == 0 
	
	gen age = Q101
	
	// generate time since first birth variable
	replace Q203 = . if Q203 == 99 
	gen timefb = age - Q203			// current age - age at first birth 
	replace timefb = . if timefb < 0 
	
	keep sample_weight iso3 age ceb cd timefb svdate

// Check data for consistency and plausibility 
	// check missingness
	count if ceb == . 
	count if cd == . 
	count if age == . 
	count if timefb == . & ceb != 0 
	
	// check that ceb is always greater than or equal to cd 
	compare ceb cd 
	
	// check that the tabulations of ceb and cd make sense
	// For example that the number of children is plausible given a woman's age
	tab ceb
	tab cd
	tab ceb cd
	
	// check that the tabulation of mother's age and time since first birth makes sense
	tab age 
	tab timefb
	
// Create the MAC dataset (5 year maternal age groups)
	preserve
	keep if age >= 15 & age <= 49 & age != . & ceb != . & cd != . 
	gen agegroup = string(5*floor(age/5)) + "-" + string(5*floor(age/5)+4)
	gen n_mothers = sample_weight 
	replace ceb = ceb*sample_weight
	replace cd = cd*sample_weight
	collapse (sum) ceb cd n_mothers, by(svdate iso3 agegroup) 
	saveold "MAC_totals.dta", replace
	restore 

// Create the TFBC dataset (5 year time since first birth groups)
	preserve
	keep if timefb>= 0 & timefb <= 34 & timefb != . & ceb != . & cd != . 
	gen agegroup = string(5*floor(timefb/5)) + "-" + string(5*floor(timefb/5)+4)
	gen n_mothers = sample_weight 
	replace ceb = ceb*sample_weight
	replace cd = cd*sample_weight
	collapse (sum) ceb cd n_mothers, by(svdate iso3 agegroup) 
	saveold "TFBC_totals.dta", replace
	restore 
	
// Create the MAP datasets (2 year maternal age groups, except 15-17) 
	preserve
	keep if age >= 15 & age <= 49 & age != . & ceb != . & cd != . 
	gen agegroup = string(2*floor(age/2)) + "-" + string(2*floor(age/2)+1)
	replace agegroup = "15-17" if age>=15 & age <= 17
	tempfile MAP
	save `MAP', replace 
	collapse (sum) sample_weight, by(iso3 svdate agegroup ceb)
	saveold "MAP_ageceb.dta", replace 
	use `MAP', clear
	collapse (sum) sample_weight, by(iso3 svdate agegroup cd)
	saveold "MAP_agecd.dta", replace 	
	restore 
	
// Create the TFBP datasets (two year time since first birth groups, except 32-34)
	preserve
	keep if timefb>= 0 & timefb <= 34 & timefb != . & ceb != . & cd != . 	
	gen agegroup = string(2*floor(timefb/2)) + "-" + string(2*floor(timefb/2)+1)
	replace agegroup = "32-34" if timefb>=32 & timefb<=34
	tempfile TFBP
	save `TFBP', replace
	collapse (sum) sample_weight, by(iso3 svdate agegroup ceb)
	saveold "TFBP_timefbceb.dta", replace 
	use `TFBP', clear
	collapse (sum) sample_weight, by(iso3 svdate agegroup cd)
	saveold "TFBP_timefbcd.dta", replace 	
	restore 
