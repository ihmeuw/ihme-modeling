/*
Description: example for prepping SBH data. 
*/ 

	clear all
	capture cleartmp 
	set more off 
		
	cd "FILEPATH"

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

	local filepath = "FILEPATH"
	local iso3 = "iso3Code"
	local sv = "strSurveyName" 
	local svdate = "strSurveyDate"
		
	local savepath = "FILEPATH"	
	local save_folder = "FILEPATH"	
	
    use "`filepath'", clear   
   	
    * OBTAIN DATA
   
	// Force consistency in variable names 
    cap renpfix V v
    cap renpfix BORD bord
    cap renpfix B b
    cap drop b*_*_*
 
	// Determine sample type
	cap inspect v020 
	if (_rc == 0) { 
		if (`r(N)' > 0) { 
			if (v020[1] == 0) gen sample = "all" 
			if (v020[1] == 1) gen sample = "ever-married" 
		} 
		else { 
			count if v501 == 0 
			if (`r(N)' > 0) gen sample = "all" 
			if (`r(N)' == 0) gen sample = "ever-married" 		
		} 
	} 
	else { 
		count if v501 == 0 
		if (`r(N)' > 0) gen sample = "all" 
		if (`r(N)' == 0) gen sample = "ever-married" 
	} 

	
	* check for timefb variable
	cap inspect v211
	local v211_error = _rc
	if( r(N) == 0 ) local v211_error = 99999

	* check for psu variable
	cap sum v021
	local v021_error = _rc
 
	* check for all women factor variable
	cap sum awfactt
	local awfactt_error = _rc
			
    if( `v211_error' == 0 & `v021_error' == 0 & `awfactt_error' == 0) keep iso3 v001 v021 v005 v008 v012 v201 v211 v218 awfactt sample
    if( `v211_error' != 0 & `v021_error' == 0 & `awfactt_error' == 0) keep iso3 v001 v021 v005 v008 v012 v201 v218 awfactt sample
    if( `v211_error' == 0 & `v021_error' != 0 & `awfactt_error' == 0) keep iso3 v001 v005 v008 v012 v201 v211 v218 awfactt sample
    if( `v211_error' != 0 & `v021_error' != 0 & `awfactt_error' == 0) keep iso3 v001 v005 v008 v012 v201 v218 awfactt sample
    if( `v211_error' == 0 & `v021_error' == 0 & `awfactt_error' != 0) keep iso3 v001 v021 v005 v008 v012 v201 v211 v218 sample
    if( `v211_error' != 0 & `v021_error' == 0 & `awfactt_error' != 0) keep iso3 v001 v021 v005 v008 v012 v201 v218 sample
    if( `v211_error' == 0 & `v021_error' != 0 & `awfactt_error' != 0) keep iso3 v001 v005 v008 v012 v201 v211 v218 sample
    if( `v211_error' != 0 & `v021_error' != 0 & `awfactt_error' != 0) keep iso3 v001 v005 v008 v012 v201 v218 sample
	
    if( `v021_error' == 0 ) {
      qui inspect v021
      if ( r(N_unique) == 0 ) drop v021
      else drop v001
    }
 
	cap gen awfactt = 100				// create all woman factor if it doesn't exist
	replace awfactt = awfactt / 100		// per DHS instructions (http://www.measuredhs.com/help/Datasets/All_Women_Factors.htm)
			
	// Rename variables 
    rename v005 sample_weight
    rename v012 age
    rename v201 ceb
    rename v218 cs
    rename v008 svdate
    cap rename v001 psu
    cap rename v021 psu

    replace sample_weight = sample_weight/10^6
    gen cd = ceb - cs
    drop cs
	

	// Calculate time since first birth 
    if( `v211_error' == 0 ) {
      gen timefb = (svdate - v211)/12
      replace timefb = int(timefb)
      drop v2*
    }
    else {
      gen timefb = .		
    }

    // Calculate survey date 
	replace svdate = svdate/12 + 1900
    egen svdate2 = mean(svdate)
    replace svdate = svdate2
    drop svdate2
    
    // AGE GROUPINGS
    gen age_group5 = .
    local ticker = 1
    foreach g of numlist 15(5)45 {
      replace age_group5 = `ticker' if age >= `g' & age < (`g' + 5)
      local ticker = `ticker' + 1
    }
    label define age_group5lbl 1 "15-19" 2 "20-24" 3 "25-29" 4 "30-34" 5 "35-39" 6 "40-44" 7 "45-49"
    label values age_group5 age_group5lbl
  
    gen age_group2 = .
    local ticker = 2
    replace age_group2 = 1 if age >= 15 & age < 18
    foreach g of numlist 18(2)48 {
      replace age_group2 = `ticker' if age >= `g' & age < (`g' + 2)
      local ticker = `ticker' + 1
    }
    label define age_group2lbl 1 "15-17" 2 "18-19" 3 "20-21" 4 "22-23" 5 "24-25" 6 "26-27" 7 "28-29" 8 "30-31" 9 "32-33" 10 "34-35" 11 "36-37" 12 "38-39" 13 "40-41" 14 "42-43" 15 "44-45" 16 "46-47" 17 "48-49"
    label values age_group2 age_group2lbl

    // TIMEFB GROUPINGS
    gen timefb_group5 = .
    local ticker = 1
    foreach g of numlist 0(5)30 {
      replace timefb_group5 = `ticker' if timefb >= `g' & timefb < (`g' + 5)
      local ticker = `ticker' + 1
    }
    label define timefb_group5lbl 1 "0-4" 2 "5-9" 3 "10-14" 4 "15-19" 5 "20-24" 6 "25-29" 7 "30-34"
    label values timefb_group5 timefb_group5lbl
  
    gen timefb_group2 = .
    local ticker = 1
    replace timefb_group2 = 17 if timefb >= 32 & timefb < 35
    foreach g of numlist 0(2)30 {
      replace timefb_group2 = `ticker' if timefb >= `g' & timefb < (`g' + 2)
      local ticker = `ticker' + 1
    }
    label define timefb_group2lbl 1 "0-1" 2 "2-3" 3 "4-5" 4 "6-7" 5 "8-9" 6 "10-11" 7 "12-13" 8 "14-15" 9 "16-17" 10 "18-19" 11 "20-21" 12 "22-23" 13 "24-25" 14 "26-27" 15 "28-29" 16 "30-31" 17 "32-34"
    label values timefb_group2 timefb_group2lbl
    
    // If numbers are too small for a particular TFB group, drop them (every group must have more than 10)
    forvalues x = 1/7 {
		count if timefb_group5 == `x'
		local n = r(N)
    		if `n' < 10 replace timefb_group5 = . if timefb_group5 == `x'
		}
    
    forvalues x = 1/17 {
		count if timefb_group2 == `x'
		local n = r(N)
		if `n' < 10 replace timefb_group2 = . if timefb_group2 == `x'
		}    

	// Save all data 
	gen country_year = floor(svdate)
	tostring country_year, replace force
	replace country_year = iso3 + ", " + country_year

	saveold "`savepath'/PROJECT_INT_DHS_SummaryBH.dta", replace

  
// Create age and time since first birth specific datasets 
	use "`savepath'/PROJECT_INT_DHS_SummaryBH.dta", clear
  
	preserve
	drop timefb*
	renpfix age_
	drop if age == . | age < 15 | age > 49
	saveold "`savepath'/PROJECT_INT_DHS_SummaryBH_age.dta", replace
	restore

	preserve
	drop age*
	renpfix timefb_
	drop if timefb == . | timefb > 34 | timefb < 0
	saveold "`savepath'/PROJECT_INT_DHS_SummaryBH_timefb.dta", replace
	restore
	
	keep iso3 svdate sample NID
	duplicates drop 
	saveold "`savepath'/survey_women_sample_types.dta", replace 
	
  cd "`savepath'"
  use "PROJECT_INT_DHS_SummaryBH_age.dta", clear
  
  global grouping_vars = "country_year iso3 svdate NID"	
	
	
// MAC data
  preserve
  gen n_mothers = sample_weight
  gen n_mothers_nowgt = 1 
  gen n_children = ceb*sample_weight
  gen n_children_nowgt = ceb  
  replace ceb = ceb*sample_weight
  replace cd = cd*sample_weight
  
  decode group5, gen(agegroup)
  
  collapse (sum) n_mothers n_children n_mothers_nowgt n_children_nowgt ceb cd, by($grouping_vars agegroup)
  saveold MAC_totals, replace
   
  restore

  
// MAP data
  preserve
  collapse (sum) sample_weight, by($grouping_vars group2 ceb)
  decode group2, gen(agegroup)
  drop group2
  drop if ceb == 0
  saveold MAP_ageceb, replace
  restore
  
  preserve
  collapse (sum) sample_weight, by($grouping_vars group2 cd)
  decode group2, gen(agegroup)
  drop group2
  drop if cd == . | cd == 0
  saveold MAP_agecd, replace
  restore	
  
  
  // TFBC data
  preserve 
  gen n_mothers = sample_weight
  gen n_mothers_nowgt = 1   
  gen n_children = ceb*sample_weight
  gen n_children_nowgt = ceb
  replace ceb = ceb*sample_weight
  replace cd = cd*sample_weight
  
  decode group5, gen(timefbgroup)
  
  collapse (sum) n_mothers n_children n_mothers_nowgt n_children_nowgt ceb cd, by($grouping_vars timefbgroup)
  saveold TFBC_totals, replace

  restore

  
  // TFBP data
  preserve
  collapse (sum) sample_weight, by($grouping_vars group2 ceb)
  decode group2, gen(timefbgroup)
  drop group2
  drop if ceb == 0
  saveold TFBP_timefbceb, replace
  restore
  
  preserve
  collapse (sum) sample_weight, by($grouping_vars group2 cd)
  decode group2, gen(timefbgroup)
  drop group2
  drop if cd == . | cd == 0
  saveold TFBP_timefbcd, replace
  restore
