
** **********************
** Set up Stata 
** **********************

	clear all 
	capture cleartmp
	set mem 500m
	set more off
    cap restore, not

	
** **********************
** Filepaths  
** **********************

	local version_id `1'
	global main_dir FILEPATH
	global both_sex_only_indicators FILEPATH
	global sex_ratios FILEPATH
	global smoothed_comp_file = FILEPATH
	global pop_file = FILEPATH
	global deaths_file = FILEPATH
	global save_file = FILEPATH
	
	
** **********************
** Format deaths data
** **********************
	cap import delim using "$both_sex_only_indicators", clear
	count
	local both_sex_only_count `r(N)'
	if `both_sex_only_count' > 0 {
		gen sex = "both"
		gen both_sex_only = 1
		tempfile both_sex_only_flags
		save `both_sex_only_flags', replace
	}

	cap import delim using "$sex_ratios", clear
	count
	local sex_ratios_count `r(N)'
	if `sex_ratios_count' > 0 {
		tempfile sex_ratios
		save `sex_ratios', replace		
	}

	use "$deaths_file", clear

	* Merge on file of both sex only identifiers
	if `both_sex_only_count' > 0 & `sex_ratios_count' > 0 {
		merge 1:1 ihme_loc_id year sex source_type using `both_sex_only_flags', keep(1 3) nogen
		replace both_sex_only = 0 if mi(both_sex_only)

		* Merge on file of both sex only identifiers
		preserve
		keep if both_sex_only == 1

		* Expand to create males and females
		gen id = _n 
		expand 2
		bysort id: replace sex = "male" if _n == 1
		bysort id: replace sex = "female" if _n == 2
		drop id

		* Get the max age group for each observation
		gen max_age = 0
		gen max_age_tempstore = 0
		forvalues i = 0/99 {
			replace max_age_tempstore = agegroup`i'
			replace max_age = max_age_tempstore if max_age_tempstore > max_age & max_age_tempstore != .
		}
		drop max_age_tempstore

		* Merge on sex ratios
		merge 1:1 ihme_loc_id year sex using `sex_ratios', keep(3) nogen

		* Apply the sex ratios
		forvalues j = 0/100 {
			* If it's greater than the max age, multiply subsequent age groups by the ratio for the terminal age group
			* If it's greater than the max age and the max age is greater than 80, multiply subsequent age groups by 80plus 
			replace deaths`j' = deaths`j' * ratio`j' if deaths`j' != . & ratio`j' != .
			replace deaths`j' = deaths`j' * ratio80plus if `j' >= max_age & `j' >= 80
			replace deaths`j' = deaths`j' * ratio60plus if `j' >= max_age & `j' < 80 & `j' >= 60
			replace deaths`j' = deaths`j' * ratio45plus if `j' >= max_age & `j' < 60 & `j' >= 45
		}

		drop max_age ratio*
		tempfile scaled_data
		save `scaled_data', replace
		restore

		append using `scaled_data'
		drop both_sex_only
	}

** drop data for both sexes and before 1950 
	drop if sex == "both"
	drop if year < 1950

** drop country-years with an open interval starting at less than 60
	gen openendv = .
	forvalues j = 0/99 {
		local jplus = `j'+1
		replace openendv = agegroup`j' if agegroup`jplus' == . & openendv == .
	}
	replace openendv = 100 if openendv == .
	drop if openendv < 60

** drop country-years with unhelpful age groups 
	forvalues j = 0/99 {
		local jplus = `j'+1
		gen dist`j'_`jplus' = agegroup`jplus'-agegroup`j'
	}

	drop if dist0_1 == 10 | dist0_1 == 15 | dist0_1 == 25
	drop if dist1_2 == 14 | dist1_2 == 15 | dist1_2 == 19 | dist1_2 == 49

	forvalues j = 0/99 {
		local jplus = `j'+1
		drop if dist`j'_`jplus' > 10 & agegroup`j' != . & agegroup`jplus' != . & agegroup`j' < openendv	
	}

	forvalues j = 0/100 {
		rename agegroup`j' agegroupv`j'
	}

** Generate max and min age gaps for each data point
	gen max_age_gap = 0
	gen min_age_gap = 500
	gen age_gap_tempstore = 0
	forvalues i = 0/99 {
		local iplus = `i' + 1
		replace age_gap_tempstore = agegroupv`iplus' - agegroupv`i'
		replace max_age_gap = age_gap_tempstore if age_gap_tempstore > max_age_gap & age_gap_tempstore != . & agegroupv`i' > 5
		replace min_age_gap = age_gap_tempstore if age_gap_tempstore <= min_age_gap & age_gap_tempstore != . & agegroupv`i' > 5
	}


** cumulate deaths into 5-year age groups 
    gen vr_0to0 = deaths0
    egen vr_1to4 = rowtotal(deaths1-deaths4)
	forvalues j = 5(5)95 {
		local jplus = `j'+4
		egen vr_`j'to`jplus' = rowtotal(deaths`j'-deaths`jplus') if `j' < openendv
	}
	levelsof openendv, local(terminalages)
	foreach tt of local terminalages {
		egen vr_`tt'plus = rowtotal(deaths`tt'-deaths100) if `tt' == openendv
	}

	keep ihme_loc_id year sex country vr* deaths_source source_type deaths_nid deaths_underlying_nid hh_scaled min_age_gap max_age_gap
	sort ihme_loc_id year sex source_type
	
	replace source_type = "DSP" if regexm(source_type, "DSP") == 1 
	replace source_type = "SRS" if regexm(source_type, "SRS") == 1 
	replace source_type = "VR" if regexm(source_type, "VR") == 1 & source_type != "VR-SSA"
	
	tempfile deathdataformatted
	save `deathdataformatted', replace

	
** **********************
** Combine deaths data and completeness estimates
** **********************

** get smoothed adult completeness estimates
	use "$smoothed_comp_file", clear

	rename source source_type
	rename final_comp comp

	rename u5_comp_pred comp_u5
    rename u5_comp comp_u5_pt_est

	tempfile ddm
	save `ddm', replace 
	
** combine deaths data and completeness 
	merge 1:1 ihme_loc_id year sex source_type using `deathdataformatted'
	drop if _m == 1
	drop _m
	
	save `deathdataformatted', replace
	
** **********************
** Combine deaths and population data
** **********************	

	use "$pop_file", clear
	drop if sex == "both" 
	keep if source_type == "IHME" 
	keep ihme_loc_id year sex pop_source c1* pop_nid underlying_pop_nid
	tempfile pop1
	save `pop1'	

	use "$pop_file", clear
	drop if sex == "both" 
	drop if inlist(source_type, "IHME", "VR") 
	keep ihme_loc_id year sex source_type pop_source c1* pop_nid underlying_pop_nid
	
	tempfile pop2
	save `pop2'
	
	use `deathdataformatted', clear
	merge m:1 ihme_loc_id year sex using `pop1'
	drop if _m == 2
	drop _m 
	
	merge m:1 ihme_loc_id year sex source_type using `pop2', update replace
	drop if _m == 2
	drop _m 
	
	drop if vr_15to19 == . | c1_15to19 == . 
	
** **********************
** Calculate unadjusted and adjusted 45q15
** **********************

** Generate unadjusted (observed) mortality rate 
	forvalues j = 15(5)55 {
		local jplus = `j'+4
		capture: gen vr_`j'to`jplus' = .
		capture: gen c1_`j'to`jplus' = .
		gen obsasmr_`j'to`jplus' = vr_`j'to`jplus'/c1_`j'to`jplus' if vr_`j'to`jplus' != . & c1_`j'to`jplus'!= . 
	}
	
** Pull in sources where all we have is unadjusted mortality rates
	preserve
	use FILEPATH, clear
	drop if year == 2005
	append using FILEPATH
	append using FILEPATH
	append using FILEPATH
    
	rename iso3 ihme_loc_id
	
	merge 1:1 ihme_loc_id year source_type sex using `ddm'
	drop if _m == 2
	drop _m
	gen pop_source = "none - mortality rates" 

	tempfile add_mr
	save `add_mr', replace
	restore 
	append using `add_mr'
	
	replace deaths_nid = "93495" if ihme_loc_id == "DZA" & year == 2010 & source_type == "VR"
	replace deaths_nid = "118440" if ihme_loc_id == "DZA" & year == 2011 & source_type == "VR"
	replace deaths_nid = "57646" if ihme_loc_id == "BGD" & inlist(year, 1990, 2004, 2006) & source_type == "SRS"
	replace deaths_nid = "33837" if ihme_loc_id== "IND" & source_type == "SRS" & inlist(year, 1981, 1982, 1983, 1984, 1985)
	replace deaths_nid = "68236" if ihme_loc_id== "IND" & source_type == "SRS" & year == 1987
	replace deaths_nid = "25080" if ihme_loc_id== "IND" & source_type == "SRS" & year == 1990
	replace deaths_nid = "33841" if ihme_loc_id== "IND" & source_type == "SRS" & year == 1991
	replace deaths_nid = "33867" if ihme_loc_id== "IND" & source_type == "SRS" & year == 1995
	replace deaths_nid = "33764" if ihme_loc_id== "IND" & source_type == "SRS" & year == 2009
	
** Adjust the age-specific mortality rates
	forvalues j = 15(5)55 {
		local jplus = `j'+4
			gen adjasmr_`j'to`jplus' = (1/comp)*(obsasmr_`j'to`jplus') if obsasmr_`j'to`jplus' != . 
	}

** Calculate px values 
	forvalues j = 15(5)55 {
		local jplus = `j'+4
		gen obspx_`j'to`jplus' = 1-((obsasmr_`j'to`jplus'*5)/(1+2.5*obsasmr_`j'to`jplus')) 
		gen adjpx_`j'to`jplus' = 1-((adjasmr_`j'to`jplus'*5)/(1+2.5*adjasmr_`j'to`jplus')) 
	}
	
** Calculate 45q15
	gen obs45q15 = 1-(obspx_15to19*obspx_20to24*obspx_25to29*obspx_30to34*obspx_35to39*obspx_40to44*obspx_45to49*obspx_50to54*obspx_55to59)
	gen adj45q15 = 1-(adjpx_15to19*adjpx_20to24*adjpx_25to29*adjpx_30to34*adjpx_35to39*adjpx_40to44*adjpx_45to49*adjpx_50to54*adjpx_55to59)
	drop adjpx* obspx*
	replace adjust = 0 if adj45q15 == . | (hh_scaled == 0 & regexm(source_type, "VR") != 1 & regexm(source_type, "CENSUS") != 1)
    
	replace adj45q15 = obs45q15 if adj45q15 == . | (hh_scaled == 0 & regexm(source_type, "VR") != 1 & regexm(source_type, "CENSUS") != 1)
	replace deaths_source = "DYB" if ihme_loc_id == "DOM" & deaths_source == "WHO_causesofdeath+DYB" & inrange(year, 2007, 2010)
	replace deaths_source = "WHO_causesofdeath" if ihme_loc_id == "DOM" & deaths_source == "WHO_causesofdeath+DYB" & inrange(year, 2003, 2006) 
	
** **********************
** Format and save results 
** **********************
	
	keep ihme_loc_id source_type sex year deaths_source pop_source comp_u5_pt_est comp_u5 comp sd adjust hh_scaled obs45q15 adj45q15 vr* c1* obsasmr_* adjasmr_* *_nid min_age_gap max_age_gap
	order ihme_loc_id source_type sex year deaths_source pop_source *_nid comp_u5_pt_est comp_u5 comp sd adjust hh_scaled obs45q15 adj45q15 vr* c1* obsasmr_* adjasmr_* 
	sort ihme_loc_id source_type sex year deaths_source pop_source comp_u5_pt_est comp_u5 comp sd adjust hh_scaled obs45q15 adj45q15 vr* c1* obsasmr_* adjasmr_*
	compress

	saveold "$save_file", replace
