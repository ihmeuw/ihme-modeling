** ********************************************************************
** Date created: August 10, 2009
** Description: Combines deaths, population, and completeness and calculates 45q15
** 
** NOTE: IHME OWNS THE COPYRIGHT
** ********************************************************************

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

	if (c(os)=="Unix") global root "FILEPATH"
	if (c(os)=="Windows") global root "FILEPATH"
	global smoothed_comp_file = "FILEPATH/d08_smoothed_completeness.dta"
	global pop_file = "FILEPATH/d09_denominators.dta"
	global deaths_file = "FILEPATH/d01_formatted_deaths.dta"
	global save_file = "FILEPATH/d10_45q15.dta"
	
	
** **********************
** Format deaths data
** **********************

	use "$deaths_file", clear

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

** cumulate deaths into 5-year age groups 
    gen vr_0to0 = deaths0
    egen vr_1to4 = rowtotal(deaths1-deaths4)
	forvalues j = 5(5)95 {
		local jplus = `j'+4
		egen vr_`j'to`jplus' = rowtotal(deaths`j'-deaths`jplus') if `j' < openendv
	}

	keep ihme_loc_id year sex country vr* deaths_source source_type deaths_nid
	sort ihme_loc_id year sex source_type
	
	replace source_type = "DSP" if regexm(source_type, "DSP") == 1 
	replace source_type = "SRS" if regexm(source_type, "SRS") == 1 
	replace source_type = "VR" if regexm(source_type, "VR") == 1 & !regexm(source_type, "HMD") & source_type != "VR-SSA"
	
	tempfile deathdataformatted
	save `deathdataformatted', replace

	
** **********************
** Combine deaths data and completeness estimates
** **********************

** get smoothed adult completeness estimates
	use "$smoothed_comp_file", clear
	split iso3_sex_source, parse("&&")
	rename iso3_sex_source3 source_type
	rename iso3_sex_source2 sex
	
	drop iso3_sex_source1 // drop extra digits
	
	** calculate usable completeness estimate -- mean of sims for census/survey, mean of trunc sims for VR/SRS/DSP
	replace comp = pred
	replace comp = trunc_pred if regexm(source_type, "VR")==1 | regexm(source_type, "SRS")==1 | regexm(source_type, "DSP")==1
		** there are duplicates here because during the simulation each line is estimated separately and the final truncated 
		** estimates are not exactly the same 
    ren u5_comp_pred comp_u5
    ren u5_comp comp_u5_pt_est
	collapse (mean) comp sd comp_u5 comp_u5_pt_est, by(ihme_loc_id sex source_type year)
	
	** duplicate for both sexes everywhere but SAU (SAU has estimates for both males and females) 
	gen id = _n 
	expand 2 if sex == "both"
	bysort id: replace sex = "male" if _n == 1 & sex == "both"
	bysort id: replace sex = "female" if _n == 2 & sex == "both"
	tab sex
	drop id 
 
	** we need to duplicate SSPC-DC so that it matches with both SSPC and DC 
	expand 2 if source_type == "SSPC-DC" 
	replace source_type = "DC" if source_type == "SSPC-DC" 
	bysort year ihme_loc_id sex source_type: replace source_type = "SSPC" if source_type == "DC" & _n == 2
	
    ** recombine South Korea 
	drop if source == "VR1" & ihme_loc_id == "KOR" & year > 1977 
	drop if source == "VR2" & ihme_loc_id == "KOR" & (year <= 1977 | year >= 2000)
	drop if source == "VR3" & ihme_loc_id == "KOR" & year < 2000

	
	** recombine china subnational DSP
	drop if source == "DSP1" & regexm(ihme_loc_id,"CHN_") & ihme_loc_id != "CHN_44533" &  year >= 1996
	drop if source == "DSP2" & regexm(ihme_loc_id,"CHN_") & ihme_loc_id != "CHN_44533" & (year < 1996 | year >=2004)
    drop if source == "DSP3" & regexm(ihme_loc_id,"CHN_") & ihme_loc_id != "CHN_44533" & year < 2004

    
	** recombine china DSP
	drop if source == "DSP-1996-2000" & ihme_loc_id == "CHN_44533" & (year < 1996 | year >=2004)
    drop if source == "DSP-2004-2010" & ihme_loc_id == "CHN_44533" & year < 2004

	
	** Recombine the ZAF VR system
	drop if source == "VR_pre2002" & regexm(ihme_loc_id,"ZAF") & year > 2002
	drop if source == "VR_post2002" & regexm(ihme_loc_id,"ZAF") & year <= 2002

	** Recombine the MEX VR System
	drop if source == "MEX_VR_pre2011" & regexm(ihme_loc_id,"MEX") & year > 2011
	drop if source == "MEX_VR_post2011" & regexm(ihme_loc_id,"MEX") & year <= 2011

	bysort ihme_loc_id source_type: egen min_comp = min(comp)

	replace min_comp = 0.951 if ihme_loc_id == "SVN"

	replace source = "VR" if regexm(source, "VR") & ihme_loc_id == "KOR"
	replace source = "DSP" if regexm(ihme_loc_id,"CHN_") & ihme_loc_id != "CHN_44533" & regexm(source,"DSP")
	replace source = "DSP" if ihme_loc_id == "CHN_44533" & regexm(source,"DSP")
	replace source = "VR" if inlist(source,"VR_pre2002","VR_post2002") & regexm(ihme_loc_id,"ZAF")
	replace source = "VR" if inlist(source,"MEX_VR_pre2011","MEX_VR_post2011") & regexm(ihme_loc_id,"MEX")

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
	keep ihme_loc_id year sex pop_source c1* pop_nid
	tempfile pop1
	save `pop1'	

	use "$pop_file", clear
	drop if sex == "both" 
	drop if source_type == "IHME" 
	keep ihme_loc_id year sex source_type pop_source c1* pop_nid

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
	
** Pull in sources where all we have is unadjusted mortality rates (these are select years from systems that we are able to DDM parts of)
	preserve
	use "FILEPATH/BGD_SRS_1999_2005_MR_UNADJUSTED.DTA", clear
	drop if year == 2005
	append using "FILEPATH/IND_SRS_1990_1991_1995_MR_UNADJUSTED.DTA"
	append using "FILEPATH/IND_SRS_1981_2009_MR_UNADJUSTED.DTA"
	append using "FILEPATH/DZA_VR/USABLE_DZA_VR_2010_2011_MR.DTA"
    
	rename iso3 ihme_loc_id
	
	merge 1:1 ihme_loc_id year source_type sex using `ddm'
	drop if _m == 2
	drop _m
	gen pop_source = "none - mortality rates" 
	tempfile add_mr
	save `add_mr', replace
	restore 
	append using `add_mr'
	
** generate an adjust variable
	gen adjust = 1 if comp != . 

	replace comp = 1 if min_comp > 0.95 & min_comp != . & inlist(source_type, "VR", "VR-SSA", "SRS", "DSP") & ihme_loc_id != "JAM"
	replace adjust = 3 if min_comp > 0.95 & min_comp != . & inlist(source_type, "VR", "VR-SSA", "SRS", "DSP") & ihme_loc_id != "JAM"
	
	tempfile all 
	save `all', replace
	keep ihme_loc_id source_type adjust sd
	keep if adjust == 3
	duplicates drop
	duplicates tag ihme_loc_id source_type, gen(d)
	bysort ihme_loc_id source_type adjust  : gen ob = _n 
	drop if d > 0 & ob > 1
	drop d ob

	rename (ihme_loc_id sd adjust) (parent_loc_id parent_sd parent_adjust)

	replace parent_loc_id = "CHN" if parent_loc_id == "CHN_44533"
	tempfile parent_comp
	save `parent_comp'

	use `all', clear
	gen parent_loc_id = substr(ihme_loc_id,1,3) if regexm(ihme_loc_id,"_")
	merge m:1 parent_loc_id source_type  using `parent_comp', keep(1 3)
	replace comp = 1 if parent_adjust == 3 & comp != .
*	replace sd = 0 if parent_adjust == 3 & comp != .
	replace sd = parent_sd if parent_adjust == 3 & comp != .
	replace adjust = 3 if parent_adjust == 3 & comp != .
	drop parent_adjust parent_loc_id

** make a manual exception for Taiwan, Hong Kong, and DEU
	replace comp = 1 if inlist(ihme_loc_id, "TWN", "CHN_354", "DEU")
	replace adjust = 3 if inlist(ihme_loc_id, "TWN", "CHN_354", "DEU")
    
** Output a high- / low- quality VR metric for use in CoD model selection process
** We say that if completeness is over .95 for at least 25 years past 1980, and it's on the CoD cause-specific VR list, it should be called good quality
    tempfile master
    save `master'
	
		use "FILEPATH/VR_data_master_file_with_cause.dta", clear
		** keep only the country years
		keep iso3 year
		duplicates drop
		** get rid of India
		drop if iso3=="IND"
		tempfile cod_years
		save `cod_years'
		
	// Get points marked as outliers in 45q15
		insheet using "FILEPATH/raw.45q15.txt", comma clear
		keep if regexm(source_type,"VR") 
		collapse (sum) exclude shock, by(ihme_loc_id year source_type)
		keep if shock > 0 | exclude > 0
		keep ihme_loc_id year source_type
		gen outlier_45q15 = 1
		tempfile ihme_outliers
		save `ihme_outliers'
	
    use `master' if year >= 1980 & (regexm(source_type,"VR") | source_type == "DSP"), clear
    collapse (mean) comp, by(source_type ihme_loc_id year)
    split ihme_loc_id, parse("_") 
    rename ihme_loc_id1 iso3
    drop ihme_loc_id2 
    
	// Keep data that's listed in CoD
	merge m:1 iso3 year using `cod_years', keep(3) nogen
	
	// Keep only points that weren't marked as outliers in 45q15
	merge 1:1 ihme_loc_id year source_type using `ihme_outliers', keep(1 3) nogen
	replace outlier_45q15 = 0 if outlier_45q15 == .
	
    sort ihme_loc_id year
    outsheet using "FILEPATH/cod_completeness.csv", comma replace
    
    gen comp_year_count = 1 if comp >= .95 | inlist(ihme_loc_id,"CHN_354","CHN_361")
    replace comp_year_count = 0 if comp_year_count == . | outlier_45q15 == 1
    collapse (sum) comp_year_count, by(ihme_loc_id)
    gen quality = "High" 
    replace quality = "Low" if comp < 25
    outsheet using "FILEPATH/cod_comp_collapsed.csv", comma replace
	
	use `master', clear
	
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
	replace adjust = 0 if adj45q15 == . | regexm(source_type, "HOUSEHOLD") | (ihme_loc_id == "ZAF" & year == 2007 & deaths_source == "IPUMS_HHDEATHS")
    
    ** don't use adjusted value if we can't do the adjustment or if it is a household death point (including the ZAF IPUMS HH deaths survey)
	replace adj45q15 = obs45q15 if adj45q15 == . | regexm(source_type, "HOUSEHOLD") | (ihme_loc_id == "ZAF" & year == 2007 & deaths_source == "IPUMS_HHDEATHS")
	
	** split out DOM series that was combined
	replace deaths_source = "DYB" if ihme_loc_id == "DOM" & deaths_source == "WHO_causesofdeath+DYB" & inrange(year, 2007, 2010)
	replace deaths_source = "WHO_causesofdeath" if ihme_loc_id == "DOM" & deaths_source == "WHO_causesofdeath+DYB" & inrange(year, 2003, 2006) 
	
** **********************
** Format and save results 
** **********************
	
	keep ihme_loc_id source_type sex year deaths_source pop_source comp_u5_pt_est comp_u5 comp sd adjust obs45q15 adj45q15 vr* c1* obsasmr_* adjasmr_* *_nid
	order ihme_loc_id source_type sex year deaths_source pop_source *_nid comp_u5_pt_est comp_u5 comp sd adjust obs45q15 adj45q15 vr* c1* obsasmr_* adjasmr_* 
	sort ihme_loc_id source_type sex year deaths_source pop_source comp_u5_pt_est comp_u5 comp sd adjust obs45q15 adj45q15 vr* c1* obsasmr_* adjasmr_*
	compress

	saveold "$save_file", replace
