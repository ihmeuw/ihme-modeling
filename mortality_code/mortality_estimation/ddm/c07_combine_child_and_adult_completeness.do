********************************************************
** Date created: August 10, 2009
** Description: Formats population and deaths data.
** 
** NOTE: IHME OWNS THE COPYRIGHT
********************************************************

** **********************
** Set up Stata 
** **********************

	clear all
	capture cleartmp 
	set mem 500m
	set more off

	
** **********************
** Filepaths 
** **********************

	if (c(os)=="Unix") global root "FILEPATH"	
	if (c(os)=="Windows") global root "FILEPATH"
	global ddm_file = "FILEPATH/d05_formatted_ddm.dta"
	global child_file = "FILEPATH/d06_child_completeness.dta"
	global save_file = "FILEPATH/d07_child_and_adult_completeness.dta"


** **********************
** Combine all comp estimates
** **********************

** Format raw DDM estimates 
	use "$ddm_file", clear

	g year1 = substr(pop_years,strpos(pop_years," ")-4,4)
	g year2 = substr(pop_years,-4,.)
	destring year1 year2, replace force
    g year = (year1+year2)/2
	drop year1 year2 

	rename ggb compggb
	rename seg compseg
	rename ggbseg compggbseg
	
	bysort ihme_loc_id sex source_type: gen id = _n 
	
	reshape long comp, i(ihme_loc_id pop_years sex source_type id) j(comp_type, string)
	drop if comp == . 
	drop if comp == 0 

** Recombine sources that were DDMd separately 
	replace source_type = "SRS" if regexm(source_type, "SRS")==1
	replace source_type = "DSP" if regexm(source_type, "DSP")==1
	// We don't replace and re-split these sources later because the DDM year estmiate is based on midpoint of censuses which might not actually line up with the years of death data used
	// e.g. IRN_VR_post2003 might be called year = 2002 or year=2003 because that's the midpoint of the censuses used to analyze it
	replace source_type = "VR" if regexm(source_type, "VR")==1 & !inlist(source_type,"VR-SSA","VR_pre2002","VR_post2002","IRN_VR_pre2003","IRN_VR_post2003","VR1","VR2", "VR3") & !inlist(source_type,"MEX_pre2011","MEX_post2011") 
	replace source_type = "SSPC-DC" if source_type == "SSPC" | source_type == "DC"
	
** Format child comp as combine with DDM 
	preserve
	use "$child_file", clear 
	gen id = _n 
	expand 3 
	gen sex = "both"
	bysort id: replace sex = "male" if _n == 2
	bysort id: replace sex = "female" if _n == 3
	drop id 
	tempfile child
	save `child' 
	restore
	
	append using `child' 
	
	
** **********************
** Prep for Smoothing
** **********************
    
** resplit South Korea
	replace source_type = "VR1" if ihme_loc_id == "KOR" & source_type == "VR" & year <= 1977
	replace source_type = "VR2" if ihme_loc_id == "KOR" & source_type == "VR" & year > 1977 & year < 2000
	replace source_type = "VR3" if ihme_loc_id == "KOR" & source_type == "VR" & year >= 2000
	
** resplit CHN provincial DSP
	replace source_type = "DSP1" if regexm(ihme_loc_id,"CHN_") & ihme_loc_id != "CHN_44533" & source_type == "DSP" & year < 1996
    replace source_type = "DSP2" if regexm(ihme_loc_id,"CHN_") & ihme_loc_id != "CHN_44533" &  source_type == "DSP" & year >= 1996 & year < 2004
	replace source_type = "DSP3" if regexm(ihme_loc_id,"CHN_") & ihme_loc_id != "CHN_44533" &  source_type == "DSP" & year >= 2004
    
** resplit CHN  DSP
    replace source_type = "DSP-1996-2000" if ihme_loc_id == "CHN_44533" & source_type == "DSP" & year >= 1996 & year < 2004
	replace source_type = "DSP-2004-2010" if ihme_loc_id == "CHN_44533" & source_type == "DSP" & year >= 2004

** resplit child ZAF VR (adult is already split from the !inlist on recombine)
    replace source_type = "VR_pre2002" if regexm(ihme_loc_id,"ZAF") & source_type == "VR" & year <= 2002
    replace source_type = "VR_post2002" if regexm(ihme_loc_id,"ZAF") & source_type == "VR" & year > 2002
	
** resplit child MEX VR
    replace source_type = "MEX_VR_pre2011" if regexm(ihme_loc_id,"MEX") & source_type == "VR" & year <= 2011
    replace source_type = "MEX_VR_post2011" if regexm(ihme_loc_id,"MEX") & source_type == "VR" & year > 2011
	
	
** Format for smoothing
	g iso3_sex_source = ihme_loc_id + "&&" + sex + "&&" + source_type
	g detailed_comp_type = comp_type
	replace comp_type = "ddm" if comp_type != "u5"

	bysort iso3_sex_source comp_type year: g num = _n
	bysort iso3_sex_source comp_type year: egen numofddm = max(num)
	
** save input to GPR 
	order ihme_loc_id iso3_sex_source year comp_type comp detailed_comp_type id
	keep ihme_loc_id iso3_sex_source year comp_type comp detailed_comp_type id 
	sort  ihme_loc_id iso3_sex_source comp_type year
	saveold "$save_file", replace

