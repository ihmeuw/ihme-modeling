
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
	local version_id `1'
	global main_dir = FILEPAHT
	global ddm_file = FILEPATH
	global ccmp_file = FILEPATH
	global child_file = FILEPATH
	global save_file = FILEPATH


** **********************
** Combine all comp estimates
** **********************

** Merge DDM and CCMP 
	use "$ccmp_file", clear
	replace ihme_loc_id = substr(ihme_loc_id,1,strpos(ihme_loc_id, "&&")-1) if strpos(ihme_loc_id,"&&") ~= 0
	tempfile ccmp
	save `ccmp'

	use "$ddm_file", clear
	merge 1:1 ihme_loc_id pop_years deaths_years country source_type sex using `ccmp'
	drop _merge
	
** Format raw DDM estimates 

	g year1 = substr(pop_years, 1, strpos(pop_years," ") - 1)
	g year2 = substr(pop_years,strpos(pop_years," ") + 1,.)
	destring year1 year2, replace force

    g year = (year1+year2)/2
	drop pop_years

	rename ggb compggb
	rename seg compseg
	rename ggbseg compggbseg
	rename CCMP_aplus_no_migration compCCMP_aplus_no_migration
	rename CCMP_aplus_migration compCCMP_aplus_migration
	
	bysort ihme_loc_id sex source_type: gen id = _n 
	
	reshape long comp, i(ihme_loc_id year1 year2 sex source_type id pop_nid underlying_pop_nid deaths_nid deaths_underlying_nid) j(comp_type, string)
	drop if comp == . 
	drop if comp == 0 

** Recombine sources that were DDMd separately 
	replace source_type = "SRS" if regexm(source_type, "SRS")==1
	replace source_type = "DSP" if regexm(source_type, "DSP")==1
	replace source_type = "VR" if regexm(source_type, "VR")==1 & !inlist(source_type,"VR-SSA", "IRN_VR_pre2015", "IRN_VR_post2015", "QOM_VR1", "QOM_VR2")
	replace source_type = "SSPC-DC" if source_type == "SSPC" | source_type == "DC"


** Format deaths/pop nids into nid and underlying nid columns
	replace deaths_nid = subinstr(deaths_nid, " ", "", .)
	replace pop_nid = subinstr(pop_nid, " ", ";", .) if regexm(pop_nid, ";") != 1
	replace pop_nid = subinstr(pop_nid, " ", "", .)
	gen nid = ""
	replace nid = pop_nid
	replace nid = pop_nid + "," + deaths_nid if deaths_nid != "" & pop_nid != "" 
	replace nid = deaths_nid if deaths_nid != "" & pop_nid == "" & nid == ""
	drop pop_nid deaths_nid

	replace deaths_underlying_nid = subinstr(deaths_underlying_nid, " ", "", .)
	replace underlying_pop_nid = subinstr(underlying_pop_nid, " ", ";", .) if regexm(underlying_pop_nid, ";") != 1
	replace underlying_pop_nid = subinstr(underlying_pop_nid, " ", "", .)
	gen underlying_nid = ""
	replace underlying_nid = underlying_pop_nid
	replace underlying_nid = underlying_pop_nid + "," + deaths_underlying_nid if deaths_underlying_nid != "" & underlying_pop_nid != "" 
	replace underlying_nid = deaths_underlying_nid if deaths_underlying_nid != "" & underlying_pop_nid == "" & nid == ""
	drop deaths_underlying_nid underlying_pop_nid

** Format child comp and combine with DDM 
	preserve
	use "$child_file", clear 
	gen id = _n 
	expand 3 
	gen sex = "both"
	bysort id: replace sex = "male" if _n == 2
	bysort id: replace sex = "female" if _n == 3
	drop id 
	replace year = floor(year)
	tempfile child
	save `child' 
	restore
	
	append using `child' 
	
** Format for smoothing
	g iso3_sex_source = ihme_loc_id + "&&" + sex + "&&" + source_type
	g detailed_comp_type = comp_type
	replace comp_type = "ddm" if comp_type != "u5"

	bysort iso3_sex_source comp_type year: g num = _n
	bysort iso3_sex_source comp_type year: egen numofddm = max(num)
	
** save input to GPR 
	order ihme_loc_id iso3_sex_source year comp_type comp detailed_comp_type id nid underlying_nid
	keep ihme_loc_id iso3_sex_source year year1 year2 comp_type comp detailed_comp_type id nid underlying_nid
	sort  ihme_loc_id iso3_sex_source comp_type year
	save "$save_file", replace
