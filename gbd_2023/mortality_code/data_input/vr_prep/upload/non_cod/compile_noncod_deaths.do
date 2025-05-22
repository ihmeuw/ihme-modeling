** ***********************************************************************
** Description: Compiles data on deaths by age and sex from a variety of sources besides Causes of Deaths data.
**	Therefore it combines recently prepared (GBD 2017) VR data with older empirical deaths sources
** 	Originally compiled data on deaths by age and sex from all available sources.
**
** ***********************************************************************

** ***********************************************************************
** Set up Stata
** ***********************************************************************

	clear all
	capture cleartmp
	set mem 500m
	set more off
	pause on
	capture restore, not

local new_run_id = "FILEPATH"
local run_folder = "FILEPATH"
local input_dir = "FILEPATH"
local output_dir = "FILEPATH"

** ***********************************************************************
** Filepaths
** ***********************************************************************

	if (c(os)=="Unix") global root "FILEPATH"
	if (c(os)=="Windows") global root "FILEPATH"
	local date = c(current_date)
	global rawdata_dir "FILEPATH"
	global dyb_dir "FILEPATH"
	global newdata_dir "FILEPATH"
	global hhdata_dir "FILEPATH"
	global process_inputs "FILEPATH"

	adopath + "FILEPATH"

** ***********************************************************************
** Set up codes for merging
** ***********************************************************************

	import delimited using "FILEPATH", clear
	keep local_id_2013 ihme_loc_id location_name
	rename local_id_2013 iso3
	rename location_name country
	sort iso3
	tempfile countrymaster
	save `countrymaster'

	// Create a duplicate observation for subnationals
	expand 2 if regexm(ihme_loc_id,"_") & iso3 != "", gen(new)
	replace iso3 = ihme_loc_id if new == 1
	drop new

	replace iso3 = ihme_loc_id if iso3 == ""
	tempfile countrycodes
	save  `countrycodes', replace

	// Make a list of all the countries which contain subnational locations, for use in scaling/aggregation later
	import delimited using "FILEPATH", clear
	keep if regexm(ihme_loc_id,"_")
	split ihme_loc_id, parse("_")
	duplicates drop ihme_loc_id1, force
	keep ihme_loc_id1
	rename ihme_loc_id1 ihme_loc_id
	replace ihme_loc_id = "CHN_44533" if ihme_loc_id == "CHN"
	keep ihme_loc_id
	tempfile parent_map
	save `parent_map'

	// Get all subnational locations, along with the total number of subnationals expected in each
	import delimited using "FILEPATH", clear
	drop if level == 4 & (regexm(ihme_loc_id,"CHN") | regexm(ihme_loc_id,"IND"))
	drop if ihme_loc_id == "GBR_4749"
	split ihme_loc_id, parse("_")
	rename ihme_loc_id1 parent_loc_id
	keep ihme_loc_id parent_loc_id
	bysort parent_loc_id: gen num_locs = _N
	keep ihme_loc_id parent_loc_id num_locs
	replace parent_loc_id = "CHN_44533" if parent_loc_id == "CHN"
	tempfile subnat_locs
	save `subnat_locs'

	// Get UTLA parent map for aggregation to regions from CoD data
	import delimited using "FILEPATH", clear
	keep if regexm(ihme_loc_id, "GBR")
	keep ihme_loc_id parent_id
	gen parent_loc_id = "GBR_" + string(parent_id)
	drop parent_id
	tempfile gbr_locs
	save `gbr_locs', replace

	// Import NID sourcing file, and standardize for merging
	use "FILEPATH", clear
	tempfile deaths_nids_sourcing
	save `deaths_nids_sourcing', replace

	import delimited "FILEPATH", clear
	tempfile source_type_ids
	save `source_type_ids', replace

** ***********************************************************************
** Compile data
** ***********************************************************************
	noisily: display in green "COMPILE DATA"

** ************
** Multi country sources
** ************

** WHO database (both CoD and raw)
	** raw WHO VR numbers
	use "FILEPATH", clear

	replace SUBDIV = "VR"
	gen NID = 287600
	gen outlier = .
	replace outlier = 1 if CO == "HKG" & VR_SOURCE == "WHO" & YEAR >= 1969

	replace outlier = 1 if CO == "GBR" & VR_SOURCE == "WHO"

	** Drop MMR VR data
	replace outlier = 1 if CO == "MMR" & SUBDIV == "VR"

	** Drop Bahamas 1969 WHO point
	replace outlier = 1 if CO == "BHS" & SUBDIV == "VR" & VR_SOURCE == "WHO" & YEAR == 1969

	** Drop Dominican Republic 2010 WHO point and CoD 2011 VR point
	replace outlier = 1 if CO == "DOM" & SUBDIV == "VR" & VR_SOURCE == "WHO" & inlist(YEAR, 2010, 2009, 2008, 2007)

	** Drop Scotland 2019 and use country specific source
	replace outlier = 1 if CO == "GBR_434" & VR_SOURCE == "WHO" & YEAR == 2019

** WHO Internal Database
	append using "FILEPATH"

	tempfile compiled
	save `compiled'

** Demographic Yearbook
	// prepare dyb data
	use "FILEPATH", clear
	drop if COUNTRY == "SSD" & AREA == .
	drop NID
	gen NID = 140966
	replace NID = 140201 if VR_SOURCE == "DYB_CD"
	tempfile internal
	save `internal', replace

	// fix a terminal age group in dyb data in ZAF census 2011
	use `internal', clear
	keep if COUNTRY == "ZAF" & YEAR == 2011 & SUBDIV == "CENSUS"
	assert DATUM90plus == .
	drop DATUM90plus
	rename DATUM90to120 DATUM90plus
	tempfile zaf_dyb_fixed_terminal_age
	save `zaf_dyb_fixed_terminal_age'

	// fix a terminal age group in dyb data in MYS 2005, 2006
	use `internal', clear
	keep if COUNTRY == "MYS" & inlist(YEAR, 2005, 2006) & SUBDIV == "VR"
	drop DATUM98plus
	tempfile mys_dyb_fixed_terminal_age
	save `mys_dyb_fixed_terminal_age'

	// fix terminal age group in dyb data OMN 2012
	use `internal', clear
	keep if COUNTRY == "OMN" & YEAR == 2012 & SUBDIV == "VR"
	replace DATUM65plus = DATUM65to69 + DATUM70plus
	drop DATUM65to69 DATUM70plus
	tempfile omn_dyb_fixed_terminal_age
	save `omn_dyb_fixed_terminal_age'

	// At this point that data is as it was when WHO internal database was appeneded
	use `compiled', clear
	// append DYB internal
	quietly append using `internal'
	// drop bad age group from ZAF 2011
	drop if COUNTRY == "ZAF" & YEAR == 2011 & SUBDIV == "CENSUS"
	// append fixed age group
	append using `zaf_dyb_fixed_terminal_age'
	// drop bad age group from MYS 2005 and 2006
	drop if COUNTRY == "MYS" & inlist(YEAR, 2005, 2006) & SUBDIV == "VR" & VR_SOURCE == "DYB_download"
	// append the fixed data
	append using `mys_dyb_fixed_terminal_age'
	// drop bad age group from OMN 2012
	drop if COUNTRY == "OMN" & YEAR == 2012 & SUBDIV == "VR"
	//append fixed data
	append using `omn_dyb_fixed_terminal_age'

	**  internal are problematic when missing youngest age groups
	replace outlier = 1 if VR_SOURCE == "DYB_INTERNAL" & DATUM0to0 == . & DATUM0to4 == .

	** older VR data in MMR, GHA, and KEN are not nationally representative
	replace outlier = 1 if (CO == "GHA" | CO == "KEN") & SUBDIV == "VR" & regexm(VR_SOURCE, "DYB")
	replace outlier = 1 if CO == "MMR" & SUBDIV == "VR" & YEAR < 2000 & regexm(VR_SOURCE, "DYB")

	** VR data in GNQ, AGO, MOZ, CAF, MLI, TGO, and GNB are colonial-period only and likely have poor (or no) coverage of non-Europeans
	replace outlier = 1 if inlist(CO, "GNQ", "AGO", "MOZ", "CAF", "MLI", "TGO", "GNB")==1 & SUBDIV == "VR" & regexm(VR_SOURCE, "DYB")

	** Drop DYB from Dominican Republic in favor of CoD
	replace outlier = 1 if CO == "DOM" & VR_SOURCE == "DYB_ONLINE" & YEAR == 2011

	replace outlier = 1 if CO == "SAU" & regexm(VR_SOURCE, "DYB") & SUBDIV == "VR"

    ** not sure if 1993 SDN is just north, or both north and south, so we drop it
    replace outlier = 1 if CO == "SDN" & regexm(VR_SOURCE, "DYB") & SUBDIV == "CENSUS" & YEAR == 1993

	** make source type corrections
	replace SUBDIV = "VR" if CO == "KOR" & regexm(VR_SOURCE, "DYB")
	replace SUBDIV = "VR" if CO == "PRY" & regexm(VR_SOURCE, "DYB")
	replace SUBDIV = "CENSUS" if CO == "SRB" & YEAR <= 1991 & regexm(VR_SOURCE, "DYB")
	replace SUBDIV = "CENSUS" if CO == "MWI" & YEAR == 1977 & regexm(VR_SOURCE, "DYB")
	replace SUBDIV = "CENSUS" if CO == "PRK" & regexm(VR_SOURCE, "DYB")
	replace SUBDIV = "Statistical Report" if CO == "BWA" & YEAR == 2007 & regexm(VR_SOURCE, "DYB")
	replace SUBDIV = "CENSUS" if CO == "CHN" & regexm(VR_SOURCE, "DYB")
	replace SUBDIV = "CENSUS" if CO == "NAM" & YEAR == 2001 & regexm(VR_SOURCE, "DYB")
	replace SUBDIV = "CENSUS" if CO == "BOL" & YEAR == 1991 & regexm(VR_SOURCE, "DYB")
	replace SUBDIV = "SRS" if CO == "PAK" & regexm(VR_SOURCE, "DYB")
	replace SUBDIV = "SRS" if CO == "BGD" & YEAR >= 1980 & regexm(VR_SOURCE, "DYB")

	** dropping CHN census 2000
	replace outlier = 1 if COUNTRY == "CHN" & YEAR == 1999 & SUBDIV == "CENSUS" & VR_SOURCE == "DYB_ONLINE"

	duplicates tag COUNTRY YEAR VR_SOURCE SUBDIV if COUNTRY == "CHN_361" & YEAR == 2012 & outlier != 1, generate(dup)
	replace dup = 0 if mi(dup)
	replace outlier = 1 if dup >= 1 & CO == "CHN_361"
	drop dup

	** drop DYB CHN_354 points in favor of other sources
	replace outlier = 1 if inlist(CO, "CHN_354", "HKG") & regexm(VR_SOURCE, "DYB") & SUBDIV == "VR" & YEAR > 1954

	** drop mainland DYB point in favor of other census tabs
	replace outlier = 1 if CO == "CHN_44533" & YEAR == 2010 & regexm(VR_SOURCE, "DYB") & SUBDIV == "CENSUS"

	** ALB 2013 doesn't fit with the rest of our data
	replace outlier = 1 if CO == "ALB" & YEAR == 2013 & regexm(VR_SOURCE, "DYB")

	preserve

** Human Mortality Database
	use "$newdata_dir/hmd_vr/data/USABLE_ALL_AGE_DEATHS_HMD_VR.DTA", clear
	generate outlier = 0
    replace outlier = 1 if inlist(COUNTRY,"XNI","XSC") & SUBDIV == "VR" & VR_SOURCE == "HMD"

    ** Drop Belgium
    replace outlier = 1 if CO == "BEL" & YEAR > 2000 & VR_SOURCE == "HMD"

    ** Drop Latvia 1959
    replace outlier = 1 if CO == "LVA" & YEAR == 1959 & VR_SOURCE == "HMD"

	tempfile hmd
	save `hmd', replace
	restore
	append using `hmd'

	// NO NIDS some of these rows do not have nids, they are marked as outliers.
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	replace outlier = 1 if VR_SOURCE == "" & mi(NID)


** OECD database
	preserve
	use "FILEPATH", clear
	replace NID = 18447 if inlist(COUNTRY, "ISR", "AGO", "DZA")
	tempfile oecd
	save `oecd', replace
	restore
	append using `oecd'

	** drop subnational and suspicious data
	** not obvious what the source is
	replace outlier = 1 if CO == "IRN" & VR_SOURCE == "OECD"
	replace outlier = 1 if CO == "JAM" & VR_SOURCE == "OECD"
	replace outlier = 1 if CO == "NPL" & VR_SOURCE == "OECD"
	** rural only
	replace outlier = 1 if CO == "MAR" & VR_SOURCE == "OECD"
	** duplicate of VR
	replace outlier = 1 if CO == "SUR" & VR_SOURCE == "OECD"
	replace outlier = 1 if CO == "SYC" & VR_SOURCE == "OECD" & YEAR == 1974
	replace outlier = 1 if CO == "MDG" & VR_SOURCE == "OECD"
	replace outlier = 1 if CO == "KEN" & VR_SOURCE == "OECD"
	** subnational
	replace outlier = 1 if CO == "DZA" & VR_SOURCE == "OECD"
	replace outlier = 1 if CO == "TCD" & VR_SOURCE == "OECD"
	replace outlier = 1 if CO == "TUN" & VR_SOURCE == "OECD"
	** duplicate of a survey in DYB
	replace outlier = 1 if CO == "TGO" & VR_SOURCE == "OECD"
	** duplicates of other sources
	replace outlier = 1 if CO == "CPV" & VR_SOURCE == "OECD"
	replace outlier = 1 if CO == "AGO" & VR_SOURCE == "OECD"
	replace outlier = 1 if CO == "SLV" & VR_SOURCE == "OECD"
	** non national VR
	replace outlier = 1 if CO == "MOZ" & VR_SOURCE == "OECD"

	** make source type corrections for OECD data
	replace SUBDIV = "VR" if CO == "CUB" & VR_SOURCE == "OECD"
	replace SUBDIV = "VR" if CO == "MAC" & VR_SOURCE == "OECD"
	replace SUBDIV = "VR" if CO == "KOR" & VR_SOURCE == "OECD"
	replace SUBDIV = "CENSUS" if CO == "COM" & VR_SOURCE == "OECD"
	replace SUBDIV = "CENSUS" if CO == "SYC" & VR_SOURCE == "OECD"

	replace outlier = 1 if CO == "BDI" & VR_SOURCE == "OECD"
	replace outlier = 1 if CO == "GIN" & VR_SOURCE == "OECD"
	replace outlier = 1 if CO == "GAB" & VR_SOURCE == "OECD"

** IPUMS
	append using "FILEPATH"
	append using "FILEPATH"
    append using "FILEPATH"
	** reclassify non-census source
	replace SUBDIV = "SURVEY" if CO == "ZAF" & YEAR == 2007 & VR_SOURCE == "IPUMS_HHDEATHS"

** ************
** Country-Specific sources
** ************

** ARE: VR
	append using "FILEPATH"
	append using "FILEPATH"

** AUS: VR
	preserve
	use "FILEPATH", clear
	gen outlier = 1
	tempfile aus_vr_2010
	save `aus_vr_2010', replace

	restore
	append using `aus_vr_2010'


** BGD: Sample Registration System
	append using "FILEPATH"
	replace outlier = 1 if YEAR == 2003 & CO == "BGD" & VR_SOURCE == "SRS_LIBRARY"
	append using "FILEPATH"
	append using "FILEPATH"

** BFA: 1985, 1996, 2006 Censuses
	append using "FILEPATH"

** BRA: 2010 Census
	append using "FILEPATH"

** CAN: 2010-2011 VR deaths
	append using "FILEPATH"

** CAN: 2012 VR deaths
	append using "FILEPATH"

** CHN: 1982 Census
	append using "FILEPATH"

** CHN: 2000 Census
	append using "FILEPATH"

** CHN: 2010 Census
	append using "FILEPATH"

** CHN: DSP
	** this is urban/rural aggregated
	append using "FILEPATH"
	append using "FILEPATH"

	** new CHN DSP data from provincial aggregated up to national
	append using "FILEPATH"

** CHN: Intra-census surveys (1%; DC)
	append using "FILEPATH"

** CHN: SSPC (1 per 1000)
	append using "FILEPATH"

** CIV: 1998 Census
	append using "FILEPATH"

** CMR: 1987 Census
	append using "FILEPATH"

** ETH: 2007 Census
	append using "FILEPATH"

** IDN: SUSENAS & SUPAS & 2000 long form census
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"

** IDN prepped at province level: SUPAS, SUSENAS, CENSUS
** ********************************
	tempfile master
	save `master', replace

	import delimited using "FILEPATH", clear
	keep if regexm(ihme_loc_id, "IDN")
	keep ihme_loc_id location_id
	rename ihme_loc_id COUNTRY
	drop if location_id == 11
	tempfile idn_provs
	save `idn_provs', replace

	use "FILEPATH"
	keep prov_num location_id
	drop if mi(location_id)
	merge 1:1 location_id using `idn_provs', nogen
	save `idn_provs', replace

	use "FILEPATH", clear
	drop COUNTRY

	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	drop raw*
	drop if SEX==.
	merge m:1 prov_num using `idn_provs', nogen
	drop if prov_num == 54
	drop prov_num location_id

	tempfile idn_subnat
	save `idn_subnat', replace

	use `master', clear
	append using `idn_subnat'

** ********************************

** IND: Sample Registration System
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
    append using "FILEPATH"

	// Subnational SRS
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
    append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"

	replace YEAR = 2017 if VR_SOURCE == "IND_SRS_2017" & NID == 413455
	replace YEAR = 2018 if VR_SOURCE == "IND_SRS_2018" & NID == 449948
	replace YEAR = 2019 if VR_SOURCE == "IND_SRS_2019" & NID == 498546
	replace YEAR = 2020 if VR_SOURCE == "IND_SRS_2020" & NID == 522751

	drop if inlist(COUNTRY, "IND_4871", "IND_43872", "IND_43902", "IND_43908", "IND_43938") & YEAR < 2014
	// replace Old Andhra Pradesh with Andhra Pradesh
	replace CO = "IND_44849" if CO == "IND_4841" & YEAR < 2014

	** agg AP/Telangana into Old AP for new SRS data
	preserve
		keep if YEAR >= 2014 & inlist(COUNTRY, "IND_4841", "IND_4871")
		foreach var of varlist DATUM* {
			count if mi(`var')
			if `r(N)' == _N drop `var'
		}
		collapse (sum) DATUM*, by( YEAR SEX SUBDIV VR_SOURCE AREA NID)
		gen COUNTRY = "IND_44849"
		tempfile new_ap_old
		save `new_ap_old', replace
	restore
	append using `new_ap_old'

	drop if SEX == 3 & regexm(COUNTRY, "IND")

** LKA: VR from collaborator
	append using "FILEPATH"

** LBN: VR from collaborator
	append using "FILEPATH"

** LBY: VR
	preserve
	use "FILEPATH", replace
	gen outlier = 1
	tempfile lby_vr_2006_2007
	save `lby_vr_2006_2007', replace
	restore

	append using `lby_vr_2006_2007'

** LTU: VR
	append using "FILEPATH"

** MNG: Stat yearbook
	append using "FILEPATH"
	** dropping other sources for MNG in 1999
	replace outlier = 1 if YEAR == 1999 & CO == "MNG" & VR_SOURCE != "MNG_STAT_YB_2001"

** MOZ: 2007 Census
	append using "FILEPATH"

** PAK: SRS
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"


** SAU: 2007 Demographic Bulletin
	append using "FILEPATH"

** TUR: 1989 Demographic Survey, household deaths scaled up to population
	append using "FILEPATH"

** TUR: 2009-2010 VR
	append using "FILEPATH"
		replace outlier = 1 if COUNTRY == "TUR" & inlist(YEAR,2009,2010) & VR_SOURCE == "Stats website"

** TUR: 2010 and 2011 VR from TurkStat Tabulations
	append using "FILEPATH"
	replace outlier = 1 if COUNTRY == "TUR" & inlist(YEAR,2010,2011) & VR_SOURCE == "TurkStat_Tabs_MERNIS_data"

** USA: 2010 VR
	append using "FILEPATH"

** USA CDC 2015 VR
	append using "FILEPATH"

** USA CDC VR 1968-1979
	append using "FILEPATH"
	replace outlier = 1 if regexm(COUNTRY, "USA") & YEAR >= 1959 & YEAR <= 1979 & VR_SOURCE == "CDC"

** WSM: 2006 Census
	append using "FILEPATH"

** ZAF: 2010 VR (Stats South Africa; de facto)
    append using "FILEPATH"
    replace SUBDIV = "VR-SSA" if VR_SOURCE == "stats_south_africa"
    replace outlier = 1 if VR_SOURCE == "stats_south_africa" & YEAR == 2010

** CHN provincial data

    ** Censuses
        append using "FILEPATH"
        append using "FILEPATH"

        append using "FILEPATH"
        append using "FILEPATH"

    ** DSP
        append using "FILEPATH"
		append using "FILEPATH"

	** 1 % survey
		append using "FILEPATH"

	** Family planning survey 1992
		append using "FILEPATH"

** IND urban rural
	** SRS
		// Also includes national 2012 SRS numbers
		append using "FILEPATH"

** CHL deaths from Ministeria de Salud 2010-2011
    quietly append using "FILEPATH"
    replace outlier = 1 if CO == "CHL" & VR_SOURCE == "CHL_MOH" & SUBDIV == "VR" & YEAR == 2010

** ********************************
** Add in household deaths (will be dropped before DDM and added back in at the 45q15 calculation)
** ********************************

** BDI Demographic survey 1965
	append using "FILEPATH"

** BDI Demographic survey 1970-1971
	append using "FILEPATH"

** BDI 2008 census
	append using "FILEPATH"

** BGD 2011 census
	append using "FILEPATH"

** BWA Demographic Survey 2006
	append using "FILEPATH"

** BWA census 1981
	append using "FILEPATH"

** CHN 2010 Survey
	append using "FILEPATH"

** CHN 2012 Survey
	append using "FILEPATH"

** CMR: Census 1976
	append using "FILEPATH"

** Cote d'Ivoire 1978-1979 Demographic Survey
	append using "FILEPATH"

** COG census 1984
	append using "FILEPATH"

** Ecuador ENSANUT 2012
	append using "FILEPATH"

** HND EDENH 1971-1972
	append using "FILEPATH"

** HND survey of living conditions 2004
	append using "FILEPATH"

** IRQ: IMIRA
    append using "FILEPATH"

** KEN: Census 2009
	append using "FILEPATH"
	replace VR_SOURCE = "7427#KEN 2009 Census 5% sample" if VR_SOURCE == ""

** KEN AIDS Indicator Survey 2007
	append using "FILEPATH"

** KHM 1997 socioeconomic survey
	append using "FILEPATH"

** KIR 2010 census
	append using "FILEPATH"

** Malawi Population Change Survey 1970-1972
	append using "FILEPATH"

** Mauritania 1988 Census
	append using "FILEPATH"

** NAM 2011 census
	append using "FILEPATH"

** NGA: GHS 2006
    append using "FILEPATH"

** SLB 2009 census
	append using "FILEPATH"

** Tanzania Census 1967
	append using "FILEPATH"

** TGO Census 2010
	append using "FILEPATH"

** ZAF: October Household Survey 1993, 1995-1998
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"

** ZAF community survey 2007
	append using "FILEPATH"

** ZMB: 2008 HHC
    append using "FILEPATH"

** ZMB LCMS
    append using "FILEPATH"

** ZMB SBS
    append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"

// Drop location ID variable
	cap drop location_id

** VNM NHS
	append using "FILEPATH"

** Papchild surveys: DZA EGY LBN LBY MAR MRT SDN SYR TUN YEM
	append using "FILEPATH"
	replace SUBDIV = "HOUSEHOLD" if SUBDIV == ""
	cap drop location_id

** DHS surveys, ERI 1995-1996 and 2002, NGA 2013, MWI 2010, ZMB 2007, RWA 2005, IND 1998-1999, DOM 2013, UGA 2006, HTI 2005-2005, ZWE 2005-2006, NIC 2001, BGD SP 2001, JOR 1990, DOM_2013, MWI 2010
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"

** ZAF NIDS
	append using "FILEPATH"

** TZA LSMS
	append using "FILEPATH"

	replace SUBDIV = "HOUSEHOLD" if SUBDIV == ""

** Subnationals not previously run at the national level

	** MEX SAGE
	append using "FILEPATH"

	** CHN SAGE
	append using "FILEPATH"
	replace SUBDIV = "HOUSEHOLD" if SUBDIV == ""

	** ZAF CS IPUMS 2007
	append using "FILEPATH"

	** ZAF IPUMS 2001
	append using "FILEPATH"

	replace SUBDIV = "HOUSEHOLD" if SUBDIV == ""

** Subnationals previously run at the national level
	** ZAF Census 2011
	append using "FILEPATH"

	replace SUBDIV = "HOUSEHOLD" if SUBDIV == ""

** SLV_IPUMS_CENSUS
	append using "FILEPATH"

** DOM_ENHOGAR
	append using "FILEPATH"

** GIN_Demosurvey_1954_1955
	append using "FILEPATH"

** THA Population Change survey
	append using "FILEPATH"

** NRU Census 2011
	append using "FILEPATH"

** MWI FFS 1984
	append using "FILEPATH"

** DJI Demographic Survey
	append using "FILEPATH"
	replace SUBDIV = "HOUSEHOLD" if SUBDIV == ""

** IND DLHS4 2012-2014
	append using "FILEPATH"
	replace SUBDIV = "HOUSEHOLD" if SUBDIV == ""
	replace SUBDIV = "DLHS" if regexm(VR_SOURCE,"DLHS")

**All CAUSE VR
	append using "FILEPATH"

	replace SUBDIV = "DSP>2003" if NID == 338606 & VR_SOURCE == "CHN_DSP"

**VUT HH CENSUS 2009
	append using "FILEPATH"

**USSR mortality tables 1939, 1959, 1970, 1970
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"
	append using "FILEPATH"

** ***********************************************************************
** Drop data from before 1950
** ***********************************************************************
	drop if YEAR < 1950


** ***********************************************************************
** Apply proportional splits to split out USSR and Yugoslavian deaths into their respective subnational locations
** ***********************************************************************
	tempfile master
	save `master', replace

	local prop_dir = "FILEPATH"

	import delimited using "FILEPATH", clear
	rename ihme_loc_id COUNTRY
	rename year YEAR
	rename sex_id SEX
	tempfile props_ussr
	save `props_ussr'

	import delimited using "FILEPATH", clear
	rename ihme_loc_id COUNTRY
	rename year YEAR
	rename sex_id SEX
	tempfile props_yug
	save `props_yug'

	use `master' if COUNTRY == "XSU", clear

	drop COUNTRY
	merge 1:m YEAR SEX using `props_ussr', keep(3) nogen

	foreach var of varlist DATUM* {
		local var_stub = subinstr("`var'", "DATUM", "", .)

		if inlist("`var'", "DATUMUNK", "DATUMTOT") {
			replace `var' = `var' * scalartot
		}
		else if inlist("`var'", "DATUM12to23months", "DATUM2to2", "DATUM3to3", "DATUM4to4", "DATUM1to4", "DATUMenntoenn") | inlist("DATUMlnntolnn", "DATUMpnatopna", "DATUMpnbtopnb", "DATUMPostNeonatal", "DATUM2to4")  {
			replace `var' = `var' * scalar1to4
		}
		else if inlist("`var'", "DATUM80to84", "DATUM85plus", "DATUM85to89", "DATUM90to94", "DATUM95to99", "DATUM95plus") {
			replace `var' = `var' * scalar80plus
		}
		else {
			capture replace `var' = `var' * scalar`var_stub'
		}
	}

	** Create an all-ages total

	** Sum up U1
	replace DATUM0to0 = DATUMenntoenn + DATUMlnntolnn + DATUMpnatopna + DATUMpnbtopnb if DATUM0to0 == . & DATUMpnatopna != . & DATUMpnbtopnb != .
	replace DATUM0to0 = DATUMenntoenn + DATUMlnntolnn + DATUMPostNeonatal if DATUM0to0 == . & DATUMPostNeonatal != .

	** Sum up to 1-4
	replace DATUM1to4 = DATUM12to23months + DATUM2to4 if DATUM1to4== .

	replace DATUMTOT = DATUM0to0 + DATUM1to4 + DATUM5to9 + DATUM10to14 + DATUM15to19 + DATUM20to24 + DATUM25to29 + ///
	DATUM30to34 + DATUM35to39 + DATUM40to44 + DATUM45to49 + DATUM50to54 + DATUM55to59 + DATUM60to64 + DATUM65to69 + ///
	DATUM70to74 + DATUM75to79 + DATUM80to84 + DATUM85plus + DATUMUNK if DATUMUNK != .

	drop scalar*

	tempfile ussr_split
	save `ussr_split'


	use `master' if COUNTRY == "XYG", clear

	drop if VR_SOURCE == "DYB_CD" & inrange(YEAR, 1960,1990)
	drop COUNTRY
	merge 1:m YEAR SEX using `props_yug', keep(3) nogen


	foreach var of varlist DATUM* {
		local var_stub = subinstr("`var'", "DATUM", "", .)
		if inlist("`var'", "DATUMUNK") {
			replace `var' = `var' * scalartot
		}
		else if inlist("`var'", "DATUM1to1", "DATUM2to2", "DATUM3to3", "DATUM4to4", "DATUM1to4", "DATUMenntoenn") | inlist("DATUMlnntolnn", "DATUMpnatopna", "DATUMpnbtopnb", "DATUMPostNeonatal", "DATUM2to4")  {
			replace `var' = `var' * scalar1to4
		}
		else if inlist("`var'", "DATUM80to84", "DATUM85plus", "DATUM85to89", "DATUM90to94", "DATUM95to99", "DATUM95plus") {
			replace `var' = `var' * scalar80plus
		}
		else {
			capture replace `var' = `var' * scalar`var_stub'
		}
	}

	** Sum up U1
	replace DATUM0to0 = DATUMenntoenn + DATUMlnntolnn + DATUMpnatopna + DATUMpnbtopnb if DATUM0to0 == . & DATUMpnatopna != . & DATUMpnbtopnb != .
	replace DATUM0to0 = DATUMenntoenn + DATUMlnntolnn + DATUMPostNeonatal if DATUM0to0 == . & DATUMPostNeonatal != .

	** Sum up to 1-4
	replace DATUM1to4 = DATUM12to23months + DATUM2to4 if DATUM1to4== .

	replace DATUMTOT = DATUM0to0 + DATUM1to4 + DATUM5to9 + DATUM10to14 + DATUM15to19 + DATUM20to24 + DATUM25to29 + ///
	DATUM30to34 + DATUM35to39 + DATUM40to44 + DATUM45to49 + DATUM50to54 + DATUM55to59 + DATUM60to64 + DATUM65to69 + ///
	DATUM70to74 + DATUM75to79 + DATUM80to84 + DATUM85plus + DATUMUNK


	drop scalar*

	tempfile yug_split
	save `yug_split'


	use `master' if !inlist(COUNTRY, "XYG", "XSU"), clear

	// keep the country years of ussr splits that only exist in using
	preserve
	merge m:1 COUNTRY YEAR SEX using `ussr_split', keep(2) nogen
	tempfile ussr_split
	save `ussr_split'
	restore

	// keep the country years of yugoslavia splits that only exist in using
	preserve
	merge m:1 COUNTRY YEAR SEX using `yug_split', keep(2) nogen
	tempfile yug_split
	save `yug_split'
	restore

	// append to master
	append using `ussr_split'
	append using `yug_split'


** ***********************************************************************
** Drop duplicates and other problematic data
** ***********************************************************************
	noisily: display in green "DROP OUTLIERS AND MAKE CORRECTIONS TO THE DATABASE"

	replace outlier = 0 if outlier != 1

	** Drop the WHO Mortality Database data
	drop if VR_SOURCE == "WHO_Mortality_Database"

** Drop if AREA is urban or rural
	keep if AREA == 0 | AREA == .

** Drop unknown sex
	drop if SEX == 9

** Drop if everything is missing
	lookfor DATUM
	return list
	local misscount = 0
		foreach var of varlist `r(varlist)' {
			local misscount = `misscount'+1
		}
	egen misscount = rowmiss(DATUM*)
	drop if misscount == `misscount'
	drop misscount

** Drop duplicates
		duplicates tag COUNTRY YEAR SEX if inlist(COUNTRY, "DEU", "TWN", "ESP") & (VR_SOURCE == "HMD" | regexm(VR_SOURCE, "WHO")==1) & outlier != 1, generate(deu_twn_esp_duplicates)
		replace deu_twn_esp_duplicates = 0 if mi(deu_twn_esp_duplicates)
		replace outlier = 1 if COUNTRY == "ESP" & YEAR <= 1973 & deu_twn_esp_duplicates != 0 & VR_SOURCE != "HMD"
		drop deu_twn_esp_duplicates

		** Prioritize Norway HMD when available, pre-1990
		duplicates tag COUNTRY YEAR SEX if COUNTRY=="NOR", generate(nor_dups)
		replace outlier = 1 if COUNTRY=="NOR" & nor_dups > 0 & YEAR <= 1990 & VR_SOURCE != "HMD"

		drop nor_dups

		replace outlier = 1 if CO == "BRA" & strpos(VR_SOURCE, "WHO")!=0 & YEAR<=1980
		replace outlier = 1 if CO == "ARG" & inlist(YEAR, 1966, 1967) & VR_SOURCE=="WHO"
		replace outlier = 1 if CO == "MYS" & VR_SOURCE=="WHO" & (YEAR > 1999 & YEAR < 2009)
		replace outlier = 1 if CO == "KOR" & YEAR >= 1985 & YEAR <= 1995 & VR_SOURCE=="WHO"
		replace outlier = 1 if CO == "PAK" & (YEAR == 1993 | YEAR == 1994) & strpos(VR_SOURCE,"WHO") != 0
		replace outlier = 1 if CO == "BHS" & (YEAR == 1969 | YEAR == 1971) & VR_SOURCE == "WHO"
		replace outlier = 1 if CO == "EGY" & (YEAR >= 1954 & YEAR <= 1964) & VR_SOURCE == "WHO"

	** Take out duplicates from the DYB for the same country year
	gsort +COUNTRY +YEAR +SEX -DATUMTOT

	** Initial drop to keep DYB_CD over everything else
	duplicates tag COUNTRY YEAR SEX SUBDIV if strpos(VR_SOURCE, "DYB") != 0, g(dup)
	drop if COUNTRY == "KOR" & VR_SOURCE == "DYB_download" & dup == 1 & YEAR >= 1977 & YEAR <= 1989
	drop if COUNTRY == "KOR" & VR_SOURCE == "DYB_ONLINE" & YEAR == 1993 & dup == 1
	drop dup

	** Drop duplicates within DYB_ONLINE
	gsort +COUNTRY +YEAR +SEX -DATUMTOT -DATUM0to0 -DATUM0to4
	duplicates drop COUNTRY YEAR SEX SUBDIV if strpos(VR_SOURCE, "DYB_ONLINE") != 0, force

	** Drop duplicates between online and download
	duplicates tag COUNTRY YEAR SEX SUBDIV if strpos(VR_SOURCE, "DYB") != 0, g(dup)
	drop if VR_SOURCE == "DYB_ONLINE" & dup == 1
	drop dup

	** Drop WHO internal VR if we have other sources
	duplicates tag COUNTRY YEAR SEX if SUBDIV == "VR" & outlier != 1, g(dup)
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & VR_SOURCE == "WHO_internal"
    drop dup

    ** Drop DYB VR if we have other sources
    duplicates tag COUNTRY YEAR SEX  if SUBDIV == "VR" & outlier != 1, g(dup)
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & strpos(VR_SOURCE,"DYB") != 0
    drop dup

	** Drop HMD VR if we have other sources
	duplicates tag COUNTRY YEAR SEX  if SUBDIV == "VR" & outlier != 1, g(dup)
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & strpos(VR_SOURCE,"HMD") != 0
    drop dup

	duplicates tag COUNTRY YEAR SEX  if SUBDIV == "VR" & outlier != 1, g(dup)
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & VR_SOURCE == "WHO"
    drop dup

    ** Drop CHN national data from provincial level estimates
    duplicates tag CO YEAR SEX if SUBDIV == "CENSUS" & CO == "CHN" & outlier != 1, g(dup)
	replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & CO == "CHN" & regexm(VR_SOURCE,"CHN_PROV_CENSUS")
	drop dup

	** Drop DYB Census if we have other sources
	duplicates tag COUNTRY YEAR SEX if SUBDIV == "CENSUS" & outlier != 1, g(dup)
	replace dup = 0 if dup == .
	replace outlier = 1 if dup != 0  & regexm(VR_SOURCE, "DYB")
	drop dup

    ** Drop if we have other sources
    duplicates tag CO YEAR SEX if SUBDIV == "VR" & outlier != 1, g(dup)
	replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & VR_SOURCE == ""
    replace outlier = 1 if dup !=0 & VR_SOURCE == "VR" & CO == "AUS"
    drop dup

    ** drop USA NCHS data if we have other sources
    duplicates tag CO YEAR SEX if SUBDIV == "VR" & outlier != 1, g(dup)
	replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & CO == "USA" & VR_SOURCE == "NCHS"
    drop dup


	** drop MNG, LBY, GBR duplicates
	duplicates tag COUNTRY YEAR SEX SUBDIV if outlier != 1, gen(dup)
	replace outlier = 1 if COUNTRY == "LBY" & VR_SOURCE == "report" & SUBDIV == "VR" & inlist(YEAR, 2006, 2007, 2008) & dup >= 1
	replace outlier = 1 if COUNTRY == "MNG" & VR_SOURCE == "" & SUBDIV == "VR" & inlist(YEAR, 2004, 2005) & dup >=1
	replace outlier = 1 if regexm(COUNTRY,"GBR_") & CO != "GBR_4749" & SUBDIV == "VR" & !regexm(VR_SOURCE, "WHO") & YEAR < 2013
	drop dup

	** drop LTU duplicates
	duplicates tag COUNTRY YEAR SEX SUBDIV, gen(dup)
	replace outlier = 1 if dup >= 1 & COUNTRY == "LTU" & YEAR == 2010 & SUBDIV == "VR" & VR_SOURCE == "135808#LTU COD REPORT 2010"
	replace outlier = 1 if dup >= 1 & COUNTRY == "LTU" & YEAR == 2011 & SUBDIV == "VR" & VR_SOURCE == "135810#LTU COD REPORT 2011"
	replace outlier = 1 if dup >= 1 & COUNTRY == "LTU" & YEAR == 2012 & SUBDIV == "VR" & VR_SOURCE == "135811#LTU COD REPORT 2012"

** replace outlier = 1 VR years before Cyprus split
	replace outlier = 1 if YEAR < 1974 & CO == "CYP"
	drop dup

** PNG
	replace DATUM75plus = DATUM75to79 if CO =="PNG" & YEAR == 1977
	foreach var of varlist DATUM75to79 DATUM80to84 DATUM80plus DATUM85plus {
		replace `var' = . if CO =="PNG" & YEAR == 1977
	}

** IDN separate SUPAS and SUSENAS and 2000 Census-Survey
	replace SUBDIV = "SUSENAS" if strpos(VR_SOURCE,"SUSENAS") !=0
	replace SUBDIV = "SUPAS" if strpos(VR_SOURCE,"SUPAS") !=0
	replace VR_SOURCE = "2000_CENS_SURVEY" if VR_SOURCE == "SURVEY" & CO == "IDN" & YEAR == 1999
	replace SUBDIV = "SURVEY" if VR_SOURCE == "2000_CENS_SURVEY" & CO == "IDN" & YEAR == 1999

** Drop Canada source and use WHO VR
	duplicates tag COUNTRY YEAR SEX SUBDIV, gen(dup)
	replace outlier = 1 if dup >= 1 & COUNTRY == "CAN" & (YEAR == 2010 | YEAR == 2011) & SUBDIV == "VR" & VR_SOURCE == "STATISTICS_CANADA_VR_121924"

** Drop Chile source and use WHO VR
	replace outlier = 1 if dup >= 1 & COUNTRY == "CHL" & YEAR == 2011 & SUBDIV == "VR"	& VR_SOURCE == "CHL_MOH"

** Drop China DC survey: Duplicate of China 1 % survey
	replace outlier = 1 if dup >= 1 & COUNTRY == "CHN" & YEAR == 2005 & SUBDIV == "DC" & VR_SOURCE == "DC"
	replace outlier = 1 if dup >= 1 & COUNTRY == "CHN" & YEAR >= 1996 & SUBDIV == "DSP" & VR_SOURCE == "CHN_DSP"
	drop dup
** Fix age group naming for youngest age groups
	replace DATUM0to0 = DATUM0to1 if DATUM0to0 == . & DATUM0to1 != . & (DATUM1to4 != . | DATUM0to4 != .)
	replace DATUM0to1 = . if DATUM0to0 != . & (DATUM1to4 != . | DATUM0to4 != .)

** Relabel source_type for surveys that overlap time-wise but have same source type
	replace SUBDIV = "HHC" if VR_SOURCE == "ZMB_HHC" & COUNTRY == "ZMB"

** drop VR in 2009 in LKA
	replace outlier = 1 if COUNTRY=="LKA" & YEAR==2009 & VR_SOURCE=="S_Dharmaratne"

** Fix categorization of Indonesia DYB
	replace SUBDIV = "Census" if CO == "IDN" & YEAR == 2010 & SUBDIV == "VR"

** Additional drops
	replace outlier = 1 if COUNTRY == "CHL" & inlist(VR_SOURCE, "WHO", "HMD") & YEAR == 1993

** drop duplicate NZL 2019 source
    replace outlier = 1 if COUNTRY == "NZL_44850" & YEAR == 2019 & NID == 338403
	replace outlier = 1 if COUNTRY == "NZL_44851" & YEAR == 2019 & NID == 338403


** ***********************************************************************
** Format the database
** ***********************************************************************
	noisily: display in green "FORMAT THE DATABASE"
	drop RECTYPE RELIABIL AREA MONTH DAY
	cap drop _fre
	rename COUNTRY iso3
	gen sex = "both" if SEX == 0
	replace sex = "male" if SEX == 1
	replace sex = "female" if SEX == 2
	drop SEX

	// RENAME occurs here
	rename YEAR year
	rename VR_SOURCE deaths_source
	rename SUBDIV source_type
	rename FOOTNOTE deaths_footnote

	// Add locations
	replace iso3 = "" if ihme_loc_id != ""
	merge m:1 iso3 using `countrycodes', update
	levelsof iso3 if _merge == 1
	levelsof iso3 if _merge == 2
	levelsof iso3 if ihme_loc_id == ""
	keep if ihme_loc_id != "" & _merge != 2
	drop _merge iso3
	merge m:1 ihme_loc_id using `countrymaster', update
	keep if ihme_loc_id != "" & _merge != 2
	drop _merge iso3

	// Recode all data prepped as China as China Mainland
	replace ihme_loc_id = "CHN_44533" if ihme_loc_id == "CHN"

	order ihme_loc_id country sex year deaths_source source_type deaths_footnote *

	replace deaths_source = "DYB" if regexm(deaths_source, "DYB_")

    ** if we have data for age 0 and ages 1-4, then drop data for ages 0-4 pooled
    replace DATUM0to4 = . if DATUM0to0 != . & DATUM1to4 != .

    duplicates tag ihme_loc_id year sex source_type, gen(dup2)
	replace outlier = 1 if regexm(ihme_loc_id , "GBR_") & !regexm(deaths_source, "WHO") & dup2 != 0
	drop dup2

	drop if ihme_loc_id == "CAN" & year == 2013 & deaths_source == "StatCan"

	//USA 2010 mark as not outliers
	replace outlier = 0 if ihme_loc_id == "USA" & year == 2010 & deaths_source == "WHO"

	drop if ihme_loc_id == "ZAF" & year == 2011 & deaths_source == "12146#stats_south_africa 2011 census"

	** Egypt 1958 DYB
	replace outlier = 1 if ihme_loc_id == "EGY" & year == 1958 & deaths_source == "DYB"

	** Russia 1958 IDEM data
	replace outlier = 1 if ihme_loc_id == "RUS" & year == 1958 & (NID == 336438 | nid == 336438)

	** ARG 2015; prefer the COD source
	replace outlier = 1 if ihme_loc_id == "ARG" & year == 2015 & deaths_source == "Argentina_VR_all_cause"

	** BRA
	replace outlier = 1 if regexm(ihme_loc_id, "BRA") & year == 2016
	replace outlier = 0 if regexm(ihme_loc_id, "BRA") & year == 2016 & deaths_source == "Brazil_SIM_ICD10_allcause"


** CHN and CHN subnationals
	replace outlier = 1 if regexm(ihme_loc_id, "CHN") & deaths_source == "CHN_DSP" & year >= 2004 & source_type == "VR"

** HONG KONG
	replace outlier = 1 if ihme_loc_id == "CHN_354" & deaths_source != "WHO" & year >= 1979 & year <= 2000
	replace outlier = 0 if ihme_loc_id == "CHN_354" & deaths_source == "WHO" & year >= 1979 & year <= 2000

	replace outlier = 1 if ihme_loc_id == "CHN_354" & deaths_source != "WHO" & year > 2000 & year <= 2017
	replace outlier = 0 if ihme_loc_id == "CHN_354" & deaths_source == "WHO" & year > 2000 & year <= 2017

** MACAO
	replace outlier = 1 if ihme_loc_id == "CHN_361" & year == 1994
	replace outlier = 0 if ihme_loc_id == "CHN_361" & year == 1994 & deaths_source == "DYB"
	replace outlier = 1 if ihme_loc_id == "CHN_361" & year >= 2009 & year <= 2012 & deaths_source == "DYB"

	** Iran 2011-2015
	replace source_type = "Civil Registration" if deaths_source == "IRN_NOCR_allcause_VR"

	** ESP 1974
	replace outlier = 1 if ihme_loc_id == "ESP" & year == 1974
	replace outlier = 0 if ihme_loc_id == "ESP" & year == 1974 & deaths_source == "HMD"

	* BEL 1986 & 1987
	replace outlier = 1 if ihme_loc_id == "BEL" & inrange(year, 1986, 1987)
	replace outlier = 0 if ihme_loc_id == "BEL" & inrange(year, 1986, 1987) & deaths_source == "HMD"

	** DEU 1980 - 1989
	replace outlier = 1 if ihme_loc_id == "DEU" & inrange(year, 1980, 1989)
	replace outlier = 0 if ihme_loc_id == "DEU" & inrange(year, 1980, 1989) & deaths_source == "HMD"

    ** JOR
	replace outlier = 1 if ihme_loc_id == "JOR" & year <= 1967

    ** CHN DSP
	replace outlier = 1 if regexm(ihme_loc_id, "CHN") & year == 2016 & deaths_source == "CHN_DSP"

    ** BRA
	replace outlier = 1 if ihme_loc_id == "BRA_4764" & NID == 153001 & year == 1979

	// outlier Grenada 1984
	replace outlier = 1 if ihme_loc_id == "GRD" & year == 1984 & deaths_source == "WHO"
	// outlier WHO BLZ 1981
	replace outlier = 1 if ihme_loc_id == "BLZ" & year == 1981 & deaths_source == "WHO"
	replace outlier = 0 if ihme_loc_id == "BLZ" & year == 1981 & deaths_source == "DYB"

	// make sure we're using CostaRica_VR_all_cause
	replace outlier = 1 if ihme_loc_id == "CRI" & inrange(year, 2015, 2016)
	replace outlier = 0 if ihme_loc_id == "CRI" & inrange(year, 2015, 2016) & deaths_source == "CostaRica_VR_all_cause"

	// outlier Maldives allcause VR 2015 and 2016
	replace outlier = 1 if ihme_loc_id == "MDV" & year == 2015 & NID == 325219
	replace outlier = 1 if ihme_loc_id == "MDV" & year == 2016 & NID == 257555

	// outlier Latvia allcause VR 2016
	replace outlier = 1 if ihme_loc_id == "LVA" & year == 2016 & NID == 324971

	// outlier Guyana_VR_all_cause 2014-2016
	replace outlier = 1 if ihme_loc_id == "GUY" & inrange(year, 2014, 2016) & NID == 324983

	// outlier SVK,MON,LBN,SMR partial sources
	replace outlier = 1 if deaths_source == "Slovakia_VR_all_cause" & missing(DATUMTOT)
	replace outlier = 1 if ihme_loc_id == "MCO" & deaths_source == "ICD9_BTL" & missing(DATUMTOT)
	replace outlier = 1 if deaths_source == "Lebanon_VR_all_cause" & missing(DATUMTOT)
	replace outlier = 1 if ihme_loc_id == "SMR" & deaths_source == "ICD9_BTL" & missing(DATUMTOT)

	// outlier Cuba Health Statistics Yearbook for 2013,2014,2015
	replace outlier = 1 if NID == 339991 & year == 2015
	replace outlier = 1 if NID == 339989 & year == 2014
	replace outlier = 1 if NID == 339988 & year == 2013

	// outlier Greenland Deaths and Mean Population in Municipalities, between 1988 and 2014
	replace outlier = 1 if NID == 173789 & year >= 1988 & year <= 2014

    //outlier BWA_VITAL_STATISTICS_REPORT & keep DYB
	replace outlier = 1 if ihme_loc_id == "BWA" & year == 2012 & deaths_source == "BWA_VITAL_STATISTICS_REPORT"
	replace outlier = 0 if ihme_loc_id == "BWA" & year == 2012 & deaths_source == "DYB"

	// outlier BHR VR all cause
	replace outlier = 1 if ihme_loc_id == "BHR" & (inrange(year, 2001, 2016) & year != 2015) & deaths_source == "BHR_VR_allcause"

	// Canada CANSIM
	replace outlier= 1 if ihme_loc_id=="CAN" & inrange(year, 2012, 2016) & NID == 279537

	replace outlier = 1 if deaths_source == "NCHS" & year == 2010

	** drop male or female if the other is missing
	duplicates tag ihme_loc_id country year source_type deaths_source outlier, gen(count_sex)
	drop if count_sex == 0 & sex != "both"
	** drop "both" if there are males/females
	drop if count_sex > 0 & sex == "both"

	tempfile all
	save `all'

	** outlier sources that only have "both" sex and we have another sources with males/females
	keep if outlier == 0
	gen x = 1 if sex =="male"|sex=="female"
	replace x = 9 if sex == "both"
	collapse(sum) x, by(ihme_loc_id country year source_type deaths_source outlier)
	duplicates tag ihme_loc_id country year source_type outlier, gen(dup)
	replace outlier = 1 if dup > 0 & x == 9
	drop x dup
	rename outlier outlier_new

	tempfile dropdupboth
	save `dropdupboth'

	use `all',clear
	merge m:1 ihme_loc_id country year source_type deaths_source using `dropdupboth', nogen
	replace outlier = 1 if outlier_new == 1
	replace outlier = 0 if mi(outlier)
	drop outlier_new


	save `master', replace

	*** Consolidate DATUM12to23months and DATUM1to1
	replace DATUM12to23months = DATUM1to1 if DATUM12to23months == . & DATUM1to1 != .
	drop DATUM1to1


** ***********************************************************************
** Drop All ages
** ***********************************************************************
/*
	The DATUMTOT column is a sum of all the other ages.  It should be dropped
	to avoid it being split later

*/
	drop DATUMTOT

	** outlier sources that only have DATUMTOT
	egen sumdatum  = rowtotal(DATUM*)
	replace outlier = 1 if sumdatum == 0
	drop sumdatum

	save `master', replace


** ***********************************************************************
** Split sources to be DDM'd seperately
** ***********************************************************************
	noisily: display in green "Split sources to be DDM'd seperately"

** We want DSP to be analyzed separately before and after the 3rd National Survey

	replace source_type = "DSP 96-01" if ihme_loc_id == "CHN_44533" & year >= 1996 & year <= 2000 & source_type == "DSP"
	replace source_type = "DSP>2003" if ihme_loc_id == "CHN_44533" & year >= 2004 & year <= 2010 & source_type == "DSP"

** We want SRS to be analyzed in five parts
	replace source_type = "SRS 1970-1977" if ihme_loc_id == "IND" & year >= 1970 & year <= 1976 & source_type == "SRS"
	replace source_type = "SRS 1977-1983" if ihme_loc_id == "IND" & year >= 1977 & year <= 1982 & source_type == "SRS"
	replace source_type = "SRS 1983-1993" if ihme_loc_id == "IND" & year >= 1983 & year <= 1992 & source_type == "SRS"
	replace source_type = "SRS 1993-2004" if regexm(ihme_loc_id, "IND") & year >= 1993 & year <= 2003 & source_type == "SRS"
	replace source_type = "SRS 2004-2014" if regexm(ihme_loc_id, "IND") & year >= 2004 & year <= 2013 & source_type == "SRS"
	replace source_type = "SRS 2014-2016" if regexm(ihme_loc_id, "IND") & inrange(year, 2014, 2016) & source_type == "SRS"

	//India CRS
	drop if inlist(ihme_loc_id, "IND_4871", "IND_43872", "IND_43902", "IND_43908", "IND_43938") & year < 2014 & deaths_source != "IND_CRS_allcause_VR"
	replace ihme_loc_id = "IND_44849" if deaths_source != "IND_CRS_allcause_VR" & ihme_loc_id == "IND_4841" & year < 2014
	replace ihme_loc_id = "IND_44849" if deaths_source == "IND_CRS_allcause_VR" & ihme_loc_id == "IND_4841" & year < 2013
	replace country = "Old Andhra Pradesh" if country != "Old Andhra Pradesh" & ihme_loc_id == "IND_44849"

** We want to outlier India CRS Nagaland & union territories 2014 and 2015
	replace outlier = 1 if ihme_loc_id == "IND_4864" & inrange(year, 2014, 2015) & deaths_source == "IND_CRS_allcause_VR"
	replace outlier = 1 if inlist(ihme_loc_id, "IND_44538","IND_44539","IND_44540") & inrange(year, 2014, 2015) & deaths_source == "IND_CRS_allcause_VR"

** We want Korea VR analyzed in two parts since there were two different collection methods
	replace source_type = "VR pre-1978" if ihme_loc_id == "KOR" & year <= 1977
	replace source_type = "VR 1978-2000" if ihme_loc_id == "KOR" & year > 1977 & year < 2000
	replace source_type = "VR post-2000" if ihme_loc_id == "KOR" & year >= 2000

** We want China provincial DSP analyzed separately before and after 3rd National survey in 2004
    replace source_type = "DSP<1996" if source_type == "DSP" & deaths_source == "CHN_DSP" & year < 1996
	replace source_type = "DSP 96-04" if source_type == "DSP" & deaths_source == "CHN_DSP" & year >= 1996 & year < 2004
    replace source_type = "DSP>2003" if source_type == "DSP" & deaths_source == "CHN_DSP" & year >= 2004

** We want ZAF data analyzed separately before and after 2002
	replace source_type = "VR pre-2003" if regexm(ihme_loc_id,"ZAF") & year <= 2002 & source_type == "VR"
	replace source_type = "VR post-2003" if regexm(ihme_loc_id,"ZAF") & year > 2002 & source_type == "VR"
	replace source_type = "VR" if deaths_source == "stats_south_africa" & ihme_loc_id == "ZAF" // Adjusted VR source?

** MEX VR
	replace source_type = "VR pre-2011" if regexm(ihme_loc_id,"MEX") & year <= 2011 & source_type == "VR"
	replace source_type = "VR post-2011" if regexm(ihme_loc_id,"MEX") & year > 2011 & source_type == "VR"

** make manual combination of DYB / COD deaths for DOM 2003-2010
	replace deaths_source = "WHO_causesofdeath+DYB" if ihme_loc_id == "DOM" & inrange(year, 2003, 2010) & regexm(deaths_source, "DYB|WHO_causesofdeath")


** Bulk recodes of source_type to standardize source_type_id prior to merges
	replace source_type = lower(source_type)
	replace source_type = "other" if source_type == "survey"

	tempfile prepped_source
	save `prepped_source'

	merge m:1 source_type using `source_type_ids', keep(1 3)
	count if source_type_id == .
	if `r(N)' > 0 {
		di in red "The following source types do not exist in the source type map:"
		levelsof source_type if source_type_id == . , c
		BREAK
	}

	save `prepped_source', replace

** ***********************************************************************
** Save
** ***********************************************************************

** keeping appropriate variables

	destring NID, replace
	destring nid, replace
	replace nid = NID if nid == . & NID != .
	rename nid deaths_nid
	destring ParentNID, replace
	replace ParentNID = . if ParentNID == 99999
	rename ParentNID deaths_underlying_nid

	describe *nid* *NID*


	tostring deaths_nid, replace
	replace deaths_nid = "" if deaths_nid == "." | deaths_nid == " "

	tostring deaths_underlying_nid, replace
	replace deaths_underlying_nid = "" if deaths_underlying_nid == "." | deaths_underlying_nid == " "

	keep ihme_loc_id country sex year deaths_source source_type_id deaths_footnote DATUM* deaths_nid deaths_underlying_nid outlier
	order ihme_loc_id country sex year deaths_source source_type_id deaths_footnote deaths_nid deaths_underlying_nid outlier DATUM*
	replace outlier = 1 if year < 1930
	replace year = floor(year)
	drop if deaths_source == "IPUMS_HHDEATHS" & ihme_loc_id == "ZAF" & year == 2006

	** Eritrea 1995-1996 DHS is accidentally coded as 1994-1995
	replace year = 1996 if year == 1995 & deaths_source == "19546#ERI_DHS_1995-1996"
	replace year = 1995 if year == 1994 & deaths_source == "19546#ERI_DHS_1995-1996"
	replace year = 2002 if year == 2001 & deaths_source == "19539#ERI_DHS_2002"

	** Bangladesh Household: need to drop it
	drop if year == 1999 & deaths_source == "18920#BGD_SP_DHS_2001"

	//drop duplicates
	drop if ihme_loc_id == "CHN_354" & year == 2011 & deaths_nid == "140966" & deaths_footnote != ""
	drop if ihme_loc_id == "CHN_44533" & year == 2010 & deaths_source == "DYB" & country == "China"
	drop if ihme_loc_id == "MAR" & year == 1972 & deaths_source == "OECD" & deaths_footnote == "edit1 E. CERED 1ER PASSAGE"

	// drop duplicated NZL subnational 2018
	replace outlier = 1 if deaths_nid == "456540" & year == 2018

	// drop duplicate NLD 2023 source with wrong nid
	drop if deaths_nid == "544863" & year == 2023 & ihme_loc_id == "NLD"

	// drop duplicate non-preferred NZL 2023 source
	replace outlier = 1 if deaths_nid == "546733" & year == 2023

	// drop BIH nid typo
	drop if deaths_nid == "538735" & ihme_loc_id == "BIH"

	// drop duplicate DOM 2022
	replace outlier = 1 if deaths_nid == "540255" & year == 2022


** final check to make sure no duplicates
	assert !mi(outlier)
	preserve
		keep if outlier == 0
		count if mi(deaths_nid)
		isid ihme_loc_id sex year source_type_id deaths_source outlier
		** add nids from sourcing
		rename deaths_nid deaths_nid_orig
		rename deaths_underlying_nid deaths_underlying_nid_orig

		// deaths_nid and deaths_underlying_nid are the nids from deaths_nids_sourcing file
		merge 1:1 ihme_loc_id sex year source_type_id deaths_source using `deaths_nids_sourcing', keep(1 3)
		count if _m == 3
		drop _merge

		// deaths_nid are from the deaths_nids_sourcing file
		replace deaths_nid  = deaths_nid_orig if deaths_nid == ""
		count if deaths_nid == ""

		// deaths_underlying_nid are from the deaths_nids_sourcing
		replace deaths_underlying_nid  = deaths_underlying_nid_orig if deaths_underlying_nid == ""

		drop deaths_nid_orig deaths_underlying_nid_orig

		// replace bad nids that were contained in the deaths_nids_sourcing file
		replace deaths_nid = "121922" if deaths_nid == "237478"
		replace deaths_nid = "237659" if deaths_nid == "237681"

		tempfile not_outliers
		save `not_outliers'
	restore
	keep if outlier != 0
	append using `not_outliers'

	//standardize underlying nid
	replace deaths_underlying_nid = "" if deaths_underlying_nid == "."

	//all-cause vr outlier
	replace outlier = 1 if ihme_loc_id == "PRY" & year == 1992 & deaths_source == "DYB"
	replace outlier = 1 if ihme_loc_id == "VIR" & year == 1990
	replace outlier = 1 if ihme_loc_id == "TUN" & inlist(year, 2009, 2013)
	replace outlier = 1 if ihme_loc_id == "ASM" & year == 1957
	replace outlier = 1 if ihme_loc_id == "JOR" & year <= 1967

	// drop duplicated source
	replace outlier = 1 if ihme_loc_id == "MDA" & year == 2016 &  deaths_source =="Moldova Statistical Databank"
	replace outlier =1 if ihme_loc_id == "LKA" & inrange(year, 1995,2006) & deaths_source == "Sri Lanka Vital Statistics"
	replace outlier = 1 if ihme_loc_id == "TUR" & year == 2014 & deaths_source == "Turkey_allcause_VR"

	//keep GRL dyb and drop another source
	replace outlier = 0 if ihme_loc_id == "GRL" & deaths_source == "DYB" & inrange(year, 1979, 1987)
	replace outlier = 1 if ihme_loc_id == "GRL" & deaths_source != "DYB" & inrange(year, 1979, 1987)


	//new VR creating duplicates. Drop older sources when applicable
	replace outlier=1 if ihme_loc_id=="BLR" & deaths_nid=="321823"
	replace outlier=1 if ihme_loc_id=="GEO" & deaths_nid=="325172"
	replace outlier=1 if ihme_loc_id=="GRC" & deaths_nid=="324965"
	replace outlier=1 if ihme_loc_id=="MKD" & deaths_nid=="325093"
	replace outlier=1 if ihme_loc_id=="MNG" & inlist(deaths_nid, "24178", "24184", "93857")
	replace outlier=1 if ihme_loc_id=="SYC" & deaths_nid=="325202"

	// Pick source with better age groups
	replace outlier=1 if ihme_loc_id=="BIH" & year==2010 & deaths_nid == "322108"
	replace outlier=1 if ihme_loc_id=="BLR" & year==2017 & deaths_nid == "424222"
	replace outlier=1 if ihme_loc_id=="BWA" & year==2017 & deaths_nid == "396197"
	replace outlier=1 if ihme_loc_id=="CAN" & year==2017 & deaths_nid == "236521"
	replace outlier=1 if ihme_loc_id=="COK" & year==2009 & deaths_nid == "375220"
	replace outlier=1 if ihme_loc_id=="CUB" & year==2017 & deaths_nid == "374869"
	replace outlier=1 if ihme_loc_id=="GEO" & year==2017 & deaths_nid == "423302"
	replace outlier=1 if ihme_loc_id=="ISL" & year==2017 & deaths_nid == "118335"
	replace outlier=1 if ihme_loc_id=="LKA" & year==2014 & deaths_nid == "401029"
	replace outlier=1 if ihme_loc_id=="LUX" & year==2017 & deaths_nid == "400963"
	replace outlier=1 if ihme_loc_id=="MKD" & year==2018 & deaths_nid == "429534"
	replace outlier=1 if ihme_loc_id=="MNG" & year==2018 & deaths_nid == "401084"
	replace outlier=1 if ihme_loc_id=="NIU" & year==2009 & deaths_nid == "277706"
	replace outlier=1 if ihme_loc_id=="PAN" & year==2017 & deaths_nid == "398459"
	replace outlier=1 if ihme_loc_id=="PRY" & year==2018 & deaths_nid == "438548"
	replace outlier=1 if ihme_loc_id=="SVK" & year==2017 & deaths_nid == "400652"
	replace outlier=1 if ihme_loc_id=="SYC" & year==2018 & deaths_nid == "400154"
	replace outlier=1 if ihme_loc_id=="TUR" & year==2017 & deaths_nid == "432156"
    replace outlier=1 if ihme_loc_id=="LBN" & year==2012 & deaths_nid == "222733"
	replace outlier=1 if ihme_loc_id=="MUS" & year==2018 & deaths_nid == "465415"
	// incomplete source
	replace outlier=1 if ihme_loc_id=="ASM" & year==2019 & deaths_nid == "472997"
	replace outlier=1 if ihme_loc_id=="ASM" & year==2018 & deaths_nid == "472934"
	// keep 2018 nvss since more granular age groups + similar values as VR
	replace outlier=1 if ihme_loc_id=="GUM" & year==2018 & deaths_nid == "426273"
	replace outlier=1 if ihme_loc_id=="PRI" & year==2018 & deaths_nid == "426273"
	// drop source
	replace outlier=1 if ihme_loc_id=="PRK" & year==2014 & deaths_nid == "357110"

	// PHL VR too high
	replace outlier=1 if ihme_loc_id=="PHL_53533" & year==1974 & deaths_nid == "372600"

	// New DYB update 2017-20 has duplicate VR that is identical
	replace outlier=1 if ihme_loc_id=="CHN_361" & year==2017 & deaths_source=="DYB"
	replace outlier=1 if ihme_loc_id=="IDN" & year==2010 & deaths_source=="DYB"

	// duplicate source USA 2019 - NVSS and DYB
	replace outlier=1 if ihme_loc_id=="USA" & year==2019 & deaths_source=="DYB"

	// duplicate in GIN 2014
	replace outlier=1 if ihme_loc_id=="GIN" & year==2014 & source_type_id==16

	//duplicate BGR data
	replace outlier=1 if ihme_loc_id=="BGR" & year==2017 & deaths_nid=="323950"

	//keep Scotland 2019 country specific source
	replace outlier=0 if ihme_loc_id=="GBR_434" & year==2019 & deaths_nid=="456778"

	// only want to use this MEX subnat source for 2020
	replace outlier = 1 if year < 2020 & deaths_nid == "483925"

	// only want to use eurostat for ITA 2020 for now
	replace outlier = 1 if ihme_loc_id == "ITA" & year == 2020 & deaths_nid == "494866"

	// only want IND sources with u5 age splitting
	replace outlier = 1 if ihme_loc_id == "IND" & (inrange(year, 1996,2008) | inrange(year, 2010,2012)) & deaths_source != "SRS"

	// make sure that hong kong country specific source is not outliered
	replace outlier = 1 if ihme_loc_id == "CHN_354" & year == 2019 & deaths_nid == "502678"
	replace outlier = 0 if ihme_loc_id == "CHN_354" & year >= 2018 & deaths_nid == "538727"

	// drop duplicate hong kong VR source: less granular age groups
	replace outlier = 1 if ihme_loc_id == "CHN_354" & year == 2018 & deaths_nid == "426273"

	// drop DYB sources conflicting with new GBD2023 non-cod: less granular age groups
	replace outlier = 1 if ihme_loc_id == "CHN_354" & year == 2019 & deaths_nid == "502678"
	replace outlier = 1 if ihme_loc_id == "CHN_361" & year == 2022 & deaths_nid == "140966"
	replace outlier = 1 if ihme_loc_id == "MMR" & year == 2016 & deaths_nid == "426273"

	// drop DYB BRA 2021
	replace outlier = 1 if ihme_loc_id == "BRA" & year == 2021 & deaths_nid == "140966"

	// drop DYB BLZ 2018
	replace outlier = 1 if ihme_loc_id == "BLZ" & year == 2018 & deaths_nid == "426273"

	// drop DZA 2017-2019
	replace outlier = 1 if ihme_loc_id == "DZA" & year == 2017 & deaths_nid == "356617"
	replace outlier = 1 if ihme_loc_id == "DZA" & year == 2018 & deaths_nid == "412535"
	replace outlier = 1 if ihme_loc_id == "DZA" & year == 2019 & deaths_nid == "481929"

	// drop DYB BTN 2019-2021
	replace outlier = 1 if ihme_loc_id == "BTN" & inrange(year, 2019, 2021) & deaths_nid == "140966"

	// drop MCO 2005-2022
	replace outlier = 1 if ihme_loc_id == "MCO" & inrange(year, 2005, 2021) & deaths_nid == "517103"
	replace outlier = 1 if ihme_loc_id == "MCO" & year == 2022 & deaths_nid == "543890"

	// drop NVSS ASM 2021-2022
	replace outlier = 1 if ihme_loc_id == "ASM" & year == 2021 & deaths_nid == "537463"
	replace outlier = 1 if ihme_loc_id == "ASM" & year == 2022 & deaths_nid == "552340"

	// drop Eurostat LUX 2023
	replace outlier = 1 if ihme_loc_id == "LUX" & year == 2023 & deaths_nid == "550695"

	// fix LKA 2009-10 NID
	replace deaths_nid = "525875" if ihme_loc_id == "LKA" & deaths_nid == "272961"
	replace deaths_nid = "219193" if ihme_loc_id == "LKA" & deaths_nid == "272962"

	// outlier duplicate DOM
	replace outlier = 1 if ihme_loc_id == "DOM" & year == 2019 & deaths_nid == "466709"
	replace outlier = 1 if ihme_loc_id == "DOM" & year == 2021 & deaths_nid == "502625"

	// outlier eurostat ROU 2023
	replace outlier = 1 if ihme_loc_id == "ROU" & year == 2023 & deaths_nid == "475313"

	// un-outlier new CHN DSP 2018-21
	replace outlier = 0 if deaths_nid == "469163"
	replace outlier = 0 if deaths_nid == "469165"
	replace outlier = 0 if deaths_nid == "528380"
	replace outlier = 0 if deaths_nid == "528381"
	replace source_type_id = 3 if deaths_nid == "469163"
	replace source_type_id = 3 if deaths_nid == "469165"
	replace source_type_id = 3 if deaths_nid == "528380"
	replace source_type_id = 3 if deaths_nid == "528381"

	// use latest IRN data from Mohsen for 2011-2020
	replace source_type_id = 56 if deaths_source == "Iran Civil Registration Recorded Deaths"
	replace outlier = 1 if inrange(year, 2011, 2020) & regexm(ihme_loc_id,"IRN") & deaths_source == "IRN_NOCR_allcause_VR"

	** Check for duplicates
	preserve
		** keep things that are not outliers
		keep if outlier != 1
		** check for duplicates so we can see what they are (if any)
		sort ihme_loc_id country sex year source_type_id
		by ihme_loc_id country sex year source_type_id: generate nobs = _N
		keep if nobs > 1
		save "FILEPATH", replace
		drop DATUM*
		count if nobs > 1
		assert nobs <= 1
	restore


	count if deaths_nid == ""
	count if deaths_underlying_nid == ""

	// it should be outliered if it doesn't have an NID
	assert outlier == 1 if deaths_nid == ""


** ***********************************************************************
** Filter outliers
** ***********************************************************************
/*
	We can't age-sex split outliered data.  But we also want to upload the
    outliered data.
*/
	assert !mi(outlier)
	assert outlier == 0 | outlier == 1

	quietly compress

	// save non-outliers
	preserve
	keep if outlier == 0
	saveold "FILEPATH", replace
	restore

	// save outliers after adding estimate stage id
	preserve
	keep if outlier == 1
	gen estimate_stage_id = 22

	tempfile compiled_outliers_22
	save `compiled_outliers_22', replace

	replace estimate_stage_id = 21
	append using `compiled_outliers_22'

	// save outliers with each estimate_stage_id
	saveold "FILEPATH", replace
	restore

exit, clear STATA
