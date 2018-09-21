** ******************************************************
** Date created: August 10, 2009
** Description:
** Compiles data on population by age and sex from a variety of sources.
**
**
**
** NOTE: IHME OWNS THE COPYRIGHT
** ******************************************************

** **********************
** Set up Stata 
** **********************

	clear all 
	capture cleartmp
    capture restore, not
	set mem 500m
	set more off
	pause on

** **********************
** Filepaths 
** **********************

	if (c(os)=="Unix") global root "FILEPATH"
	if (c(os)=="Windows") global root "FILEPATH"
	global rawdata_dir "FILEPATH"
    global newdata_dir "FILEPATH"
    global hhdata_dir "FILEPATH"
	global save_file "FILEPATH/d00_compiled_population.dta"
	
	adopath + "FILEPATH"

** **********************
** Set up codes for merging 
** **********************
	get_locations, gbd_type(ap_old) level(estimate)
	keep if location_name == "Old Andhra PrUSER"
	tempfile ap_old 
	save `ap_old', replace

	get_locations
	append using `ap_old'
	keep local_id_2013 ihme_loc_id location_name // Keeping iso3 to merge on with old iso3s here
	rename local_id_2013 iso3
	rename location_name country
	sort iso3
	tempfile countrymaster
	save `countrymaster'
		
	// Create a duplicate observation for subnationals: we want to make sure that GBD2013 subnationals have iso3s
	// For both the fake iso3 (X**) and the real iso3 (GBR_***) to merge appropriately
	expand 2 if regexm(ihme_loc_id,"_") & iso3 != "", gen(new)
	replace iso3 = ihme_loc_id if new == 1 
	drop new
	
	replace iso3 = ihme_loc_id if iso3 == "" // For new locations without subnationals

	tempfile countrycodes
	save  `countrycodes', replace
	
	// Make a list of all the countries which contain subnational locations, for use in scaling/aggregation later
	get_locations, level(subnational)
	append using `ap_old'
	keep if regexm(ihme_loc_id,"_")
	split ihme_loc_id, parse("_")
	duplicates drop ihme_loc_id1, force
	keep ihme_loc_id1
	rename ihme_loc_id1 ihme_loc_id
	replace ihme_loc_id = "CHN_44533" if ihme_loc_id == "CHN" // Mainland has all the data
	keep ihme_loc_id
	tempfile parent_map
	save `parent_map'
	
	// Get all subnational locations, along with the total number of subnationals expected in each
	get_locations, level(subnational)
	drop if level == 4 & (regexm(ihme_loc_id,"CHN") | regexm(ihme_loc_id,"IND"))
	append using `ap_old'
	split ihme_loc_id, parse("_")
	rename ihme_loc_id1 parent_loc_id
	keep ihme_loc_id parent_loc_id
	bysort parent_loc_id: gen num_locs = _N
	keep ihme_loc_id parent_loc_id num_locs
	replace parent_loc_id = "CHN_44533" if parent_loc_id == "CHN" // Mainland has all the data
	tempfile subnat_locs
	save `subnat_locs'
	

	// Get UTLA parent map for aggregation to regions from CoD data 
	get_locations, subnat_only(GBR)
	keep ihme_loc_id parent_id 
	gen parent_loc_id = "GBR_" + string(parent_id)
	drop parent_id
	tempfile gbr_locs 
	save `gbr_locs', replace 


** **********************
** Compile census data from multi-country sources 
** **********************

** DYB 
	use "$rawdata_dir/DYB/DYB 1948-1997/USABLE_CENSUS_DYB_GLOBAL_1948-1997.dta", clear
	append using "$rawdata_dir/DYB/DYB 1998-1999 2002/USABLE_CENSUS_DYB_GLOBAL_1998-2002.dta"
	append using "$rawdata_dir/DYB/DYB 2000-2001 2003-2004/USABLE_CENSUS_DYB_GLOBAL_2000-2004.dta"
	append using "$rawdata_dir/DYB/DYB 2006/USABLE_CENSUS_DYB_GLOBAL_2006.dta"
	append using "$rawdata_dir/DYB/DYB 2000-2004/USABLE_CENSUS_DYB_GLOBAL_2000-2004.dta"
	append using "$rawdata_dir/DYB/DYB download/USABLE_VR_DYB_GLOBAL_1948_2011_DOWNLOAD_POP.dta"
    append using "$rawdata_dir/DYB/DYB 2012/USABLE_CENSUS_POP_DYB_2012_GLOBAL.DTA"
    append using "$rawdata_dir/DYB/DYB 2013 2014/USABLE_CENSUS_POP_DYB_GLOBAL_2013_2014.dta"
    append using "$newdata_dir/unstat_censuses/data/USABLE_ALL_AGE_POP_UNSTAT_CENSUSES.dta" // not DYB, but UNStats

	** drop bad data
		** there is an age group missing for the 1981 Syrian census in the download version
	drop if CO == "SYR" & YEAR == 1981 & CENSUS_SOURCE == "DYB_download" 
		** drop NIC 2002, not a real census
	drop if CO == "NIC" & YEAR == 2002
		** drop VEN 1998, not a real census
	drop if CO == "VEN" & YEAR == 1998
        ** not sure if 1993 SDN is just north, or both north and south, so we drop it
    drop if CO == "SDN" & YEAR == 1993 & regexm(CENSUS_SOURCE,"DYB_ONLINE")
        ** 2006 BWA is based on 2006 demographic survey, not a census
    drop if CO == "BWA" & YEAR == 2006
		** Virgin Islands has duplicates that look strange
	drop if CO == "VIR" & YEAR == 2000 & DATUMTOT == .
	drop if CO == "VIR" & YEAR == 2000 & RELIABIL == 0 // Duplicate of other DYB data
		** Northern Marianas: We bring in more data later on extracted directly from the census (more years etc)
	drop if CO == "MNP" & inlist(YEAR,1980,2000) 
		** ZAF: South Africa not nationally representative
	drop if CO == "ZAF" & YEAR < 1996

	drop if CO == "ALB" & YEAR == 2011
		** AND: concerned about foreign nationals, swapping in data from stats department
	drop if CO == "AND"

	** drop duplicates on all variables 
    duplicates drop
	
	** drop duplicates by keeping preferred sources
	keep if AREA == 0
	drop if RECTYPE == 7 | RECTYPE == 8 | FOOTNOTE == "Not a true census."
	drop if RECTYPE == 5 | RECTYPE == 6
	gen priority = 1 if CENSUS_SOURCE == "DYB_download" 
	replace priority = 2 if regexm(CENSUS_SOURCE, "DYB_ONLINE")
	replace priority = 3 if CENSUS_SOURCE == "DYB_INCRUDE"
	replace priority = 4 if CENSUS_SOURCE == "DYB_CD" 
	bysort CO YEAR SEX: egen best = min(priority) 
	keep if priority == best
	drop priority best
	
	** drop duplicates by DATUMTOT
	egen temp = rowtotal(DATUM*)
	replace DATUMTOT = temp if DATUMTOT == . 
	duplicates tag CO YEAR SEX, gen(d)
	bysort CO YEAR SEX: egen double best = max(DATUMTOT)
	keep if DATUMTOT == best
	drop temp best d
		
	** drop duplicates by age groups
	egen miss = rowmiss(DATUM*)
	bysort CO YEAR SEX: egen best = min(miss)
	keep if best == miss
	drop miss best
	
	** drop duplicates by census data
	gen temp = MONTH/12 + DAY/365.25
	bysort CO YEAR SEX: egen double best = max(temp)
	keep if temp == best 
	drop temp best 
    
    ** drop duplicates by online source
    duplicates tag CO YEAR SEX, g(d)
    drop if d != 0 & CENSUS_SOURCE != "DYB_ONLINE_2006"
    drop d
    
	isid CO YEAR SEX 
	
	** CHINA FIXES: DROP 2000 CENSUS FROM DYB
	** Also, make 1982 and 1990 Census populations reflect that those numbers are representative of 1981 and 1989
	drop if CO == "CHN" & YEAR == 2000
	replace YEAR = 1981 if CO == "CHN" & YEAR == 1982
	replace YEAR = 1989 if CO == "CHN" & YEAR == 1990
	
** IPUMS 
	append using "$rawdata_dir/IPUMS/USABLE_CENSUS_IPUMS_GLOBAL_1960-2005.dta"
	append using "$rawdata_dir/IPUMS/PROJECT_IPUMS2011_GLOBAL_vPOP.dta"
	append using "$rawdata_dir/IPUMS/EGY_FRA_2006_IPUMS.dta" 
    append using "$newdata_dir/ipums_census/data/USABLE_ALL_AGE_POP_IPUMS_CENSUS_2013_UPDATE.dta"
	
	** we prefer IPUMS to the above
	drop temp 
	duplicates tag CO YEAR SEX, gen(d)
	drop if d == 1 & CENSUS_SOURCE != "IPUMS"
	drop d
	isid CO YEAR SEX 

** HMD (GBR (and England/Wales, N Ireland, and Scotland), NLD, CHE, TWN, BEL - only census years that we want to use, except for subnational where we use all years)
	append using "$newdata_dir/hmd_pop/data/USABLE_ALL_AGE_POP_HMD.dta"
    drop if CENSUS_SOURCE == "HMD" & !inlist(COUNTRY,"GBR","XEN", "XNI","XSC","NLD","CHE","TWN","BEL")  
	drop if CENSUS_SOURCE == "HMD" & COUNTRY == "GBR" & !inlist(YEAR, 1931, 1941, 1951, 1961) 
    drop if CENSUS_SOURCE == "HMD" & COUNTRY == "XEN" & !inlist(YEAR, 1931, 1941, 1951, 1961, 1971, 1981, 1991, 2001, 2011) 
    drop if CENSUS_SOURCE == "HMD" & COUNTRY == "NLD" & !inlist(YEAR, 1850, 1860, 1870, 1880, 1890, 1900, 1910, 1920, 1930) & !inlist(YEAR, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010) 
    drop if CENSUS_SOURCE == "HMD" & COUNTRY == "CHE" & !inlist(YEAR, 1950, 1960, 1970, 1980, 1990, 2000, 2010)
    drop if CENSUS_SOURCE == "HMD" & COUNTRY == "TWN" & !inlist(YEAR, 1970, 1975, 1980, 1990, 2000, 2010)
    drop if CENSUS_SOURCE == "HMD" & COUNTRY == "BEL" & !inlist(YEAR, 1846, 1856, 1866, 1880, 1890, 1900, 1910, 1920, 1930) & !inlist(YEAR, 1947, 1961, 1970, 1981, 1991)
    ** keep censuses from NI and Scotland, not sample in 1966
    drop if CENSUS_SOURCE == "HMD" & CO == "XNI" & !inlist(YEAR, 1951, 1961, 1971, 1981, 1991, 2001, 2011)
    drop if CENSUS_SOURCE == "HMD" & CO == "XSC" & !inlist(YEAR, 1951, 1961, 1971, 1981, 1991, 2001, 2011)

	** we prefer HMD to the above
	duplicates tag CO YEAR SEX, gen(d)
	drop if d == 1 & CENSUS_SOURCE != "HMD"
	drop d
	isid CO YEAR SEX

** Other
	append using "$rawdata_dir/CELADE/USABLE_CENSUS_CELADE_GLOBAL_2000-2001-2002-2005.dta"
	append using "$rawdata_dir/CARICOM/USABLE_CENSUS_CARICOM_MULTIPLE_1970_1990_1991_2001.dta"
	
	** we prefer these to the above
	duplicates tag CO YEAR SEX, gen(d)
	drop if d == 1 & CENSUS_SOURCE != "CELADE" & CENSUS_SOURCE != "CARICOM" & CENSUS_SOURCE != "IBGE"
	drop d
	isid CO YEAR SEX 

** **********************
** Compile census data from country-specific sources 
** **********************
	
** Bahrain 
	** we prefer this version to the DYB version as it's de jure not de facto
	drop if CO == "BHR" & (YEAR == 2001 | YEAR == 2010)
	append using "$rawdata_dir/BHR_2001_2010/BHR_2001_2010_vPopulation.dta"
	
** Botswana 2011 census
	append using "$newdata_dir/bwa_census/data/USABLE_ALL_AGE_POP_BWA_2011_CENSUS.dta"
	
** Brazil
	drop if CO == "BRA" & inlist(YEAR, 1970, 1980, 1991, 1996, 2000, 2010)
	append using "$rawdata_dir/BRA_IGBE/BRA_IGBE_CENSUS_POPULATION.DTA"
	
** China: 2010 and 2000	
	append using "$rawdata_dir/CHN_census_2010/USABLE_CHN_CENSUS_2010_POPULATION.DTA"
	append using "$rawdata_dir/CHN_2000_CENSUS_STATYB2002/USABLE_CHN_2000_CENSUS_STATYB2002_POPULATION.dta"

** Fiji
	append using "$rawdata_dir/FJI_Census_2007/USABLE_FJI_CENSUS_2007.dta"
	
** Georgia 
	drop if CO == "GEO" & YEAR == 2002
	append using "$rawdata_dir/Georgia/USABLE_CENSUS_GEO_2002.dta"

** Indonesia
	append using "$rawdata_dir/SUSENAS/USABLE_SURVEY_SUSENAS_IDN_2000_2004_2007_vPOPULATION.dta"
	append using "$rawdata_dir/SUPAS/USABLE_SURVEY_SUPAS_IDN_1985_vPOPULATION.dta"
	append using "$rawdata_dir/IDN CENSUS/USABLE_CENSUS_IDN_2000_vPOPULATION.dta"
	
** Indonesia prepped at province level
** ***********************************
	tempfile master
	save `master', replace

	get_locations, subnat_only(IDN)
	keep ihme_loc_id location_id
	rename ihme_loc_id COUNTRY
	drop if location_id == 11
	tempfile idn_provs
	save `idn_provs', replace

	use "FILEPATH"
	keep prov_num location_id
	drop if mi(location_id) // East Timor
	merge 1:1 location_id using `idn_provs', nogen
	save `idn_provs', replace 

	use "$rawdata_dir/IDN CENSUS/CENSUS_2000_POPULATION.dta", clear
	drop COUNTRY

	append using "$rawdata_dir/SUPAS/SUPAS_1985_POPULATION.dta"
	append using "$rawdata_dir/SUPAS/SUPAS_2005_POPULATION.dta"
	append using "$rawdata_dir/SUSENAS/SUSENAS_2000_POPULATION.dta"
	append using "$rawdata_dir/SUSENAS/SUSENAS_2004_POPULATION.dta"
	append using "$rawdata_dir/SUSENAS/SUSENAS_2007_POPULATION.dta"
	drop sample*
	drop if SEX==.
	merge m:1 prov_num using `idn_provs', nogen
	drop if prov_num == 54
	drop prov_num location_id

	tempfile idn_subnat 
	save `idn_subnat', replace

	use `master', clear
	append using `idn_subnat'


** **********************************

** Japan 
	append using "$rawdata_dir/Japanese Historical data/USABLE_POP_JPN_vHistoricalData.dta"
	** only use these to fill in above sources
	duplicates tag CO YEAR SEX, gen(d)
	drop if d == 1 & CENSUS_SOURCE == "KS"
	drop d 

** Kuwait
	** we want to drop all other Kuwait populations - can't be reliably used due to the out-migration after the gulf war
	drop if CO == "KWT"
	append using "$rawdata_dir/KWT_1995_2005_CENSUS/USABLE_CENSUS_KWT_1995_2005.dta"

** North Korea 2008 census
	drop if CO == "PRK" & YEAR == 2008
	append using "$rawdata_dir/PRK/PRK_2008_vPopulation.dta"

** Peru 1993 Census
	append using "$newdata_dir/per_census_1993/data/USABLE_ALL_AGE_POP_PER_1993_CENSUS.dta"

** Saudi Arabia 
	append using "$rawdata_dir/SAU/SAU_CENSUS_2007_BULLETIN_SAU_2007_vPOP.dta"

** Slovakia 
	append using "$rawdata_dir/GOV/SVK/USABLE_CENSUS_GOV_SVK_1930_1961_1980.dta"
	append using "$rawdata_dir/GOV/SVK/USABLE_CENSUS_GOV_SVK_1950.dta"

** Sri Lanka
	drop if CO == "LKA" & YEAR == 2001
	append using "$rawdata_dir/LKA/USABLE_CENSUS_GOV_LKA_2001.dta"

** Tonga
	drop if CO == "TON" & YEAR == 2006
	append using "$rawdata_dir/Tonga/USABLE_CENSUS_TONGASTATDEP_TONGA_2006_vPOP"	
	
** United Arab Emirates 
	drop if CO == "ARE" & (YEAR == 1995 |  YEAR == 2005)
	// LDL 20Apr2012
	append using "$rawdata_dir/ARE/ARE_UN_population.dta" 

** United States 
	drop if CO == "USA" & YEAR == 2010
	append using "$rawdata_dir/USA/USA_2010_CENSUS_POP.DTA"
	
** Uruguay
	append using "$rawdata_dir/URY_CENSUS_2011/USABLE_URY_CENSUS_2011.dta"
    
** ZAF: 2011 Census (Stats South Africa) Better than the DYB 2012 numbers, use these instead
    append using "$newdata_dir/zaf_census/data/USABLE_ALL_AGE_POP_ZAF_CENSUS_SSA_2011.dta" 
    
** MEX IPUMS Populations, 1990, 2000, 2005, 2010
   append using "$rawdata_dir/MEX_subnat_pop_IPUMS/USABLE_MEX_POP_IPUMS_1990_2000_2005_2010.dta"

** **********************
** Compile non-census populations
** **********************

	gen source_type = "CENSUS" 

** Andorra from Department of Statistics
	append using "$newdata_dir/and_vr/data/USABLE_ALL_AGES_ANDORRA_POP_1947_2016.dta"
	replace source_type = "VR" if source_type == "" & CO == "AND"

** BanglUSER
	append using "$newdata_dir/bgd_srs/data/USABLE_ALL_AGE_POP_BGD_SRS_REPORT_2012-2014.DTA"
	
** China 
	append using "$rawdata_dir/CHINA DSP/1996_2000/USABLE_DSPPOP_CHN_1996-2000.dta"
	append using "$rawdata_dir/CHINA DSP/2004_2010/USABLE_DSP_2004_2010_noweight.dta"
	** the following code substitutes provincial DSP estimates aggregated to national (must comment out the above 2 lines as well)
	append using "$newdata_dir/chn_dsp/data/USABLE_ALL_AGE_POP_CHN_DSP_91_12.dta" 
	drop if !inlist(YEAR,1991,1992,1993,1994,1995,2001,2002,2012) & CENSUS_SOURCE == "CHN_DSP" // Keep these years from the newdata file
	replace source_type = "DSP" if source_type == "" 
	
	append using "$rawdata_dir/CHN_SSPC/USABLE_SSPC_CHN_1986-2008.dta"
	append using "$newdata_dir/chn_sspc_2010_2012/data/USABLE_ALL_AGE_POP_CHN_SSPC_2010_2012.dta"
	replace source_type = "SSPC" if source_type == "" 
	
	append using "$rawdata_dir/CHINA 1 percent/USABLE_INT_GOV_CHN_1995_vPop.dta"
	replace source_type = "DC" if source_type == "" 
	replace CENSUS_SOURCE = "1 percent survey" if CENSUS_SOURCE == ""

** India 
	append using "$rawdata_dir/SRS LIBRARY/INDIA 1997-2000-2006/NEWUSABLE_SRSPOP_IND_1997-2000-2008.dta" // includes correct population-weighting for 1992 SRS to be consistent with rest of analysis group
	append using "$rawdata_dir/SRS LIBRARY/INDIA 1970-1988/USABLE_SRSPOP_IND_1970-1988.dta"
	append using "$rawdata_dir/IND_SRS/USABLE_IND_SRS_2010_POPULATION.DTA"	
    append using "$newdata_dir/ind_srs/data/USABLE_ALL_AGE_POP_IND_SRS_2011.dta"
	replace source_type = "SRS" if source_type == ""  	
  
** TUR: 1989 Demographic Survey, survey population scaled up to population
	append using "$rawdata_dir/tur_demog_surv_1989/USABLE_tur_demog_surv_1989_population.dta"	
	replace source_type = "SURVEY" if source_type == ""
	
** TUR: add in 2010, 2011, 2012 population from the registry system
	append using "$rawdata_dir/turkstat_tabulations/USABLE_TURKSTAT_TABULATIONS_2010-2011_POPULATION.dta"
	replace source_type = "VR" if source_type == "" & CENSUS_SOURCE == "TurkStat_ADNKS"
	
** United Arab Emirates 
	append using "$rawdata_dir/ARE/ARE_population_MinOfPlanning.dta" 
	replace source_type = "MOH survey" if source_type == "" 
	
** ** ZAF 2007 community survey scaled up to population
	append using "$rawdata_dir/IPUMS_ZAF_COMM_SURVEY_2007/data/USABLE_ZAF_2007_COMM_SURVEY_IPUMS_POPULATIONS.dta"
    replace source_type = "SURVEY" if source_type == ""
	
** Great Britain : England and Wales by Region -- no longer using collaborator files
    append using "$rawdata_dir/GBR/USABLE_ALL_UK_POP_BY_REGION_1981_2012.dta"
    replace source_type = "VR" if CENSUS_SOURCE == "AD" 
    replace CO = "GBR_4636" if CO == "XGP"
    drop if COUNTRY != "GBR_4636" & CENSUS_SOURCE == "AD"

** UK : UTLAs
	append using "$newdata_dir/gbr_vr/data/USABLE_ALL_AGES_POP_GBR_UTLA_1971_2015.dta"
	replace source_type = "VR" if source_type == ""
    replace VR_SOURCE = "Public Health England" if VR_SOURCE == "" & regexm(COUNTRY, "GBR_")
    drop if CO == "GBR_4636" & CENSUS_SOURCE != "AD"

	** aggregate UTLAs from CoD data to regions 
	preserve 
		keep if regexm(COUNTRY, "GBR_") & !inlist(COUNTRY, "GBR_4749", "GBR_4636", "GBR_433", "GBR_434") 
		replace ihme_loc_id = COUNTRY if mi(ihme_loc_id) & regexm(COUNTRY, "GBR_")
		merge m:1 ihme_loc_id using `gbr_locs', keep(3) nogen
		collapse (sum) DATUM*, by(parent_loc_id SEX YEAR SUBDIV CENSUS_SOURCE nid FOOTNOTE AREA)
		rename parent_loc_id COUNTRY

		** fix zeros from collapse
		foreach var of varlist DATUM* {
			count if `var' == 0
			if `r(N)' == _N drop `var'
		}
		tempfile gbr_regs
		save `gbr_regs', replace

	restore
	append using `gbr_regs'
	
	** zeros before 2001
	replace source_type = "VR" if regexm(COUNTRY, "GBR_") & mi(source_type)
	replace DATUM0to0 =. if DATUM0to0 == 0 & regexm(CO, "GBR_") & inrange(YEAR, 1971, 2000)
	replace DATUM1to4 =. if DATUM1to4 == 0 & regexm(CO, "GBR_") & inrange(YEAR, 1971, 2000)
	replace DATUM85to89 =. if DATUM85to89 == 0 & regexm(CO, "GBR_") & inrange(YEAR, 1971, 2000)
	replace DATUM90plus =. if DATUM90plus == 0 & regexm(CO, "GBR_") & inrange(YEAR, 1971, 2000)
	** zeros after 2001
	replace DATUM85plus =. if DATUM85plus == 0 & regexm(CO, "GBR_") & YEAR > 2000


** CHN provincial data

    ** censuses
        append using "$newdata_dir/chn_province_census_2000/data/USABLE_ALL_AGE_POP_CHN_PROVINCE_2000_CENSUS.DTA"
        append using "$newdata_dir/chn_province_census_2010/data/USABLE_ALL_AGE_POP_CHN_PROVINCE_2010_CENSUS.DTA"
        
        append using "$newdata_dir/chn_province_census_1990/data/USABLE_ALL_AGE_POP_CHN_PROVINCE_1990_CENSUS.DTA"
        append using "$newdata_dir/chn_province_census_1982/data/USABLE_ALL_AGE_POP_CHN_PROVINCE_1982_CENSUS.dta"
        
        append using "$newdata_dir/chn_province_census_1990/data/NOT_USABLE_ALL_AGE_POP_CHN_PROVINCE_1990_CENSUS.DTA"
        
        append using "$newdata_dir/chn_province_census_1964/data/USABLE_ALL_AGE_POP_CHN_PROVINCE_1964_CENSUS.DTA"
        
    ** DSP
        append using "$newdata_dir/chn_dsp_prov/data/USABLE_ALL_AGE_POP_CHN_PROVINCE_DSP_91_12.dta"
		append using "$newdata_dir/chn_dsp_prov/data/useable_all_age_pop_CHN_province_DSP_13_14.dta"
        replace source_type = "DSP1" if source_type==""
        replace source_type = "DSP2" if source_type == "DSP1" & YEAR >= 1996 & YEAR < 2004
        replace source_type = "DSP3" if source_type == "DSP1" & YEAR >= 2004
		replace CENSUS_SOURCE = "CHN_DSP" if CENSUS_SOURCE == "" & regexm(ihme_loc_id,"CHN_")
				
	** 1% survey 2005
		append using "$newdata_dir/chn_1percent_survey/data/USABLE_ALL_AGE_DEATHS_CHN_PROVINCE_1PERCENT_SURVEY_2005.dta"
		replace source_type = "DC" if source_type == ""
		
	** Family planning survey 1992
		append using "$newdata_dir/chn_ffps_1992/data/USABLE_ALL_AGE_POP_CHN_FFPS_1992.dta"
		replace source_type = "FFPS" if source_type == ""	
		
** IND urban rural
	** SRS 
	// Also includes national 2012 SRS numbers, which do not exist elsewhere currently
		append using "$newdata_dir/ind_srs_urban_rural/data/USABLE_ALL_AGE_POP_IND_URBAN_RURAL_SRS.dta"
		replace source_type = "SRS" if source_type == ""
		
** IND Urban Rural States
	** SRS
		append using "$newdata_dir/ind_srs_state_urban_rural/data/USABLE_ALL_AGE_POP_IND_STATE_URBAN_RURAL_SRS_1995_2013.dta"
		append using "$newdata_dir/ind_srs_2014_2015/data/USABLE_ALL_AGE_POP_IND_SRS_2014_2015.dta"
		replace source_type = "SRS" if source_type == ""

		** Agg AP and Telangana into old AP for 2014-15 SRS data
		preserve 
			keep if inlist(CO, "IND_4871", "IND_4841") & inrange(YEAR, 2014, 2015)
			foreach var of varlist DATUM* {
				count if mi(`var')
				if `r(N)' == _N drop `var'
			}
			collapse (sum) DATUM*, by( YEAR SEX SUBDIV CENSUS_SOURCE source_type AREA)
			gen COUNTRY = "IND_44849"
			tempfile new_ap_old 
			save `new_ap_old', replace 
		restore
		append using `new_ap_old'


** MEX pop 1970-2013, use censuses instead instead of subnational
	append using "$rawdata_dir/MEX_POP/USABLE_MEX_POP_SUBNATIONAL_1970_2013.dta"
    replace source_type = "NOT_USABLE_MODELED_MEX" if source_type == ""
	
** US Census subnational populations -- these were mismarked as CENSUS for all years
	append using "$newdata_dir/usa_census/data/USABLE_ALL_AGE_POP_USA_1950_2012_STATE_CENSUS.dta"
	replace source_type = "VR" if regexm(COUNTRY, "USA") & !inlist(YEAR, 1950, 1960, 1970, 1980, 1990, 2000, 2010)

** Brazil Census subnational populations
	// append using "$newdata_dir/bra_vr/data/USABLE_ALL_AGE_POP_BRA_1980_1999_CENSUS.dta" // Old data pre-Fatima
	// append using "$newdata_dir/bra_vr/data/USABLE_ALL_AGE_POP_BRA_2000_2013_CENSUS.dta" // New data from Fatima -- pop update
	// Now we use the 2000_2013 file (same as the new data from Fatima) along with back-calculation of pops to 1970 from collaborators
	// They use an inverse cohort approach
	append using "$newdata_dir/bra_pop_update/data/USABLE_ALL_AGE_POP_BRA_1970_2013_CENSUS.dta"
	
** Sweden subnational Populations
	append using "$newdata_dir/swe_vr/data/USABLE_ALL_AGE_POP_SWE_1968_2014_CENSUS.dta"

** India state/urbanicity populations
	append using "$newdata_dir/ind_state_census/data/USABLE_ALL_AGE_POP_IND_1961_2011_STATE_CENSUS.dta"
	
** Greenland population
	append using "$newdata_dir/grl_statbank/data/USABLE_ALL_AGE_POP_GRL_1977_2015_CENSUS.dta"
		
** Japan subnational populations
	append using "$newdata_dir/jpn_census/data/USABLE_ALL_AGE_POP_JPN_1975_2010_CENSUS.dta"
	
** ZAF population
	append using "$newdata_dir/zaf_state_census/data/USABLE_ALL_AGE_POP_ZAF_1996_2011_STATE_CENSUS.dta"
	
** MNP Census
	append using "$newdata_dir/mnp_census/data/USABLE_ALL_AGE_POP_MNP_1968_2000_CENSUS.dta"
	
** SAU Subnational Census
	append using "$newdata_dir/sau_census/data/USABLE_ALL_AGE_POP_SAU_2004_2013_CENSUS.dta"

** RUS (RosStat) populations 2001-2014 from collaborator
	append using "$newdata_dir/rus_vr/data/USABLE_ALL_AGE_POP_RUS_2003_2014.dta"
	drop if COUNTRY == "RUS" & YEAR == 2010 & CENSUS_SOURCE == "RosStat_collaborator"

** *****************************
** Add in household deaths exposure (will be dropped before DDM)
** *****************************
** BDI Demographic Survey 1965
	append using "$hhdata_dir/BDI_demographic_survey_1965/data/USABLE_ALL_AGE_POP_BDI_demographic_survey_1965.dta"
	
** BDI Demographic Survey 1970-1971
	append using "$hhdata_dir/BDI_demographic_survey_1970-1971/data/USABLE_ALL_AGE_POP_BDI_demographic_survey_1970-1971.dta"
	
** BGD 2011 census
	append using "$hhdata_dir/BGD_CENSUS_2011/data/USABLE_ALL_AGE_POP_BGD_CENSUS_2011.dta"	

** BWA Demographic Survey 2006
	append using "$hhdata_dir/bwa_demog_survey_2006/data/USABLE_ALL_AGE_POP_BWA_DEMOG_SURVEY_2006.dta"
	replace source_type = "HOUSEHOLD" if source_type == ""
    ** ** temporarily make it run through DDM by adding in populations (not exposure) and by duplicating all censuses to also have entries for this source
    ** append using "$hhdata_dir/bwa_demog_survey_2006/data/USABLE_ALL_AGE_POP_BWA_DEMOG_SURVEY_2006_FOR_DDM.dta"
    ** replace source_type = "DEM_SURV_2006" if source_type == ""
    ** expand 2 if COUNTRY == "BWA" & source_type == "CENSUS"
    ** bysort COUNTRY YEAR SEX source_type: replace source_type = "DEM_SURV_2006" if _n == _N & COUNTRY == "BWA" & source_type == "CENSUS"
	
** BWA census 1981
	append using "$hhdata_dir/BWA_census_1981/data/USABLE_ALL_AGE_POP_BWA_CENSUS_1981.dta"
	
** CMR: 1976 census
	append using "$hhdata_dir/cmr_census_1976/data/USABLE_ALL_AGE_POP_CMR_CENSUS_1976.dta"
	replace source_type = "HOUSEHOLD" if source_type == ""

** Cote d'Ivoire 1978-1979 Demographic Survey
	append using "$hhdata_dir/CIV_DS_1978_1979/data/USABLE_ALL_AGE_POP_CIV_DS_1978_1979.dta"	

** COG census 1984
	append using "$hhdata_dir/COG_census_1984/data/USABLE_ALL_AGE_POP_COG_CENSUS_1984.dta"
	
** Ecuador ENSANUT 2012
	append using "$hhdata_dir/ECU_ENSANUT_2012/data/USABLE_ALL_AGE_POP_ECU_ENSANUT_2012.dta"
	
** HND EDENH 1971-1972
	append using "$hhdata_dir/HND_EDENH_1971_1972/data/USABLE_ALL_AGE_POP_HND_EDENH_1971_1972.dta"	

** HND survey of living conditions 2004
	append using "$hhdata_dir/HND_SLC_2004/data/USABLE_ALL_AGE_POP_HND_SLC_2004.dta"	
	
** IRQ
    append using "$hhdata_dir/irq_imira_2004/data/USABLE_ALL_AGE_POP_IRQ_IMIRA_2004.dta"
    replace source_type = "HOUSEHOLD" if source_type == ""
	
** KEN: Census 2009
	append using "$hhdata_dir/ken_census_2009/data/USABLE_ALL_AGE_POP_KEN_2009_CENSUS.dta"	
	replace source_type = "HOUSEHOLD" if source_type == ""
	
** KEN AIDS Indicator Survey 2007	
	append using "$hhdata_dir/KEN_AIS_2007/data/USABLE_ALL_AGE_POP_KEN_AIS_2007.dta"
	
** KHM: Socioeconomic survey 1997
	append using "$hhdata_dir/khm_socioeconomic_survey_1997/data/USABLE_ALL_AGE_POP_KHM_SOCIOECONOMIC_SURVEY_1997.dta"
    replace source_type = "HOUSEHOLD" if source_type == ""
	
** KIR 2010 Census
	append using "$hhdata_dir/KIR_CENSUS_2010/data/USABLE_ALL_AGE_POP_KIR_CENSUS_2010.dta"
	
** Malawi Population Change Survey 1970-1972
	append using "$hhdata_dir/MWI_POP_CHANGE_SURVEY_1970-1972/data/USABLE_ALL_AGE_POP_MWI_POP_CHANGE_SURVEY_1970-1972.dta"
	
** Mauritania 1988 Census
	append using "$hhdata_dir/MRT_CENSUS_1988/data/USABLE_ALL_AGE_POP_MRT_CENSUS_1988.dta"	

** MOZ census 2007
	append using "$hhdata_dir/MOZ_census_2007/data/USABLE_ALL_AGE_POP_MOZ_CENSUS_2007.dta"
	
** NAM 2011 census
	append using "$hhdata_dir/NAM_CENSUS_2011/data/USABLE_ALL_AGE_POP_NAM_CENSUS_2011.dta"
	
** SLB 2009 census
	append using "$hhdata_dir/SLB_CENSUS_2009/data/USABLE_ALL_AGE_POP_SLB_CENSUS_2009.dta"
	
** Tanzania Census 1967
	append using "$hhdata_dir/TZA_CENSUS_1967/data/USABLE_ALL_AGE_POP_TZA_CENSUS_1967.dta"
	
** TGO Census 2010
	append using "$hhdata_dir/tgo_census_2010/data/USABLE_ALL_AGE_POP_TGO_CENSUS_2010.dta"
	replace source_type = "HOUSEHOLD" if source_type == ""
	
** NGA: GHS 2006
    append using "$hhdata_dir/nga_ghs_2006/data/USABLE_ALL_AGE_POP_NGA_GHS_2006.dta"
    replace source_type = "HOUSEHOLD" if source_type == ""
	
** ZAF community survey 2007
	append using "$hhdata_dir/ZAF_CS_2007/data/USABLE_ALL_AGE_POP_ZAF_CS_2007.dta"
	
** ZMB: 2008 HHC
    append using "$hhdata_dir/zmb_hhc_2008/data/USABLE_ALL_AGE_POP_ZMB_HHC_2008.dta"
    replace source_type = "HOUSEHOLD" if source_type == ""
    
** ZMB LCMS
    append using "$hhdata_dir/zmb_lcms/data/USABLE_ALL_AGE_POP_ZMB_LCMS.dta"
    replace source_type = "HOUSEHOLD" if source_type == ""
    
** ZMB SBS
    append using "$hhdata_dir/zmb_sbs/data/USABLE_ALL_AGE_POP_ZMB_SBS_2009.dta"
    replace source_type = "HOUSEHOLD" if source_type == ""
	append using "$hhdata_dir/zmb_sbs/data/USABLE_ALL_AGE_POP_ZMB_SBS_2005.dta"
    replace source_type = "HOUSEHOLD" if source_type == ""
	append using "$hhdata_dir/zmb_sbs/data/USABLE_ALL_AGE_POP_ZMB_SBS_2003.dta"
    replace source_type = "HOUSEHOLD" if source_type == ""
 
// Drop location ID variable (used as IHME location id above, but filepath of the source below)
	cap drop location_id
 
** VNM NHS
	append using "$hhdata_dir/vnm_nhs_2001_2002/data/USABLE_ALL_AGE_POP_1986_NHS.dta"
	replace source_type = "HOUSEHOLD" if source_type == ""

** Papchild surveys: DZA EGY LBN LBY MAR MRT SDN SYR TUN YEM
	append using "$hhdata_dir/papchild/data/USABLE_ALL_AGE_POP_1990_1997_papchild.dta"
	replace source_type = "HOUSEHOLD" if source_type == ""
	cap drop location_id
	
** DHS surveys, ERI 1995-1996 and 2002 NGA 2013, MWI 2010, ZMB 2007, RWA 2005, IND 1998-1999, DOM 2013, UGA 2006, HTI 2005-2006, ZWE 2005-2006, NIC 2001, BGD SP 2001, JOR 1990, DOM 2013
	append using "$hhdata_dir/ERI_DHS/data/USABLE_ALL_AGE_POP_ERI_DHS.dta"
	append using "$hhdata_dir/NGA_DHS_2013/data/USABLE_ALL_AGE_POP_NGA_DHS_2013.dta"
	append using "$hhdata_dir/MWI_DHS_2010/data/USABLE_ALL_AGE_POP_MWI_DHS_2010.dta"
	append using "$hhdata_dir/ZMB_DHS_2007/data/USABLE_ALL_AGE_POP_ZMB_DHS_2007.dta"
	append using "$hhdata_dir/RWA_DHS_2005/data/USABLE_ALL_AGE_POP_RWA_DHS_2005.dta"
	append using "$hhdata_dir/DOM_DHS_2013/data/USABLE_ALL_AGE_POP_DOM_DHS_2013.dta"
	append using "$hhdata_dir/UGA_DHS_2006/data/USABLE_ALL_AGE_POP_UGA_DHS_2006.dta"	
	append using "$hhdata_dir/HTI_DHS_2005_2006/data/USABLE_ALL_AGE_POP_HTI_DHS_2005_2006.dta"
	append using "$hhdata_dir/ZWE_DHS_2005_2006/data/USABLE_ALL_AGE_POP_ZWE_DHS_2005_2006.dta"
	append using "$hhdata_dir/NIC_DHS_2001/data/USABLE_ALL_AGE_POP_NIC_DHS_2001.dta"
	append using "$hhdata_dir/BGD_SP_DHS_2001/data/USABLE_ALL_AGE_POP_BGD_SP_DHS_2001.dta"
	append using "$hhdata_dir/JOR_DHS_1990/data/USABLE_ALL_AGE_POP_JOR_DHS_1990.dta"

	replace source_type = "HOUSEHOLD" if source_type == ""	
	
** ZAF NIDS
	append using "$hhdata_dir/ZAF_NIDS_2010_2011/data/USABLE_ALL_AGE_POP_ZAF_NIDS_2010_2011.dta" 

** TZA LSMS
	append using "$hhdata_dir/TZA_LSMS_2008_2009/data/USABLE_ALL_AGE_POP_TZA_LSML_2008_2009.dta" 
	replace source_type = "HOUSEHOLD" if source_type == ""
		
** Subnationals not previously run at the national level
	
	** MEX SAGE
	append using "$hhdata_dir/MEX_SAGE_2009_2010/data/USABLE_ALL_AGE_POP_MEX_SAGE_2009_2010.dta" 
	
	** CHN SAGE
	append using "$hhdata_dir/CHN_SAGE_2008_2010/data/USABLE_ALL_AGE_POP_CHN_SAGE_2008_2010.dta" 
	replace source_type = "HOUSEHOLD" if source_type == ""
	
	** ZAF CS IPUMS 2007
	append using "$hhdata_dir/IPUMS/data/USABLE_ALL_AGE_POP_ZAF_IPUMS_2007.dta" 
	
	** ZAF IPUMS 2001
	append using "$hhdata_dir/IPUMS/data/USABLE_ALL_AGE_POP_ZAF_IPUMS_2001.dta" 
	
	** ZAF October HS 1993,1995,1996,1997,1998
	append using "$hhdata_dir/zaf_household_survey/data/USABLE_ALL_AGE_POP_ZAF_OCT_HOUSEHOLD_SURVEY_1993.dta" 
	append using "$hhdata_dir/zaf_household_survey/data/USABLE_ALL_AGE_POP_ZAF_OCT_HOUSEHOLD_SURVEY_1995.dta" 
	append using "$hhdata_dir/zaf_household_survey/data/USABLE_ALL_AGE_POP_ZAF_OCT_HOUSEHOLD_SURVEY_1996.dta" 
	append using "$hhdata_dir/zaf_household_survey/data/USABLE_ALL_AGE_POP_ZAF_OCT_HOUSEHOLD_SURVEY_1997.dta" 
	append using "$hhdata_dir/zaf_household_survey/data/USABLE_ALL_AGE_POP_ZAF_OCT_HOUSEHOLD_SURVEY_1998.dta" 
	cap drop location_id
	replace source_type = "HOUSEHOLD" if source_type == ""
		
** Subnationals previously run at the national level 
	** ZAF Census 2011
	append using "$hhdata_dir/zaf_census_2011/data/USABLE_ALL_AGE_POP_ZAF_CENSUS_2011.dta" 

	replace source_type = "HOUSEHOLD" if source_type == ""

** SLV_IPUMS_CENSUS
	append using "$hhdata_dir/SLV_IPUMS_CENSUS_2007/data/USABLE_ALL_AGE_POP_SLV_IPUMS_CENSUS_2007.dta"
	replace source_type = "HOUSEHOLD" if source_type == ""
	
** DOM_ENHOGAR
	append using "$hhdata_dir/dom_enhogar_2006/data/USABLE_ALL_AGE_POP_DOM_ENHOGAR_2006.dta"
	replace source_type = "HOUSEHOLD" if source_type == ""
	
** GIN Demographic Survey 1954 1955
	append using "$hhdata_dir/GIN_Demosurvey_1954_1955/data/USABLE_ALL_AGE_POP_GIN_DemoSurvey_1954_1955.dta"

** THA Survey Population Change 2005 2006
	append using "$hhdata_dir/THA_SurveyPopChange_2005_06/data/USABLE_ALL_AGE_POP_THA_SPC_2005_2006.dta"

** NRU Census 2011
	append using "$hhdata_dir/NRU_Census_2011/data/USABLE_ALL_AGE_POP_NRU_CENSUS_2011.dta"

** MWI FFS 1984
	append using "$hhdata_dir/MWI_FFS_1984/data/USABLE_ALL_AGE_POP_MWI_FFS_1984.dta"	
	
** DJI Demographic Survey 1991
	append using "$hhdata_dir/DJI_Demosurvey_1991/data/USABLE_ALL_AGE_POP_DJI_DEMOSURVEY_1991.dta"		
	replace source_type = "HOUSEHOLD" if source_type == ""

** IND DLHS4 2012-2014
	append using "$hhdata_dir/IND_DLHS4_2012_2014/data/USABLE_ALL_AGE_POP_IND_DLHS_2012_2014.dta"
	replace source_type = "HOUSEHOLD_DLHS" if source_type == ""
	drop if inlist(ihme_loc_id, "IND_4871", "IND_43872", "IND_43902", "IND_43908", "IND_43938")  & !inrange(YEAR, 2014, 2015)
	replace ihme_loc_id = "IND_44849" if ihme_loc_id == "IND_4841" & !inrange(YEAR, 2014, 2015)
	
** **********************
** Make corrections to the database 
** **********************

** Fill in month and day

    ** South Africa census dates
    replace MONTH = 9 if CO == "ZAF" &  regexm(source_type,"CENSUS") & YEAR == 1951
    replace MONTH = 5 if CO == "ZAF" &  regexm(source_type,"CENSUS") & YEAR == 1960
    replace MONTH = 5 if CO == "ZAF" &  regexm(source_type,"CENSUS") & YEAR == 1970
    replace MONTH = 5 if CO == "ZAF" &  regexm(source_type,"CENSUS") & YEAR == 1980
    replace MONTH = 3 if CO == "ZAF" &  regexm(source_type,"CENSUS") & YEAR == 1985
    replace MONTH = 3 if CO == "ZAF" &  regexm(source_type,"CENSUS") & YEAR == 1991
    replace MONTH = 10 if CO == "ZAF" & regexm(source_type,"CENSUS") & YEAR == 1996
    replace MONTH = 10 if CO == "ZAF" & regexm(source_type,"CENSUS") & YEAR == 2001
    replace MONTH = 11 if CO == "ZAF" & regexm(source_type,"CENSUS") & YEAR == 2011
    replace DAY = 6 if CO == "ZAF" &    regexm(source_type,"CENSUS") & YEAR == 1951
    replace DAY = 6 if CO == "ZAF" &    regexm(source_type,"CENSUS") & YEAR == 1960
    replace DAY = 7 if CO == "ZAF" &    regexm(source_type,"CENSUS") & YEAR == 1970
    replace DAY = 7 if CO == "ZAF" &    regexm(source_type,"CENSUS") & YEAR == 1980
    replace DAY = 5 if CO == "ZAF" &    regexm(source_type,"CENSUS") & YEAR == 1985
    replace DAY = 8 if CO == "ZAF" &    regexm(source_type,"CENSUS") & YEAR == 1991
    replace DAY = 10 if CO == "ZAF" &   regexm(source_type,"CENSUS") & YEAR == 1996
    replace DAY = 15 if CO == "ZAF" &   regexm(source_type,"CENSUS") & YEAR == 2001
    replace DAY = 15 if CO == "ZAF" &   regexm(source_type,"CENSUS") & YEAR == 2011
    
    ** Turkey census dates
    replace MONTH = 10 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1950
    replace MONTH = 10 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1955
    replace MONTH = 10 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1960
    replace MONTH = 10 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1965
    replace MONTH = 10 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1970
    replace MONTH = 10 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1975
    replace MONTH = 10 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1980
    replace MONTH = 10 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1985
    replace MONTH = 10 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1990
    replace MONTH = 10 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 2000
    replace MONTH = 6 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 2010
    replace MONTH = 6 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 2011
    replace DAY = 24 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1950
    replace DAY = 23 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1955
    replace DAY = 23 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1960
    replace DAY = 22 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1965
    replace DAY = 25 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1970
    replace DAY = 26 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1975
    replace DAY = 12 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1980
    replace DAY = 15 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1985
    replace DAY = 15 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 1990
    replace DAY = 15 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 2000
    replace DAY = 15 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 2010
    replace DAY = 15 if CO == "TUR" & regexm(source_type,"CENSUS") & YEAR == 2011
    
	replace MONTH = 6 if MONTH == 0 | MONTH == .
	replace DAY = 15 if DAY == 0 | DAY == .

** Drop unknown sex 
	drop if SEX == 9

** Drop urban or rural only estimates 
	keep if AREA == 0 | AREA == .
	drop AREA

** Drop sample surveys
	gen temp_footnote = lower(FOOTNOTE)
	drop if regexm(FOOTNOTE, "sample survey") & COUNTRY != "NER" & COUNTRY != "SDN" 

** Correct youngest ages
	replace DATUM0to0 = DATUM0to1 if DATUM0to1 != . & DATUM0to0 == . & DATUM1to4 != .
	replace DATUM0to1 = . if DATUM0to1 != . & DATUM0to0 != . & DATUM1to4 != .

** Drop if everything is missing 
	looUSER DATUM
	return list
	local misscount = 0
	foreach var of varlist `r(varlist)' {
		local misscount = `misscount'+1
	}
	egen misscount = rowmiss(DATUM*)
	drop if misscount == `misscount'
	drop misscount

** **********************
** Make country-specific changes 
** **********************
** Drop ECU 2001 census -- age distribution problems
	drop if YEAR == 2001 & CO == "ECU" & source_type == "CENSUS"

** Drop before Cyprus split
	drop if YEAR < 1974 & CO == "CYP"
	
** Canada has censuses every 5 years on the 1 and 6, so 1992 is not a real census
	drop if YEAR == 1992 & CO == "CAN"
	
** Drop the 2010 Qatar census; the DDM estimates generated using this census are way too low
	drop if CO == "QAT" & YEAR == 2010
	
** Drop the 2007 Fiji Census from IPUMS; we have the data from the actual census
	drop if CO == "FJI" & YEAR == 2007 & CENSUS_SOURCE == "IPUMS"
	
** Drop 2003 Suriname census: there are only provisional figures for this census because all the census forms were lost in a fire. Suriname conducted another census in 2004 
	drop if CO == "SUR" & YEAR == 2003

** Duplicate from DYB in NLD
	drop if COUNTRY == "NLD" & CENSUS_SOURCE == "DYB_INCRUDE" & YEAR == 2002

** Drop CHN national data from provincial level estimates, as well as DYB 2010 census (AS: 1/23/2014)
    duplicates tag CO YEAR SEX if source_type == "CENSUS" & CO == "CHN", g(dup)
    drop if dup != 0 & dup != . & CO == "CHN" & (regexm(CENSUS_SOURCE,"CHN_PROV_CENSUS") | regexm(CENSUS_SOURCE,"DYB_ONLINE_2012"))
    drop dup
    
** We want DSP to be analyzed separately before and after the 3rd National Survey 
	replace source_type = "DSP-1996-2000" if COUNTRY == "CHN" & YEAR >= 1996 & YEAR <= 2000 & source_type == "DSP"
	replace source_type = "DSP-2004-2010" if COUNTRY == "CHN" & YEAR >= 2004 & YEAR <= 2010 & source_type == "DSP"

** We want SRS to be analyzed in five parts
	// This is because the sampling schemes are changed every 10 years or so
	// http://censusindia.gov.in/Vital_Statistics/SRS/Sample_Registration_System.aspx
	replace source_type = "SRS-1970-1976" if COUNTRY == "IND" & YEAR >= 1970 & YEAR <= 1976 & source_type == "SRS"
	replace source_type = "SRS-1976-1982" if COUNTRY == "IND" & YEAR >= 1977 & YEAR <= 1982 & source_type == "SRS"
	replace source_type = "SRS-1983-1992" if COUNTRY == "IND" & YEAR >= 1983 & YEAR <= 1992 & source_type == "SRS"
	replace source_type = "SRS-1993-2003" if regexm(COUNTRY, "IND") & YEAR >= 1993 & YEAR <= 2003 & source_type == "SRS" 
	replace source_type = "SRS-2004-2013" if regexm(COUNTRY, "IND") & YEAR >= 2004 & YEAR <= 2013 & source_type == "SRS" 
	replace source_type = "SRS-2014-2015" if regexm(COUNTRY, "IND") & inrange(YEAR, 2014, 2015) & source_type == "SRS"
	** Drop out all current and urb/rural. Replace current with "Old" in SRS
	drop if inlist(COUNTRY, "IND_4871", "IND_43872", "IND_43902", "IND_43908", "IND_43938") & !inrange(YEAR, 2014, 2015)
	replace COUNTRY = "IND_44849" if COUNTRY == "IND_4841" & !inrange(YEAR, 2014, 2015)

** We want to use national populations for the Pakistan & BanglUSER SRS
	preserve
	keep if (CO == "PAK" | CO == "BGD") & source_type == "CENSUS" 
	replace source_type = "SRS" 
	tempfile add 
	save `add', replace
	restore
	append using `add' 
	
** Indonesia has censuses and SURVEYS. Censuses have no death numbers, but we want to pair them up with our scaled up SUPAS survey, for smoothed comp estimates, so label them as SUPAS. 
** Relabel the 2000 census survey as a SURVEY since it duplicates years with SUSENAS.
	replace source_type = "SUSENAS" if strpos(CENSUS_SOURCE,"SUSENAS") != 0 & regexm(CO, "IDN")
	replace source_type = "SUPAS" if strpos(CENSUS_SOURCE,"SUPAS") != 0 & regexm(CO, "IDN")
	replace source_type = "2000_CENS_SURVEY" if strpos(CENSUS_SOURCE,"SURVEY") != 0 & CO == "IDN" & YEAR == 1999
	replace CENSUS_SOURCE = "2000_CENS_SURVEY" if strpos(CENSUS_SOURCE,"SURVEY") != 0 & CO == "IDN" & YEAR == 1999
	replace source_type = "SUPAS" if CO == "IDN" & strpos(CENSUS_SOURCE,"DYB") != 0
	
** Make the source type of ZAF community survey to be "SURVEY" to match what the deaths file has.  This will help with merging
	replace source_type = "SURVEY" if COU == "ZAF" & source_type == "HOUSEHOLD" & SUBDIV == "SURVEY" & YEAR > 2006 & YEAR < 2007

** if we have data for age 0 and ages 1-4, then drop data for ages 0-4 pooled
    replace DATUM0to4 = . if DATUM0to0 != . & DATUM1to4 != .

** For Zambia, analyze two household sources separately
	replace source_type = "HOUSEHOLD_HHC" if CENSUS_SOURCE == "ZMB_HHC" & COUNTRY == "ZMB"

		
	** **********************
	** Recode countries
	** **********************
	rename COUNTRY iso3
	// Add locations
	replace iso3 = "" if ihme_loc_id != "" // We don't want any issues if iso3 was filled out as national but data is sub or something
	merge m:1 iso3 using `countrycodes', update
	levelsof iso3 if _merge == 1
	levelsof iso3 if _merge == 2
	levelsof iso3 if ihme_loc_id == "" 
	keep if ihme_loc_id != "" & _merge != 2 // If it merged ok or updated, or if the country had ihme_loc_id already but not iso3
	drop _merge iso3
	merge m:1 ihme_loc_id using `countrymaster', update // Update country variable if ihme_loc_id was already present
	keep if ihme_loc_id != "" & _merge != 2
	drop _merge iso3
	
	// Recode all data prepped as China as China Mainland
	replace ihme_loc_id = "CHN_44533" if ihme_loc_id == "CHN"
		
** **********************
** Handle both sexes combined
** **********************
	
** Drop both sexes if the source or footnote does not match the male and female source
	preserve
	keep ihme_loc_id YEAR SEX FOOTNOTE CENSUS_SOURCE source_type
	reshape wide CENSUS_SOURCE FOOTNOTE, i(ihme_loc_id YEAR source_type) j(SEX)
	keep if (CENSUS_SOURCE0 != CENSUS_SOURCE1 | CENSUS_SOURCE0 != CENSUS_SOURCE2 | FOOTNOTE0 != FOOTNOTE1 | FOOTNOTE0 != FOOTNOTE2) & CENSUS_SOURCE0 != "" & FOOTNOTE0 != ""
	drop CENSUS_SOURCE*
	tempfile drop_both
	save `drop_both' 
	restore
	merge m:1 ihme_loc_id YEAR source_type using `drop_both'
	drop if _m == 3 & SEX == 0 
	drop _m 
	
** Calculate both sexes where it is missing 
	preserve
	drop if SEX == 0 
	gen temp = 1
	foreach var of varlist DATUM* { 
		replace `var' = -999 if `var' == . 
	} 
	collapse (sum) DATUM* temp, by(YEAR CENSUS_SOURCE FOOTNOTE MONTH DAY ihme_loc_id country source_type nid)
	foreach var of varlist DATUM* { 
		replace `var' = . if `var' < 0  
	} 	
	keep if temp == 2
	gen SEX = 0
	tempfile both
	save `both'
	restore
	append using `both'
	duplicates tag ihme_loc_id source_type SEX YEAR CENSUS_SOURCE FOOTNOTE MONTH DAY, gen(d)
	drop if d == 1 & temp == 2
	drop d temp 

	tempfile master
	save `master', replace

** **********************
** Format and save the database 
** **********************
	cap replace DATUMUNK = DATUMunk if DATUMUNK == . & DATUMunk != .
	cap drop DATUMunk
	gen sex = "both" if SEX == 0
	replace sex = "male" if SEX == 1
	replace sex = "female" if SEX == 2
	drop SEX

	rename YEAR year
	rename CENSUS_SOURCE pop_source
	rename FOOTNOTE pop_footnote
	rename MONTH month
	rename DAY day
	
	replace nid = NID if nid == . & NID != .
	rename nid pop_nid
	tostring pop_nid, replace
	replace pop_nid = "" if pop_nid == "." | pop_nid == " "
	
	drop if year < 1930
	replace year = floor(year) 
		replace year = 2006.69 if ihme_loc_id == "ZAF" & source_type == "SURVEY" & year == 2006
	
	isid ihme_loc_id sex year source_type
	keep ihme_loc_id country sex year month day source_type pop_source pop_footnote DATUM* pop_nid
	order ihme_loc_id country sex year month day source_type pop_source pop_footnote pop_nid *
	sort ihme_loc_id sex year source_type 
	saveold "$save_file", replace
