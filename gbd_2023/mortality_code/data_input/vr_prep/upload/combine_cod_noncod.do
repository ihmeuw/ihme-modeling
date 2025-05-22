** ***********************************************************************
** Author: Jake Marcus
** Date created: August 10, 2009

** Description: Compiles data on deaths by age and sex from a variety of sources.

**
** NOTE: IHME OWNS THE COPYRIGHT
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
	if (c(os)=="Unix") global root "FILEPATH"
	if (c(os)=="Windows") global root "FILEPATH"

	local new_run_id = ""
	local run_folder = "FILEPATH"
	local input_dir = "FILEPATH"
	local output_dir = "FILEPATH"
	global newdata_dir "FILEPATH"

	import delimited "FILEPATH", clear
	tempfile source_type_ids
	save `source_type_ids', replace

	// Import NID sourcing file, and standardize for merging
	use "FILEPATH", clear
	gen estimate_stage_id = 22

	tempfile deaths_nids_sourcing_22
	save `deaths_nids_sourcing_22', replace

	replace estimate_stage_id = 21
	append using `deaths_nids_sourcing_22'
	tempfile deaths_nids_sourcing
	save `deaths_nids_sourcing', replace


** ***********************************************************************
** Combine non-CoD and CoD VR
** ***********************************************************************
	use "FILEPATH", clear
	append using "FILEPATH"

	// set underlying NID to a string
	tostring underlying_nid, replace

	drop if inlist(COUNTRY, "IND_4871", "IND_43872", "IND_43902", "IND_43908", "IND_43938") & YEAR < 2014 & VR_SOURCE == "WHO_causesofdeath"
	replace CO = "IND_44849" if CO == "IND_4841" & YEAR < 2014 & VR_SOURCE == "WHO_causesofdeath"

	replace COUNTRY = "CHN_354" if COUNTRY == "HKG"

    ** For 2016, aggregate Scotland, Wales, England from COD VR with N Ireland from all cause VR
	** CoD have sources for N Ireland now
	drop if COUNTRY == "GBR" & YEAR == 2016
	preserve
		keep if inlist(COUNTRY, "GBR_4749", "GBR_4636", "GBR_433", "GBR_434") & YEAR == 2016

		sort COUNTRY YEAR SEX estimate_stage_id
		quietly by COUNTRY YEAR SEX estimate_stage_id:  gen dup = cond(_N==1,0,_n)

		** Make sure there aren't any duplicates in the aggregation
		sort COUNTRY SEX YEAR NID estimate_stage_id
		keep if VR_SOURCE == "WHO_causesofdeath"
		quietly by COUNTRY SEX YEAR NID estimate_stage_id: gen gbr_dup = cond(_N==1,0,_n)
		assert gbr_dup == 0
		drop gbr_dup

		** Set the non-aggregated fields
		replace COUNTRY = "GBR"
		replace SUBDIV = "VR"
		replace VR_SOURCE = "WHO_causesofdeath"
		replace NID = 350840
		replace FOOTNOTE = ""
		replace AREA = 0
		replace source_type_id = 1 if source_type_id == .

		** aggregate
		collapse (sum) DATUM*, by(COUNTRY SEX YEAR SUBDIV VR_SOURCE source_type_id FOOTNOTE AREA estimate_stage_id)
        gen NID = 350840
		gen underlying_nid = ""

		foreach var of varlist DATUM* {
			replace `var' = . if (`var' == 0)
		}

		sort COUNTRY YEAR SEX estimate_stage_id
		quietly by COUNTRY YEAR SEX estimate_stage_id:  gen dup = cond(_N==1,0,_n)
		assert dup == 0
		drop dup

		tempfile gbr_2016_aggs
		save `gbr_2016_aggs', replace

	restore
	append using `gbr_2016_aggs'

	gen outlier = 0

** ***********************************************************************
** Prioritize non-CoD vs. CoD VR
** ***********************************************************************
	replace SUBDIV = upper(SUBDIV)
	replace AREA = 0 if AREA == .
	// At this point after reading in the COD VR we have the USA 2010 WHO_causesofdeath points too.
	replace outlier = 1 if regexm(COUNTRY, "USA") & YEAR >= 1959 & YEAR <= 1979 & VR_SOURCE == "WHO_causesofdeath"

	** Removing Duplicates between WHO and COD

		replace outlier = 1 if CO == "GBR" & VR_SOURCE == "WHO_causesofdeath"  & YEAR <= 2013
		replace outlier = 1 if CO == "GBR" & VR_SOURCE == "WHO"

		** replace outlier = 1 RUS 1999-2000
		replace outlier = 1 if CO == "RUS"  & SUBDIV == "VR" & VR_SOURCE == "WHO_causesofdeath" & inrange(YEAR, 1999, 2000)

		** replace outlier = 1 CoD zero points for Sweden and subnationals 1987=1989 in favor of WHO
		replace outlier = 1 if regexm(CO,"SWE") & VR_SOURCE == "WHO_causesofdeath" & YEAR >= 1987 & YEAR <= 1989

		** Ensure we don't overwrite HMD data pre-1979, but keep 2017 Norway
		replace outlier = 1 if COUNTRY=="NOR" & VR_SOURCE == "WHO_causesofdeath" & YEAR < 2017

		** General duplicate removal
		duplicates tag CO YEAR SEX estimate_stage_id if outlier != 1 & (VR_S == "WHO" | VR_S == "WHO_causesofdeath"), g(dup)
		replace dup = 0 if mi(dup)
		replace outlier = 1 if dup != 0 & VR_S == "WHO"
		drop dup

	** Drop Tunisia 2006
	replace outlier = 1 if CO == "TUN" & SUBDIV == "VR" & VR_SOURCE == "WHO_causesofdeath" & YEAR == 2006

	** Drop CoD from Morocco
	replace outlier = 1 if CO == "MAR" & SUBDIV == "VR" & VR_SOURCE == "WHO_causesofdeath" & YEAR >= 2000

	** Drop recent COD deaths in TWN
	replace outlier = 1 if CO == "TWN" & SUBDIV == "VR" & VR_SOURCE == "WHO_causesofdeath" & YEAR > 2010 & YEAR <= 2014

	** drop new MNG points from COD
	replace outlier = 1 if CO == "MNG" & VR_SOURCE == "WHO_causesofdeath" & !inlist(YEAR, 1994, 2010, 2019, 2020, 2021, 2022)

	** drop England pre-1980 from COD
	replace outlier = 1 if CO == "GBR_4749" & VR_SOURCE == "WHO_causesofdeath" & YEAR < 1980

	** drop MDG VR
	replace outlier = 1 if CO == "MDG" & VR_SOURCE == "WHO_causesofdeath"

	** drop specific india states MCCD from COD
	replace outlier = 1 if CO == "IND_43874" & YEAR < 1999 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "IND_43884" & YEAR < 1999 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "IND_43893" & YEAR < 1999 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "IND_43894" & YEAR < 1999 & VR_SOURCE == "WHO_causesofdeath"

	** drop some other new COD points
	replace outlier = 1 if CO == "MOZ" & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "GHA" & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "MLI" & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "CAN" & YEAR == 2012 & VR_SOURCE != "WHO_causesofdeath"

	** drop BLZ, COL, CUB, ECU,GRL,GUM, HUN, KAZ WHO causes of death source
	replace outlier = 1 if CO == "BLZ" & YEAR >= 1999 & YEAR <= 2010 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "COL" & YEAR >= 2012 & YEAR <= 2015 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "CUB" & YEAR >= 2010 & YEAR <= 2015 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "ECU" & YEAR >= 2013 & YEAR <= 2014 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "GRL" & YEAR >= 1995 & YEAR <= 2015 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "GUM" & YEAR == 2004 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "HUN" & YEAR == 2016 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "ISL" & YEAR >= 1981 & YEAR <= 2017 & VR_SOURCE == "WHO_causesofdeath"
	replace outlier = 1 if CO == "KAZ" & YEAR >= 2009 & YEAR <= 2015 & VR_SOURCE == "WHO_causesofdeath"

	// unoutlier the NZL Cod data past 2014
	replace outlier = 0 if (regexm(CO, "NZL") == 1) & VR_SOURCE == "WHO_causesofdeath" & YEAR >= 2014

	tempfile master
	save `master', replace

	import delimited using "FILEPATH", varnames(1) clear
	cap rename year YEAR
	tempfile correction
	save `correction', replace

	use `master', clear
	keep if CO == "PSE" & YEAR > 2007 & YEAR < 2012 & VR_SOURCE == "WHO_causesofdeath"
	merge m:1 YEAR using `correction', keep(3) nogen
	foreach var of varlist DATUM* {
		replace `var' = `var' * scale
	}

	replace FOOTNOTE = "We think these COD points count only the West Bank, so we scaled by (total Palestine deaths / West Bank deaths)"
	drop deaths_p deaths_wb scale
	tempfile corrected
	save `corrected', replace

	use `master', clear
	drop if CO == "PSE" & YEAR > 2007 & YEAR < 2012 & VR_SOURCE == "WHO_causesofdeath"
	append using `corrected'

	** also want to drop <2008 COD PSE data
	replace outlier = 1 if CO == "PSE" & YEAR < 2008 & VR_SOURCE == "WHO_causesofdeath"

** ***********************************************************************
** Add or apply scalars to deaths for non-representativeness
** ***********************************************************************
	** Population-based scalars for Cyprus (N/S Cyprus), Moldova (Transnistria), and Serbia (Kosovo)
	** Scalars are all-age, both-sex
	** Cyprus will be used for all rather than Cyprus death scalars which are more sketchy
	** Serbia will only be used for years that we don't have Serbian data -- e.g. 1998-2008
	tempfile master
	save `master', replace
	insheet using "FILEPATH", comma clear

	rename scalar death_scalar
	keep parent_ihme_loc_id year_id death_scalar

	collapse (mean) death_scalar, by(parent_ihme_loc_id)
	rename parent_ihme_loc_id COUNTRY
	merge 1:m COUNTRY using `master', keep(3) nogen

	** Drop MDA until 1996 as data is representative of all Moldova (MDA plus Transnistria)
	keep if SUBDIV =="VR" | (SUBDIV == "CENSUS" & COUNTRY == "SRB")
	keep if !inlist(COUNTRY, "MDA") | (COUNTRY == "MDA" & YEAR >= 1997)
	levelsof COUNTRY, local(adjust_countries) c

	foreach var of varlist DATUM* {
		replace `var' = `var' * death_scalar
	}

	drop death_scalar
	tempfile scaled_deaths
	save `scaled_deaths'

	use `master', clear
	foreach c in `adjust_countries' {
		di "`c'"
		if "`c'" != "SRB" & "`c'" != "MDA" {
			drop if SUBDIV =="VR" & COUNTRY == "`c'"
		}
		else if "`c'" == "MDA" {
			drop if SUBDIV =="VR" & COUNTRY == "`c'" & YEAR >= 1997
		}
		else if "`c'" == "SRB" {
			drop if (SUBDIV =="VR" | SUBDIV == "CENSUS") & COUNTRY == "`c'"
		}
	}

	append using `scaled_deaths'
	save `master', replace

** ***********************************************************************
** Drop duplicates and other problematic data
** ***********************************************************************
	noisily: display in green "DROP OUTLIERS AND MAKE CORRECTIONS TO THE DATABASE"

** Drop if AREA is urban or rural. We assume missing means national, not urban or rural
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


** We want Korea VR analyzed in two parts since there were two different collection methods
	replace SUBDIV = "VR PRE-1978" if COUNTRY == "KOR" & YEAR <= 1977
	replace SUBDIV = "VR 1978-2000" if COUNTRY == "KOR" & YEAR > 1977 & YEAR < 2000
	replace SUBDIV = "VR POST-2000" if COUNTRY == "KOR" & YEAR >= 2000

** We want China provincial DSP analyzed separately before and after 3rd National survey in 2004
    replace SUBDIV = "DSP<1996" if SUBDIV == "DSP" & VR_SOURCE == "CHN_DSP" & YEAR < 1996
	replace SUBDIV = "DSP 96-04" if SUBDIV == "DSP" & VR_SOURCE == "CHN_DSP" & YEAR >= 1996 & YEAR < 2004
    replace SUBDIV = "DSP>2003" if SUBDIV == "DSP" & VR_SOURCE == "CHN_DSP" & YEAR >= 2004

** We want ZAF data analyzed separately before and after 2002, when it started to become more complete
	replace SUBDIV = "VR PRE-2003" if regexm(COUNTRY,"ZAF") & YEAR <= 2002 & SUBDIV == "VR"
	replace SUBDIV = "VR POST-2003" if regexm(COUNTRY,"ZAF") & YEAR > 2002 & SUBDIV == "VR"
	replace SUBDIV = "VR" if VR_SOURCE == "stats_south_africa" & COUNTRY == "ZAF"

** MEX VR most recent YEARs are a lot more complete
	replace SUBDIV = "VR PRE-2011" if regexm(COUNTRY,"MEX") & YEAR <= 2011 & SUBDIV == "VR"
	replace SUBDIV = "VR POST-2011" if regexm(COUNTRY,"MEX") & YEAR > 2011 & SUBDIV == "VR"

** make manual combination of DYB / COD deaths for DOM 2003-2010
	replace VR_SOURCE = "WHO_causesofdeath+DYB" if COUNTRY == "DOM" & inrange(YEAR, 2003, 2010) & regexm(VR_SOURCE, "DYB|WHO_causesofdeath")

** ***********************************************************************
** General Rule: Keep WHO from causes of deaths over WHO over HMD over DYB
** ***********************************************************************
		duplicates tag COUNTRY YEAR SEX estimate_stage_id if inlist(COUNTRY, "DEU", "TWN", "ESP") & (VR_SOURCE == "HMD" | regexm(VR_SOURCE, "WHO")==1) & outlier != 1, generate(deu_twn_esp_duplicates)
		replace deu_twn_esp_duplicates = 0 if mi(deu_twn_esp_duplicates)
		replace outlier = 1 if COUNTRY == "ESP" & YEAR <= 1973 & deu_twn_esp_duplicates != 0 & VR_SOURCE != "HMD"
		drop deu_twn_esp_duplicates

		replace outlier = 1 if CO == "BRA" & strpos(VR_SOURCE, "WHO")!=0 & YEAR<=1980
		replace outlier = 1 if CO == "ARG" & inlist(YEAR, 1966, 1967) & VR_SOURCE=="WHO"
		replace outlier = 1 if CO == "MYS" & VR_SOURCE=="WHO_causesofdeath" & !inlist(YEAR,2010,2011,2013)
		replace outlier = 1 if CO == "MYS" & VR_SOURCE=="WHO" & (YEAR > 1999 & YEAR < 2009)
		replace outlier = 1 if CO == "KOR" & YEAR >= 1985 & YEAR <= 1995 & VR_SOURCE=="WHO"
		replace outlier = 1 if CO == "FJI" & YEAR==1999 & VR_SOURCE == "WHO_causesofdeath"
		replace outlier = 1 if CO == "PAK" & (YEAR == 1993 | YEAR == 1994) & strpos(VR_SOURCE,"WHO") != 0
		replace outlier = 1 if CO == "BHS" & (YEAR == 1969 | YEAR == 1971) & VR_SOURCE == "WHO_causesofdeath"
		replace outlier = 1 if CO == "EGY" & YEAR == 1954 & VR_SOURCE == "WHO"
		replace outlier = 1 if CO == "EGY" & (YEAR >= 1955 & YEAR <= 1964) & VR_SOURCE == "WHO_causesofdeath"

	** Drop duplicates between online and download
	duplicates tag COUNTRY YEAR SEX SUBDIV estimate_stage_id if strpos(VR_SOURCE, "DYB") != 0, g(dup)
	replace outlier = 1 if VR_SOURCE == "DYB_ONLINE" & dup == 1
	drop dup

	** Drop WHO internal VR if we have other sources
	duplicates tag COUNTRY YEAR SEX estimate_stage_id if regexm(SUBDIV, "VR") & outlier != 1, g(dup)
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & VR_SOURCE == "WHO_internal"
    drop dup

    ** Drop DYB VR if we have other sources
    duplicates tag COUNTRY YEAR SEX estimate_stage_id if regexm(SUBDIV, "VR") & outlier != 1, g(dup)
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & strpos(VR_SOURCE,"DYB") != 0
    drop dup

	** Drop HMD VR if we have other sources
	duplicates tag COUNTRY YEAR SEX estimate_stage_id if regexm(SUBDIV, "VR") & outlier != 1, g(dup)
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & strpos(VR_SOURCE,"HMD") != 0
    drop dup

	** Drop original WHO VR if we have other sources
	duplicates tag COUNTRY YEAR SEX estimate_stage_id if regexm(SUBDIV, "VR") & outlier != 1, g(dup)
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & VR_SOURCE == "WHO"
    drop dup

	** Drop DYB Census if we have other sources
	duplicates tag COUNTRY YEAR SEX estimate_stage_id if SUBDIV == "CENSUS" & outlier != 1, g(dup)
	replace dup = 0 if dup == .
	replace outlier = 1 if dup != 0  & regexm(VR_SOURCE, "DYB")
	drop dup

    duplicates tag CO YEAR SEX estimate_stage_id if regexm(SUBDIV, "VR") & outlier != 1, g(dup)
	replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & VR_SOURCE == "ALAN_LOPEZ"
    replace outlier = 1 if dup !=0 & VR_SOURCE == "VR" & CO == "AUS"
    drop dup

    ** drop USA NCHS data if we have other sources
    duplicates tag CO YEAR SEX estimate_stage_id if regexm(SUBDIV, "VR") & outlier != 1, g(dup)
	replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & CO == "USA" & VR_SOURCE == "NCHS"
    drop dup


	** drop MNG, LBY, GBR duplicates
	duplicates tag COUNTRY YEAR SEX SUBDIV estimate_stage_id if outlier != 1, gen(dup)
	replace outlier = 1 if COUNTRY == "LBY" & VR_SOURCE == "report" & SUBDIV == "VR" & inlist(YEAR, 2006, 2007, 2008) & dup >= 1
	replace outlier = 1 if COUNTRY == "MNG" & VR_SOURCE == "ALAN_LOPEZ_MNG_YEARBOOK" & SUBDIV == "VR" & inlist(YEAR, 2004, 2005) & dup >=1
	replace outlier = 1 if regexm(COUNTRY,"GBR_") & SUBDIV == "VR" & !regexm(VR_SOURCE, "WHO") & dup > 0
	drop dup

	** drop LTU duplicates: we have WHO (causes of death) for 2010
	duplicates tag COUNTRY YEAR SEX SUBDIV estimate_stage_id, gen(dup)
	replace outlier = 1 if dup >= 1 & COUNTRY == "LTU" & YEAR == 2010 & SUBDIV == "VR" & VR_SOURCE == "135808#LTU COD REPORT 2010"
	replace outlier = 1 if dup >= 1 & COUNTRY == "LTU" & YEAR == 2011 & SUBDIV == "VR" & VR_SOURCE == "135810#LTU COD REPORT 2011"
	replace outlier = 1 if dup >= 1 & COUNTRY == "LTU" & YEAR == 2012 & SUBDIV == "VR" & VR_SOURCE == "135811#LTU COD REPORT 2012"
	replace outlier = 1 if YEAR < 1974 & CO == "CYP"
	drop dup

	** Additional drops to get GBD2017 consistency
	replace outlier = 1 if COUNTRY == "IRN" & VR_SOURCE == "WHO_causesofdeath" & YEAR == 2001

	** Drop Canada source and use WHO VR
	duplicates tag COUNTRY YEAR SEX SUBDIV estimate_stage_id, gen(dup)
	replace outlier = 1 if dup >= 1 & COUNTRY == "CAN" & (YEAR == 2010 | YEAR == 2011) & SUBDIV == "VR" & VR_SOURCE == "STATISTICS_CANADA_VR_121924"

    ** Drop Chile source and use WHO VR
	replace outlier = 1 if dup >= 1 & COUNTRY == "CHL" & YEAR == 2011 & SUBDIV == "VR"	& VR_SOURCE == "CHL_MOH"

    ** Drop China DC survey: Duplicate of China 1 % survey
	replace outlier = 1 if dup >= 1 & COUNTRY == "CHN" & YEAR == 2005 & SUBDIV == "DC" & VR_SOURCE == "DC"
    ** Drop China DSP duplicates -- we will keep the 1991-1995 data from CHN_DSP
	replace outlier = 1 if dup >= 1 & COUNTRY == "CHN" & YEAR >= 1996 & SUBDIV == "DSP" & VR_SOURCE == "CHN_DSP"
	replace outlier = 1 if COUNTRY == "CHN_44533" & YEAR >= 1996 & YEAR <=2000 & VR_SOURCE == "CHN_DSP"
	drop dup
    ** Fix age group naming for youngest age groups
	cap replace DATUM0to0 = DATUM0to1 if DATUM0to0 == . & DATUM0to1 != . & (DATUM1to4 != . | DATUM0to4 != .)
	cap replace DATUM0to1 = . if DATUM0to0 != . & (DATUM1to4 != . | DATUM0to4 != .)

    ** Relabel source_type for surveys that overlap time-wise but have same source type
	replace SUBDIV = "HHC" if VR_SOURCE == "ZMB_HHC" & COUNTRY == "ZMB"

    ** drop VR in 2009 in LKA from S_Dharmaratne because it duplicates data in the COD file
	replace outlier = 1 if COUNTRY=="LKA" & YEAR==2009 & VR_SOURCE=="S_Dharmaratne"

    ** Fix categorization of Indonesia DYB -- should be a Census rather than a VR point
	replace SUBDIV = "Census" if CO == "IDN" & YEAR == 2010 & SUBDIV == "VR"

    ** Additional drops to get GBD2017 consistency
	replace outlier = 1 if COUNTRY == "IRN" & VR_SOURCE == "WHO_causesofdeath" & YEAR == 2001
	replace outlier = 1 if COUNTRY == "CHL" & inlist(VR_SOURCE, "WHO", "HMD") & YEAR == 1993


** ***********************************************************************
** Format the database
** ***********************************************************************

	** Rename variables
	rename SEX sex
	rename COUNTRY ihme_loc_id
	rename YEAR year
	rename NID deaths_nid
	rename underlying_nid deaths_underlying_nid
	rename FOOTNOTE deaths_footnote
	rename SUBDIV source_type
	rename VR_SOURCE deaths_source
	gen country = substr(ihme_loc_id,1,3)

	// Recode all data prepped as China as China Mainland
	replace ihme_loc_id = "CHN_44533" if ihme_loc_id == "CHN"

	order ihme_loc_id country sex year deaths_source source_type deaths_footnote estimate_stage_id *

	replace deaths_source = "DYB" if regexm(deaths_source, "DYB_")

    ** if we have data for age 0 and ages 1-4, then drop data for ages 0-4 pooled
    cap replace DATUM0to4 = . if DATUM0to0 != . & DATUM1to4 != .

    duplicates tag ihme_loc_id year sex source_type estimate_stage_id, gen(dup2)
	replace outlier = 1 if regexm(ihme_loc_id , "GBR_") & !regexm(deaths_source, "WHO") & dup2 != 0
	drop dup2

	// Drop StatCan duplicates in favor of WHO VR
	drop if ihme_loc_id == "CAN" & year == 2013 & deaths_source == "StatCan"

	//USA 2010 mark as not outliers
	replace outlier = 0 if ihme_loc_id == "USA" & year == 2010 & deaths_source == "WHO"

	drop if ihme_loc_id == "ZAF" & year == 2011 & deaths_source == "12146#stats_south_africa 2011 census"

	** Egypt 1958 DYB
	replace outlier = 1 if ihme_loc_id == "EGY" & year == 1958 & deaths_source == "DYB"

	** Russia 1958 IDEM data
	replace outlier = 1 if ihme_loc_id == "RUS" & year == 1958 & deaths_nid == 336438

	** competing sources in NZL locs 2015 and 2016
	replace outlier = 1 if (ihme_loc_id == "NZL_44851" | ihme_loc_id == "NZL_44850" | ihme_loc_id == "NZL") & (year == 2015 | year==2016) & deaths_source == "NZL_VR_2015-2018"

	** competing sources in MDA year 2016
	replace outlier = 1 if ihme_loc_id == "MDA" & year == 2016 & deaths_source == "WHO_causesofdeath"

	** competing sources in Greece year 2015
	replace outlier = 1 if ihme_loc_id == "GRC" & year == 2015 & deaths_source != "WHO_causesofdeath"

	// outlier NLD 2016 all cause source
	replace outlier = 1 if ihme_loc_id == "NLD" & year == 2016 & deaths_source == "Netherlands_VR_all_cause"

	** competing sources in TUR year 2014-2016
	replace outlier = 1 if ihme_loc_id == "TUR" & (year == 2014 | year == 2015 | year == 2016) & deaths_source == "WHO_causesofdeath"

	** competing sources in SYC year 2015
	replace outlier = 1 if ihme_loc_id == "SYC" & year == 2015 & deaths_source == "WHO_causesofdeath"

	** competing sources in BRA subnationals in 2016
	replace outlier = 1 if regexm(ihme_loc_id, "BRA") & year == 2016 & deaths_source == "WHO_causesofdeath"

	** competing sources in ARG in 2015
	replace outlier = 1 if ihme_loc_id == "ARG" & year == 2015 & deaths_source == "Argentina_VR_all_cause"

	** competing sources in USA (national) in 2010
	replace outlier = 1 if ihme_loc_id == "USA" & year == 2010 & deaths_source == "WHO"

	** competing sources in USA (national) in 2015
	replace outlier = 1 if ihme_loc_id == "USA" & year == 2015 & deaths_source == "CDC_report"

	** competing sources in HRV (Croatia) in 2015
	replace outlier = 1 if ihme_loc_id == "HRV" & year == 2015 & deaths_source == "Croatia_allcause_VR"

	** competing sources in CHE (Switzerland) in 2015
	replace outlier = 1 if ihme_loc_id == "CHE" & year == 2015 & deaths_source == "WHO_causesofdeath"

	** 2 sources for ARG, ARG_VR_Allcause and WHO cod, outlier all cause
	replace outlier=1 if ihme_loc_id == "ARG" & (year==2009 | year==2010 | year==2013) & deaths_source == "ARG_VR_Allcause"

	** 2 sources for AUS, AUS_VR_Allcause and WHO Cause of Death
	replace outlier= 1 if ihme_loc_id == "AUS" & (year>= 2000 & year <= 2014) & deaths_source == "AUS_VR_Allcause"

	** 2 sources for Bahrain, BHR Health Statistics and WHO causesofdeath, outlier all cause
	replace outlier= 1 if ihme_loc_id == "BHR" & (year>= 2000 & year <= 2014) & deaths_source == "BHR Health Statistics"

	** 2 sources for Canada, CAN_CANSIM and WHO causesofdeath, outlier all cause
	replace outlier= 1 if ihme_loc_id == "CAN" & (year>= 2010 & year <= 2013) & deaths_source == "CAN_CANSIM_DEATHS_BY_AGE_SEX"

	** 2 sources for Bosnia, BIH demography and health statistics and WHO, outlier all cause
	replace outlier= 1 if ihme_loc_id == "BIH" & (year== 2011 | year == 2014) & deaths_source=="BIH_Demography_and_Social_Statistics_2015"

	replace outlier = 1 if regexm(ihme_loc_id, "BRA") & year == 2016
	replace outlier = 0 if regexm(ihme_loc_id, "BRA") & year == 2016 & deaths_source == "Brazil_SIM_ICD10_allcause"

    ** CHN and CHN subnationals
	replace outlier = 1 if regexm(ihme_loc_id, "CHN") & deaths_source == "CHN_DSP" & year >= 2004 & source_type == "VR"

    ** HONG KONG
	replace outlier = 1 if ihme_loc_id == "CHN_354" & deaths_source != "WHO_causesofdeath" & year >= 1979 & year <= 2000
	replace outlier = 0 if ihme_loc_id == "CHN_354" & deaths_source == "WHO_causesofdeath" & year >= 1979 & year <= 2000

	replace outlier = 1 if ihme_loc_id == "CHN_354" & deaths_source != "WHO" & year > 2000 & year <= 2017
	replace outlier = 0 if ihme_loc_id == "CHN_354" & deaths_source == "WHO" & year > 2000 & year <= 2017

	// keep country specific source for 2019
	replace outlier = 0 if ihme_loc_id == "CHN_354" & year >= 2018 & deaths_nid == 538727

    ** MACAO
	replace outlier = 1 if ihme_loc_id == "CHN_361" & year == 1994
	replace outlier = 0 if ihme_loc_id == "CHN_361" & year == 1994 & deaths_source == "DYB"

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

	count if regexm(ihme_loc_id, "CHN") & ihme_loc_id != "CHN_361" & ihme_loc_id != "CHN_354" & deaths_source == "WHO_causesofdeath"
	drop if regexm(ihme_loc_id, "CHN") & ihme_loc_id != "CHN_361" & ihme_loc_id != "CHN_354" & deaths_source == "WHO_causesofdeath"

	replace outlier = 1 if ihme_loc_id == "JOR" & year <= 1967

	/*
	Need to make sure that CHN DSP 2016 is outliered
	*/
	replace outlier = 1 if regexm(ihme_loc_id, "CHN") & year == 2016 & deaths_source == "CHN_DSP"

	replace outlier = 1 if ihme_loc_id == "ALB" & year == 2010 & deaths_source == "WHO_causesofdeath"
	replace outlier = 1 if ihme_loc_id == "DZA" & inlist(year, 2005, 2006) & deaths_source == "WHO_causesofdeath"
	replace outlier = 1 if ihme_loc_id == "SYR" & year == 1980 & deaths_source == "WHO_causesofdeath"
	replace outlier = 1 if ihme_loc_id == "ZWE" & year == 2007 & deaths_source == "WHO_causesofdeath"
	replace outlier = 1 if ihme_loc_id == "BRA_4764" & deaths_nid == 153001 & year == 1979

	// want COD VR MEX and not all cause MEX
	replace outlier = 1 if regexm(ihme_loc_id, "MEX") & year == 2016 & deaths_source == "MEX_VR_all_cause"
	replace outlier = 0 if regexm(ihme_loc_id, "MEX") & year == 2016 & deaths_source == "WHO_causesofdeath"

	** Replace MCCD source type to own source type to prevent duplicates conflicts with IND CRS system which is also VR
	replace source_type = "MCCD" if deaths_source == "WHO_causesofdeath" & source_type == "VR" & regexm(ihme_loc_id, "IND")

	//all-cause vr outlier
	replace outlier = 1 if ihme_loc_id == "ARM" & year == 1988 & deaths_source == "WHO_causesofdeath"
	replace outlier = 1 if ihme_loc_id == "PRY" & year == 1992 & deaths_source == "DYB"
	replace outlier = 1 if ihme_loc_id == "VIR" & year == 1990
	replace outlier = 1 if ihme_loc_id == "VIR" & year == 2015 & deaths_source == "WHO_causesofdeath"
	replace outlier = 1 if ihme_loc_id == "MRT" & deaths_source == "WHO_causesofdeath"
	replace outlier = 1 if ihme_loc_id == "TUN" & inlist(year, 2009, 2013)
	replace outlier = 1 if ihme_loc_id == "ASM" & year == 1957
	replace outlier = 1 if ihme_loc_id == "JOR" & year <= 1967

    ** Drop allcause sources and use COD ones for new locations
	replace outlier = 1 if ihme_loc_id == "KNA" & deaths_source != "WHO_causesofdeath" & (inrange(year, 1979, 1995) | inrange(year, 1998, 2012) | inrange(year, 2014, 2015))
	replace outlier = 1 if ihme_loc_id == "MCO" & deaths_source != "WHO_causesofdeath" & inrange(year, 1986, 1987)
	replace outlier = 1 if regexm(ihme_loc_id, "POL") & deaths_source != "WHO_causesofdeath" & inrange(year, 1988, 1996)
	replace outlier = 1 if ihme_loc_id == "SMR" & deaths_source != "WHO_causesofdeath" & (inrange(year, 1995, 2000) | year == 2002 | year == 2005)
	replace outlier = 1 if ihme_loc_id == "COK" & deaths_source == "COK_ICD10_tab" & inrange(year, 2001, 2016)

    ** Use non-cod SWE 2020
    replace outlier = 1 if ihme_loc_id == "SWE" & year == 2020 & deaths_nid == 486907

    ** Bulk recodes of source_type to standardize source_type_id prior to merges
	replace source_type = lower(source_type)
	replace source_type = "other" if source_type == "survey"

    ** NZL_44850 NZL_44851 drop all VR before 1996
	replace outlier = 1 if inlist(ihme_loc_id, "NZL_44850", "NZL_44851") & year < 1996

	** Outlier GRL 1979-1987 deaths for being too high
	replace outlier = 1 if ihme_loc_id=="GRL" & year >= 1979 & year <= 1987 & deaths_source=="DYB"

	** Outlier Nat Capital Region pre-1980 VR
	replace outlier=1 if ihme_loc_id=="PHL_53614" & year < 1980 & deaths_source=="Philippines Vital Statistics Report"

	** For slovenia between 1985/1990, make sure 85+ is the terminal age and not overwritten by 95+ or 90+
	replace DATUM95plus = . if inrange(year, 1985, 1990) & ihme_loc_id=="SVN" & deaths_source=="WHO_causesofdeath" & DATUM85plus > 0 & DATUM85plus != .
	replace DATUM90plus = . if inrange(year, 1985, 1990) & ihme_loc_id=="SVN" & deaths_source=="WHO_causesofdeath" & DATUM85plus > 0 & DATUM85plus != .


	** Utilize previous nonCoD VR over new CoD VR
	replace outlier = 1 if deaths_source=="WHO_causesofdeath" & ihme_loc_id=="GBR_433" & (inrange(year, 1968, 1980))
	replace outlier = 0 if deaths_source != "WHO_causesofdeath" & ihme_loc_id=="GBR_433" & (inrange(year, 1968, 1980))

	** Outlier new AFR DYB
	replace outlier = 1 if deaths_nid == 426273 & (inlist(ihme_loc_id, "EGY", "COG", "KEN", "ZMB", "LSO", "NAM") | inlist(ihme_loc_id, "ZAF", "GHA", "GIN", "MLI", "STP", "SLE"))

** Unoutlier WHO causes of death for ESP 2018 and 2019
replace outlier = 0 if country == "ESP" & deaths_source == "WHO_causesofdeath" & inrange(year, 2018, 2019)

** use noncod sources for QAT where they have higher terminal ages than cod
replace outlier = 0 if country == "QAT" & deaths_nid == 140201 & year == 1985
replace outlier = 1 if country == "QAT" & deaths_nid == 68829 & year == 1985
replace outlier = 0 if country == "QAT" & deaths_nid == 140966 & (year == 2000 | year == 2002 | year == 2003)
replace outlier = 1 if country == "QAT" & deaths_nid == 69913 & year == 2000
replace outlier = 1 if country == "QAT" & deaths_nid == 69918 & year == 2002
replace outlier = 1 if country == "QAT" & deaths_nid == 69922 & year == 2003
replace outlier = 0 if country == "QAT" & deaths_nid == 426273 & year == 2017
replace outlier = 1 if country == "QAT" & deaths_nid == 408429 & year == 2017
replace outlier = 0 if country == "QAT" & deaths_nid == 478337 & year == 2019
replace outlier = 1 if country == "QAT" & deaths_nid == 481820 & year == 2019

** Use noncod source for MNE over COD 2017 & 2019
replace outlier = 0 if country == "MNE" & deaths_nid == 325106 & year == 2017
replace outlier = 1 if country == "MNE" & deaths_nid == 497759 & year == 2017
replace outlier = 0 if country == "MNE" & deaths_nid == 325106 & year == 2019
replace outlier = 1 if country == "MNE" & deaths_nid == 497759 & year == 2019

** Use noncod source for MNG over COD 2019-20
replace outlier = 0 if country == "MNG" & deaths_nid == 401084 & year == 2019
replace outlier = 1 if country == "MNG" & deaths_nid == 470696 & year == 2019
replace outlier = 0 if country == "MNG" & deaths_nid == 401084 & year == 2020
replace outlier = 1 if country == "MNG" & deaths_nid == 470697 & year == 2020

** outlier GBR_433
replace outlier = 1 if ihme_loc_id == "GBR_433" & year == 1979

** make sure CoD LUX 2018 is unoutliered
replace outlier = 1 if country == "LUX" & deaths_nid == 400963 & year == 2018
replace outlier = 0 if country == "LUX" & deaths_nid == 521161 & year == 2018

** outlier PSE 2021
replace outlier = 1 if country == "PSE" & deaths_nid == 530175 & year == 2021

** replace cod DEU 2020-2022 with noncod
replace outlier = 1 if country == "DEU" & deaths_nid == 492345 & year == 2020
replace outlier = 0 if country == "DEU" & deaths_nid == 400760 & year == 2020
replace outlier = 1 if country == "DEU" & deaths_nid == 527931 & year == 2021
replace outlier = 0 if country == "DEU" & deaths_nid == 400760 & year == 2021
replace outlier = 1 if country == "DEU" & deaths_nid == 542661 & year == 2022
replace outlier = 0 if country == "DEU" & deaths_nid == 400760 & year == 2022

** replace cod DNK 2021 with HMD
replace outlier = 1 if country == "DNK" & deaths_nid == 544122 & year == 2021
replace outlier = 0 if country == "DNK" & deaths_nid == 465016 & year == 2021

** ***********************************************************************
** Format and save
** ***********************************************************************
replace source_type = lower(source_type)
replace source_type = "other" if source_type == "survey"
tostring deaths_nid, replace
tostring sex, replace
replace sex = "male" if sex == "1"
replace sex = "female" if sex == "2"
replace sex = "both" if sex == "0"

tempfile prepped_source
save `prepped_source'

// Merge on source_type_id
rename source_type_id source_type_id_noncod
merge m:1 source_type using `source_type_ids', keep(1 3) nogen
replace source_type_id_noncod = source_type_id if source_type_id_noncod == .
assert source_type_id_noncod !=.
drop source_type_id
rename source_type_id_noncod source_type_id

if r(N) > 0 {
		di in red "The following source types do not exist in the source type map:"
		levelsof source_type if source_type_id == . , c
		break
}

assert source_type_id !=.

drop AREA source_type

tempfile check_dup
save `check_dup'

keep if outlier==1
tempfile outliers
save `outliers'


// keep CoD data by default
use `check_dup',clear
keep if outlier != 1
sort ihme_loc_id year sex source_type_id estimate_stage_id
quietly by ihme_loc_id year sex source_type_id estimate_stage_id:  gen dup = cond(_N==1,0,_n)
list ihme_loc_id year deaths_source if dup > 0
replace outlier = 1 if dup > 0 & deaths_source != "WHO_causesofdeath"
drop dup

		** add nids from sourcing
		rename deaths_nid deaths_nid_orig
		rename deaths_underlying_nid deaths_underlying_nid_orig

		// deaths_nid and deaths_underlying_nid are the nids from deaths_nids_sourcing file
		merge 1:1 ihme_loc_id sex year source_type_id estimate_stage_id deaths_source using `deaths_nids_sourcing', keep(1 3)
		count if _m == 3
		drop _merge

		replace deaths_nid  = deaths_nid_orig if deaths_nid == ""
		count if deaths_nid == ""

		replace deaths_underlying_nid  = deaths_underlying_nid_orig if deaths_underlying_nid == ""

		// use original nid/underlyingnid if there's deaths_underlying_nid_orig available
		replace deaths_nid  = deaths_nid_orig if deaths_nid_orig != "" & deaths_underlying_nid_orig != ""
		replace deaths_underlying_nid  = deaths_underlying_nid_orig if deaths_nid_orig != "" & deaths_underlying_nid_orig != ""

		//use deaths_nid_orig if it's different than deaths_nid
		replace deaths_nid  = deaths_nid_orig if deaths_nid_orig!= deaths_nid & deaths_source == "WHO_causesofdeath"

		replace deaths_nid = "121922" if deaths_nid == "237478"
		replace deaths_nid = "237659" if deaths_nid == "237681"
		replace deaths_nid = "331136" if deaths_source=="SRS" & year==2016

		preserve
		keep ihme_loc_id country sex year estimate_stage_id deaths_source deaths_nid_orig deaths_underlying_nid_orig deaths_nid deaths_underlying_nid
		destring deaths_nid_orig deaths_underlying_nid_orig deaths_nid deaths_underlying_nid, replace
		gen same_nid = ( deaths_nid_orig == deaths_nid )
		gen same_underlyingnid = ( deaths_underlying_nid_orig == deaths_underlying_nid)
		tab same_nid same_underlyingnid
		saveold "`output_dir'/check_nid_change.dta", replace
		restore

		drop deaths_nid_orig deaths_underlying_nid_orig


append using `outliers'

** make sure CHN DSP 2018-2021 is un-outliered
replace outlier = 0 if deaths_nid == "469163"
replace outlier = 0 if deaths_nid == "469165"
replace outlier = 0 if deaths_nid == "528380"
replace outlier = 0 if deaths_nid == "528381"

// No duplicates across unique identifiers or, for VR, no duplicates across location/year/sex
preserve
keep if outlier != 1
sort ihme_loc_id year sex source_type_id estimate_stage_id
quietly by ihme_loc_id year sex source_type_id estimate_stage_id:  gen dup = cond(_N==1,0,_n)
list ihme_loc_id year deaths_source if dup > 0
assert dup == 0

restore

tempfile compiled_deaths
//convert to string to append outliered data
tostring deaths_underlying_nid, replace
save `compiled_deaths'

// bring in outliered data that was outliered early in the non-cod part of this process.
use "FILEPATH", clear
replace country = substr(ihme_loc_id,1,3)
append using `compiled_deaths'

** Combine DATUM1to1 and DATUM12to23
replace DATUM12to23 = DATUM1to1 if DATUM12to23 == . & DATUM1to1 != .
drop DATUM1to1

// 2. When missing or 0, sum up enn/lnn/pnn deaths to get 0-1
replace DATUM0to0 = DATUMenntoenn + DATUMlnntolnn + DATUMpnatopna + DATUMpnbtopnb if (DATUM0to0 == . | DATUM0to0 == 0) & DATUMenntoenn != . & DATUMlnntolnn != . & DATUMpnatopna != . & DATUMpnbtopnb != .

//Null checks for NID and other key variables
assert deaths_nid != "" if outlier == 0
assert ihme_loc_id != ""
assert sex != ""

saveold "FILEPATH", replace

exit, clear STATA
