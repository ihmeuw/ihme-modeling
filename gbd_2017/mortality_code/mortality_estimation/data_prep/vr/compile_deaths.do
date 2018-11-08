** ***********************************************************************
** Author:
** Description: Compiles data on deaths by age and sex from a variety of sources.
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

local new_run_id = "`1'"
local run_folder = "FILEPATH"
local input_dir = "FILEPATH"
local output_dir = "FILEPATH"

** ***********************************************************************
** Filepaths 
** ***********************************************************************

    local date = c(current_date)
    global rawdata_dir "FILEPATH"
    global dyb_dir "FILEPATH"
    global newdata_dir "FILEPATH"
    global hhdata_dir "FILEPATH"
    global process_inputs "FILEPATH"


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

    expand 2 if regexm(ihme_loc_id,"_") & iso3 != "", gen(new)
    replace iso3 = ihme_loc_id if new == 1 
    drop new

    replace iso3 = ihme_loc_id if iso3 == "" 
    tempfile countrycodes
    save  `countrycodes', replace

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

    import delimited using "FILEPATH", clear
    split ihme_loc_id, parse("_")
    rename ihme_loc_id1 parent_loc_id
    keep ihme_loc_id parent_loc_id
    bysort parent_loc_id: gen num_locs = _N
    keep ihme_loc_id parent_loc_id num_locs
    replace parent_loc_id = "CHN_44533" if parent_loc_id == "CHN" 
    tempfile subnat_locs
    save `subnat_locs'

    import delimited using "FILEPATH", clear
    keep if regexm(ihme_loc_id, "GBR")
    keep ihme_loc_id parent_id 
    gen parent_loc_id = "GBR_" + string(parent_id)
    drop parent_id
    tempfile gbr_locs 
    save `gbr_locs', replace 

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

    use "FILEPATH", clear
    replace SUBDIV = "VR"
    gen outlier = .
    replace outlier = 1 if CO == "HKG" & VR_SOURCE == "WHO" & YEAR >= 1969  

    append using "FILEPATH"

    replace outlier = 1 if regexm(COUNTRY, "USA") & YEAR >= 1959 & YEAR <= 1979 & VR_SOURCE == "WHO_causesofdeath"

    replace outlier = 1 if CO == "GBR" & VR_SOURCE == "WHO_causesofdeath"  & YEAR <= 2013
    replace outlier = 1 if CO == "GBR" & VR_SOURCE == "WHO" 
    replace outlier = 1 if CO == "RUS"  & SUBDIV == "VR" & VR_SOURCE == "WHO_causesofdeath" & inrange(YEAR, 1999, 2000)
    replace outlier = 1 if regexm(CO,"SWE") & VR_SOURCE == "WHO_causesofdeath" & YEAR >= 1987 & YEAR <= 1989

    duplicates tag CO YEAR SEX if outlier != 1 & (VR_S == "WHO" | VR_S == "WHO_causesofdeath"), g(dup)
    replace dup = 0 if mi(dup)
    replace outlier = 1 if dup != 0 & VR_S == "WHO"
    drop dup

    replace outlier = 1 if CO == "MMR" & SUBDIV == "VR"
    replace outlier = 1 if CO == "BHS" & SUBDIV == "VR" & VR_SOURCE == "WHO" & YEAR == 1969
    replace outlier = 1 if CO == "DOM" & SUBDIV == "VR" & VR_SOURCE == "WHO" & inlist(YEAR, 2010, 2009, 2008, 2007)
    replace outlier = 1 if CO == "TUN" & SUBDIV == "VR" & VR_SOURCE == "WHO_causesofdeath" & YEAR == 2006
    replace outlier = 1 if CO == "MAR" & SUBDIV == "VR" & VR_SOURCE == "WHO_causesofdeath" & YEAR >= 2000
    replace outlier = 1 if CO == "TWN" & SUBDIV == "VR" & VR_SOURCE == "WHO_causesofdeath" & YEAR > 2010 & YEAR <= 2014
    replace outlier = 1 if CO == "MNG" & VR_SOURCE == "WHO_causesofdeath" & !inlist(YEAR, 1994, 2010)
    replace outlier = 1 if CO == "GBR_4749" & VR_SOURCE == "WHO_causesofdeath" & YEAR < 1980
    replace outlier = 1 if CO == "MDG" & VR_SOURCE == "WHO_causesofdeath"
    replace outlier = 1 if CO == "IND_43874" & YEAR < 1999 & VR_SOURCE == "WHO_causesofdeath" 
    replace outlier = 1 if CO == "IND_43884" & YEAR < 1999 & VR_SOURCE == "WHO_causesofdeath"
    replace outlier = 1 if CO == "IND_43893" & YEAR < 1999 & VR_SOURCE == "WHO_causesofdeath"
    replace outlier = 1 if CO == "IND_43894" & YEAR < 1999 & VR_SOURCE == "WHO_causesofdeath"
    replace outlier = 1 if CO == "MOZ" & VR_SOURCE == "WHO_causesofdeath"
    replace outlier = 1 if CO == "GHA" & VR_SOURCE == "WHO_causesofdeath"
    replace outlier = 1 if CO == "MLI" & VR_SOURCE == "WHO_causesofdeath"

    preserve 
        keep if regexm(COUNTRY, "SAU_")
        if _N > 0 {
            collapse (sum) DATUM*, by(SEX YEAR AREA SUBDIV VR_SOURCE NID FOOTNOTE)
            gen COUNTRY = "SAU"
            replace FOOTNOTE = "aggregated sau subnationals to national"
            foreach var of varlist DATUM* {
                replace `var' =. if `var' == 0
            }
            tempfile sau 
            save `sau', replace 
            restore
            drop if COUNTRY == "SAU"
            append using `sau'
        }
        else {
            restore
        }

    preserve 
        keep if regexm(COUNTRY, "GBR_") & !inlist(COUNTRY, "GBR_4749", "GBR_4636", "GBR_433", "GBR_434") 
        rename COUNTRY ihme_loc_id
        merge m:1 ihme_loc_id using `gbr_locs', keep(3) nogen
        collapse (sum) DATUM*, by(parent_loc_id SEX YEAR SUBDIV VR_SOURCE NID FOOTNOTE AREA)
        rename parent_loc_id COUNTRY

        foreach var of varlist DATUM* {
            count if `var' == 0
            if `r(N)' == _N drop `var'
        }
        tempfile gbr_regs
        save `gbr_regs', replace

    restore
    append using `gbr_regs'

** WHO Internal Database
    append using "FILEPATH"

** Demographic Yearbook
    preserve
    use "FILEPATH", clear
    drop if COUNTRY == "SSD" & AREA == .
    drop NID
    gen NID = 140966 
    replace NID = 140201 if VR_SOURCE == "DYB_CD"
    tempfile internal
    save `internal', replace
    restore
    append using `internal'

    replace outlier = 1 if VR_SOURCE == "DYB_INTERNAL" & DATUM0to0 == . & DATUM0to4 == .
    replace outlier = 1 if (CO == "GHA" | CO == "KEN") & SUBDIV == "VR" & regexm(VR_SOURCE, "DYB") 
    replace outlier = 1 if CO == "MMR" & SUBDIV == "VR" & YEAR < 2000 & regexm(VR_SOURCE, "DYB") 
    replace outlier = 1 if inlist(CO, "GNQ", "AGO", "MOZ", "CAF", "MLI", "TGO", "GNB")==1 & SUBDIV == "VR" & regexm(VR_SOURCE, "DYB") 
    replace outlier = 1 if CO == "DOM" & VR_SOURCE == "DYB_ONLINE" & YEAR == 2011
    replace outlier = 1 if CO == "SAU" & regexm(VR_SOURCE, "DYB") & SUBDIV == "VR"
    replace outlier = 1 if CO == "SDN" & regexm(VR_SOURCE, "DYB") & SUBDIV == "CENSUS" & YEAR == 1993
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
    replace outlier = 1 if COUNTRY == "CHN" & YEAR == 1999 & SUBDIV == "CENSUS" & VR_SOURCE == "DYB_ONLINE"


    duplicates tag COUNTRY YEAR VR_SOURCE SUBDIV if COUNTRY == "CHN_361" & YEAR == 2012 & outlier != 1, generate(dup)
    replace dup = 0 if mi(dup)
    replace outlier = 1 if dup >= 1 & CO == "CHN_361"
    drop dup

    replace outlier = 1 if inlist(CO, "CHN_354", "HKG") & regexm(VR_SOURCE, "DYB") & SUBDIV == "VR" & YEAR > 1954
    replace outlier = 1 if CO == "CHN_44533" & YEAR == 2010 & regexm(VR_SOURCE, "DYB") & SUBDIV == "CENSUS"
    replace outlier = 1 if CO == "ALB" & YEAR == 2013 & regexm(VR_SOURCE, "DYB")

    preserve

** Human Mortality Database
    use "FILEPATH", clear
    generate outlier = 0
    replace outlier = 1 if inlist(COUNTRY,"XNI","XSC") & SUBDIV == "VR" & VR_SOURCE == "HMD"
    replace outlier = 1 if CO == "BEL" & YEAR > 2000 & VR_SOURCE == "HMD"
    replace outlier = 1 if CO == "LVA" & YEAR == 1959 & VR_SOURCE == "HMD"

    tempfile hmd
    save `hmd', replace
    restore
    append using `hmd'

** Data from 
    append using "FILEPATH"
    append using "FILEPATH"
    append using "FILEPATH"

** OECD database
    preserve
    use "FILEPATH", clear
    replace NID = 18447 if inlist(COUNTRY, "ISR", "AGO", "DZA")
    tempfile oecd
    save `oecd', replace
    restore
    append using `oecd'

    replace outlier = 1 if CO == "IRN" & VR_SOURCE == "OECD"  
    replace outlier = 1 if CO == "JAM" & VR_SOURCE == "OECD" 
    replace outlier = 1 if CO == "NPL" & VR_SOURCE == "OECD" 
    replace outlier = 1 if CO == "MAR" & VR_SOURCE == "OECD" 
    replace outlier = 1 if CO == "SUR" & VR_SOURCE == "OECD" 
    replace outlier = 1 if CO == "SYC" & VR_SOURCE == "OECD" & YEAR == 1974 
    replace outlier = 1 if CO == "MDG" & VR_SOURCE == "OECD"
    replace outlier = 1 if CO == "KEN" & VR_SOURCE == "OECD"    
    replace outlier = 1 if CO == "DZA" & VR_SOURCE == "OECD" 
    replace outlier = 1 if CO == "TCD" & VR_SOURCE == "OECD" 
    replace outlier = 1 if CO == "TUN" & VR_SOURCE == "OECD" 
    replace outlier = 1 if CO == "TGO" & VR_SOURCE == "OECD" 
    replace outlier = 1 if CO == "CPV" & VR_SOURCE == "OECD"
    replace outlier = 1 if CO == "AGO" & VR_SOURCE == "OECD" 
    replace outlier = 1 if CO == "SLV" & VR_SOURCE == "OECD"    
    replace outlier = 1 if CO == "MOZ" & VR_SOURCE == "OECD"
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
    replace SUBDIV = "SURVEY" if CO == "ZAF" & YEAR == 2007 & VR_SOURCE == "IPUMS_HHDEATHS"

** ************
** Country-Specific sources
** ************

** ARE: VR
    append using "FILEPATH"

** AUS: VR
    append using "FILEPATH"

** BGD: Sample Registration System
    append using "FILEPATH"
    replace outlier = 1 if YEAR == 2003 & CO == "BGD" & VR_SOURCE == "SRS_LIBRARY" 
    append using "FILEPATH"

** BFA: 1985, 1996, 2006 Censuses
    append using "FILEPATH"

** BRA: 2010 Census
    append using "FILEPATH"

** CAN: 2010-2011 VR deaths
    append using "FILEPATH"

** CAN: 2012 VR deaths -- new file with 2012 - 2016
    *append using "FILEPATH" 
    append using "FILEPATH" 
    replace outlier = 1 if CO == "CAN" & YEAR == 2012 & VR_SOURCE != "WHO_causesofdeath"

** CHN: 1982 Census
    append using "FILEPATH"

** CHN: 2000 Census
    append using "FILEPATH"

** CHN: 2010 Census
    append using "FILEPATH"

** CHN: DSP
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

** IDN prepped at province level: SUPAS, SUSENAS, CENSUS
** ********************************
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
    drop if mi(location_id) 
    merge 1:1 location_id using `idn_provs', nogen
    save `idn_provs', replace 

    use "FILEPATH", clear
    drop COUNTRY

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

    preserve
        keep if YEAR >= 2014 & inlist(COUNTRY, "IND_4841", "IND_4871")
        foreach var of varlist DATUM* {
            count if mi(`var')
            if `r(N)' == _N drop `var'
        }
        collapse (sum) DATUM*, by( YEAR SEX SUBDIV VR_SOURCE AREA)
        gen COUNTRY = "IND_44849"
        tempfile new_ap_old 
        save `new_ap_old', replace 
    restore
    append using `new_ap_old'

** LKA: VR from collaborator -- 02.01.16 JM
    append using "FILEPATH"

** LBN: VR from collaborator
    append using "FILEPATH"

** LBY: VR
    append using "FILEPATH"

** LTU: VR
    append using "FILEPATH"

** MNG: Stat yearbook
    append using "FILEPATH"

    replace outlier = 1 if YEAR == 1999 & CO == "MNG" & VR_SOURCE != "MNG_STAT_YB_2001"

** MOZ: 2007 Census
    append using "FILEPATH"

** PAK: SRS
    append using "FILEPATH"

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

    tempfile corrected
    save `corrected', replace

    use `master', clear
    drop if CO == "PSE" & YEAR > 2007 & YEAR < 2012 & VR_SOURCE == "WHO_causesofdeath"
    append using `corrected'

    replace outlier = 1 if CO == "PSE" & YEAR < 2008 & VR_SOURCE == "WHO_causesofdeath"

** SAU: 2007 Demographic Bulletin
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

** IND urban rural
        append using "FILEPATH"

** CHL deaths from Ministeria de Salud 2010-2011
    append using "FILEPATH"
    replace outlier = 1 if CO == "CHL" & VR_SOURCE == "CHL_MOH" & SUBDIV == "VR" & YEAR == 2010


** ********************************

** ********************************
** BDI Demographic survey 1965
    append using "FILEPATH"

** BDI Demographic survey 1970-1971
    append using "FILEPATH" 

** BGD 2011 census
    append using "FILEPATH" 

** BWA Demographic Survey 2006
    append using "FILEPATH"

** BWA census 1981
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

** ZAF community survey 2007
    append using "FILEPATH"

** ZMB: 2008 HHC
    append using "FILEPATH"

** ZMB LCMS
    append using "FILEPATH"

** ZMB SBS
    append using "FILEPATH"
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
    append using "FILEPATH" 

    append using "FILEPATH" 
    replace SUBDIV = "HOUSEHOLD" if SUBDIV == ""

    append using "FILEPATH" 

    replace SUBDIV = "HOUSEHOLD" if SUBDIV == ""

** Subnationals previously run at the national level 
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
** this is where all cause VR prepared in GBD 2017 is brought in
    append using "FILEPATH"
    replace SUBDIV = "DSP>2003" if NID == 338606 & VR_SOURCE == "CHN_DSP"

    keep if !(COUNTRY == "GBR" & YEAR == 2016) 
    preserve
        keep if inlist(COUNTRY, "GBR_4749", "GBR_4636", "GBR_433", "GBR_434") & YEAR == 2016 & SEX > 0

        sort COUNTRY SEX YEAR NID
        keep if VR_SOURCE == "WHO_causesofdeath" | VR_SOURCE == "Northern_Ireland_VR_all_cause"
        quietly by COUNTRY SEX YEAR NID: gen gbr_dup = cond(_N==1,0,_n)
        keep if gbr_dup <= 1
        drop gbr_dup

        replace COUNTRY = "GBR"
        replace SUBDIV = "VR"
        replace VR_SOURCE = "WHO_causesofdeath"
        replace NID = 350840
        replace FOOTNOTE = ""
        replace AREA = 0

        collapse (sum) DATUM*, by(COUNTRY SEX YEAR SUBDIV VR_SOURCE NID FOOTNOTE AREA)

        foreach var of varlist DATUM* {
            replace `var' = . if (`var' == 0)
        }

        tempfile gbr_2016_aggs
        save `gbr_2016_aggs', replace

    restore
    append using `gbr_2016_aggs'

**VUT HH CENSUS 2009
    append using "FILEPATH"

**USSR mortality tables 1939, 1959, 1970, 1970
    append using "FILEPATH"


** ***********************************************************************

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
        else if inlist("`var'", "DATUM1to1", "DATUM2to2", "DATUM3to3", "DATUM4to4", "DATUM1to4")  {
            replace `var' = `var' * scalar1to4
        } 
        else if inlist("`var'", "DATUM80to84", "DATUM85plus", "DATUM85to89", "DATUM90to94", "DATUM95to99", "DATUM95plus") {
            replace `var' = `var' * scalar80plus
        }

        else {
            capture replace `var' = `var' * scalar`var_stub'
        }
    }
    replace DATUMTOT = DATUM0to0 + DATUM1to4 + DATUM5to9 + DATUM10to14 + DATUM15to19 + DATUM20to24 + DATUM25to29 + 
    DATUM30to34 + DATUM35to39 + DATUM40to44 + DATUM45to49 + DATUM50to54 + DATUM55to59 + DATUM60to64 + DATUM65to69 + 
    DATUM70to74 + DATUM75to79 + DATUM80to84 + DATUM85plus + DATUMUNK if DATUMUNK != .

    replace DATUMTOT = DATUM0to0 + DATUM1to4 + DATUM5to9 + DATUM10to14 + DATUM15to19 + DATUM20to24 + DATUM25to29 + 
    DATUM30to34 + DATUM35to39 + DATUM40to44 + DATUM45to49 + DATUM50to54 + DATUM55to59 + DATUM60to64 + DATUM65to69 + 
    DATUM70to74 + DATUM75to79 + DATUM80to84 + DATUM85to89 + DATUM90to94 + DATUM95plus if DATUMUNK == .

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
        else if inlist("`var'", "DATUM1to1", "DATUM2to2", "DATUM3to3", "DATUM4to4", "DATUM1to4")  {
            replace `var' = `var' * scalar1to4
        } 
        else if inlist("`var'", "DATUM80to84", "DATUM85plus") {
            replace `var' = `var' * scalar80plus
        }

        else {
            capture replace `var' = `var' * scalar`var_stub'
        }
    }
    replace DATUMTOT = DATUM0to0 + DATUM1to4 + DATUM5to9 + DATUM10to14 + DATUM15to19 + DATUM20to24 + 
    DATUM25to29 + DATUM30to34 + DATUM35to39 + DATUM40to44 + DATUM45to49 + DATUM50to54 + DATUM55to59 + 
    DATUM60to64 + DATUM65to69 + DATUM70to74 + DATUM75to79 + DATUM80to84 + DATUM85plus + DATUMUNK

    drop scalar*

    tempfile yug_split
    save `yug_split'

    use `master' if !inlist(COUNTRY, "XYG", "XSU"), clear

    preserve
    merge m:1 COUNTRY YEAR SEX using `ussr_split', keep(2) nogen
    tempfile ussr_split
    save `ussr_split'
    restore

    preserve
    merge m:1 COUNTRY YEAR SEX using `yug_split', keep(2) nogen
    tempfile yug_split
    save `yug_split'
    restore

    append using `ussr_split'
    append using `yug_split'

** ***********************************************************************
** Add or apply scalars to deaths for non-representativeness
** ***********************************************************************

***************************************
    tempfile master
    save `master', replace
    insheet using "FILEPATH", comma clear

    rename scalar death_scalar
    keep parent_ihme_loc_id year_id death_scalar

    collapse (mean) death_scalar, by(parent_ihme_loc_id)
    rename parent_ihme_loc_id COUNTRY
    merge 1:m COUNTRY using `master', keep(3) nogen

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

    gsort +COUNTRY +YEAR +SEX -DATUMTOT 
    duplicates tag COUNTRY YEAR SEX SUBDIV if strpos(VR_SOURCE, "DYB") != 0, g(dup)
    drop if COUNTRY == "KOR" & VR_SOURCE == "DYB_download" & dup == 1 & YEAR >= 1977 & YEAR <= 1989
    drop if COUNTRY == "KOR" & VR_SOURCE == "DYB_ONLINE" & YEAR == 1993 & dup == 1
    drop dup
    gsort +COUNTRY +YEAR +SEX -DATUMTOT -DATUM0to0 -DATUM0to4
    duplicates drop COUNTRY YEAR SEX SUBDIV if strpos(VR_SOURCE, "DYB_ONLINE") != 0, force

    duplicates tag COUNTRY YEAR SEX SUBDIV if strpos(VR_SOURCE, "DYB") != 0, g(dup)
    drop if VR_SOURCE == "DYB_ONLINE" & dup == 1
    drop dup

    duplicates tag COUNTRY YEAR SEX if SUBDIV == "VR" & outlier != 1, g(dup) 
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & VR_SOURCE == "WHO_internal"
    drop dup

    duplicates tag COUNTRY YEAR SEX  if SUBDIV == "VR" & outlier != 1, g(dup) 
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & strpos(VR_SOURCE,"DYB") != 0
    drop dup

    duplicates tag COUNTRY YEAR SEX  if SUBDIV == "VR" & outlier != 1, g(dup) 
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & strpos(VR_SOURCE,"HMD") != 0 
    drop dup

    duplicates tag COUNTRY YEAR SEX  if SUBDIV == "VR" & outlier != 1, g(dup) 
    replace dup = 0 if dup == .
    replace outlier = 1 if dup != 0 & VR_SOURCE == "WHO"
    drop dup

    duplicates tag CO YEAR SEX if SUBDIV == "CENSUS" & CO == "CHN" & outlier != 1, g(dup)
    replace dup = 0 if dup == . 
    replace outlier = 1 if dup != 0 & CO == "CHN" & regexm(VR_SOURCE,"CHN_PROV_CENSUS")
    drop dup

    duplicates tag COUNTRY YEAR SEX if SUBDIV == "CENSUS" & outlier != 1, g(dup)
    replace dup = 0 if dup == . 
    replace outlier = 1 if dup != 0  & regexm(VR_SOURCE, "DYB") 
    drop dup

    duplicates tag CO YEAR SEX if SUBDIV == "VR" & outlier != 1, g(dup)
    replace dup = 0 if dup == . 
    replace outlier = 1 if dup != 0 & VR_SOURCE == "ALAN_LOPEZ"    

    replace outlier = 1 if dup !=0 & VR_SOURCE == "VR" & CO == "AUS" 
    drop dup 

    duplicates tag CO YEAR SEX if SUBDIV == "VR" & outlier != 1, g(dup)
    replace dup = 0 if dup == . 
    replace outlier = 1 if dup != 0 & CO == "USA" & VR_SOURCE == "NCHS"
    drop dup

    duplicates tag COUNTRY YEAR SEX SUBDIV if outlier != 1, gen(dup)
    replace outlier = 1 if COUNTRY == "LBY" & VR_SOURCE == "report" & SUBDIV == "VR" & inlist(YEAR, 2006, 2007, 2008) & dup >= 1
    replace outlier = 1 if COUNTRY == "MNG" & VR_SOURCE == "ALAN_LOPEZ_MNG_YEARBOOK" & SUBDIV == "VR" & inlist(YEAR, 2004, 2005) & dup >=1
    replace outlier = 1 if regexm(COUNTRY,"GBR_") & CO != "GBR_4749" & SUBDIV == "VR" & !regexm(VR_SOURCE, "WHO") & YEAR < 2013 
    drop dup

    duplicates tag COUNTRY YEAR SEX SUBDIV, gen(dup)
    replace outlier = 1 if dup >= 1 & COUNTRY == "LTU" & YEAR == 2010 & SUBDIV == "VR" & VR_SOURCE == "135808#LTU COD REPORT 2010"
    replace outlier = 1 if dup >= 1 & COUNTRY == "LTU" & YEAR == 2011 & SUBDIV == "VR" & VR_SOURCE == "135810#LTU COD REPORT 2011"
    replace outlier = 1 if dup >= 1 & COUNTRY == "LTU" & YEAR == 2012 & SUBDIV == "VR" & VR_SOURCE == "135811#LTU COD REPORT 2012"


** IDN separate SUPAS and SUSENAS and 2000 Census-Survey
    replace SUBDIV = "SUSENAS" if strpos(VR_SOURCE,"SUSENAS") !=0
    replace SUBDIV = "SUPAS" if strpos(VR_SOURCE,"SUPAS") !=0
    replace VR_SOURCE = "2000_CENS_SURVEY" if VR_SOURCE == "SURVEY" & CO == "IDN" & YEAR == 1999
    replace SUBDIV = "SURVEY" if VR_SOURCE == "2000_CENS_SURVEY" & CO == "IDN" & YEAR == 1999


** Fix age group naming for youngest age groups 
    replace DATUM0to0 = DATUM0to1 if DATUM0to0 == . & DATUM0to1 != . & (DATUM1to4 != . | DATUM0to4 != .)
    replace DATUM0to1 = . if DATUM0to0 != . & (DATUM1to4 != . | DATUM0to4 != .)     


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

    rename YEAR year
    rename VR_SOURCE deaths_source
    rename SUBDIV source_type
    rename FOOTNOTE deaths_footnote

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

    replace ihme_loc_id = "CHN_44533" if ihme_loc_id == "CHN"

    order ihme_loc_id country sex year deaths_source source_type deaths_footnote *

    replace deaths_source = "DYB" if regexm(deaths_source, "DYB_")

    replace DATUM0to4 = . if DATUM0to0 != . & DATUM1to4 != .

    duplicates tag ihme_loc_id year sex source_type, gen(dup2)
    replace outlier = 1 if regexm(ihme_loc_id , "GBR_") & !regexm(deaths_source, "WHO") & dup2 != 0
    drop dup2

    drop if ihme_loc_id == "CAN" & year == 2013 & deaths_source == "StatCan"

    replace outlier = 0 if ihme_loc_id == "USA" & year == 2010 & deaths_source == "WHO"

    drop if ihme_loc_id == "ZAF" & year == 2011 & deaths_source == "12146#stats_south_africa 2011 census"

    replace outlier = 1 if ihme_loc_id == "EGY" & year == 1958 & deaths_source == "DYB"

    replace outlier = 1 if ihme_loc_id == "RUS" & year == 1958 & (NID == 336438 | nid == 336438)

    replace outlier = 1 if (ihme_loc_id == "NZL_44851" | ihme_loc_id == "NZL_44850" | ihme_loc_id == "NZL") & year == 2015 & deaths_source == "WHO_causesofdeath"
    replace outlier = 1 if ihme_loc_id == "MDA" & year == 2016 & deaths_source == "WHO_causesofdeath"

    replace outlier = 1 if ihme_loc_id == "GRC" & year == 2015 & deaths_source == "WHO_causesofdeath"

    replace outlier = 1 if ihme_loc_id == "NLD" & year == 2016 & deaths_source == "Netherlands_VR_all_cause"

    replace outlier = 1 if ihme_loc_id == "TUR" & (year == 2014 | year == 2015 | year == 2016) & deaths_source == "WHO_causesofdeath"

    replace outlier = 1 if ihme_loc_id == "SYC" & year == 2015 & deaths_source == "WHO_causesofdeath"

    replace outlier = 1 if regexm(ihme_loc_id, "BRA") & year == 2016 & deaths_source == "WHO_causesofdeath"

    replace outlier = 1 if ihme_loc_id == "ARG" & year == 2015 & deaths_source == "Argentina_VR_all_cause"

    replace outlier = 1 if ihme_loc_id == "USA" & year == 2010 & deaths_source == "WHO"

    replace outlier = 1 if ihme_loc_id == "USA" & year == 2015 & deaths_source == "CDC_report"  

    replace outlier = 1 if ihme_loc_id == "HRV" & year == 2015 & deaths_source == "Croatia_allcause_VR"

    replace outlier = 1 if ihme_loc_id == "CHE" & year == 2015 & deaths_source == "WHO_causesofdeath"

    replace outlier = 1 if regexm(ihme_loc_id, "BRA") & year == 2016
    replace outlier = 0 if regexm(ihme_loc_id, "BRA") & year == 2016 & deaths_source == "Brazil_SIM_ICD10_allcause"

** CHN and CHN subnationals
    replace outlier = 1 if regexm(ihme_loc_id, "CHN") & deaths_source == "CHN_DSP" & year >= 2004 & source_type == "VR"

** HONG KONG

    replace outlier = 1 if ihme_loc_id == "CHN_354" & deaths_source != "WHO_causesofdeath" & year >= 1979 & year <= 2000

    replace outlier = 0 if ihme_loc_id == "CHN_354" & deaths_source == "WHO_causesofdeath" & year >= 1979 & year <= 2000

    replace outlier = 1 if ihme_loc_id == "CHN_354" & deaths_source != "WHO" & year > 2000

    replace outlier = 0 if ihme_loc_id == "CHN_354" & deaths_source == "WHO" & year > 2000

** MACAO

    replace outlier = 1 if ihme_loc_id == "CHN_361" & year == 1994
    replace outlier = 0 if ihme_loc_id == "CHN_361" & year == 1994 & deaths_source == "DYB"

    replace source_type = "Civil Registration" if deaths_source == "IRN_NOCR_allcause_VR"

    replace outlier = 1 if ihme_loc_id == "ESP" & year == 1974
    replace outlier = 0 if ihme_loc_id == "ESP" & year == 1974 & deaths_source == "HMD"

    * BEL 1986 & 1987

    replace outlier = 1 if ihme_loc_id == "BEL" & inrange(year, 1986, 1987)
    replace outlier = 0 if ihme_loc_id == "BEL" & inrange(year, 1986, 1987) & deaths_source == "HMD"

    replace outlier = 1 if ihme_loc_id == "DEU" & inrange(year, 1980, 1989)
    replace outlier = 0 if ihme_loc_id == "DEU" & inrange(year, 1980, 1989) & deaths_source == "HMD"    

    count if regexm(ihme_loc_id, "CHN") & ihme_loc_id != "CHN_361" & ihme_loc_id != "CHN_354" & deaths_source == "WHO_causesofdeath"
    drop if regexm(ihme_loc_id, "CHN") & ihme_loc_id != "CHN_361" & ihme_loc_id != "CHN_354" & deaths_source == "WHO_causesofdeath"

    replace outlier = 1 if ihme_loc_id == "JOR" & year <= 1967

    replace outlier = 1 if regexm(ihme_loc_id, "CHN") & year == 2016 & deaths_source == "CHN_DSP"

    replace outlier = 1 if ihme_loc_id == "ALB" & year == 2010 & deaths_source == "WHO_causesofdeath"
    replace outlier = 1 if ihme_loc_id == "DZA" & inlist(year, 2005, 2006) & deaths_source == "WHO_causesofdeath"
    replace outlier = 1 if ihme_loc_id == "SYR" & year == 1980 & deaths_source == "WHO_causesofdeath"
    replace outlier = 1 if ihme_loc_id == "ZWE" & year == 2007 & deaths_source == "WHO_causesofdeath"
    replace outlier = 1 if ihme_loc_id == "BRA_4764" & NID == 153001 & year == 1979

    replace outlier = 1 if ihme_loc_id == "GRD" & year == 1984 & deaths_source == "WHO"

    replace outlier = 1 if ihme_loc_id == "BLZ" & year == 1981 & deaths_source == "WHO"
    replace outlier = 0 if ihme_loc_id == "BLZ" & year == 1981 & deaths_source == "DYB"

    replace outlier = 1 if ihme_loc_id == "CRI" & inrange(year, 2015, 2016)
    replace outlier = 0 if ihme_loc_id == "CRI" & inrange(year, 2015, 2016) & deaths_source == "CostaRica_VR_all_cause"

    replace outlier = 1 if ihme_loc_id == "MDV" & year == 2015 & NID == 325219 
    replace outlier = 1 if ihme_loc_id == "MDV" & year == 2016 & NID == 257555

    replace outlier = 1 if ihme_loc_id == "LVA" & year == 2016 & NID == 324971

    replace outlier = 1 if ihme_loc_id == "GUY" & inrange(year, 2014, 2016) & NID == 324983

    replace outlier = 1 if regexm(ihme_loc_id, "MEX") & year == 2016 & deaths_source == "MEX_VR_all_cause"
    replace outlier = 0 if regexm(ihme_loc_id, "MEX") & year == 2016 & deaths_source == "WHO_causesofdeath"

** ***********************************************************************
** Combine males and females to get both if they're not already in there 
** ***********************************************************************
    preserve 
    keep if outlier == 1
    tempfile outlier
    save `outlier'
    restore

    noisily: display in green "Generate both sexes combined"

    replace outlier = 0 if mi(outlier)
    keep if outlier == 0
    replace NID = nid if NID == .

    describe *nid* *NID*

    replace source_type = "MCCD" if deaths_source == "WHO_causesofdeath" & source_type == "VR" & regexm(ihme_loc_id, "IND")

    keep if outlier != 1
    sort ihme_loc_id year source_type outlier sex
    quietly by ihme_loc_id year source_type outlier sex: gen dups = cond(_N==1,0,_n)
    replace outlier = 1 if dups > 1

    keep if outlier == 0
    drop dups

    tempfile all 
    drop if deaths_source == "NCHS" & year == 2010
    save `all', replace
        keep ihme_loc_id sex year deaths_source source_type outlier
        duplicates list ihme_loc_id year source_type outlier sex 
        reshape wide deaths_source, i(ihme_loc_id year source_type outlier) j(sex, string)
        keep if (deaths_sourceboth != deaths_sourcefemale | deaths_sourceboth != deaths_sourcemale) & deaths_sourceboth != "" & deaths_sourcefemale != "" & deaths_sourcemale != ""
        keep ihme_loc_id year source_type
        tempfile dropboth
        save `dropboth'

    use `all', clear 
    merge m:1 ihme_loc_id year source_type using `dropboth'
    drop if _m == 3 & sex == "both"
    drop _m 

    save `all', replace 
        keep if outlier == 0
        keep ihme_loc_id sex year deaths_source source_type outlier

        reshape wide deaths_source, i(ihme_loc_id year source_type outlier) j(sex, string)
        keep if (deaths_sourcemale == "" & deaths_sourcefemale != "") | (deaths_sourcemale != "" & deaths_sourcefemale == "")
        keep ihme_loc_id year source_type
        tempfile dropone
        save `dropone'

    use `all', clear
    merge m:1 ihme_loc_id year source_type using `dropone'
    drop if _m == 3 & sex != "both"
    drop _m     

    preserve
    bysort ihme_loc_id country year source_type outlier: egen maxs = count(year)
    keep if maxs == 2
    drop if ihme_loc_id == "USA" & deaths_source == "NCHS"
    replace NID = 99999 if NID == .
    replace ParentNID = 99999 if ParentNID == .
    lookfor DATUM
    return list
        foreach var of varlist `r(varlist)' {
            replace `var' = -1000000 if `var' == .
        }

    collapse (sum) DATUM*, by(ihme_loc_id country year deaths_source source_type deaths_footnote outlier NID ParentNID)
    gen sex = "both"

    lookfor DATUM
    return list
        foreach var of varlist `r(varlist)' {
            replace `var' = . if `var' < -100000
        }

    tempfile moreboth
    save `moreboth', replace
    restore
    append using `moreboth'

    tempfile master
    save `master', replace

    append using `outlier'
    save `master', replace
** ***********************************************************************
** Split sources to be DDM'd seperately 
** ***********************************************************************
    noisily: display in green "Split sources to be DDM'd seperately"    

    replace source_type = "DSP 96-01" if ihme_loc_id == "CHN_44533" & year >= 1996 & year <= 2000 & source_type == "DSP"
    replace source_type = "DSP>2003" if ihme_loc_id == "CHN_44533" & year >= 2004 & year <= 2010 & source_type == "DSP"

    replace source_type = "SRS 1970-1977" if ihme_loc_id == "IND" & year >= 1970 & year <= 1976 & source_type == "SRS"
    replace source_type = "SRS 1977-1983" if ihme_loc_id == "IND" & year >= 1977 & year <= 1982 & source_type == "SRS"
    replace source_type = "SRS 1983-1993" if ihme_loc_id == "IND" & year >= 1983 & year <= 1992 & source_type == "SRS"
    replace source_type = "SRS 1993-2004" if regexm(ihme_loc_id, "IND") & year >= 1993 & year <= 2003 & source_type == "SRS" 
    replace source_type = "SRS 2004-2014" if regexm(ihme_loc_id, "IND") & year >= 2004 & year <= 2013 & source_type == "SRS" 
    replace source_type = "SRS 2014-2016" if regexm(ihme_loc_id, "IND") & inrange(year, 2014, 2016) & source_type == "SRS"

 
    replace country = "Old Andhra Pradesh" if country != "Old Andhra Pradesh" & ihme_loc_id == "IND_44849"
    replace outlier = 1 if ihme_loc_id == "IND_4864" & inrange(year, 2014, 2015) & deaths_source == "IND_CRS_allcause_VR" 
    replace outlier = 1 if inlist(ihme_loc_id, "IND_44538","IND_44539","IND_44540") & inrange(year, 2014, 2015) & deaths_source == "IND_CRS_allcause_VR" 
    replace source_type = "VR pre-1978" if ihme_loc_id == "KOR" & year <= 1977
    replace source_type = "VR 1978-2000" if ihme_loc_id == "KOR" & year > 1977 & year < 2000
    replace source_type = "VR post-2000" if ihme_loc_id == "KOR" & year >= 2000
    replace source_type = "DSP<1996" if source_type == "DSP" & deaths_source == "CHN_DSP" & year < 1996 
    replace source_type = "DSP 96-04" if source_type == "DSP" & deaths_source == "CHN_DSP" & year >= 1996 & year < 2004 
    replace source_type = "DSP>2003" if source_type == "DSP" & deaths_source == "CHN_DSP" & year >= 2004 

    replace source_type = "VR pre-2003" if regexm(ihme_loc_id,"ZAF") & year <= 2002 & source_type == "VR"
    replace source_type = "VR post-2003" if regexm(ihme_loc_id,"ZAF") & year > 2002 & source_type == "VR"
	replace source_type = "VR" if deaths_source == "stats_south_africa" & ihme_loc_id == "ZAF" 

    replace source_type = "VR pre-2011" if regexm(ihme_loc_id,"MEX") & year <= 2011 & source_type == "VR"
    replace source_type = "VR post-2011" if regexm(ihme_loc_id,"MEX") & year > 2011 & source_type == "VR"

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

** keeping appropriate variables - extra variables can have far reaching consequences in the DDM process
    replace nid = NID if nid == . & NID != .
    rename nid deaths_nid
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


    assert !mi(outlier)
    preserve
        keep if outlier == 0
        isid ihme_loc_id sex year source_type_id deaths_source outlier

        rename deaths_nid deaths_nid_orig
        rename deaths_underlying_nid deaths_underlying_nid_orig
        merge 1:1 ihme_loc_id sex year source_type_id deaths_source using `deaths_nids_sourcing', keep(1 3)
        count if _m == 3
        drop _merge
        replace deaths_nid  = deaths_nid_orig if deaths_nid == ""
        replace deaths_underlying_nid  = deaths_underlying_nid_orig if deaths_underlying_nid == ""
        drop deaths_nid_orig deaths_underlying_nid_orig
        tempfile not_outliers
        save `not_outliers'
    restore
    keep if outlier != 0
    append using `not_outliers'

    replace deaths_underlying_nid = "" if deaths_underlying_nid == "."

    replace outlier = 1 if ihme_loc_id == "ARM" & year == 1988 & deaths_source == "WHO_causesofdeath"
    replace outlier = 1 if ihme_loc_id == "PRY" & year == 1992 & deaths_source == "DYB"
    replace outlier = 1 if ihme_loc_id == "VIR" & year == 1990
    replace outlier = 1 if ihme_loc_id == "VIR" & year == 2015 & deaths_source == "WHO_causesofdeath"
    replace outlier = 1 if ihme_loc_id == "MRT" & deaths_source == "WHO_causesofdeath"
    replace outlier = 1 if ihme_loc_id == "TUN" & inlist(year, 2009, 2013)
    replace outlier = 1 if ihme_loc_id == "ASM" & year == 1957
    replace outlier = 1 if ihme_loc_id == "JOR" & year <= 1967

    preserve
        keep if outlier != 1
        keep if source_type_id == 1
        sort ihme_loc_id country sex year
        by ihme_loc_id country sex year: generate nobs = _N
        drop DATUM*
        count if nobs > 1
        list if nobs > 1
        assert nobs <= 1
    restore

    compress
    saveold "FILEPATH", replace
exit, clear STATA
