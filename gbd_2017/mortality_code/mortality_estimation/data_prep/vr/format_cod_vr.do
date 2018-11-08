
// Purpose:     Format the main VR database compiled by the IHME Cause of Death team into the standard format used for Death Distribution Methods and Mortality team processing.

    clear all
    set more off
    set mem 500m
    cap restore, not

if c(os)=="Windows" {
    local prefix="J:"

if c(os)=="Unix" {
    local prefix="/home/j"

global new_run_id = "`1'"

run "FILEPATH"
create_connection_string
local conn_string = r(conn_string)

local run_folder = "FILEPATH"
local input_dir = "FILEPATH"
local output_dir = "FILEPATH"

local all_cause_dir = "FILEPATH"

    import delimited using "FILEPATH", clear    
    keep location_name ihme_loc_id location_type location_id
    gen gbd_country_iso3 = substr(ihme_loc_id, 1, 3) if inlist(location_type, "admin1", "admin2")
    keep location_name ihme_loc_id gbd_country_iso3 location_id
    duplicates drop
    tempfile countrycodes 
    save `countrycodes', replace

    odbc load, exec("") `conn_string' clear
    tempfile xcodes
    save `xcodes', replace      
    odbc load, exec("") `conn_string' clear
    duplicates drop
    tempfile county
    save `county', replace

cd "`FILEPATH'"

local dropnid = "no"

local whocomp = "no"
local whocompfile = "FILEPATH"

local newcurrent = "no"
local newcurrentall = "no"
local percentdiffcriteria = 1

local archivedcompfile = "FILEPATH"

    use "`archivedcompfile'", clear
    rename COUNTRY iso3
    merge m:1 iso3 using `xcodes'
    replace iso3 = ihme_loc_id if _m == 3
    drop if _m ==2
    drop _m ihme
    rename iso3 COUNTRY
    tempfile archivedcompfile
    save `archivedcompfile', replace

    local date = c(current_date)

cap mkdir "FILEPATH"    

global graphsdir "FILEPATH"

use "FILEPATH", clear
replace location_id = 152 if iso3 == "SAU"

merge m:1 location_id using `countrycodes', keep(3)
drop gbd_country_iso3

drop if sex == 9 | sex == 0
drop if year == 9999

    gen gbd_country_iso3 = iso3 if location_id != .

        replace iso3 = "HKG" if location_id == 354
        replace iso3 = "MAC" if location_id == 361
        replace gbd_country_iso3 = "" if inlist(iso3, "HKG", "MAC")
        replace location_id = . if inlist(iso3, "HKG", "MAC")
        replace ihme_loc_id = "HKG" if iso3 == "HKG"
        replace ihme_loc_id = "MAC" if iso3 == "MAC"

        replace ihme_loc_id = "PRI" if location_id == 385
        replace gbd_country = "" if ihme_loc_id == "PRI"

        drop iso3
        order ihme

    duplicates tag ihme_loc_id sex year source, gen(dup)

    sort ihme_loc_id year sex deaths3
    drop if dup == 1 & source == "US_NCHS_counties_ICD10" & year == 2003

    drop dup
    duplicates tag ihme_loc_id sex year source, gen(dup)

    drop dup

    collapse (sum) deaths*, by(ihme_loc_id gbd_country_iso3 sex year source NID)

    duplicates tag ihme_loc_id year sex, gen(dup)

    ** Georgia and VIR
    sort ihme year sex deaths3
    drop if ihme == "GEO" & source == "Georgia_VR" & dup ==1
    drop if ihme == "VIR" & source == "ICD9_BTL" & dup ==1
    drop if ihme == "PHL" & source == "Philippines_2006_2012" & dup ==1
    drop if ihme_loc_id == "THA" & source == "Thailand_Public_Health_Statistics" & dup == 1
    drop if ihme_loc_id == "RUS" & source == "Russia_FMD_1999_2011" & dup == 1
    drop if ihme_loc_id == "MNG" & source == "Other_Maternal" & dup == 1
    drop if ihme_loc_id == "IRN" & source == "ICD10" & dup == 1

    drop dup
    capture isid ihme_loc_id year sex
    if _rc > 0 {
        drop if source == "Iceland_VR" & year == 2016

    drop if source == "Middle_East_Maternal"

    duplicates tag ihme_loc_id year sex, gen(dup)
    drop if dup != 0
    drop dup

** *******************************************************************************************************

drop if ihme_loc_id == "IND"
drop if ihme_loc_id == "STP" & source == "ICD9_BTL" & year == 1985
drop if ihme_loc_id == "BHR" & source == "ICD9_BTL" & year == 1986
drop if ihme_loc_id == "HTI" & source == "ICD9_BTL" & year == 1981
drop if ihme_loc_id == "TON" & source == "Tonga_2002_2004" & year == 2003
drop if ihme_loc_id == "GEO" & source =="hospital workshop" & year == 2004
drop if ihme_loc_id == "GEO" & source == "morticd10" & year == 2004
drop if ihme_loc_id == "DOM" & year == 2002 & source == "morticd10"
drop if ihme_loc_id == "PHL" & year > 1980 & year < 1990 & source != "Philippines_1979_2000"
drop if ihme_loc_id == "JPN" & year == 1950
drop if ihme_loc_id == "AZE" & inlist(year, 2001, 2002, 2003, 2004)
drop if ihme_loc_id == "BHR" & year == 1998
drop if ihme_loc_id == "BMU" & inlist(year, 1986, 1992, 1993, 1994, 1995, 2000)
drop if ihme_loc_id == "BRA" & year == 2009
drop if ihme_loc_id == "COL" & inlist(year, 1981, 1988, 1995, 1996, 2002, 2003, 2004, 2009)
drop if ihme_loc_id == "DOM" & inlist(year, 2007, 2008, 2009, 2010)
drop if ihme_loc_id == "DMA" & inlist(year, 1981, 1984, 1986, 1996, 1997)
drop if ihme_loc_id == "EGY" & year == 1980
drop if ihme_loc_id == "EST" & inlist(year, 1989, 1990, 1991, 1992, 1993)
drop if ihme_loc_id == "GRD" & inlist(year, 1994, 1995)
drop if ihme_loc_id == "HTI" & year == 1999
drop if ihme_loc_id == "IRL" & inlist(year, 2004, 2005)
drop if ihme_loc_id == "KIR" & inlist(year, 1995, 1999, 2000, 2001)
drop if ihme_loc_id == "LCA" & year == 2008
drop if ihme_loc_id == "LVA" & inlist(year, 1991, 1992, 1993, 1994, 1995)
drop if ihme_loc_id == "LTU" & inlist(year, 1991, 1992)
drop if ihme_loc_id == "MAC" & year == 1994
drop if ihme_loc_id == "MEX" & year >= 1979 & year <= 2012
drop if ihme_loc_id == "MNG" & year >= 2004 & year <= 2008
drop if ihme_loc_id == "MNE" & year == 2006
drop if ihme_loc_id == "OMN" & year == 2009
drop if ihme_loc_id == "PER" & inlist(year, 2002, 2003, 2004, 2005)
drop if ihme_loc_id == "SAU" & year == 2009
drop if ihme_loc_id == "SLV" & year == 1992
drop if ihme_loc_id == "SYC" & inlist(year, 2005, 2006)
drop if ihme_loc_id == "SUR" & year == 2007
drop if ihme_loc_id == "GBR" & inlist(year, 1979, 1980)
drop if ihme_loc_id == "ATG" & source == "ICD9_BTL"
drop if ihme_loc_id == "BHS" & source == "ICD9_BTL"
drop if ihme_loc_id == "BLZ" & source == "ICD9_BTL"
drop if ihme_loc_id == "BMU" & source == "ICD9_BTL"
drop if ihme_loc_id == "BRB" & source == "ICD9_BTL"
drop if ihme_loc_id == "CRI" & source == "ICD9_BTL"
drop if ihme_loc_id == "CUB" & source == "ICD9_BTL"
drop if ihme_loc_id == "DMA" & source == "ICD9_BTL"
drop if ihme_loc_id == "DOM" & source == "ICD9_BTL"
drop if ihme_loc_id == "ECU" & source == "ICD9_BTL"
drop if ihme_loc_id == "GRD" & source == "ICD9_BTL"
drop if ihme_loc_id == "GTM" & source == "ICD9_BTL"
drop if ihme_loc_id == "GUY" & source == "ICD9_BTL"
drop if ihme_loc_id == "HND" & source == "ICD9_BTL" & inlist(year, 1979, 1980, 1981, 1987, 1988, 1989, 1990)
drop if ihme_loc_id == "JAM" & source == "ICD9_BTL"
drop if ihme_loc_id == "LCA" & source == "ICD9_BTL"
drop if ihme_loc_id == "MUS" & source == "ICD9_BTL"
drop if ihme_loc_id == "NIC" & source == "ICD9_BTL"
drop if ihme_loc_id == "PAN" & source == "ICD9_BTL"
drop if ihme_loc_id == "PER" & source == "ICD9_BTL"
drop if ihme_loc_id == "PRY" & source == "ICD9_BTL"
drop if ihme_loc_id == "SLV" & source == "ICD9_BTL"
drop if ihme_loc_id == "SUR" & source == "ICD9_BTL"
drop if ihme_loc_id == "TTO" & source == "ICD9_BTL"
drop if ihme_loc_id == "VCT" & source == "ICD9_BTL"
drop if ihme_loc_id == "VEN" & source == "ICD9_BTL"
drop if ihme_loc_id == "PRI" & source == "ICD9_BTL"
drop if ihme_loc_id == "ROU" & source == "ICD7A" & year <=1968
drop if ihme_loc_id == "RUS" & source == "Russia_FMD_1989_1998" & NID == 133400 & (year >=1989 & year <=1998)
drop if ihme_loc_id == "HND" & source == "ICD7A" & year == 1966
drop if ihme_loc_id == "BOL" & inlist(year, 2000, 2001, 2002, 2003) & source == "ICD10" & NID == 156411
drop if ihme_loc_id == "ARG" & source == "ICD7A" & inlist(year, 1966, 1967)
drop if ihme_loc_id == "FJI" & source == "ICD9_BTL" & year == 1999
drop if ihme_loc_id == "DEU" & source == "ICD8A" & inlist(year, 1968, 1969, 1970)
drop if ihme_loc_id == "ISR" & inlist(source, "ICD7A", "ICD8A") & year <=1974 & year >= 1954
drop if ihme_loc_id == "MYS" & (year >=1997 & year <=2008)
drop if ihme_loc_id == "PRT" & source == "ICD10" & inlist(year, 2005, 2006)
drop if ihme_loc_id == "KOR" & source == "ICD9_BTL" & year <=1994
drop if (inlist(ihme_loc_id, "GBR_4621", "GBR_4623", "GBR_4624", "GBR_4618", "GBR_4619", "GBR_4625", "GBR_4626", "GBR_4622", "GBR_4620") | ihme_loc_id == "GBR_4636") & source == "UK_deprivation_1981_2000" & year == 1995
drop if ihme_loc_id == "GBR" & year == 1995
drop if ihme_loc_id == "BRA" & year < 2012
drop if ihme_loc_id == "MAR" & source == "Morocco_Health_In_Figures" & inlist(year, 1996, 1997, 2001)
drop if ihme_loc_id == "VIR" & source == "ICD9_BTL" & year == 1980

** **************************************************************************************   

    foreach var of varlist deaths3-deaths25 {
        count if `var' == .
        di in red "`var' has `r(N)' missing"
        assert `var' != .

    gen terminal = .
    replace terminal = 25 if deaths25 != 0
    forvalues j = 24(-1)7 {
        local jplus = `j'+1
        local jplus2 = `j'+2
        replace terminal = `j' if deaths`j' != 0 & deaths`jplus' == 0 & terminal == . & `j' == 24
        if `j' < 24 {
        replace terminal = `j' if deaths`j' != 0 & deaths`jplus' == 0 & deaths`jplus2' == 0 & terminal == . & `j' < 25

    forvalues j = 7/25 {
        local k = (`j'-6)*5
        local k4 = `k' + 4

        cap: gen DATUM`k'to`k4' = .
        replace DATUM`k'to`k4' = deaths`j' if `j' < terminal 

        cap: gen DATUM`k'plus = .
        replace DATUM`k'plus = deaths`j' if `j' == terminal 

    gen DATUM0to0 = deaths2
    gen DATUM1to4 = deaths3 

    lookfor DATUM
    foreach var of varlist `r(varlist)' {
        summ `var'
        if `r(N)' == 0 {
            drop `var'

    drop deaths* 
    cap drop DATUM95to99

*************************
    merge m:1 ihme_loc_id using `countrycodes'
    replace location_name = "Hong Kong Special Administrative Region of China" if ihme_loc_id == "HKG"
    replace location_name = "Macao Special Administrative Region of China" if ihme_loc_id == "MAC"

    drop if _m == 2         
    drop _m

    gen VR_SOURCE = "WHO_causesofdeath"
    rename ihme_loc_id COUNTRY
    rename sex SEX
    rename year YEAR
    g FOOTNOTE = ""

    duplicates tag COUNTRY YEAR SEX, g(dup) 

    drop if inlist(dup, 1, 2)
    assert dup == 0             

    drop dup

    drop source terminal location_name gbd_country_iso3 location_id

    cap g DATUMUNK = .
    cap drop DATUMTOT
    egen DATUMTOT = rowtotal(DATUM*)
    assert DATUMTOT != .

    preserve
    lookfor DATUM
    foreach var of varlist `r(varlist)' {
        replace `var' = -10000000 if `var' == .

    collapse (sum) DATUM*, by(COUNTRY YEAR VR_SOURCE FOOTNOTE NID)
    gen SEX = 0

    lookfor DATUM
    foreach var of varlist `r(varlist)' {
        replace `var' = . if `var' < 0

    tempfile both
    save `both', replace
    restore

    append using `both'

    cap drop AREA
    g AREA = 0
    cap drop SUBDIV
    g SUBDIV = "VR"

    order COUNTRY SEX YEAR AREA SUBDIV VR_SOURCE NID

    if "`dropnid'" == "yes" {
        drop NID

    saveold "FILEPATH", replace
