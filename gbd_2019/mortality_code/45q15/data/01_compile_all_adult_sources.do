** *************************************************************************
  ** Description: Compile estimates of 45q15 from all sources
** *************************************************************************
  
  ** **********************
  ** Set up Stata
** **********************
  
  clear all
cap restore, not
capture cleartmp
set mem 500m
set more off
pause on

if (c(os)=="Unix") global root "FILEPATH"
if (c(os)=="Windows") global root "FILEPATH"
local user "`c(username)'"
global homeprefix= "FILEPATH"

local new_run_id = "`1'"
local ddm_run_id = "`2'"

local run_folder = "FILEPATH"

import delimited using "`run_folder'/inputs/loc_map.csv"
tempfile iso3_map
save `iso3_map'

** **********************
** Add in DDM and growth balance files
** **********************
	cd "FILEPATH"

** Main DDM file
	use "FILEPATH/d10_45q15.dta", clear

	gen exposure = c1_15to19 + c1_20to24 + c1_25to29 + c1_30to34 + c1_35to39 + c1_40to44 + c1_45to49 + c1_50to54 + c1_55to59
	cap rename deaths_nid nid
	cap rename deaths_underlying_nid underlying_nid
	keep ihme_loc_id year sex deaths_source source_type comp sd adjust adj45q15 obs45q15 exposure nid underlying_nid
	destring nid, replace force
	gen filename = "d10_45q15.dta"


** **********************
** Add China DSP data
** **********************
	preserve
		use "d08_smoothed_completeness.dta", clear
		keep if iso3_sex_source == "CHN_44533_both_DSP" & inlist(year, 1986, 1987, 1988)
		keep year pred2 sd
		rename pred2 comp
		tempfile comp
		save `comp'

insheet using "FILEPATH", clear
rename v1 ihme_loc_id
replace ihme_loc_id = "CHN_44533"
rename v2 year
rename v3 sex
rename v4 obs45q15
merge m:1 year using `comp'
		drop _m
		gen adj45q15 = 1-exp((1/comp)*ln(1-obs45q15))
		gen adjust = 1
		gen source_type = "DSP"
		gen deaths_source = "DSP"
		* drop obs45q15
		tempfile china
		save `china'
restore
append using `china'

	gen exclude = 0


** **********************
** Add in SIBS
** **********************
	preserve
	use "FILEPATH", clear
	gen adjust = 0
	gen exclude = 0
	gen source_type = "SIBLING_HISTORIES"
	drop if adj45q15 == .
drop adj45q15_*
  cap drop svy
tempfile sibs
save `sibs', replace
	restore
	append using `sibs'

** **********************
  ** Add in aggregate points from reports (no microdata)
** **********************
  cd "FILEPATH"

preserve

use "FILEPATH", clear
append using "FILEPATH"
append using "FILEPATH"
append using "FILEPATH"
append using "FILEPATH"
append using "FILEPATH"

keep iso3 sex year source_type deaths_source adjust adj45q15

// Merge on ihme_loc_id
merge m:1 iso3 using `iso3_map', keep(1 3) nogen keepusing(ihme_loc_id)
	drop iso3

	replace ihme_loc_id = "CHN_44533" if ihme_loc_id == "CHN" & inlist(deaths_source,"FERTILITY_SURVEY","EPI_SURVEY") // Relabel fertility survey and epi survey

	replace adjust = 0
	replace source_type = "HOUSEHOLD_DEATHS"
	tempfile other
	save `other', replace

restore
append using `other'

	drop if ihme_loc_id=="IND_4871" & year < 2014 // get rid of Telangana
	drop if ihme_loc_id == "IND_43938" & year < 2014
	drop if ihme_loc_id == "IND_43902" & year < 2014
	drop if ihme_loc_id == "IND_43872" & year < 2014
	drop if ihme_loc_id == "IND_43908" & year < 2014
	replace ihme_loc_id="IND_44849" if ihme_loc_id=="IND_4841" & year < 2014

** **********************
** Source NID
** **********************
	replace nid = 5583 if ihme_loc_id == "IDN" & year == 1993
	replace nid = 6706 if ihme_loc_id == "IDN" & year == 1996
	replace nid = 5827 if ihme_loc_id == "IDN" & year == 1998 & deaths_source == "IFLS"
	replace nid = 6767 if ihme_loc_id == "IDN" & year == 1998 & deaths_source == "SUSENAS"
	replace nid = 20021 if ihme_loc_id == "IDN" & year == 2007

	replace nid = 83523 if ihme_loc_id == "VNM" & year == 2006.5 & deaths_source == "PCFPS"

	replace nid = 108908 if ihme_loc_id == "PAK" & year == 1991 & deaths_source == "PAK_demographic_survey"
	replace nid = 140966 if ihme_loc_id == "PAK" & year == 1992 & deaths_source == "PAK_demographic_survey"
	replace nid = 93477 if ihme_loc_id == "PAK" & year == 1995 & deaths_source == "PAK_demographic_survey"
	replace nid = 93477 if ihme_loc_id == "PAK" & year == 1996 & deaths_source == "PAK_demographic_survey"
	replace nid = 108915 if ihme_loc_id == "PAK" & year == 1997 & deaths_source == "PAK_demographic_survey"
	replace nid = 93489 if ihme_loc_id == "PAK" & year == 1999 & deaths_source == "PAK_demographic_survey"
	replace nid = 43008 if ihme_loc_id == "PAK" & year == 2003 & deaths_source == "PAK_demographic_survey"
	replace nid = 43010 if ihme_loc_id == "PAK" & year == 2005 & deaths_source == "PAK_demographic_survey"


** **********************
** Mark shocks
** **********************

	gen shock = 0

	replace shock = 1 if ihme_loc_id == "ALB" & floor(year) == 1997
	replace shock = 1 if ihme_loc_id == "ARM" & year == 1988
	replace shock = 1 if ihme_loc_id == "BGD" & floor(year) == 1975
	replace shock = 1 if ihme_loc_id == "COG" & floor(year) == 1997 // Civil war
	replace shock = 1 if ihme_loc_id == "CYP" & sex == "male" & floor(year) == 1974
	replace shock = 1 if ihme_loc_id == "GTM" & floor(year) == 1981
	replace shock = 1 if ihme_loc_id == "HRV" & (floor(year) == 1991 | floor(year) == 1992)
	replace shock = 1 if ihme_loc_id == "HUN" & floor(year) ==1956
	replace shock = 1 if ihme_loc_id == "IDN" & floor(year)>=1963 & floor(year) <= 1966
replace shock = 1 if ihme_loc_id == "IDN" & source_type == "SIBLING_HISTORIES" & year == 2004.5
	replace shock = 1 if ihme_loc_id == "IRQ" & floor(year) >= 2003 & floor(year) <= 2013
	replace shock = 1 if ihme_loc_id == "JPN" & source_type == "VR" & floor(year) == 2011
	replace shock = 1 if ihme_loc_id == "LKA" & floor(year) == 1996
	replace shock = 1 if ihme_loc_id == "PAN" & floor(year) == 1989
	replace shock = 1 if ihme_loc_id == "PRT" & (floor(year) == 1975 | floor(year) == 1976)
	replace shock = 1 if ihme_loc_id == "RWA" & (floor(year) == 1994 | floor(year) == 1993)
	replace shock = 1 if ihme_loc_id == "SLV" & (floor(year) >= 1980 & floor(year) <= 1983) & source_type == "VR"
	replace shock = 1 if ihme_loc_id == "TJK" & floor(year) == 1993
	replace shock = 1 if ihme_loc_id == "TLS" & floor(year) == 1999
	replace shock = 1 if ihme_loc_id == "MEX_4651" & floor(year) == 1985

	replace shock = 1 if ihme_loc_id == "JPN_35426" & floor(year) == 2011
	replace shock = 1 if ihme_loc_id == "JPN_35427" & floor(year) == 2011
	replace shock = 1 if ihme_loc_id == "JPN_35430" & floor(year) == 2011
	replace shock = 1 if ihme_loc_id == "JPN_35451" & floor(year) == 1995


** **********************
** Mark outliers
** **********************
	tostring(source_date), replace

	replace exclude = 0 if exclude == .

	** weird VR points - 12/5/2016
	replace exclude = 1 if ihme_loc_id == "GBR" & year >= 2014 & source_type == "VR"

	replace exclude = 1 if ihme_loc_id == "AFG" & source_type == "SIBLING_HISTORIES" & deaths_source == "DHS"
	replace exclude = 1 if ihme_loc_id == "AGO" & source_type == "VR" & year <=1965 & year >= 1961
	replace exclude = 1 if ihme_loc_id == "AND" & source_type == "VR" & year <= 1952

	replace exclude = 1 if ihme_loc_id == "BFA" & source_type == "SURVEY" & year == 1960
	replace exclude = 1 if ihme_loc_id == "BGD" & source_type == "SURVEY"
	replace exclude = 1 if ihme_loc_id == "BGD" & deaths_source == "DHS 2001" & year == 2001
	replace exclude = 1 if ihme_loc_id == "BGD" & source_type == "SRS" & inlist(year, 1982, 1990, 2002)
	replace exclude = 1 if ihme_loc_id == "BLR" & deaths_source == "HMD" & inlist(year,2013,2014)
	replace exclude = 1 if ihme_loc_id == "BRA" & source_type == "CENSUS" & year == 2010
	replace exclude = 1 if regexm(ihme_loc_id,"BRA_") & deaths_source == "dhs_bra" 
	replace exclude = 1 if ihme_loc_id == "BRA_4755" & source_type == "VR" & year < 1986
	replace exclude = 1 if ihme_loc_id == "BRA_4758" & source_type == "VR" & (year < 1983 | inlist(year,1990,1991))
	replace exclude = 1 if ihme_loc_id == "BRA_4759" & source_type == "VR" & year < 1985
	replace exclude = 1 if ihme_loc_id == "BRA_4761" & source_type == "VR" & year == 1979
	replace exclude = 1 if ihme_loc_id == "BRA_4763" & source_type == "VR" & year == 1979
	replace exclude = 1 if ihme_loc_id == "BRA_4767" & source_type == "VR" & year < 1982
	replace exclude = 1 if ihme_loc_id == "BRA_4769" & source_type == "VR" & year == 1979
	replace exclude = 1 if ihme_loc_id == "BRA_4776" & source_type == "VR" & year == 1990
	replace exclude = 1 if ihme_loc_id == "BRN" & year < 1960.5
	replace exclude = 1 if ihme_loc_id == "BTN" & year == 2005 & source_type == "CENSUS"
	replace exclude = 1 if ihme_loc_id == "BWA" & deaths_source == "21970#bwa_demographic_survey_2006" & year == 2006
	replace exclude = 1 if ihme_loc_id == "BWA" & deaths_source == "DYB" & inlist(year,2001,2007)
	replace exclude = 1 if ihme_loc_id == "BWA" & source_type == "HOUSEHOLD" & year == 1981

replace exclude = 1 if ihme_loc_id == "CAF" & source_type == "SIBLING_HISTORIES" & year == 1984.5
replace exclude = 1 if ihme_loc_id == "CHN_44533" & source_type == "DSP" & year <= 1994
replace exclude = 1 if ihme_loc_id == "CHN_361" & year < 1970 & adj45q15 > 0.4
replace exclude = 1 if ihme_loc_id == "CHN_361" & source_type == "VR" & year < 1970
replace exclude = 1 if ihme_loc_id == "CMR" & deaths_source == "105633#CMR 1976 Census IPUMS" & source_type == "household" & year == 1976
replace exclude = 1 if ihme_loc_id == "COL" & source_type == "VR" & (year == 1979 | year == 1980)
replace exclude = 1 if ihme_loc_id == "COG" & year == 1984 & source_type == "HOUSEHOLD"
replace exclude = 1 if ihme_loc_id == "CPV" & source_type == "VR" & year < 1980
replace exclude = 1 if ihme_loc_id == "CYP" & source_type == "VR" & year == 1975 & sex == "female"

replace exclude = 1 if ihme_loc_id == "DOM" & source_type == "VR" & deaths_source == "WHO_causesofdeath" & year >= 2006 & year <= 2009
replace exclude = 1 if ihme_loc_id == "DOM" & deaths_source == "DOM_ENHOGAR" & year==2001 
replace exclude = 1 if ihme_loc_id == "DOM" & deaths_source == "77819#DOM_DHS_2013" & year==2011
replace exclude = 1 if ihme_loc_id == "DZA" & source_type == "HOUSEHOLD" & year == 1991

replace exclude = 1 if ihme_loc_id == "ECU" & source_type == "HOUSEHOLD" & deaths_source == "153674#ECU_ENSANUT_2012" & year == 2011
replace exclude = 1 if ihme_loc_id == "ETH" & deaths_source == "Ethiopia 2007 Census" & source_type == "HOUSEHOLD_DEATHS"
replace exclude = 1 if ihme_loc_id == "EGY" & adj45q15 < 0.15 & sex == "female" & year < 1965
replace exclude = 1 if ihme_loc_id == "EGY" & adj45q15 < 0.25 & sex == "male" & year < 1965
replace exclude = 1 if ihme_loc_id == "EGY" & source_type=="HOUSEHOLD" & year == 1990 
replace exclude = 1 if ihme_loc_id == "ERI" & (year == 2001 | year == 1994) & source_type == "HOUSEHOLD"

replace exclude = 1 if ihme_loc_id =="GBR" & year==2000
replace exclude = 1 if ihme_loc_id == "GHA" & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "GNQ" & source_type == "VR"

replace exclude = 1 if ihme_loc_id == "HND" & source_type == "VR" & year == 1989
replace exclude = 1 if ihme_loc_id == "HND" & source_type == "VR" & year >= 2008
replace exclude = 1 if ihme_loc_id == "HND" & source_type == "HOUSEHOLD" & year == 2001 
replace exclude = 1 if ihme_loc_id == "HTI" & (source_type == "VR" | source_type == "SURVEY")
replace exclude = 1 if ihme_loc_id == "HTI" & source_type == "HOUSEHOLD" & inlist(year,2005,2006)

replace exclude = 1 if ihme_loc_id == "IDN" & source_type == "HOUSEHOLD_DEATHS"

replace exclude = 1 if ihme_loc_id == "IDN" & source_type == "SUPAS"
replace exclude = 1 if ihme_loc_id == "IDN" & source_type == "SUSENAS"
replace exclude = 1 if ihme_loc_id == "IDN" & source_type == "SURVEY"
replace exclude = 1 if ihme_loc_id == "IDN" & source_type == "2000_CENS_SURVEY"

replace exclude = 1 if ihme_loc_id == "IND" & source_type == "SRS" & (year == 1970 | year == 1971)
replace exclude = 1 if ihme_loc_id == "IND" & source_type == "HOUSEHOLD_DEATHS"

replace exclude = 1 if ihme_loc_id == "IND_43883" & (year==2001 | year==2002) & sex=="male"
replace exclude = 1 if ihme_loc_id == "IND_43883" & (year==1999) & sex=="female"
replace exclude = 1 if ihme_loc_id == "IND_43899" & (year==1997) & sex=="male"
replace exclude = 1 if ihme_loc_id == "IND_43916" & (year==2004 | year==2006) & sex=="female"
replace exclude = 1 if ihme_loc_id == "IND_43919" & (year==1999) & sex=="male"
replace exclude = 1 if ihme_loc_id == "IND_43932" & (year==2002) & sex=="female"
replace exclude = 1 if ihme_loc_id == "IND_4852" & (year==1999) & sex=="female"
replace exclude = 1 if ihme_loc_id == "IND_4853" & (year==2007) & sex=="female"
replace exclude = 1 if strmatch(ihme_loc_id, "IND*") & source_type=="HOUSEHOLD"

replace exclude = 1 if ihme_loc_id == "IND" & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "IRN" & source_type == "VR" & year == 1986
replace exclude = 1 if ihme_loc_id == "IRN" & source_type == "VR" & year >= 2004 & year < 2015 
replace exclude = 1 if ihme_loc_id == "IRQ" & source_type == "VR" & year == 2008

replace exclude = 1 if ihme_loc_id == "JAM" & source_type == "VR" & inlist(year, 2005)

replace exclude = 1 if ihme_loc_id == "JOR" & deaths_source == "DHS_1990" & source_type == "HOUSEHOLD_DEATHS"

replace exclude = 1 if ihme_loc_id == "KEN" & source_type == "VR"
replace exclude = 1 if regexm(ihme_loc_id,"KEN") & source_type == "HOUSEHOLD" & year == 2008
replace exclude = 1 if ihme_loc_id == "KEN" & deaths_source == "133219#KEN_AIS_2007" & year == 2007
replace exclude = 1 if ihme_loc_id == "KIR" & deaths_source == "193927#KIR_CENSUS_2010"
replace exclude = 1 if ihme_loc_id == "KHM" & source_type == "CENSUS" & year == 2008 
replace exclude = 1 if ihme_loc_id == "KHM" & source_type == "HOUSEHOLD" & year == 1996
replace exclude = 1 if ihme_loc_id == "KOR" & year < 1977

replace exclude = 1 if ihme_loc_id == "LBR" & source_type == "SURVEY" & year == 1970
replace exclude = 1 if ihme_loc_id == "LBY" & deaths_source == "7761#LBY_papchild_1995"
replace exclude = 1 if ihme_loc_id == "LKA" & source_type == "VR" & (year == 2005 | year == 2006)

replace exclude = 1 if ihme_loc_id == "MAR" & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "MAR" & source_type == "HOUSEHOLD" & year == 1996 
// replace exclude = 1 if ihme_loc_id == "MDG" & year >= 1977.5 & year <= 1981.5 & source_type == "SIBLING_HISTORIES"
replace exclude = 1 if ihme_loc_id == "MEX_4671" & year < 1985
replace exclude = 1 if ihme_loc_id == "MEX_4657" & year < 1985 & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "MEX_4664" & year <= 1982 & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "MEX_4668" & year <= 1983 & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "MEX_4649" & year == 1983 & source_type == "VR" // Sudden drop in VR that isn't matched on either side
	replace exclude = 1 if ihme_loc_id == "MEX_4674" & year == 2012 & source_type == "VR"
	replace exclude = 1 if ihme_loc_id == "MMR" & source_type == "VR"
	replace exclude = 1 if ihme_loc_id == "MOZ" & source_type == "VR"
	replace exclude = 1 if ihme_loc_id == "MRT" & source_type == "HOUSEHOLD" & year == 1989

	replace exclude = 1 if ihme_loc_id == "NAM" & deaths_source == "134132#NAM_CENSUS_2011"
	replace exclude = 1 if ihme_loc_id == "NGA" & deaths_source == "NGA_GHS" & year == 2006
	replace exclude = 1 if ihme_loc_id == "NGA" & deaths_source == "NGA_MCSS" & year == 2000
	replace exclude = 1 if ihme_loc_id == "NGA" & source_type == "VR" & deaths_source == "WHO_causesofdeath" & year == 2007
	replace exclude = 1 if ihme_loc_id == "NGA" & source_type == "HOUSEHOLD" & year == 2013 
	replace exclude = 1 if ihme_loc_id == "NIC" & source_type == "HOUSEHOLD" & year == 2000
	replace exclude = 1 if ihme_loc_id == "NPL" & source_type == "SIBLING_HISTORIES" & year == 2005.5 & deaths_source != "dhs_npl"

	replace exclude = 1 if ihme_loc_id == "OMN" & source_type == "VR" & year <= 2004

	replace exclude = 1 if ihme_loc_id == "PAK" & source_type == "SURVEY"
	replace exclude = 1 if ihme_loc_id == "PAK" & deaths_source == "PAK_demographic_survey"
	replace exclude = 1 if ihme_loc_id == "PAN" & source_type == "CENSUS"
	replace exclude = 1 if ihme_loc_id == "PER" & source_type == "SIBLING_HISTORIES" & (nid==210231 | nid==209930)

	replace exclude = 1 if ihme_loc_id == "PRK" & source_type == "CENSUS" & year == 1993
	replace exclude = 1 if ihme_loc_id == "PRY" & source_type == "VR" & (year == 1950 | year == 1992)

	replace exclude = 1 if ihme_loc_id == "RWA" & source_type == "CENSUS"
	replace exclude = 1 if ihme_loc_id == "RWA" & source_type == "HOUSEHOLD" & year == 2005 

	replace exclude = 1 if regexm(ihme_loc_id,"SAU") & source_type == "HOUSEHOLD" & year == 2007
	replace exclude = 1 if regexm(ihme_loc_id,"SAU") & source_type == "SURVEY" & year == 2006 
	replace exclude = 1 if ihme_loc_id == "SAU_44542" & source_type == "VR" & year == 2000 
	replace exclude = 1 if ihme_loc_id == "SAU_44543" & source_type == "VR" & inlist(year,2000,2002,2003)
replace exclude = 1 if ihme_loc_id == "SAU_44544" & source_type == "VR" & year == 2000 // Big spike in 45q15
replace exclude = 1 if ihme_loc_id == "SAU_44546" & source_type == "VR" & inlist(year,1999,2000,2001) 
	replace exclude = 1 if ihme_loc_id == "SAU_44547" & source_type == "VR" & inlist(year,1999,2000,2009)
	replace exclude = 1 if ihme_loc_id == "SAU_44548" & source_type == "VR" & inlist(year,1999,2000,2001)
	replace exclude = 1 if ihme_loc_id == "SAU_44549" & source_type == "VR" & inlist(year,1999,2000,2001)
	replace exclude = 1 if ihme_loc_id == "SAU_44553" & source_type == "VR" & inlist(year,1999,2000,2001)
	replace exclude = 1 if ihme_loc_id == "SDN" & source_type == "CENSUS" & year == 2008
	replace exclude = 1 if ihme_loc_id == "SDN" & source_type == "HOUSEHOLD" & year == 1992
	replace exclude = 1 if ihme_loc_id == "SEN" & source_type == "CENSUS"
	replace exclude = 1 if ihme_loc_id == "SLB" & source_type == "HOUSEHOLD" & year == 2009
	replace exclude = 1 if ihme_loc_id == "SLE" & deaths_source == "DHS" & inlist(year, 1998.5, 1999.5, 2000.5, 2001.5, 2002.5) &  nid == 131467 & sex=="male"
	replace exclude = 1 if ihme_loc_id == "SRB" & source_type == "VR" & year >= 1998 & year <= 2007
	replace exclude = 1 if ihme_loc_id == "SUR" & adj45q15 < 0.2 & year < 1976.5
	replace exclude = 1 if ihme_loc_id == "SYR" & deaths_source == "PAPCHILD" & year == 1993

	replace exclude = 1 if ihme_loc_id == "THA" & source_type == "VR" & (year == 1997 | year == 1998)
	replace exclude = 1 if ihme_loc_id == "THA" & deaths_source == "209221#THA Survey Population Change 2005-200" & year == 2006
	replace exclude = 1 if ihme_loc_id == "TGO" & source_type == "SURVEY"
	replace exclude = 1 if ihme_loc_id == "TGO" & source_type == "HOUSEHOLD" & year == 2010 
	replace exclude = 1 if ihme_loc_id == "TLS" & source_type == "SIBLING_HISTORIES" & year == 1994.5 
	replace exclude = 1 if ihme_loc_id == "TON" & source_type == "VR" & year == 1966
	replace exclude = 1 if ihme_loc_id == "TUN" & source_type == "VR" & inlist(year,2006,2009,2013) 
	replace exclude = 1 if ihme_loc_id == "TUR" & source_type == "SURVEY" & (year == 1967 | year == 1989)
	replace exclude = 1 if ihme_loc_id == "TZA" & source_type == "HOUSEHOLD" & year == 2007

	replace exclude = 1 if ihme_loc_id == "UGA" & floor(year) == 2006 & deaths_source =="21014#UGA_DHS_2006"

	replace exclude = 1 if ihme_loc_id == "VNM" & deaths_source == "PCFPS" & year == 2006.5
	replace exclude = 1 if ihme_loc_id == "VCT" & year == 2009

	replace exclude = 1 if ihme_loc_id == "YEM" & source_type == "HOUSEHOLD" & year == 1990 

	replace exclude = 1 if ihme_loc_id == "ZAF" & source_type == "HOUSEHOLD" & inlist(year,2007,2009)
	replace exclude = 1 if ihme_loc_id == "ZAF" & source_type == "CENSUS" & year == 2000 
	replace exclude = 1 if ihme_loc_id == "ZMB" & deaths_source == "ZMB_LCMS"
	replace exclude = 1 if ihme_loc_id == "ZMB" & deaths_source == "ZMB_SBS"
	replace exclude = 1 if ihme_loc_id == "ZMB" & deaths_source == "ZMB_HHC"
	replace exclude = 1 if ihme_loc_id == "ZMB" & deaths_source == "21117#ZMB_DHS_2007"

	replace exclude = 1 if inlist(ihme_loc_id, "CHN_493", "CHN_497","CHN_499") & source_type == "CENSUS" & year == 1982

	** CHN subnational DSP
	replace exclude = 1 if regexm(ihme_loc_id,"CHN_") & ihme_loc_id != "CHN_44533" & year >= 1990 & year <= 1995 & source_type == "DSP"
	replace exclude = 1 if inlist(ihme_loc_id,"CHN_491","CHN_496","CHN_500","CHN_502","CHN_504","CHN_508","CHN_512","CHN_515") & source_type == "DSP" & year >= 1996 & year <= 2003
	replace exclude = 1 if inlist(ihme_loc_id,"CHN_493","CHN_507","CHN_511","CHN_519","CHN_520") & source_type == "DSP" & year >= 1996 & year <= 2003
	replace exclude = 1 if inlist(ihme_loc_id, "CHN_493", "CHN_498", "CHN_499", "CHN_508", "CHN_510", "CHN_512", "CHN_515", "CHN_516") & source_type == "DSP"
	replace exclude = 1 if inlist(ihme_loc_id, "CHN_511") & source_type == "DSP"
	replace exclude = 1 if ihme_loc_id == "CHN_492" & source_type == "DSP" & year == 1996
	replace exclude = 1 if ihme_loc_id == "CHN_44533" & year < 1996 & source_type == "DSP"  
	replace exclude = 1 if ihme_loc_id =="CHN_44533" & (year== 2001 | year==2002) & source_type=="DSP" 
	replace exclude = 1 if ihme_loc_id == "CHN_501"  & year <= 2002 & source_type == "DSP"
	replace exclude = 1 if ihme_loc_id == "CHN_508"  & year <= 2002 & source_type == "DSP"
	replace exclude = 1 if ihme_loc_id == "CHN_514"  & year == floor(2001) & source_type == "DSP" 
	replace exclude = 1 if ihme_loc_id == "CHN_514" & (year == 2012 | year == 2013 | year== 2014) & source_type=="DSP"

	** KEN subnational DHS
	replace exclude = 1 if ihme_loc_id == "KEN_35641" & source_type == "SIBLING_HISTORIES" 
	replace exclude = 1 if ihme_loc_id == "KEN_35641" & source_type == "SIBLING_HISTORIES" 
	replace exclude = 1 if ihme_loc_id == "KEN_35642" & sex=="male" & source_type == "SIBLING_HISTORIES" 
	replace exclude = 1 if ihme_loc_id == "KEN_35659" & sex=="female" & source_type == "SIBLING_HISTORIES" 
	replace exclude = 1 if ihme_loc_id == "KEN_35663" & sex=="male" & source_type == "SIBLING_HISTORIES" 
	replace exclude = 1 if ihme_loc_id == "KEN_35623"
	replace exclude = 1 if ihme_loc_id == "KEN_35628" 
	replace exclude = 1 if ihme_loc_id == "KEN_35636"
	replace exclude = 1 if ihme_loc_id == "KEN_35637" 
	replace exclude = 1 if ihme_loc_id == "KEN_35644"

	replace exclude = 1 if ihme_loc_id == "KEN_35627"
	replace exclude = 1 if ihme_loc_id == "KEN_35631"
	replace exclude = 1 if ihme_loc_id == "KEN_35653"
	replace exclude = 1 if ihme_loc_id == "KEN_35658"

replace exclude = 1 if adjust == 0 & !regexm(source_type,"HOUSEHOLD") & !regexm(source_type,"household") & !regexm(source_type,"SIBLING_HISTORIES")

replace exclude = 0 if ihme_loc_id == "AFG"  & year == floor(1979) & source_type == "CENSUS"

replace exclude = 1 if ihme_loc_id == "BOL" & year == 2012 & source_type == "CENSUS"
replace exclude = 1 if ihme_loc_id == "IDN_4741" & year < 1990
replace exclude = 1 if ihme_loc_id == "MDG" & source_type == "VR" & year > 1980
replace exclude = 1 if ihme_loc_id == "IND_43892" & sex == "male" & source_type == "VR" & year >= 1975
replace exclude = 1 if ihme_loc_id == "IND_43894" & source_type == "VR" & year >= 1975
replace exclude = 1 if ihme_loc_id == "IND_43900" & source_type == "VR" & year <= 1999
replace exclude = 1 if ihme_loc_id == "IND_43903"  & source_type == "VR" & year >= 1975
replace exclude = 1 if ihme_loc_id == "IND_43898"  & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "IND_43901"  & source_type == "VR" & year >= 2000
replace exclude = 1 if ihme_loc_id == "IND_43906"  & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "IDN_4730" & year == 1984 & sex == "male"
replace exclude = 1 if ihme_loc_id == "IDN_4739" & year == 1989 & sex == "male"
replace exclude = 1 if ihme_loc_id == "TLS" & sex == "female" & adj45q15 > 0.4
replace exclude = 1 if ihme_loc_id == "IND_43887" & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "IND_43890" & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "IND_43886" & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "IND_43882" & source_type == "VR" & year <= 1999
replace exclude = 1 if ihme_loc_id == "IND_43881" & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "KEN_35649" & sex == "male" & adj45q15 < 0.1
replace exclude = 1 if ihme_loc_id == "KEN_35652" & sex == "female" & adj45q15 < 0.1
replace exclude = 1 if ihme_loc_id == "KEN_35655" & sex == "male" & adj45q15 < 0.2
replace exclude = 1 if ihme_loc_id == "GIN" & year < 1960
replace exclude = 1 if ihme_loc_id == "TUR" & inlist(year,2011,2010,2009, 1981, 1982, 1983, 1984, 1985)
replace exclude = 1 if ihme_loc_id == "IND_43899" & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "LUX" & year == 2014 & source_type == "VR"
replace exclude = 1 if ihme_loc_id == "KEN_35630" & year == 2005 & sex == "male"
replace exclude = 1 if ihme_loc_id == "KEN_35640" & source_type == "SIBLING_HISTORIES"
replace exclude = 1 if ihme_loc_id == "BGD" & sex == "female" & year == 1990 & source_type == "SIBLING_HISTORIES"
replace exclude = 1 if ihme_loc_id ==  "PAK" & year > 2006
replace exclude = 1 if ihme_loc_id == "IDN_4709" & sex == "male" & adj45q15 > 0.5
replace exclude = 1 if ihme_loc_id == "IDN_4730" & sex == "male" & adj45q15 < 0.1
replace exclude = 1 if ihme_loc_id == "PAK" & year < 1980 & source_type == "SRS"
replace exclude = 1 if ihme_loc_id ==  "PAK" & year == 2006 & sex == "male"
replace exclude = 1 if ihme_loc_id == "KEN_35625" & source_type == "SIBLING_HISTORIES"
replace exclude = 1 if ihme_loc_id == "ZAF_486" & sex == "female" & adj45q15 > 0.3 & source_type == "SIBLING_HISTORIES"
replace exclude = 1 if ihme_loc_id == "ZAF_486" & sex == "female" & adj45q15 < 0.1 & source_type == "SIBLING_HISTORIES"
replace exclude = 1 if ihme_loc_id == "IDN_4737" & sex == "male" & year == 1997 & adj45q15 < 0.1
replace exclude = 1 if ihme_loc_id == "IDN_4737" & sex == "female" & adj45q15 > 0.7
replace exclude = 1 if ihme_loc_id == "KEN_44797" & sex == "female" & adj45q15 > 0.6
replace exclude = 1 if ihme_loc_id == "KEN_44797" & sex == "male" & adj45q15 < 0.2
replace exclude = 1 if ihme_loc_id == "IDN_4739" & sex == "male" & adj45q15 < 0.2

replace exclude = 1 if ihme_loc_id == "KEN_35630" & year == 2005 & sex == "male"
replace exclude = 1 if ihme_loc_id == "TUN" & inlist(year, 2009, 2013)
replace exclude = 1 if ihme_loc_id == "PRY" & year == 1992
replace exclude = 1 if ihme_loc_id == "PSE" & year == 2014


** **********************
  ** Delete scrubs (no longer scrub data, just outlier it)
** **********************
  
  replace exclude = 1 if ihme_loc_id == "YEM" & deaths_source == "PAPCHILD" & source_type == "HOUSEHOLD_DEATHS"
replace exclude = 1 if ihme_loc_id == "PNG" & source_type == "VR"

** **********************
  ** Correct exposure sizes where necessary
** **********************
	preserve
		use "FILEPATH", clear
		keep if source_type == "IHME" & sex != "both"
		egen natl_pop = rowtotal(c1_15to19-c1_55to59)
		keep ihme_loc_id year sex natl_pop
		tempfile pop
		save `pop'
	restore
	replace year = floor(year)
	merge m:1 ihme_loc_id year sex using `pop'
	drop if _m == 2
	drop _m

	** correct nat'l populations where appropriate
	gen correction = 1

	** SRS
	replace correction = 0.006 if inlist(ihme_loc_id,"IND","XIR","XIU") & source_type == "SRS"
	replace correction = 0.003 if ihme_loc_id == "BGD" & source_type == "SRS" 
	replace correction = 0.01 if ihme_loc_id == "PAK" & source_type == "SRS"

	** DSP national
	preserve
		insheet using "FILEPATH", clear
		rename prop_covered correction
		keep if iso3 == "CHN"
		rename iso3 ihme_loc_id
		replace ihme_loc_id = "CHN_44533"
		keep ihme_loc_id year correction
		gen source_type = "DSP"
		expand 10 if year == 1990
		bysort year: replace year = 1979 + _n if _n > 1
		tempfile chn_correction
		save `chn_correction'
	restore
	merge m:1 ihme_loc_id year source_type using `chn_correction', update replace
	drop if _m == 2
	drop _m

	** DSP subnational
	preserve
		** these are sample populations from the actual DSP
		use "FILEPATH", clear
		replace nid = NID
		rename COUNTRY iso3
		merge m:1 iso3 using `iso3_map', keep(1 3) nogen keepusing(ihme_loc_id)
		drop iso3

		append using "FILEPATH"

		ren SEX sex
	  tostring sex, replace
	  replace sex = "male" if sex == "1"
	  replace sex = "female" if sex == "2"
	  ren YEAR year
	  egen sample_pop = rowtotal(DATUM15to19 DATUM20to24 DATUM25to29 DATUM30to34 DATUM35to39 DATUM40to44 DATUM45to49 DATUM50to54 DATUM55to59)
	  gen source_type = "DSP"
	  keep ihme_loc_id sex year sample_pop source_type

		tempfile chn_sub_correction
		save `chn_sub_correction'
	restore

	merge m:1 ihme_loc_id sex year source_type using `chn_sub_correction', nogen
	replace correction = sample_pop/natl_pop if regexm(ihme_loc_id,"CHN_") & ihme_loc_id != "CHN_44533" & source_type == "DSP"
	drop sample_pop
	** Other China sources
	replace correction = 0.001 if ihme_loc_id == "CHN_44533" & (source_type == "SSPC" | deaths_source == "EPI_SURVEY")
	replace correction = 0.01 if ihme_loc_id == "CHN_44533" & source_type == "DC"
	** Other (generic) sources
	replace correction = 0.005 if inlist(source_type, "HOUSEHOLD_DEATHS", "SURVEY", "UNKNOWN") & natl_pop >= 5*10^6
	replace correction = 0.01 if inlist(source_type, "HOUSEHOLD_DEATHS", "SURVEY", "UNKNOWN") & natl_pop < 5*10^6

	replace exposure = correction*natl_pop if inlist(source_type, "SRS", "DSP")
  replace exposure = correction*natl_pop if regexm(deaths_source, "DYB") & inlist(source_type, "SURVEY", "UNKNOWN")

	replace exposure = correction*natl_pop if exposure == . & source_type != "SIBLING_HISTORIES"

	//append DSS
	preserve
	// Get DSS NIDs
	import delimited using "FILEPATH", clear
	keep nid site
	replace site = trim(site)
	tempfile dss_nids
	save `dss_nids', replace
	// Get DSS exposure data
	import delimited using "FILEPATH", clear
	replace site = trim(site)
	rename person_years exposure
	tempfile dss_exposure
	save `dss_exposure', replace
	// Get 45q15 DSS data
	import delimited using "FILEPATH", clear
	replace site = trim(site)
	merge m:1 site using `dss_nids', keep(1 3) nogen
	merge m:1 location_id site year_id sex_id using `dss_exposure', keep(1 3) assert(2 3) nogen
	drop location_id
	generate exclude = 0
	replace exclude = 1 if site == "Ghana - Dodowa HDSS"
	replace exclude = 1 if site == "Ghana - Navrongo HDSS"
	replace exclude = 1 if site == "Mozambique - Manhica HDSS"
	generate correction = 1
	generate shock = 0
	generate source_date = "2017"
	replace nid = 335911
	egen underlying_nid = group(site)
	tostring(underlying_nid), replace
	rename site deaths_source

	rename year_id year
	rename sex_id sex
	tostring sex, replace
	replace sex = "male" if sex == "1"
	replace sex = "female" if sex == "2"
	tempfile DSS
	save `DSS'
	restore
	append using `DSS'

	//save a copy after DSS append
	export delimited using "`run_folder'/outputs/post_dss_checkpoint.csv", replace

	replace exclude = 1 if regexm(ihme_loc_id, "IND") & year >= 2000 & (source_type == "HOUSEHOLD_DLHS" | source_type=="DSS")

** **********************
** Format and save
** **********************
	merge m:1 ihme_loc_id using `iso3_map', keepusing(location_id country)
levelsof ihme_loc_id if _merge == 2
keep if _m == 3
drop _m

label define comp 0 "unadjusted" 1 "ddm_adjusted" 2 "gb_adjusted" 3 "complete"
label values adjust comp

order location_id country ihme_loc_id year sex source_type deaths_source nid adjust comp sd adj45q15 obs45q15 exclude shock exposure
sort ihme_loc_id year sex source_type
local date = c(current_date)
local date = subinstr("`date'", " ", "_", 2)

export delimited using "`run_folder'/outputs/raw_45q15.csv", replace