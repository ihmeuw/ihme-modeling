clear all
set more off
cap restore, not
	
if c(os)=="Windows" local root = "FILEPATH"
if c(os)=="Unix"    local root = "FILEPATH"

** cd into input dir
cd "FILEPATH"

run "FILEPATH/get_locations.ado"
get_locations, level(all)
keep location_name ihme_loc_id location_type location_id
gen gbd_country_iso3 = substr(ihme_loc_id, 1, 3) if inlist(location_type, "USERNAME", "USERNAME")
keep location_name ihme_loc_id gbd_country_iso3 location_id
duplicates drop
tempfile countrycodes 
save `countrycodes', replace

// if you want WHO VR vs. COD VR comparison graphs, change this to "yes"
local whocomp = "no"
local whocompfile = "FILEPATH/USABLE_ALL_AGE_DEATHS_VR_WHO_1950-2011.dta"

// if you want new vs. current COD VR comparison graphs, change this to "yes" and change the archived data file path you want to compare it with
// newcurrentall means you'll get every country-year of VR data by agegroup: this takes about 45 minutes to run
// if you have newcurrent = yes and newcurrentall = no and percentdiffcriteria = 1, you'll end up with graphs by age group for each country-year
//	that has a difference between current and new VR of more than 1%
local newcurrent = "yes"
local newcurrentall = "no"
local percentdiffcriteria = 1

local archivedcompfile = "FILEPATH/USABLE_ALL_AGE_DEATHS_VR_WHO_GLOBAL_vICD7-10_ 7 Mar 2016.dta"

** read in the shock deduplicated VR file from COD
use "FILEPATH/VR_with_cause_shocks_deduplicated.dta", clear 
merge m:1 location_id using `countrycodes', keep(3) nogen

duplicates tag ihme_loc_id sex year source, gen(dup)
drop if dup == 1 & source == "US_NCHS_counties_ICD10" & year == 2003

drop dup
duplicates tag ihme_loc_id sex year , gen(dup)

egen deaths1 = rowtotal(deaths*)
sort ihme year sex deaths1

drop if ihme == "GEO" & source == "Georgia_VR" & dup ==1
drop if ihme == "VIR" & source == "ICD9_BTL" & dup ==1
drop if ihme == "PHL" & source == "Philippines_2006_2012" & dup ==1
drop if ihme_loc_id == "THA" & source == "Thailand_Public_Health_Statistics" & dup == 1
drop if ihme_loc_id == "RUS" & source == "Russia_FMD_1999_2011" & dup == 1
drop if ihme_loc_id == "MNG" & source == "Other_Maternal"
drop if ihme_loc_id == "IRN" & source == "ICD10" & dup == 1

drop dup
isid ihme_loc_id year sex
drop deaths1

drop if gbd_country_iso3 == "CHN" & (inlist(NID, 127808, 127809, 127810, 127811, 127812, 127813, 127814, 127815) | ///
	inlist(NID, 128814, 128815, 128816, 128817, 128818, 128819, 128820, 128821, 128822) | inlist(NID, 128823, 128824, 128825, 128831, 223780, 223781))
drop if source == "Middle_East_Maternal"

// get totals for each country	
preserve
	keep if (ihme_loc_id != gbd_country_iso3 & gbd_country != "") 
	keep if regexm(ihme_loc_id, "BRA") | (regexm(ihme_loc_id, "GBR") & !regexm(source, "UTLA")) | regexm(ihme_loc_id, "MEX") | regexm(ihme_loc_id, "JPN") | ///
	 regexm(ihme_loc_id, "SWE") | regexm(ihme_loc_id, "USA") | regexm(ihme_loc_id, "SAU") | regexm(ihme_loc_id, "ZAF") | regexm(ihme_loc_id, "CHN")

	drop if year < 1979 & regexm(ihme, "GBR_")
	drop if year == 2012 & regexm(ihme, "SAU_")
	
	// for GBR, there are 2 sources used for subnational: you can add the sources together because they're for different provinces. make sure this is still the case, with the below "assert"
	// if there are duplicates, you'll have to choose which source to use and drop one of them.
	duplicates tag gbd_country_iso3 ihme_loc_id year sex, gen(dup)
	assert dup == 0
	drop dup
	collapse (sum) deaths*, by(gbd_country_iso3 year sex)
	generate ihme_loc_id = gbd_country_iso3
	replace ihme_loc_id = "CHN_44533" if ihme == "CHN"
	replace gbd_country_iso3 = ""
	tempfile nationals_calculated
	save `nationals_calculated', replace

restore	
append using `nationals_calculated'
duplicates tag ihme_loc_id year sex, gen(dup)
drop dup


** *******************************************************************************************************
// dropping points that are OK for COD based on good Cause Fractions but not great for Mortality in overall sample size/ mortality rate

// 1. these points are from a Hospital Data source that the Registrar of India communicated was actually VR
drop if ihme_loc_id == "IND"

// drop VR point in ZWE -- this is a urban point and should not be used
drop if ihme_loc_id == "ZWE"	
		
// dropping the COD data and using the raw WHO data instead here
drop if ihme_loc_id == "STP" & source == "ICD9_BTL" & year == 1985
drop if ihme_loc_id == "BHR" & source == "ICD9_BTL" & year == 1986
drop if ihme_loc_id == "HTI" & source == "ICD9_BTL" & year == 1981
drop if ihme_loc_id == "SYR" & source == "ICD9_BTL" & year == 1980
drop if ihme_loc_id == "TON" & source == "Tonga_2002_2004" & year == 2003

// dropping hospital data from GEO & other 2004 points that were too high
drop if ihme_loc_id == "GEO" & source =="hospital workshop" & year == 2004
drop if ihme_loc_id == "GEO" & source == "morticd10" & year == 2004

// drop DOM 2002 points that were incredibly low
drop if ihme_loc_id == "DOM" & year == 2002 & source == "morticd10"

// don't want to have variable sources in the 1980's for PHL
drop if ihme_loc_id == "PHL" & year > 1980 & year < 1990 & source != "Philippines_1979_2000"

// 1950 point in japan is unreasonably low
drop if ihme_loc_id == "JPN" & year == 1950

// Drop points where there is a larger than 1% difference with the WHO databank and we trust WHO over COD sources
// Often they perfer datasource by quality of cause fraction which does not always translate to better all-cause mort data
drop if ihme_loc_id == "ALB" & year >= 2005
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
drop if ihme_loc_id == "CHN_361" & year == 1994
drop if ihme_loc_id == "MEX" & year >= 1979 & year <= 2012
drop if ihme_loc_id == "MNG" & year >= 2004 & year <= 2008
drop if ihme_loc_id == "MNE" & year == 2006
drop if ihme_loc_id == "OMN" & year == 2009
drop if ihme_loc_id == "PER" & inlist(year, 2002, 2003, 2004, 2005)
drop if ihme_loc_id == "SAU" & year == 2009
drop if ihme_loc_id == "SLV" & year == 1992
drop if ihme_loc_id == "SYC" & inlist(year, 2005, 2006)
drop if ihme_loc_id == "SGP" & year == 2003
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

	** Romania, death numbers are too low pre-1969
	drop if ihme_loc_id == "ROU" & source == "ICD7A" & year <=1968
	** Russia: the CoD source isn't WHO, so they won't align. Keep the WHO source post 1989
	drop if ihme_loc_id == "RUS" & source == "Russia_FMD_1989_1998" & NID == 133400 & (year >=1989 & year <=1998)
	** Honduras: the CoD source is much lower than the new WHO estimates for 1966
	drop if ihme_loc_id == "HND" & source == "ICD7A" & year == 1966
	** Bolivia: The CoD VR data are  higher than the WHO raw data
	drop if ihme_loc_id == "BOL" & inlist(year, 2000, 2001, 2002, 2003) & source == "ICD10" & NID == 156411

	** Algeria after 2000, deaths are much lower
	drop if ihme_loc_id == "DZA" & NID == 108858 & inlist(year, 2005, 2006)
	** Argentina 1966 and 1967, deaths are lower than trend
	drop if ihme_loc_id == "ARG" & source == "ICD7A" & inlist(year, 1966, 1967)
	** Brazil Paraiba: deaths are much lower in 1979
	drop if ihme_loc_id == "BRA_4764" & NID == 153001 & year == 1979
	** Fiji, much lower than nearby year
	drop if ihme_loc_id == "FJI" & source == "ICD9_BTL" & year == 1999
	** Germany: deaths are super low from ICD8A
	drop if ihme_loc_id == "DEU" & source == "ICD8A" & inlist(year, 1968, 1969, 1970)
	** Israel: deaths are too low before 1974
	drop if ihme_loc_id == "ISR" & inlist(source, "ICD7A", "ICD8A") & year <=1974 & year >= 1954
	** Malaysia, deaths are much lower than previous VR
	drop if ihme_loc_id == "MYS" & (year >=1997 & year <=2008)
	** portugal: 2005/2006 are much lower than the trend
	drop if ihme_loc_id == "PRT" & source == "ICD10" & inlist(year, 2005, 2006)
	** South Korea ICD9_BTL different level and trend than ICD 10 and previous data
	drop if ihme_loc_id == "KOR" & source == "ICD9_BTL" & year <=1994
	** UK regions huge spike in deaths in 1995
	drop if (inlist(ihme_loc_id, "GBR_4621", "GBR_4623", "GBR_4624", "GBR_4618", "GBR_4619", "GBR_4625", "GBR_4626", "GBR_4622", "GBR_4620") | ihme_loc_id == "GBR_4636") & source == "UK_deprivation_1981_2000" & year == 1995
	drop if ihme_loc_id == "GBR" & year == 1995
	** trust WHO more than CoD in BRA, no CoD national total
*	drop if ihme_loc_id == "BRA" & year < 2012
	** drop CoD VR for MAR where we have DYB
	drop if ihme_loc_id == "MAR" & source == "Morocco_Health_In_Figures" & inlist(year, 1996, 1997, 2001)
	** drop VIR ICD9 BTL because it's unrealistically high
	drop if ihme_loc_id == "VIR" & source == "ICD9_BTL" & year == 1980

** **************************************************************************************	
// check that none of the variables are missing any death data		
	foreach var of varlist deaths* {
		count if `var' == .
		di in red "`var' has `r(N)' missing"
		assert `var' != .
	}
	
// format deaths into standard morality data structure: the data are in COD format 2	
//  generating terminal age group - although all of this data is theoretically formatting in COD format 2
// there are often cases when deaths26 does not have any deaths -- Need to determine terminal agegroup manually
	gen terminal = .
	replace terminal = 25 if deaths25 != 0
	forvalues j = 24(-1)7 {
		local jplus = `j'+1
		local jplus2 = `j'+2
		replace terminal = `j' if deaths`j' != 0 & deaths`jplus' == 0 & terminal == . & `j' == 24
		if `j' < 24 {
		replace terminal = `j' if deaths`j' != 0 & deaths`jplus' == 0 & deaths`jplus2' == 0 & terminal == . & `j' < 25
		}
	}

// format deaths into standard morality data structure
	forvalues j = 7/25 {
		local k = (`j'-6)*5
		local k4 = `k' + 4
	
		cap: gen DATUM`k'to`k4' = .
		replace DATUM`k'to`k4' = deaths`j' if `j' < terminal 
	
		cap: gen DATUM`k'plus = .
		replace DATUM`k'plus = deaths`j' if `j' == terminal 
	}	

	gen DATUM0to0 = deaths91 + deaths93 + deaths94
	gen DATUM1to4 = deaths3
	
	looUSER DATUM
	foreach var of varlist `r(varlist)' {
		summ `var'
		if `r(N)' == 0 {
			drop `var'
		}
	}

	drop deaths* 
	cap drop DATUM95to99
		** 95to99 got generated from some SWE observations that only have under1 data -- drop it

*************************
// Step 2: general formatting 

	// merge in country name
	merge m:1 ihme_loc_id using `countrycodes'
	replace location_name = "Hong Kong Special Administrative Region of China" if ihme_loc_id == "HKG"
	replace location_name = "Macao Special Administrative Region of China" if ihme_loc_id == "MAC"
	** assert _m !=1
	drop if _m == 2			// this drops non-ihme indicator countries and observations that are specific to DEU-WB
	drop _m
	
	gen VR_SOURCE = "WHO_causesofdeath"
	rename ihme_loc_id COUNTRY
	rename sex SEX
	rename year YEAR
	g FOOTNOTE = ""
	
	// data check 
	duplicates tag COUNTRY YEAR SEX, g(dup)		
	assert dup == 0				
	// if code breaks here, figure out why duplicates have been introduced and fix 	
	drop dup
	
	drop source terminal location_name gbd_country_iso3 location_id iso3
	
	// add in unknown deaths and total deaths
	cap g DATUMUNK = .
	cap drop DATUMTOT
	egen DATUMTOT = rowtotal(DATUM*)
	assert DATUMTOT != .
	
	// add in both sexes combined
	preserve
	looUSER DATUM
	foreach var of varlist `r(varlist)' {
		replace `var' = -10000000 if `var' == .
	}
	collapse (sum) DATUM*, by(COUNTRY YEAR VR_SOURCE FOOTNOTE)
	gen SEX = 0

	looUSER DATUM
	foreach var of varlist `r(varlist)' {
		replace `var' = . if `var' < 0
	}	

	tempfile both
	save `both', replace
	restore

	append using `both'
	
// more general formatting 
	cap drop AREA
	g AREA = 0
	cap drop SUBDIV
	g SUBDIV = "VR"
	
	order COUNTRY SEX YEAR AREA SUBDIV VR_SOURCE NID
	
	if "`dropnid'" == "yes" {
		drop NID
	}

	// saving dataset
	saveold "FILEPATH/USABLE_ALL_AGE_DEATHS_VR_WHO_GLOBAL_vICD7-10.dta", replace
	saveold "FILEPATH/USABLE_ALL_AGE_DEATHS_VR_WHO_GLOBAL_vICD7-10_`date'.dta", replace
	
** **********************************************************************
** ******************************* Graphs *******************************
** **********************************************************************


// graphing code
	cap quietly do "FILEPATH/pdfmaker.do"
	cap quietly do "FILEPATH/pdfmaker_acrobat10.do"
	cap quietly do "FILEPATH/pdfmaker_acrobat11.do"
	
if "`whocomp'" == "yes" {
	******************************
	** WHO vs. new VR
	******************************
		use "FILEPATH/USABLE_ALL_AGE_DEATHS_VR_WHO_GLOBAL_vICD7-10.dta", clear
		renpfix DATUM COD

		sort COUNTRY YEAR SEX
		merge 1:1 COUNTRY YEAR SEX using "`whocompfile'"
		// drop unknown sex observations
		drop if SEX == 9 							
		
	// creating thresholds for graphing significant differences
		g difference = abs(DATUMTOT - CODTOT)
		g percentdiff = difference/DATUMTOT
		tostring percentdiff, force replace format(%12.3f)
		destring percentdiff, replace

		gen sexs = "b" if SEX == 0
		replace sexs = "m" if SEX == 1
		replace sexs = "f" if SEX == 2

		g SEX_COUNTRY = sexs + COUNTRY

		levelsof SEX_COUNTRY if percentdiff >=.01 & percentdiff != ., local(sexcountries) // & percentdiff != ., local(sexcountries)  // take out percentdiff != . if you want to graph years where one source is missing

		replace DATUMTOT = DATUMTOT/(10^6)
		replace CODTOT = CODTOT/(10^6)
		
		* get table for 1 percent difference years
		preserve
		gen indic = 1 if percentdiff >=.01 & percentdiff != .
		keep if indic == 1
		keep COUNTRY SEX YEAR VR_SOURCE version CODTOT DATUMTOT difference percentdiff
		rename DATUMTOT WHOmortTOT
		replace WHOmortTOT = WHOmortTOT*(10^6)
		replace CODTOT = CODTOT*(10^6)
		sort COUNTRY YEAR SEX
		outsheet using "$graphsdir/country-sex-years_1percentdiff.csv", comma replace
		restore
		
	************************************
	** WHO vs. new VR: 1 percent diff
	************************************
	pdfstart using "$graphsdir/comp_graphs_1percent_no missing.pdf"

	foreach sc of local sexcountries {
		di "on `sc'"
		local iso3 = substr("`sc'",2,.)
		local sex = substr("`sc'",1,1)
		if "`sex'" == "b" local sx = "both sexes"
		if "`sex'" == "m" local sx = "males"
		if "`sex'" == "f" local sx = "females" 
		
		scatter DATUMTOT YEAR if COUNTRY == "`iso3'" & sexs == "`sex'" & percentdiff != ., msize(large) msymbol(O)  || ///
		scatter CODTOT YEAR if COUNTRY == "`iso3'" & sexs == "`sex'" & percentdiff != ., msize(large) msymbol(X) ytitle( "Deaths (millions)") ///
		xtitle( "") title( "`iso3' `sx'") subtitle( "Countries with > 1% difference between WHO and COD") legend(label(1 "Raw WHO") label(2 "CoD VR"))
		
		pdfappend
			
	}


	pdffinish, view
	
	************************************
	** WHO vs. new VR: 2 percent diff
	************************************
	// Want to organize first by country, then by sex: looking at 2% difference between COD and WHO VR numbers
	preserve
	gen temp = 1 if percentdiff >=.02 & percentdiff != .
	bysort COUNTRY: egen onepercent_indic = max(temp)  
	drop temp
	keep if onepercent_indic == 1

	levelsof COUNTRY, local(isos)
	levelsof sexs, local(sexes)

	pdfstart using "$graphsdir/comp_graphs_2percent_no missing_byiso3.pdf"
	foreach iso of local isos {
		foreach s of local sexes {
				if "`s'" == "b" local sx = "both sexes"
				if "`s'" == "m" local sx = "males"
				if "`s'" == "f" local sx = "females"
			di "on: `iso' `sx'"
			scatter DATUMTOT YEAR if COUNTRY == "`iso'" & sexs == "`s'" & percentdiff != ., msize(large) msymbol(O)  || ///
			scatter CODTOT YEAR if COUNTRY == "`iso'" & sexs == "`s'" & percentdiff != ., msize(large) msymbol(X) ytitle( "Deaths (millions)") ///
			scheme(s1color) xtitle( "") title( "`iso' `sx'") subtitle( "Countries with > 2% difference between WHO and COD") legend(label(1 "Raw WHO") label(2 "CoD VR"))
			
			pdfappend
		}
	}
	pdffinish, view

	restore
	
		* get table for 1 percent difference years
		preserve
		gen indic = 1 if percentdiff >=.02 & percentdiff != .
		keep if indic == 1
		keep COUNTRY SEX YEAR VR_SOURCE version CODTOT DATUMTOT difference percentdiff
		rename DATUMTOT WHOmortTOT
		replace WHOmortTOT = WHOmortTOT*(10^6)
		replace CODTOT = CODTOT*(10^6)
		sort COUNTRY YEAR SEX
		outsheet using "$graphsdir/country-sex-years_2percentdiff.csv", comma replace
		restore
}
if "`whocomp'" == "no" {
	di in red "not making WHO comparison"
}
******************************
** Current vs. new VR
******************************
if "`newcurrent'" == "yes" {
	// Graph for all age groups combined (but by sex), scatter plots of old vs new vr data with an equiavlence line
	use "FILEPATH/USABLE_ALL_AGE_DEATHS_VR_WHO_GLOBAL_vICD7-10.dta", clear
		renpfix DATUM NEW
		sort COUNTRY YEAR SEX
		
	merge 1:1 COUNTRY YEAR SEX using "`archivedcompfile'"
		qui count if _m==1
		di in red "There are `r(N)' points only in the new data"
		qui count if _m==2 
		di in red "There are `r(N)' points only in the old data"
		
		
		preserve
		// get a list of dropped country-years
			keep if _m==2
			keep COUNTRY SEX YEAR
			outsheet using "$graphsdir/dropped_country_sex_years.csv", comma replace
		restore 

		replace DATUMTOT = DATUMTOT/(10^6)
		replace NEWTOT = NEWTOT/(10^6)
		
	levelsof COUNTRY, local(isos)
	levelsof SEX, local(sexes)

	********************************************************
	******* scatter versions against each other ************
	********************************************************
	pdfstart using "$graphsdir/comp_graphs_current_and_new_VR.pdf"

	foreach iso of local isos {
		di "`iso'"
		foreach s of local sexes {
				if `s' == 0 local sx = "both sexes"
				if `s' == 1 local sx = "males"
				if `s' == 2 local sx = "females"
			
			* make sure that there are observations for both new and old data; if not, can't do this graph
			* if the local of the total is equal to 2, that means that both the min and max are 1, as in, all of the observations are missing
			qui tabmiss NEWTOT if COUNTRY == "`iso'" & SEX == `s'
			local newtotal = `r(min)'+`r(max)'
			
			qui tabmiss DATUMTOT if COUNTRY == "`iso'" & SEX == `s'
			local oldtotal = `r(min)'+`r(max)'
			
			if `newtotal' != 2 & `oldtotal' != 2 {
			 
				tw scatter NEWTOT NEWTOT if COUNTRY == "`iso'" & SEX == `s' & _m==1, msize(large) msymbol(O) mcol(blue) || ///
					scatter DATUMTOT DATUMTOT if COUNTRY == "`iso'" & SEX == `s' & _m==2, msize(large) msymbol(O) mcol(red) || ///
					scatter DATUMTOT NEWTOT if COUNTRY == "`iso'" & SEX == `s' & _m==3, msize(large) msymbol(Oh) mcol(green) || ///
					function y=x if COUNTRY == "`iso'" & SEX == `s', range(NEWTOT) ///
				scheme(s1color) xtitle( "Deaths in Millions (New COD VR)") ytitle( "Deaths in Millions (Current COD VR)") title( "`iso' `sx'") ///
				subtitle( "Comparing currently used and new COD VR") ///
				legend(label(1 "New VR") label(2 "Dropped VR") label(3 "Same VR") label(4 "Equivalence line"))
				
				pdfappend
		 	}
			else {
				di "cannot make graph for `iso' `sx' because one of the datasets does not have this country"
			} 
		}
	}
	pdffinish, view	
	
	if "`newcurrentall'" == "yes" {
	*******************************************************************
	******* scatter age-specific deaths by sex and version ************
	*******************************************************************	
	use "FILEPATH/USABLE_ALL_AGE_DEATHS_VR_WHO_GLOBAL_vICD7-10.dta", clear
		gen time = "new"
		
	append using "`archivedcompfile'"
		replace time = "current" if time == ""
		
	reshape long DATUM, i(CO SEX YEAR AREA SUBDIV VR time) j(age, string)
	split age, parse( "to") 
	
		* to be able to destring age...
		replace age1 = "71" if age1 == "70plus"
		replace age1 = "76" if age1 == "75plus"
		replace age1 = "81" if age1 == "80plus"
		replace age1 = "86" if age1 == "85plus"
		replace age1 = "91" if age1 == "90plus"
		replace age1 = "96" if age1 == "95plus"

		drop if age1 == "UNK" | age1 == "TOT"
		destring age1, replace
	
	levelsof CO, local(isos)
	
	replace DATUM = DATUM/(10^6)
	
	tempfile prep
	save `prep', replace
	
	pdfstart using "$graphsdir/comp_graphs_current_and_new_VR_byage.pdf"

	foreach iso of local isos {
		preserve
		keep if CO == "`iso'"
		di in red "`iso'"
		levelsof YEAR, local(years)
		foreach yr of local years {
			
			tw scatter DATUM age1 if CO == "`iso'" & YEAR == `yr' & time == "current" & SEX == 1, mcol(blue) msymb(T) || ///
				scatter DATUM age1 if CO == "`iso'" & YEAR == `yr' & time == "current" & SEX == 2, mcol(green) msymb(S) || ///
				scatter DATUM age1 if CO == "`iso'" & YEAR == `yr' & time == "new" & SEX == 1, mcol(orange) msymb(Th) || ///
				scatter DATUM age1 if CO == "`iso'" & YEAR == `yr' & time == "new" & SEX == 2, mcol(red) msymb(Sh) ///
				legend(label(1 "Current, males") label(2 "Current, females") label(3 "New, males") label(4 "New, females")) ///
				xtitle( "Age") ytitle( "Deaths in Millions") title( "`iso' `yr'") ///
				subtitle( "Comparing currently used and new COD VR") 
			
			
			pdfappend
		
		}
		restore
	}
	pdffinish, view
	}
	
	
	*******************************************************************
	****** get percent difference between new & current ests **********
	*******************************************************************
	use "FILEPATH/USABLE_ALL_AGE_DEATHS_VR_WHO_GLOBAL_vICD7-10.dta", clear
		gen time = "new"
		
	append using "`archivedcompfile'"
		replace time = "current" if time == ""
		
	drop NID
	reshape long DATUM, i(CO SEX YEAR AREA SUBDIV VR time) j(age, string)
	split age, parse( "to")
	
		* to be able to destring age...
		replace age1 = "71" if age1 == "70plus"
		replace age1 = "76" if age1 == "75plus"
		replace age1 = "81" if age1 == "80plus"
		replace age1 = "86" if age1 == "85plus"
		replace age1 = "91" if age1 == "90plus"
		replace age1 = "96" if age1 == "95plus"
		
		drop if age1 == "UNK" | age1 == "TOT"
		destring age1, replace
	
	levelsof CO, local(isos)
	
	replace DATUM = DATUM/(10^3)
	drop if SEX == 0
	reshape wide DATUM, i(CO SE YEAR AREA SUBDIV VR_S age) j(time, string)
	gen perdiff = ((DATUMnew-DATUMcurrent)/DATUMcurrent)*100
	sort CO YEAR SEX age1
	
	* want to keep only the country-years that have differences greater than the criteria
	gen indic = 0
	replace indic = 1 if perdiff >= `percentdiffcriteria'
	replace indic = 1 if perdiff <= -`percentdiffcriteria'
	replace indic = 0 if perdiff == .
		assert indic == 0 if perdiff == .
		assert indic == 0 if perdiff == 0
	
	bysort COUNTRY YEAR: egen indic2 = max(indic)
	keep if indic2 == 1
	
	levelsof CO, local(isos)
	
	pdfstart using "$graphsdir/comp_graphs_current_and_new_VR_byage_`percentdiffcriteria'percentdiff.pdf"

	foreach iso of local isos {
		preserve
		keep if CO == "`iso'"
		di in red "`iso'"
		levelsof YEAR, local(years)
		foreach yr of local years {
			
			tw scatter DATUMcurrent age1 if CO == "`iso'" & YEAR == `yr' & SEX == 1, mcol(blue) msymb(T) || ///
				scatter DATUMcurrent age1 if CO == "`iso'" & YEAR == `yr' & SEX == 2, mcol(green) msymb(S) || ///
				scatter DATUMnew age1 if CO == "`iso'" & YEAR == `yr' & SEX == 1, mcol(orange) msymb(Th) || ///
				scatter DATUMnew age1 if CO == "`iso'" & YEAR == `yr' & SEX == 2, mcol(red) msymb(Sh) ///
				legend(label(1 "Current, males") label(2 "Current, females") label(3 "New, males") label(4 "New, females")) ///
				xtitle( "Age") ytitle( "Deaths in Thousands") title( "`iso' `yr'") ///
				subtitle( "Comparing currently used and new COD VR") 
			
			
			pdfappend
		
		}
		restore
	}
	pdffinish, view
	
	* get a table of the country-year-sex-ages with a percent difference greater than 1%
	preserve
	replace DATUMcurrent = DATUMcurrent*(10^3)
	replace DATUMnew = DATUMnew*(10^3)
	keep if indic==1
	assert indic2==1
	drop AREA SUBDIV FOOTNOTE indic indic2 age1 age2
	rename DATUM* TOTAL*
	outsheet using "$graphsdir/country-year-sex-age_greaterthan`percentdiffcriteria'percent_diff.csv", comma replace
	restore
}
if "`newcurrent'" == "no" {
	di in red "not making new-to-current comparison"
	} 
	
	