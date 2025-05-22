** ************************************************************************************
** Format the dataset so that CBH and SBH sources can be merged together
** ************************************************************************************
	** keep only necessary variables
	keep ihme_loc_id sourcetype super_region_name region_name year source source_year log10sdq5_cbh sdq5_sbh variance graphingsource
	keep if regexm(sourcetype, "CBH") | regexm(sourcetype, "SBH")

	count if regexm(sourcetype, "SBH")
	local obs = r(N)

	** generate a separate variable for CBH variance to add in later
	gen cbh_var = variance if regexm(sourcetype, "CBH")

	** gen survey indicator to be able to get averages by survey series (this won't include small one-time surveys)
	gen indic_survey = graphingsource
	replace indic_survey = "CDC-RHS" if regexm(source, "cdc rhs") | regexm(source, "cdc-rhs")
	replace indic_survey = "LSMS" if regexm(source, "lsms")
	replace indic_survey = "PAPFAM" if regexm(source, "papfam")
	replace indic_survey = "PAPCHILD" if regexm(source, "papchild")
	replace indic_survey = "SUPAS" if regexm(source, "supas")
	replace indic_survey = "IFLS" if regexm(source, "ifls")
	replace indic_survey = "DHS SP" if regexm(source, "dhs sp")
	replace indic_survey = "ENADID" if regexm(source, "enadid")
	replace indic_survey = "PNAD" if regexm(source, "pnad")
	replace indic_survey = "RHS" if regexm(source, "rhs")
	replace indic_survey = "RHS" if regexm(source, "reproductive health")
	replace indic_survey = "DS" if regexm(source, "demographic") & regexm(source, "survey")
	replace indic_survey = "DS" if regexm(source, "demografica") & regexm(source, "encuesta")
	replace indic_survey = "DS" if regexm(source, "demographique") & regexm(source, "enquete")
	replace indic_survey = "ENSANUT" if regexm(source, "ensanut")
	replace indic_survey = "ENHOGAR" if regexm(source, "enhogar")
	replace indic_survey = "ENIGH" if regexm(source, "enigh")
	replace indic_survey = "ECV" if regexm(source, "ecv")
	replace indic_survey = "DHS ITR" if regexm(source, "dhs itr")
	replace indic_survey = "ENDESA" if regexm(source, "endesa")
	replace indic_survey = "DLHS" if regexm(source, "dlhs")
	replace indic_survey = "CPS" if regexm(source, "contraceptive")
	replace indic_survey = "FS" if regexm(source, "fertility") | regexm(source, "fecondite") | regexm(source, "fecundidad")
	tempfile cbh_sbh
	save `cbh_sbh', replace

	keep if regexm(sourcetype, "CBH")
	ren source source_cbh
	drop log10sdq5_cbh sdq5_sbh variance sourcetype

	** get source years to match SBH
	split source_cbh, parse(" ")

	** fill in this variable with the year integers
	gen source_year2 = source_year
	gen year_range = source_cbh2 if regexm(source_cbh2, "-")
	destring source_cbh2, force replace
	replace source_year2 = source_cbh2 if source_cbh2 != .
	replace year_range = source_cbh3 if regexm(source_cbh3, "-")
	destring source_cbh3, force replace
	replace source_year2 = source_cbh3 if source_cbh3 != .
	split year_range, parse("-")
	destring year_range2 year_range1, force replace
	replace source_year2 = year_range2 if year_range2 != .

	** Standard DHS
	replace source_year2 = year_range1 if year_range1 != . & indic_survey == "Standard_DHS" & (ihme_loc_id == "BDI" | ///
	(ihme == "BOL" & year_range1 == 2003) | (ihme == "BGD" & year_range1 == 2011) | (ihme == "BFA" & year_range1 == 2010) | ///
	(ihme == "CAF" & year_range1 == 1994) | (ihme == "COG" & year_range1 == 2011) | ihme == "EGY" | ///
	(ihme == "ERI" & year_range1 == 1995) | (ihme == "GAB" & year_range1 == 2000) | (ihme == "GHA" & year_range1 == 1993) | ///
	(ihme == "HTI" & year_range1 == 1994) | ihme == "KHM" | ihme == "LSO" | (ihme == "MAR" & year_range1 == 2003) | ///
	(ihme == "MRT" & year_range1 == 2000) | (ihme == "MWI" & year_range1 == 2004) | ihme == "PAK")
	** CDC-RHS
	replace source_year2 = year_range1 if year_range1 != . & indic_survey == "RHS" & (ihme_loc_id == "CRI" | ///
	(ihme == "GEO" & year_range1 == 1999) | (ihme == "HND" & year_range1 == 1991) | (ihme == "NIC" & year_range1 == 2006) | ///
	(ihme == "PRY" & year_range1 == 1995))
	** WFS
	replace source_year2 = year_range1 if year_range1 != . & indic_survey == "WFS" & (ihme_loc_id == "GHA" | ///
	(ihme == "JAM" & year_range1 == 1975) | (ihme == "MEX" & year_range1 == 1976) | (ihme == "PER" & year_range1 == 1977))

	capture drop source_cbh1
	capture drop source_cbh2
	capture drop source_cbh3
	capture drop source_cbh4
	capture drop source_cbh5
	capture drop source_cbh6
	capture drop source_cbh7
	capture drop source_cbh8
	capture drop source_cbh9
	capture drop year_range1
	capture drop year_range2
	capture drop year_range

	replace source_year2 = 2004 if source_cbh == "demographic and health survey 2004" & ihme == "PSE"
	replace source_year2 = 2003 if source_cbh == "east timor 2003" & ihme == "TLS"
	replace source_year2 = 2005 if source_cbh == "hds 2005" & ihme == "IND"
	replace source_year2 = 2006 if regexm(source_cbh, "enhogar") & ihme == "DOM"
	replace source_year2 = 2000 if source_cbh == "population census 2000" & ihme == "IDN"
	replace source_year2 = 2004 if regexm(source_cbh, "imira") & ihme == "IRQ"
	replace source_year2 = 2007 if regexm(source_cbh, "ifhs") & ihme == "IRQ"
	replace source_year2 = 2008 if regexm(source_cbh, "global fund") & ihme == "MWI"
	replace source_year2 = 1999 if source_cbh == "iran household survey 2000" & ihme == "IRN"
	replace source_year2 = 1971 if source_cbh == "census, 31 august 1971" & ihme == "HTI"
	** DHS SP
	replace source_year2 = 2001 if regexm(source_cbh, "dhs sp") & ihme == "BGD"
	replace source_year2 = 2007 if regexm(source_cbh, "dhs sp") & ihme == "GHA"
	replace source_year2 = 1998 if regexm(source_cbh, "dhs sp") & ihme == "KHM"
	replace source_year2 = 1995 if regexm(source_cbh, "dhs sp") & ihme == "MAR"
	replace source_year2 = 2002 if regexm(source_cbh, "dhs sp") & ihme == "UZB"

	replace source_year = source_year2

	drop source_year2
	tempfile cbh
	save `cbh', replace

** ***********************************************************************************
** generate averages by survey series, location, or survey type and location
** *************************************************************************************
** 2. average by survey series within country
bysort ihme indic_survey: egen mean_ss_country = mean(cbh_var)
** 3a. average by survey series within region
bysort region_name indic_survey: egen mean_ss_region = mean(cbh_var)
** 3b. average by survey series within super-region
bysort super_region_name indic_survey: egen mean_ss_superregion = mean(cbh_var)
** 3c. average by survey series globally
bysort indic_survey: egen mean_ss_global = mean(cbh_var)
** 4a. average by source-type within region
bysort region_name graphingsource: egen mean_st_region = mean(cbh_var)
** 4b. average by source-type within super-region
bysort super_region_name graphingsource: egen mean_st_superregion = mean(cbh_var)
** 4c. average by source-type globally
bysort graphingsource: egen mean_st_global = mean(cbh_var)
** 5. average of everything
egen mean_global = mean(cbh_var)

** save in levels for merging
save `cbh', replace

	** source specific dataset
	keep ihme super_region_name source_cbh region_name source_year graphingsource indic_survey cbh_var year
	tempfile cbh_source
	save `cbh_source', replace

	** survey series data set
	use `cbh', clear
	keep ihme super_region_name source_cbh region_name source_year graphingsource year indic_survey mean_ss*
	tempfile cbh_ss
	save `cbh_ss', replace

	** source type data set
	use `cbh', clear
	keep ihme super_region_name source_cbh region_name source_year graphingsource year indic_survey mean_st* mean_global
	tempfile cbh_st
	save `cbh_st', replace

** ************************************************************************************
** Merge the CBH variance onto the SBH dataset
** ************************************************************************************
	** 1. merge by source
	use `cbh_sbh', clear
	keep if regexm(sourcetype, "SBH")
	drop cbh_var
	merge m:m ihme super_region_name region_name source_year graphingsource indic_survey year using `cbh_source'
	tempfile merge
	save `merge', replace
	keep if _m ==3
	drop _merge source_cbh
	ren cbh_var add_var
	tempfile source_sbh
	save `source_sbh', replace

	** 2. merge by survey series
	use `merge', clear
	keep if _m == 1
	drop _m cbh_var source_cbh

	** 2. country
	merge m:m ihme super_region_name region_name graphingsource indic_survey year using `cbh_ss'
	save `merge', replace
	keep if _m ==3 & mean_ss_country != .
	drop  _merge source_cbh mean_ss_region mean_ss_superregion mean_ss_global
	ren mean_ss_country add_var
	tempfile country_ss_sbh
	save `country_ss_sbh', replace

	** 3a. region
	use `merge', clear
	replace _m = 1 if _m ==3 & mean_ss_country == .
	keep if _m ==1
	drop _m mean_ss_country mean_ss_region mean_ss_superregion mean_ss_global  source_cbh
	merge m:m region_name super_region_name graphingsource indic_survey year using `cbh_ss'
	save `merge', replace
	keep if _m ==3 & mean_ss_region != .
	drop _merge source_cbh mean_ss_country mean_ss_superregion mean_ss_global
	ren mean_ss_region add_var
	tempfile region_ss_sbh
	save `region_ss_sbh', replace

	** 3b. super-region
	use `merge', clear
	replace _m = 1 if _m ==3 & mean_ss_region == .
	keep if _m ==1
	drop _m mean_ss_country mean_ss_region mean_ss_superregion mean_ss_global source_cbh
	merge m:m super_region_name graphingsource indic_survey year using `cbh_ss'
	save `merge', replace
	keep if _m ==3 & mean_ss_superregion != .
	drop _merge source_cbh mean_ss_country mean_ss_region mean_ss_global
	ren mean_ss_superregion add_var
	tempfile supregion_ss_sbh
	save `supregion_ss_sbh', replace

	** 3c. global
	use `merge', clear
	replace _m = 1 if _m ==3 & mean_ss_superregion == .
	keep if _m ==1
	drop _m mean_ss_country mean_ss_region mean_ss_superregion mean_ss_global source_cbh
	merge m:m graphingsource indic_survey year using `cbh_ss'
	save `merge', replace
	keep if _m ==3 & mean_ss_global != .
	drop _merge source_cbh mean_ss_country mean_ss_region mean_ss_superregion
	ren mean_ss_global add_var
	tempfile global_ss_sbh
	save `global_ss_sbh', replace

	** 4. merge by source-type
	** 4a. region
	use `merge', clear
	replace _m = 1 if _m ==3 & mean_ss_global == .
	keep if _m ==1
	drop _m mean_ss_country mean_ss_region mean_ss_superregion mean_ss_global source_cbh
	merge m:m region_name super_region_name graphingsource year using `cbh_st'
	save `merge', replace
	keep if _m ==3 & mean_st_region != .
	drop mean_st_superregion mean_st_global mean_global _merge source_cbh
	ren mean_st_region add_var
	tempfile region_st_sbh
	save `region_st_sbh', replace

	** 4b. super-region
	use `merge', clear
	replace _m = 1 if _m ==3 & mean_st_region == .
	keep if _m ==1
	drop _m mean_st_region mean_st_superregion mean_st_global mean_global source_cbh
	merge m:m super_region_name graphingsource year using `cbh_st'
	save `merge', replace
	keep if _m ==3 & mean_st_superregion != .
	drop mean_st_region mean_st_global mean_global _merge source_cbh
	ren mean_st_superregion add_var
	tempfile supregion_st_sbh
	save `supregion_st_sbh', replace

	** 4c. global
	use `merge', clear
	replace _m = 1 if _m ==3 & mean_st_superregion == .
	keep if _m ==1
	drop _m mean_st_region mean_st_superregion mean_st_global mean_global source_cbh
	merge m:m graphingsource year using `cbh_st'
	save `merge', replace
	keep if _m ==3 & mean_st_global != .
	drop mean_st_region mean_st_superregion mean_global _merge source_cbh
	ren mean_st_global add_var
	tempfile global_st_sbh
	save `global_st_sbh', replace

	** 5. global (for the ones that didn't merge)
	use `merge', clear
	replace _m = 1 if _m ==3 & mean_st_global == .
	keep if _m ==1
	drop _m mean_st_region mean_st_superregion mean_st_global mean_global source_cbh
	merge m:m graphingsource using `cbh_st'
	save `merge', replace
	keep if _m ==3 & mean_global != .
	drop _merge source_cbh  mean_st*
	gen add_var = mean_global
	tempfile global_sbh
	save `global_sbh', replace

** append together
append using `global_st_sbh' `supregion_st_sbh' `region_st_sbh' `global_ss_sbh' `supregion_ss_sbh' `region_ss_sbh' `country_ss_sbh' `source_sbh'
drop sdq5_sbh log10sdq5_cbh indic_survey

** ****************************************************************************
** Add the CBH variance to the SBH variance
** *****************************************************************************

** add together variance
egen var_tot = rowtotal(variance add_var)

duplicates drop
duplicates tag ihme_loc_id sourcetype super_region_name region_name source source_year graphingsource year, gen(dup)
drop if dup == 1 & add_var == .
drop dup
duplicates tag ihme_loc_id sourcetype super_region_name region_name source source_year graphingsource year, gen(dup)
bysort ihme source source_year: gen count = _n
duplicates drop ihme_loc_id sourcetype super_region_name region_name source source_year graphingsource year, force
drop dup count

** assert _N == `obs'
tempfile sbh_all
save `sbh_all', replace
