** ****************************************************
** Purpose: Merge together the different stages of the CoD process to compare. Translate deaths into cause fractions with sample size
** ******************************************************************************************************************

 ** Prep Stata
	clear all
	set more off, perm
	if c(os) == "Unix" {
		set mem 10G
		set odbcmgr unixodbc
		global j "/home/j"
	}
	else if c(os) == "Windows" {
		set mem 800m
		global j "J:"
	}
	
** Source
	global source "`1'"

** Date
	global timestamp "`2'"
	
** Username
	global username "`3'"
	
	
**  Local for the directory where all the COD data are stored
	local in_dir "$j/WORK/03_cod/01_database/03_datasets"
	global in_dir "$j/WORK/03_cod/01_database/03_datasets"

** Local for the output directory
	local out_dir "$j/WORK/03_cod/01_database/02_programs/compile"

** Log output
	capture log close _all
	log using "`in_dir'//$source//logs/07_mergelong_${timestamp}", replace

** FINAL AFTER RDP
	use "`in_dir'//$source//data/final/06_hiv_corrected.dta", clear
	label drop _all

	** Save raw compiled file
		foreach i of numlist 1/26 91/94 {
			capture gen double deaths`i' = 0
			capture gen double orig_deaths`i' = 0
			capture gen double cf`i' = 0
			capture gen double orig_cf`i' = 0
		}
		renpfix deaths deaths_rd
		renpfix orig_deaths deaths_corr
		renpfix cf cf_rd
		renpfix orig_cf cf_corr
		** Enforce variable types
			** String
				foreach var of varlist iso3 dev_status acause source source_label source_type list subdiv {
					tostring(`var'), replace force
					replace `var' = "" if `var' == "." | `var' == "0"
				}
			** Numeric
				foreach var of varlist location_id year sex NID national region beforeafter deaths_rd* deaths_corr* cf_rd* cf_corr* {
					destring(`var'), replace
				}
			** IDs
				foreach var of varlist location_id NID {
					replace `var' = . if `var' == 0
				}
		aorder
		order iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status beforeafter deaths_rd* deaths_corr* cf_rd* cf_corr*
		keep iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status beforeafter deaths_rd* deaths_corr* cf_rd* cf_corr*
		compress
		label drop _all
		tempfil rdp
		save `rdp', replace
		
	** FINAL AFTER AGE-SEX-SPLIT
	if inlist(source, "US_NCHS_counties_ICD9", "US_NCHS_counties_ICD10") {
		use "`in_dir'//$source//data/intermediate/02_agesexsplit_compile_states.dta", clear
	}
	else {
		use "`in_dir'//$source//data/intermediate/02_agesexsplit_compile.dta", clear
	}

	** Reformat age-sex split file
		foreach i of numlist 1/26 91/94 {
			capture gen deaths`i' = 0
			capture gen cf`i' = 0
		}
		** Enforce variable types
			** String
				foreach var of varlist iso3 dev_status acause source source_label source_type list subdiv {
					tostring(`var'), replace force
					replace `var' = "" if `var' == "0" | `var'=="."
				}
			** Numeric
				foreach var of varlist location_id year sex NID national region deaths* cf* {
					destring(`var'), replace
				}
			** IDs
				foreach var of varlist location_id NID {
					replace `var' = . if `var' == 0
			}
			
		renpfix deaths deaths_raw
		renpfix cf cf_raw
		drop if acause=="_gc"
		order iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status deaths_raw* cf_raw*
		keep iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status deaths_raw* cf_raw*
		collapse(sum) deaths* cf*, by(iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status) fast
		compress
		label drop _all

	** Bring them together
		merge 1:1 iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status using `rdp', gen(raw_only)

		count if raw_only==3
		if `r(N)'==0 {
			noisily display in red "NONE OF THE DATA MATCHES, THERE'S A PROBLEM WITH YOUR IDENTIFIERS"
			BREAK
			}
		count if raw_only==1
		if `r(N)'>0 {
			display in red "ENTIRE CAUSES ARE DROPPED IN RESTRICTION/HIV CORRECTION"
			tab source if raw_only==1
		}
		
		drop raw_only
		
** *********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** STEP 1: CHECK AND RECODE VARIABLES/COUNTRIES

	** Check and standardize variables
		** iso3
			count if iso3 == ""
			if `r(N)'>0 {
				display "we have blank iso3 values, FIX"
				BREAK
			}
		** year
			count if year == .
			if `r(N)'>0 {
				display "we have blank year values, FIX"
				BREAK
			}
		** sex
			count if sex!=1 & sex!=2
			if `r(N)'>0 {
				display "we have blank/both sex values, FIX"
				BREAK
			}
		** acause

			count if acause == "_gc" & (deaths_rd1>0)
			if `r(N)'>0 {
				display "we have garbage codes after redistribution, FIX"
				BREAK
			}
			count if acause == ""
			if `r(N)'>0 {
				display "we have blank acause values, FIX"
				BREAK
			}			
			replace acause = "cc_code" if acause == "CC_code" | acause == "CC Code" | acause == "CC code" | acause== "cc code"
		** source
			count if source == ""
			if `r(N)'>0 {
				display "we have blank acause source, FIX"
				break
			}				
		** NID
			capture gen NID=.
			count if NID == . | NID==0
			if `r(N)'>0 {
				tab source if NID == .
				display "we have blank NID values, FIX"
				BREAK
			}
		** source_label
			replace source_label = source if source_label == ""
		** national
			replace national = 0 if source_type == "VA Subnational" | source_type == "VR Subnational"
		** source_type
			replace source_type = "Survey" if source_type == "survey" | source_type == "HH deaths, survey"
			replace source_type = "Census" if source_type == "HH deaths, census"
			replace source_type = "VA Subnational" if source_type == "VA National" & national == 0
		** list
		** subdiv
			replace subdiv = "" if subdiv == "."
			count if regexm(subdiv, ",")
			if `r(N)'>0 {
				keep source source_label subdiv
				duplicates drop
				list, clean noobs
				display "above are subdivs with commas, fix in the format code and rerun"
				BREAK
				}

		collapse (sum) deaths* cf*, by(iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status beforeafter) fast
	
	** A few VR sources have duplicate country-years, drop them here
		drop if source == "Iran MoH" & iso3 == "IRN" & (year == 2000 | year == 2001)
		drop if source == "ICD9_BTL" & iso3 == "LKA" & (year == 1995 | year == 1996)
		drop if source == "Other_Maternal" & iso3 == "MNG" & year == 1994
		drop if source == "Philippines_2001_2005" & iso3 == "PHL" & (year == 2000 | year == 2001 | year == 2002 | year == 2003)
		
	** Any census, survey, silbling history data should be reduced down to just injuries, maternal, and CC_code
		** make a flag for observations that need to be reduced. In sibling history we should only keep maternal (not injuries)
		gen flag = 1 if (regexm(source_type, "Survey")| regexm(source_type, "Census")) & regexm(acause, "maternal") !=1 & regexm(acause, "inj") !=1
		replace flag = 1 if regexm(source_type, "Sibling") & regexm(acause, "maternal") !=1
				
		** replace to cc code if it's flagged
		replace acause = "cc_code" if flag ==1
		replace beforeafter = 3 if flag ==1
		drop flag
		
** *********************************************************************************************************************************************************************
** 
** ********************************************************************************************************************************************************************
** STEP 2: COMBINE SUBNATIONAL: CHINA (strata)

	if inlist("$source", "China_1991_2002", "China_2004_2012") {
		do "`out_dir'/code/subnational_China_rescale.do"
	}

	if inlist("$source", "China_Child_1996_2012", "China_MMS_1996_2005", "China_MMS_2006_2012") {
		replace subdiv = substr(subdiv, 1, 7)
		collapse (sum) cf* deaths* (max) beforeafter, by(iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status) fast
	}

** ********************************************************************************************************************************************************************
** ********************************************************************************************************************************************************************
** STEP 3: CALCULATE CAUSE FRACTIONS AND THE TOTAL NUMBER OF OBSERVED DEATHS (SAMPLE_SIZE)

	** If rounding error from redistribution created a really tiny number of deaths, reset it to zero
		foreach i of numlist 1/26 91/94 {
			capture replace deaths_rd`i' = 0 if deaths_rd`i' < .0001 & deaths_corr`i' == 0
		}
	
	**  Make sure that the data are in format 2, with the terminal age at 80+
		aorder
		cap drop tmp 
		egen double tmp = rowtotal(deaths_rd3 deaths_rd4 deaths_rd5 deaths_rd6)
		replace deaths_rd3 = tmp
		drop tmp deaths_rd4-deaths_rd6

		egen double tmp = rowtotal(deaths_corr3 deaths_corr4 deaths_corr5 deaths_corr6)
		replace deaths_corr3 = tmp
		drop tmp deaths_corr4-deaths_corr6
		
		egen double tmp = rowtotal(deaths_raw3 deaths_raw4 deaths_raw5 deaths_raw6)
		replace deaths_raw3 = tmp
		drop tmp deaths_raw4-deaths_raw6

		egen double tmp = rowtotal(deaths_corr22-deaths_corr23)
		replace deaths_corr22 = tmp
		drop tmp deaths_corr23

		egen double tmp = rowtotal(deaths_rd22-deaths_rd25)
		replace deaths_rd22 = tmp
		drop tmp deaths_rd23 deaths_rd24 deaths_rd25 deaths_rd26
		
		egen double tmp = rowtotal(deaths_raw22-deaths_raw25)
		replace deaths_raw22 = tmp
		drop tmp deaths_raw23 deaths_raw24 deaths_raw25 deaths_raw26
		
		forvalues i = 4/6 {
			cap gen deaths_rd`i' = 0
			cap gen deaths_corr`i' = 0
			cap gen deaths_raw`i' = 0
		}
		
	** Recalculate total deaths
		aorder
		egen double tot = rowtotal(deaths_rd3-deaths_rd94)
		replace deaths_rd1 = tot
		egen double orig_tot = rowtotal(deaths_corr3-deaths_corr94)
		replace deaths_corr1 = orig_tot
		egen double raw_tot = rowtotal(deaths_raw3-deaths_raw93)
		replace deaths_raw1 = raw_tot
		drop tot orig_tot raw_tot deaths_rd2 deaths_corr2 deaths_raw2

	** Make a separate file of the data that already comes with cause fractions. We'll just append this back on later after calculating CFs for the rest of the data
		egen double keep_cf = rowtotal(cf*)
		replace keep_cf = 1 if keep_cf != 0
		egen double keep_deaths = rowtotal(deaths*)
		replace keep_deaths = 1 if keep_deaths != 0
		preserve
			keep if keep_cf == 1 & keep_deaths != 1
			tempfile causefractions
			save `causefractions', replace
		restore
		
	** Drop data that already had cause fractions
		drop if keep_cf == 1 & keep_deaths != 1
		drop *cf* keep_deaths

	** Calculate cause fractions
		** For redistributed deaths.
		** Calculate by only iso3, location_id, year, and sex for Cancer_Registry. Data should already have been verified for consistency within these variables, and multiple subdiv-NID-source_labels may be used for one iso3-location_id-year-sex.
		foreach i of numlist 3 7/22 91/94 {
			if "$source" != "Cancer_Registry" bysort iso3 location_id year sex source_type source_label source NID list national region dev_status subdiv: egen cf_rd`i' = pc(deaths_rd`i'), prop
			else bysort iso3 location_id year sex: egen cf_rd`i' = pc(deaths_rd`i'), prop
		}
		
	** For original deaths and raw deaths, we have to use redistributed deaths as the denominator becuase orig_deaths and raw_deaths no longer includes garbage deaths
		** Calculate by only iso3, location_id, year, and sex for Cancer_Registry. Data should already have been verified for consistency within these variables, and multiple subdiv-NID-source_labels may be used for one iso3-location_id-year-sex.
		foreach i of numlist 3 7/22 91/94 {
			if "$source" != "Cancer_Registry" bysort iso3 location_id year sex source_type source_label source NID list national region dev_status subdiv: egen cf_corr`i'_denominator = total(deaths_rd`i')
			else bysort iso3 location_id year sex: egen cf_corr`i'_denominator = total(deaths_rd`i')
			gen double cf_corr`i' = deaths_corr`i' / cf_corr`i'_denominator
			gen double cf_raw`i' = deaths_raw`i' / cf_corr`i'_denominator
			drop cf_corr`i'_denominator
		}
	
	** Calculate the total number of observed deaths by country/year/age/sex group by study/data source which we'll call sample_size: 
	** We later use the sample size variable to calculate the standard deviation of the CF by taking a normal approximation of a binomial distribution
	** from the equation sigma=(cf(1-cf)/sample_size)^(1/2). Refer to the equation found here: http://en.wikipedia.org/wiki/Binomial_distribution
	** if the sample size is zero, we can just drop that data because it's not actual data...do this after reshaping long
		** Calculate by only iso3, location_id, year, and sex for Cancer_Registry. Data should already have been verified for consistency within these variables, and multiple subdiv-NID-source_labels may be used for one iso3-location_id-year-sex.
		foreach s of numlist 3 7/22 91/94 {
			if "$source" != "Cancer_Registry" bysort iso3 location_id year sex source_type source_label source NID list national region dev_status subdiv: egen sample_size`s' = total(deaths_rd`s'), missing
			else bysort iso3 location_id year sex: egen sample_size`s' = total(deaths_rd`s'), missing
		}
			
	**  Sometimes we have sample size <1. Sample sizes that are <1 interfere with the model due to a rounding process. For a quick fix, replace sample size = 1 if it's <1 and >=.0001 (the lower limit)
		foreach i of numlist 3 7/22 91/94 {
			replace sample_size`i' = 1 if sample_size`i' < 1 & sample_size`i' >= .0001
		}
		
	** Append data that originally had cause fractions back on
		append using "`causefractions'"

		drop keep_cf cf_rd1 cf_rd2 cf_rd4-cf_rd6 cf_rd23-cf_rd26

** ********************************************************************************************************************************************************************
** ********************************************************************************************************************************************************************
** STEP 4: MAKE AGE LONG

	** Change age from WHO format to actual age groups
		forvalues i = 7/22 {
			local j = (`i'-6)*5
			rename cf_rd`i' cf_rd_`j'
			rename cf_corr`i' cf_corr_`j'
			rename cf_raw`i' cf_raw_`j'
			rename sample_size`i' sample_size_`j'
			rename deaths_rd`i' deaths_rd_`j'
			rename deaths_corr`i' deaths_corr_`j'
			rename deaths_raw`i' deaths_raw_`j'
		}
		rename cf_rd3 cf_rd_1
		rename cf_corr3 cf_corr_1
		rename cf_raw3 cf_raw_1
		rename sample_size3 sample_size_1
		rename deaths_rd3 deaths_rd_1
		rename deaths_corr3 deaths_corr_1
		rename deaths_raw3 deaths_raw_1
		
	** deaths91 refers to 0-6 days, deaths93 refers to 7-27 days, and deaths94 refers to 28-365 days
		foreach f of numlist 91 93/94 {
			rename cf_rd`f' cf_rd_`f'
			rename cf_corr`f' cf_corr_`f'
			rename cf_raw`f' cf_raw_`f'
			rename sample_size`f' sample_size_`f'
			rename deaths_rd`f' deaths_rd_`f'
			rename deaths_corr`f' deaths_corr_`f'
			rename deaths_raw`f' deaths_raw_`f'
		}
	
	** Drop some unnecessary empty variables
		aorder
		foreach i of numlist 1/2 4/6 92 {
			capture drop deaths_rd`i'
			capture drop deaths_corr`i'
			capture drop deaths_raw`i'
		}

	** Let's collapse one last time to get rid of any duplicates
		collapse (sum) cf_raw_* cf_corr_* cf_rd_* deaths_rd_* deaths_corr_* deaths_raw_* (mean) sample_size_*, by(iso3 dev_status region NID source source_label source_type list location_id subdiv national sex year acause beforeafter) fast
		
	** Reshape data to be age long
		egen meta_id = group(iso3 dev_status region NID source source_label source_type list location_id subdiv national sex year acause beforeafter), missing
		preserve
			keep meta_id iso3 dev_status region NID source source_label source_type list location_id subdiv national sex year acause beforeafter
			duplicates drop
			tempfile meta_id
			save `meta_id', replace
		restore
		drop iso3 dev_status region NID source source_label source_type list location_id subdiv national sex year acause beforeafter
		reshape long cf_rd_ cf_corr_ cf_raw_ sample_size_ deaths_rd_ deaths_corr_ deaths_raw_, j(age) i(meta_id)
		label drop _all
		merge m:1 meta_id using `meta_id', keep(1 3) assert(3) nogen
		drop meta_id
		rename cf_raw_ cf_raw
		rename cf_corr_ cf_corr
		rename cf_rd_ cf_rd
		rename sample_size_ sample_size
		rename deaths_raw_ deaths_raw
		rename deaths_corr_ deaths_corr
		rename deaths_rd_ deaths_rd
				
	** save before aggregation
	compress
	save "`in_dir'/$source/data/final/07_merged_long.dta", replace
	save "`in_dir'/$source/data/final/_archive/07_merged_long_$timestamp.dta", replace

