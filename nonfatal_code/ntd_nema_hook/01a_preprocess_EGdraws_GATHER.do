/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:  GBD2016 GATHER      
====================================================================*/

/*====================================================================
                        0: Program set up
====================================================================*/

	version 13.1
	drop _all
	clear mata
	set more off

	set maxvar 32000
	if c(os) == "Unix" {
		local j "FILENAME"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "FILENAME"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step 01a
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATHs"
	*directory for temporary outputs on cluster to be utilized through process
	local tmp_dir "FILEPATH"
	*local temporary directory for other things:
	local local_tmp_dir "FILEPATH"
	*directory for output of draws > on ihme/scratch
	local out_dir "FILEPATH"
	*directory for logs
	local log_dir "FILEPATH"
	*directory for progress files
	local progress_dir "FILEPATH"

* Make and Clear Directories

	*make all directories
	local make_dirs code in tmp local_tmp out log progress_dir
	foreach dir in `make_dirs' {
		capture mkdir `dir'_dir
	}
	*clear tempfiles, logs and progress files
	foreach dir in log progress tmp {
		capture cd `dir'_dir
		capture shell rm *
	}

*Directory for standard code files
	adopath + FILEPATH


	*****start log*********
	capture log close
	local date = string(date(c(current_date),"DMY"),"%tdDD.NN.CCYY")
	local time = subinstr("$S_TIME", ":" , "", .)
	log using "`log_dir'/01a_preprocess_EGdraws_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir


/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Demographics
	
	get_demographics, gbd_team("epi") clear
		local gbdages="`r(age_group_ids)'"
		local gbdyears="`r(year_ids)'"
		local gbdsexes="`r(sex_ids)'"

*--------------------1.2: Location Metadata

	*Get GBD location metadata
		get_location_metadata,location_set_id(35) clear
			preserve
				keep if most_detailed==1
				levelsof location_id,local(gbdlocs) clean
			restore
		
		save "`local_tmp_dir'/`step'_metadata.dta", replace
		

/*====================================================================
                        1: PREP STH DATA
====================================================================*/

*--------------------1.1: IMPORT CHINA 2005 DATA
*[china_update.csv]: Updated prevalence estimates for China in 2005, available for 1 year only (2005), strateified by age and intensity

	*Use total data and merge on new china data
		insheet using "`in_dir'/china_update_gbd2015COPY.csv", double names clear	
	   
		tempfile china_update
		save `china_update', replace

*--------------------1.2: IMPORT ALL DATA 1990, 2005, 2010
*[ALL_DATA_reshaped.dta]: Prevalence data for all countries for 3 year bins, stratified by year and age
		
		use "`in_dir'/ALL_DATA_reshaped_gbd2015COPY.dta", clear
		tempfile ALL_DATA_reshaped
		save `ALL_DATA_reshaped', replace


*--------------------1.3: MERGE CHINA 2005 DATA & ALL DATA 1990, 2005, 2010

	*Merge china data into alldata
		merge 1:1 iso3 year age intensity helminth_type using "`china_update'", keep(master match) nogen
	
		replace mapvar = mapvar2 if mapvar2 != .
		replace lower = lower2 if lower2 != .
		replace upper = upper2 if upper2 != .
		sort iso3 age intensity helminth_type year
		bysort iso3 age intensity helminth_type : replace mapvar2 = mapvar2[_n-1] if missing(mapvar2)
		bysort iso3 age intensity helminth_type : replace lower2 = lower2[_n-1] if missing(lower2)
		bysort iso3 age intensity helminth_type : replace upper2 = upper2[_n-1] if missing(upper2)
		replace lower = lower2 if mapvar2 < mapvar & helminth_type == "hk"
		replace upper = upper2 if mapvar2 < mapvar & helminth_type == "hk"
		replace mapvar = mapvar2 if mapvar2 < mapvar & helminth_type == "hk"
		replace lower = lower2 if mapvar2 < mapvar & helminth_type == "tt"
		replace upper = upper2 if mapvar2 < mapvar & helminth_type == "tt"
		replace mapvar = mapvar2 if mapvar2 < mapvar & helminth_type == "tt"
		drop *2
		
		replace countryname = "British Virgin Islands" if countryname == "British Virgin Islands"
		replace countryname = "Guinea-Bissau" if countryname == "Guinea Bissau"
		replace countryname = "North Korea" if countryname == "Korea, North"
		replace countryname = "Libya" if countryname == "Libyan Arab Jamahiriya"
		replace countryname = "Federated States of Micronesia" if countryname == "Micronesia (Federated States of)"
		replace countryname = "Saint Vincent and the Grenadines" if countryname == "St Vincent"
		replace countryname = "Syria" if countryname == "Syrian Arab Republic"
		replace countryname = "Timor-Leste" if countryname == "Timor Leste"
		replace countryname = "United Arab Emirates" if countryname == "United Arab Emerates"
		replace countryname = "Tanzania" if countryname == "United Republic of Tanzania"
		replace countryname = "Vietnam" if countryname == "Viet Nam"

		tempfile alldata_chinaupdate
		save `alldata_chinaupdate'

*--------------------1.4: IMPORT AFRICA 2010 DATA (w/ Seychelles)
*[Africa_2010.dta]: Prevalence data for all in Africa (+ Seychelles in GBD SE Asia), 1 year (2010), stratified by age/intensity

	*Bring in Africa data
		use "`in_dir'/Africa_2010_gbd2015COPY.dta", clear
							
		drop ttprev
		sort iso3 age intensity helminth_type year
		
	*Expand to copy draws for all 3 years - 1990, 2005 & 2010
		expand 3
		bysort iso3 age intensity helminth_type: gen x=_n
		replace year = 1990 if x == 1
		replace year = 2005 if x == 2
		replace year = 2010 if x == 3
		drop x
		
	*Cleanup
		drop ihme_country

		replace countryname = "The Gambia" if countryname == "Gambia"
		replace countryname = "Guinea-Bissau" if countryname == "Guinea Bissau"
		replace countryname = "Tanzania" if countryname == "United Republic of Tanzania"

		tempfile Africa_2010_edited
		save `Africa_2010_edited', replace


*--------------------1.5: IMPORT REDUCTIONS DATA

	*Bring in reductions data		 
		insheet using "`in_dir'/STH_reductions_gbd2015COPY.csv", double names clear
		  			
	*Rename variables
		rename country countryname
		rename *_2005_2010 *
		
		reshape long asc_ tt_ hk_, i(countryname coverage) j(age) string
		rename *_ *
		rename asc helminth_type_asc
		rename hk helminth_type_hk
		rename tt helminth_type_tt
		
		reshape long helminth_type_, i(countryname coverage age) j(helminth_type) string
		rename helminth_type_ adjustment_factor
		
		expand 2
		bysort countryname helminth_type age: gen x = _n
		replace age = "0to4" if age == "rest" & x == 1
		replace age = "15plus" if age == "rest" & x == 2
		replace age = "5to9" if age == "sac" & x == 1
		replace age = "10to14" if age == "sac" & x == 2
		gen year = 2010
		drop x
		sort countryname age helminth_type
		
		replace countryname = "Cote d'Ivoire" if countryname == "Côte d'Ivoire"
		replace countryname = "North Korea" if countryname == "Democratic People's Republic of Korea"
		replace countryname = "Guinea-Bissau" if countryname == "Guinea Bissau"
		replace countryname = "Laos" if countryname == "Lao People's Democratic Republic"
		replace countryname = "Venezuela" if countryname == "Venezuela (Bolivarian Republic of)"
		replace countryname = "Vietnam" if countryname == "Viet Nam"
		
		tempfile STH_reductions_edited
		save `STH_reductions_edited', replace


*--------------------1.6: MERGE AFRICA ADJUSTED DATA WITH REDUCTIONS DATA

		use "`Africa_2010_edited'", clear
		merge m:1 countryname age helminth_type year using "`STH_reductions_edited'", keep(master matched) nogen
					
		gen new_2005_mapvar = mapvar * (1 + adjustment_factor) if year == 2010
		gen new_2005_lower = lower * (1 +  adjustment_factor) if year == 2010
		gen new_2005_upper = upper * (1 +  adjustment_factor) if year == 2010
		
	*Copy the adjusted value to all years - 1990, 2005, and 2010 - for that loc/age/intensity/helminth
		sort iso3 age intensity helminth_type year
		gsort iso3 age intensity helminth_type -year
		bysort iso3 age intensity helminth_type: replace new_2005_mapvar = new_2005_mapvar[_n-1] if missing(new_2005_mapvar)
		bysort iso3 age intensity helminth_type: replace new_2005_lower = new_2005_lower[_n-1] if missing(new_2005_lower)
		bysort iso3 age intensity helminth_type: replace new_2005_upper = new_2005_upper[_n-1] if missing(new_2005_upper)
					
		replace mapvar = new_2005_mapvar if new_2005_mapvar != . & (year == 2005 | year == 1990)
		replace lower = new_2005_lower if new_2005_lower != . & (year == 2005 | year == 1990)
		replace upper = new_2005_upper if new_2005_upper != . & (year == 2005 | year == 1990)

	*Clean up
		sort iso3 age intensity helminth_type year
		drop new_* coverage adjustment_factor
		order year, after(iso3)
		rename mapvar mapvar2 
		rename lower lower2
		rename upper upper2

	*Get all locs into a macro
		levelsof iso3,local(isos) clean
		
		tempfile Africa_2010_reduced
		save `Africa_2010_reduced', replace

*--------------------1.7: MERGE ALL DATA W/ CHINA UPDATED & AFRICA EDITED/REDUCED DATA

	use "`alldata_chinaupdate'", clear
	merge 1:1 iso3 year age helminth_type intensity using "`Africa_2010_reduced'"
		
	*Mayotte dropped, not a gbd location
		drop if iso3 == "MYT"
		
	*Replace estimates with estimates from adjusted African countries data file for countries not originally in the all_data file (_merge==2)
		replace mapvar = mapvar2 if _m == 2
		replace lower = lower2 if _m == 2
		replace upper = upper2 if _m == 2
		drop _m

	*Replacing for Central Sub-Saharan Africa (gbd_super_region 3): 46 locations | SYC
		replace mapvar = mapvar2 if mapvar2 != . & gbd_super_region == 3 | iso3 == "SYC"
		replace lower = lower2 if lower2 != . & gbd_super_region == 3 | iso3 == "SYC"
		replace upper = upper2 if upper2 != . & gbd_super_region == 3 | iso3 == "SYC"
		
	*Cleanup
		drop *2
		replace helminth_type = "ascar" if helminth_type == "asc"
		replace helminth_type = "trich" if helminth_type == "tt"
		replace helminth_type = "hook" if helminth_type == "hk"
		replace intensity = "inf_all" if intensity == "prev"
		replace intensity = "inf_med" if intensity == "med"
		replace intensity = "inf_heavy" if intensity == "heavy"
		replace intensity = "inf_light" if intensity == "light"

*--------------------1.8: Save
	
	*Save
		save "`local_tmp_dir'/`step'_PrePreppedData.dta", replace
	


/*====================================================================
                        2: Update Format For Use in GBD2016
====================================================================*/

		use "`local_tmp_dir'/`step'_PrePreppedData.dta", clear

*--------------------2.1: Clean Up Variables for Merging

	*Rename variables
		rename countryname location_name
		rename iso3 ihme_loc_id
		rename year year_id
		rename mapvar mean

*--------------------2.2: Get Location IDs

	*Merge with full metadata table
		merge m:1 ihme_loc_id using "`local_tmp_dir'/`step'_metadata.dta", keep(matched)
		keep location_id location_name ihme_loc_id year_id age intensity helminth_type mean upper lower 
			*Will drop observations for non-GBD locations:

*--------------------2.3: Get Both Sexes
	
	*Add sexes
		expand 2,gen(new)
		gen sex_id=1
		replace sex_id=2 if new==1
		drop new

*--------------------2.3: Get GBD2016 Age Groups

	*Translate age variable to GBD age groups
		gen age_group_id=.
		replace age_group_id=1 if age=="0to4"
		replace age_group_id=6 if age=="5to9"
		replace age_group_id=7 if age=="10to14"
		replace age_group_id=29 if age=="15plus"
		drop age

	*Make assumptions to expand break out container age groups
			gen rownum = _n
		**Assumption #1: Prevalence in all age groups under 5yo = prevlaence of 0-4yo
			expand 2 if age_group_id == 1
				** // to get post neonatal (28-364 days) and 1-4 i.e. age 0.1-4
			bysort rownum: gen x = _n
			replace age_group_id = 4 if age_group_id == 1 & x == 1
				** //Post Neonatal
			replace age_group_id = 5 if age_group_id == 1 & x == 2
				** //1-4yo

		**Assumption #2: Prevlanece in all age groups over 15yo = prevalence of 15+yo
			expand 17 if age_group_id==29
				** // to get the 14 gbd age groups from 15-19, 20-24, ..., 95+
			bysort rownum: gen y = _n
			forvalues y = 1/17 {
			  replace age_group_id = 7 + `y' if age_group_id == 29 & y == `y'
			}
			replace age_group_id = 30 if age_group_id == 21
				** //80-84yo
			replace age_group_id = 31 if age_group_id == 22
				** //85-90yo
			replace age_group_id = 32 if age_group_id == 23
				** //90-95yo
			replace age_group_id = 235 if age_group_id == 24
				** //95+

*--------------------2.4: Generate 1000 Draws

	*Replace zero means and confidence intervals that break the code
		replace mean = 0 if mean < 1.0e-10 | upper < 1.0e-10
		replace lower = 0 if mean < 1.0e-10
		replace upper = 0 if mean < 1.0e-10

	*Produces draws independently for different age and intensities of infection,
		generate avg_sd = abs(ln(upper)-ln(lower))/(invnormal(0.975)*2)
		generate sd_half = abs(ln(upper)-ln(mean))/invnormal(0.975) if missing(avg_sd) & upper > 0
		replace sd_half = abs(ln(lower)-ln(mean))/invnormal(0.975) if missing(avg_sd) & lower > 0
		
		di in red "Creating Draw..."
		forvalues x=0/999 {
			di in red "`x' ." _continue

			quietly{
				generate double draw_`x'= mean * exp(rnormal()*avg_sd)
				replace draw_`x' = 1 if draw_`x' > 1 & !missing(draw_`x')
				replace draw_`x' = 0 if upper == 0 & lower == 0 & missing(draw_`x')
				replace draw_`x'= mean * exp(rnormal()*sd_half) if missing(draw_`x')
			}
		}

*--------------------2.5: SAVE

	*Format - keep only what is needed
		keep location_id ihme_loc_id year_id age_group_id sex_id helminth_type intensity draw_*

	*Save
		save "`in_dir'/`step'_preprocessed_EGdraws.dta", replace

/*====================================================================
                        3: Create Other Needed Files
====================================================================*/


*--------------------3.1: Zeroes File for Nonendemic Locations

	clear
	quietly set obs 21
	quietly generate double age_group_id = .
	quietly format age %16.0g
	quietly replace age = _n+3
	quietly replace age_group_id = 30 if age_group_id == 21
	quietly replace age_group_id = 31 if age_group_id == 22
	quietly replace age_group_id = 32 if age_group_id == 23
	quietly replace age_group_id = 235 if age_group_id == 24
	sort age_group_id

	forvalues i = 0/999 {
		quietly generate draw_`i' = 0
	}

	quietly format draw* %16.0g

	save "`in_dir'/`step'_zeroes.dta", replace	





log close
exit
/* End of do-file */