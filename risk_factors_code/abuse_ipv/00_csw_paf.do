// apply csw proportions to squeezed hiv models to generate proportion of hiv attributable to csw transmission

 
// Set preferences for STATA
	// Clear memory and set memory and variable limits
		clear all
		macro drop _all
		set mem 700m
		set maxvar 32000
	// Set to run all selected code without pausing
		set more off
	// Set to enable export of large excel files
		set excelxlsxlargefile on
	// Set graph output color scheme
		set scheme s1color
	// Remove previous restores
		cap restore, not

// Close previous logs
	cap log close

// create locals
local output "FILEPATH"

// load shared functions
run "FILEPATH/get_draws.ado"
run "FILEPATH/get_outputs.ado"
run "FILEPATH/get_location_metadata.ado"
run "FILEPATH/get_model_results.ado"
run "FILEPATH/get_draws.ado"

// location_ids
	get_location_metadata, location_set_id(35) clear
	keep if most_detailed == 1

	keep ihme_loc_id location_id location_ascii_name super_region_name super_region_id region_name region_id
	
	rename ihme_loc_id iso3
	export delimited using "`output'/country_codes.csv", replace
	tempfile country_codes
	save `country_codes', replace

	levelsof location_id, local(locations)


*** * ***************************************************************************************
*** LOAD CODCORRECT RESULTS 
*** * ***************************************************************************************

clear

// call results for HIV deaths in years of interest by age and sex
	
	local ages 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235
	local measures 1 4 // for deaths and YLLs
	local years 1980 1985 1990 1995 2000 2005 2010 2016

	get_outputs, topic(cause) cause_id(298) measure_id(`measures') age_group_id(`ages') sex_id(2) year_id(`years') location_id(`locations') compare_version_id("latest") clear
	
// Reshape
	rename measure_name measure
	replace measure = "death" if measure == "Deaths", nopromote
	replace measure ="yll" if measure =="YLLs (Years of Life Lost)", nopromote
	keep val upper lower location_id year_id age_group_id measure sex_id
	reshape wide val upper lower, i(location_id year_id age_group_id) j(measure) string

	rename valdeath mean_abs_death 
	rename valyll mean_abs_yll

	export delimited using "`output'/hiv_cod.csv", replace
	tempfile hiv_cod
	save `hiv_cod', replace
	

*** * ********************************************************************************************
*** LOAD PROPORTION DUE TO CSW RESULTS
*** * ********************************************************************************************
 
   get_model_results, gbd_team("epi") gbd_id("2636") location_id(-1) location_set_id(35) clear
   
	rename mean prop_sex_due_to_csw

	keep if sex_id == 2

	export delimited using "`output'/csw_results.csv", replace
	tempfile csw_results
	save `csw_results', replace

*** * ******************************************************************************************
*** SQUEEZE DISMOD RESULTS FOR HIV TRANSMISSION MODELS
*** * ******************************************************************************************
	
    local MEs 2637 2638 2639 // Proportion HIV .... modelable_entity_ids
    local x=0
    foreach me of local MEs { 
        get_model_results, gbd_team("epi") gbd_id(`me') location_id(-1) location_set_id(35) clear
		drop if sex_id == 1
		gen modelable_entity_id = `me'
        local x = `x' + 1
        tempfile `x'
        save ``x'', replace 
    }
    clear
    forvalues i = 1/`x' {
        append using ``i''
    }
	
    ** re-scale HIV models
        bysort location_id year_id age_group_id sex_id: egen scalarm = total(mean)
		bysort location_id year_id age_group_id sex_id: egen scalaru = total(upper)
		bysort location_id year_id age_group_id sex_id: egen scalarl = total(lower)
        replace mean = mean / scalarm
		replace upper = upper / scalaru
		replace lower = lower / scalarl
		rename mean paf_mean
		rename upper paf_upper
		rename lower paf_lower
		drop scalar*

*** * ******************************************************************************************
*** MERGE DISMOD AND CODCORRECT RESULTS
*** * ******************************************************************************************

// keep only relevant observations. female and healthstate == hiv_sex
	//keep if sex == 2
	keep if modelable_entity_id == 2638 // HIV due to sex 

// merge in outcome data
	merge m:1 location_id year_id age_group_id sex_id using `hiv_cod', keep(3) nogen

// merge in csw data
	merge m:1 location_id year_id age_group_id sex_id using `csw_results', keep(3) nogen

// only want to apply CSW proportions for those age 15-45. Set proportion csw for other ages = 0
	replace prop_sex_due_to_csw = 0 if inlist(age_group_id, 1, 2, 3, 4, 5, 6, 7, 14, 15, 16, 17, 18, 19, 20, 21)

// create non-csw scalar. this will be the proportion of sexually transmitted hiv not attributable to csw = 1 - prop(csw)
	gen prop_non_csw = 1 - prop_sex_due_to_csw

// create attributable deaths for each age, sex, risk entry
	gen att_deaths_all_sex = paf_mean * mean_abs_death
	gen att_ylls_all_sex = paf_mean * mean_abs_yll
	
	gen att_deaths_non_csw = paf_mean * mean_abs_death * prop_non_csw
	gen att_ylls_non_csw = paf_mean * mean_abs_yll * prop_non_csw
	
// gen variables with proportion of hiv deaths/ylls attributable to non_csw sexual transmission for each age group
	gen paf_deaths_non_csw = att_deaths_non_csw / mean_abs_death
	gen paf_ylls_non_csw = att_ylls_non_csw / mean_abs_yll 
	
// variable with overall proportion attributable to non-CSW sexual transmission
	gen prop_sexual_non_csw = paf_mean * prop_non_csw
	
// rename for clarity
	rename paf_mean prop_sexual_all
 
// keep only relevant data:  healthstate = hiv_sex
	keep if modelable_entity_id == 2638
	keep if sex_id == 2

	tempfile all 
	save `all', replace

// Merge on age groups 
	insheet using "`output'/convert_to_new_age_ids.csv", comma names clear 
	merge 1:m age_group_id using `all', keep(3) nogen

	merge m:1 location_id using `country_codes', keep(3) nogen

	rename age_start age 

// keep only relevant vars
	keep age location_id year sex prop_sexual_all mean_abs_death mean_abs_yll prop_sex_due_to_csw prop_non_csw att_deaths_all_sex att_ylls_all_sex att_deaths_non_csw att_ylls_non_csw paf_deaths_non_csw paf_ylls_non_csw prop_sexual_non_csw

// save
	save "`output'/csw_pafs.dta", replace
	export excel using "`output'/csw_pafs.xlsx", firstrow(var) replace
