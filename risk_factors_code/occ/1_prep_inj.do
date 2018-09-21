// *********************************************************************************************************************************************************************
** Prepare raw economic activity data for modelling for occupational exposures
// *********************************************************************************************************************************************************************

** **************************************************************************
** RUNTIME CONFIGURATION
** **************************************************************************
// Set preferences for STATA
	// Clear memory and set memory and variable limits
		clear all
		set mem 12g
		set maxvar 32000
	// Set to run all selected code without pausing
		set more off
	// Set graph output color scheme
		set scheme s1color
	// Remove previous restores
		capture restore, not
	// Define drive (data) for cluster (UNIX) and Windows (Windows)
		if c(os) == "Unix" {
			global j "FILEPATH"
			global h "FILEPATH"
			set odbcmgr unixodbc
		}
		else if c(os) == "Windows" {
			global j "FILEPATH"
			global h "FILEPATH"
		}
		
// Close previous logs
	cap log close
	
// Create timestamp for logs
	local c_date = c(current_date)
	local c_time = c(current_time)
	local c_time_date = "`c_date'"+"_" +"`c_time'"
	display "`c_time_date'"
	local time_string = subinstr("`c_time_date'", ":", "_", .)
	local timestamp = subinstr("`time_string'", " ", "_", .)
	display "`timestamp'"
	
// Create macros to contain toggles for various blocks of code:
	local prep 0

// Store filepaths/values in macros
	local code_dir 				"FILEPATH"
	local exp_dir 				"FILEPATH"
	local gbd_functions			"FILEPATH"
	local ilo_dir 				"FILEPATH"
	local tabs_dir				"FILEPATH"
	local output_dir 			"FILEPATH"
	local logs_dir				"FILEPATH"
	local age_mapping 			"FILEPATH"

// Function library	
	// GBD SHARED
		include `gbd_functions'/get_covariate_estimates.ado
		include `gbd_functions'/get_demographics.ado
		include `gbd_functions'/get_location_metadata.ado
	// crosswalking
	    include "`code_dir'/FILEPATH"

// Set to log
	if c(os) == "Unix" log using "`logs'/prep_for_GPR`timestamp'.log", replace

// Save critical values
    local min_value = .001 // This is the min value subbed in for 0s

// Read in location metadata
	get_location_metadata, location_set_id(35) clear
	// Keep if national or subnational
		keep if level >= 3

	//cleanup
		keep location_id location_name super_region_id region_id ihme_loc_id

	tempfile location_metadata
	save `location_metadata', replace

** ILO Data - injury rates
    insheet using "`ilo_dir'/ILO_ILOSTAT_IND_FTL_INJURIES_RATE_SEX.csv", comma clear

    // cleanup
    drop classif2* classif3* classif4* classif5* collection* sexlabel
    rename ref_area ihme_loc_id
    rename ref_arealabel ilo_location_name
    rename obs_status flag
    rename obs_statuslabel flag_label
    rename time year_id
    rename classif1 classif1_item_code
    rename classif1label occ_label

    ** Gather unique identifiers, and convert them to ihme standards
        gen nid = 144370
        isid ilo_location_name sex source year_id classif1_item_code
        
        ** Merge on location metadata
        replace ihme_loc_id = "CHN_354" if ihme_loc_id == "HKG"
        replace ihme_loc_id = "CHN_361" if ihme_loc_id == "MAC"
		merge m:1 ihme_loc_id using `location_metadata', keep (match) nogen
       
        ** classif1_item_code = Contains both ISIC version and code value.
        gen isic_version = regexs(2) if regexm(classif1_item_code, "^(ECO_)(ISIC[1-9])(_)([A-Z0-9,-]*)$")
        drop if isic_version == "" //aggregate values
        
        ** Code is last letter/number or the word TOTAL
        gen isic_code = regexs(4) if regexm(classif1_item_code, "^(ECO_)(ISIC[1-9])(_)([A-Z0-9,-]*)$")

        isid ihme_loc_id sex source year_id isic_version isic_code
        
    * ** Look for useful notes (like whether the study is subnational)
    *     ** Stata throws errors when there are $'s so get rid of them
    *     replace qtable_notes_string = subinstr(qtable_notes_string, "$", "", .)
    *     split qtable_notes_string, gen(notes_) parse(#)
        
		gen natlrep = 1 // observation is nationally representative (and reliable)
		gen cw_cvg_rep = 0 // the coverage is declared to be "reported"
		gen cw_cvg_comp = 0 // the coverage is declared to be "compensated"
		gen cw_inc_dis= 1 // estimate includes diseases, in addition to injuries
		gen cw_wrks_100k = 0 // estimate is in terms of a rate per 100,000 workers (dUSERt)
		gen cw_insured = 0 // estimate is in terms of a rate per 100,000 insured or reference group "ensured persons"
		gen cw_wrks_1k = 0 // estimate is in terms of a rate per 1,000 workers (fix by multiplying by 100)
		gen cw_wrks_1mh = 0 // estimate is in terms of a rate per 1,000,000 hours (fix by multiplying by 100000 / (40 * 50 * 1000000))
		gen cw_fu_1d = 0 // estimate only includes deaths within 1 day of accident (dUSERt=1 year)
		gen cw_fu_15d = 0 // estimate only includes deaths within 15 days of accident (dUSERt=1 year)
		gen cw_fu_1m = 0 // estimate only includes deaths within 1 month of accident (dUSERt=1 year)
		gen cw_fu_6m = 0
		gen cw_fu_1y = 0
		gen cw_fu_2y = 0

		    ** Deal with notes field, and look for useful notes (like whether the study is subnational)
        ** Stata doesn't like the $'s so get rid of them
        split notes_sourcelabel, gen(source_notes_) parse(|)
        split note_indicatorlabel, gen(ind_notes_) parse(|)
        
        foreach var of var source_notes_* ind_notes_* {

        	di "scanning `var'"

        	replace `var' = trim(`var') //removing leading/trailing blanks

			replace natlrep = 0 if `var' == "Establishment size coverage: All establishments with at least 10 employees"
			replace natlrep = 0 if `var' == "Establishment size coverage: All establishments with at least 16 employees"
			replace natlrep = 0 if `var' == "Establishment size coverage: All establishments with at least 20 employees"
			replace natlrep = 0 if `var' == "Establishment size coverage: All establishments with at least 50 employees"
			replace natlrep = 0 if `var' == "Establishment size coverage: All establishments with at least 100 employees"
			replace natlrep = 0 if `var' == "Geographical coverage: Main city or metropolitan area" 
			replace natlrep = 0 if `var' == "Geographical coverage: Total national, excluding some areas"
			replace natlrep = 0 if `var' == "Geographical coverage: Non applicable"
			replace natlrep = 0 if `var' == "Population coverage: Nonstandard population coverage"
			replace natlrep = 0 if `var' == "Establishment size coverage: All establishments with at least 20 employees"
			replace natlrep = 0 if `var' == "Establishment size coverage: Nonstandard establishment size coverage"
			replace natlrep = 0 if `var' == "Reference group coverage: Insured persons"
			replace natlrep = 0 if `var' == "Reference group coverage: Reference group coverage: Wage earners / blue collar / production workers" 
			replace natlrep = 0 if `var' == "Institutional sector coverage: Private sector only"
			replace natlrep = 0 if `var' == "Institutional sector coverage: Public sector only"
			replace natlrep = 0 if `var' == "Institutional sector coverage: Nonstandard institutional sector coverage"
			replace natlrep = 0 if `var' == "Type of cases of occupational injuries: Nonstandard coverage of occupational injuries"
			
			replace cw_cvg_rep = 1 if `var' == "Coverage of occupational injuries: Reported injuries"
			
			replace cw_cvg_comp = 1 if `var' == "Coverage of occupational injuries: Compensated injuries"
			
            replace cw_inc_dis = 0 if `var' == "Type of cases of occupational injuries: Including cases of occupational disease"
            replace cw_inc_dis = 0 if `var' == "Type of cases of occupational injuries: Including cases of injury due to commuting accidents"
            replace cw_inc_dis = 0 if `var' == "Type of cases of occupational injuries: Including cases of occupational disease and cases of injury due to commuting accidents"

			
			replace cw_wrks_100k = 1 if `var' == "Type of rate: Incidence rate (per 100'000 employees)"
			replace cw_wrks_100k = 1 if `var' == "Type of rate: Incidence rate (per 100'000 workers employed)"
			replace cw_wrks_100k = 1 if `var' == "Type of rate: Incidence rate (per 100'000 workers exposed to risk)"
			replace cw_insured = 1 if `var' == "Type of rate: Incidence rate (per 100'000 persons insured)"
			replace cw_insured = 1 if `var' == "Reference group coverage: Insured persons"
			replace cw_wrks_100k = 1 if `var' == "Type of rate: Incidence rate (per 100'000 full-time equivalents)"

			replace cw_wrks_1k = 1 if `var' == "Type of rate: Incidence rate (per 1'000 employees)"
			
			replace cw_wrks_1mh = 1 if `var' == "Type of rate: Frequency rate (per 1'000'000 hours worked)"
			
			replace cw_fu_1d = 1 if `var' == "Time period for occurrence of death: Deaths occurring within 1 day"
			replace cw_fu_1d = 1 if `var' == "Time period for occurrence of death: Deaths occurring within 2 days"
			replace cw_fu_1d = 1 if `var' == "Time period for occurrence of death: Deaths occurring within 3 days"
			replace cw_fu_1d = 1 if `var' == "Time period for occurrence of death: Deaths occurring within 4 days"
			replace cw_fu_1d = 1 if `var' == "Time period for occurrence of death: Deaths occurring within 5 days"
			replace cw_fu_1m = 1 if `var' == "Time period for occurrence of death: Deaths occurring within 15 days"        
			replace cw_fu_1m = 1 if `var' == "Time period for occurrence of death: Deaths occurring within 1 month"
			replace cw_fu_6m = 1 if `var' == "Time period for occurrence of death: Deaths occurring within 4 months"
			replace cw_fu_6m = 1 if `var' == "Time period for occurrence of death: Deaths occurring within 6 months"
			replace cw_fu_1y = 1 if `var' == "Time period for occurrence of death: Deaths occurring within the same reference year"
			replace cw_fu_1y = 1 if `var' == "Time period for occurrence of death: Deaths occurring within the same reference financial year"
			replace cw_fu_2y = 1 if `var' == "Time period for occurrence of death: Deaths occurring within 2 years"
		}
        
	** According to ILO email the flags mean this:
		** C Confidential value
		** E Estimated value
		** F Forecast value
		** L Computed value
		** P Provisional
		** S Not significant
		** X Not applicable
		** N Not available
		** U Unreliable
		** R Recoded category
		** Z Non-official estimated value
		** I Imputed value
	** determined that flags C, E, F, L, P, U, Z and I to be subnational because they don't look as reliable
		replace natlrep = 0 if regexm(flag, "[CEFLPUZI]")
        
	** Rather than included as 0s if the observation isn't available, drop it
		drop if flag == "N"
		
	** Make sure that the crosswalk variables are the same across the same survey so we can crosswalk ISIC defs before
	** dealing with other crosswalk variables.
		foreach var of var cw* {
			bysort ihme_loc_id sex source nid year_id isic_version: egen mean_`var' = mean(`var')
			assert `var' == mean_`var'
			drop mean_`var'
		}
		
		
     ** Convert to 100,000 workers from 1,000 workers (multiply by 100) and 1,000,000 hours by assuming 40 hour week, 50 week year
		rename obs_value rate_unadj
		
		replace rate_unadj = rate_unadj * (100000 * (50 * 40) / 1000000) if cw_wrks_1mh == 1
		
	// outliers
	gen outlier = 0
	replace outlier = 1 if rate_unadj > 1000 // note this will also drop missing
	replace outlier = 1 if rate_unadj > 500 & occ_label != "ISIC-Rev.2: Mining and Quarrying" & occ_label != "ISIC-Rev.2: Transport, Storage and Communication" & occ_label != "ISIC-Rev.3: Fishing"  & occ_label != "ISIC-Rev.3: Transport, storage and communications"
	replace outlier = 1 if source == "DA:2502" 	
	replace outlier = 1 if sourcelabel == "Records of workers' organizations"  
	replace outlier = 1 if ihme_loc_id == "THA" & year_id == 2013 
	replace outlier = 1 if ihme_loc_id == "KOR" & isic_code == "2" & rate_unadj > 300 
	replace outlier = 1 if ihme_loc_id == "MMR" & year_id < 2007 
	replace outlier = 1 if ihme_loc_id == "PHL" 
	replace outlier = 1 if ihme_loc_id == "JPN" 
	replace outlier = 1 if ihme_loc_id == "BHR" & year_id == 1985 & isic_code == "TOTAL" 

	// cleanup
		keep ihme_loc_id sex year_id ilo_location_name source sourcelabel occ_label rate_unadj location_name super_region_id region_id isic_version isic_code natlrep cw* outlier

		export delimited using "`output_dir'/pre_cw_occ_inj.csv", nolabel replace 

		
    ** Crosswalk ISICs (see functions file for definition of function)
        ** Can't get useful information from all codes:
        crosswalk_isic isic_version isic_code, gen(isic_group) 
        **gen new_isic_version = "GBD"
		**crosswalk_isic_inj new_isic_version isic_group, gen(group) 
        **drop new_isic_version isic_group

  *   ** Collapse
  *       **collapse (mean) obs_value cw* (min) natlrep [fweight=weight], by(iso3 sex survey nid year isic_version group)
  *       gen round_weight = round(weight) //cant use non-integer weights
		* collapse (mean) obs_value cw* (min) natlrep [fweight=round_weight], by(ihme_loc_id sex source nid year_id isic_version isic_group)
		* rename obs_value rate_unadj
		
  *   ** Crosswalk data where needed
		* ** ISIC versions have slightly different definitions.
		* gen cw_isic3 = (isic_version == "ISIC3")
		* gen cw_isic4 = (isic_version == "ISIC4")
				
        tempfile ilo_data
        save `ilo_data'