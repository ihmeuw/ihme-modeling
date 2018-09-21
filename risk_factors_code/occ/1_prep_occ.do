
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

** ILO Data
    insheet using "`ilo_dir'/ILO_ILOSTAT_OCC_LVL_1_SEX.csv", clear

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

        ** sex_item_code = SEX_F/M/T. We only need sex specific and generate a sex variable with 1/2/3
        drop if sex == "SEX_T"
        gen sex_id = 1 if sex == "SEX_M"
        replace sex_id = 2 if sex == "SEX_F"
        
        ** classif1_item_code Contains both ISCO version and code value.
		
        ** Macau has its own classification system. Since Maccau is not a GBD country, I drop it
		** rather than trying to figure out how to crosswalk it's system.
		drop if ihme_loc_id == "MAC" & regexm(classif1_item_code, "^OCU_NSCO_")

        ** classif1_item_code = Contains both ISCO version and code value.
		gen isco_version = regexs(2) if regexm(classif1_item_code, "^(OCU_)([A-Z0-9]*)(_)([A-Z0-9,-]*)$")
        assert isco_version != ""
        
        gen isco_code = regexs(4) if regexm(classif1_item_code, "^(OCU_)([A-Z0-9]*)(_)([A-Z0-9,-]*)$")
        assert isco_code != ""
        
        isid ihme_loc_id sex_id source year_id isco_version isco_code

        // albania 2011-2015 reports a row for isco88 and isco08 for each classification
         duplicates tag ihme_loc_id sex_id source year_id isco_code if isco_version != "AGGREGATE", gen(dupe)
         drop if dupe==1 & isco_version == "ISCO88" & ihme_loc_id == "ALB"
         drop dupe

    ** Deal with notes field, and look for useful notes (like whether the study is subnational)
        ** Stata doesn't like the $'s so get rid of them
        split notes_sourcelabel, gen(notes_) parse(|)
        
        ** Mark subnational estiamtes
		gen subnational = regexm(notes_2, "Geographical")
		replace subnational = regexm(notes_2, "Population coverage") if subnational != 1
		replace subnational = regexm(notes_2, "Establishment size coverage") if subnational != 1
		gen natlrep = 1 
		replace natlrep = 0 if subnational == 1

        * gen natlrep = 1
        * foreach var of var notes_* {
        *     replace natlrep = 0 if `var' == "1437Geographical coverage:Government controlled areas"
        *     replace natlrep = 0 if `var' == "31Geographical coverage:Urban areas only"
        *     replace natlrep = 0 if `var' == "32Geographical coverage:Main city or metropolitan area"
        *     replace natlrep = 0 if `var' == "33Geographical coverage:Main cities or metropolitan areas"
        *     replace natlrep = 0 if `var' == "34Geographical coverage:Total national, excluding some areas"
        *     replace natlrep = 0 if `var' == "35Geographical coverage:Nonstandard geographical coverage"
        *     replace natlrep = 0 if `var' == "44Population coverage:Nonstandard population coverage"
        *     replace natlrep = 0 if `var' == "49Establishment size coverage:All establishments with at least 20 employees"
        * }
        
        
        ** Acording to ILO email the flags mean this:
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
		replace subnational = 1 if regexm(flag, "[CEFLPUZI]")
        
	** Rather than included as 0s if the observation isn't available, drop it
		drop if flag == "N"

	// Keep the totals as denominator, note that sometimes will need to use the aggregate total as denom if total LVL1 wasn't provided
	// After this we can drop all aggregates and totals
		bysort ihme_loc_id year_id source sex year_id: egen t_lvl_1_denominator = sum(obs_value) if isco_code == "TOTAL" & isco_version != "AGGREGATE"
		bysort ihme_loc_id year_id source sex year_id: egen t_agg_denominator = sum(obs_value) if isco_code == "TOTAL" & isco_version == "AGGREGATE"
		bysort ihme_loc_id year_id source sex year_id: egen t_agg_denominator_filled = mean(t_agg_denominator)
		bysort ihme_loc_id year_id source sex year_id: egen denominator = mean(t_lvl_1_denominator)
		bysort ihme_loc_id year_id source sex year_id: replace denominator = t_agg_denominator_filled if denominator == .
		drop t*
		drop if isco_version == "AGGREGATE" | isco_code == "TOTAL"

		order ihme_loc_id year_id source sex obs_value isco* subnational denominator, first
        
    ** Crosswalk ISCOs (see functions file for definition of function)
        ** Relative risks are in terms of ISCO68, but this combines sales workers and services workers.
		** We split them by using the average breakdown of ISCO68.
		** Note that actually over 90% of the data doesn't distinguish this combination, so the ideal 
		** solution would use the later definition, where the two are combined.

	// note  decided to convert occs to isco88 instead of 68 to get more granularity
	// this will require splitting group 7-9 into 7, 8, 9 & group 1-2 into 2, 3
	// i've also added a super-regional element to the split to adjust for spatial variation given our data coverage

		// production split: 7-9 (Production and related workers, transport equipment operators and labourers)
		// into 7 (Craft and related workers), 8 (Plant and machine operators and assemblers), 9 (Elementary occupations) 
		preserve
			keep if isco_version != "ISCO68" & inlist(isco_code, "7", "8", "9")
			** If we don't have all 3 data points, then we can't use it to split, so drop them.
			bysort ihme_loc_id sex source year_id: gen dup = _N
			keep if dup == 3
			drop dup
			
			bysort ihme_loc_id sex_id source year_id: egen prop = pc(obs_value), prop
			collapse (mean) prop, by(super_region_id isco_code)
			
			** Set up for merge with original dataset with a row for each isco_code/isco_version we need.
			rename isco_code isco_code_new 
			replace isco_code_new = "7-9." + isco_code_new
			gen isco_code = "7-9" 
			gen isco_version = "ISCO68"
			tempfile production_split
			save `production_split'
		restore

		// proffesional/technical split: 0-1 (Professional, technical and related workers)
		// into 2 (Professionals), 3 (Technicians and associate professionals)
		preserve
			keep if isco_version != "ISCO68" & inlist(isco_code, "2", "3")
			** If we don't have both data points, then we can't use it to split, so drop them.
			bysort ihme_loc_id sex source year_id: gen dup = _N
			keep if dup == 2
			drop dup
			
			bysort ihme_loc_id sex_id source year_id: egen prop = pc(obs_value), prop
			collapse (mean) prop, by(super_region_id isco_code)
			
			** Set up for merge with original dataset with a row for each isco_code/isco_version we need.
			rename isco_code isco_code_new 
			replace isco_code_new = "0-1." + isco_code_new
			gen isco_code = "0-1" 
			gen isco_version = "ISCO68"

			append using `production_split'

			tempfile isco_68_splits
			save `isco_68_splits'
		restore

		joinby isco_version isco_code super_region_id using `isco_68_splits', unmatched(master)
		replace obs_value = obs_value * prop if _m == 3
		replace isco_code = isco_code_new if _m == 3
		drop _m prop isco_code_new
		
		** Now we're ready to crosswalk
        crosswalk_isco isco_version isco_code, gen(group) 
        
    ** Collapse
        collapse (sum) obs_value (min) natlrep (max) denominator, by(ihme_loc_id sex_id source nid year_id isco_version group)

    ** Get percentage of worUSERce from absolute numbers
        gen exp_unadj = obs_value / denominator
        bysort ihme_loc_id sex_id source nid year_id isco_version: egen total = sum(exp_unadj)
        keep if total > .3 // drop any survey years where the total proportions cover less than 1/3 the worUSERece
		
        drop total obs_value denominator

        tempfile ilo_data
        save `ilo_data'

** Get UK data
    ** Pull all of the UK census data.
    tempfile uk_data
    forvalues year = 2004/2012 {
    	di "working on `year'"
        import excel using "`exp_dir'/nomis_2013_09_17_000654.xls", sheet("Jan `year'-Dec `year'") cellrange(A8:EX20) clear

        ** Format variables
        rename A region
        gen year = `year'
        gen nid = 144372
        local keepvars "nid region year"
        
        ** Extract useful information from first row using regular expressions.
        foreach var of var B-EX {
            ** All percentage variables have a part of the first observation 
            ** that equals xxx: where xxx is a set of numbers
            if regexm(`var'[1], "([1-9a-z,-]*)(:)") local value = regexs(1) 
            else local value = ""
            
            ** Occupations have "(soc2010)"
            local occ = regexm(`var'[1], "(soc2010)")
            
            ** females contain "females" on first line.
            local female = regexm(`var'[1], "female")
            if `female' == 1 local sex "2"
            else local sex "1"
            
            ** Rename variable based on values extracted from line
            if "`value'" != "" & `occ' == 1 {
                local value = subinstr("`value'", ",", "", .)
                local value = subinstr("`value'", "-", "", .)
                
                rename `var' occ`value'_`sex'
                local keepvars "`keepvars' occ`value'_`sex'"
            }
        }
        ** Clean up dataset
        keep `keepvars'
        drop in 1
        
        unab vars: *_1
        local vars = subinstr("`vars'", "_1", "_", .)
        reshape long `vars', i(region year) j(sex)
        
        reshape long occ@_, i(region year sex) j(code) string
        rename occ_ exp_unadj
        replace exp_unadj = "0" if exp_unadj == "!"
        destring exp_unadj, replace
        replace exp_unadj = exp_unadj / 100
        
        capture append using `uk_data'
        save `uk_data', replace
    }

    ** Crosswalk codes
    gen isco_version = "SOC2010"
    crosswalk_isco isco_version code, gen(group)
    collapse (sum) exp_unadj, by(region sex nid year isco_version group)
	
	**standardize location_names
	rename region location_name
	replace location_name = "East of England" if location_name=="East"
	replace location_name = "Greater London" if location_name=="London"
	replace location_name = "North East England" if location_name=="North East"
	replace location_name = "North West England" if location_name=="North West"
	replace location_name = "South East England" if location_name=="South East"
	replace location_name = "South West England" if location_name=="South West"
	replace location_name = "Yorkshire and the Humber" if location_name == "Yorkshire and The Humber"
	merge m:m location_name using `location_metadata', keep(1 3) keepusing(ihme_loc_id location_id) nogen // used m:m because some other random subnat locnames are duped
	drop location_name

	**standardize varnames
	rename sex sex_id
	rename year year_id
    gen natlrep = 1
    gen source = "UK Census"
        
	
    save `uk_data', replace
	
**Prep China data

	**CHINA_1995_3_2
	import excel using "`exp_dir'/CHINA_1995_3-2.xls", cellrange(A3:BC36) firstrow clear
	
	**Standardize occ proportions
	rename (AV AW) (cat_7_1 cat_7_2)
	rename (AP AQ) (cat_6_1 cat_6_2)
	rename (AJ AK) (cat_5_1 cat_5_2)
	rename (AD AE) (cat_4_1 cat_4_2)
	rename (X Y) (cat_3_1 cat_3_2)
	rename (R S) (cat_2_1 cat_2_2)
	rename (L M) (cat_1_1 cat_1_2)
	rename (BB BC) (cat_0_1 cat_0_2)
	
	**clean up dataset
	keep Region cat*
	
	**standardize location variable
	rename Region location_name
	replace location_name = "China" if location_name=="Total"
	drop if location_name == ""
	merge m:m location_name using `location_metadata', keep(3) keepusing(ihme_loc_id location_id super_region_id) nogen
	
	**destring variables
	forvalues s = 1/2 {
		forvalues n = 0/7 {
				replace cat_`n'_`s' = "." if cat_`n'_`s' == "--"
				destring(cat_`n'_`s'), replace
				replace cat_`n'_`s' = cat_`n'_`s'/100
				}
			}

	**reshape data
		reshape long cat_, i(location_id) j(variable) string
		split variable, gen(var) parse(_)
		rename var1 isco_code
		rename var2 sex_id
		destring sex_id, replace
		rename cat_ exp_unadj
		gen isco_version = "ISCO68"
		replace isco_code = "0-1" if isco_code == "1"
		replace isco_code = "7-9" if isco_code == "7"
		replace isco_code = "X" if isco_code == "0"

	// split isco68 aggregates using previously generated super region patterns, then convert to isco88
		joinby isco_version isco_code super_region_id using `isco_68_splits', unmatched(master)
		replace exp_unadj = exp_unadj * prop if _m == 3
		replace isco_code = isco_code_new if _m == 3
		drop _m prop isco_code_new
		
		** Now we're ready to crosswalk
        crosswalk_isco isco_version isco_code, gen(group) 
        
	**generate other variables to standardize dataset
		gen year_id = 1995
		gen nid = 74290 
		gen source = "China 1% National Population Sample Survey"
		gen natlrep = 1 /*modeling subnational locations independently so okay to assume natlrep = 1 for this dataset*/

	  ** Collapse
        collapse (sum) exp_unadj (min) natlrep, by(location_id ihme_loc_id sex_id source nid year_id isco_version group)

		tempfile chn_1995
		save `chn_1995', replace
	
	**CHINA_2000_L4-2
	import excel using "`exp_dir'/CHINA_2000_L4-2.xlsx", cellrange(A3:HF36) firstrow clear
	
	**standardize location variable
	rename Region location_name
	replace location_name = "China" if location_name=="Total"
	replace location_name = trim(strproper(location_name))
	drop if location_name == ""
	merge m:m location_name using `location_metadata', keep(3) keepusing(ihme_loc_id location_id super_region_id) nogen
	
	**Standardize occ proportions
	rename (EB EC) (cat_7_1 cat_7_2) /*production/transport*/
	rename (BQ BR) (cat_3_1 cat_3_2) /*clerical and related*/
	rename (F G) (cat_2_1 cat_2_2) /*admin/managerial*/
	rename (X Y) (cat_1_1 cat_1_2) /*professional/technical*/
	rename (CI CJ CL CM) (sales1_1 sales1_2 sales2_1 sales2_2) 
	rename (CO CP CR CS CU CV CX CY DA DB DD DE) ///
		(serv1_1 serv1_2 serv2_1 serv2_2 serv3_1 serv3_2 serv4_1 serv4_2 serv5_1 serv5_2 serv6_1 serv6_2) 
	rename (DJ DK DM DN DP DQ DS DT) (agri_1 agri_2 fores_1 fores_2 anim_1 anim_2 fish_1 fish_2)
		
	foreach var in "sales1" "sales2" "serv1" "serv2" "serv3" "serv4" "serv5" "serv6" "agri" "fores" "anim" "fish" {
	forvalues n = 1/2 {
		destring(`var'_`n'), replace 
		}
	}
			
	forvalues n = 1/2 {
		gen cat_4_`n' = sales1_`n' + sales2_`n'
		gen cat_5_`n' = serv1_`n' + serv2_`n' + serv3_`n' + serv4_`n' + serv5_`n' + serv6_`n'
		gen cat_6_`n' = agri_`n' + fores_`n' + anim_`n' + fish_`n' 
	}
	
	**format occ prop variables
	**destring variables
	destring(C D), replace 
	forvalues s = 1/2 {
		forvalues n = 1/7 {
				destring(cat_`n'_`s'), replace
				}
			}
			
	**calculate proportions
	forvalues s = 1/2 {
		forvalues n = 1/7 {	
				**males
				if `s'==1 {
					replace cat_`n'_`s' = cat_`n'_`s' / C
				}
				**females
				if `s'==2 {
					replace cat_`n'_`s' = cat_`n'_`s' / D
				}
			}
		}
		
	**Clean up dataset
	keep location_name location_id super_region_id ihme_loc_id cat*
	
	**generate other variables to standardize dataset
	gen year_id = 2000
	gen nid = 60470 
	gen source = "Population Census Data Assembly"
	gen natlrep = 1 /*modeling subnational locations independently so okay to assume natlrep = 1 for this dataset*/
	
	**reshape data
		reshape long cat_, i(location_id) j(variable) string
		split variable, gen(var) parse(_)
		rename var1 isco_code
		rename var2 sex_id
		destring sex_id, replace
		rename cat_ exp_unadj
		gen isco_version = "ISCO68"
		replace isco_code = "0-1" if isco_code == "1"
		replace isco_code = "7-9" if isco_code == "7"
		replace isco_code = "X" if isco_code == "0"

	// split isco68 aggregates using previously generated super region patterns, then convert to isco88
		joinby isco_version isco_code super_region_id using `isco_68_splits', unmatched(master)
		replace exp_unadj = exp_unadj * prop if _m == 3
		replace isco_code = isco_code_new if _m == 3
		drop _m prop isco_code_new
		
		** Now we're ready to crosswalk
        crosswalk_isco isco_version isco_code, gen(group) 

    ** Collapse
        collapse (sum) exp_unadj (min) natlrep, by(location_id ihme_loc_id sex_id source nid year_id isco_version group)


	**save
	tempfile chn_2000
	save `chn_2000', replace 
	
	**CHINA_2005_5-3
	import excel using "`exp_dir'/CHINA_2005_5-3.xlsx", cellrange(A4:I124) firstrow clear
	
	**Standardize sex variable
	gen sex = 2 if _n>82 
	replace sex = 1 if _n>43 & _n<82
	drop if sex == . 
	
	**standardize location variable
	rename Sex location_name
	replace location_name = "China" if (location_name=="Region" | location_name == "Female" | location_name == "Male") 
	drop if location_name == ""
	replace location_name = "Heilongjiang" if location_name=="Heilongji"
	replace location_name = "Inner Mongolia" if location_name=="Inner Mon"
	merge m:m location_name using `location_metadata', keep(3) keepusing(ihme_loc_id location_id super_region_id) nogen
	
	**generate other variables to standardize dataset
	rename sex sex_id
	destring sex_id, replace
	gen year_id = 2005
	gen nid = 2918 
	gen source = "China 1% National Population Sample Survey"
	gen natlrep = 1
	
	**generate occupation proportions
	gen cat_1 = ProfessionalTechnical/Total
	gen cat_2 = AdministratorManager/Total
	gen cat_3 = ClericalRelatedWorkers/Total
	gen cat_5 = SalesServiceWorkers/Total
	gen cat_6 = AgricultureRelatedWorkers/Total
	gen cat_7 = ProductionRelatedWorkers/Total
	gen cat_0 = NotStated/Total

	**reshape data
		reshape long cat_, i(sex_id location_id) j(variable) string
		split variable, gen(var) parse(_)
		rename variable isco_code
		rename cat_ exp_unadj
		gen isco_version = "ISCO68"
		replace isco_code = "0-1" if isco_code == "1"
		replace isco_code = "7-9" if isco_code == "7"
		replace isco_code = "X" if isco_code == "0"

	// split isco68 aggregates using previously generated super region patterns, then convert to isco88
		joinby isco_version isco_code super_region_id using `isco_68_splits', unmatched(master)
		replace exp_unadj = exp_unadj * prop if _m == 3
		replace isco_code = isco_code_new if _m == 3
		drop _m prop isco_code_new
		
		** Now we're ready to crosswalk
        crosswalk_isco isco_version isco_code, gen(group) 

    ** Collapse
        collapse (sum) exp_unadj (min) natlrep, by(location_id ihme_loc_id sex_id source nid year_id isco_version group)

	tempfile chn_2005
	save `chn_2005', replace
	
	**compile datasets
	use `ilo_data', clear
	merge m:1 ihme_loc_id using `location_metadata', keep(3) keepusing(location_name location_id region_id super_region_id) nogen
	append using `uk_data'		
	merge m:1 ihme_loc_id using `location_metadata', keep(3) keepusing(location_name location_id) nogen
	
**append subnational china data 
append using `chn_1995' `chn_2000' `chn_2005'
merge m:1 ihme_loc_id using `location_metadata', keep(3) keepusing(region_id super_region_id) nogen

**Gen variables for crosswalking between different versions.
    gen cv_isco68 = (isco_version == "ISCO68")
    gen cv_isco08 = (isco_version == "ISCO08")
    gen cv_iscouk = (isco_version == "SOC2010")

lowess exp_unadj year_id, by(location_id sex_id group) gen(lowess_hat) nograph    

	gen residual = lowess_hat - exp_unadj
	gen sd = .

	foreach year of numlist 1970/2015 {
		bysort location_id group sex_id: egen temp = sd(residual) if inrange(year_id, `year'-5, `year'+5)
		replace sd = temp if year_id == `year'
		drop temp
	}	

	foreach year of numlist 1970/1975 {
		bysort location_id group sex_id: egen temp = sd(residual) if inrange(year_id, 1970, `year'+10-(`year'-1970))
		replace sd = temp if year_id == `year'
		drop temp
	}

	foreach year of numlist 2010/2015 {
		bysort location_id group sex_id: egen temp = sd(residual) if inrange(year_id, `year'-10+(2015-`year'), 2015)
		replace sd = temp if year_id == `year'
		drop temp
	}

	replace sd = . if exp_unadj == . 

// use regional/superregional avg CV to impute SD where missing (countries with less than 3 years of data -> cant fit lowess)
gen cv = sd / exp_unadj
bysort region_id: egen region_avg_cv = mean(cv) 
bysort super_region_id: egen super_region_avg_cv = mean(cv) 
replace sd = exp_unadj * region_avg_cv if sd == .
replace sd = exp_unadj * super_region_avg_cv if sd == .
drop cv *cv

*Get all of the final variables needed for ST-GPR
rename exp_unadj data
gen age_group_id=22
gen variance = sd^2
rename sd standard_deviation
gen sample_size = .
rename isco_version cv_isco_version
gen cv_subgeo = 0 // prefered way to downweight points
replace cv_subgeo = 1 if natlrep != 1
drop lowess residual natlrep 

**save whole dataset
save "`output_dir'/prepped_occ`timestamp'.dta", replace 
outsheet using "`output_dir'/prepped_occ`timestamp'.csv", comma replace 


**make names cleaner
gen category = "mgmt" if group == 1
replace category = "pro" if group == 2
replace category = "tech" if group == 3
replace category = "clerk" if group == 4
replace category = "sales" if group == 5
replace category = "agri" if group == 6
replace category = "craft" if group == 7
replace category = "machine" if group == 8
replace category = "elem" if group == 9
replace category = "undef" if group == 0

export delimited using "`output_dir'/prepped_occ_major.csv", nolabel replace 

** save each separatelyqstat
levelsof category, local(cats)

foreach category of local cats {

		di in yellow "starting with occ_occ_`category'"

		preserve

		keep if category == "`category'"
		summ group
		local code = `r(mean)'
		gen me_name = "occ_occ_major_`code'_"+lower("`category'")
		local location = "`output_dir'/occ_occ_major_`code'_"+lower("`category'")+".csv"
		outsheet using "`location'", comma replace

		restore
}