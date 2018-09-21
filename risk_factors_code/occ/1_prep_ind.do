
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

	duplicates drop location_name, force
	tempfile loc_duplicates
	save `loc_duplicates', replace

** ILO Data - injury rates
    insheet using "`ilo_dir'/ILO_ILOSTAT_IND_LVL_1_SEX.csv", comma clear

    // cleanup

    drop classif2* classif3* classif4* classif5* collection* sexlabel
    drop if obs_value == . // get rid of missing values
    rename ref_area ihme_loc_id
    rename ref_arealabel ilo_location_name
    rename obs_status flag
    rename obs_statuslabel flag_label
    rename time year_id
    rename classif1 classif1_item_code
    rename classif1label occ_label

    ** Gather unique identifiers, and convert them to ihme standards
		**Enter NID for International Labour Organization Database (ILOSTAT) - Employment by Sex and Economic Activity
        gen nid = 144369 
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
        drop if isic_code == "2-9" //aggregate values 

        isid ihme_loc_id sex source year_id isic_version isic_code
        
    ** Deal with notes field, and look for useful notes (like whether the study is subnational)
        ** Stata doesn't like the $'s so get rid of them
        split notes_sourcelabel, gen(notes_) parse(|)
        
        ** Mark subnational estiamtes
		gen subnational = regexm(notes_2, "Geographical")
		replace subnational = regexm(notes_2, "Population coverage") if subnational != 1
		replace subnational = regexm(notes_2, "Establishment size coverage") if subnational != 1
		gen natlrep = 1 
		replace natlrep = 0 if subnational == 1

        
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
		replace natlrep = 0 if regexm(flag, "[CEFLPUZI]")
  	
	** Rather than included as 0s if the observation isn't available, drop it
		drop if flag == "N"

	 ** Get percentage of worUSERce from absolute numbers
        gen temp = .
        replace temp = obs_value if isic_code == "TOTAL"
        bysort ihme_loc_id sex source nid year_id isic_version: egen total = max(temp)
        drop if isic_code == "TOTAL"
		drop if total == . // Can't use data if we don't have denominator
        gen exp_unadj = obs_value / total
        drop temp total obs_value

     // fix sex id
     	gen sex_id = 3 if sex == "SEX_T"
     	replace sex_id = 1 if sex == "SEX_M"
     	replace sex_id = 2 if sex == "SEX_F"

     // final cleanup
     keep ihme_loc_id location_id location_name source sourcelabel sex_id year_id nid isic_version isic_code natlrep exp_unadj
        
        tempfile ilo_data
        save `ilo_data'

 ** UK Census data
    ** UK data doesn't have as detailed of a split as ILO data. Use the ILO's UK data to get a split then apply the national average 
    ** the category B/D/E. WE will 
    use `ilo_data', clear
    keep if ihme_loc_id == "GBR"

    preserve
    ** split BDE
    keep if inlist(isic_code, "B", "D", "E") & isic_version == "ISIC4"
    ** UK has 2 sets of estimates for some years. Use the ones which are available for all years, as they are the ones we
    ** have from the census.
    keep if sourcelabel == "Labour force survey"
    isid isic_code year_id sex_id
    ** Create percent variables and merge variables
    bysort year_id sex_id: egen pct = pc(exp_unadj), prop
    gen code = "bde"
    gen new_code = isic_code
    keep year_id sex_id code new_code pct

    ** Carry backward first year with ISIC4 to use for ISIC 3 years (pre 2008)
    quietly summ year
    local carry_year = r(min)
    local first_year = `carry_year' - 1
    local last_year = 2004
    forvalues y = `last_year'/`first_year' {
        expand 2 if year == `carry_year', gen(dup)
        replace year = `y' if dup == 1
        drop dup
    }

    tempfile bde_split
    save `bde_split'

    restore

    preserve
    ** split GI
    keep if inlist(isic_code, "G", "I")
    ** UK has 2 sets of estimates for some years. Use the ones which are available for all years, as they are the ones we
    ** have from the census.
    keep if sourcelabel == "Labour force survey"
    isid isic_code year_id sex_id
    ** Create percent variables and merge variables
    bysort year_id sex_id: egen pct = pc(exp_unadj), prop
    gen code = "gi"
    gen new_code = isic_code
    keep year_id sex_id code new_code pct

    ** Carry backward first year with ISIC4 to use for ISIC 3 years (pre 2008)
    quietly summ year
    local carry_year = r(min)
    local first_year = `carry_year' - 1
    local last_year = 2004
    forvalues y = `last_year'/`first_year' {
        expand 2 if year == `carry_year', gen(dup)
        replace year = `y' if dup == 1
        drop dup
    }
    
    tempfile gi_split
    save `gi_split'

    restore
    preserve
    ** split HJ
    keep if inlist(isic_code, "H", "J")
    ** UK has 2 sets of estimates for some years. Use the ones which are available for all years, as they are the ones we
    ** have from the census.
    keep if sourcelabel == "Labour force survey"
    isid isic_code year_id sex_id
    ** Create percent variables and merge variables
    bysort year_id sex_id: egen pct = pc(exp_unadj), prop
    gen code = "hj"
    gen new_code = isic_code
    keep year_id sex_id code new_code pct
    
    ** Carry backward first year with ISIC4 to use for ISIC 3 years (pre 2008)
    quietly summ year
    local carry_year = r(min)
    local first_year = `carry_year' - 1
    local last_year = 2004
    forvalues y = `last_year'/`first_year' {
        expand 2 if year == `carry_year', gen(dup)
        replace year = `y' if dup == 1
        drop dup
    }

    tempfile hj_split
    save `hj_split'

    restore

    preserve
    ** split KN
    keep if inlist(isic_code, "K", "L", "M", "N") & year_id > 2004
    ** UK has 2 sets of estimates for some years. Use the ones which are available for all years, as they are the ones we
    ** have from the census.
    keep if sourcelabel == "Labour force survey"
    isid isic_code year_id sex_id
    ** Create percent variables and merge variables
    bysort year_id sex_id: egen pct = pc(exp_unadj), prop
    gen code = "kn"
    gen new_code = isic_code
    keep year_id sex_id code new_code pct
    
    ** Carry backward first year with ISIC4 to use for ISIC 3 years (pre 2008)
    quietly summ year
    local carry_year = r(min)
    local first_year = `carry_year' - 1
    local last_year = 2004
    forvalues y = `last_year'/`first_year' {
        expand 2 if year == `carry_year', gen(dup)
        replace year = `y' if dup == 1
        drop dup
    }

    tempfile kn_split
    save `kn_split'

    restore

    preserve
    ** split OQ
    keep if inlist(isic_code, "O", "P", "Q")
    ** UK has 2 sets of estimates for some years. Use the ones which are available for all years, as they are the ones we
    ** have from the census.
    keep if sourcelabel == "Labour force survey"
    isid isic_code year_id sex_id
    ** Create percent variables and merge variables
    bysort year_id sex_id: egen pct = pc(exp_unadj), prop
    gen code = "oq"
    gen new_code = isic_code
    keep year_id sex_id code new_code pct
    
    ** Carry backward first year with ISIC4 to use for ISIC 3 years (pre 2008)
    quietly summ year
    local carry_year = r(min)
    local first_year = `carry_year' - 1
    local last_year = 2004
    forvalues y = `last_year'/`first_year' {
        expand 2 if year == `carry_year', gen(dup)
        replace year = `y' if dup == 1
        drop dup
    }

    tempfile oq_split
    save `oq_split'

    restore

    preserve
    ** split RU
    keep if inlist(isic_code, "R", "S", "T", "U")
    ** UK has 2 sets of estimates for some years. Use the ones which are available for all years, as they are the ones we
    ** have from the census.
    keep if sourcelabel == "Labour force survey"
    isid isic_code year_id sex_id
    ** Create percent variables and merge variables
    bysort year_id sex_id: egen pct = pc(exp_unadj), prop
    gen code = "ru"
    gen new_code = isic_code
    keep year_id sex_id code new_code pct

    ** Carry backward first year with ISIC4 to use for ISIC 3 years (pre 2008)
    quietly summ year
    local carry_year = r(min)
    local first_year = `carry_year' - 1
    local last_year = 2004
    forvalues y = `last_year'/`first_year' {
        expand 2 if year == `carry_year', gen(dup)
        replace year = `y' if dup == 1
        drop dup
    }

    // now  collect all splits
    append using `bde_split'
    append using `gi_split'
    append using `hj_split'
    append using `kn_split'
    append using  `oq_split'

    tempfile uk_splits
    save `uk_splits'

    restore

 ** Pull all of the UK census data.
    tempfile uk_data
    forvalues year = 2004/2012 {
        import excel using "`exp_dir'/nomis_2013_09_17_000654.xls", sheet("Jan `year'-Dec `year'") cellrange(A8:EX20) clear
        **Source: Nomis Labor Market Statistics Database - Annual Population Survey, Economic Activity Tables, NID = 144372
		gen nid = 144372
        
        ** Format variables
        rename A region
        gen year_id = `year'
        local keepvars "nid region year_id"
        
        ** Extract useful information from first row using regular expressions.
        foreach var of var B-EX {
            ** All percentage variables have a part of the first observation 
            ** that equals xxx: where xxx is a set of letters, commas or hyphens
            if regexm(`var'[1], "([1-9a-z,-]*)(:)") local value = regexs(1) 
            else local value = ""
            
            ** Economic activities have "(sic 2007)"
            local econ_act = regexm(`var'[1], "(sic 2007)")
            
            ** females contain "females" on first line.
            local female = regexm(`var'[1], "female")
            if `female' == 1 local sex "2"
            else local sex "1"
            
            ** Rename variable based on values extracted from line
            if "`value'" != "" & `econ_act' == 1 {
                local value = subinstr("`value'", ",", "", .)
                local value = subinstr("`value'", "-", "", .)
                
                rename `var' `value'_`sex'
                local keepvars "`keepvars' `value'_`sex'"
            }
        }
        ** Clean up dataset
        keep `keepvars'
        drop in 1
        
        unab vars: *_1
        local vars = subinstr("`vars'", "1", "", .)
        reshape long `vars', i(region year) j(sex_id)
        
        reshape long @_, i(region year sex_id) j(code) string
        rename _ exp_unadj
        replace exp_unadj = "0" if exp_unadj == "!"
        destring exp_unadj, replace
        replace exp_unadj = exp_unadj / 100
        
        capture append using `uk_data'
        save `uk_data', replace
    }
    
    ** Split using ilo data
    joinby year_id sex_id code using `uk_splits', unmatched(master)
    assert _m == 3 if inlist(code, "bdi", "gi", "hj", "kn", "oq", "ru")
    replace exp_unadj = exp_unadj * pct if _m == 3
    replace code = new_code if _m == 3
    drop _m new_code pct

    ** cleanup
    gen isic_code = upper(code)
    gen isic_version = "SIC2007"
    gen natlrep = 1
    gen source = "UK Census"

	
	**standardize location_names
	rename region location_name
	replace location_name = "East of England" if location_name=="East"
	replace location_name = "Greater London" if location_name=="London"
	replace location_name = "North East England" if location_name=="North East"
	replace location_name = "North West England" if location_name=="North West"
	replace location_name = "South East England" if location_name=="South East"
	replace location_name = "South West England" if location_name=="South West"
	replace location_name = "Yorkshire and the Humber" if location_name == "Yorkshire and The Humber"
	merge m:1 location_name using `loc_duplicates', keep (1 3)  nogen

// final cleanup
     keep ihme_loc_id location_id location_name source sex_id year_id nid isic_version isic_code natlrep exp_unadj
    
    save `uk_data', replace


    ** China subnational data - Census/1% registration data**
		** 1995  3-1**
		import excel using "`exp_dir'/CHINA_1995_3-1.xls", cellrange(A3:CY36) firstrow clear
	
		**rename columns to classify reported injuries to match ILo classification
		rename (C D F G) (eap_pop1 eap_pop2 eap_1 eap_2)
		rename (L M R S X Y AD AE AJ AK AV AW) (cat1_1 cat1_2 cat2_1 cat2_2 cat3_1 cat3_2 cat4_1 cat4_2 cat5_1 cat5_2 cat7_1 cat7_2) 
		rename (BB BC BH BI BT BU) (cat6_1 cat6_2 cat8_1 cat8_2 cat9_1 cat9_2)
		rename (AP AQ BN BO) (geo1 geo2 real1 real2) 
		rename (BZ CA CF CG CL CM CR CS CX CY) (health1 health2 edu1 edu2 sci1 sci2 govt1 govt2 other1 other2)
		keep Region eap* cat* geo* real* health* edu* sci* govt* other*
		replace Region = "China" if Region=="Total"
		drop if Region==""
		
		**destring variables 
		forvalues s = 1/2 {
			forvalues n = 1/9 {
				destring(cat`n'_`s'), replace
			}
		}
			
		forvalues s = 1/2 {
		foreach var in "eap_" "eap_pop" "geo" "real" "health" "edu" "sci" "govt" "other" {
			replace `var'`s' = "" if `var'`s'=="--"
			destring(`var'`s'), replace 
			replace `var'`s' = 0 if `var'`s'==. 
			}
		}
		
		** Classify in an ad-hoc way. Just create columns with sums we need.
		forvalues n = 1/2 {
			replace cat4_`n' = cat4_`n' + geo`n' /*combine water conservancy with electricity/water/gas*/
			replace cat8_`n' = cat8_`n' + real`n' /*combine finance, insurance, and real estate into a single category*/
			replace cat9_`n' = cat9_`n' + health`n' + edu`n' + sci`n' + govt`n' /*combine all social and community services into a single category*/
		}

		// shift to proportions
		forvalues n = 1/9 {
			replace cat`n'_1 = cat`n'_1/100
			replace cat`n'_2 = cat`n'_2/100
		}
		
		**drop extraneous vars
		keep Region cat*
		
		**standardize location_names
		rename Region location_name

		replace location_name = "China" if location_name=="Total"
		drop if location_name == ""
		merge m:1 location_name using `loc_duplicates', keep(3) keepusing(ihme_loc_id location_id super_region_id) nogen
	
	**reshape data
		reshape long cat, i(location_id) j(variable) string
		split variable, gen(var) parse(_)
		rename var1 isic_code
		rename var2 sex_id
		destring sex_id, replace
		rename cat exp_unadj
		gen isic_version = "ISIC2"

**generate other variables to standardize dataset
		gen year_id = 1995
		gen nid = 74290 
		gen source = "China 1% National Population Sample Survey"
		gen natlrep = 1 /*modeling subnational locations independently so okay to assume natlrep = 1 for this dataset*/
		

		**final cleanup
		keep exp_unadj location_id ihme_loc_id year_id sex_id isic_code isic_version natlrep source nid	
		tempfile chn_1995
		save `chn_1995', replace

		**2000 L4-1**
		import excel using "`exp_dir'/CHINA_2000_L4-1.xlsx", cellrange(A3:IS36) firstrow clear
		
		**limit dataset to necessary vars
		keep Region Total C D FarmingForestryAnimalHusbandr    F G MiningandQuarrying X Y Manufacturing AV AW	 ///
			ProductionandSupplyofElectri EK EL	Construction EW EX	GeologicalProspectingandWater FI FJ ///
			TransportStoragePostalandT FR FS	WholesaleRetailTradeandCat GV GW ///
			FinanceandInsurance HQ HR	RealEstate	HZ IA SocialServices IL IM	PublicServices	IO IP ResidentServices	IR IS	
			
		**standardize names of vars
		rename (C D F G X Y  AV AW EK EL EW EX FR FS GV GW HQ HR) ///
			(eap1 eap2 cat1_1 cat1_2 cat2_1 cat2_2 cat3_1 cat3_2 cat4_1 cat4_2 cat5_1 cat5_2 cat7_1 cat7_2 cat6_1 cat6_2 cat8_1 cat8_2)
		rename (FI FJ HZ IA IL IM IO IP IR IS) (geo1 geo2 real1 real2 social1 social2 public1 public2 personal1 personal2)
		
		drop if Region==""
		
		**standardize industry proportions
		foreach var in "eap" "geo" "real" "social" "public" "personal" {
			destring(`var'1), replace
			destring(`var'2), replace 
		}

		forvalues s = 1/2 {
			forvalues n = 1/8 {
				destring(cat`n'_`s'), replace
			}
		}
		
		**generate industry categories to match ILO classification
		forvalues s = 1/2 {
			replace cat4_`s' = cat4_`s' + geo`s'
			replace cat8_`s' = cat8_`s' + real`s'
			gen cat9_`s' = social`s' + public`s' + personal`s'
			}
			
		**clean up dataset
		keep Region eap* cat*
		
		**standardize location var
		rename Region location_name
		drop if location_name==""
		replace location_name = trim(strproper(location_name))
		replace location_name = "China" if location_name=="Total"
		replace location_name = "Tibet" if location_name == "Xizang"
		merge m:1 location_name using `loc_duplicates', keep(3) keepusing(ihme_loc_id location_id super_region_id) nogen
		
	**calculate proportions for each industry
		forvalues n = 1/9 {
			replace cat`n'_1 = cat`n'_1/eap1
			replace cat`n'_2 = cat`n'_2/eap2
		}

	**reshape data
		reshape long cat, i(location_id) j(variable) string
		split variable, gen(var) parse(_)
		rename var1 isic_code
		rename var2 sex_id
		destring sex_id, replace
		rename cat exp_unadj
		gen isic_version = "ISIC2"

		**generate other variables to standardize dataset
		gen year_id = 2000
		gen nid = 60470
		gen source = "Population Census Data Assembly"
		gen natlrep = 1 /*modeling subnational locations independently so okay to assume natlrep = 1 for this dataset*/

		**final cleanup
		keep exp_unadj location_id ihme_loc_id year_id sex_id isic_code isic_version natlrep source nid
		tempfile chn_2000
		save `chn_2000', replace

		**2005 5-1**
		import excel using "`exp_dir'/CHINA_2005_5-1.xlsx", cellrange(A4:V123) firstrow clear
		
		**standardize sex variable
		rename Sex location_name 
		gen sex_id = 2 if _n>=82
		replace sex_id = 1 if _n>=43 & _n<82
		drop if sex_id ==. 
		
		
		**standardize location name
		replace location_name = "China" if (location_name=="Female" | location_name=="Male")
		drop if location_name == ""
		replace location_name = "Heilongjiang" if location_name=="Heilongji"
		replace location_name = "Inner Mongolia" if location_name=="Inner Mon"
		merge m:1 location_name using `loc_duplicates',	keep(3) keepusing(ihme_loc_id location_id super_region_id) nogen	

**generate other variables to standardize dataset
		gen year_id = 2005
		gen nid = 2918
		gen source = "China 1% National Population Sample Survey"
		gen natlrep = 1 /*modeling subnational locations independently so okay to assume natlrep = 1 for this dataset*/
		
		**standardize industry vars
		replace InternationalOrganizations = 0 if InternationalOrganizations==. 
		
		**generate proportion employed in industry categories used by ILO classification
		gen cat1 = FarmingForestryAnimalHusbandr/Employedpopulation
		gen cat2 = MinjingandQuarrying/Employedpopulation
		gen cat3 = Manufacturing/Employedpopulation
		gen cat4 = ProductionandSupplyofElectri/Employedpopulation
		gen cat5 = Construction/Employedpopulation
		gen cat6 = (TransportStoragePostalandT + TelecommunicationComputerServi)/Employedpopulation
		gen cat7 = (WholesaleRetailTrade+HotelsandCateringServices)/Employedpopulation
		gen cat8 = (Finance + RealEstate + LeasingandBusiness)/Employedpopulation
		gen cat9 =  (ScientificResearchPolytechnic+ManagementofWaterConservancy+ServicestoHouseholdsandOther+Education+HealthSocialSecuritiesandSoc+CultureSportsandEntertainment+PublicManagementandSocialOrg+InternationalOrganizations)/Employedpopulation

**reshape data
		reshape long cat, i(sex_id location_id) j(variable) string
		split variable, gen(var) parse(_)
		rename variable isic_code
		rename cat exp_unadj
		gen isic_version = "ISIC2"

		**drop extraneous variables
		keep location_id location_name ihme_loc_id year_id sex_id nid natlrep source isic_code isic_version exp_unadj
		
		**append other china sources
		append using `chn_1995'
		append using `chn_2000'

		tempfile chn_data
		save `chn_data', replace 

// append all
	append using `ilo_data'
	append using `uk_data'

// add croswalk variables
	gen cw_isic2 = 0
	gen cw_isic4 = 0
	gen cw_sic2007 = 0
	replace cw_isic2 = 1 if isic_version == "ISIC2"
	replace cw_isic4 = 1 if isic_version == "ISIC4"
	replace cw_sic = 1 if isic_version == "SIC2007"
		replace isic_version = "ISIC4" if isic_version == "SIC2007" //major categories are functionally the same

// outliers
    gen outlier = 0
    replace outlier == 1 if ihme_loc_id == "GRL" & year_id == 2013 

   		//output for crosswalking and final prep for model
		export delimited using "`output_dir'/pre_cw_occ_ind.csv", nolabel replace 