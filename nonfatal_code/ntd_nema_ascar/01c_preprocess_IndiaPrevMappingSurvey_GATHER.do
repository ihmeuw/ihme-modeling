/*====================================================================
project:       GBD2016
Dependencies:  IHME
----------------------------------------------------------------------
Do-file version:   GBD2016 GATHER    
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
		local j "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local j "FILEPATH"
	}
	

* Directory Paths
	*gbd version (i.e. gbd2013)
	local gbd = "gbd2016"
	*model step
	local step 01c
	*local root
	local localRoot "FILEPATH"
	*cluster root
	local clusterRoot "FILEPATH"
	*directory for code
	local code_dir "FILEPATH"
	*directory for external inputs
	local in_dir "FILEPATH"
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
	log using "`log_dir'/01c_preprocess_IndiaPrevMappingSurvey_log_`date'_`time'.smcl", replace
	***********************	

	*print macros in log
	macro dir


/*====================================================================
                        1: Get GBD Info
====================================================================*/


*--------------------1.1: Location Metadata

	get_location_metadata,location_set_id(35) clear

	*Metadata for urban/rural
		preserve
			split path_to_top_parent,p(",")
			keep if path_to_top_parent4=="163"
			keep if most_detailed == 1
			save "`local_tmp_dir'/`step'_metasubnatsall.dta", replace
		restore

	*Metadata for states
		keep if parent_id==163
		save "`local_tmp_dir'/`step'_metastates.dta", replace
		
		levelsof location_id,local(indiastates) c
			*get population for state-level, not urban/rural to calculate cases

*--------------------1.1: Population

	get_population, location_id("`indiastates'") sex_id("3") age_group_id("22") year_id("2016") clear
		save "`local_tmp_dir'/`step'_getindiapop.dta", replace

/*====================================================================
                        2: Load and Clean Extracted Survey Data
====================================================================*/


*--------------------2.1: Import Data for Prevalence of 3 STH combined

	*Import
		import excel "FILEPATH/170601 India STH Prevalence Mapping Survey Summary_prevalence.xlsx", sheet("Slide9") firstrow clear
	
	*Format measure
		gen double prevalence=Prevalence/100
		drop Prev*

	*Format Variables
		replace State = subinstr(State," ", "", .) 
		split State,p("(")
		replace State=State1
		drop State1 State2 State3
		split State,p("*")
		replace State="Dadra&NagarHaveli" if State=="Dadar&NagarHaveli"
		replace State="WestBengal" if State=="Bengal*"
		replace State="HimachalPrUSER" if State=="Himachal"
		keep State prevalence
		rename prevalence all_sth_combined

	*Save
		save "`local_tmp_dir'/`step'_all_sth_by_state.dta", replace

*--------------------2.1: Import Data for 3 STH Individually by State in LF and Non-LF Areas

	*Import
		import excel "FILEPATH/170601 India STH Prevalence Mapping Survey Summary_prevalence.xlsx", sheet("Slide13to16") firstrow clear

	*Format Variables
		replace States = subinstr(States," ", "", .) 
		split States,p("*")
		replace States=States1
		drop States1
		rename States State
		replace Overall="NonLF" if Overall=="Non LF sites"
		replace Overall="LF" if Overall=="LF sites"
		replace Overall = subinstr(Overall," ", "", .) 
		gen double ascar_lt_=ALLight/100
		gen double trich_lt_=TTlight/100
		gen double hook_lt_=HWlight/100
		gen double all_sth_combined_=Prevalence/100
		drop ALLight TTlight HWlight Prevalence

		reshape wide all_sth_combined ascar_lt hook_lt trich_lt,i(SNo State ) j(Overall) s
		collapse (sum) all_sth_combined_LF ascar_lt_LF hook_lt_LF trich_lt_LF all_sth_combined_NonLF ascar_lt_NonLF hook_lt_NonLF trich_lt_NonLF,by( State)
		
	*Save
		save "`local_tmp_dir'/`step'_worm_worm_LF.dta", replace
		

*--------------------2.2: Merge Datasets
	
	*Merge and save
		merge 1:1 State using "`local_tmp_dir'/`step'_all_sth_by_state.dta", nogen

		save "`local_tmp_dir'/`step'_loadandcleanextractedsurvey", replace

/*====================================================================
                        3: Back Calculate Prevalence By State
====================================================================*/
		use "`local_tmp_dir'/`step'_loadandcleanextractedsurvey", clear


	*Calculate proportion of the population in LF areas and non LF areas
		gen PropPopLF=(all_sth_combined- all_sth_combined_NonLF)/( all_sth_combined_LF- all_sth_combined_NonLF)
		gen PropPopNonLF=1-PropPopLF
		
	*Use proportions to calculate the prevalence in the whole population by worm (not combined) as a weight average
		local meid 2999
		foreach worm in ascar hook trich {
			gen prev`meid'=( `worm'_lt_LF* PropPopLF)+(`worm'_lt_NonLF* PropPopNonLF)
			local ++meid
		}
		
	*Replace missing with average of the LF and non-Lf prevalence since that would result in denominator=0 and thus missing
		local meid 2999
		foreach worm in ascar hook trich {
			egen temp=rowmean(`worm'_lt_LF `worm'_lt_NonLF)
			replace prev`meid'=temp if prev`meid'==.
			drop temp
			local ++meid
		}

	*Save
		save "`local_tmp_dir'/`step'_prevalencebystate.dta", replace


/*====================================================================
                        4: Format India State Prevalence for GBD 2016
====================================================================*/

		use "`local_tmp_dir'/`step'_prevalencebystate.dta", clear

*--------------------4.1: Format Location Names and IDs

	*Keep only needed variables + format names to match GBD
		keep State prev*

		*local 6minorNAME "The Six Minor Territories"
		local 6minorNAME "Union Territories other than Delhi"

		replace State="`6minorNAME'" if State=="Andaman&Nicobar"
		replace State="Andhra PrUSER" if State=="AndhraPrUSER"
		replace State="Arunachal PrUSER" if State=="ArunachalPrUSER"
		replace State="`6minorNAME'" if State=="Chandigarh"
		replace State="`6minorNAME'" if State=="Dadra&NagarHaveli"
		replace State="`6minorNAME'" if State=="Daman"
		replace State="Himachal PrUSER" if State=="HimachalPrUSER"
		replace State="Jammu and Kashmir" if State=="Jammu&Kashmir"
		replace State="`6minorNAME'" if State=="Lakshadweep"
		replace State="Madhya PrUSER" if State=="MadhyaPrUSER"
		replace State="`6minorNAME'" if State=="Puducherry"
		replace State="Tamil Nadu" if State=="Tamilnadu"
		replace State="Uttar PrUSER" if State=="UttarPrUSER"
		replace State="West Bengal" if State=="WestBengal"
		rename State location_name
		
	*Get mean across six minor territories
		collapse (mean) prev*,by(location_name)
			
	*Format
		reshape long prev,i(location_name) j(helminth_type) s
		replace helminth_type="ascar" if helminth_type=="2999"
		replace helminth_type="trich" if helminth_type=="3001"
		replace helminth_type="hook" if helminth_type=="3000"
		
	*Get location_ids
		merge m:1 location_name using "`local_tmp_dir'/`step'_metastates.dta", nogen
		
	*SAVE
		keep location_id location_name helminth_type prev
		save "`local_tmp_dir'/`step'_formatindiaprev2016.dta", replace
		
	
/*====================================================================
                        5: Calculate Proportion of All India Cases in Each State (to be used for state splits/raking)
====================================================================*/

		use "`local_tmp_dir'/`step'_formatindiaprev2016.dta", clear

*--------------------5.1: Calculate Proportions
	
	*Get population to calculate cases
		merge m:1 location_id using "`local_tmp_dir'/`step'_getindiapop.dta", nogen
	
	*Calculate cases
		gen surveytotal_STATE = prev*population
		
	*Get cases totals by country
		bysort helminth_type: egen surveytotal_NATIONAL = total(STATE_surveytotal)
	
	*Proportion of cases by state
		gen surveyprop_STATE = surveytotal_STATE/surveytotal_NATIONAL


*--------------------5.2: Format for Raking
			
	*Expand 2 to get urban rural
		expand 2, gen(temp)
		gen subnat="Urban" if temp == 0
		replace subnat="Rural" if temp == 1
		
		egen location_nameX=concat(location_name subnat),p(", ")
		
		drop subnat temp
		rename location_name state_name
		rename location_nameX location_name
	
	*Re-get location_ids for urban/rural
		drop location_id
		merge m:1 location_name using "`local_tmp_dir'/`step'_metasubnatsall.dta", nogen keep(matched)
			*will drop the non urban/rural subnats from metadata
			
	*Keep only needed variables
		keep state_name location_name location_id helminth_type surveytotal_STATE surveytotal_NATIONAL surveyprop_STATE 
	
	*Save
		save "`in_dir'/`step'_indiaprevalencemappingsurvey.dta", replace
		




log close
exit
/* End of do-file */
