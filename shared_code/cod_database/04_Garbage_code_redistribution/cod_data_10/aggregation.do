** ****************************************************
** Purpose: Create cause heirarchy and apply adjustments for HIV and shocks
** ******************************************************************************************************************
 // Prep Stata
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
	
// Source
	global source "`1'"

// Date
	global timestamp "`2'"
	
// Username
	global username "`3'"
	
//  Local for the directory where all the COD data are stored
	local in_dir "$j/WORK/03_cod/01_database/03_datasets"
	global in_dir "$j/WORK/03_cod/01_database/03_datasets"
	global prog_dir "$j/WORK/03_cod/01_database/02_programs"

// Local for the output directory
	local out_dir "$j/WORK/03_cod/01_database/02_programs/compile"
	
// Make temp directory
	capture mkdir "/ihme/cod/prep/01_database/10_aggregation"
	local tmp_dir "/ihme/cod/prep/01_database/10_aggregation/${source}/"
	!rm -rf "`tmp_dir'"
	capture mkdir "`tmp_dir'"

// Log output
	capture log close _all
	log using "`in_dir'//$source//logs/10_aggregation_${timestamp}", replace

// Read in data (only 1980 and beyond)
	use "`in_dir'/$source/data/final/09_adjusted.dta" if year >= 1980, clear

// ********************************************************************************************************************************************************************
// ********************************************************************************************************************************************************************
//  IDENTIFY COUNTRY YEARS THAT CANNOT BE AGGREGATED TO A HIGHER LEVEL IN THE COD HIERARCHY DUE TO COMPOSITIONAL ISSUES
	// Get cause identifiers
		merge m:1 acause using "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", assert(2 3) keep(3) keepusing(cause_id) nogen
		drop acause
		
	// Special for ICD-based maps: aggregate completely without condition
		count if regexm(list, "ICD") & regexm(source_type, "VR")
		local agg_all = `r(N)'
		count if inlist(source, "UNODC_Homicides", "UN_CTS_Homicides") 
		local hom_non_agg = `r(N)'
		count if inlist(source, "Various_RTI", "GSRRS_Bloomberg_RTI")
		local rti_non_agg = `r(N)'
		
	// keep source/cause groups and reshape source wide
		tempfile master
		save `master', replace
		keep source source_label NID cause_id subdiv
		duplicates drop
		order source source_label subdiv NID cause_id
		sort source source_label subdiv NID cause_id
		drop if cause_id == . | source == "" | source_label == "" |  NID == .
		capture mkdir "`in_dir'/$source/maps/agg_maps"
		save "`in_dir'/$source/maps/agg_maps/identify_aggregation_causes_input.dta", replace
		save "`in_dir'/$source/maps/agg_maps/identify_aggregation_causes_input_${timestamp}.dta", replace
		egen source_group = group(source source_label subdiv NID), missing
		preserve
			keep source subdiv source_label NID source_group
			duplicates drop
			tempfile sources
			save `sources', replace
		restore
		drop source source_label NID subdiv
		gen _ = 1
		reshape wide _, i(cause_id) j(source_group)

	// merge on cause map and parse out levels of hierarchy
		merge 1:1 cause_id using "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", nogen keepusing(path_to_top_parent level cod_agg_other cod_agg_req secret_cause)
		** drop all_cause
		drop if level == 0
		** drop secret cause
		drop if secret_cause==1
		drop secret_cause
	// Special for ICD-based maps: aggregate completely without condition
		if `agg_all' > 0 {
		replace cod_agg_req = .
		}
	// There isn't aggregation criteria for acause level 0 causes, make sure that RTI  and Homicide only studies don't get aggregated to _inj
		if `hom_non_agg' > 0 {
		replace cod_agg_req = 1
		}
		if `rti_non_agg' {
		replace cod_agg_req = 1 if regexm(path_to_top_parent, "294")
		}
		
		** make cause parents for each level
		split path_to_top_parent, p(",")
		renpfix path_to_top_parent cause_parent
		destring cause_parent*, replace
		drop cause_parent cause_parent1
		forval x = 2/6 {
			local y = `x'-1
			capture rename cause_parent`x' cause_parent`y'
		}
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	// loop through sources and determine which causes can be aggregated
		quietly {
		foreach var of varlist _* {
			gen agg`var' = 0
			foreach i of numlist 4 3 2 1 {
				levelsof cause_id if level == `i', local(causes)
				foreach cause of local causes {
					// count child causes and skip if no child causes
					count if cause_parent`i' == `cause' & cause_id != `cause'
					if (r(N) <= 1) continue
					// check if causes are mapped to both the residual and the parent
					count if cause_id == `cause' & level == `i' & (`var' != 0 & `var' != .)
					if (r(N) > 0) {
						count if cause_parent`i' == `cause' & level == `i' + 1 & cod_agg_other == 1 & (`var' != 0 & `var' != .)
						if (r(N) > 0) {
							noisily di in red "+++ Causes mapped to both parent and residual --> $source, PARENT: `cause' +++"
							** end
						}
					}
					// count missing child causes and set parent aggregation indicator to 0 if required child cause is missing
					count if cause_parent`i' == `cause' & cause_id != `cause' & level == `i' + 1 & cod_agg_req == 1 & (`var' == 0 | `var' == .)
					if (r(N) > 0) replace `var' = 0 if cause_id == `cause' & (`var' == 0 | `var' == .)
					if (r(N) == 0) replace `var' = 2 if cause_id == `cause' & (`var' == 0 | `var' == .)
					// set can_be_aggregated variable for cause group
					count if cause_id == `cause' & (`var' == 1 | `var' == 2)
					if (r(N) > 0) replace agg`var' = 1 if cause_parent`i' == `cause' & cause_id != `cause' & level == `i' + 1
				}
			}
		}
		}
	// Make a single cause parent
		gen cause_parent = .
		foreach level in 5 4 3 2 {
			local j = `level'-1
			replace cause_parent = cause_parent`j' if level == `level'
		}
		drop cause_parent1 cause_parent2 cause_parent3 
	// save aggregation map
		keep cause_id level cause_parent _* *agg*
		reshape long _ agg_, i(cause_id) j(source_group)
		merge m:1 source_group using `sources', keep(3) assert(matched) nogen
		drop source_group
		rename agg_ can_be_aggregated
		rename _ cause_indicator
		order source source_label NID cause_id level cause_parent cause_indicator can_be_aggregated
		sort source source_label NID cause_id
		save "`in_dir'/$source/maps/agg_maps/identify_aggregation_causes_output.dta", replace
		save "`in_dir'/$source/maps/agg_maps/identify_aggregation_causes_output_${timestamp}.dta", replace
		drop cause_indicator
		tempfile agg
		save `agg', replace
	
	// aggregate causes from lower to higher level
		use `master', clear
				
		foreach i of numlist 5 4 3 2 {
			capture drop acause_level acause_parent can_be_aggregated
			merge m:1 source source_label subdiv NID cause_id using `agg', keep(1 3) nogen
			count if can_be_aggregated == 1 & level == `i'
			if (r(N) == 0) continue
			keep if can_be_aggregated == 1 & level == `i'
			collapse (sum) cf_final cf_corr cf_rd cf_raw, by(iso3 region year age sex source source_label source_type national list subdiv NID location_id sample_size cause_parent) fast
			rename cause_parent cause_id
			append using `master'
			tempfile master
			save `master', replace
		}
		
		collapse (sum) cf_final cf_corr cf_rd cf_raw, by(iso3 region year age sex source source_label source_type national list subdiv NID location_id sample_size cause_id) fast
		
		merge m:1 cause_id using "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", assert(2 3) keep(3) nogen keepusing(acause)
		drop cause_id
		
	// DHS subnational has very few deaths so deaths are too high/too low depending on demographic groups. Run subnational DHS at the national and subnational levels through the rest of this process so we can later rake the subnational to the national data. 
		tempfile data
		save `data', replace
	
		keep if source_type == "Sibling history, survey" & source == "Other_Maternal" & location_id != .
		count
		local sibs = `r(N)'
		if `sibs' > 0 {
			foreach var of varlist cf* {
				gen double deaths_`var' = `var' * sample_size
			}
			rename deaths_cf* deaths*
			collapse (sum) deaths* sample_size, by(iso3 year age NID list national region sex source source_label source_type acause) fast
			foreach var of varlist deaths* {
				gen double cf_`var' = `var'/sample_size
			}
			rename cf_deaths* cf*
			gen location_id = .
			** Make identifiers for noise reduction and subnational aggregation used to later drop this data
			**replace iso3 = iso3 + "_national"
			replace source = "subnat_agg"
			tempfile dhs_nat
			save `dhs_nat', replace
		}
		use `data', clear
		** Append the national data
		if `sibs' > 0 {
			append using `dhs_nat'
		}		
	
	// Make maternal_hiv datapoints
		do "$j/WORK/03_cod/01_database/02_programs/compile/code/hiv_maternal_paf_adjustment.do"
		
	// Square data before smoothing
		count if (regexm(source_type, "VR") | regexm(lower(source_type), "vital")) & source == "$source"
		local VR = `r(N)'
		count if source == "$source"
		if (`VR'/`r(N)'==1) | inlist("$source", "Mexico_BIRMM", "SUSENAS", "China_MMS_1996_2005", "China_MMS_2006_2012", "China_Child_1996_2012", "China_1991_2002") | strmatch("$source", "ICD*") {
			// Assign groups and submit jobs
				replace location_id = 0 if location_id == .
				replace subdiv = "/NA" if subdiv == ""
				egen geo_group = group(iso3 list national source source_type source_label location_id subdiv NID)
				save "`tmp_dir'/all_geo_groups.dta", replace
				levelsof geo_group, local(ggs)
				foreach gg of local ggs {
					!qsub -P proj_codprep -pe multi_slot 2 -l mem_free=4g -N "CoD_10s_${source}_`gg'" "$prog_dir/prep/code/shellstata13_${username}.sh" "`out_dir'/code/square_data.do" "$source $username `gg'"
					!qsub -P proj_codprep -pe multi_slot 2 -l mem_free=4g -N "CoD_10s_${source}_`gg'_check" -hold_jid  "CoD_10s_${source}_`gg'" "$prog_dir/prep/code/shellstata13_${username}.sh" "`out_dir'/code/square_data_check.do" "$source $username `gg'"
					sleep 1000
				}
			// Check for file
				clear
				foreach gg of local ggs {
					capture confirm file "`tmp_dir'/geo_group_`gg'_squared.dta"
					if _rc == 0 {
						display "FOUND!"
					}
					while _rc != 0 {
					display "Group `gg' not found, checking again in 30 seconds"
						sleep 30000
						capture confirm file "`tmp_dir'/geo_group_`gg'_squared.dta"
						if _rc == 0 {
							display "FOUND!"
						}
					}
					display "Appending `gg'"
					capture append using "`tmp_dir'/geo_group_`gg'_squared.dta"
					if _rc != 0 {
						sleep 15000
						append using "`tmp_dir'/geo_group_`gg'_squared.dta"
					}
				}
			// De-group
				drop geo_group
				replace location_id = . if location_id == 0
				replace subdiv = "" if subdiv == "/NA"
		}
		
	// In the recode step for BTL some cancer deaths were moved to the cancer parent. The squaring step created 0's.
	if "$source" == "ICD9_BTL" {
		tempfile btl
		save `btl', replace
		// Load country-years by cause to apply adjustments
		import excel using "$j/WORK/03_cod/01_database/02_programs/compile/data/btl_cancer_recodes_list.xlsx", firstrow clear
		tempfile recode_list
		save `recode_list', replace
		// Drop the 0's in these locations 
		use `btl', clear
		merge m:1 iso3 year acause  using `recode_list', keep(1) nogen
	}
	
	// make hiv free cause fractions 
	if "$source" != "Cancer_Registry" & !regexm(source_type, "VR") & !regexm(source_type, "VA") do /home/j/WORK/03_cod/01_database/02_programs/compile/code/hiv_free_maternal.do
	if "$source" != "Cancer_Registry" do /home/j/WORK/03_cod/01_database/02_programs/compile/code/make_hiv_shock_free_cfs.do
	** Make identifiers for noise reduction and subnational aggregation used to later drop this data
	replace iso3 = iso3 + "_national" if source == "subnat_agg"
	
	
	// save before noise reduction
		compress
		save "`in_dir'/$source/data/final/10_aggregated.dta", replace
		save "`in_dir'/$source/data/final/_archive/10_aggregated_$timestamp.dta", replace
		
