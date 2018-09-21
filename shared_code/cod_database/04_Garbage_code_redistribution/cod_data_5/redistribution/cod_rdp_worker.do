// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose: Formats for redistribution, runs redistribution, and then reformats it back to CoD format

** **************************************************************************
** ANALYSIS CONFIGURATION
** **************************************************************************

	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application preferences and defines the local variables.  
	** 	The local applications preferences include memory allocation, variables 
	**	limits, color scheme, and defining the J drive (data).  Local variables include
	**	the file pathway for data inputs and outputs, and any GBD-related data.
	**
	** ****************************************************************

		// Set application preferences
			// Clear memory and set memory and variable limits
				clear all
				set mem 10G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color
			
			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "/home/j"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "J:"
				}
			// Set up PDF maker
				do "$prefix/Usable/Tools/ADO/pdfmaker_Acrobat10.do"
			

	** ****************************************************************
	** DEFINE LOCALS
	** ****************************************************************
		local source `1'
		local code_version `2'
		local split_group `3'
		
		// Code folder (where you want the redistribution script to appear)
			local code_folder "$prefix/WORK/03_cod/01_database/03_datasets/`source'/code"
		// Name of data set
			local data_set_name "`source'"
		// Temporary folder
			capture mkdir "/ihme/cod/prep"
			capture mkdir "/ihme/cod/prep/01_database"
			capture mkdir "/ihme/cod/prep/01_database/05_rdp"
			capture mkdir "/ihme/cod/prep/01_database/05_rdp/`source'"
			capture mkdir "/ihme/cod/prep/01_database/05_rdp/`source'/_input_data"
			capture mkdir "/ihme/cod/prep/01_database/05_rdp/`source'/_logs"
			capture mkdir "/ihme/cod/prep/01_database/05_rdp/`source'/_temp"
			capture mkdir "/ihme/cod/prep/01_database/05_rdp/`source'/split_`split_group'"
			capture mkdir "/ihme/cod/prep/01_database/05_rdp/`source'/split_`split_group'/_logs"
			capture mkdir "/ihme/cod/prep/01_database/05_rdp/`source'/split_`split_group'/intermediate"
			capture mkdir "/ihme/cod/prep/01_database/05_rdp/`source'/split_`split_group'/final"
			local temp_folder "/ihme/cod/prep/01_database/05_rdp/`source'/split_`split_group'"
		// Input folder
			local input_folder "/ihme/cod/prep/01_database/05_rdp/`source'/_input_data"
		// Log
			capture log close
			log using "/ihme/cod/prep/01_database/05_rdp/`source'/split_`split_group'/_logs/split_`split_group'.log", text replace
			
		display "`source' `code_version' `split_group'"	
			
	** ****************************************************************
	** GET RESOURCES
	** ****************************************************************
		// Get location hierarchy
			// Get data
				use "/ihme/cod/prep/01_database/05_rdp/`source'/_temp/location_hierarchy.dta", clear
			// Save
				tempfile location_hierarchy
				save `location_hierarchy', replace
			
		// Get codesystem IDs which are used in the Engine Room to denote source-source label combinations
			// Get data
				use "$prefix/WORK/00_dimensions/03_causes/temp/packagesets_`code_version'.dta", clear
			// Figure out if this contains source labels
				count
				if `r(N)' == 0 {
					display in red "THERE AREN'T ANY CODE SYSTEMS IN $prefix/WORK/00_dimensions/03_causes/temp/packagesets_`code_version'.dta"
					display in red "Check the MASTER_CAUSE_MAP.PY file to make sure things are working"
					BREAK
				}
				else if `r(N)' > 1 {
					local source_label_tag = 1
				}
				else {
					local source_label_tag = 0
				}
			// Save
				tempfile packagesets_ids
				save `packagesets_ids', replace
				
				
** ****************************************************************
** RUN PROGRAM
** ****************************************************************
	// Get data
		insheet using "`input_folder'/split_`split_group'.csv", comma names case double clear
	// Reformat string variables to remove leading apostrophe
		foreach var of varlist * {
			capture replace `var' = subinstr(`var',"'","",1)
		}
		format deaths* %16.0g
	// Collapse data down
		collapse (sum) deaths*, by(location_id subdiv national source source_label source_type NID list sex year frmat im_frmat split_group cause) fast
	// Save a before so we can merge things on later
		// Rename
			foreach i of numlist 1/26 91/94 {
				capture gen deaths`i' = 0
				rename deaths`i' orig_deaths`i'
			}
		// Save
			tempfile before_data
			save `before_data', replace
		// Rename back
			foreach i of numlist 1/26 91/94 {
				rename orig_deaths`i' deaths`i'
			}
	// Reshape age wide
		egen uid = group(location_id subdiv national source source_label source_type NID list sex year frmat im_frmat cause), missing
		reshape long deaths, i(uid) j(gbd_age)
		drop uid
	// Drop causes that have 0 deaths
		drop if deaths == 0
	// Convert GBD age to age groups
		gen age = .
		replace age = 0 if gbd_age == 91
		replace age = 0.01 if gbd_age == 93
		replace age = 0.1 if gbd_age == 94
		replace age = 1 if gbd_age == 3
		replace age = (gbd_age - 6) * 5 if (gbd_age - 6) * 5 >= 5 & (gbd_age - 6) * 5 <= 80
		drop if age == .
		drop gbd_age
		tostring(age), replace format("%12.2f") force
		destring(age), replace
	
	// Merge on location hierarchy metadata
		merge m:1 location_id using `location_hierarchy', keep(1 3) keepusing(location_id ihme_loc_id global dev_status super_region region country subnational_level1 subnational_level2) assert(2 3) nogen
		drop ihme_loc_id
	
	// Rename deaths
		rename deaths freq
		
	// Save intermediate file for RDP
		saveold "`temp_folder'/intermediate/for_rdp.dta", replace
		
	// Prep resources for redistribution (and afterwards)
		// Get source label if needed
			if `source_label_tag' == 1 {
				levelsof(source_label), local(source_label) clean
			}
		// Get cause map
			use "$prefix/WORK/00_dimensions/03_causes/temp/map_`code_version'.dta", clear
			if `source_label_tag' == 1 {
				keep if source_label == "`source_label'"
			}
			keep cause_code cause_name yll_cause
			rename cause_code cause
			rename yll_cause acause
			tempfile cause_map
			save `cause_map', replace
		// Get packagesets_ids
			use `packagesets_ids', clear
			if `source_label_tag' == 1 {
				keep if source_label == "`source_label'"
			}
			count
			if `r(N)' == 0 {
				display in red "THERE AREN'T ANY SOURCE LABELS TAGGED `source_label' IN $prefix/WORK/00_dimensions/03_causes/temp/packagesets_`code_version'.dta"
				display in red "Failing redistribution now... bye-bye"
				BREAK
			}
			else if `r(N)' > 1 {
				display in red "THERE TOO MANY CODE SYSTEMS IN $prefix/WORK/00_dimensions/03_causes/temp/packagesets_`code_version'.dta `source_label'"
				display in red "Failing redistribution now... bye-bye"
				BREAK
			}
			levelsof(package_set_id), local(package_set_id) clean
	
	// Run redistribution (no magic tables for US counties or below)
		local magic_table = 1
		!python "$prefix/WORK/03_cod/01_database/02_programs/redistribution/code/redistribution.py" "`source'" `package_set_id' `split_group' `magic_table'
	
	// Get redistributed data
		use "`temp_folder'/final/post_rdp.dta", clear
		compress
	// Replace lingering ZZZ with CC code
		replace cause = "cc_code" if cause == "ZZZ"
	// Make sure everything is collapsed down
		collapse (sum) freq, by(location_id subdiv national year sex age source source_label source_type NID list frmat im_frmat split_group cause)
	// Rename deaths
		rename freq deaths
	// Convert age groups to GBD age
		tostring(age), replace format("%12.2f") force
		destring(age), replace
		gen gbd_age = .
		replace gbd_age = (age/5) + 6 if age * 5 >= 5 & age <= 80
		replace gbd_age = 91 if age == 0
		replace gbd_age = 93 if age == 0.01
		replace gbd_age = 94 if age == 0.1
		replace gbd_age = 3 if age == 1
		drop age
	// Reshape age wide
		egen uid = group(location_id subdiv national source source_label source_type NID list sex year frmat im_frmat cause), missing
		reshape wide deaths, i(uid) j(gbd_age)
		drop uid
	// Merge on original data source
		merge 1:1 location_id subdiv national source source_label source_type NID list sex year frmat im_frmat split_group cause using `before_data'
		rename _merge beforeafter
	// Reformat death variables
		// Make sure all death variables exist
			foreach i of numlist 3/26 91/94 {
				capture gen deaths`i' = 0
				replace deaths`i' = 0 if deaths`i' == .
			}
		// Recalculate aggregates
			aorder
			egen double deaths2 = rowtotal(deaths91-deaths94)
			egen double deaths1 = rowtotal(deaths3-deaths94)
	// Reformat hierarchies
		merge m:1 location_id using `location_hierarchy', keep(1 3) assert(2 3) keepusing(dev_status region_id ihme_loc_id) nogen
		rename region_id region
		drop location_id
		split ihme_loc_id, p("_")
		rename ihme_loc_id1 iso3
		capture rename ihme_loc_id2 location_id
		capture destring(location_id), replace
		capture gen location_id = .
		drop ihme_loc_id
	// Merge on acause
		merge m:1 cause using `cause_map', keep(1 3) keepusing(acause)
		count if _merge == 1 & cause != "cc_code"
		if `r(N)' > 0 {
			display in red "The following causes are not in the cause map:"
			tab cause if _merge != 3 & cause != "cc_code"
			BREAK
		}
		replace acause = "cc_code" if cause == "cc_code"
	// Fill in 0's where needed
		foreach var of varlist deaths* orig_deaths* {
			replace `var' = 0 if `var' == .
		}
	// Final format
		aorder
		keep dev_status region iso3 location_id national subdiv source source_label source_type NID list year sex im_frmat frmat cause acause deaths* orig_deaths* beforeafter
		order dev_status region iso3 location_id national subdiv source source_label source_type NID list year sex im_frmat frmat cause acause deaths* orig_deaths* beforeafter
	// Save
		compress
		save "`temp_folder'/final/redistributed_split_`split_group'.dta", replace
		
	// Get max before after for 
		egen temp = max(beforeafter), by(dev_status region iso3 location_id national subdiv source source_label source_type NID list year sex im_frmat frmat acause)
		replace beforeafter = temp
	
	// Collapse to acause level
		collapse(sum) *deaths*, by(dev_status region iso3 location_id national subdiv source source_label source_type NID list year sex im_frmat frmat acause beforeafter) fast
		gen split_group = `split_group'
		
	// Save
		compress
		save "`temp_folder'/final/redistributed_split_`split_group'_collapsed.dta", replace	
		
	// Write text file for completion
		file open finish using "`temp_folder'/final/redistributed_split_`split_group'_complete.txt", write replace
		file write finish "Done!" _n
		file close finish
		
		
	// Produce magic table
	if `magic_table' == 1 {
		// Make a list of proportion locations
			// Get full location hierarchy
				use `location_hierarchy', clear
			// Keep only at the level we are generating proportion_ids
				keep if (country != "" & subnational_level1 == "" & subnational_level2 == "") | (subnational_level1 != "" & subnational_level2 == "")
			// Keep only what we need
				keep location_id country subnational_level1
			// Save
				tempfile proportion_locations
				save `proportion_locations', replace
				
		// Get proportion ID metadata
			use "`temp_folder'/final/proportion_metadata.dta", clear
			keep source source_label source_type NID list country subnational_level1 national subdiv year sex age frmat im_frmat proportion_id split_group
			tempfile proportion_metadata
			save `proportion_metadata', replace
		
		// Get magic table data
			// Get data
				use "`temp_folder'/final/magic_table.dta", clear
			// Bump up sequence numbers
				replace seq = seq + 1
			// Merge on causes
				merge m:1 cause using `cause_map', keep(1 3) keepusing(acause) assert(2 3) nogen
			// Collapse to acause
				collapse (sum) freq, by(proportion_id seq package garbage acause) fast
			// Save
				compress
				tempfile magic_table
				save `magic_table', replace
			
		// Get original data
			// Get data
				use "`temp_folder'/intermediate/for_rdp.dta", clear
			// Merge on location hierarchy metadata
				merge m:1 location_id using `location_hierarchy', keep(1 3) keepusing(country subnational_level1) assert(2 3) nogen
				drop location_id
			// Merge on proportion IDs
				foreach var of varlist country subnational_level1 subdiv source source_label source_type list {
					capture tostring(`var'), replace
				}
				merge m:1 source source_label source_type NID list country subnational_level1 national subdiv year sex age frmat im_frmat split_group using `proportion_metadata', keep(1 3) assert(2 3) keepusing(proportion_id) nogen
			// Collapse down to acause
				merge m:1 cause using `cause_map', keep(1 3) keepusing(acause) assert(2 3) nogen
				collapse (sum) freq, by(proportion_id acause) fast
			// Prepare to append into original table
				gen seq = 0
				gen garbage = 0
				gen package = "Original"
				
		// Append original and magic table data together 
			append using `magic_table'
			
		// Merge on proportion ID metadata
			merge m:1 proportion_id using `proportion_metadata', keep(1 3) assert(2 3) nogen
			drop proportion_id
			
		// Merge on location_ids
			merge m:1 country subnational_level1 using `proportion_locations', keep(1 3) assert(2 3) nogen
			drop country subnational_level1
			
		// Generate metrics of interest
			// Make all ages
				preserve
					replace age = 99
					collapse (sum) freq, by(location_id subdiv national year sex age source source_label source_type NID list frmat im_frmat split_group acause seq garbage package) fast
					tempfile all_age_data
					save `all_age_data', replace
				restore
				append using `all_age_data'
			// Make location aggregates
				// Merge on hierarchy
					merge m:1 location_id using `location_hierarchy', keep(1 3) keepusing(global super_region region country subnational_level1 subnational_level2) assert(2 3) nogen
					drop location_id
				// Make geographic level				
					gen geo_level = ""
					foreach geo_level in global super_region region country subnational_level1 {
						replace geo_level = "`geo_level'" if `geo_level' != ""
					}
				// Aggregate
					foreach geo_level in subnational_level1 country region super_region {
						count if geo_level == "`geo_level'"
						if `r(N)' > 1 {
							display "Aggregating `geo_level' up"
							preserve
								keep if geo_level == "`geo_level'"
								replace `geo_level' = ""
								collapse (sum) freq, by(global super_region region country subnational_level1 subnational_level2 subdiv national year sex age source source_label source_type NID list frmat im_frmat split_group acause seq garbage package) fast
								gen geo_level = ""
								replace geo_level = "country" if "`geo_level'" == "subnational_level1"
								replace geo_level = "region" if "`geo_level'" == "country"
								replace geo_level = "super_region" if "`geo_level'" == "region"
								replace geo_level = "global" if "`geo_level'" == "super_region"
								tempfile aggregated_geography_data
								save `aggregated_geography_data', replace
							restore
							append using `aggregated_geography_data'
						}
					}
				// Merge location_ids back on
					merge m:1 global super_region region country subnational_level1 subnational_level2 using `location_hierarchy', keep(1 3) keepusing(location_id) assert(2 3) nogen
					drop global super_region region country subnational_level1 subnational_level2

		// Reformat
			order split_group geo_level source source_label source_type NID list location_id national subdiv year sex age frmat im_frmat seq package garbage acause freq
			gsort split_group geo_level location_id subdiv year sex age frmat im_frmat source source_label source_type NID list national seq -garbage -freq acause
			
		// Save
			compress
			save "`temp_folder'/final/magic_table_`split_group'_collapsed.dta", replace	
			
		// Write text file for completion
			file open finish using "`temp_folder'/final/magic_table_`split_group'_complete.txt", write replace
			file write finish "Done!" _n
			file close finish
	}
	
	capture log close
	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
