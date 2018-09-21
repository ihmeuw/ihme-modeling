// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose: File that submits redistribution jobs to cluster

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
		local username "`1'"
		local source "`2'"
		local code_version "`3'"
		local timestamp "`4'"
		
		local location_set_version_id 38
		
		display "`username' `source' `code_version' `timestamp'"
		
		// Log
			capture log using "$prefix/WORK/03_cod/01_database/03_datasets/`source'/logs/05_rdp_`source'_`timestamp'.txt", text replace
		// Code folder (where you want the redistribution script to appear)
			local code_folder "$prefix/WORK/03_cod/01_database/03_datasets/`source'/code"
		// Input file
			local input_file "$prefix/WORK/03_cod/01_database/03_datasets/`source'/data/intermediate/04_before_redistribution.dta"
		// Output folder
			local output_folder "$prefix/WORK/03_cod/01_database/03_datasets/`source'/data/final/"
			capture mkdir "`output_folder'"
			capture mkdir "`output_folder'/_archive"
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
			local temp_folder "/ihme/cod/prep/01_database/05_rdp/`source'"
			
			
	** ****************************************************************
	** GET RESOURCES
	** ****************************************************************
		// Pull location hierarchy
			// Get data from hierarchy
				odbc load, exec("SELECT * FROM shared.location_hierarchy_history WHERE location_set_version_id = `location_set_version_id'") strConnection clear
			// Keep only what we need
				keep location_id ihme_loc_id path_to_top_parent is_estimate most_detailed location_name developed
			// Fix India urban path to parrent
				replace path_to_top_parent = "1,158,159,163,44538,44540" if location_id == 44540
			// Save temp file
			preserve
				odbc load, exec("SELECT location_id, location_ascii_name FROM shared.location;") strConnection clear
				rename location_ascii_name parent_name
				rename location_id parent_id
				tempfile location_hierarchy
				save `location_hierarchy', replace
			restore
			// Split path to top parent
				split path_to_top_parent, p(",")
			// Rename path to parent splits
				local i = 1
				foreach t in global super_region region country subnational_level1 subnational_level2 subnational_level3 {
					rename path_to_top_parent`i' parent_id
					destring(parent_id), replace
					merge m:1 parent_id using `location_hierarchy', keep(1 3) keepusing(parent_name) nogen
					rename parent_id `t'
					rename parent_name `t'_name
					** drop `t'_name
					local i = `i' + 1
				}
			// Rename development status
				replace developed = "0" if inlist(country_name, "China", "India", "Kenya", "Saudi Arabia")
				gen dev_status = "D" + developed
			// Do some manual corrections
				// England
					gen tag_england = 1 if subnational_level1 == 4749 & location_id != 4749
					replace subnational_level1 = subnational_level2 if tag_england == 1
					replace subnational_level1_name = subnational_level2_name if tag_england == 1
					replace subnational_level2 = subnational_level3 if tag_england == 1
					replace subnational_level2_name = subnational_level3_name if tag_england == 1
					replace subnational_level3 = . if tag_england == 1
					replace subnational_level3_name = "" if tag_england == 1
					drop tag_england
				// China (without Hong Kong and Macao)
					gen tag_chn_wo_hkmc = 1 if subnational_level1 == 44533 & location_id != 44533
					replace subnational_level1 = subnational_level2 if tag_chn_wo_hkmc == 1
					replace subnational_level1_name = subnational_level2_name if tag_chn_wo_hkmc == 1
					replace subnational_level2 = subnational_level3 if tag_chn_wo_hkmc == 1
					replace subnational_level2_name = subnational_level3_name if tag_chn_wo_hkmc == 1
					replace subnational_level3 = . if tag_chn_wo_hkmc == 1
					replace subnational_level3_name = "" if tag_chn_wo_hkmc == 1
					drop tag_chn_wo_hkmc
				// Append in US county list from US county team
					drop if country_name == "United States" & subnational_level2_name != ""
					tempfile location_hierarchy
					save `location_hierarchy', replace
					insheet using "$prefix/WORK/03_cod/01_database/02_programs/redistribution/code/us_counties.csv", comma names clear
					rename state_location_id location_id
					rename cnty_location_id subnational_level2
					rename cnty_name subnational_level2_name
					merge m:1 location_id using `location_hierarchy', keep(1 3) keepusing(ihme_loc_id global dev_status super_region region country subnational_level1 global_name super_region_name region_name country_name subnational_level1_name)
					replace location_id = subnational_level2
					replace ihme_loc_id = "USA_"+string(location_id)
					keep location_id ihme_loc_id global* dev_status super_region* region* country* subnational_*
					append using `location_hierarchy'
				// Append in King county list from US county team
				quietly {
					tempfile location_hierarchy
					save `location_hierarchy', replace
					foreach c in 43944 43945 43946 43948 43949 43950 43951 43952 43953 43954 43955 43956 43957 43958 43959 43960 43961 43963 43964 43965 43966 43967 43968 43969 43971 43972 43973 43974 43975 43976 43977 43978 43979 43980 43981 43982 43983 43985 43986 43987 43989 43990 43992 43993 43994 43995 43996 43997 43998 43999 44000 44001 44002 44003 44004 44005 44007 44008 44009 44010 44011 44012 44013 44014 44015 44016 44017 44018 44019 44021 44023 44024 44025 44026 44027 44029 44030 44031 44032 44033 44034 44035 44037 44038 44039 44040 44041 44042 44043 44044 44045 44046 44047 44048 44049 44050 44051 44053 44054 44055 44057 44058 44059 44060 44062 44063 44064 44065 44066 44068 44069 44070 44071 44073 44074 44075 44076 44078 44079 44081 44082 44083 44084 44086 44087 44088 44089 44090 44091 44092 44093 44094 44095 44096 44097 44099 44100 44101 44102 44103 44104 44105 44106 44107 44109 44110 44111 44112 44113 44115 44116 44117 44119 44120 44121 44122 44123 44124 44126 44127 44129 44130 44131 44132 44133 44135 44137 44138 44141 44142 44143 44144 44146 44147 44148 44149 44150 44151 44153 44154 44155 44156 44158 44159 44160 44161 44163 44164 44165 44166 44168 44169 44170 44171 44173 44174 44175 44176 44177 44178 44179 44180 44181 44183 44184 44185 44186 44188 44189 44190 44191 44192 44194 44196 44197 44200 44201 44202 44204 44205 44206 44207 44209 44210 44212 44213 44216 44217 44218 44219 44221 44222 44223 44224 44225 44226 44227 44228 44229 44231 44232 44234 44235 44236 44237 44238 44239 44240 44241 44242 44243 44244 44245 44246 44247 44248 44250 44251 44252 44253 44254 44255 44256 44258 44259 44260 44262 44263 44265 44266 44269 44270 44271 44272 44275 44276 44277 44278 44279 44281 44283 44284 44285 44286 44288 44289 44290 44292 44293 44294 44295 44296 44298 44299 44302 44303 44304 44305 44306 44307 44308 44311 44312 44313 44315 44316 44317 44318 44319 44320 44321 44322 44324 44325 44326 44328 44329 44330 44331 44333 44334 44336 44337 44338 44339 44340 44342 44343 44344 44346 44347 44348 44350 44351 44353 44355 44356 44357 44360 44361 44362 44363 44364 44365 44366 44368 44369 44370 44371 44372 44373 44375 44376 44377 44378 44380 44381 44383 44384 44385 44386 44390 44391 44393 44394 44395 44396 44397 44398 44403 44405 44407 44409 44411 44412 44413 44414 44415 44416 44417 44418 44419 44420 44421 44422 44423 44424 44425 44427 44428 44429 44431 44432 44434 44435 44436 44437 {
						use `location_hierarchy', clear
						keep if location_id == 3543
						replace location_id = `c'
						replace ihme_loc_id = "USA_`c'"
						replace subnational_level2 = `c'
						replace subnational_level2_name = "King County - `c'"
						append using `location_hierarchy'
						tempfile location_hierarchy
						save `location_hierarchy', replace
					}
				}
			// Reformat
				foreach var of varlist global super_region region country subnational_level1 subnational_level2 {
					rename `var' `var'_id
					rename `var'_name `var'
					replace `var' = subinstr(`var', "'", " ", .)
				}
				keep location_id ihme_loc_id global global_id dev_status super_region super_region_id region region_id country country_id subnational_level1 subnational_level1_id subnational_level2 subnational_level2_id
				order location_id ihme_loc_id global global_id dev_status super_region super_region_id region region_id country country_id subnational_level1 subnational_level1_id subnational_level2 subnational_level2_id
				sort global super_region region country subnational_level1 subnational_level2
			// Save
				tempfile location_hierarchy
				save `location_hierarchy', replace
				save "`temp_folder'/_temp/location_hierarchy.dta", replace
				
		// Check that all of the redistribution packages for this source are here & copy resources
			// Get data
				use "$prefix/WORK/00_dimensions/03_causes/temp/packagesets_`code_version'.dta", clear
				copy "$prefix/WORK/00_dimensions/03_causes/temp/packagesets_`code_version'.dta" "/ihme/cod/prep/01_database/05_rdp/`source'/_temp/packagesets_`code_version'.dta", public replace
			// Check to make sure packages exist and if they do, copy them to a temp folder on clustertmp
				levelsof(package_set_id), local(package_set_ids) clean
				foreach package_set_id of local package_set_ids {
					local checkfile = "/ihme/cod/prep/01_database/02_programs/redistribution/rdp/`package_set_id'/cause_map.csv"
					display "Checking for formatted redistribution package for package_set_id `package_set_id'"
					display "/ihme/cod/prep/01_database/02_programs/redistribution/rdp/`package_set_id'"
					local c = 0
					capture confirm file "`checkfile'"
					while _rc {
						sleep 15000
						capture confirm file "`checkfile'"
						if `c' >= 60 {
							display in red ""
							display in red "BREAKING REDISTRIBUTION"
							display in red "Redistribution package for package_set_id `package_set_id' not found after 15 minutes"
							display in red "This may be because the redistribution package took too long to generate or because there was a problem generating the packages"
							BREAK
						}
						local c = `c' + 1
					}
					capture mkdir "`temp_folder'/_temp/`package_set_id'"
					display "Found!  Copying over..."
					local file_set: dir "/ihme/cod/prep/01_database/02_programs/redistribution/rdp/`package_set_id'/" files "*", respectcase
					foreach f of local file_set {
						display "Copying `f'"
						copy "/ihme/cod/prep/01_database/02_programs/redistribution/rdp/`package_set_id'/`f'" "`temp_folder'/_temp/`package_set_id'/`f'", replace public
					}
					display "Done!"
				}
			// Save
				tempfile packageset_ids
				save `packageset_ids', replace		
		
				
				
** ****************************************************************
** RUN PROGRAM
** ****************************************************************
	// Get data
		use "`input_file'", clear
	// Determine if there are any garbage codes
		count if acause=="_gc"
	// If there is, proceed running redistribution, if not, just resave the data set
	if `r(N)' > 0 {
		// Location manipulation
		** We want to make sure that we have explicit location information within our data set (global, super_region, region, country, subnational_level1, subnational_level2)
			// Reformat country identifiers
				gen ihme_loc_id = iso3
				replace ihme_loc_id = ihme_loc_id + "_" + string(location_id) if location_id != .
				drop location_id region dev_status iso3
			// Merge on location hierarchy
				capture merge m:1 ihme_loc_id using `location_hierarchy', keep(1 3) assert(2 3) keepusing(location_id ihme_loc_id global dev_status super_region region country subnational_level1 subnational_level2)
				if _rc!=0 {
					tab ihme_loc_id	if _merge==1
					BREAK
				}
				else {
					drop _merge
				}
				
		// Generate groups to split
			if inlist("`source'", "_US_king_county_ICD10", "_US_king_county_ICD9") | regexm("`source'", "US_NCHS_") {
				egen split_group = group(country subnational_level1 subdiv national source source_label source_type NID list year frmat im_frmat sex), missing
			}
			else {
				egen split_group = group(country subnational_level1 subdiv national source source_label source_type NID list year frmat im_frmat), missing
			}
			
		// Save a temporary copy
			tempfile split_group_data
			save `split_group_data', replace
			
		// Reformat
			// Drop acause (don't need it since redistribution operates at the code level
				** drop acause
			// Drop location hierarchy info to save space (will keep location_id so that we can merge the hierarchy info back on in the worker jobs)
				 drop ihme_loc_id global dev_status super_region region country subnational_level1 subnational_level2
			// Add apostrophe to all string variables
				foreach var of varlist * {
					capture replace `var' = "'" + `var'
				}
				
		// Split groups and submit jobs
		quietly {
			format deaths* %16.0g
			summ split_group
			local split_min = `r(min)'
			local split_max = `r(max)'
			forvalues sg = `split_min'/`split_max' {
				noisily display "Submitting split `sg' (of `split_max')"
				// Remove old file
					capture rm "`temp_folder'/split_`sg'/final/redistributed_split_`sg'.dta"
					capture rm "`temp_folder'/split_`sg'/final/redistributed_split_`sg'_collapsed.dta"
					capture rm "`temp_folder'/split_`sg'/final/redistributed_split_`sg'_complete.txt"
					capture rm "`temp_folder'/split_`sg'/final/magic_table.csv"
					capture rm "`temp_folder'/split_`sg'/final/magic_table_`sg'_complete.txt"
					capture rm "`temp_folder'/split_`sg'/final/post_rdp.dta"
					capture rm "`temp_folder'/_input_data/split_`sg'.csv"
				// Save
					capture confirm file "`temp_folder'/_input_data/split_`sg'.csv"
					if _rc {
						outsheet using "`temp_folder'/_input_data/split_`sg'.csv" if split_group == `sg', comma names replace
					}
					
				// Submit job
					local checkfile = "`temp_folder'/split_`sg'/final/redistributed_split_`sg'_complete.txt"
					capture confirm file "`checkfile'"
					if _rc {
						!/usr/local/bin/SGE/bin/lx24-amd64/qsub -P proj_codprep -pe multi_slot 4 -l mem_free=8g -N "CoD_05_`source'_`sg'" "$prefix/WORK/03_cod/01_database/02_programs/prep/code/shellstata13_`username'.sh" "$prefix/WORK/03_cod/01_database/02_programs/redistribution/code/cod_rdp_worker.do" "`source' `code_version' `sg'"
					}
			}
		}
		
		// Check for completion & get files collapsed by acause
			clear
			forvalues sg = `split_min'/`split_max' {
				noisily display "Checking for acause-level split `sg' (of `split_max')"
				local checkfile = "`temp_folder'/split_`sg'/final/redistributed_split_`sg'_complete.txt"
				capture confirm file "`checkfile'"
				while _rc {
					sleep 5000
					capture confirm file "`checkfile'"
				}
				noisily display "Found!"
				append using "`temp_folder'/split_`sg'/final/redistributed_split_`sg'_collapsed.dta"
			}
			
		// Double check to make sure we haven't lost deaths
			preserve
				collapse (sum) deaths1 orig_deaths1, by(split_group) fast
				gen double diff = abs(deaths1 - orig_deaths1)
				summ diff
				if `r(max)' > 1 {
					save "`output_folder'/05b_for_compilation_bad.dta", replace
					display in red "Number of deaths before redistribution does not match number of deaths after"
					display in red "Check magic tables"
				}
			restore
			
		// Save 05b_for_compilation
			if `r(max)' < 1 {
				save "`output_folder'/05b_for_compilation.dta", replace
				save "`output_folder'/_archive/05b_for_compilation_`timestamp'.dta", replace
			}
			
		// Save a file of redistribution by split group for each demograpic group, location, year, package
			// Check for completion & get files collapsed by acause
				clear
				forvalues sg = `split_min'/`split_max' {
					noisily display "Checking for magic table split `sg' (of `split_max')"
					local checkfile = "`temp_folder'/split_`sg'/final/magic_table_`sg'_complete.txt"
					capture confirm file "`checkfile'"
					while _rc {
						sleep 5000
						capture confirm file "`checkfile'"
					}
					noisily display "Found!"
					append using "`temp_folder'/split_`sg'/final/magic_table_`sg'_collapsed.dta"
				}
				
			// Save 05c_magic_table
				save "`output_folder'/05c_magic_table.dta", replace
	}
	else {
			
		// Ensure all death variables are present then duplicate them for orig_deaths
			foreach i of numlist 1/26 91/94 {
				capture gen deaths`i' = 0
				gen orig_deaths`i' = deaths`i'
			}
		// Generate beforeafter variable
			gen beforeafter = 3
		// Final format
			aorder
			keep dev_status region iso3 location_id national subdiv source source_label source_type NID list year sex im_frmat frmat cause acause deaths* orig_deaths* beforeafter
			order dev_status region iso3 location_id national subdiv source source_label source_type NID list year sex im_frmat frmat cause acause deaths* orig_deaths* beforeafter
		// Save 05a_after_redistribution
			save "`output_folder'/05a_after_redistribution.dta", replace
		// Collapse to acause level
			collapse(sum) *deaths*, by(dev_status region iso3 location_id national subdiv source source_label source_type NID list year sex im_frmat frmat acause beforeafter) fast
		// Save 05b_for_compilation
			save "`output_folder'/05b_for_compilation.dta", replace
			save "`output_folder'/_archive/05b_for_compilation_`timestamp'.dta", replace
	}
	
	
	capture log close


	
// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
