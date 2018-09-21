** *****************************************************************************	**
** Purpose: Prep data for age-sex splitting, call the age-sex-splitting function to assign deaths to 5 year age groups in ages over 1 and to neonatal age groups in ages under 1, and clean up age formatting
** *****************************************************************************	**
clear
set more off

// Set the prefix for whichever os we're in
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" { 
		global j "/home/j"
		set odbcmgr unixodbc
	}
 // Set up fast collapse
 do "$j/WORK/04_epi/01_database/01_code/04_models/prod/fastcollapse.ado"
 
// Pass on globals from master_prep.do
global data_name "`2'"
global timestamp "`3'"
global tmpdir "`4'"
global source_dir "`5'"

// Make the temp directory for this particular dataset
capture mkdir "$tmpdir/02_agesexsplit"	
capture mkdir "$tmpdir/02_agesexsplit/${data_name}"	
global temp_dir "$tmpdir/02_agesexsplit/${data_name}"
	
// Define other macros
global wtsrc acause
global codmod_level acause
global weight_dir "$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/data/weights"
	
// Log our efforts
log using "$source_dir/$data_name/logs/02_agesexsplit_${timestamp}", replace

display "data name: $data_name"
display "timestamp: $timestamp"
display "tmpdir: $tmpdir"
display "source_dir: $source_dir"
	
// Load and prep the population data
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/env_wide.do"
	keep if location_id == .
	drop env*
	** Simulate previous years for 
	if strmatch("$data_name","_Australia*") | "$data_name" == "ICD7A" | "$data_name" == "ICD8A" {
		forval y = 1907/1969 {
			expand 2 if year == 1970, gen(exp)
			replace year = `y' if exp == 1
			drop exp
		}
	}
	save "$temp_dir/pop_prepped.dta", replace

 // Bring in mapped data
	if inlist("${data_name}", "India_CRS", "India_SCD_states_rural") {
		use "$source_dir/$data_name/data/intermediate/02a_before_agesex.dta", clear
	} 
	else {
		use "$source_dir/$data_name/data/intermediate/01_mapped.dta", clear
	}

// Identify data sources that require deaths to be split into GBD age groups, sexes, or GBD age and sex groups.
count if frmat!=2 | im_frmat>2 | sex==9 | sex==. | sex==3
if `r(N)'>0 {
	
	// Collapse deaths in ages 80-84 with 85+
	egen double oldest = rowtotal(deaths22 deaths23)
	replace deaths22 = oldest
	replace deaths23 = 0
	drop oldest	

	// Run and load into memory the fuctions for age splitting, agesex splitting, and storing summary statistics
	quietly do "$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/code/agesexsplit.ado"
	quietly do "$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/code/agesplit.ado"
	quietly do "$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/code/summary_store.ado"

	// Erase any old agesplit and agesexsplit files so that are not appended with the new age and sex split files. Call any remaining old age and sex split files in the temp directory and store in the local split_files
	local split_files: dir "$temp_dir" files "${data_name}_ages*", respectcase 
	foreach i of local split_files {
		capture erase "$temp_dir/`i'"
	}

	// Create vectors to store checks along the way
	local maxobs 10000
	global currobs 1
	mata: stage = J(`maxobs', 1, "")
	mata: sex = J(`maxobs', 1, .)
	mata: sex_orig = J(`maxobs', 1, .)
	mata: frmat = J(`maxobs', 1, .)
	mata: frmat_orig = J(`maxobs', 1, .)
	mata: im_frmat = J(`maxobs', 1, .)
	mata: im_frmat_orig = J(`maxobs', 1, .)
	mata: deaths_sum = J(`maxobs', 1, .)
	mata: deaths1 = J(`maxobs', 1, .)
			
	// Recalculate deaths1 to ensure first set of summary statistics will be accurate
	capture drop deaths1
	aorder 
	egen deaths1 = rowtotal(deaths3-deaths94)
	
	// Store preliminary values for deaths_sum and deaths1 before making any other changes to the dataset
	preserve
		aorder
		egen deaths_sum = rowtotal(deaths3-deaths94)
		summary_store, stage("beginning") currobs(${currobs}) storevars("deaths_sum deaths1") ///
			sexvar("sex") frmatvar("frmat") im_frmatvar("im_frmat")
	restore
		
	// Clean up age formatting
		egen tmp = rowtotal(deaths91 deaths92)
		replace deaths91 = tmp
		replace deaths92 = 0
		drop tmp
		replace im_frmat = 2 if im_frmat == 1

		** If im_frmat = 8, replace deaths91 with deaths2 to ensure that we get all deaths in age 0.
		replace deaths2 = deaths91 if deaths2==0 & im_frmat==8
		** When standardizing input data, deaths2 gets replaced with the sum of all deaths in age groups under 1 (deaths91-deaths94). Now replace deaths91 with deaths2. This ensures deaths91 is the sum of deaths91-deaths94 when im_frmat = 8. 
		** Note that we also need to zero out the other infant deaths to prevent double counting. 
		replace deaths91 = deaths2 if im_frmat == 8
		foreach var of varlist deaths92 deaths93 deaths94 {
			replace `var' = 0 if im_frmat == 8
		}

		** If im_frmat = 9, add on deaths3 since im_frmat = 9 is for 0-4 years combined
		replace deaths91 = deaths2 + deaths3 if im_frmat == 9
		
		** Recalculate deaths1 to ensure total is correct
		drop deaths1
		aorder 
		egen deaths1 = rowtotal(deaths3-deaths94)
		** Make sure deaths1 = deaths26 in data with no age distribution
		replace deaths26 = deaths1 if frmat == 9
		
		** Drop deaths2. We don't use "age 0" for anything once the ages have been split into neonatal age groups. 
		drop deaths2
		
		** Get rid of extraneous observations without deaths.
		aorder
		capture drop tmp
		egen tmp = rowtotal(deaths*) 
		** Exception for DHS subnational sources we want to leave in the rows with 0 deaths for maternal.
		gen keep = 1 if source == "Other_Maternal" & inlist(iso3, "BRA", "KEN", "ZAF") & regexm(source_label, "DHS")
		drop if tmp == 0 & keep != 1
		drop tmp keep
		
	// Verify missing sex incase it was missed in standardized data prep. 
		replace sex=9 if sex==3 | sex==.

	// Check that frmat and im_frmat are designated for all observations
	count if frmat == .
	if `r(N)' != 0 {
		di in red "WARNING: frmat variable is missing values.  Splitting will not work."
		pause
	}
	count if im_frmat == .
	if `r(N)' != 0 {
		di in red "WARNING: im_frmat variable is missing values.  Splitting will not work."
		pause
	}
		
	// Check that we actually need to do age-sex splitting. Only run the rest of the code if there are observations that need to be split. 
	// All datasets must be run up to this point so we can change any data tagged with frmat 1 to frmat 2 and to ensure frmats are designated for all datsets.
	count if (sex != 1 & sex != 2) | frmat != 2 | im_frmat != 2 
	if `r(N)' != 0 { 
		// Record the total number of deaths in the original data before we age-sex split. 
			capture drop deathsorig_all
			egen deathsorig_all = rowtotal(deaths3-deaths94)
			// Make a tempfile of the entire dataset
			noisily display "Saving allfrmats tempfile"
			tempfile allfrmats
			save `allfrmats', replace

		// Store a summary of the deaths_sum and deaths1 at this stage.
		egen deaths_sum = rowtotal(deaths3-deaths94)
		summary_store, stage("allfrmats") currobs(${currobs}) storevars("deaths_sum deaths1") ///
			sexvar("sex") frmatvar("frmat") im_frmatvar("im_frmat")
		drop deaths_sum

		// Prepare data for splitting
			** rename deaths variables to be deaths_1, deaths_5, deaths_10, etc. and cause
			rename deaths3 deaths_1
			rename deaths26 deaths_99
			rename deaths91 deaths_91
			rename deaths93 deaths_93
			rename deaths94 deaths_94
			replace deaths22 = 0 if deaths22 == .
			replace deaths23 = 0 if deaths23 == .
			replace deaths22 = deaths22 + deaths23
			replace deaths23 = 0
			forvalues i = 7/22 {
				local j = (`i'-6)*5
				rename deaths`i' deaths_`j'
			}
			rename cause cause_orig
			
			// Map to the splitting cause (level 3)
				preserve
					use "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", clear
					keep cause_id path_to_top_parent level acause yld_only yll_age_start yll_age_end male female
					** Make sure age restriction variables are doubles, not floats
					foreach var of varlist yll_age_start yll_age_end {
						recast double `var'
						replace `var' = 0.01 if `var' > 0.009 & `var' < 0.011
						replace `var' = 0.1 if `var' > 0.09 & `var' < 0.11
					}
					** Drop the parent "all_cause"
					levelsof cause_id if level == 0, local(top_cause)
					drop if path_to_top_parent =="`top_cause'"
					replace path_to_top_parent = subinstr(path_to_top_parent,"`top_cause',", "", .)	
					** Make cause parents for each level
					rename path_to_top_parent agg_
					split agg_, p(",")
					** Take the cause itself out of the path to parent, also map the acause for each aggregate
					rename acause acause_orig
					rename cause_id cause_id_orig
					forvalues i = 1/5 {
						rename agg_`i' cause_id
						destring cause_id, replace
						merge m:1 cause_id using "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", keepusing(acause) keep(1 3) nogen
						rename acause acause_`i'
						rename cause_id agg_`i'
						tostring agg_`i', replace
						replace agg_`i' = "" if level == `i' | agg_`i' == "."
						replace acause_`i' = "" if level == `i'
						order acause_`i', after(agg_`i')
					}
					rename acause_orig acause
					rename cause_id_orig cause_id
					compress
					tempfile all
					save `all', replace
				restore
				
			// Make an acause which is the level 3 version of acause if acause is level 4 or 5
				merge m:1 acause using `all', keep(1 3) keepusing(level acause_3) nogen
				gen cause = acause
				forvalues i = 4/5 {
					replace cause = acause_3 if level == `i'
				}
				drop level acause_*
				
			// There are too many variables that could possibly be identifying variables in these datasets so generate a unique id. 
			gen id_variable = iso3 + string(location_id) + string(year) + string(region) + dev_status + string(sex) + cause + cause_orig + acause + source + source_label + source_type ///
				+ string(national) + subdiv + string(im_frmat) + string(frmat) + string(NID)
			capture replace id_variable = id_variable + string(subnational)
			
			// Load age format keys containing information about whether the frmat/age combination needs to be split.
				// im_frmats
				preserve
					insheet using "$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/maps/frmat_im_key.csv", comma clear
					tempfile frmat_im_key
					save `frmat_im_key', replace
				restore
			
			// other frmats
				preserve
					insheet using "$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/maps/frmat_key.csv", comma clear
					tempfile frmat_key
					save `frmat_key', replace
				restore
				
				// frmat 9
				preserve
					insheet using "$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/maps/frmat_9_key.csv", comma clear
					tempfile frmat_9_key
					save `frmat_9_key', replace
				restore
			
			// Save all data, post-prep
			noisily display "Saving allfrmats_prepped tempfile"
			tempfile allfrmats_prepped
			save `allfrmats_prepped', replace

			// Make a tempfile of the observations that need to be age-split only, with age long. 
				// Double check that data needs to be age split and im_frmat or frmat is not missing. Exclude sex == 9,  that's dealt with in age-sex-splitting.
				keep if ((frmat > 2 & frmat != .) | (im_frmat > 2 & im_frmat != .)) & sex != 9

				// Drop the age groups we don't need
				capture drop deaths4 deaths5 deaths6 deaths23 deaths24 deaths25 deaths1 deaths92
				
				// Reshape to get age long for infants, adults, and frmat 9 separately as it takes too long to do it all at once.
					** Reshape infant formats
					preserve
						// Reduce to non-standard im_frmats, excluding them if frmat == 9 (deaths_99 == 0 | deaths_99 == .).  Deal with frmat = 9 last
						keep if (im_frmat > 2) & (deaths_99 == 0 | deaths_99 == .) & frmat != 9
						// We only need infant deaths, not the other ages or frmat 9
						aorder
						drop deaths_1-deaths_80 deaths_99
						reshape long deaths_, i(id_variable) j(age)
						
						// Merge with key from above to keep only the frmats and age groups that need to be split.
						merge m:1 im_frmat age using `frmat_im_key', keep(3) nogenerate
						keep if needs_split == 1
						// Save for later
						tempfile ims_age
						save `ims_age', replace
					restore
					
					// Now reshape adult formats
					preserve
						// Reduce to non-standard frmats, excluding them if frmat == 9 (deaths_99 == 0  deaths_99 == .). Deal with frmat = 9 last
						keep if (frmat > 2) & (deaths_99 == 0 | deaths_99 == .)
						// We only need adult deaths, not infant or frmat 9.
						drop deaths_91 deaths_93 deaths_94 deaths_99
						reshape long deaths_, i(id_variable) j(age)
						
						// Merge with key from above to keep only the frmats and age groups that need to be split
						merge m:1 frmat age using `frmat_key', keep(3) nogenerate
						keep if needs_split == 1
						// Save for later
						tempfile adults_age
						save `adults_age', replace
					restore
					
					// Lastly, reshape frmat = 9
					preserve
						// Reduce to frmat 9 deaths
						keep if (deaths_99 != 0 & deaths_99 != .)
						// We only need deaths where frmat = 9 (deaths_99)
						aorder
						drop deaths_1-deaths_94
						reshape long deaths_, i(id_variable) j(age)
						// Ensure that the frmat is properly labeled
						replace frmat = 9
						
						// Merge with key from above to keep only the frmats and age groups that need to be split
						merge m:1 frmat age using `frmat_9_key', keep(3) nogenerate
						keep if needs_split == 1
						// Save for later
						tempfile frmat9_age
						save `frmat9_age', replace
					restore
							
					// Append these tempfiles together now we have a reshaped dataset with age long
					clear
					use `ims_age'
					append using `adults_age'
					append using `frmat9_age'
					rename deaths_ deaths
						
					// Save what we have so far. This will be the file that gets age-split.
					noisily display "Saving $temp_dir/temp_to_agesplit.dta"
					save "$temp_dir/temp_to_agesplit.dta", replace
					
					// Store a summary of the deaths_sum at this stage.
					rename deaths deaths_sum
					summary_store, stage("temp_to_agesplit") currobs(${currobs}) storevars("deaths_sum") ///
						sexvar("sex") frmatvar("frmat") im_frmatvar("im_frmat")
					rename deaths_sum deaths

			// Make a tempfile of the observations that need to be age-sex-split OR sex-split only, with age long
				// Open file with all data after prepping
				use `allfrmats_prepped', clear

				// Keep only the observations that need age-sex-splitting or sex-splitting.
				keep if sex == 9
				// Drop the age groups we don't need
				capture drop deaths4 deaths5 deaths6 deaths23 deaths24 deaths25 deaths1 deaths92
				
				// Reshape to get age long for infants, adults, and frmat 9 separately as it takes too long to do it all at once
					** Reshape im_frmats
					preserve
						// Exclude observations if frmat == 9 (deaths_99 == 0 | deaths_99 == .), deal with these last.
						keep if (deaths_99 == 0 | deaths_99 == .) & frmat != 9
						//  We only need infant deaths, not infant or frmat 9
						aorder
						drop deaths_1-deaths_80 deaths_99
						reshape long deaths_, i(id_variable) j(age)
						
						// Merge with key from above to get values for age_start and age_end
						merge m:1 im_frmat age using `frmat_im_key', keep(3) nogenerate
						// Save for later
						tempfile ims_agesex
						save `ims_agesex', replace
					restore
							
					**  Now reshape adult formats
					preserve
						// Exclude observations if frmat == 9 (deaths_99 == 0  deaths_99 == .),  deal with these later. 
						keep if (deaths_99 == 0 | deaths_99 == .) & frmat != 9
						// We only need adult deaths, not infant or frmat 9.
						drop deaths_91 deaths_93 deaths_94 deaths_99
						reshape long deaths_, i(id_variable) j(age)
						
						// Merge with key from above to get values for age_start and age_end
						merge m:1 frmat age using `frmat_key', keep(3) nogenerate
						// Save for later
						tempfile adults_agesex
						save `adults_agesex', replace
					restore
					
					** Lastly, reshape frmat = 9.
					preserve
						// Reduce to frmat 9 deaths
						keep if (deaths_99 != 0 & deaths_99 != .)
						
						// We only need deaths where frmat = 9 (deaths_99)
						aorder
						drop deaths_1-deaths_91
						reshape long deaths_, i(id_variable) j(age)
						// Ensure that the frmat is properly labeled
						replace frmat = 9
						
						// Merge with key from above to keep only the frmats and age groups that need to be split
						merge m:1 frmat age using `frmat_9_key', keep(3) nogenerate
						// Save for later
						tempfile frmat9_agesex
						save `frmat9_agesex', replace
					restore
						
				// Append these tempfiles together now we have a reshaped dataset with age long.
				clear
				use `ims_agesex'
				append using `adults_agesex'
				append using `frmat9_agesex'
				rename deaths_ deaths
				
				// Save what we have so far this will be the file that gets age-sex-split.
				noisily display "Saving $temp_dir/temp_to_agesexsplit.dta"
				save "$temp_dir/temp_to_agesexsplit.dta", replace
				
				// Store a summary of the deaths_sum at this stage
				rename deaths deaths_sum
				// Make isopop iso3
				gen isopop = iso3
				
				summary_store, stage("temp_to_agesexsplit") currobs(${currobs}) storevars("deaths_sum") ///
					sexvar("sex") frmatvar("frmat") im_frmatvar("im_frmat")
				rename deaths_sum deaths

		// Run the splitting function 
			// Split data that needs age splitting only
			noisily display _newline "Beginning agesplitting"
				
				// Open file for age-splitting
				use "$temp_dir//temp_to_agesplit.dta", clear
				
				// If there are observations in this dataset, loop through each cause and split the ages for that cause
				capture levelsof cause, local(causes)
				if ! _rc {
					foreach c of local causes {
						noisily di in red "Splitting cause `c'"
							agesplit, splitvar("deaths") splitfil("$temp_dir/temp_to_agesplit.dta") ///
							outdir("$temp_dir") weightdir("$weight_dir") mapfil("$iso3_dir/countrycodes_official.dta") ///
							dataname("$data_name") cause("`c'") wtsrc("$wtsrc")
					}
				}
								
			
			// Split datat that needs age-sex-splitting and sex-splitting only
			noisily display _newline "Beginning agesexsplitting"
			
				// Open file for age-sex-splitting
				use "$temp_dir/temp_to_agesexsplit.dta", clear
				
				// If there are observations in this dataset, loop through each cause and split the ages for that cause
				capture levelsof cause, local(causes)
				if ! _rc {
					foreach c of local causes {
						noisily di in red "Splitting cause `c'" 
						agesexsplit, splitvar("deaths") splitfil("$temp_dir/temp_to_agesexsplit.dta") ///
							outdir("$temp_dir") weightdir("$weight_dir") mapfil("$iso3_dir/countrycodes_official.dta") ///
							("$data_name") cause("`c'") wtsrc("$wtsrc")
					}
				}
			
			
		// Compile the split files
			// Prepare a dataset for appending
			clear
			set obs 1
			gen split = ""

			// Store the names of the age-split files
			local files: dir "$temp_dir" files "${data_name}_agesplit*", respectcase
			// Loop through the age-split files to combine them
			foreach f of local files {
				append using "$temp_dir/`f'"
			}
			// Mark that observations have come from the age-splitting code
			replace split = "age"
			
			// Store the names of the age-sex-split files
			local files: dir "$temp_dir" files "${data_name}_agesexsplit*", respectcase
			// Loop through the age-sex-split files to combine them
			foreach f of local files {
				append using "$temp_dir/`f'"
			}
			// Mark that these observations have come from the age-sex-splitting code
			replace split = "age-sex" if split == ""
			
			// Mark observations with missing population
			gen nopop_adult = (nopop_10 == 1)
			gen nopop_im = (nopop_1 == 1)
			gen nopop_frmat9 = ((nopop_adult == 1 | nopop_im == 1) & frmat == 9)
			drop nopop_1-nopop_80
			
			// Rename the deaths variables so they are consistent with the WHO age format
			rename deaths deaths_orig_split
			rename deaths_1 deaths3
			forvalues i = 5(5)80 {
				local j = (`i'/5) + 6
				rename deaths_`i' deaths`j'
			}
			renpfix deaths_ deaths
			
			// Record original frmats (Note: original sexes were recorded in the agesexsplitting algorithm)
			gen frmat_orig = frmat
			gen im_frmat_orig = im_frmat

			// If population for adults and infants exist reset the frmat and im_frmat for successfully split observations. 
			replace frmat = 2 if (needs_split == 1 | sex_orig == 9) & nopop_adult != 1 & nopop_frmat9 != 1 	
			replace im_frmat = 2 if (needs_split == 1 | sex_orig == 9) & nopop_im != 1 & nopop_frmat9 != 1 
			
			// Rename cause and cause_orig back to their original names
			drop cause
			rename cause_orig cause
			
			//  Generate a deaths92, deaths4-deaths6, and deaths23-deaths26 since we have those in the pre-age split tempfile, `allfrmats'.
			generate deaths92 = 0
			forvalues i = 4/6 {
				generate deaths`i' = 0
			}
			forvalues i = 23/26 {
				generate deaths`i' = 0
			}
			
			// Clean-up
			drop if cause == "" & year == .
			
			// Save compiled split files
			tempfile splits
			save `splits', replace
			noisily display _newline "Saving splits tempfile"
			
			// Store a summary of the deaths_sum at this stage
			preserve
			aorder
			egen deaths_sum = rowtotal(deaths3-deaths94)
			summary_store, stage("splits") currobs(${currobs}) storevars("deaths_sum") sexvar("sex_orig") frmatvar("frmat_orig") im_frmatvar("im_frmat_orig")
			restore
			
			
		// Combine split data with frmats and age groups that did not need to be split by merging on `allfrmats'. Be sure we are careful with the data in deaths* that are missing and which are zero.
			// Prepare file with split data
				// Zero out all deaths* columns that are missing so that they're not accidentally updated during the merge.  Also create a copy for use in frmats that cross the adult/infant divide.
				foreach var of varlist deaths* {
					replace `var' = 0 if `var' == .
					gen `var'_tmp = `var'
				}
				
				// Number each observation within each group, so we know which entry to replace later on
				bysort iso3 region dev_status location_id NID subdiv source *national ///
						*frmat* subdiv list sex year cause acause split: egen obsnum = seq()
				
				// Fix deathsorig_all. We need to zero out all but one of these entries, so that it doesn't multiply when we collapse (sum) later.		
				replace deathsorig_all = 0 if obsnum != 1
				
				// For individual frmats, change to missing one observation for each deaths category that didn't need to be split. As with deaths_orig_all, we're only changing one observation because we don't want it to multiply when we collapse
					// Establish the keys that mark entries that need to be changed to missing as "9999"
					preserve
						insheet using "$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/maps/frmat_replacement_key.csv", comma clear
						tempfile replacement_key
						save `replacement_key', replace
					restore
					
					preserve
						insheet using "$j/WORK/03_cod/01_database/02_programs/age_sex_splitting/maps/frmat_im_replacement_key.csv", comma clear
						tempfile replacement_im_key
						save `replacement_im_key', replace
					restore

					// Merge on original frmat, split, and obsnum to make sure that only the first observation in each set is replaced with 9999, and only the proper age groups for each frmat are marked.  Note that by merging on split (= "age") as well, we're only doing these replacements for observations that have only been age-split.  For age-sex-splitting or sex-splitting, all the post-split observations need to stay as they are.
					merge m:1 frmat_orig split obsnum using `replacement_key', update replace
					drop if _m == 2
					drop _m
					
					merge m:1 im_frmat_orig split obsnum using `replacement_im_key', update replace
					drop if _m == 2
					drop _m
					
					// Fix formats and im_frmats that cross the adult/infant divide.
						// Fix deaths3 if im_frmat is 9
						replace deaths3 = deaths3_tmp if im_frmat_orig == 9
						// Fix infant deaths if frmat is 9
						forvalues i = 91/94 {
							replace deaths`i' = deaths`i'_tmp if frmat_orig == 9
						}
						// Fix infant deaths if im_frmat is 10
						local adult_deaths 3 7 8
						foreach i of local adult_deaths {
							replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 10
						}
						// Fix infant deaths if im_frmat is 11
						local adult_deaths 3 7 8 9 10 11 12 13 14 15 16 17
						foreach i of local adult_deaths {
							replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 11
						}
						// Fix infant formats if im_frmat is 05
						local adult_deaths 3
						foreach i of local adult_deaths {
							replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 5
						}						
						// Fix infant formats if im_frmat is 06
						local adult_deaths 3
						foreach i of local adult_deaths {
							replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 6
						}						
						// Fix infant formats if im_frmat is 12
						local adult_deaths 3 4
						foreach i of local adult_deaths {
							replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 12
						}						
						// Fix infant formats if im_frmat is 13
						local adult_deaths 3
						foreach i of local adult_deaths {
							replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 13
						}						
						// Fix infant formats if im_frmat is 14
						local adult_deaths 3
						foreach i of local adult_deaths {
							replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 14
						}						
						// Fix infant formats if im_frmat is 15
						local adult_deaths 3 4
						foreach i of local adult_deaths {
							replace deaths`i' = deaths`i'_tmp if im_frmat_orig == 15
						}

						// Get rid of temporary deaths variables now that we've finished fixing frmats that cross the infant/adult divide
						foreach var of varlist deaths*tmp {
							drop `var'
						}
					
					// Change marked observations to be missing
					foreach var of varlist deaths* {
						replace `var' = . if `var' == 9999
					}
					
				// Change to missing one observation for each deaths category that wasn't split because of missing population. as above, we're only changing one observation because we don't want it to multiply when we collapse.
				foreach var of varlist deaths91-deaths3 {
					replace `var' = . if nopop_im == 1 & obsnum == 1
				}
				foreach var of varlist deaths7-deaths22 {
					replace `var' = . if nopop_adult == 1 & obsnum == 1
				}
				foreach var of varlist deaths91-deaths22 {
					replace `var' = . if nopop_frmat9 == 1 & obsnum == 1
				}
				
			// Combine the split data with pre-split data, updating the split data for age groups that did not need to be split.  This needs to be done separately for age-split and age-sex-split observations, since age-split data need to be merged on sex, but we can't do this for age-sex-split data.
				// For age-split observations
					// Prepare `allfrmats' to only include those observations which should have been age-split
					preserve
						// Load prepared data
						use `allfrmats', clear
						drop if sex == 9
						
						gen frmat_orig = frmat
						gen im_frmat_orig = im_frmat
						
						noisily display "Saving allfrmats_ageonly tempfile"
						tempfile allfrmats_ageonly
						save `allfrmats_ageonly', replace
						
						// Store a summary of the deaths_sum and deaths1 at this stage
						aorder
						egen deaths_sum = rowtotal(deaths3-deaths94)
						summary_store, stage("allfrmats_ageonly") currobs(${currobs}) storevars("deaths_sum deaths1") sexvar("sex") frmatvar("frmat_orig") im_frmatvar("im_frmat_orig")
					restore
					
					// Merge with age-split observations
					preserve
						drop if sex_orig == 9
						merge m:1 iso3 year sex cause acause *frmat_orig subdiv location_id NID source* using `allfrmats_ageonly', update
					
					noisily display "Saving postsplit_ageonly tempfile"
						tempfile postsplit_ageonly
						save `postsplit_ageonly', replace
						
						// Store a summary of the deaths_sum and deaths1 at this stage
						aorder
						egen deaths_sum = rowtotal(deaths3-deaths94)
						summary_store, stage("postsplit_ageonly") currobs(${currobs}) storevars("deaths_sum deaths1") sexvar("sex") frmatvar("frmat_orig") im_frmatvar("im_frmat_orig")
					restore
				
					// Age-sex-split and sex-split observations
						// Prepare `allfrmats' to only include those observations which should have been age-sex-split
						preserve
							// Load Prepare data
							use `allfrmats', clear
							keep if sex == 9
							
							gen frmat_orig = frmat
							gen im_frmat_orig = im_frmat
						
							noisily display "Saving allfrmats_agesexonly tempfile"
							tempfile allfrmats_agesexonly
							save `allfrmats_agesexonly', replace
							
							// Store a summary of the deaths_sum and deaths1 at this stage
							aorder
							egen deaths_sum = rowtotal(deaths3-deaths94)
							summary_store, stage("allfrmats_agesexonly") currobs(${currobs}) storevars("deaths_sum deaths1") ///
								sexvar("sex") frmatvar("frmat_orig") im_frmatvar("im_frmat_orig")
							rename deaths_sum deaths
						restore
							
					// All observations where sex_orig == 9 should have been split, except when population was missing. In this case, we'll need to have multiple observations for the same iso3 year cause; one for each sex for which we have data (split or not split).
						// Use infants data with no population
						preserve
							// Determine which observations from the split files weren't split because of missing infant populations (we'll deal with no population for frmat 9 later)
							keep if sex_orig == 9
							keep if nopop_im == 1 & nopop_frmat9 != 1
							
							// For these iso3/year/causes, merge with pre-split data so we can recover the deaths
							keep iso3 location_id NID source source_label source_type year cause cause_name list nopop* national subdiv dev_status region
							capture duplicates drop
							merge 1:m iso3 location_id NID source source_label source_type year cause cause_name list national subdiv dev_status region using `allfrmats_agesexonly', keep(3) nogenerate
							
							// Zero out everything, except the infant deaths that couldn't be split
							aorder
							foreach var of varlist deaths1-deaths26 {
								replace `var' = 0
							}
							
							// Save file
							tempfile nopop_im
							save `nopop_im', replace
						restore
						
						// Use adults data with no population
						preserve
							// Determine which observations from the split files weren't split because of missing adult populations (we'll deal with no population for frmat 9 later)
							keep if sex_orig == 9
							keep if nopop_adult == 1 & nopop_frmat9 != 1
							
							// For these iso3/year/causes, merge with pre-split data so we can recover the deaths
							keep iso3 location_id NID source source_label source_type year cause cause_name list nopop* national subdiv dev_status region
							capture duplicates drop
							merge 1:m iso3 location_id NID source source_label source_type year cause cause_name list national subdiv dev_status region using `allfrmats_agesexonly', keep(3) nogenerate
							
							// Zero out everything, except the adult deaths that couldn't be split
							aorder
							foreach var of varlist deaths91-deaths94 {
								replace `var' = 0
							}
							
							// Save file
							tempfile nopop_adult
							save `nopop_adult', replace
						restore
						
						// Use no age distribution data with no population
						preserve
							// Determine which split data wasn't split because of no population in with infants or adults with ftrmat9
							keep if sex_orig == 9
							keep if nopop_frmat9 == 1
							
							// For these iso3/year/causes, merge with pre-split data so we can recover the deaths
							keep iso3 location_id NID source source_label source_type year cause cause_name nopop*
							capture duplicates drop
							merge 1:m iso3 location_id NID source source_label source_type year cause cause_name using `allfrmats_agesexonly', keep(3) nogenerate
							
							// Nothing was split successfully, so don't zero out any deaths in this case
							aorder
							
							//  Save file
							tempfile nopop_frmat9
							save `nopop_frmat9', replace
						restore
						
						// Append these files together so we maintain all the deaths for sex-split observations
						keep if sex_orig == 9
						append using `nopop_im'
						append using `nopop_adult'
						append using `nopop_frmat9'
						noisily display "Saving postsplit_agesexonly tempfile"
						tempfile postsplit_agesexonly
						save `postsplit_agesexonly', replace
						
						// Store a summary of the deaths_sum and deaths1 at this stage
						aorder
						egen deaths_sum = rowtotal(deaths3-deaths94)
						summary_store, stage("postsplit_agesexonly") currobs(${currobs}) storevars("deaths_sum deaths1") ///
							sexvar("sex") frmatvar("frmat_orig") im_frmatvar("im_frmat_orig")
								
				// Combine both age-split and age-sex-split observations
				use `postsplit_ageonly', clear
				append using `postsplit_agesexonly'

			// Since we have multiple observations per group, collapse to get a total count
				// Make sure that the proper deaths are set to missing.  If the original im_frmat was 3, we don't have any information about deaths94 so set it missing.
				replace deaths94 = . if im_frmat_orig == 3 & frmat_orig != 9
			
				// Collapse sums of missing entries to make 0, so we need to mark the groups where all the observations for a variable are missing
					// Create a group variable rather than always sorting on all these variables
					egen group = group(iso3 region dev_status location_id NID subdiv source* *national ///
						frmat im_frmat subdiv list sex year cause acause), missing
					
					// Loop through variables, creating a miss_`var' variable that records whether all the observations within the group are missing for the variable `var'
					foreach var of varlist deaths* {
						bysort group (`var'): gen miss_`var' = mi(`var'[1])
					}
				
				// Collapse retaining the value for miss_`var'
				collapse (sum) deaths* (mean) miss* nopop*, by(iso3 location_id ///
					subdiv source* NID *national frmat im_frmat list sex year cause cause_name acause region dev_status)
				
				// Use miss_`var' to inform which variables need to be reverted back to missing
				foreach var of varlist deaths* {
					di "`var'"
					replace `var' = . if miss_`var' != 0
				}
				drop miss*
			
		// Final adjustments and formatting
			// Drop any data that don't contain any information (i.e. frmat = 9 and lacking population numbers)
			capture drop tmp
			egen tmp = rowtotal(deaths91-deaths22 deaths26), missing
			drop if tmp == .
			drop tmp

			// If deaths were in deaths26 when the rest of the observations was in a frmat other than frmat 9, there may be remaining deaths in deaths26.  We'll redistribute them using cause-level age proportions.
				// First create country-year-sex-age proportions by cause
				aorder
				capture drop deaths_known
				
				// Temporarily generate a deaths23 that incorporates deaths23-deaths25, since we correct restriction violations after age splitting and we need deaths24 and 25.
				gen deaths23_tmp = deaths23
				capture drop tmp
				egen tmp = rowtotal(deaths23 deaths24 deaths25)
				replace deaths23 = tmp if frmat != 9
				drop tmp

				// Total up deaths we've ended up with after splitting
				egen deaths_known = rowtotal(deaths3-deaths23 deaths91-deaths94)
				** replace deaths_known = round(deaths_known)

				capture drop totdeaths
				bysort iso3 location_id year sex $codmod_level source* NID: egen totdeaths = total(deaths_known)
				
				foreach i of numlist 3/23 91/94 {
					bysort iso3 location_id year sex $codmod_level source* NID: egen num`i' = total(deaths`i')
					generate codmodprop`i' = num`i'/totdeaths
				
					// Now redistribute the remaining deaths26
					generate new_deaths`i' = deaths`i' + (deaths26*codmodprop`i')
				
					// Replace deaths = newdeaths where frmat is still 9
					replace deaths`i' = new_deaths`i' if frmat == 9
				}
				
			// We don't have any deaths for some country-year-sex-cause groups, so use global averages
				// First mark those observations where we weren't able to split up deaths26 because that country/year/sex/cause level group doesn't have any deaths (they will have numtot = 0)
				aorder
				egen numtot = rowtotal(num*)
					
				// Also replace deaths with 0 where they are missing
				foreach i of numlist 3/23 91/94 {
					replace deaths`i' = 0 if deaths`i' == .
				}
				
				bysort $codmod_level: egen totknown = total(deaths_known)
				foreach i of numlist 3/23 91/94 {
					bysort $codmod_level: egen totnum`i' = total(deaths`i')
					generate totprop`i' = totnum`i'/totknown
					generate othernew`i' = deaths`i' + (deaths26*totprop`i')
					replace deaths`i' = othernew`i' if frmat == 9 & numtot == 0
				}
				
				// Do some final cleanup
				replace deaths23 = deaths23_tmp if frmat != 9
				drop deaths23_tmp
				replace frmat = 2 if frmat == 9
				replace deaths26 = 0	
			
			// Recalculate deaths1
			aorder
			capture drop tmp
			egen tmp = rowtotal(deaths3-deaths94)
			replace deaths1 = tmp
			drop tmp
			
			
		// Warn user if there is no population
		count if nopop_adult != 0 & nopop_adult != .
		if r(N) > 0 {
			preserve
			local numobs = r(N)
			egen totadult = rowtotal(deaths3-deaths25)
			summ totadult if nopop_adult != 0 & nopop_adult != .
			noisily di in red "WARNING: Missing adult population numbers.  " ///
				"`numobs' observations and `r(sum)' deaths not split for adults.  "
			keep if nopop_adult != 0 & nopop_adult != .
			keep iso3 location_id year
			duplicates drop
			sort iso3 year
			noisily di in red "The following country-years are missing adult population numbers:"
			noisily list
			restore
		}

		count if nopop_im != 0 & nopop_im != .
		if r(N) > 0 {
			preserve
			local numobs = r(N)
			summ deaths91 if nopop_im != 0 & nopop_im != .
			noisily di in red "WARNING: Missing infant population numbers.  " ///
				"`numobs' observations and `r(sum)' deaths not split for infants.  "
			keep if nopop_im != 0 & nopop_im != .
			keep iso3 location_id year
			duplicates drop
			sort iso3 year
			noisily di in red "The following country-years are missing infant population numbers:"
			noisily list
			restore
		}

		count if nopop_frmat9 != 0 & nopop_frmat9 != .
		if r(N) > 0 {
			preserve
			keep if nopop_frmat9 != 0 & nopop_frmat9 != .
			keep iso3 location_id year
			duplicates drop
			sort iso3 year
			noisily di in red "WARNING: Missing population numbers AND frmat 9.  Big problems.  " ///
				"(Deaths in deaths26 have been lost.)  The following country-years are problematic:"
			noisily list
			restore
		}


		// Drop variables we don't need
		drop nopop* *orig* codmodprop* num* new_deaths* tot* other* deaths_known

		// Store a summary of the deaths_sum and deaths1 and export to .csv
		preserve
			egen deaths_sum = rowtotal(deaths3-deaths94)
			summary_store, stage("end") currobs(${currobs}) storevars("deaths_sum deaths1") sexvar("sex") ///
				frmatvar("frmat") im_frmatvar("im_frmat")

			// Export to Stata dataset
			clear
			getmata stage sex sex_orig deaths_sum deaths1 frmat frmat_orig im_frmat im_frmat_orig
			drop if stage == ""
			
			// Calculate useful comparisons
				// Difference between stage-specific deaths_sum and deaths1 (not expected to be correct for postsplit files, so make these missing).  NOTE: these should match.
				gen stage_diff = deaths_sum - deaths1
				replace stage_diff = . if regexm(stage, "postsplit")
				
				// Calculate death differences
				local deaths_diff = $allfrmats_deaths_sum - $end_deaths_sum
				local deaths_to_split = $temp_to_agesplit_deaths_sum + $temp_to_agesexsplit_deaths_sum
				local split_diff = `deaths_to_split' - $splits_deaths_sum
				local ageonly_diff = $allfrmats_ageonly_deaths_sum - $postsplit_ageonly_deaths_sum
				local agesexonly_diff = $allfrmats_agesexonly_deaths_sum - $postsplit_agesexonly_deaths_sum
							
			// Report important comparisons
				// Total deaths
				noisily di in red _newline "Total deaths"
				noisily di "# of deaths at start: ${allfrmats_deaths_sum}" _newline ///
					"# of deaths at end: ${end_deaths_sum}" _newline ///
					"Total deaths lost during splitting: `deaths_diff'"
				if `deaths_diff' > 0.01 {
					noisily di in red "***WARNING: More than 0.01 deaths lost during the agesexsplitting process!***"
				}
						
				// Deaths that should have been split
				noisily di in red _newline "Deaths that should have been split (includes both agesplit and sexsplit)"
				noisily di "# of deaths to be split: `deaths_to_split'" _newline ///
					"# of deaths after splitting: ${splits_deaths_sum}" _newline ///
					"Deaths lost during splitting stage: `split_diff'"
				
				// Observations to have been agesplit only		
				noisily di in red _newline "Observations to have been agesplit only"
				noisily di "# of deaths in original file to be agesplit: ${allfrmats_ageonly_deaths_sum}" _newline ///
					"# of deaths in postsplit file that were agesplit: ${postsplit_ageonly_deaths_sum}" _newline ///
					"Deaths lost during age-splitting: `ageonly_diff'"
				
				// Observations to have been agesexsplit only
				noisily di in red _newline "Observations to have been agesexsplit only"
				noisily di "# of deaths in original file to be agesexsplit: ${allfrmats_agesexonly_deaths_sum}" _newline ///
					"# of deaths in postsplit file that were agesexsplit: ${postsplit_agesexonly_deaths_sum}" _newline ///
					"Deaths lost during agesex-splitting: `agesexonly_diff'"
					
			// Store summary statistics
			outsheet using "$source_dir/$data_name/logs/02_agesexsplitchecks_${data_name}_${timestamp}.csv", comma replace
			noisily display _newline "Saving summary statistics to $source_dir/$data_name/logs/"
		
		restore
	}
	else {
		noisily display "All observations are in the desired age and sex formats.  No splitting required."
	}
	
		// Recalculate deaths2
			egen imtot = rowtotal(deaths91 deaths92 deaths93 deaths94)
			capture gen deaths2 = 0
			replace deaths2 = imtot
			drop imtot	

		if "${data_name}"=="Morocco_Health_In_Figures" {
			replace deaths9=0 if source_label=="deaths_people_15_64"
			order deaths*, last sequential
			drop deaths1
			egen deaths1 = rowtotal(deaths3-deaths94)
			order deaths*, last sequential
		}
			
		// Save dataset
		compress
		collapse(sum) deaths*, by(iso3 location_id year sex acause cause cause_name NID source source_label source_type list national subdiv region dev_status im_frmat frmat) fast
		save "$source_dir/${data_name}/data/intermediate/02_agesexsplit.dta", replace
		save "$source_dir/${data_name}/data/intermediate/_archive/02_agesexsplit_${timestamp}.dta", replace
		
		// Save dataset for compile
		collapse(sum) deaths*, by(iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status im_frmat frmat) fast
		save "$source_dir/${data_name}/data/intermediate/02_agesexsplit_compile.dta", replace
		save "$source_dir/${data_name}/data/intermediate/_archive/02_agesexsplit_compile_${timestamp}.dta", replace
		
		// US counties are not needed after redistribution. Aggregate to state level and save dataset for compile. 
		preserve
			odbc load, exec("SELECT location_id, location_parent_id FROM shared.location WHERE location_parent_id BETWEEN 523 and 573 OR location_parent_id = 385") strConnection clear
			tempfile states
			save `states', replace
		restore

		if inlist(source, "US_NCHS_counties_ICD9", "US_NCHS_counties_ICD10") {
			merge m:1 location_id using `states', keep(3) nogen
			// Aggregate
			fastcollapse deaths*, by(NID acause dev_status frmat im_frmat iso3 list national region sex source source_label source_type subdiv year location_parent_id) type(sum)
			rename location_parent_id location_id
			
			save "$source_dir/${data_name}/data/intermediate/02_agesexsplit_compile_states.dta", replace
			save "$source_dir/${data_name}/data/intermediate/_archive/02_agesexsplit_compile_states_${timestamp}.dta", replace
		}
		
		
		capture log close
