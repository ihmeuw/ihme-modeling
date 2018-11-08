/***********************************************************************************************************																			
 Project: HSA (Child)																		
 Purpose: Clean and integrate report data		

 1. Reports (from google doc)
 2. Survey data from WHO_UNICEF			

 Age split based on coverage ratios for those particular countries/regions if not available				
																				
***********************************************************************************************************/


// Generating agegroup and birthyear for the cohort and agegroup methods

gen agegroup = .
	replace agegroup = 1 if inlist(age, "0-11", "0-12")
	replace agegroup = 2 if inlist(age, "12-23", "11-23", "12-24", "13-24")
	replace agegroup = 3 if inlist(age, "24-35")
	replace agegroup = 4 if inlist(age, "36-47")
	replace agegroup = 5 if inlist(age, "48-59")

	preserve
	keep if agegroup != .
	tempfile goodtogo
	save `goodtogo', replace
	restore	

	// For age ranges greater than 1 year and non traditional age groups, setting the split based on survey data age ratios
	keep if agegroup == .
	levelsof iso3 if agegroup == ., local(iso3_agesplit)
	gen source_id = _n
	merge m:m iso3 using "FILEPATH", keep(3)

		// Reshape long so can match on the entity
		foreach cov in anc1 anc4 sba ifd {
			rename `cov' coverage`cov'
		}

		reshape long coverage, i(source_id) j(cov) string
		keep if coverage != .

		gen source_cov_id = _n
		

	tempfile need_match
	save `need_match', replace
	

	// Going to survey collapse list to find the most recent year of extracted data for that iso3
		local collapse_file: dir "FILEPATH" files "collapse_list*.dta"
			cap local collapse_file = subinstr(`collapse_file', "", "", .) 
		use "FILEPATH", clear
		gen match_year = regexs(0) if regexm(file_name, "[0-9][0-9][0-9][0-9]")
		destring match_year, replace
		merge m:m iso3 using "FILEPATH", keep(3)


			// Reshape long so that can match on covination
			foreach cov in anc1 anc4 sba ifd {
				rename `cov' has_cov`cov'
			}

			reshape long has_cov, i(file_path file_name) j(cov) string
			keep if has_cov != .

			// Getting rid of surveys with weird age coding
			drop if min_age > 20
			drop if max_age < 20
		

		rename file_* match_*
		tempfile match_list
		save `match_list', replace

		// Goign through and cycling iso3, region, superregion to find a survey that matches with the closest year
		
		
		// Finding nearest iso3 in micro data that has survey data


		forvalues threshold = 5(5)10 {
		foreach match_criteria in iso3 gbd_analytical_region_id {
			use `need_match', clear

			// If there are any items that need to be matched
			if _N > 0 {
				use `match_list', clear


				joinby `match_criteria' cov  using `need_match', unmatched(using)


				// If there are any that successfully matched
					if _N > 0 {
						gen match_item = `match_criteria'
					tostring match_item, replace

						// For those that matched, take the difference between match_year and surv_year
						gen diff = abs(match_year - surv_year) if match_name != ""
						
						// By ID, find the minimum difference (closest year)
						bysort source_cov_id: egen min_diff = min(diff) if match_name != ""
						drop _merge				


						// Keep if within threshold years
						keep if min_diff <= `threshold' & diff == min_diff		

						// If there any that are within threshold
						if _N > 0 {

						duplicates drop source_cov_id, force
						keep source_cov_id match_* surv_year
			
						// Merge the matched paths onto the files that need one
						merge 1:m source_cov_id using `need_match', nogen
	
						// Pull out those that have matched and save to a different list
						save `need_match', replace
						keep if match_name != ""
			
						
						if ("`match_criteria'" == "iso3" & "`threshold'" == "5") {
							tempfile done_match
							save `done_match', replace
						}
						else {
							append using `done_match'
							save `done_match', replace	
						}
					
			
						// Refresh those that still need match
						use `need_match', clear
						keep if match_name == ""
						drop match_*
						save `need_match', replace


					} // If any meets threshold
				} // If any matched to criteria
			} // If anything needs to be matched
		} // Criteria loop
		} // Threshold loop


	// Age Splitting
		/*	
			1.) Based on the age range, figures out what to split to
			2.) Loop through source id, opens up the file that was matched on	
			3.) Finds the ratio of the each split group to the age range
			4.) Applies that ratio to report coverage
		*/

	use `done_match', clear
	// Find age ranges to split to
	split age, p("-")
	destring age*, replace

		// New age range
			gen new_min = .
			gen new_max = .
			forvalues agegroups = 1/5 {
				replace new_min = 12*`agegroups'-12 if inrange(age1, 12*`agegroups'-12, 12*`agegroups'-1)
				replace new_max = 12*`agegroups'-1 if inrange(age2, 12*`agegroups'-12, 12*`agegroups'-1)			
			}
				// If new max > 59, set it to 59
				replace new_max = 59 if age2 > 59
		// Create flags for what to split on
			forvalues agegroups = 1/5 {
				gen agegroup`agegroups' = .
				replace agegroup`agegroups' = 1 if new_min <= 12*`agegroups' - 12
			}
			forvalues agegroups = 1/5 {
				replace agegroup`agegroups' = . if new_max < 12*`agegroups' - 1
			}
	save `done_match', replace

	levelsof source_cov_id, local(split_loop)
	foreach x in `split_loop' {
		use `done_match', clear
		keep if source_cov_id == `x'
		foreach var of varlist * {
			local `var' = `var'
		}

		tempfile adjust_temp
		save `adjust_temp'

		// Open File
		insheet using "`match_path'/`match_name'", clear

		quietly: inspect strata
		if r(N_unique) == 0 {
			quietly: svyset [pweight = pweight]
		}
		else {
			quietly: svyset psu [pweight=pweight], strata(strata)
		}
		
		// Doublecheck age groups for covines
		forvalues flag = 1/5 {
			quietly: summ `cov' if inrange(age_mo, 12*`flag'-12, 12*`flag'-1)
			if r(N) == 0 {
				local agegroup`flag' = .
			}
		}

		// If flag is 1
		forvalues flag = 1/5 {
		if `agegroup`flag'' == 1 {
		
			// Calculate the ratio of report age group/flag age group for the covine

			quietly: svy linearized, subpop(if inrange(age_mo, `age1', `age2')): mean `cov' 
			matrix cov_mean = e(b)
			local cov_reportagegroup = cov_mean[1,1]

			quietly: svy linearized, subpop(if inrange(age_mo, 12*`flag'-12, 12*`flag'-1)): mean `cov'
			matrix cov_mean = e(b)
			local cov_flagagegroup = cov_mean[1,1]
		
			local `cov'_`flag'_ratio = `cov_flagagegroup'/`cov_reportagegroup'	
				
			}
			}

		// Bring back report data
		use `adjust_temp', clear
			// Replace agegroupflags with the adjusted coverage
			forvalues flag = 1/5 {
			if `agegroup`flag'' == 1 {
				gen `cov'`flag' = coverage*``cov'_`flag'_ratio'
				
			}
			}

		// Append to export file
		if `x' == 1 {
			tempfile output
			save `output', replace
		}
		else {
			append using `output'
			save `output', replace
		}
		
	}

