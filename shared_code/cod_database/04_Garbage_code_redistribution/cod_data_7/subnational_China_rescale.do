// *********************************************************************************************************************************************************************
// Purpose:		Rescale China data from strata to province

** **************************************************************************
** CONFIGURATION
** **************************************************************************
	** ****************************************************************
	** INITIAL SAVE OF DATA
	** ****************************************************************
		compress
		tempfile master_compile_data
		save `master_compile_data', replace
	

	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application preferences.  The local applications
	**	preferences include memory allocation, variables limits, color scheme,
	**	defining the J drive (data), and setting a local for the date.
	**
	** ****************************************************************
		// Set application preferences
			// Clear memory and set memory and variable limits
				clear all
				set mem 15G

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
			
	** ****************************************************************
	** GET ADDITIONAL RESOURCES
	** ****************************************************************
		// in/out of hospital proportions
			// Get proportions
				use "$prefix/WORK/03_cod/01_database/03_datasets/China_2004_2012/data/raw/China/in_out_hospital_strata_proportions_2013_12_19.dta", clear
			// Convert age numbers to match CoD format
				// Under 1 ages
					foreach n of numlist 91 93/94 {
						gen wgt`n' = fr0
					}
					drop fr0
				// Ages 1 - 4
					rename fr1 wgt3
				// All other ages
					forvalues i = 5(5)80 {
						local j = (`i'/5)+6
						gen wgt`j' = fr`i'
						drop fr`i'
					}
				drop frtotal
			// Get sum of weights
				foreach n of numlist 3 7/22 91 93/94 {
					egen double total_wgt`n' = total(wgt`n'), by(provid strata sex)
				}
			// Save
				compress
				tempfile hospital_weights
				save `hospital_weights', replace
			
		// Population by strata
			// Get strata codes for China sites
				use "$prefix/WORK/03_cod/01_database/03_datasets/China_2004_2012/data/raw/China/strata.dta", clear
				keep code2010 provid strata
				rename code2010 code
				// Drop blank provinces
					drop if provid == . | strata == .
				tempfile china_geos
				save `china_geos', replace
				use "$prefix/WORK/03_cod/01_database/03_datasets/China_2004_2012/data/raw/China/2013-2014/strata_added.dta", clear
				tostring code, replace
				keep if substr(code, -4,4) != "0000" & substr(code, -2,2) != "00"
				// Assign strata 2 (urban) to the 6 counties with missing strata
				replace strata_added = 2 if strata == .
				rename strata_added strata
				destring code, replace		
				append using `china_geos'
				tempfile china_geos
				save `china_geos', replace
				
					// Get data
						do "$prefix/WORK/03_cod/01_database/02_programs/prep/code/env_long.do"
						keep if location_id >= 491 & location_id <= 521
					// Reformat
						keep env pop year age sex location_id
						tostring(age), replace format("%12.2f") force
						destring(age), replace
					// Merge on province id
						gen provid = .
						replace provid = 11 if location_id == 492
						replace provid = 12 if location_id == 517
						replace provid = 13 if location_id == 500
						replace provid = 14 if location_id == 515
						replace provid = 15 if location_id == 505
						replace provid = 21 if location_id == 509
						replace provid = 22 if location_id == 508
						replace provid = 23 if location_id == 501
						replace provid = 31 if location_id == 514
						replace provid = 32 if location_id == 506
						replace provid = 33 if location_id == 521
						replace provid = 34 if location_id == 491
						replace provid = 35 if location_id == 494
						replace provid = 36 if location_id == 507
						replace provid = 37 if location_id == 513
						replace provid = 41 if location_id == 502
						replace provid = 42 if location_id == 503
						replace provid = 43 if location_id == 504
						replace provid = 44 if location_id == 496
						replace provid = 45 if location_id == 497
						replace provid = 46 if location_id == 499
						replace provid = 50 if location_id == 493
						replace provid = 51 if location_id == 516
						replace provid = 52 if location_id == 498
						replace provid = 53 if location_id == 520
						replace provid = 54 if location_id == 518
						replace provid = 61 if location_id == 512
						replace provid = 62 if location_id == 495
						replace provid = 63 if location_id == 511
						replace provid = 64 if location_id == 510
						replace provid = 65 if location_id == 519
						
					preserve
						// Keep only the data that we need
							drop env
							keep if age < 1
							drop location_id 
							** drop provname
						// Generate population proportion for each under 1 age group by province year sex
							egen prop = pc(pop), prop by(provid year sex)
							drop pop
						// Reshape
							gen age_string = string(age)
							replace age_string = "0_6days" if age == 0
							replace age_string = "7_28days" if age == .01
							replace age_string = "29_365days" if age == .1
							drop age
							reshape wide prop, i(provid year sex) j(age_string) s
						// Save
							compress
							tempfile province_under_1_prop
							save `province_under_1_prop', replace
					restore
					preserve
						// Keep only the data that we need
							drop env
							keep if age <= 1
							drop location_id 
						// Generate population proportion for each under 5 age group by province year sex
							egen prop = pc(pop), prop by(provid year sex)
							drop pop
						// Reshape
							gen age_string = string(age)
							replace age_string = "0_6days" if age == 0
							replace age_string = "7_28days" if age == .01
							replace age_string = "29_365days" if age == .1
							replace age_string = "1" if age == 1
							drop age
							reshape wide prop, i(provid year sex) j(age_string) s
						// Save
							compress
							tempfile province_under_5_prop
							save `province_under_5_prop', replace
					restore
					// Convert ages to GBD ages
						drop if age > 80
						gen gbd_age = .
						replace gbd_age = 91 if age == 0
						replace gbd_age = 93 if age == .01
						replace gbd_age = 94 if age == .1
						replace gbd_age = 3 if age == 1
						forvalues i = 5(5)80 {
							local j = (`i'/5)+6
							replace gbd_age = `j' if age == `i'
						}
					// Reshape age wide
						keep provid gbd_age env pop sex year
						reshape wide env pop, i(provid year sex) j(gbd_age)
					// Save
						compress
						tempfile province_envelope
						save `province_envelope', replace
			// Get strata population tables
				insheet using "$prefix/Project/Mortality/CHN_county/pop_interpolation/1990_2000/results/chn_county_pop_interpolated_90_00.csv", comma names clear
				foreach var of varlist pm* pf* {
					capture replace `var' = "0" if `var' == "NA" | `var' == "Inf"
					destring(`var'), replace
				}
				// There is overlap within 2000 between the two data sets
					drop if year == 2000
				tempfile chn_interpolated_pop
				save `chn_interpolated_pop', replace
				insheet using "$prefix/Project/Mortality/CHN_county/pop_interpolation/2000_2010/results/chn_county_pop_interpolated_00_10.csv", comma names clear
				foreach var of varlist pm* pf* {
					capture replace `var' = "0" if `var' == "NA" |`var' == "Inf"
					destring(`var'), replace
				}
				// There are duplicates in this data, drop them
					duplicates drop
				append using `chn_interpolated_pop'
			// Only keep the overlapping
				merge m:1 code using `china_geos', keep(3) keepusing(code provid strata) nogen
			// Make sure there are no null values
				foreach var of varlist pm* pf* {
					replace `var' = 0 if `var' == .
				}
			// Make everything 80+
				replace pm80 = pm80 + pm85
				replace pf80 = pf80 + pf85
				drop pm85 pf85
			// Collapse to strata level
				collapse (sum) pm* pf*, by(year provid strata) fast
			// Make sex long
				rename provid chn_provid
				reshape long p@, i(year chn_provid strata) j(agesex) string
				gen sex = substr(agesex,1,1)
				replace sex = "1" if sex == "m"
				replace sex = "2" if sex == "f"
				destring sex, replace
				gen age = substr(agesex,-1*(length(agesex)-1),.)
				drop agesex
				reshape wide p, i(chn_provid strata year sex) j(age) s
				rename chn_provid provid
			// Split apart under 1 age group based on province weights for 2000+
				// Merge on under 1 proportions
					merge m:1 provid year sex using `province_under_1_prop', keep(1 3) assert(2 3) nogen
				// Generate new under 1 age groups
					foreach i in 0_6days 7_28days 29_365days {
						display "Generating new envelopes for `i'"
						gen p`i' = prop`i' * p0 if year >= 2000
						drop prop`i'
					}
			// Split apart under 5 age group based on province weights for <2000
				// Merge on under 1 proportions
					merge m:1 provid year sex using `province_under_5_prop', keep(1 3) assert(2 3) nogen
				// Generate new under 1 age groups
					foreach i in 0_6days 7_28days 29_365days 1 {
						display "Generating new envelopes for `i'"
						replace p`i' = prop`i' * p0 if year < 2000
						replace p`i' = 0 if p`i' == .
					}
					drop prop* p0
			// Make proportions (by province and year)
				foreach n in 0_6days 7_28days 29_365days {
					display "Making proportions for `n'"
					egen wgt_`n' = pc(p`n'), prop by(provid year sex)
					rename p`n' pop_`n'
				}
				foreach n of numlist 1 5(5)80 {
					display "Making proportions for `n'"
					egen wgt_`n' = pc(p`n'), prop by(provid year sex)
					rename p`n' pop_`n'
				}
			// Convert age numbers to match CoD format
				// Under 1 ages
					rename pop_0_6days pop91
					rename wgt_0_6days wgt91
					rename pop_7_28days pop93
					rename wgt_7_28days wgt93
					rename pop_29_365days pop94
					rename wgt_29_365days wgt94
				// Ages 1 - 4
					rename wgt_1 wgt3
					rename pop_1 pop3
				// All other ages
					forvalues i = 5(5)80 {
						local j = (`i'/5)+6
						rename wgt_`i' wgt`j'
						rename pop_`i' pop`j'
					}
			// Get sum of weights
				foreach n of numlist 3 7/22 91 93/94 {
					egen double total_wgt`n' = total(wgt`n'), by(provid year sex)
				}
			// Save
				compress
				tempfile strata_weights
				save `strata_weights', replace
				
		// Make province envelope
			collapse(sum) pop*, by(prov year sex) fast
			tempfile province_pop
			save `province_pop', replace

				
** **************************************************************************
** RUN PROGRAM
** **************************************************************************
	// Get data
		use `master_compile_data', clear
		
	// Pull out provid, strata, and hospdead from subdiv
		gen subdiv_split = subdiv
		replace subdiv_split = subinstr(subinstr(subinstr(subinstr(subdiv_split,"Hosp",",",.),"Prov","",.),"Strata",",",.)," ","",.)
		split subdiv_split, parse(",")
		drop subdiv_split
	// Rename variables
		rename subdiv_split1 provid
		rename subdiv_split2 strata
		capture gen subdiv_split3 = ""
		rename subdiv_split3 hospdead
	// Convert to provid, strata, and hospdead to numbers
		foreach var of varlist provid strata hospdead {
			destring(`var'), replace
		}
	// Rename subdiv to be blank
		replace subdiv = ""
		
	// Figure out max beforeafter
		egen max_beforeafter = max(beforeafter), by(iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status provid)
		replace beforeafter = max_beforeafter
		drop max_beforeafter
		
	// Weight so that in and out of hospital can be combined
		// Make mortality rate (divide each cause by population of strata)
			// Merge on strata populations
				merge m:1 provid strata year sex using `strata_weights', keep(1 3) assert(2 3) keepusing(pop*) nogen
			// Make rates by dividing the deaths by the strata population
				foreach var in deaths_raw deaths_corr deaths_rd {
					foreach n of numlist 3 7/22 91 93/94 {
						display "Making death rates for `var'`n'"
						gen double `var'_rate`n' = `var'`n' / pop`n'
					}
				}
			// preserve sample sizes
			foreach var in raw rd corr {
				foreach n of numlist 3 7/22 91 93/94 {
					display "Making sample size `var'_sample`n'"
					bysort provid strata year sex: egen double sample_`var'`n' = total(deaths_`var'`n')
				}
			}
		// Multiply by in/out of hospital proportions
			merge m:1 provid strata hospdead sex using `hospital_weights', keep(1 3)
			count if _merge == 1 & year >= 2008 | _merge == 3 & year <= 2007
			if `r(N)' > 0 {
				display in red "ERROR: THERE ARE EITHER (1) HOSPITAL PROPORTIONS FOR YEARS WHERE WE DON'T HAVE HOSPITAL STRATIFICATION AND/OR ///
				(2) MISSING HOSPITAL WEIGHTS FOR YEARS WHERE WE HAVE HOSPITAL STRATIFICATION"
				BREAK
			}
			foreach var in deaths_raw deaths_rd deaths_corr {
				foreach n of numlist 3 7/22 91 93/94 {
					display "Weighting `var'_rate`n'"
					replace `var'_rate`n' = `var'_rate`n' * wgt`n' if _merge == 3
				}
			}
		// Add rates together
			collapse (sum) deaths_raw_rate* deaths_rd_rate* deaths_corr_rate*, by(iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status provid strata total_wgt* *sample* beforeafter) fast
		// Divide by the sum of the weights
			foreach var in deaths_raw deaths_rd deaths_corr {
				foreach n of numlist 3 7/22 91 93/94 {
					display "Dividing by the sum of the hospital weights for `var'`n'"
					replace total_wgt`n' = 1 if total_wgt`n' == .
					replace `var'_rate`n' = `var'_rate`n' / total_wgt`n'
				}
			}
			drop total_wgt*
		// Convert back to deaths
				merge m:1 provid strata year sex using `strata_weights', keep(1 3) assert(2 3) keepusing(pop*) nogen
			// Make rates by dividing the deaths by the strata population
				foreach var in deaths_raw deaths_corr deaths_rd {
					foreach n of numlist 3 7/22 91 93/94 {
						display "Making death rates into deaths for `var'`n'"
						gen double `var'`n' = `var'_rate`n' * pop`n'
					}
				}
			drop *rate*
			
		// Make into cause fractions and scale to original sample size
			foreach var in raw rd corr {
				foreach n of numlist 3 7/22 91 93/94 {
					display "Scaling deaths back to their original sample size in `var'`n'"
					bysort prov year sex strata: egen double cf_`var'`n' = pc(deaths_`var'`n'), prop
					replace deaths_`var'`n' = cf_`var'`n' * sample_`var'`n'
				}
			}
			drop sample* cf*

		// Make into rates for strata combination
			foreach var in deaths_raw deaths_corr deaths_rd {
				foreach n of numlist 3 7/22 91 93/94 {
					display "Making death rates for `var'`n'"
					gen double `var'_rate`n' = `var'`n' / pop`n'
					replace `var'_rate`n' = 0 if `var'_rate`n' == .
				}
			}
		
	// Weight so that strata can be combined
		// Multiply by population proportions for strata
			merge m:1 provid strata sex year using `strata_weights', keep(1 3) keepusing(wgt* total_wgt*) nogen
			foreach var in deaths_raw deaths_rd deaths_corr {
				foreach n of numlist 3 7/22 91 93/94 {
					display "Weighting `var'_rate`n'"
					replace `var'_rate`n' = `var'_rate`n' * wgt`n'
				}
			}
		// Add rates together
			collapse (sum) deaths_raw_rate* deaths_rd_rate* deaths_corr_rate*, by(iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status provid total_wgt* beforeafter) fast
		// Divide by the sum of the weights
			foreach var in deaths_raw deaths_rd deaths_corr {
				foreach n of numlist 3 7/22 91 93/94 {
					display "Dividing by the sum of the strata weights for `var'`n'"
					replace total_wgt`n' = 1 if total_wgt`n' == .
					replace `var'_rate`n' = `var'_rate`n' / total_wgt`n'
				}
			}
			drop total_wgt*
	// Multiply by rate by population
		// Merge on province envelope
			merge m:1 provid year sex using `province_pop', keep(1 3) assert(2 3) keepusing(pop*) nogen
		// Multiply by pop to get number
			foreach var in deaths_raw deaths_rd deaths_corr {
				foreach n of numlist 3 7/22 91 93/94 {
					display "Multiplying `var'_rate`n' by pop to get `var'`n' numbers"
					gen `var'`n' = `var'_rate`n' * pop`n'
					drop `var'_rate`n'
				}
			}
			drop pop*
			
	// Recalculate deaths1
		foreach var in deaths_raw deaths_rd deaths_corr {
			egen double `var'1 = rowtotal(`var'*)
		}
	// Recalculate deaths2
		aorder
		foreach var in deaths_raw deaths_rd deaths_corr {
			egen double `var'2 = rowtotal(`var'91-`var'94)
		}
		
	// Make sure all variables that we need exist
		foreach var in deaths_raw deaths_rd deaths_corr cf_raw cf_rd cf_corr {
			foreach i of numlist 1/26 91/94 {
				capture gen `var'`i' = 0
				replace `var'`i' = 0 if `var'`i' == .
			}
		}
	
	// Reorder
		order iso3 location_id year sex acause NID source source_label source_type list national subdiv region dev_status beforeafter deaths_raw* deaths_rd* deaths_corr* cf_raw* cf_rd* cf_corr*
	
		

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************