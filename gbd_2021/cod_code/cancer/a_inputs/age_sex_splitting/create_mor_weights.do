** *****************************************************************************	
** Name of Script: create_mor_weights.do												
** Description:	Create mortality rates that will be used to generate weights in 
**		age/sex splitting and acause disaggregation
** Contributors: USERNAME
** *****************************************************************************	
** Configure and Set Folders
** *****************************************************************************
// Clear memory and set memory and variable limits
	clear all
	set more off

// Input folder
	local input_folder = "FILEPATH"
	global cancer_storage = "FILEPATH"
	global common_cancer_data = "FILEPATH"
	global code_prefix = "FILEPATH"
	global registry_input_storage = "FILEPATH"

** *****************************************************************************
** Get additional resources
** *****************************************************************************
// Load map of cancer causes
	import delimited using "FILEPATH/causes.csv", clear
	keep if refresh == 2
	keep acause cause_id male female yll_age_start yll_age_end
	tempfile cancer_causes
	save `cancer_causes', replace

** *****************************************************************************
** Format CoD data to create cancer rates
** *****************************************************************************

************************
** NEW SECTION AS OF GBD 2020 DECOMP 3 TO USE COD'S WEIGHTS
	run "FILEPATH/load_cod_database_table.do" distribution_cause_age_sex
	keep cause_id age_group_id sex_id weight
	ren age_group_id gbd_age_group_id

	// Drop neo_other_benign, garbage code, neo_ben_blood, neo_ben_other, 
	drop if inlist(cause_id, 490, 743, 964, 967)

// copy in parent/child causes so we have full distribution before we create average_cancer and both_sex
	// leukemias - neo_leukemia to: neo_leukemia_ll_acute, ll_chronic, ml_acute, ml_chronic, other
	local causes = "845 846 847 848 943"
	foreach cause in `causes' {
		di "`cause'"
		preserve
			keep if cause_id == 487 // neo_leukemia
			// Update acause
			replace cause_id = `cause'
			// Manually set <20y.o. age-restricted weights for neo_leukemia_ll_chronic to 0
			replace weight = 0 if cause_id == 846 & (inlist(gbd_age_group_id, 1,2,3,4,5,6,7,8) | inlist(gbd_age_group_id, 34, 238, 388, 389))

			tempfile leuk_cause
			save `leuk_cause'
		restore

		append using `leuk_cause'
	}

	// livers - neo_liver to: liver_hbl
	local causes = "1005" // liver_hbl
	foreach cause in `causes' {
		di "`cause'"
		preserve
			keep if cause_id == 417 // neo_liver
			//update cause_id
			replace cause_id = `cause'
			// Manually set >5y.o age-restricted weights for neo_liver_hbl to 0
			replace weight = 0 if cause_id == 1005 & (inlist(gbd_age_group_id, 7,8,9,10,11,12,13,14) | inlist(gbd_age_group_id, 15,16,17,18,19,20,30,31,32) | inlist(gbd_age_group_id, 235))

			tempfile liver_cause
			save `liver_cause'
		restore

		append using `liver_cause'
	}

	// eyes - neo_eye to: neo_eye_other, neo_eye_rb
	local causes = "1009 1010"
	foreach cause in `causes' {
		di "`cause'"
		preserve
			keep if cause_id == 1008 // neo_eye
			// update cause_id
			replace cause_id = `cause'
			// Manually set >5y.o age-restricted weights for neo_eye_rb to 0
			replace weight = 0 if cause_id == 1009 & (inlist(gbd_age_group_id, 7,8,9,10,11,12,13,14) | inlist(gbd_age_group_id, 15,16,17,18,19,20,30,31,32) | inlist(gbd_age_group_id, 235))
			// Manually set <10y.o. age-restricted weights for neo_eye_other to 0
			replace weight = 0 if cause_id == 1010 & (inlist(gbd_age_group_id, 1,2,3,4,5,6) | inlist(gbd_age_group_id, 34, 238, 388, 389))

			tempfile eye_cause
			save `eye_cause'
		restore

		append using `eye_cause'
	}
	************************

	merge m:1 cause_id using `cancer_causes', keep(3) nogen

	************************
// NEW SECTION TO AGGREGATE <5 GRANULAR AGES INTO 0-4 AGE GROUP
	tempfile all_causes_present
	save `all_causes_present'

	// NOTE: THIS FILE NEEDS TO BE UPDATED EVERY NEW DECOMP STEP WITH UPDATED POPULATION
	import delimited using "FILEPATH/young_pops.csv", clear
	ren age_group_id gbd_age_group_id
	// merge population
	merge 1:m gbd_age_group_id sex_id using `all_causes_present'
	keep if _merge == 3
	drop _merge
	// multiply by population to get count
	gen deaths = weight * population
	// update age group
	replace gbd_age_group_id = 1

	// Aggregate ages 0-4 if yll_age_start = 0
	preserve
		keep if yll_age_start == 0
		collapse (sum) deaths population, by(gbd_age_group_id sex_id cause_id acause male female yll_age_start yll_age_end)

		tempfile all_young
		save `all_young'
	restore
	// aggregate ages 0-4 if yll_age_start = 1
	preserve
		keep if yll_age_start == 1
		collapse (sum) deaths population, by(gbd_age_group_id sex_id cause_id acause male female yll_age_start yll_age_end)

		tempfile non_neonatal
		save `non_neonatal'
	restore
	// aggregate ages 0-4 if yll_age_start = 2
	preserve
		keep if yll_age_start == 2
		collapse (sum) deaths population, by(gbd_age_group_id sex_id cause_id acause male female yll_age_start yll_age_end)

		tempfile above_two
		save `above_two'
	restore

	// Remove duplicates & append aggregates
	drop if yll_age_start <= 2
	append using `all_young'
	append using `non_neonatal'
	append using `above_two'

	// Recalculate weight & drop unneeded cols
	replace weight = deaths / population if weight == .
	drop population deaths

	tempfile young_age_aggregated
	save `young_age_aggregated'

	// Load all causes, append 0-4 aggregate
	use `all_causes_present', clear
	append using `young_age_aggregated'

	tempfile all_causes_present
	save `all_causes_present'

	************************

	************************
	// NEW SECTION TO CREATE POST NEONATAL AGE GROUP [28-364 days] = 388+389 (1-5 and 6-11 months)
	// NOTE: THIS FILE NEEDS TO BE UPDATED EVERY NEW DECOMP STEP WITH UPDATED POPULATION.
	import delimited using "FILEPATH/young_pops.csv", clear
	ren age_group_id gbd_age_group_id
	keep if inlist(gbd_age_group_id, 388,389)
	// merge population
	merge 1:m gbd_age_group_id sex_id using `all_causes_present'
	keep if _merge == 3
	drop _merge
	// multiply by population to get count
	gen deaths = weight * population
	// update age group
	replace gbd_age_group_id = 4

	preserve
		keep if yll_age_start == 0
		collapse (sum) deaths population, by(gbd_age_group_id sex_id cause_id acause male female yll_age_start yll_age_end)

		tempfile pn
		save `pn'
	restore

	drop if yll_age_start == 0
	append using `pn'

	// Recalculate weight & drop unneeded cols
	replace weight = deaths / population if weight == .
	drop population deaths

	tempfile pn_generated
	save `pn_generated'

	// Load all causes, append PN aggregate
	use `all_causes_present', clear
	append using `pn_generated'

	tempfile all_causes_present
	save `all_causes_present'
	************************

	************************
	// NEW SECTION TO CREATE 1-4 AGE GROUP = 34+238 (12-23mo and 2-4)
	// NOTE: THIS FILE NEEDS TO BE UPDATED EVERY NEW DECOMP STEP WITH UPDATED POPULATION.
	import delimited using "$FILEPATH/young_pops.csv", clear
	ren age_group_id gbd_age_group_id
	keep if inlist(gbd_age_group_id, 34,238)
	// merge population
	merge 1:m gbd_age_group_id sex_id using `all_causes_present'
	keep if _merge == 3
	drop _merge
	// multiply by population to get count
	gen deaths = weight * population
	// update age group
	replace gbd_age_group_id = 5

	preserve
		keep if inlist(yll_age_start, 0,1,2)
		collapse (sum) deaths population, by(gbd_age_group_id sex_id cause_id acause male female yll_age_start yll_age_end)

		tempfile non_zero
		save `non_zero'
	restore

	drop if inlist(yll_age_start, 0,1,2)
	append using `non_zero'

	// Recalculate weight & drop unneeded cols
	replace weight = deaths / population if weight == .
	drop population deaths

	tempfile one_to_four
	save `one_to_four'

	// Load all causes, append 1-4 aggregate
	use `all_causes_present', clear
	append using `one_to_four'
	************************

// verify that restrictions are correctly entered
	foreach v of varlist yll_age_end yll_age_start male female {
		capture count if `v' == .
		if r(N){
			noisily di "ERROR: `v' has missing values. These must be replaced before continuing"
			BREAK
		}
	}

// add "average cancer" cause
	preserve
		collapse (mean) weight, by(sex_id gbd_age_group_id)
		gen yll_age_start = 0
		gen yll_age_end = 100
		gen male = 1
		gen female = 1
		gen acause = "average_cancer"
		tempfile average_cancer
		save `average_cancer', replace
	restore
	append using `average_cancer'
	
// Make weights for sex = 3
	preserve
		collapse (sum) weight female male (mean) yll_age_start yll_age_end, by(acause gbd_age_group_id)
		replace female = 1 if female > 1
		replace male = 1 if male > 1
		gen sex_id = 3
		tempfile sex3
		save `sex3', replace
	restore
	append using `sex3'		

// combine like- data
	collapse (sum) weight, by(sex_id acause yll_age_start yll_age_end male female gbd_age_group_id)

	tempfile pre_rates
	save `pre_rates'

// create rates - as of GBD2020 data is already in rate space from CoD
    tempfile rates_created
    save `rates_created'

    // Merge cancer age groups
	import delimited "FILEPATH/cancer_age_groups.csv", clear
	drop cancer_age_conversion_id start_age end_age
	ren (cancer_age_id gbd_age_id) (age_group_id gbd_age_group_id)

	merge 1:m gbd_age_group_id using `rates_created'
	keep if _merge == 3
	drop _merge
	ren weight rate
  
// // Drop data that is unusable per restrictions
    // create marker for restriction violations
        gen is_restriction_violation = 0

    // mark restricted sexes
        replace is_restriction_violation = 1 if sex_id == 1 & male == 0
        replace is_restriction_violation = 1 if sex_id == 2 & female == 0

    // mark restricted ages.
        replace is_restriction_violation = 1 if yll_age_start == 1 & inlist(age_group_id, 91,92,93,388,389) // if age start is 1y.o. but neonatal ages are present
    	replace is_restriction_violation = 1 if yll_age_start == 2 & inlist(age_group_id, 91,92,93,238,388,389) // if age start is 2y.o. but neonatal and <1y.o. ages are present

        replace is_restriction_violation = 1 if yll_age_start == 5 & (age_group_id < 7 | inlist(age_group_id, 34,91,92,93,94,238,388,389)) // if age start is 5y.o. but <5y.o. ages present
		replace is_restriction_violation = 1 if yll_age_start == 10 & (age_group_id < 8 | inlist(age_group_id, 34,91,92,93,94,238,388,389)) // if age start is 10y.o. but <10y.o. ages present
		replace is_restriction_violation = 1 if yll_age_start == 15 & (age_group_id < 9 | inlist(age_group_id, 34,91,92,93,94,238,388,389)) // if age start is 15y.o. but <15y.o. ages present
		replace is_restriction_violation = 1 if yll_age_start == 20 & (age_group_id < 10 | inlist(age_group_id, 34,91,92,93,94,238,388,389)) // if age start is 20y.o. but <20y.o. ages present
		replace is_restriction_violation = 1 if yll_age_end == 5 & (age_group_id > 7 & age_group_id < 34) // if age end is 5y.o. but >5y.o. ages present
        replace is_restriction_violation = 1 if (yll_age_start == 1 | yll_age_start == 0) & age_group_id <= 4 & rate == 0
   
    // replace rates of restriction violoations with 0 
        replace rate = 0 if is_restriction_violation == 1

    // ensure that non-zero rates are present for every entry that is not a restriction violation 
    	capture count if inlist(rate, 0 , .) & is_restriction_violation == 0 // there shouldn't be any, br * if rate == 0 & is_restriction_violation == 0
    	assert !r(N)

    // remove marker for restriction violations 
    	drop is_restriction_violation

// keep relevant information and reshape for use in cancer prep 
	keep sex_id acause rate age_group_id
	rename rate death_rate

// sort and save
	order sex_id acause
	sort acause sex_id age_group_id
	rename (age_group_id death_rate) (age rate)
	 export delimited "FILEPATH", replace 
	 
** *****************************************************************************
** END
** *****************************************************************************
