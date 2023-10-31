
// Author: USERNAME
// Modified: USERNAME
// Purpose: Create incidence rates baset on "Gold Standard Data" that will be used to generate weights in age age/sex splitting and acause disaggregation

** **************************************************************************
** Configure and Set Folders
** **************************************************************************
// Clear memory and set memory and variable limits
clear all
set more off

// Load and run set_common function based on the operating system (will load common functions and filepaths relevant for registry intake)

global cancer_repo = "FILEPATH"
global code_prefix = "FILEPATH"
global cancer_storage = "FILEPATH"
global registry_input_storage = "FILEPATH"
global registry_input_code = "FILEPATH"
global prep_process_storage = "FILEPATH"
global common_cancer_data = "FILEPATH"
// Input folder
local registry_inputs = "FILEPATH"
// Output folders
local temp_folder = "FILEPATH"
local code_folder = "FILEPATH"


** ****************************************************************
** GET ADDITIONAL RESOURCES
** ****************************************************************
// Load cause restrictions
	import delimited using "FILEPATH/causes.csv", clear
	keep if refresh == 2
	keep acause male female yll_age_start yll_age_end
	rename yll_* *

	gen gbd_cause = acause
// save
	tempfile cause_restrictions
	save `cause_restrictions', replace


// load ihme_population
    use "$common_cancer_data/populations_03182020.dta", clear 

    // create proportion of population in granular age groups to multiply cases by later
    preserve
    	keep if inlist(age_group_id, 1,2,3,4,5,34,238,388,389)
    	local to_calc = "2 3 4 5 34 238 388 389"

    	foreach num in `to_calc' {
    		di "`num'"
    		// sum population between 0-4 and given age group
    		egen double pop_total = sum(population) if inlist(age_group_id, 1, `num'), by(location_id year_id sex_id)
    		// subtract so we're left with pop[0-4] and pop[age_group]
    		replace pop_total = pop_total - population if age_group_id == 1
    		gen age_group_`num' = .
    		// create age_group proportion 
    		replace age_group_`num' = pop_total / population if age_group_id == 1
    		drop pop_total
    	}
    	
    	keep if age_group_id == 1

    	tempfile granular_props
    	save `granular_props'
    restore

    drop if age_group_id == 1
    append using `granular_props'

    
    // Keep cancer gbd age groups
	keep if inrange(age_group_id, 1, 20) | inlist(age_group_id, 30, 31, 32, 34, 235, 238, 388, 389) 
	// Convert to cancer age groups
	gen age = age_group_id + 1 if age_group_id < 30 
	replace age = 91 if age_group_id == 2
	replace age = 92 if age_group_id == 3
	replace age = 93 if age_group_id == 4
	replace age = 94 if age_group_id == 5
	replace age = 22 if age_group_id == 30 
	replace age = 23 if age_group_id == 31 
	replace age = 24 if age_group_id == 32 
	replace age = age_group_id if age_group_id == 34
	replace age = 25 if age_group_id == 235	
	replace age = age_group_id if age_group_id == 238
	replace age = age_group_id if age_group_id == 388
	replace age = age_group_id if age_group_id == 389
	drop age_group_id

    rename (year_id sex_id) (year_start sex)
    tempfile ihme_pop
    save `ihme_pop', replace

** ********************
** Get Data
** ********************
// // Pool all incidence datasets: Get list of sources in cancer prep folder and append all sets together
	clear

	local loopnum = 1
	foreach folder in "CI5" "NORDCAN" "USA" "IICC" {
		local subfolders: dir "`registry_inputs'/`folder'" dirs "*", respectcase
		foreach subFolder in `subfolders' {
			if substr("`subFolder'", 1, 1) == "_" | substr("`subFolder'", 1, 1) == "0" continue
			if regexm(upper("`subFolder'"), "APPENDIX") continue
			if "`folder'" == "USA" & "`subFolder'" != "usa_seer_1973_2013_inc"  continue  // just SEER data
			if "`folder'" == "CI5" & "`subFolder'" == "CI5_Q280_I" continue

            local data_path = "`registry_inputs'/`folder'/`subFolder'"

            local incidence_file = "`data_path'/03_mapped_inc.dta"
			capture confirm file "`incidence_file'"
			if !_rc {
				noisily di "`subFolder'"
				use "`incidence_file'", clear

				*****
				// We know this dataset has both eye and nmsc subcauses so we will use it to create an eye&nmsc parent cause
				if "`subFolder'" == "CI5_Q661_I" {
					preserve
						keep if inlist(gbd_cause, "neo_eye_rb", "neo_eye_other")

						egen eye_parent = sum(cases), by(registry_index year_start year_end sex_id age)

						drop if gbd_cause == "neo_eye_other"
						replace cause_name = "neo_eye"
						replace gbd_cause = "neo_eye"
						replace acause1 = "neo_eye"

						replace cases = eye_parent
						drop eye_parent

						tempfile eye_parent
						save `eye_parent'
					restore
					append using `eye_parent'

					preserve
						keep if inlist(gbd_cause, "neo_nmsc_bcc", "neo_nmsc_scc")

						egen nmsc_parent = sum(cases), by(registry_index year_start year_end sex_id age)

						drop if gbd_cause == "neo_nmsc_bcc"
						replace cause_name = "neo_nmsc"
						replace gbd_cause = "neo_nmsc"
						replace acause1 = "neo_nmsc"

						replace cases = nmsc_parent
						drop nmsc_parent

						tempfile nmsc_parent
						save `nmsc_parent'
					restore
					append using `nmsc_parent'
				}

				// We know this dataset has all leukemia subcauses so we will use it to create a leukemia parent
				if "`subFolder'" == "NORDCAN_1980_2014" {
					preserve
						keep if inlist(gbd_cause, "neo_leukemia_ll_acute", "neo_leukemia_ll_chronic", "neo_leukemia_ml_acute", "neo_leukemia_ml_chronic", "neo_leukemia_other")

						egen leuk_parent = sum(cases), by(registry_index year_start year_end sex_id age)

						keep if gbd_cause == "neo_leukemia_ll_acute"
						replace cause_name = "neo_leukemia"
						replace gbd_cause = "neo_leukemia"
						replace acause1 = "neo_leukemia"

						replace cases = leuk_parent
						drop leuk_parent

						tempfile leuk_parent
						save `leuk_parent'
					restore
					append using `leuk_parent'
				}

				*****
				
				// Manual fix to both IICC datasets where neo_tissue_sarcoma wasn't being dropped
				drop if gbd_cause == "C46.1"
				// Manual fix to NORDCAN_1980_2014 dataset where one C-Code wasn't dropped
				drop if gbd_cause == "C69"
				// Manual fix to a number of datasets where maternal_other was an observation
				drop if gbd_cause == "maternal_other"
				// Manual dropping of neo_ben_blood since we don't have enough data on other neo_ben_'s to create the parent and don't currently need it
				drop if gbd_cause == "neo_ben_blood"
				
				// One of the IICC datasets had sex_id 3 in it - we create this later
				drop if sex_id == 3

				// THIS NEXT LINE ABOUT INDIA REGISTRY_INDEX SHOULD BE REMOVED AFTER GBD2020 DECOMP STEP3
				// Manual fix to registry_index for two India datasets
				replace registry_index = "163.43890.2" if registry_index == "163.4878.2"
				replace registry_index = "163.43924.3" if registry_index == "163.4880.3"

				rename sex_id sex 
                drop if sex == 9
				rename (frmat im_frmat) (frmat_inc im_frmat_inc)
                drop if inlist(age, 3,4,5,6,26,91,92,93,94)

                split registry_index, gen(loc) parse(.)
                gen location_id = loc2
                replace location_id = loc1 if loc2 == "0" //35437
                destring location_id, replace
                drop if inlist(location_id,332,363,338,339,370,387) // no ihme_pop for these 

				// create dummy ages for 23 (NORDCAN), 24, 25 so we can have pop attached for these
				tempfile temp_save
				save `temp_save'
				keep if age == 23
				replace age = 24 if age == 23
				tempfile ninety_ninety_five
				save `ninety_ninety_five'
				replace age = 25 if age == 24
				append using `ninety_ninety_five'
				save `ninety_ninety_five', replace

				use `temp_save', clear
				keep if "`subFolder'" == "NORDCAN_1980_2014" & age == 22
				replace age = 23 if age == 22
				tempfile eighty_eighty_five
				save `eighty_eighty_five'
				replace age = 24 if age == 23
				tempfile eighty_five_more
				save `eighty_five_more'
				replace age = 25 if age == 24
				append using `eighty_eighty_five'
				append using `eighty_five_more'
				append using `ninety_ninety_five'
				append using `temp_save'

                merge m:1 location_id age sex year_start using `ihme_pop', keep(3) assert(2 3)
                drop _merge
				gen dataset = "`subFolder'"
				if `loopnum' != 1 append using "`temp_folder'/all_mapped_inc_data_test.dta"
				else local loopnum = 0
				save "`temp_folder'/all_mapped_inc_data_test.dta", replace
			}
			else display "MISSING: `subFolder' inc data"
		}
	}

	drop if dataset == "nordcan_1960_2008_inc" // old nordcan data
	drop if inlist(gbd_cause, "neo_lymphoma_other", "neo_lymphoma_burkitt", "neo_lymphoma")

	tempfile all_mapped_inc_data
	save `all_mapped_inc_data', replace

	use `all_mapped_inc_data', clear


// Create cases for the granular age groups from the 0-4 age group
	keep if age == 2
	local gbd_ages = "2 3 4 5 34 238 388 389"

	foreach age in `gbd_ages' {
		di "`age'"
		preserve
			keep if age == 2
			gen double cases_`age' = 1.0
			// Multiply cases my population age proportion created above
			replace cases_`age' = cases * age_group_`age'
			// Recode to cancer age groups
			replace age = 91 if `age' == 2
			replace age = 92 if `age' == 3
			replace age = 93 if `age' == 4
			replace age = 94 if `age' == 5
			replace age = 34 if `age' == 34
			replace age = 238 if `age' == 238
			replace age = 388 if `age' == 388
			replace age = 389 if `age' == 389

			replace cases = cases_`age'
			drop cases_`age'

			tempfile granule_age
			save `granule_age'
		restore
		append using `granule_age'
	}
	// remove existing cases
	drop if age == 2
	append using `all_mapped_inc_data'
	tempfile temp_save_all
	save `temp_save_all'

	// append static lymphoma cases
	// create dummy ages for lymphoma
	use "FILEPATH", replace
	// create dummy ages for 24, 25 so we can have pop attached for these
	tempfile temp_lymph
	save `temp_lymph'
	keep if age == 23
	replace age = 24 if age == 23
	tempfile lymph_old
	save `lymph_old'
	replace age = 25 if age == 24
	append using `lymph_old'
	drop population
	merge m:1 location_id age sex year_start using `ihme_pop', keep(3) assert(2 3)
	drop _merge
	append using `temp_lymph'

	append using `temp_save_all'
	tempfile small_ages_mapped_inc
	save `small_ages_mapped_inc'	



	/*********************************************************************/
// Calculate proportional split of lymphoma:lymphoma_other/hbl from CI data
	/* ONE-OFF dataset of lymphoma cases that is appended above */
	// use `small_ages_mapped_inc', clear
	// // subset to just lymphoma non-parent cases
	// keep if inlist(gbd_cause, "neo_lymphoma_other", "neo_lymphoma_burkitt")
	// // call then lymphoma parent cases
	// replace gbd_cause = "neo_lymphoma"
	// tempfile temp_lym_parent
	// save `temp_lym_parent'

	// use `small_ages_mapped_inc', clear
	// // subset to just lymphoma non-parent cases
	// keep if inlist(gbd_cause, "neo_lymphoma_other", "neo_lymphoma_burkitt")
	// append using `temp_lym_parent'
	// // write static file to be read in for the future
	// save "FILEPATH/lymphoma_replacement.dta", replace
	/*********************************************************************/


** ********************
** Format and Keep data of interest
** ********************
	use `small_ages_mapped_inc', clear

// // Keep only "gold standard" data
    // reshape pop to long (first drop NPCR)
        drop if dataset == "USA_NPCR_1999_2011"
        drop if gbd_cause == "_gc" // remove this line when creating garbage code specific age weights

    // remove cause column and collapse
        collapse (sum) cases (first) pop*, by(age registry_index year_start year_end sex gbd_cause dataset)

	// generate country_id and location_id
		split registry_index, p(.)
		drop registry_index3
		rename (registry_index1 registry_index2) (country_id location_id)
		destring country_id location_id, replace
		replace location_id = country_id if location_id == 0

	// generate an average year to be used for dropping data
		gen year = floor((year_start+year_end)/2)
		gen year_span = 1 + year_end - year_start

	// drop USA data that is not from SEER, since SEER is the most trustworthy
		drop if country_id == 102 & !regexm(lower(dataset), "seer")

	// drop NORDCAN data except for special causes with little data
		drop if regexm(lower(dataset), "nordcan") & regexm(gbd_cause,"neo_meso") & regexm(gbd_cause,"neo_leukemia")

	// more known data than unknown data
		drop if cases == . & age == 1
		drop if cases != 0 & age == 26

	// drop non-CI5 data if it can be obtained from CI5
		egen uid = concat(registry_index year* sex age gbd_cause), punct("_")
		gen is_nord = 1 if regexm(dataset,"NORDCAN") | regexm(dataset,"nordcan")
		bysort uid: egen has_nord = total(is_nord)
        drop if has_nord > 0 & is_nord == .
		drop has_nord is_nord
		gen is_ci5 = 1 if regexm(dataset,"CI5") | regexm(dataset,"ci5")
		bysort uid: egen has_ci5 = total(is_ci5)
		drop if has_ci5 > 0 & is_ci5 == .
		drop is_ci5 has_ci5 uid
		duplicates tag registry_index gbd_cause sex year* age dataset, gen(tag)
		if tag > 0 {
			pause on
			pause duplications exist for registry, cause, sex, year & dataset. Issue needs to be resolved before moving on
		}
		drop tag

	// Drop within-dataset duplications due to multiple year spans
		sort location_id sex registry_index gbd_cause year
		egen uid = concat(location_id sex registry_index gbd_cause year), punct("_")
		duplicates tag uid, gen(duplicate)
		bysort uid: egen smallestSpan = min(year_span)
		drop if duplicate != 0 & year_span != smallestSpan
		drop year year_span

	// save
		tempfile good_format
		save `good_format', replace

** ********************
** Enforce restrictions
** ********************
	use `good_format', clear
    keep registry_index year_start year_end sex age gbd_cause dataset cases country_id location_id pop*

    merge m:1 gbd_cause using `cause_restrictions'
    keep if _merge == 3
    drop _merge

    gen restriction = 0
    // sex restrictions
    replace restriction = 1 if sex == 1 & male == 0
    replace restriction = 1 if sex == 2 & female == 0

    // age restrictions
    replace restriction = 1 if age_end == 5 & !inlist(age,2,7,34,91,92,93,94,238,388) & !inlist(age,389) // If age_ends is 5 but age_group is 10-14 or above
    
    replace restriction = 1 if age_start == 1 & inlist(age, 91,92,93,388,389) //if age_start is 1 but age group is <1
    replace restriction = 1 if age_start == 2 & inlist(age, 91,92,93,238,388,389) 
	replace restriction = 1 if age_start == 5 & inlist(age, 2,34,91,92,93,94,238,388,389)
	replace restriction = 1 if age_start == 10 & (inlist(age, 2,7,34,91,92,93,94,238,388) | inlist(age, 389))
	replace restriction = 1 if age_start == 15 & (inlist(age, 2,7,8,34,91,92,93,94,238) | inlist(age, 388,389))
	replace restriction = 1 if age_start == 20 & (inlist(age, 2,7,8,9,34,91,92,93,94) | inlist(age, 238,388,389))
	// zero out cases that are restricted
    replace cases = 0 if restriction == 1

    drop restriction acause male female age_start age_end

    tempfile restrictions_applied
    save `restrictions_applied'
    


** *************************
** Create 85+y.o. age groups 
** *************************
/* Section to create age groups >85y.o. using proportions of CI's data */
	// This file is created from the format_hospital_weights.py file
	import delimited using "FILEPATH", clear
	drop if sex_id == 3 | acause == "average_cancer"
	keep sex_id acause age counts
	// Keep the ages we wish to calculate
	keep if inlist(age, 22,23,24,25)

	reshape wide counts, i(sex_id acause) j(age)
	// calculate rates of change between age groups
	// sum the denominators for both calculations
	egen double total_80plus = rowtotal(counts22 counts23 counts24 counts25)
	egen double total_85plus = rowtotal(counts23 counts24 counts25)
	// calculate the 80+ aggregate proportions
	gen double change_8084_80plus = counts22/total_80plus
	gen double change_8589_80plus = counts23/total_80plus
	gen double change_9094_80plus = counts24/total_80plus
	gen double change_95plus_80plus = counts25/total_80plus
	// calculate the 85+ aggregate proportions
	gen double change_8589_85plus = counts23/total_85plus
	gen double change_9094_85plus = counts24/total_85plus
	gen double change_95plus_85plus = counts25/total_85plus

	drop counts* total_*
	ren (sex_id acause) (sex gbd_cause)

	tempfile ci_props
	save `ci_props'


	use `restrictions_applied', clear

	keep if inlist(age, 22,23,24,25)
	merge m:1 sex gbd_cause using `ci_props'
	keep if _merge == 3 // there should be <10 rows dropped from the `using' data- things like cervical in males, testicular in females, etc.
	drop _merge
	tempfile pre_elderly_split
	save `pre_elderly_split'

	// First distributed 80+ aggregate cases observed only in the NORDCAN dataset
	use `pre_elderly_split', clear
	keep if dataset == "NORDCAN_1980_2014"

	// saving pop to re-merge with new ages later and dropping created ages 23, 25 and 24
	tempfile temp_save
	save `temp_save'
	keep population registry_index year_start year_end sex age gbd_cause dataset
	sort population registry_index year_start year_end sex age gbd_cause dataset
	quietly by population registry_index year_start year_end sex age gbd_cause dataset:  gen dup = cond(_N==1,0,_n)
	drop if dup>1
	drop dup
	tempfile temp_pop
	save `temp_pop'
	save "`temp_folder'/eighty_plus_pop_data.dta", replace
	use `temp_save'
	drop if inlist(age, 23, 24, 25)

	local all_causes = "neo_bladder neo_bone neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_eye neo_eye_other neo_eye_rb neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_liver_hbl neo_lung neo_lymphoma neo_lymphoma_other neo_lymphoma_burkitt neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_neuro neo_nmsc neo_nmsc_bcc neo_nmsc_scc neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_tissue_sarcoma neo_uterine"
	// Loop over all causes
	foreach cause in `all_causes' {
		preserve
			keep if gbd_cause == "`cause'" & age == 22 // age 22 is the 80+ age group for these causes
			tempfile orig
			save `orig'
			// calculate the non-present age groups first so we don't lose our reference cases
			local ages_to_calc = "23 24 25" // 85-89, 90-94, 95+
			foreach age in `ages_to_calc' {
				use `orig', clear
				if `age' == 23 {
					gen double cases_`age' = cases * change_8589_80plus
				}
				else if `age' == 24 {
					gen double cases_`age' = cases * change_9094_80plus
				}
				else {
					gen double cases_`age' = cases * change_95plus_80plus
				}

				replace cases = cases_`age'
				replace age = `age'
				drop cases_`age'

				tempfile cases_`age'
				save `cases_`age''
			}
			// calculate the final age group
			use `orig', clear
			gen double cases_22 = cases * change_8084_80plus
			replace cases = cases_22
			drop cases_22
			// append everything
			append using `cases_23'
			append using `cases_24'
			append using `cases_25'
			tempfile `cause'_cases
			save ``cause'_cases'
		restore
		drop if gbd_cause == "`cause'" & age == 22
		append using ``cause'_cases'
	}

	save "`temp_folder'/eighty_plus_before_pop_data.dta", replace
	// re-merge population for the new ages
	drop population
	merge m:1 registry_index year_start year_end sex age gbd_cause dataset using `temp_pop', keep(3)
    drop _merge

	save "`temp_folder'/eighty_plus_applied_data.dta", replace
	tempfile nordcan_dist
	save `nordcan_dist'

	// Distribute the 85+ aggregate observed in all other datasets
	use `pre_elderly_split', clear
	keep if dataset != "NORDCAN_1980_2014" & age != 22

	// saving pop to re-merge with new ages later and dropping created ages 25 and 24
	// NOTE: Concern in the future if we do use datasets that had 24, 25 ages originally
	tempfile temp_save
	save `temp_save'
	keep population registry_index year_start year_end sex age gbd_cause dataset
	sort population registry_index year_start year_end sex age gbd_cause dataset
	quietly by population registry_index year_start year_end sex age gbd_cause dataset:  gen dup = cond(_N==1,0,_n)
	drop if dup>1
	tempfile temp_pop
	save `temp_pop'
	save "`temp_folder'/eighty_five_plus_pop_data.dta", replace
	use `temp_save'
	drop if inlist(age, 24, 25)

	local all_causes = "neo_bladder neo_bone neo_brain neo_breast neo_cervical neo_colorectal neo_esophageal neo_eye neo_eye_other neo_eye_rb neo_gallbladder neo_hodgkins neo_kidney neo_larynx neo_leukemia neo_leukemia_ll_acute neo_leukemia_ll_chronic neo_leukemia_ml_acute neo_leukemia_ml_chronic neo_leukemia_other neo_liver neo_liver_hbl neo_lung neo_lymphoma neo_lymphoma_other neo_lymphoma_burkitt neo_melanoma neo_meso neo_mouth neo_myeloma neo_nasopharynx neo_neuro neo_nmsc neo_nmsc_bcc neo_nmsc_scc neo_other_cancer neo_otherpharynx neo_ovarian neo_pancreas neo_prostate neo_stomach neo_testicular neo_thyroid neo_tissue_sarcoma neo_uterine"
	// Loop over each cause
	foreach cause in `all_causes' {
		preserve
			keep if gbd_cause == "`cause'" & age == 23 // age 23 is the 85+ age group for these causes
			tempfile orig
			save `orig'
			// calculate the non-present age groups first so we don't lose our reference cases
			local ages_to_calc = "24 25" // 90-94, 95+
			foreach age in `ages_to_calc' {
				use `orig', clear
				if `age' == 24 {
					gen double cases_`age' = cases * change_9094_85plus
				}
				else {
					gen double cases_`age' = cases * change_95plus_85plus
				}

				replace cases = cases_`age'
				replace age = `age'
				drop cases_`age'

				tempfile cases_`age'
				save `cases_`age''
			}
			// calculate the final age group
			use `orig', clear
			gen double cases_23 = cases * change_8589_85plus
			replace cases = cases_23
			drop cases_23
			// append everything
			append using `cases_24'
			append using `cases_25'
			tempfile `cause'_cases
			save ``cause'_cases'
		restore
		drop if gbd_cause == "`cause'" & age == 23
		append using ``cause'_cases'
	}
	save "`temp_folder'/eighty_five_plus_before_pop_data.dta", replace

	// re-merge population for the new ages
	drop population
	merge m:1 registry_index year_start year_end sex age gbd_cause dataset using `temp_pop', keep(3)
    drop _merge
	save "`temp_folder'/eighty_five_plus_applied_data.dta", replace
	append using `nordcan_dist'
	save "`temp_folder'/elder_data.dta", replace
	tempfile elderly_filled
	save `elderly_filled'

	use `restrictions_applied', clear
	// Remove aggregate age group observations accordingly
	drop if age == 22 & dataset == "NORDCAN_1980_2014"
	drop if age == 23 & dataset != "NORDCAN_1980_2014"

	// Remove dummy ages
	drop if inlist(age, 23, 24, 25) & dataset == "NORDCAN_1980_2014"
	drop if inlist(age, 24, 25)

	append using `elderly_filled'

	drop change*
	save "`temp_folder'/elder_and_all_data.dta", replace
	tempfile all_ages_all_causes
	save `all_ages_all_causes'


** ********************
** Create dataset for "average cancer"
** ********************
	use `all_ages_all_causes', clear
	// Create "average_cancer"
		preserve
			drop if inlist(gbd_cause, "neo_nmsc", "neo_nmsc_bcc", "neo_nmsc_scc") // decision to remove skin cancers from average cancer calculation
			keep location_id registry_index year* sex pop* age
			//keep location_id registry_index year* sex age 
			duplicates drop
			collapse (mean) pop*, by (sex age) // explored other calculations, mean is best here
			tempfile average_cancer_pop
			save `average_cancer_pop', replace
		restore
		preserve
			drop if inlist(gbd_cause, "neo_nmsc", "neo_nmsc_bcc", "neo_nmsc_scc") // decision to remove skin cancers from average cancer calculation
			collapse (mean) cases*, by(sex age gbd_cause) // collapse first including cause so we can ensure parents are >= to children in this calculation
				// make sure the leukemia parent counts are >= to the subcause counts
				egen double leuk_sum = sum(cases) if inlist(gbd_cause, "neo_leukemia", "neo_leukemia_ll_acute", "neo_leukemia_ll_chronic", "neo_leukemia_ml_acute", "neo_leukemia_ml_chronic", "neo_leukemia_other"), by(sex age)
				replace cases = leuk_sum if gbd_cause == "neo_leukemia"
				// make sure the lymphoma parent counts are >= to the subcause counts
				egen double lymph_sum = sum(cases) if inlist(gbd_cause, "neo_lymphoma", "neo_lymphoma_other", "neo_lymphoma_burkitt"), by(sex age)
				replace cases = lymph_sum if gbd_cause == "neo_lymphoma"
				// make sure the eye parent counts are == to the subcause counts (adding back rb since the parent has some additional missed cases)
				egen double eye_sum = sum(cases) if inlist(gbd_cause, "neo_eye", "neo_eye_rb"), by(sex age)
				replace eye_sum = eye_sum - cases if gbd_cause == "neo_eye"
				replace cases = eye_sum if gbd_cause == "neo_eye" & eye_sum != 0
			collapse (mean) cases*, by(sex age) // now collapse by all causes
			merge 1:1 sex age using `average_cancer_pop', nogen
			gen gbd_cause = "average_cancer"
			tempfile average_cancer
			save `average_cancer', replace
		restore

	// Make weights for other cancers
		preserve
			keep location_id registry_index year* sex pop* age
			collapse (mean) pop*, by(location_id registry_index age year* sex)
			tempfile pop_byLocationYear
			save `pop_byLocationYear', replace
		restore
			drop pop*
			merge m:1 location_id age registry_index year* sex using `pop_byLocationYear', nogen
		append using `average_cancer'
		drop if gbd_cause == "_gc"

	// Append, rename, and save
		collapse cases* pop*, by(age sex gbd_cause) fast
		// Again make sure parents are >= to children, this time in the whole dataset
		// make sure the leukemia parent counts are >= to the subcause counts
			egen double leuk_sum = sum(cases) if inlist(gbd_cause, "neo_leukemia", "neo_leukemia_ll_acute", "neo_leukemia_ll_chronic", "neo_leukemia_ml_acute", "neo_leukemia_ml_chronic", "neo_leukemia_other"), by(sex age)
			replace cases = leuk_sum if gbd_cause == "neo_leukemia"
			drop leuk_sum
		// make sure the lymphoma parent counts are >= to the subcause counts
			egen double lymph_sum = sum(cases) if inlist(gbd_cause, "neo_lymphoma", "neo_lymphoma_other", "neo_lymphoma_burkitt"), by(sex age)
			replace cases = lymph_sum if gbd_cause == "neo_lymphoma"
			drop lymph_sum
		// make sure the eye parent counts are == to the subcause counts (adding back rb since the parent has some additional missed cases)
			egen double eye_sum = sum(cases) if inlist(gbd_cause, "neo_eye", "neo_eye_rb"), by(sex age)
			replace eye_sum = eye_sum - cases if gbd_cause == "neo_eye"
			replace cases = eye_sum if gbd_cause == "neo_eye" & eye_sum != 0
			drop eye_sum
		rename gbd_cause acause

	// save
		save "`temp_folder'/gold_standard_mapped_inc_data_test.dta", replace

** ********************
** Create Rates
** ********************
	use "`temp_folder'/gold_standard_mapped_inc_data_test.dta", clear

// Rename and format variables
    rename population pop
	reshape wide cases pop, i(sex acause) j(age)
    drop if acause == ""
	aorder
	keep acause sex cases* pop*
	gen obs = _n
	reshape long cases@ pop@, i(sex acause obs) j(gbd_age)
	rename gbd_age age

// Generate sex = 3 data
	tempfile split_sex
	save `split_sex', replace
	collapse (sum) cases* pop*, by(age acause)
	gen sex = 3
	append using `split_sex'

// ensure non-zero data if data should not be zero
	egen uid = concat(acause sex), p("_")
	sort uid age
	replace cases = ((cases[_n-1]+cases[_n+1])/2) if cases == 0 &  cases[_n-1] > 0 & cases[_n+1] > 0 & uid[_n-1] == uid & uid[_n+1] == uid
	drop uid

// Create Rate
	gen double inc_rate = cases/pop
	save "`temp_folder'/rate_data_with_pops.dta", replace
	drop cases pop obs
	replace inc_rate = 0 if inc_rate == . | inc_rate < 0

// Finalize and Save
	order sex acause
	sort acause sex
	ren (sex inc_rate) (sex_id rate)
	outsheet using "FILEPATH", comma replace
** ****
** END
** ****
