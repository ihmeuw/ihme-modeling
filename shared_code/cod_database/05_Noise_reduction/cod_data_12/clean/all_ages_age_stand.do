** ************************************************************************************************* **
** Purpose: Sum datasets with all ages present to all ages and age-standardized rate
** ************************************************************************************************* **
	if c(os) == "Unix" {
		set mem 10G
		set odbcmgr unixodbc
		global prefix "/home/j"
	}
	else if c(os) == "Windows" {
		set mem 800m
		global prefix "J:"
	}
	
	// Set up IHME fastcollapse function
		do "$prefix/WORK/04_epi/01_database/01_code/04_models/prod/fastcollapse.ado"
		
	// Set directories & files
		preserve
			odbc load, exec("SELECT ROUND(age_group_years_start,2) AS age, age_group_weight_value AS weight FROM shared.age_group_weight JOIN shared.age_group USING (age_group_id) JOIN shared.gbd_round USING (gbd_round_id) WHERE (age_group_id BETWEEN 2 AND 21) AND gbd_round = 2015") strConnection clear
			replace age = 91 if age == 0
			replace age = 93 if age == 0.02
			replace age = 94 if age == 0.08
			tempfile age_weights
			save `age_weights', replace
		restore
	// Determine "all age" ranges
		preserve
			** Get age_group_ids
			odbc load, exec("SELECT ROUND(age_group_years_start,2) AS age, age_group_id FROM shared.age_group WHERE (age_group_id BETWEEN 2 AND 21)") strConnection clear
			replace age = 0.01 if age == 0.02
			replace age = 0.1 if age == 0.08
			tempfile age_groups
			save `age_groups', replace
			** Identify age start/end for each cause
			use acause yll_age_start yll_age_end secret_cause yld_only is_estimate using "$prefix/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta" if secret_cause != 1 & yld_only != 1 & is_estimate == 1, clear
			keep acause yll_age_start yll_age_end
			recast double yll_age_start yll_age_end
			foreach bound in yll_age_start yll_age_end {
				rename `bound' age
				replace age = round(age, 0.01)
				merge m:1 age using `age_groups', assert(2 3) keep(3) nogen
				rename age_group_id `bound'_id
				drop age
			}
			** Get number of age groups by cause
			gen req_age_groups = yll_age_end_id - yll_age_start_id + 1
			keep acause req_age_groups
			tempfile age_count
			save `age_count', replace
		restore
	// Count age groups
		bysort NID iso3 location_id subdiv sex year acause (age) : gen age_count = _N
		merge m:1 acause using `age_count', keep(1 3) nogen
		count if age_count >= req_age_groups
		if `r(N)' > 0 {
	// Get population for ASDR
		preserve
			do "$prefix/WORK/03_cod/01_database/02_programs/prep/code/env_long.do"
			keep if age <= 80
			replace age = 91 if age == 0
			replace age = 93 if age == 0.01
			replace age = 94 if age == 0.1
			tempfile pop
			save `pop', replace
			replace age = 99
			fastcollapse pop env, by(iso3 location_id year age sex) type(sum)
			append using `pop'
			save `pop', replace
		restore
	// Sum qualifying datasets to all ages
		preserve
			** Keep qualifiing age ranges
			keep if age_count >= req_age_groups
			capture drop deaths*
			capture merge m:1 iso3 location_id year age sex using `pop', assert(2 3) keep(3) nogen
			if _rc != 0 {
				keep if pop==.
				save "$j/WORK/03_cod/01_database/03_datasets/$source/data/final/allage_fail.dta", replace
				BREAK
			}
			tempfile all
			save `all', replace
			** Make all ages
			foreach step in final corr rd raw {
				gen deaths_`step' = cf_`step'*env
			}
			replace age = 99
			fastcollapse deaths_final deaths_corr deaths_rd deaths_raw sample_size, by(age NID iso3 list location_id national region sex source source_label source_type subdiv year acause) type(sum)
			merge m:1 iso3 location_id year age sex using `pop', assert(2 3) keep(3) keepusing(env) nogen
			foreach step in final corr rd raw {
				gen cf_`step' = deaths_`step'/env
			}
			drop deaths*
			tempfile sum
			save `sum', replace
			** Make ASDR
			use `all', clear
			merge m:1 age using `age_weights', assert(2 3) keep(3) nogen
			foreach step in final corr rd raw {
				replace cf_`step' = ((cf_`step'*env)/pop)*weight
			}
			replace age = 98
			fastcollapse cf_final cf_corr cf_rd cf_raw sample_size, by(age NID iso3 list location_id national region sex source source_label source_type subdiv year acause) type(sum)
			tempfile asr
			save `asr'
		restore
		append using `sum'
		append using `asr'
		}
		capture drop age_count req_age_groups
** ********************************************************************************