*AGE SPLITTING GBS DATA
//Author: May 2016
	//this code splits any literature data with age_span > 10 
	//I will use USA Dismod fit (from marketscan and a little hospital) for the age pattern 
	

clear all
	set more off
	set mem 2g
	set maxvar 32000
	set type double, perm
	if c(os) == "Unix" {
		global prefix "/home/j"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
	}
	// directory for standard code files
		adopath + FILEPATH

	//Load GBD age metadata 
	run "FILEPATH/query_table.ado"
		query_table, table_name(age_group) clear 
			rename age_group_years_start age_start
			rename age_group_years_end age_end 
			keep age_group_id age_start age_end
			keep if age_group_id <= 21 //drop age_standardized 
			drop if inlist(age_group_id, 1, 2, 3, 4) //Drop discrete <5 groups and 0-5, just keeping 1-5 onward
		tempfile ages 
		save `ages', replace 

	//Load GBD populations 
		run "FILEPATH/get_demographics.ado"
		run "FILEPATH/get_populations.ado"
 		get_demographics, gbd_team(epi)
 		local location_ids "6 8 67 71 78 79 81 82 86 89 90 92 93 95 101 102 127 129 135 136 142 145 147 155 189 354 501 527 545 4625 4775 4944"
		get_populations, year_id(2000) location_id(`location_ids') sex_id($sex_ids) age_group_id($age_group_ids) clear 
      		

      	merge m:1 age_group_id using `ages', keep(3) nogen 
      		gen sex = ""
			replace sex = "Male" if sex_id == 1
			replace sex = "Female" if sex_id == 2

		tempfile demographics 
		save `demographics', replace 

		//Generate both sex group 
		collapse (sum) pop, by(age_group_id year_id location_id age_start age_end) 
			gen sex = "Both"
			replace age_end = 99 if age_end > 99 

		append using `demographics'
		save `demographics', replace 

**********************************************************
   *** LOAD STANDARD (USA) AGE PATTERN  ***
**********************************************************
	//Load best GBS model 
		get_best_model_versions, gbd_team(epi) id_list(2404) clear
		levelsof model_version_id, local(model_version_id)
	//Pull USA results (arbitrarily choose 2000)
		get_estimates, gbd_team(epi) model_version_id(`model_version_id') measure_ids(5) year_ids(2000) location_ids(102) clear 
		gen sex = ""
			replace sex = "Male" if sex_id == 1
			replace sex = "Female" if sex_id == 2
		tempfile USA_estimates
		save `USA_estimates', replace 
		
		//Merge to age metadata
		merge m:1 age_group_id using `ages', nogen keep(3)
		//merge to US population 
		merge m:1 location_id sex_id age_group_id using `pops', nogen keep(3)

		//Prep variables for standardization (see later for notation)
			rename mean Rr
			rename pop Nr
			gen Dr = Rr * Nr 

		tempfile reference
		save `reference', replace 

		//Generate both sex group 
		collapse (mean) Rr (sum) Nr Dr, by(age_start age_end age_group_id) 
			gen sex = "Both"
			replace age_end = 99 if age_end > 99 

		append using `reference'
		save `reference', replace 

	

**********************************************************
   *** LOAD LITERATURE DATA WITH WIDE AGE BANDS ***
**********************************************************
* get_data, modelable_entity_id(2404) clear 
	import excel "FILEPATH.xlsx", firstrow clear 
	tempfile data
	save `data', replace


	gen age_span = age_end - age_start
	drop if measure != "incidence"
	keep if age_span > 10 
	drop if age_start >= 80 
	//NOTE: these data all have cases and sample size. If not, you'd have to back-calculate it 
	tempfile pre_age_split
	save `pre_age_split', replace 	


* use `pre_age_split', clear 
* local i 6
count 
forval i = 1/`r(N)' {
	use `pre_age_split', clear 
	gen n = _n 
	keep if n == `i'
	ds
	foreach var in `r(varlist)' {
		local `var' = `var'
		}

	
	local D = cases 
	local parent_id = row_num

	tempfile temp 
	save `temp', replace 
	**********************************************************
	   *** AGE SPLIT!!! LOOP THROUGH EVERY DATA POINT ***
	**********************************************************
		***CALCULATION: 
		*Observed_age_spec = Expected_age_spec * (Obs_all_age / Exp_all_age)

		//Load country template 
		use `demographics' if age_start >= `age_start' & age_end <= `age_end' & location_id == `location_id' & sex == "`sex'", clear 

			//Merge to reference rates 
			merge 1:1 age_group_id sex using `reference', keep(3)
			
			
			//AGE SPLIT!!!
			sum pop
				local pop_total = `r(sum)'
			

				//Expected single age: Rr * sample_size_agespec 
					gen E_i = Rr * (`sample_size' * pop / `pop_total') 
				//Observed single age:
				//Observed all age: D
				//Expected all age: sum(Rr * sample_size_agespec) 
					gen E_t = Rr * (`sample_size' * pop / `pop_total') 
					sum E_t
					local E_t = `r(sum)' 

				*Observed single age =  
					gen cases = E_i * (`D' / `E_t')

			
			gen sample_size = `sample_size' * (pop / `pop_total')

		
			//merge back to study info 
			gen split = 1 
			gen n = `i'
			gen parent_id = `parent_id'
			gen row_num = . 
			keep n cases sample_size parent_id age_start age_end split
			merge m:1 n age_start age_end using `temp', nogen 

			foreach var in modelable_entity_id modelable_entity_name nid field_citation_value page_num table_num source_type location_name location_id ihme_loc_id smaller_site_memo site_memo sex sex_issue year_start year_end year_issue age_issue age_demographer measure unit_type unit_value_as_published measure_issue measure_id  representative_name urbanicity_type recall_type recall_type_value sampling_type response_rate case_name case_definition case_diagnostics extractor is_outlier {
				cap replace `var' = ``var'' if `var' == . 
				cap replace `var' = "``var''" if `var' == ""
				}

			replace input_type = "adjusted" if split == 1 
			*replace measure_adjustment = "1" if split == 1 
			replace uncertainty_type = "Sample size" if split == 1 


		/*
		***DIAGNOSTICS
			gen age = (age_start + age_end) / 2 
			replace mean = cases / sample_size if mean == . 
			twoway(scatter mean age if parent_id == .) ///
				(scatter mean age if parent_id != .)
		*/

		if `i' == 1	tempfile post_age_split
		else append using `post_age_split'
		save `post_age_split', replace 
		} //next data point 

	
replace note_modeler = "age-split from parent by USERNAME" if split == 1 


		
		***DIAGNOSTICS
			gen age = (age_start + age_end) / 2 
			replace mean = cases / sample_size if mean == . 
			twoway(scatter mean age if parent_id == .) ///
				(scatter mean age if parent_id != .)
			gen ln_mean = ln(mean)
			twoway(scatter ln_mean age if parent_id == .) ///
				(scatter ln_mean age if parent_id != .)
			drop ln_mean age
		

//Save parents 
preserve
drop if split == 1
export excel "FILEPATH.xlsx", firstrow(var) sheet("extraction") replace 
restore

//Save children 
keep if split == 1 
drop age_span n split 
export excel "FILEPATH.xlsx", firstrow(var) sheet("extraction") replace 



