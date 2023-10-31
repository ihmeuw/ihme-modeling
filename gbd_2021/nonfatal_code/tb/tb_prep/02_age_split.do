// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Age-split prevalence data using age distributions from China and Brazil
// Author:		Hmwe Kyu
// Edited:      Jorge Ledesma
// Description:	Create custom age groups for populations
//				Age-split using China age-pattern
//				Age-split the 75+ ages using Brazil age-pattern
//				Split the sample size
// Variables:	date, decomp_step
// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
         
**********************************************************************************************************************
** ESTABLISH SETTINGS FOR SCRIPT
**********************************************************************************************************************		 
		 
// Load settings

	// Clear memory and establish settings
	clear all
	set more off
	set scheme s1color

	// Define focal drives
	if c(os) == "Unix" {
		global prefix "/home/j"
		local function_prefix "/ihme/cc_resources"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "J:"
		local function_prefix "K:"
	}

	// Close any open log file
	cap log close
	
// Load helper objects and directories
	
	// locals
	local date 2020_05_26
	local decomp_step iterative

**********************************************************************************************************************
** STEP 1: GATHER ALL THE INPUTS - AGE PATTERN, DATA TO SPLIT, POPULATIONS
**********************************************************************************************************************

	// Load data to age-split
	import excel using "/Volumes/snfs/WORK/12_bundle/tb/712/01_input_data/03_ep_adjusted/`date'/`decomp_step'_all_ep_inflated_`date'.xlsx", firstrow clear

	// Clean data
	replace group_review=0 if (inlist(age_start, 0, 15, 45, 50, 55, 60, 65)) & (inlist(age_end, 99, 100))
	replace specificity="before age split" if (inlist(age_start, 0, 15, 45, 50, 55, 60, 65)) & (inlist(age_end, 99, 100))
	replace group=0 if (inlist(age_start, 0, 15, 45, 50, 55, 60, 65)) & (inlist(age_end, 99, 100))

	// Save tempfile
	tempfile to_add_later
	save `to_add_later', replace

	// Load age-pattern from CHN
	//use "\\ihme.washington.edu\ihme\snfs\WORK\04_epi\01_database\02_data\tb\1175\04_temp\age patterns\CHN_age_pattern.dta", clear
	use "/Volumes/snfs/WORK/04_epi/01_database/02_data/tb/1175/04_temp/age patterns/CHN_age_pattern.dta", clear
	// Clean data
	keep age_start age_end sex mean
	rename mean rate

	// Save tempfile
	tempfile CHN_age_pattern
	save `CHN_age_pattern', replace

	// Load age-pattern from BRA
	//use "\\ihme.washington.edu\ihme\snfs\WORK\04_epi\01_database\02_data\tb\1175\01_input_data\01_nonlit\05_notifications by country\BRA\BRA_inc_age_pattern.dta", clear
	use "/Volumes/snfs/WORK/04_epi/01_database/02_data/tb/1175/01_input_data/01_nonlit/05_notifications by country/BRA/BRA_inc_age_pattern.dta", clear

	tempfile BRA_age_pattern
	save `BRA_age_pattern', replace

	// Load data to age-split
	import excel using "/Volumes/snfs/WORK/12_bundle/tb/712/01_input_data/03_ep_adjusted/`date'/`decomp_step'_all_ep_inflated_`date'.xlsx", firstrow clear

	// Clean
	gen iso3=ihme_loc_id
	keep nid location_id location_name iso3 year_start year_end age_start age_end sex sample_size cases cv_diag_smear
	replace age_end = 100 if age_end == 99 
	drop if cases == .

	// Save tempfile
	tempfile countries
	save `countries', replace

	// Save tempfiles for age-bins of interest
	use `countries', clear

	forvalues i=0(5)80 {
		preserve
			keep if age_start==`i' & age_end==100
			tempfile tmp_`i'
			save `tmp_`i'', replace
		restore
	}

	use `tmp_70', clear

	// Pull populations
	insheet using "/Volumes/snfs/temp/TB/GBD2020/data/population_decomp2.csv", clear
	merge m:1 age_group_id using "/Volumes/snfs/temp/TB/GBD2016/GBD2016/age_start_id.dta", keep(3)nogen
	merge m:1 location_id using "/Volumes/snfs/temp/TB/GBD2020/data/locations_22.dta", keepusing(ihme_loc_id) keep(3)nogen

	// Clean data
	rename ihme_loc_id iso3
	gen sex=""

	// Fix sex data
	replace sex="Male" if sex_id==1
	replace sex="Female" if sex_id==2
	replace sex="Both" if sex_id==3

	// Subset data
	rename population pop
	keep iso3 year age_start age_end sex pop
	rename year year_start

**********************************************************************************************************************
** STEP 2: CREATE CUSTOM AGE-BINS OF POPULATIONS
**********************************************************************************************************************
			
	// Create the 90-100 age group
	preserve
		keep if age_start==90 | age_start==95
		collapse(sum)pop, by(iso3 year_start sex) fast
		gen age_start=90
		gen age_end=100
		tempfile pop_90to100
		save `pop_90to100', replace
	restore

	// Append the 90+ population
	drop if age_start==90 & age_end==94 | age_start==95 & age_end==100
	append using `pop_90to100'

	// Create the 75-100 age group
	preserve
		keep if age_start>=75
		collapse(sum)pop, by(iso3 year_start sex) fast
		gen age_start=75
		gen age_end=100
		tempfile pop_75to100
		save `pop_75to100', replace
	restore

	// Append the 75+ population
	append using `pop_75to100'

	// Create datasets for larger age-bins
	forvalues i=0(5)80 {
		preserve
			keep if age_start>=`i' 
			tempfile pop_`i'
			save `pop_`i'', replace
		restore
	}

	// Create 75+ age-bin
	preserve 
		keep if age_start>=75
		drop if age_start==75 & age_end==100
		tempfile pop_75_plus
		save `pop_75_plus', replace
	restore

	// Prep to create custom age-bins
	use `pop_75_plus', clear
		gen cv_diag_smear = 0
		tempfile a_pop_75
	save `a_pop_75', replace

	use `pop_75_plus', clear
		gen cv_diag_smear = 1
		append using `a_pop_75'
	save `pop_75_plus', replace

	// Create other custom age-bins
	forvalues i=0(5)70 {
		use `pop_`i'', clear
		gen cv_diag_smear = 0
		tempfile a_pop_`i'
		save `a_pop_`i'', replace
		use `pop_`i'', clear
		gen cv_diag_smear = 1
		append using `a_pop_`i''
		save `pop_`i'', replace
	}

**********************************************************************************************************************
** STEP 3: BEGIN AGE-SPLIT BY AGE CATEGORY
**********************************************************************************************************************			
	
	// Age-split 65+
		use `tmp_65', clear
		drop age*
		drop if nid == .
        
		// Merge on population
		destring year_start, replace
        merge m:m iso3 year_start sex cv_diag_smear using `pop_65', keep(3) nogen

		// Merge on age pattern
		merge m:m age_start age_end sex using `CHN_age_pattern', keep(3)nogen
		rename pop sub_pop
		gen rate_sub_pop=rate*sub_pop
		
		// Compute denominator
        preserve
			collapse (sum) rate_sub_pop, by(nid iso3 year_start sex cv_diag_smear) fast
            rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace
		restore
		
		// Merge denominator
		merge m:1 iso3 year_start sex cv_diag_smear using `sum', keep(3)nogen

		// Age-split
		gen new_cases=rate_sub_pop*(cases/sum_rate_sub_pop)
        replace cases=new_cases if new_cases !=.
		sort iso3 year_start sex age_start
		
		// Create tempfile 
		tempfile age_split_1
		save `age_split_1', replace

	// Age-split 60+
		use `tmp_60', clear
		drop age*
		drop if nid == .
		
		// Merge on population
		destring year_start, replace
		merge m:m iso3 year_start sex cv_diag_smear using `pop_60', keep(3) nogen

		// Merge on age pattern
		merge m:m age_start age_end sex using `CHN_age_pattern', keep(3)nogen
		rename pop sub_pop
		gen rate_sub_pop=rate*sub_pop

		// Compute denominator
		preserve
			collapse (sum) rate_sub_pop, by(nid iso3 year_start sex cv_diag_smear) fast
			rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace
		restore
		
		// Merge denominator
		merge m:1 iso3 year_start sex cv_diag_smear using `sum', keep(3)nogen

		// Age-split
		gen new_cases=rate_sub_pop*(cases/sum_rate_sub_pop)
		replace cases=new_cases if new_cases !=.
		sort iso3 year_start sex age_start

		// Create tempfile 
		tempfile age_split_2
		save `age_split_2', replace
		append using `age_split_1'
		save `age_split_1', replace

	// Age-split 70+
		use `tmp_70', clear
		drop age*
		drop if nid == .
		
		// Merge on population
		destring year_start, replace
		merge m:m iso3 year_start sex cv_diag_smear using `pop_70', keep(3) nogen

		// Merge on age pattern
		merge m:m age_start age_end sex using `CHN_age_pattern', keep(3)nogen
		rename pop sub_pop
		gen rate_sub_pop=rate*sub_pop

		// Compute denominator
		preserve
			collapse (sum) rate_sub_pop, by(nid iso3 year_start sex cv_diag_smear) fast
			rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace
		restore
		
		// Merge denominator
		merge m:1 iso3 year_start sex cv_diag_smear using `sum', keep(3)nogen

		// Age-split
		gen new_cases=rate_sub_pop*(cases/sum_rate_sub_pop)
		replace cases=new_cases if new_cases !=.
		sort iso3 year_start sex age_start

		// Create tempfile 
		tempfile age_split_4
		save `age_split_4', replace
		append using `age_split_1'
		save `age_split_1', replace

	// Age-split 55+
		use `tmp_55', clear
		drop age*
		drop if nid == .
		
		// Merge on population
		destring year_start, replace
		merge m:m iso3 year_start sex cv_diag_smear using `pop_55', keep(3) nogen

		// Merge on age pattern
		merge m:m age_start age_end sex using `CHN_age_pattern', keep(3)nogen
		rename pop sub_pop
		gen rate_sub_pop=rate*sub_pop

		// Compute denominator
		preserve
			collapse (sum) rate_sub_pop, by(nid iso3 year_start sex cv_diag_smear) fast
			rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace
		restore
		
		// Merge denominator
		merge m:1 iso3 year_start sex cv_diag_smear using `sum', keep(3)nogen

		// Age-split
		gen new_cases=rate_sub_pop*(cases/sum_rate_sub_pop)
		replace cases=new_cases if new_cases !=.
		sort iso3 year_start cv_diag* sex age_start

		// Create tempfile
		tempfile age_split_3
		save `age_split_3', replace
		append using `age_split_1'
		save `age_split_1', replace
			
	// Age-split 50+
		use `tmp_50', clear
		drop age*
		drop if nid == .
		
		// Merge on population
		destring year_start, replace
		merge m:m iso3 year_start sex cv_diag_smear using `pop_50', keep(3) nogen

		// Merge on age pattern
		merge m:m age_start age_end sex using `CHN_age_pattern', keep(3)nogen
		rename pop sub_pop
		gen rate_sub_pop=rate*sub_pop

		// Compute denominator
		preserve
			collapse (sum) rate_sub_pop, by(nid iso3 year_start sex cv_diag_smear) fast
			rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace
		restore
		
		// Merge denominator
		merge m:1 iso3 year_start sex cv_diag_smear using `sum', keep(3)nogen

		// Age-split
		gen new_cases=rate_sub_pop*(cases/sum_rate_sub_pop)
		replace cases=new_cases if new_cases !=.
		sort iso3 year_start cv_diag* sex age_start

		// Create tempfile
		tempfile age_split_4
		save `age_split_4', replace
		append using `age_split_1'
		save `age_split_1', replace
			
	// Age-split 45+
		use `tmp_45', clear
		drop age*
		drop if nid == .
		
		// Merge on population
		destring year_start, replace
		merge m:m iso3 year_start sex cv_diag_smear using `pop_45', keep(3) nogen

		// Merge on age pattern
		merge m:m age_start age_end sex using `CHN_age_pattern', keep(3)nogen
		rename pop sub_pop
		gen rate_sub_pop=rate*sub_pop

		// Compute denominator
		preserve
			collapse (sum) rate_sub_pop, by(nid iso3 year_start sex cv_diag_smear) fast
			rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace
		restore
		
		// Merge denominator
		merge m:1 iso3 year_start sex cv_diag_smear using `sum', keep(3)nogen

		// Age-split
		gen new_cases=rate_sub_pop*(cases/sum_rate_sub_pop)
		replace cases=new_cases if new_cases !=.
		sort iso3 year_start cv_diag* sex age_start

		// Create tempfile
		tempfile age_split_5
		save `age_split_5', replace
		append using `age_split_1'
		save `age_split_1', replace

	// Age-split 15+
		use `tmp_15', clear
		drop age*
		drop if nid == .
		
		// Merge on population
		destring year_start, replace
		merge m:m iso3 year_start sex cv_diag_smear using `pop_15', keep(3) nogen

		// Merge on age pattern
		merge m:m age_start age_end sex using `CHN_age_pattern', keep(3)nogen
		rename pop sub_pop
		gen rate_sub_pop=rate*sub_pop

		// Compute denominator
		preserve
			collapse (sum) rate_sub_pop, by(nid iso3 year_start sex cv_diag_smear) fast
			rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace
		restore
		
		// Merge denominator
		merge m:1 iso3 year_start sex cv_diag_smear using `sum', keep(3)nogen

		// Age-split
		gen new_cases=rate_sub_pop*(cases/sum_rate_sub_pop)
		replace cases=new_cases if new_cases !=.
		sort iso3 year_start cv_diag* sex age_start

		// Create tempfile
		tempfile age_split_6
		save `age_split_6', replace
		append using `age_split_1'
		save `age_split_1', replace
			
	// Age-split 0+
		use `tmp_0', clear
		drop age*
		drop if nid == .
		
		// Merge on population
		destring year_start, replace
		merge m:m iso3 year_start sex cv_diag_smear using `pop_0', keep(3) nogen

		// Merge on age pattern
		merge m:m age_start age_end sex using `CHN_age_pattern', keep(3)nogen
		rename pop sub_pop
		gen rate_sub_pop=rate*sub_pop

		// Compute denominator
		preserve
			collapse (sum) rate_sub_pop, by(nid iso3 year_start sex cv_diag_smear) fast
			rename rate_sub_pop sum_rate_sub_pop
			tempfile sum
			save `sum', replace
		restore
		
		// Merge denominator
		merge m:1 iso3 year_start sex cv_diag_smear using `sum', keep(3)nogen

		// Age-split
		gen new_cases=rate_sub_pop*(cases/sum_rate_sub_pop)
		replace cases=new_cases if new_cases !=.
		sort iso3 year_start cv_diag* sex age_start

		// Create tempfile
		tempfile age_split_7
		save `age_split_7', replace
		append using `age_split_1'
		save `age_split_1', replace

**********************************************************************************************************************
** STEP 4: SPLIT 75+ USING BRAZIL PATTERN
**********************************************************************************************************************			

	// Get age-split data
	use `age_split_1', clear

	// Get 75+ age group
	preserve
		drop if age_start==75 & age_end==100
		tempfile tmp_under75
		save `tmp_under75', replace
	restore 

	// Keep age 75-100
	keep nid location_name location_id sex year_start year_end cases sample_size iso3 age_start age_end cv_diag_smear
	keep if age_start==75 & age_end==100
	drop age*

	// Merge on population
	destring year_start, replace
	merge m:m iso3 year_start sex cv_diag_smear using `pop_75_plus', keep(3) nogen
	duplicates drop

	// Merge on age pattern
	merge m:1 age_start age_end sex using `BRA_age_pattern', keep(3)nogen
	rename pop sub_pop
	gen rate_sub_pop=rate*sub_pop

	// Split the denominator
	preserve
		collapse (sum) rate_sub_pop, by(nid iso3 year_start sex cv_diag_smear) fast
		rename rate_sub_pop sum_rate_sub_pop
		tempfile sum
		save `sum', replace
	restore

	// Split the cases 
	merge m:1 iso3 year_start sex cv_diag_smear using `sum', keep(3)nogen
	gen new_cases=rate_sub_pop*(cases/sum_rate_sub_pop)
	sort iso3 year_start sex age_start cv_diag_smear

	// Append age under 75
	append using `tmp_under75'         
	sort location_id year_start cv_diag_smear sex age_start

	// Clean data
	replace cases=new_cases if new_cases !=.
	drop new_cases

	// Save tempfile
	tempfile split_done_1
	save `split_done_1', replace

	// Output a temp csv			
	outsheet using "/Volumes/snfs/WORK/12_bundle/tb/712/01_input_data/03_ep_adjusted/age_split_tmp/prev_suveys_age_splitted_`date'.csv", comma names replace

**********************************************************************************************************************
** STEP 5: SPLIT SAMPLE SIZE
**********************************************************************************************************************			

	// Bring in the template
	import excel using "/Volumes/snfs/WORK/12_bundle/tb/712/01_input_data/03_ep_adjusted/`date'/`decomp_step'_all_ep_inflated_`date'.xlsx", firstrow clear
	replace age_end = 100 if age_end == 99 
	drop cases sample_size mean* standard_error* age_start age_end

	// Clean variables
	replace upper=.
	replace lower=.
	replace effective_sample_size=. 
	replace input_type=""
	replace is_outlier=0

	// Data cleaning
	drop specificity
	gen specificity="after age split"
	replace group_review=1
	replace group=1
	destring location_id year_start, replace

	// Save tempfile
	tempfile template
	save `template', replace

	// Bring in split data
	insheet using "/Volumes/snfs/WORK/12_bundle/tb/712/01_input_data/03_ep_adjusted/age_split_tmp/prev_suveys_age_splitted_`date'.csv", comma names clear

	// Clean data
	rename sample_size sample_size_total
	drop rate_sub_pop rate sum_rate_sub_pop
	rename sub_pop pop

	// Split the denominator
	bysort nid location_id sex cv_diag_smear sample_size_total: egen total_pop = total(pop)
	gen proportion_pop = pop/total_pop
	gen sample_size = sample_size_total*proportion_pop 

	// Recompute mean and se
	gen mean=cases/sample_size
	drop if mean > 1
	gen standard_error=sqrt(mean*(1-mean)/sample_size)
	replace standard_error = sqrt(1/sample_size * mean * (1 - mean) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if cases==0
	replace standard_error = 0.25 if standard_error > 0.25

	// Clean data
	drop pop total_pop proportion_pop sample_size_total
	sort location_id year_start cv_diag* sex age_start

	// Output a temp csvs		
	outsheet using "/Volumes/snfs/WORK/12_bundle/tb/712/01_input_data/03_ep_adjusted/age_split_tmp/sample_splitted_`date'.csv", comma names replace

**********************************************************************************************************************
** STEP 6: CLEAN DATA
**********************************************************************************************************************			

	// Start cleaning
	destring location_id year_start, replace
	merge m:m nid location_id year_start year_end sex using `template', keep(3)nogen
	duplicates drop

	// Create tempfile
	tempfile all_split_ready
	save `all_split_ready', replace

	// Add un-split data
	append using `to_add_later', force
	sort location_id year_start year_end cv_diag_smear sex age_start age_end

	// Merge in location name
	drop ihme_loc_id location_name
	drop iso3
	merge m:1 location_id using "/Volumes/snfs/temp/TB/GBD2020/data/locations_22.dta", keepusing(ihme_loc_id location_name) keep(3)nogen

	// Recompute summaries
	replace lower = mean-1.96*standard_error
	replace upper = mean+1.96*standard_error
	replace lower = 0 if lower < 0 
	replace upper = 1 if upper > 1 

	// Output an excel sheet
	export excel using "/Volumes/snfs/WORK/12_bundle/tb/712/01_input_data/03_ep_adjusted/`date'/`decomp_step'_ep_adj_age_split_`date'.xlsx", firstrow(variables) nolabel replace

	// Save tempfile
	tempfile national
	save `national', replace
		
	// Read in excel sheet
	import excel using "/Volumes/snfs/WORK/12_bundle/tb/712/01_input_data/03_ep_adjusted/`date'/`decomp_step'_ep_adj_age_split_`date'.xlsx", firstrow clear
	order seq input_type underlying_nid nid underlying_field_citation_value field_citation_value pubyear authoryear file_path page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper effective_sample_size cases sample_size design_effect unit_type unit_value_as_published measure_issue measure_adjustment uncertainty_type uncertainty_type_value representative_name urbanicity_type recall_type recall_type_value sampling_type case_name case_definition group specificity group_review note_modeler note_sr extractor is_outlier cv_subnational cv_diag_smear cv_diag_culture cv_screening mean standard_error

	// fix rows without cases
	replace group = . if cases == .
	replace specificity = "" if cases == .
	replace group_review = . if cases == .

	// Final save
	export excel using "/Volumes/snfs/WORK/12_bundle/tb/712/01_input_data/03_ep_adjusted/`date'/`decomp_step'_ep_adj_age_split_`date'.xlsx", firstrow(variables) nolabel replace
