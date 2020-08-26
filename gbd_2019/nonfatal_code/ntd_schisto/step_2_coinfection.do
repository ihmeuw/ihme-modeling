// Run glms to find ratios of species cases by region assuming normal distribution
//
*** ======================= BOILERPLATE ======================= ***
	clear all
	set more off
	set maxvar 32000
	local user : env USER
	local code_root "FILEPATH`user'"
	local data_root FILEPATH

	local exec_from_args : env EXEC_FROM_ARGS
	capture confirm variable exec_from_args, exact
	if "`exec_from_args'" == "True" {
		local params_dir 		`2'
		local draws_dir 		`4'
		local interms_dir		`6'
		local logs_dir 			`8'
	}
	else {
		local params_dir "`data_root'FILEPATH"
		local draws_dir "`data_root'FILEPATH"
		local interms_dir "`data_root'FILEPATH"
		local logs_dir "`data_root'FILEPATH"
	}

	cap log using "`logs_dir'FILEPATH`location'.smcl", replace
	if !_rc local close_log 1
	else local close_log 0

	// Source relevant functions
	adopath + FILEPATH
*** ======================= MAIN EXECUTION ======================= ***
	// Set up location data
	get_location_metadata, location_set_id(9) clear
    keep if most_detailed == 1 & is_estimate == 1
    keep ihme_loc_id location_id location_name region_name
    tempfile location_data
    save `location_data'
    clear

    // run glems to find ratio of total cases that are haematobium vs. mansoni
	import delimited "`params_dir'/FILEPATH", clear
	// keep data points that present both
	keep if cases_h != . & cases_m != .
	drop if sample_size_h == 0 | sample_size_m == 0
	// take away year, age, sex specificity
	collapse (sum) cases_h cases_m sample_size_h sample_size_m, by(nid field_citation_value case_diagnostics_h case_diagnostics_m location_name)

	// assume a normal binomial distribution
	// create draws for both species
    forvalues i = 0/999 {
        gen prob_h_`i' = rbinomial(sample_size_h, (cases_h/sample_size_h))/sample_size_h
        replace prob_h_`i' = 0 if prob_h_`i' == .
        gen prob_m_`i' = rbinomial(sample_size_m, (cases_m/sample_size_m))/sample_size_m
        replace prob_m_`i' = 0 if prob_m_`i' == .
    }

	// calculate mean (joint probability) at the draw level
    forvalues i = 0/999 {
        gen mean_`i' = prob_h_`i' + prob_m_`i' - (prob_m_`i')*(prob_h_`i')
        drop prob_h_`i' prob_m_`i'
    }

	// get upper, lower, mean, standard error
    egen mean = rowmean(mean_*)
    egen se = rowsd(mean_*)
    drop mean_*

	// methods of moments to get total cases and sample size
    gen total_cases = (mean * (mean - mean^2 - se^2))/se^2
    replace total_cases = round(total_cases)
    ** can't have 0 cases
    replace total_cases = 1 if total_cases == 0

	// clean data set
    replace cases_h = round(cases_h)
    replace cases_m = round(cases_m)
    replace total_cases = cases_h if cases_h > total_cases
    replace total_cases = cases_m if cases_m > total_cases
	// get location_id in there
    split location_name, parse("|")
    replace location_name = location_name1
    drop location_name1
    merge m:m location_name using `location_data', nogen keep(3)
	// encode region_name
    encode(region_name), gen(reg_id)

	// run generalized linear model on region
	// mansoni glm regression first, effects on region
    glm cases_m i.reg_id, family(binomial total_cases)
	// predict mean and standard error
    predict m_cases
    predict se_m, stdp
    gen prop_m = m_cases/total_cases

	// haematobium glm regression
    glm cases_h i.reg_id, family(binomial total_cases)
	// predict mean and standard error
    predict h_cases
    predict se_h, nooffset stdp
    gen prop_h = h_cases/total_cases

	// take the mean across regions to deal with rounded numbers
    collapse (mean) prop_m prop_h se_m se_h, by(region_name)
    keep region_name prop_m prop_h se_h se_m

	// expand proportions to one if they are not over 1
    gen prop_total = prop_m + prop_h
    replace prop_m = prop_m/prop_total if prop_total < 1
    replace prop_h = prop_h/prop_total if prop_total < 1
    replace se_m = se_m/prop_total if prop_total < 1
    replace se_h = se_h/prop_total if prop_total < 1
    drop prop_total

	// save file
    export delimited "`interms_dir'/FILEPATH", replace
