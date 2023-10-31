** Project: RF: Lead Exposure
** Purpose: Tabulate lead microdata

** Set preferences for STATA
    ** Clear memory and set memory and variable limits
        clear all
        macro drop _all
        set maxvar 32000
    ** Set to run all selected code without pausing 
        set more off
    ** Remove previous restores
        cap restore, not
		    ** Define J drive (data) for cluster (UNIX) and Windows (Windows)
        if c(os) == "Unix" {
            global prefix "/home/j"
            set odbcmgr unixodbc
        }
        else if c(os) == "Windows" {
            global prefix "J:"
        }
// Arguments
	local input_file `1'
	local location_name `2'
	local ihme_loc_id `3'
	local location_id `4'
	local smaller_site_unit `5'
	local representative_name `6'
	local urbanicity_type `7'
	local file_path `8'
	local nid `9'
	local year_start `10'
	local year_end `11'
	local out_file `12'
	
// check that the arguments were passed in correctly
	di "`input_file'"
	di "`location_name'"
	di "`ihme_loc_id'"
	di "`location_id'"
	di "`smaller_site_unit'"
	di "`representative_name'"
	di "`urbanicity_type'"
	di "`file_path'"
	di "`nid'"
	di "`year_start'"
	di "`year_end'"
	di "`out_file'"

    // CHOOSE INPUT FILE (change to filepath to new microdata source)
	
	import delimited "`input_file'", clear
   
	rename sex_id sex
	rename age_year age_years
	rename ihme_loc_id location_name
	rename survey_name survey
	rename pweight sample_weight_d1
	
    cap confirm variable age_month
    if !_rc {
    	cap confirm variable admin_1
		if !_rc keep admin_1 sex age_years age_month location_name year_start year_end survey psu strata sample_weight_d1 bll
		else keep sex age_years age_month location_name year_start year_end survey psu strata sample_weight_d1 bll
    }
	else {
		cap confirm variable admin_1
		if !_rc keep admin_1 sex age_years location_name year_start year_end survey psu strata sample_weight_d1 bll
		else keep sex age_years location_name year_start year_end survey psu strata sample_weight_d1 bll
	}

	// NHANES codes unknown bll as 8888
    replace bll = . if bll == 8888
	
	**create an indicator variable for age to tabulate on
	gen age_group_id = .
        replace age_group_id = 34 if age_years >= 2 & age_years < 5
            local age_group_id_counter = 6
            local age_lower = 5
            local age_upper = 10
            forvalues n = 1/16 {
                replace age_group_id = `age_group_id_counter' if age_years >= `age_lower' & age_years < `age_upper'
                    local age_group_id_counter = `age_group_id_counter' + 1
                    local age_lower = `age_lower' + 5
                    local age_upper = `age_upper' + 5
            }
        replace age_group_id = 31 if age_years >= 85 & age_years < 90
        replace age_group_id = 32 if age_years >= 90 & age_years < 95
        replace age_group_id = 235 if age years >= 95 years
        cap confirm variable age_month
        if !_rc {
        	replace age_group_id = 388 if age_years == 0 & age_month >= 1 & age_month < 6
			replace age_group_id = 389 if age_years == 0 & age_month >= 6 & age_month < 12
			replace age_group_id = 238 if age_years == 0 & age_month >= 12 & age_month < 24
			replace age_group_id = 3 if age_years == 0 & age_month == 0
			replace age_group_id = 34 if age_years == 1 & age_month == 24 // special NHANES case, not sure why the data are like this
        }
		
	drop if age_group_id == 3 & location_name == "IND"
	drop if age_group_id == 8 & location_name == "KOR" & year_start < 2009
	drop if mi(bll)

	**use the svyset command in stata to assist in tabulating the dataset
    svyset psu [pweight=sample_weight_d1], strata(strata) singleunit(scaled)
	svy: mean bll, over(age_group_id sex)
	
    **place the stored values in below named matrices
	matrix am_mean = e(b)
    matrix am_var = vecdiag(e(V))'
    matrix bll_ss = e(_N)
	
	matrix list am_var
	** adding new lines for extracting standard deviation 
	matrix sigma2 = vecdiag(vecdiag(e(V))' * e(_N))
	
	// take natural log of data (in order to get geometric mean later)
	gen bll_ln = ln(bll)
	svyset psu [pweight=sample_weight_d1], strata(strata) singleunit(scaled)
	svy: mean bll_ln, over(age_group_id sex)
	
    **place the stored values in below named matrices
	matrix gm_mean = e(b)
    matrix bll_ln_var = vecdiag(e(V))'
            
            **create the skeleton that will be populated with stored values
            keep age_group_id sex
            duplicates drop
            drop if sex == . | age_group_id == .
            sort age_group_id sex
            **create a counter local to navigate through each row/column the the matrix
            local matrix_counter = 1
            local combinations = _N
            gen sigma2 = .
            gen am_mean = .
            gen am_var = .
            gen bll_ss = .
			gen gm_mean = .
            gen bll_ln_var = .
    forvalues row_counter = 1/`combinations' {
		replace sigma2 = sigma2[1,`matrix_counter'] in `row_counter'
		replace am_mean = am_mean[1,`matrix_counter'] in `row_counter'
		replace gm_mean = gm_mean[1,`matrix_counter'] in `row_counter'
		replace am_var  = am_var[`matrix_counter',1] in `row_counter'
		replace bll_ln_var  = bll_ln_var[`matrix_counter',1] in `row_counter'
		replace bll_ss  = bll_ss[1,`matrix_counter'] in `row_counter'
		local matrix_counter = `matrix_counter' + 1
    }	
	
	// get CI for gm_mean in ln space 
	gen lower = gm_mean - (1.96*sqrt(bll_ln_var))
	gen upper = gm_mean + (1.96*sqrt(bll_ln_var))
	
	// transform back to normal space in order to generate geometric means and CI
	replace gm_mean = exp(gm_mean)
	replace lower = exp(lower)
	replace upper = exp(upper)
	gen standard_error = sqrt(am_var)
	gen standard_deviation = sqrt(sigma2)

	// separate arithmetic and geometric means 
	expand 2
	gen id = _n
	gen bll_mean = am_mean
	gen cv_mean_type = 1
	replace bll_mean = gm_mean if id > _N/2
	replace cv_mean_type = 0 if id > _N/2
	drop am_var bll_ln_var am_mean gm_mean id
	replace lower = . if cv_mean_type == 1
	replace upper = . if cv_mean_type == 1
	replace standard_error = . if cv_mean_type == 0
	
	// drop rows with no data or ones that have sample sizes below 5
	drop if mi(bll_mean)
	drop if bll_ss < 5

	// generate variables to merge with 2016 extraction sheet
	rename bll_mean mean
	gen modelable_entity_id = 8953
	gen modelable_entity_name = "Lead exposure in blood exposure"
	gen source_type = "Survey - cross-sectional"
	
	// LOCATION DETAILS
	// CHANGE FOR EACH SURVEY AS NEEDED
	gen location_name = "`location_name'" //
	gen location_id = `location_id' //
	gen ihme_loc_id = "`ihme_loc_id'" //
	gen smaller_site_unit = `smaller_site_unit' //

	rename sex sex_id
	gen sex = "Male"
	replace sex = "Female" if sex_id == 2
	gen sex_issue = 0
	
	gen age_start = (age_group_id * 5) - 25
	replace age_start = (age_group_id * 5) - 70 if age_group_id > 30
	replace age_start = 0.07671233 if age_group_id == 388
	replace age_start = 0.5 if age_group_id == 389
	replace age_start = 1 if age_group == 238
	replace age_start = 2 if age_group_id == 34
	replace age start = 95 if age_group == 235

	gen age_end = (age_group_id * 5) - 20
	replace age_end = (age_group_id * 5) - 65 if age_group_id > 30
	replace age_end = 0.5 if age_group_id == 388
	replace age_end = 0.999 if age_group_id == 389
	replace age_end = 2 if age_group_id == 238
	replace age_end = 5 if age_group_id == 34
	replace age_end = 125 if age_group == 235
	gen age_issue = 0
	
	gen age_demographer = 0 // was previously 1, but i think this was incorrect
	gen year_issue = 0
	gen measure = "continuous"
	rename bll_ss sample_size
	gen unit_type = "person"
	gen unit_value_as_published = 1
	gen measure_issue = 0
	gen measure_adjustment = 0
	
	// REPRESENTATIVENESS AND URBANICITY DETAILS
	// CHANGE FOR EACH SURVEY AS NEEDED
	gen representative_name = "`representative_name'" //
	gen urbanicity_type = "`urbanicity_type'" //

	gen recall_type = "Point"
	gen sampling_type = "Multistage"
	gen extractor = "fbennitt"
	gen is_outlier = 0
	
// NEED TO INPUT CORRECT FILEPATH, NID, CITATION, YEARS, AND OUTPUT FILE
	gen file_path = "`file_path'" //
	gen nid = `nid' //
	gen year_start = `year_start' //
	gen year_end = `year_end' //
// GIVE IT THE DESIRED NAME (OR IT WILL OVERWRITE WHATEVER'S ALREADY THERE)
	export delimited "`out_file'", replace
	
	