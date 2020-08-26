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

    // CHOOSE INPUT FILE (change to filepath to new microdata source)
    use "FILEPATH", clear
       
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
        replace age_group_id = 5 if age_years >= 1 & age_years < 5
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
        replace age_group_id = 33 if age_years >= 95 
        cap confirm variable age_month
        if !_rc {
        	replace age_group_id = 3 if age_years == 0 & age_month == 0
        	replace age_group_id = 4 if age_years == 0 & age_month > 0
        }
	
// Running into issue when lowest age group with bll measurements is (probably due to sampling error) only available for
	// one sex (shifts all other values down). Since such groups are so small that they need to be dropped anyways, this was
	// my makeshift solution for the applicable surveys
	drop if age_group_id == 3 & location_name == "IND"
	drop if age_group_id == 8 & location_name == "KOR" & year_start < 2009
	// need to drop missing bll or else it starts filling youngest age group present in survey with the first set of bll's, 
	// even if that age group wasn't actually eligible for bll measurements
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
	gen location_name = "South Korea|KOR"
	gen location_id = 68
	gen ihme_loc_id = "KOR"
	gen smaller_site_unit = 0

	rename sex sex_id
	gen sex = "Male"
	replace sex = "Female" if sex_id == 2
	gen sex_issue = 0
	
	gen age_start = (age_group_id * 5) - 25
	replace age_start = 1 if age_group_id == 5
	replace age_start = (age_group_id * 5) - 70 if age_group_id > 30
	replace age_start = 0.083 if age_group_id == 4
	replace age_start = 0 if age_group_id == 3
	gen age_end = (age_group_id * 5) - 21
	replace age_end = 4 if age_group_id == 5
	replace age_end = (age_group_id * 5) - 66 if age_group_id > 30
	replace age_end = 0.999 if age_group_id == 4
	replace age_end = 0.082 if age_group_id == 3
	gen age_issue = 0
	
	gen age_demographer = 1
	gen year_issue = 0
	gen measure = "continuous"
	rename bll_ss sample_size
	gen unit_type = "person"
	gen unit_value_as_published = 1
	gen measure_issue = 0
	gen measure_adjustment = 0
	
	// REPRESENTATIVENESS AND URBANICITY DETAILS
	gen representative_name = "Nationally representative only"
	gen urbanicity_type = "Mixed/both"

	gen recall_type = "Point"
	gen sampling_type = "Multistage"
	gen extractor = "calebi"
	gen is_outlier = 0
	
// NEED TO INPUT CORRECT FILEPATH, NID, CITATION, YEARS, AND OUTPUT FILE
	gen file_path = "FILEPATH"
	gen nid = 135721
	gen field_citation_value = "SURVEY_NAME"
	gen year_start = 2012
	gen year_end = 2012
/ GIVE IT THE DESIRED NAME (OR IT WILL OVERWRITE WHATEVER'S ALREADY THERE)	
export delimited "FILEPATH", replace
