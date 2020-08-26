		clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		set odbcmgr ADDRESS
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
	}

	// set adopath
	adopath + "$prefix//FILEPATH"
	
		
	
import excel "FILEPATH", firstrow clear
	
gen state = string(underlying_nid)
drop underlying_nid
rename state underlying_nid
order underlying_nid	  
	
	drop if measure == "proportion"
	
	drop if is_outlier == 1
	gen row_count = _n
	tempfile all_data
	save `all_data', replace
	sort row_count
	
	**************************************************************************************************************************
	** AGGREGATE TO GBD LOCATION LEVEL
	**************************************************************************************************************************

	** assigning a species group number to each set of source, year, age, sex, location, case diagnostic, measure, and species data for later aggregation
	preserve
	duplicates tag nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics measure value_num_samples value_num_slides, gen(keep)
	drop if keep == 0
	
	duplicates drop nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics measure value_num_samples value_num_slides, force
	gen species_group = _n
	keep nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics species_group measure value_num_samples value_num_slides
	tempfile dupes
	save `dupes', replace
	restore

	** merge in these unique identifiers (species_group)
	merge m:1 nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics value_num_samples value_num_slides using `dupes', nogen keep (1 3)

	
	
	drop if sample_size<cases & mean>=1
	drop if sample_size<cases
	drop if cases==.
	
	
	*creating unique identifiers for cohort groups that will later be aggregated. This is done so we can somehow backtrack to the original seqs if need be
	
	sort nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics measure
	
	*egen unique_group = group(nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics measure)
	
	egen unique_group_2 = group(nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics measure), missing
	
	preserve 
	
	
	keep unique_group_2 seq is_outlier source_type sampling_type representative_name urbanicity_type recall_type recall_type_value unit_type uncertainty_type input_type design_effect unit_value_as_published uncertainty_type_value specificity group group_review
	
	
	duplicates drop unique_group_2, force
	tempfile crosswalk_parent_seq
	save `crosswalk_parent_seq', replace
	
	restore
	
	
	** collapse to get totals by source, year, age, sex, gbd_location, case diagnostic
	collapse (sum) cases sample_size, by(nid underlying_nid field_citation_value year_start year_end age_start age_end location_name ihme_loc_id location_id sex case_name case_diagnostics measure unique_group_2)
	
	merge m:1 unique_group_2 using `crosswalk_parent_seq', nogenerate keep (1 3)
	
*recalculating lost variables such as mean, standard error, upper and lower cis cause of aggregation 

gen	mean=.
gen standard_error=.
gen upper=.
gen lower=.


replace mean = cases/sample_size
replace standard_error = sqrt(mean*(1-mean)/sample_size)
replace upper = (mean + (1.96*standard_error))
replace lower = (mean - (1.96*standard_error))

*dropping outliers 
drop if mean<0.1

*retagging some Saudi subnationals to Saudi arabia the country

replace location_id=152 if location_id==44551 | location_id==44546 | location_id==44550
replace ihme_loc_id="SAU" if location_id==152
replace location_name="Saudi Arabia" if location_id==152

export excel "FILEPATH", replace sheet("extraction") firstrow(var)

