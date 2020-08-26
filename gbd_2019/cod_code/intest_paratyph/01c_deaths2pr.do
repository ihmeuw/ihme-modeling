
*** BOILERPLATE ***
    clear all
	set more off
	set maxvar 32000
		
	if c(os) == "Unix" {
		local j ADDRESS
		local k ADDRESS
		local h ADDRESS
		}
	else {
		local j ADDRESS
		local k ADDRESS
		local h ADDRESS
		}



	adopath + FILEPATH
	run FILEPATH/get_demographics.ado
	run FILEPATH/get_cod_data.ado
	run FILEPATH/get_location_metadata.ado
	run FILEPATH/get_crosswalk_version.ado
	
	tempfile data17 data18 ages ageSex drLocs cod
	
	local date 20190812
	
	local step iterative
	

*** BACKUP AND CLEAR OUT OLD DATA ***
	foreach bundle in 17 18 {
		get_bundle_data, bundle_id(`bundle') decomp_step(`step') clear
		save FILEPATH, replace
	
		preserve
		
		keep if nid==292827
		keep seq
		if `=_N'>0 {
			export excel using FILEPATH, sheet("extraction") firstrow(variables) replace
			upload_epi_data, bundle_id(`bundle') filepath(FILEPATH) clear
			}
	
		
		restore 
		drop if nid==292827
		
		ds *, has(type string)
		foreach var of varlist `r(varlist)' {
			replace `var' = "" if trim(`var')=="."
			}
			
		foreach var of varlist underlying_nid smaller_site_unit sex_issue year_issue age_demographer age_issue measure_issue measure_adjustment {	
			capture destring `var', replace force
			}

		save `data`bundle''
		}
	


local prTyphBundle = 17
local prParaBundle = 18

foreach metric in prTyph prPara {

	use  FILEPATH, clear
	

	rename *_`metric' *
	generate effective_sample_size = sample_size


	generate sex = "Male" if sex_id==1
	replace  sex = "Female" if sex_id==2
	replace  sex = "Both" if sex_id==3

	generate source_type = "Vital registration - national" 

	replace nid = 292827
	generate unit_type = "Person"
	generate unit_value_as_published = 1
	generate measure_adjustment = 0
	generate uncertainty_type_value = 95
	generate urbanicity_type = "Mixed/both"
	generate representative_name = "Nationally representative only"
	replace  representative_name = "Representative for subnational location only" if strmatch(ihme_loc_id, "*_*")
	generate recall_type = "Point"
	generate extractor = USERNAME
	generate cv_diag_mixed = 0
	generate cv_passive = 0
	generate smaller_site_unit = 0
	generate sex_issue = 0
	generate year_issue = 0
	generate age_issue = 0
	generate age_demographer = 0
	generate measure = "proportion"
	generate measure_issue = 0
	generate response_rate = .

	drop mean_* lower_* upper_* is_estimate *region* standard_error_*

	generate seq = .
	
	
	export delimited using FILEPATH, replace
	}
	

	append using `data``metric'Bundle''
	drop if nid!=292827
	
	export excel using  FILEPATH, sheet("extraction") firstrow(variables) replace
	
	upload_epi_data, bundle_id(``metric'Bundle') filepath(FILEPATH) clear
	}
