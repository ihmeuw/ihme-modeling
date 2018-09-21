
*** BOILERPLATE ***
    clear all
	set more off
	set maxvar 32000
		
	if c(os) == "Unix" {
		local j FILEPATH
		}
	else {
		local j FILEPATH
		}


	adopath + FILEPATH
	run FILEPATH/get_demographics.ado
	run FILEPATH/get_cod_data.ado
	run FILEPATH/get_epi_data.ado
	run FILEPATH/upload_epi_data.ado
	run FILEPATH/get_location_metadata.ado
	run FILEPATH/create_connection_string.ado
	
	tempfile data17 data18 ages ageSex drLocs cod
	
	
	
*** BACKUP AND CLEAR OUT OLD DATA ***
	foreach bundle in 17 18 {
		get_epi_data, bundle_id(`bundle') clear
		save FILEPATH/`bundle'_backup_`=subinstr(trim("`c(current_date)'"), " ", "_", .)'_`=subinstr("`c(current_time)'", ":", "_", .)'.dta, replace
	
		keep seq
		if `=_N'>0 {
			export excel using FILEPATH/clear555.xlsx, sheet("extraction") firstrow(variables) replace
			upload_epi_data, bundle_id(555) filepath(FILEPATH/clear555.xlsx) clear
			}
	
		
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

	use FILEPATH/dataRichCodDataDraws.dta, clear
	
	drop regionMean regionSd regionZ regionAge*

	bysort region_id sex: egen regionMean = mean(mean_`metric')
	bysort region_id  sex: egen regionSd = sd(mean_`metric')
	gen regionZ = (mean_`metric' - regionMean) / regionSd
	bysort region_id sex age_start: egen regionAgeMean = mean(mean_`metric')
	bysort region_id  sex age_start: egen regionAgeSd = sd(mean_`metric')
	gen regionAgeZ = (mean_`metric' - regionAgeMean) / regionAgeSd 


	rename *_`metric' *
	generate effective_sample_size = sample_size

	rename year_id year_start
	generate year_end = year_start

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
	generate extractor = "NAME"
	generate is_outlier = regionAgeZ>2 & regionZ>2
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

	drop mean_* lower_* upper_* is_estimate *region*

	generate seq = .

	append using `data``metric'Bundle''

	export excel using  FILEPATH/cod2epi_`metric'.xlsx, sheet("extraction") firstrow(variables) replace
	upload_epi_data, bundle_id(``metric'Bundle') filepath(FILEPATH/cod2epi_`metric'.xlsx) clear
	}
