*NTDs Chagas disease
*Description: Estimate birth prevalence

*** BOILERPLATE ***
	clear all
	set more off  

	if c(os) == "Unix" {
		local j "FILEPATH"
		set odbcmgr unixodbc
		}
		
	else if c(os) == "Windows" {
		local j "FILEPATH"
		}

	run FILEPATH/get_location_metadata.ado
	run FILEPATH/get_draws.ado
	
*** SET ENVIRONMENTAL LOCALS (PATHS, FILENAMES MODEL NUBMERS, ETC) ***

	local model ADDRESS

	* new model / new outfile
	
	tempfile locMeta pregTemp pregTempMaster mergingTemp appendTemp grs

	import delimited ADDRESS/pr_estimates_new_demo.csv, clear
	gen nPreg = npreg
	keep if nPreg > 0
	levelsof age_group_id, local(ages) clean
	*new prPreg
	
	replace year_id = year_id + 1

	keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022, 2023, 2024)
	save `pregTemp'

	get_location_metadata, location_set_id(ADDRESS) release_id(ADDRESS) clear
	levelsof location_id if (strmatch(lower(region_name), "*latin america*") | inlist(ihme_loc_id, "BLZ", "GUY", "SUR")), local(locations) clean
	save `locMeta'
	
*** GR's
	import delimited FILEPATH/ntd_chagas_lgr.csv, clear

	drop year_end
	rename year_start year_id
	duplicates drop
	save `grs'

	local years 1990 1995 2000 2005 2010 2015 2020 2022 2023 2024
	
	get_draws, source(epi) age_group_id(`ages') gbd_id_type(modelable_entity_id) gbd_id(ADDRESS) year_id(`years') location_id(`locations') release_id(ADDRESS) sex_id(ADDRESS) measure_id(ADDRESS) version_id(`model') clear
    
	merge 1:1 location_id year_id age_group_id using `pregTemp', gen(prPregMerge)
	*stata for left join
	keep if prPregMerge == 3
	
	merge m:1 location_id using `locMeta'
	keep if _merge == 3
	
	merge m:1 location_id year_id using `grs', gen(grMerge)
	keep if grMerge == 3
	
	keep if value_endemicity == 1 | is_estimate == 0 
	levelsof location_id, local(endemicLocations)

*** DERIVE PARAMETERS OF BETA DISTRIBUTION FOR RATE OF VERTICAL TRANSMISSION ***
	local mu    = 0.047  
	local sigma = (0.056 - 0.039) / (invnormal(0.975) * 2)
	local alpha = `mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2 
	local beta  = `alpha' * (1 - `mu') / `mu' 
	  
	forvalues i = 0/999 {
		local vertical    = rbeta(`alpha', `beta')  
		quietly gen bPrev_`i' = nPreg * draw_`i' * `vertical'
		}
	
	collapse (sum) nPreg bPrev_*, by(location_id ihme_loc_id location_name year_id) fast
	
	forvalues i = 0/999 {
		quietly replace bPrev_`i' = bPrev_`i' / nPreg
		}
 
*** FORMAT FOR EPI DATABASE ***
	egen mean  = rowmean(bPrev_*)
	egen upper = rowpctile(bPrev_*), p(97.5)
	egen lower = rowpctile(bPrev_*), p(2.5)

	generate int year_start = year_id
	rename year_id year_end

	drop bPrev_* nPreg 

	generate age_start = 0
	generate age_end = 0
	generate long nid = ADDRESS
	generate underlying_nid = .

	generate acause = "ntd_chagas"

	generate source_type = "Mixed or estimation"
	generate data_type = "Other"

	generate sample_size = .
	generate effective_sample_size = .
	generate standard_error = .
	generate cases = .

	generate sex = "Both"

	generate unit_type = "Person"
	generate unit_value_as_published = 1
	generate measure_adjustment = 0
	generate uncertainty_type_value = 95
	generate urbanicity_type = "ADDRESS"
	generate representative_name = "ADDRESS"
	replace  representative_name = "ADDRESS" if strmatch(ihme_loc_id, "*_*")
	generate recall_type = "Point"
	generate extractor = "ADDRESS"
	generate is_outlier = 0
	generate cv_diag_mixed = 0
	generate cv_passive = 0
	generate smaller_site_unit = 0
	generate sex_issue = 0
	generate year_issue = 0
	generate age_issue = 0
	generate age_demographer = 0
	generate measure = "prevalence"
	generate measure_issue = 0
	generate response_rate = .
	generate note_SR = "ADDRESS"
	generate sampling_type = ""
	generate recall_type_value = .
	generate uncertainty_type = ""
	generate input_type = ""
	generate design_effect = .

	generate cv_blood_donor = 0
	generate cv_subn_endemic = 0

	generate seq = .

	*save out
	export delimited FILEPATH/birth_prev.csv
	save  FILEPATH/birthPrev.dta, replace
