*** BOILERPLATE ***
    clear
	set more off
	local FILEPATH
	local FILEPATH

	run FILEPATH
	run FILEPATH
	run FILEPATH
	run FILEPATH
	
	local cause_dir "FILEPATH"
	local run_dir "FILEPATH"

	local release_id ADDRESS
	tempfile ages appendTemp
	
*** CREATE CONNECTION STRING TO SHARED DATABASE ***

	create_connection_string, ADDRESS(ADDRESS)
	local ADDRESS = r(ADDRESS)
	
	
*** PULL AGE GROUP DATA ***
	get_age_metadata, age_group_set_id(ADDRESS) release_id(`release_id')
	rename age_group_years_start age_start
	rename age_group_years_end age_end
	drop age_group_weight_value
	generate mergeId = age_group_id
	replace  mergeId = 5 if age_group_id<5 | age_group_id == 238 | age_group_id== 34 | age_group_id == 388 | age_group_id == 389
	replace  mergeId = 32 if age_group_id==235
	save `ages', replace


*** PULL & PROCESS IMMIGRANT AGE/SEX PATTERN DATA *** 
	import delimited `FILEPATH, clear
	
	rename per_fb_male pctAgeSex1
	rename per_fb_female pctAgeSex2
	
	gen mergeId = _n + 4
	replace mergeId = 30 if mergeId==21
	replace mergeId = 31 if mergeId==22
	replace mergeId = 32 if mergeId==23
	collapse (sum) pctAgeSex*, by(mergeId) 
	
*** DETERMINE PROPORTIONS BY GBD AGE GROUPS ****
	merge 1:m mergeId using `ages', nogenerate
	replace pctAgeSex1 = pctAgeSex1 * (age_end - age_start) / 6 if mergeId==5
	replace pctAgeSex2 = pctAgeSex2 * (age_end - age_start) / 6 if mergeId==5
	replace pctAgeSex1 = pctAgeSex1 / 2 if mergeId==32
	replace pctAgeSex2 = pctAgeSex2 / 2 if mergeId==32
	keep age_group_id pctAgeSex1 pctAgeSex2
	
	reshape long pctAgeSex, i(age_group_id) j(sex_id)
	replace pctAgeSex = pctAgeSex / 100

	
*** SAVE ***
	save FILEPATH, replace
	export delimited FILEPATH, replace
