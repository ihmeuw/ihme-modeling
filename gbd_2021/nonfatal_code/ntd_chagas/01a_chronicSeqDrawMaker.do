* NTDs - Chagas disease 
* Description: post processing, draws the proportion of prevalence for chronic outcomes, 

*** BOILERPLATE ***
    clear
	set more off
	local FILEPATH
	local FILEPATH

	run FILEPATH
	run FILEPATH
	run FILEPATH
	run FILEPATH

	tempfile mergeTemp appendTemp

	local cause_dir "FILEPATH"
	local run_dir "FILEPATH"
	local gbd_round_id ADDRESS

*** CREATE CONNECTION STRING TO SHARED DATABASE ***

		
	create_connection_string, ADDRESS(ADDRESS)
	local ADDRESS = r(ADDRESS)
	
*** PULL AGE GROUP DATA ***
	get_age_metadata, age_group_set_id(ADDRESS) gbd_round_id(`gbd_round_id')
	rename age_group_years_start age_start
	rename age_group_years_end age_end
	drop age_group_weight_value
	expand 2, generate(sex_id)
	replace sex_id = sex_id + 1
	save `mergeTemp'

*** RUN SCRIPTS TO CREATE OUTCOME DRAWS & APPEND ***
	local i = 2
	foreach outcome in afib digest hf {
		do FILEPATH/`outcome'PrByAge.do
		
		use `mergeTemp', clear
		merge 1:m age_group_id sex_id using FILEPATH, nogenerate
		
		if "`outcome'"!="afib" append using `appendTemp'
		save `appendTemp', replace
		
		local ++i
		}
		

	foreach var of varlist draw_* {
		quietly replace `var' = 0 if missing(`var')
		}

order  model_idoutcome age_group_id age_start age_end sex_id
save FILEPATH, replace
export delimited FILEPATH, replace


 /* to hold