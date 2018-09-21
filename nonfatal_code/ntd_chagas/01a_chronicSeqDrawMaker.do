
*** BOILERPLATE ***
    clear
	set more off
	
	if c(os) == "Unix" {
		local j FILEPATH
		}
	else {
		local j FILEPATH
		}

	run FILEPATH/get_demographics.ado
	run FILEPATH/create_connection_string.ado
	
	tempfile mergeTemp appendTemp
	
	local dataDir FILEPATH/data

*** CREATE CONNECTION STRING TO SHARED DATABASE ***
	noisily di "{hline}" _n _n "Connecting to the database"
		
	create_connection_string, database(shared)
	local shared = r(conn_string)
	
	
*** PULL AGE GROUP DATA ***
	get_demographics, gbd_team(epi) clear
	odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`r(age_group_ids)'", " ", ",", .)')") `shared' clear
	expand 2, generate(sex_id)
	replace sex_id = sex_id + 1
	save `mergeTemp'


*** RUN SCRIPTS TO CREATE OUTCOME DRAWS & APPEND ***
	local i = 2
	foreach outcome in afib digest hf {
		do FILEPATH/01`=word("`c(alpha)'", `i')'_`outcome'PrByAge.do
		
		use `mergeTemp', clear
		merge 1:m age_group_id sex_id using `dataDir'/`outcome'PrDraws.dta, nogenerate
		
		if "`outcome'"!="afib" append using `appendTemp'
		save `appendTemp', replace
		
		local ++i
		}
		

	foreach var of varlist draw_* {
		quietly replace `var' = 0 if missing(`var')
		}

order  modelable_entity_id outcome age_group_id age_start age_end sex_id
		
save FILEPATH/chronicSeqPrDraws.dta, replace
