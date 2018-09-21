	
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
	
	tempfile ages ageSex

*** CREATE CONNECTION STRING TO SHARED DATABASE ***
	noisily di "{hline}" _n _n "Connecting to the database"
		
	create_connection_string, database(ADDRESS)
	local shared = r(conn_string)
	
	
*** PULL AGE GROUP DATA ***
	get_demographics, gbd_team(epi) clear
	odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`r(age_group_ids)'", " ", ",", .)')") `shared' clear
	save `ages'
	
	expand 2, generate(sex_id)
	replace sex_id = sex_id + 1
	save `ageSex'
	
	
*** PULL IN AFIB OUTCOME DATA ***
	import delimited FILEPATH/afibByAge.csv, bindquote(strict) case(preserve) clear 

	
*** SORT OUT AGES ***	
	generate age_start = round(age-2, 5)
	replace  age_start = 95 if age>=95
	generate age_end   = age_start + 4
	drop age

	collapse (sum) n*, by(chagas age* sex)
	rename sex sex_id
	
	merge m:1 age_start using `ages', assert(2 3) keep(3) nogenerate 
	egen age_mid = rowmean(age_start age_end)
	
	
*** RUN MODEL ***
	glm nAfib c.age_mid##c.age_mid i.sex_id i.chagas, family(binomial nTotal) vce(bootstrap, reps(200)) irls
	predict prAfib, xb
	predict prAfibSe, stdp


*** CLEAN UP AND CREATE DRAWS ***	
	reshape wide prAfib* n*, i(age_group_id sex_id) j(chagas)

	gen chagasPrev = nTotal1 / (nTotal1 + nTotal0)
	gen prPop = nAfib1 + (nAfib1==0)

	forvalues i = 0/999 {
		quietly{
			local random = rnormal(0, 1)
			generate draw_`i' = exp(prAfib1 + (`random'*prAfibSe1)) - ((exp(prAfib0 + (`random'*prAfibSe0)) - chagasPrev * exp(prAfib1 + (`random'*prAfibSe1))) / (1 - chagasPrev))
			replace draw_`i' = draw_`i'  * rbeta(prPop * .4, prPop * .6)
			}
		}
	
*** SAVE OUTPUT ***
	merge 1:1 age_group_id sex_id using `ageSex'

	sort  sex_id age_group_id 
	keep  sex_id age_group_id draw_*
	order sex_id age_group_id draw_*
	
	generate modelable_entity_id = 1452
	generate outcome = "afib"
	
	save FILEPATH/afibPrDraws.dta, replace



