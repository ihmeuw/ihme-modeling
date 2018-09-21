	
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
	
	tempfile ages

*** CREATE CONNECTION STRING TO SHARED DATABASE ***
	noisily di "{hline}" _n _n "Connecting to the database"
		
	create_connection_string, database(ADDRESS)
	local shared = r(conn_string)
	
	
*** PULL AGE GROUP DATA ***
	get_demographics, gbd_team(epi) clear
	odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`r(age_group_ids)'", " ", ",", .)')") `shared' clear
	save `ages'

	
*** PULL IN DIGESTIVE OUTCOME DATA ***	
	import delimited "FILEPATH/digestPr.csv", clear

	collapse (sum) digest totaln, by(age)
	gen prdigest = digest/totaln


*** PREP AGES ***	
	rename   age age_start
	generate age_end = age+9
	replace age_end = 85 if age_end==65

	append using `ages'
	replace age_end = 100 if age_end==125
	
	egen age_mid = rowmean(age_start age_end)
	sort age_mid

	
*** PREP DATA FOR MODELLING ***
	egen nDigest = total(digest)
	egen nTotal = total(totaln)
	gen totalPr = nDigest / nTotal
	local nTotal = nTotal in 1
	local totalPr = totalPr in 1

	gen rr = prdigest / totalPr


*** RUN MODEL ***
	nl gom3: rr age_mid
	predict g3
	replace g3 = 0 if age_mid<.1

	
*** CREATE DRAWS ***
	drop if missing(age_group_id)
	forvalues i = 0/999 {
		quietly {
			local temp = rbinomial(`nTotal', `totalPr') / `nTotal'
			generate draw_`i' = `temp' * g3
			replace draw_`i' =  0 if missing(draw_`i')
			}
		}


 *** SAVE OUTPUT ***
	keep age_group_id draw_*
	
	expand 2, generate(sex_id)
	replace sex_id = sex_id + 1
	
	expand 2, generate(modelable_entity_id)
	replace modelable_entity_id = modelable_entity_id + 1453
	
	 forvalues i = 0 / 999 {
		quietly {
		replace draw_`i' = draw_`i' * 0.6 if modelable_entity_id==1453
		replace draw_`i' = draw_`i' * 0.4 if modelable_entity_id==1454
		}
	    }
	
	generate outcome = "digest_mild" if modelable_entity_id==1453
	replace  outcome = "digest_mod"  if modelable_entity_id==1454
	
	save "FILEPATH/digestPrDraws.dta", replace
