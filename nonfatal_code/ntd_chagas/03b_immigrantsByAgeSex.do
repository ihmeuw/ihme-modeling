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
	
	tempfile ages appendTemp
	
	local dataDir FILEPATH
	
*** CREATE CONNECTION STRING TO SHARED DATABASE ***
	noisily di "{hline}" _n _n "Connecting to the database"
		
	create_connection_string, database(ADDRESS)
	local shared = r(conn_string)
	
	
*** PULL AGE GROUP DATA ***
	get_demographics, gbd_team(epi) clear
	odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`=subinstr("`r(age_group_ids)'", " ", ",", .)')") `shared' clear
	generate mergeId = age_group_id
	replace  mergeId = 5 if age_group_id<5
	replace  mergeId = 32 if age_group_id==235

	save `ages'


*** PULL & PROCESS IMMIGRANT AGE/SEX PATTERN DATA *** 
	import excel "FILEPATH\FINAL_Statistical-Portrait-of-the-Foreign-Born-2012.xlsx", sheet("9.Sex&Age") cellrange(A11:E29) clear
	keep A C E
	rename A age
	rename C pctAgeSex1
	rename E pctAgeSex2
	gen mergeId = _n + 4
	replace mergeId = 30 if mergeId==21
	replace mergeId = 31 if mergeId==22
	replace mergeId = 32 if mergeId==23
	collapse (sum) pctAgeSex*, by(mergeId) 
	
	
*** DETERMINE PROPORTIONS BY GBD AGE GROUPS ****
	merge 1:m mergeId using `ages', nogenerate
	replace pctAgeSex1 = pctAgeSex1 * (age_end - age_start) / 5 if mergeId==5
	replace pctAgeSex2 = pctAgeSex2 * (age_end - age_start) / 5 if mergeId==5
	replace pctAgeSex1 = pctAgeSex1 / 2 if mergeId==32
	replace pctAgeSex2 = pctAgeSex2 / 2 if mergeId==32
	keep age_group_id pctAgeSex1 pctAgeSex2
	reshape long pctAgeSex, i(age_group_id) j(sex_id)
	replace pctAgeSex = pctAgeSex / 100

	
*** SAVE ***
	save FILEPATH\immigrantsByAgeSex.dta, replace
