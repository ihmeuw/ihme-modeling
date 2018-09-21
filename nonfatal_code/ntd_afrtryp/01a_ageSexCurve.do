

*** BOILERPLATE ***
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix FILEPATH
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		global prefix FILEPATH
		}
		
	
	tempfile ages


*** LOAD SHARED FUNCTIONS ***			
	adopath + FILEPATH	
	run FILEPATH/get_demographics.ado
	run FILEPATH/create_connection_string.ado
	
	create_connection_string, database(ADDRESS)
	local shared = r(conn_string)
	
	
*** PULL LIST OF MODELLED AGE GROUPS AND CONVERT FROM SPACE- TO COMMA-DELIMITED VECTOR ***	
	get_demographics, gbd_team(cod) clear
	local age_groups `=subinstr("`r(age_group_ids)'", " ", ",", .)'
	

*** PULL START AND END YEARS FOR ALL MODELLED AGE GROUPS ***	
	odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (`age_groups')") `shared' clear
	save `ages'

	
*** PULL IN AGE-SPECIFIC DATA ***	
	import excel using "$FILEPATH/me_1462_ts_2015_10_15__164408.xlsx", sheet("extraction") firstrow clear
	drop if age_start == 0 & inlist(age_end, 99, 100) 

	
*** PREP FOR MODEL ***	
	append using `ages'

	egen age_mid = rowmean(age_start age_end)
	replace cases = mean * effective_sample_size
	replace effective_sample_size = 1 if missing(effective_sample_size)

	capture drop ageS*
	capture drop ageCurve
	mkspline ageS = age_mid, cubic knots(1 5 10 60) displayknots

	
*** MODEL AND PREDICT AGE/SEX PATTERN ***	
	mepoisson cases ageS*, exp(effective_sample_size) || location_id:
	generate ageSexCurve = _b[_cons] + (ageS1 * _b[ageS1]) + (ageS2 * _b[ageS2]) + (ageS3 * _b[ageS3])  
	predict ageSexCurveSe, stdp

	keep if missing(row_num)
	keep age_group_id ageSexCurve*
	expand 2, gen(sex_id)
	replace sex_id = sex_id + 1

	
*** SAVE ***	
	save FILEPATH/ageSexCurve.dta, replace
