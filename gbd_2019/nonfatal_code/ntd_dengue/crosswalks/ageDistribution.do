clear

if c(os) == "Unix" {
  local FILEPATH "FILEPATH"
  set odbcmgr ADDRESS
  }
else if c(os) == "Windows" {
  local FILEPATH "FILEPATH"
  }

run "FILEPATH"
	run FILEPATH
	
	create_connection_string, database(ADDRESS)
	local shared = r(conn_string)
  
odbc load, exec("SELECT age_group_id, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group WHERE age_group_id IN (2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30, 31,32,235)") `shared' clear
levelsof age_group_id, local(age_group) clean
levelsof age_start, local(age_start) clean
levelsof age_end, local(age_end) clean
  

tempfile pop
get_population, location_id(135) year_id(2006 2007 2008 2009) sex_id(1 2) age_group_id(-1) clear
save `pop'
 
clear  
cd FILEPATH
	append using FILEPATH
	
	keep if inlist(diag_princ, "A90", "A91")


/********************************************************************************\		
  CONVERT AGE & AGE_UNIT VARIABLES TO GBD AGE CATEGORIES: 
\********************************************************************************/

	gen tempAge = (age*(age_units=="2")/365.25) + /// convert ages in days to ages in years
				  (age*(age_units=="3")/12) +     /// convert ages in months to ages in years
				  (age*(age_units=="4"))        ///+ (80*(age_units=="5")) /// retain ages in years, capping it off at 80
				   if age_units!="0"
				   
				   
	
	generate age_group_id = .
	while "`age_group'" != "" {
		gettoken group age_group: age_group
		gettoken start age_start: age_start
		gettoken end age_end: age_end
		replace age_group_id = `group' if tempAge>=`start' & tempAge<`end'
		}			   

	
	
	keep if inlist(sex, "Female","Male") & !missing(age_group_id)
	replace sex = lower(sex)
    gen sex_id = (sex=="female") + 1 if !missing(sex)
	
	rename year year_id
	
	
	*rename tempAge age
	generate cases = 1
	
	collapse (sum) cases, by(age_group_id sex_id year_id) fast

	sum sex [fweight=cases]
	local pFemale = `r(mean)' - 1
    local pMale = 1 - `pFemale'

	*tostring age, force replace usedisplayformat
    merge 1:1 age_group_id sex_id year_id using `pop', assert(2 3) keep(3) nogenerate
	destring age, replace force
	
	collapse (sum) cases population, by(age_group_id) fast
	
	gen incCurve = cases / population

	expand 2, gen(sex_id)
	replace sex_id = sex_id + 1
	
	replace incCurve = incCurve * `pMale' if sex==1
	replace incCurve = incCurve * `pFemale' if sex==2
	
	keep incCurve age sex
	
	save FILEPATH, replace
