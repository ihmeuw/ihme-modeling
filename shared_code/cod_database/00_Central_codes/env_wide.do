** Code location: "J:/WORK/03_cod/01_database/02_programs/prep/code/env_wide.do"

** set the prefix for whichever os we're in
if c(os) == "Windows" {
	global j "J:"
}
if c(os) == "Unix" {
	global j "/home/j"
	set odbcmgr unixodbc
}

** What location version?
local lsvid 38

** Set up driver for connection string
forvalues dv=4(.1)6 {
	local dvfmtd = trim("`: di %9.1f `dv''")
	cap odbc query, conn("DRIVER={strConnection};SERVER=strServer;UID=strUser;PWD=strPassword")
	if _rc==0 {
		local driver "strConnection `dvfmtd'"
		local db_conn_str "DRIVER={`driver'};SERVER=strServer; UID=strUser; PWD=strPassword"
		continue, break
	}
}

** Get the year, location id, sex, age, mean_pop, mean_env, and iso3
** Restrict output version to the most recent
** Restrict location set version id to CoD heirarchy
** Restrict age group id to between 2 and 21 (these are standard GBD age groups, from 0-6 days to 80+ years)
** Restrict sex to just males and females
odbc load, exec("SELECT ihme_loc_id as iso3, location_id AS iso3_location_id FROM shared.location_hierarchy_history WHERE location_set_version_id = `lsvid' and level = 3") conn("`db_conn_str'") clear
tempfile loc_iso3
save `loc_iso3', replace

capture use "/ihme/cod/prep/01_database/mortality_envelope.dta", clear
if _rc !=0 {
	noisily di in red "NO POP FILE; REGENERATING"
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/env_master.do"
}
tempfile pop
save `pop', replace

drop if !regexm(path_to_top_parent, "^1,[0-9]+,[0-9]+,[0-9]+")
** make subnational iso3s into their parent country's iso3
gen iso3_location_id = regexs(2) if regexm(path_to_top_parent, "^(1,[0-9]+,[0-9]+,)([0-9]+)(.*)$")
count if iso3_location_id==""
assert r(N)==0
destring iso3_location_id, replace	
merge m:1 iso3_location_id using `loc_iso3', nogen keep(3)
count if iso3==""
assert r(N)==0

** fix Puerto Rico
replace iso3="PRI" if location_id==385
replace location_id=. if iso3=="PRI"
replace location_id=. if iso3_location_id==location_id

keep location_id iso3 year age_group_id sex pop env
** convert age ids to standard WHO age format in J:\WORK\03_cod\02_datasets\programs\age_sex_splitting\documentation\Age formats documentation.xlsx
** ids 6-21 converts to 7-22 (GBD age ids)
gen age = age_group_id+1
** 2-5 easier to convert 'by hand'
replace age = 91 if age_group_id==2
replace age = 93 if age_group_id==3
replace age = 94 if age_group_id==4
replace age = 3 if age_group_id==5
drop age_group_id
** make age wide
reshape wide pop env, i(iso3 location_id year sex) j(age)
order year iso3 location_id sex

		