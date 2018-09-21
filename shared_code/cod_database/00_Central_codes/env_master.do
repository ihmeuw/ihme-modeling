// Purpose: Query the mortality envelope data and save to a file on clustertmp

// J: drive alias
if c(os) == "Windows" {
	global j "J:"
}
if c(os) == "Unix" {
	global j "/home/j"
	set odbcmgr unixodbc
}

// What location set?
local lsid 35

** Set up driver for connection string
run "$j/WORK/10_gbd/00_library/functions/create_connection_string.ado"
create_connection_string
local conn_string = r(conn_string)

// Get the year, location id, sex, age, mean_pop, mean_env, and iso3
// Restrict output version to the most recent
// Restrict location set version id to CoD heirarchy
// Restrict age group id to between 2 and 21 (these are standard GBD age groups, from 0-6 days to 80+ years)
// Restrict sex to just males and females
#delimit ;
odbc load, exec("
	SELECT 
		o.year_id as year, o.location_id, o.sex_id as sex, 
		o.age_group_id, o.mean_pop as pop, o.mean_env_hivdeleted as env,
		loc.path_to_top_parent, o.output_version_id 
	FROM 
		mortality.output o 
	INNER JOIN 
		mortality.output_version ov
			ON o.output_version_id = ov.output_version_id and ov.is_best = 1 
	INNER JOIN 
		shared.age_group ag
			ON o.age_group_id = ag.age_group_id 
	INNER JOIN 
		shared.location_hierarchy_history loc 
			ON o.location_id=loc.location_id 
			AND loc.location_set_version_id IN (
				SELECT max(location_set_version_id) AS location_set_version_id
				FROM shared.location_set_version
				WHERE location_set_id=`lsid'
				)
			AND loc.level >= 3
	WHERE 
		o.age_group_id BETWEEN 2 and 21 and 
		o.sex_id IN(1,2) and 
		o.year_id BETWEEN 1970 and 2015
")  `conn_string' clear ;
#delimit cr

local ovid = output_version_id in 1
drop output_version_id

if c(os) == "Unix" {
	save "/ihme/cod/prep/01_database/mortality_envelope.dta", replace
	save "/ihme/cod/prep/01_database/mortality_envelope_v`ovid'.dta", replace
}
