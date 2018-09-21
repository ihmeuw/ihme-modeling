// Adapt idie upload code specifically to 45q15 process


// prep stata
	clear all
	set more off
	set maxvar 32000
	set mem 1g
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		local db "ADDRESS"
	}
	
	local out_dir "FILEPATH"
	local current_data "FILEPATH"

local date = c(current_date)
local time = c(current_time)
odbc exec("insert into idie_versions (description, status, assignee) values('45q15 `date' `time'',1,'45q15_est')"), dsn(`db') 
// Grab the version that you just created
odbc load, exec("select version_id from idie_versions where description = '45q15 `date' `time''") dsn(`db')
local ver_ddm = version_id[1]

// prep type list
	odbc load, exec("select * from idie_types_versioned ORDER BY type_id") dsn(`db') clear
	replace type_short=lower(type_short)
	tempfile types
	save `types', replace

// prep method list
	odbc load, exec("select * from idie_methods_versioned ORDER BY method_id") dsn(`db') clear
	tempfile methods
	save `methods', replace

// prep cleaned citation list
	insheet using "FILEPATH/source-id_source-citation_link.csv", comma clear
	rename suggested_mortality_citation source_citation
	keep source_id source_citation
	duplicates drop
	tempfile citations
	save `citations', replace
	

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// DDM

// prep estimates
if "`ver_ddm'" != "" {
	insheet using "`current_data'/estimates_ddm.csv", comma clear
	
	gen gbd_final="NULL"	
	replace source = lower(source)
	rename source type_short
	replace type_short = "dsp <1996" if type_short == "dsp before 1996"
	replace type_short = "dsp '96-'03" if type_short == "dsp 1996 to 2003" | type_short == "dsp-1996-2000"
	replace type_short = "dsp >2003" if type_short == "dsp 2004 and after" | type_short == "dsp-2004-2010"
	
	merge m:1 type_short using `types', keep(3) nogen
	
	keep if year >= 1949.5 & year <= 2013.5
	replace sex = "1" if sex == "male"
	replace sex = "2" if sex == "female"
	replace sex = "3" if sex == "both"
	
	gen version_id = "`ver_ddm'"

	// upload data
	keep version_id iso3 sex type_id year u5 first secondmed secondlower secondupper truncmed trunclower truncupper final_est gbd_final reportingyear final_lower final_upper
	order version_id iso3 sex type_id year u5 first secondmed secondlower secondupper truncmed trunclower truncupper final_est gbd_final reportingyear final_lower final_upper
	sort iso3 year type_id
	compress
	
	outsheet using "`out_dir'/idie_ddm.txt", replace nolabel noquote nonames	
	odbc exec("DELETE FROM idie_comp_versioned WHERE version_id = `ver_ddm';"), dsn(`db')
	odbc exec("LOAD DATA LOCAL INFILE '`out_dir'/idie_ddm.txt' INTO TABLE idie_comp_versioned FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' (version_id,iso3,sex,type_id,year,u5,first,second_med,second_lower,second_upper,trunc_med,trunc_lower,trunc_upper,final_est,gbd_final,reporting_year,final_lower,final_upper);"), dsn(`db')
}


