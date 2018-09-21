** ****************************************************
** Purpose: Upload to the CoD database
** ******************************************************************************************************************
 // Prep Stata
	clear all
	set more off, perm
	if c(os) == "Unix" {
		set mem 10G
		set odbcmgr unixodbc
		global j "/home/j"
	}
	else if c(os) == "Windows" {
		set mem 800m
		global j "J:"
	}
	
// Source
	global source "`1'"

// Date
	global timestamp "`2'"
	
// Username
	global username "`3'"
	
// Database locals
	global strConnection
	local description "GBD 2015 v$timestamp"
	local code_version = 3
	local pop_version = 3

	
//  Local for the directory where all the COD data are stored
	local in_dir "$j/WORK/03_cod/01_database/03_datasets"
	global in_dir "$j/WORK/03_cod/01_database/03_datasets"

// Local for the output directory
	local out_dir "$j/WORK/03_cod/01_database/02_programs/compile"
	
// Local for clustertemp outputs
	local tmp_dir "/ihme/cod/prep/01_database/13_upload"

// Log output
	capture log close _all
	log using "`in_dir'//$source//logs/13_upload_${timestamp}", replace
	
// Load source
	use "`in_dir'/$source//data/final/12_cleaned.dta", clear

// Only upload if the source variable has no leading underscore
	count if substr(source,1,1)=="_"
	if `r(N)' > 0 | substr("$source", 1, 1)=="_" {
		noisily di in red "SOURCE VARIABLE CONTAINS LEADING UNDERSCORE"
		exit
	}
	preserve
// load current outliers
	odbc load, exec("select data_id, outlier_id, age_group_id, sex_id, location_id, year_id, acause, nid, data_type_id, source, site, is_outlier, comment, outlier.username as outliers_username from cod.outlier left join cod.data using (data_id) left join cod.data_version using (data_version_id) left join shared.cause using (cause_id) where data_version.status=1 and outlier.status=1 and is_outlier=1 and source = '$source'") dsn($dsn) clear

	// double check that we aren't including 'smoothing' outliers from our previous auto-outliering scheme
	drop if comment=="Smoothing" 
	// only keep the most current outlier
	destring outlier_id, replace
	bysort data_id : egen double max_id = max(outlier_id)
	keep if outlier_id == max_id
	drop data_id outlier_id max_id
	compress
	count
	if `r(N)' == 0 {
		set obs 1
		replace source = "USER"
	}
	duplicates drop
	tempfile outliers
	save `outliers', replace
	restore
	
	merge 1:1 year_id sex_id age_group_id location_id acause nid data_type_id source site using `outliers', keep(1 3) nogen

// Prep for upload
	local username = c(strUser)
	gen username = "$username"
	gen description = "`description'"
	gen code_version = `code_version'
	gen pop_version = `pop_version'
	gen status = 2
	compress

	** make sure that there is nothing with status = 2
	preserve
		odbc load, exec("SELECT data_version_id FROM cod.data_version WHERE source='${source}' AND status=2") strConnection clear
		count
		assert `r(N)'==0
	restore

// upload data_versions
	preserve
		keep nid data_type_id source description code_version pop_version status
		order nid data_type_id source description code_version pop_version status
		sort nid data_type_id source
		duplicates drop
		compress
		count
		outsheet using "`tmp_dir'/data/data_version_${source}_$timestamp.csv", replace nolabel noquote nonames comma

		** take LOCAL out 
		odbc exec("LOAD DATA LOCAL INFILE '`tmp_dir'/data/data_version_${source}_$timestamp.csv' INTO TABLE cod.data_version FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (nid,data_type_id,source,description,code_version,pop_version,status);"), dsn($dsn)
		noisily display in red "Loaded new DATA VERSIONS into cod.data_version with status = 2"
		odbc load, exec("SELECT data_version_id,nid,data_type_id,source FROM cod.data_version WHERE source='$source' and status=2") dsn($dsn) clear
		destring data_version_id, replace
		format data_version_id %20.0g
		tempfile data_version_id
		save `data_version_id', replace
	restore
	merge m:1 nid data_type_id source using `data_version_id', keep(1 3) nogen
	
// upload data
	preserve
		
		keep data_version_id year_id location_id sex_id age_group_id cause_id cf_final cf_rd cf_corr cf_raw sample_size representative_id urbanicity_type_id site is_outlier
		order data_version_id year_id location_id sex_id age_group_id cause_id cf_final cf_rd cf_corr cf_raw sample_size representative_id urbanicity_type_id site is_outlier
		sort data_version_id year_id location_id sex_id age_group_id cause_id site
		count if is_outlier==2
		assert `r(N)'==0
		replace site = "\N" if site == ""
		count
		format cf_final cf_rd cf_corr cf_raw sample_size %16.0g
		outsheet using "`tmp_dir'/data/data_${source}_$timestamp.csv", replace nolabel noquote nonames comma
		odbc exec("LOAD DATA LOCAL INFILE '`tmp_dir'/data/data_${source}_$timestamp.csv' INTO TABLE cod.data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (data_version_id,year_id,location_id,sex_id,age_group_id,cause_id,cf_final,cf_rd,cf_corr,cf_raw,sample_size,representative_id,urbanicity_type_id,site,is_outlier);"), dsn($dsn)
		noisily display in red "Loaded new DATA into cod.data with status = 2"
		odbc load, exec("SELECT data_id,data_version_id,location_id,year_id,sex_id,age_group_id,site,cause_id FROM cod.data LEFT JOIN cod.data_version USING (data_version_id) WHERE source='$source' and status=2") dsn($dsn) clear
		destring data_id data_version_id, replace
		format data_id data_version_id %20.0g
		tempfile data_id
		save `data_id', replace
	restore
	merge 1:1 data_version_id location_id year_id sex_id age_group_id site cause_id using `data_id', keep(1 3) nogen

	summarize data_id
	local min = `r(min)'
	local max = `r(max)'


// upload outliers
	preserve
		count if is_outlier==2
		assert `r(N)'==0
		keep if is_outlier == 1
		drop username
		rename outliers_username username
		replace status = 1
		keep data_id status comment username
		order data_id status comment username
		sort data_id
		count
		outsheet using "`tmp_dir'/data/outlier_${source}_$timestamp.csv", replace nolabel noquote nonames comma

		odbc exec("LOAD DATA LOCAL INFILE '`tmp_dir'/data/outlier_${source}_$timestamp.csv' INTO TABLE cod.outlier FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (data_id,status,comment,username);"), dsn($dsn)
		noisily display in red "loaded new OUTLIERS into cod.outlier for the current data_ids"
	restore

// swap data
	local date = c(current_date)
	local time = c(current_time)
	local today = date("`date'", "DMY")
	local year = year(`today')
	local month = month(`today')
	local day = day(`today')
	local length : length local month
	if `length' == 1 local month = "0`month'"	
	local length : length local day
	if `length' == 1 local day = "0`day'"
	local timestamp = "`year'-`month'-`day' `time'"
	levelsof source,  local(sources) c
	foreach source of local sources {
		odbc exec("update cod.data_version set status_end = '`timestamp'' where source='$source' and status=1"), dsn($dsn)
		noisily display in red "Updated previous data_version timestamp with this end-date `timestamp'"
		odbc exec("update cod.data_version set status = 0 where source='$source' and status=1"), dsn($dsn)
		noisily display in red "Updated previous data_version as status = 0"
		odbc exec("update cod.data_version set status_start = '`timestamp'' where source='$source' and status=2"), dsn($dsn)
		noisily display in red "Updated new data_version timestamp with this start-date `timestamp'"
		odbc exec("update cod.data_version set status = 1 where source='$source' and status=2"), dsn($dsn)
		noisily display in red "Updated new data_version as status = 1"
	}
// make signal of finalization
	clear
	set obs 1
	gen complete = "${source}_$timestamp"
	save "`tmp_dir'/data/signal_${source}_complete.dta", replace
	noisily display in red "Signal saved"

capture log close _all
