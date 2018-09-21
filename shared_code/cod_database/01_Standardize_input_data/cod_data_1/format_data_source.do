** ********************************************************************************************* **
// Purpose: Clean and standardize raw input data 
** ********************************************************************************************* **

// PREP STATA
	capture restore
	clear all
	set mem 1000m

	set more off
	ssc install bygap
	
	// set the prefix for whichever os we're in
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" {
		global j "/home/j"
		set odbcmgr unixodbc
	}
	
** ********************************************************************************************* **
// Name the data source
	local data_name 

// Establish data import directories 
	local jdata_in_dir "$j/DATA/"
	local cod_in_dir "$j/WORK/03_cod/01_database/03_datasets/`data_name'/data/raw/"
** ********************************************************************************************* **
** IMPORT RAW DATA **
	// If necessary append  multiple datasets


** INITIAL CLEANING **
// Remove unnecessary rows and columns from data. Each variable should be long, with the exception of age.

	// cause (string)
	// Format the cause list from compiled datasets to be consistent. Also note if the data dataset doesn't cover all deaths (i.e. a study focusing only on injuries), create a row for "cc_code" (combined code) for each country year so cause fractions can be calculated later. "cc_code" should represent all deaths attributed to causes of death other than those that are the focus of the study. 
	gen cause = "" 
	
	// cause_name (string)
	// If the causes are just codes, we will need a cause_name to carry its labels, if available. Otherwise leave blank.
	gen cause = ""

	// year (integer) 
	// Generate a year value that is appropriate for each observation and must cover a whole year.
	gen year = .
	
	// sex (integer)
	// sex=1 if Male, sex=2 if Female, sex=9 if Unknown
	gen sex == .
	
	// deaths (integer)
	// Use the prefix "deaths" to store death counts wide by age group, following the WHO age codes format (30 groups). If deaths are in percentages or rates, find the proper denominator to convert into deaths.
	gen deaths1 = .
	gen deaths2 = .
	** ... continue up to deaths26
	gen deaths91 = .
	gen deaths92 = .
	gen deaths93 = .
	gen deaths94 = .
	
	// frmat (integer), im_frmat (integer)
	// Identify and label the age format used to group ages. If the dataset is microdata, collapse to sum into deaths by location, year, age, sex, and cause; then label as standard age format. If the dataset is tabulated, find the corresponding WHO age format here "J:\WORK\03_cod\02_datasets\programs\agesex_splitting\documentation\Age formats documentation.xlsx".
	// frmat refers to ages over 1 year, im_frmat refers to ages under 1 year
	gen frmat = .
	gen im_frmat = .
	
** ********************************************************************************************* **
** SOURCE IDENTIFIERS **
	// source (string)
	// Create a variable to identify the origin of each country year within the data source.
	capture drop source
	gen source = "`data_name'"
	
	// source_label (string)
	// If the data source is different across appended datasets give each dataset a unique identifier, otherwise label it the same as the source column.
	gen source_label = ""
	
	// NID (integer)
	// A unique GHDx identifer for each data source in the GHDx. If the data source is different across appended datasets multiple NIDs may be necessary.
	gen NID = .

	// source_types (string)
	// Identify the underlying mode of data collection. The causes of database stores the following data types: cancer registry, surveillance, survey/census,  police records, vital registration, verbal autopsy
	gen source_type = ""
	
	// national (numeric) 
	// Determine whether the data sources as nationally representative or not nationally representative. 
	// 1= yes, 0=no
	gen national = .
		
	// list (string)
	// Determine if the cause codes in the data uses shared maps and packages or the cause codes have a maps and packages specific to this dataset. Data sources with shared maps and packages will be labeled with the corresponding source name (i.e. ICD10), otherwise the list will take the same name as the source. There must only be one value for list in the whole dataset.
	gen list = ""
	
** ********************************************************************************************* **
** COUNTRY IDENTIFIERS **
	// location_id (numeric) 
	// Fill in numeric identifier of the location name, only if it is a supported subnational location.
	gen location_id = .
		
	// iso3 (string)
	// Fill in three letter country code. 
	gen iso3 = ""

	// subdiv (string)
	// Fill in detailed site information about the location if location_id does not represent the entire location, otherwise leave blank. 
	gen subdiv = ""
	
** ********************************************************************************************* **
** STANDARDIZE AGE GROUPINGS ***
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/format_gbd_age_groups.do" 
** ********************************************************************************************* **
// SPECIAL TREATMENTS ACCORDING TO RESEARCHER/COLLABORATORS AND/OR SOURCE-SPECIFIC ADJUSTMENTS 


** ********************************************************************************************* **
** CLEAN AND EXPORT STANDARDIZED DATASET **
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/export.do" `data_name'
	
	
