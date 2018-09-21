** ************************************************************************************************* **
** Author:NAME 
** Date created: 2/26/2015						
** Purpose: Format war, terrorism, and legal intervention data for the CoD prep code						
** Code Location: 2/26/2015
** Modified on date: 1/3/17
** by:NAME 
** ************************************************************************************************* **

** PREP STATA**
	clear all
	set mem 1000m
	pause on
	set more off
	//ssc install bygap
	
	// set the prefix for whichever os we're in
	if c(os) == "Windows" {
		global j ""
	}
	if c(os) == "Unix" {
		global j ""
		global h_cod_data "FILEPATH"
		set odbcmgr unixodbc
	}
	
** *********************************************************************************************
**
// What we call the data: same as folder name
	local data_name _Shock

// Establish data import directories 

	local jdata_in_dir "FILEPATH"
	local cod_in_dir "FILEPATH"
	log close _all
	log using "FILEPATH", replace
	
** *********************************************************************************************
**
// If appending multiple: best practice is to loop the import, cleaning, cause, year, sex, and age processes together.
** IMPORT **
	use "`cod_in_dir'/draws.dta", clear

** INITIAL CLEANING **
	
** CAUSE AND CAUSE_NAME (cause, cause_name) **

	gen cause_name = ""

** YEAR (year) **
	// already in the data

** SEX (sex) **

** AGE VARIABLES (deaths#, frmat, im_frmat) **
	** TEMP 04/11/2016 for yemen 2015, got the age wrong but no time to rerun draws:

	** ONCE THIS ASSERTION FAILS, TAKE OUT ALL OF THIS CODE FOR YEMEN 2015 war
	// assert deaths>0 if age==26 & iso3=="YEM" & year==2015 & cause=="war"
	// assert deaths>0 if age==91 & iso3=="YEM" & year==2015 & cause=="war"
	// assert deaths==0 if !inlist(age,91,26) & iso3=="YEM" & year==2015 & cause=="war"
	// replace age = 7 if age==26 & iso3=="YEM" & year==2015 & cause=="war"
	** 1/3/17: replacing Yemen deaths to age =26 for smoothing on formatting -> mapping -> agesex -> upload process... return to this if needed.
	* replace age = 26 if age != 26
	

	** YEM fixes for duplicate all age data;  4/14/17
	preserve
	keep if iso3 == "YEM" & year == 2016 & cause == "inj_war_war" & age == 26
	egen max_deaths = max(deaths), by(iso3 year sex age sim)
	keep if deaths == max_deaths
	isid iso3 year sex age sim cause
	drop max_deaths
	tempfile yem_16
	save `yem_16', replace
	restore
	drop if iso3 == "YEM" & year == 2016 & cause == "inj_war_war" & age == 26
	append using `yem_16'

	** age now exists in draws, so reshape on it
	reshape wide deaths, i(iso3 year sex cause sim) j(age)
	** assert the data structure from when this was last formatted
	** 2016/04/11 NAME (this is under 5 yemen deaths) ; changed to 94
	confirm variable deaths94
	
	// Format deaths for 1988 Armenia earthquake (we have specific age, and a sex breakdown)
	//gen deaths91 = deaths26 * (153/831) if nid == 247144
	//gen deaths8 = deaths26 * (138/831) if nid == 247144
	//gen deaths10 = deaths26 * (141/831) if nid == 247144
	//gen deaths12 = deaths26 * (123/831) if nid == 247144
	//gen deaths14 = deaths26 * (73/831) if nid == 247144
	//gen deaths16 = deaths26 * (88/831) if nid == 247144
	//gen deaths18 = deaths26 * (115/831) if nid == 247144
	gen deaths91 = deaths26 * (153/831) if iso3 == "ARM" & year == 1988 & cause == "inj_disaster"
	gen deaths8 = deaths26 * (138/831) if iso3 == "ARM" & year == 1988 & cause == "inj_disaster"
	gen deaths10 = deaths26 * (141/831) if iso3 == "ARM" & year == 1988 & cause == "inj_disaster"
	gen deaths12 = deaths26 * (123/831) if iso3 == "ARM" & year == 1988 & cause == "inj_disaster"
	gen deaths14 = deaths26 * (73/831) if iso3 == "ARM" & year == 1988 & cause == "inj_disaster"
	gen deaths16 = deaths26 * (88/831) if iso3 == "ARM" & year == 1988 & cause == "inj_disaster"
	gen deaths18 = deaths26 * (115/831) if iso3 == "ARM" & year == 1988 & cause == "inj_disaster"
	replace deaths26 = 0 if iso3 == "ARM" & year == 1988 & cause == "inj_disaster"
	
	expand 2 if iso3 == "ARM" & year == 1988 & cause == "inj_disaster", gen(new)
	replace sex = 1 if iso3 == "ARM" & year == 1988 & cause == "inj_disaster" & new == 0
	replace sex = 2 if iso3 == "ARM" & year == 1988 & cause == "inj_disaster" & new == 1
	drop new
	
	foreach var of varlist deaths* {
		replace `var' = `var' * (496/831) if iso3 == "ARM" & year == 1988 & cause == "inj_disaster" & sex == 2
		replace `var' = `var' * (335/831) if iso3 == "ARM" & year == 1988 & cause == "inj_disaster" & sex == 1
	}

	gen frmat = 9
	replace frmat = 101 if iso3 == "ARM" & year == 1988 & cause == "inj_disaster"

	// im_frmat (numeric): from the same file as above
	gen im_frmat = 8
	replace im_frmat = 12 if iso3 == "ARM" & year == 1988 & cause == "inj_disaster"

	// yemen 2015 war deaths are 0-5 in deaths91
	replace im_frmat = 9 if iso3=="YEM" & year==2015 & cause=="war"
	replace frmat = 77 if iso3=="YEM" & year==2015 & cause=="war"
	
	
** *********************************************************************************************
**
** SOURCE IDENTIFIERS (source, source_label, NID, source_type, national, list) **
	// First create a source variable, so we know the origin of each country year

	// if the source is different across appended datasets, give each dataset a unique source_label and NID
	gen source_label = "war/terrorism/execution/natural disaster for shock"
	/*
	gen NID = .
	replace NID = 133435 if source=="UCDP" & cause=="war"
	replace NID = 135380 if source == "Robert Strauss Center"
	replace NID = 13769 if source == "EMDAT"
	replace NID = 153841 if source=="UCDP" & NID==.
	replace NID = 666 if NID == .
	*/
	cap gen source = "SHOCKS"
	gen NID = 6668666
	assert NID != .
	

	// APPEND - add new source_labels if needed

	// source_types: Burial; Cancer registry; Census; Hospital; Mortuary; Police; Sibling history, survey; Surveillance; Survey; VA National; VA Subnational; VR; VR Subnational
	gen source_type = "Surveillance"

	rename sim subdiv
	tostring subdiv, replace

	// national (numeric) - Is this source nationally representative? should be denoted in the source itself. Otherwise consult a researcher. 1= yes 0=no
	gen national = 1
		
	// list (string): what is the tabulation of the cause list? What is the name of the sheet in master_cause_map.xlsx which will merge with the causes in this dataset?
	// For ICD-based datasets, this variable with be the name of the ICD-type. For all other datasets, this variable will be the source name. There must only be one value for list in the whole dataset; for example, if
	// the data contains ICD10 and ICD9-detail, the source should be split into two folders in 03_datasets.
	gen list = "`data_name'"
	
*********************************************************************************************
**
** COUNTRY IDENTIFIERS (location_id, iso3, subdiv) **
	// location_id (numeric) if it is a supported subnational site located in this directory: FILEPATH
	split iso3, parse("_")
	rename iso32 location_id
	destring location_id, replace
	drop iso3
	rename iso31 iso3

	
** *********************************************************************************************
** STANDARDIZE AGE GROUPINGS ***
	do "FILEPATH" 
** ************************************************************************************************* **
// SPECIAL TREATMENTS ACCORDING TO RESEARCHER REQUESTS
	// HERE YOU MAY POOL AGES, SEXES, YEARS, noting in this document: "FILEPATH"
	
	// HERE YOU MAY HAVE SOURCE-SPECIFIC ADJUSTMENTS THAT ARE NOT IN THE PREP OR COMPILE

** *********************************************************************************************
**
** EXPORT **
	do "FILEPATH" `data_name'
	
	
