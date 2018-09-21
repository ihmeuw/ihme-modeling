** ************************************************************************************************* **	
** Purpose: Format India_SRS_states_report data for the CoD prep code						
** ************************************************************************************************* **

** PREP STATA**
	capture restore
	clear all
	set mem 1000m

	set more off
	
	** set the prefix for whichever os we're in
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" {
		global j "/home/j"
		set odbcmgr unixodbc
	}
	
	local lsvid 38

** *********************************************************************************************
**
** What we call the data: same as folder name
	local data_name India_SRS_states_report

** Establish data import directories 
	local jdata_in_dir "$j/DATA/"
	local cod_in_dir "$j/WORK/03_cod/01_database/03_datasets/`data_name'/data/raw/"
	
** *********************************************************************************************
**
** If appending multiple: best practice is to loop the import, cleaning, cause, year, sex, and age processes together.
** IMPORT **
	** Pull from the raw data 
	local re_extract = 0 
	if `re_extract'==1 {
		do "$j/WORK/03_cod/01_database/03_datasets/`data_name'/code/extract_raw_to_stata.do"
	}

	** POPULATION AND MORTALITY ENVELOPE
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/env_wide.do"
	keep if iso3=="IND"
	drop if location_id==.
	** all ages only
	egen pop = rowtotal(pop*)
	egen env = rowtotal(env*)
	keep iso3 location_id year sex pop env
	keep if (year>=2010 & year<=2013) | (year>=2004&year<=2006)
	replace year=2011 if year>=2010 & year<=2013
	replace year=2005 if year>=2004 & year<=2006
	collapse (sum) pop env, by(iso3 location_id year sex) fast
	tempfile pop
	save `pop', replace
	** use location_name to get urbanicity
	odbc load, exec("SELECT location_id, location_name FROM shared.location_hierarchy_history WHERE location_set_version_id=`lsvid'") clear strConnection
	** will need to merge on location_name later
	duplicates tag location_name, gen(d)
	drop if d>0
	drop d
	tempfile locations
	save `locations', replace
	merge 1:m location_id using `pop', assert(1 3) keep(3) nogen
	tostring sex, replace
	replace sex = "male" if sex=="1"
	replace sex = "female" if sex=="2"
	save `pop', replace
	keep if regexm(location_name, "(Rural)|(Urban)")
	gen urbanicity = "urban" if regexm(location_name, "Urban")
	replace urbanicity = "rural" if regexm(location_name, "Rural")
	count if urbanicity=="" | sex=="" | !inlist(year, 2005, 2011)
	assert `r(N)'==0
	collapse (sum) pop, by(sex urbanicity year) fast
	tempfile pop_urb
	save `pop_urb', replace
	use `pop', clear
	drop if regexm(location_name, "(Rural)|(Urban)")
	count if sex=="" | !inlist(year, 2005, 2011)
	assert `r(N)'==0
	collapse (sum) pop env, by(location_name sex year) fast
	tempfile pop_state
	save `pop_state', replace

	local letters "A B C D E F"
	
	** DISTRIBUTION OF DEATHS BY REGION
	tempfile distribution
	clear
	gen foo = .
	set obs 1
	save `distribution', replace
	foreach letter of local letters {
		use "`cod_in_dir'/tables/table5_2`letter'.dta", clear
		gen year=floor((2013+2010)/2)
		tempfile d10
		save `d10', replace

		** more formatting in 2004 simply due to there being one similarly formatted excel sheet per table
		import excel using "`cod_in_dir'/tables2004/table7_2`letter'.xlsx", clear
		rename (A B D) (age_group pct_deaths_male pct_deaths_female)
		keep if real(pct_deaths_male) != .
		keep age_group pct_deaths*
		gen region = ""
		replace region = "North" if "`letter'"=="A"
		replace region = "North East" if "`letter'"=="B"
		replace region = "East" if "`letter'"=="C"
		replace region = "Central" if "`letter'"=="D"
		replace region = "West" if "`letter'"=="E"
		replace region = "South" if "`letter'"=="F"
		gen table = "7.2`letter'"
		** choose the midpoint year
		gen year = floor((2004+2006)/2)
		tempfile d04
		save `d04', replace

		append using `d10'

		append using `distribution'
		save `distribution', replace
	}
	** all ages because using age in srs is too complicated for now
	keep if age_group=="Total"
	** reshape so that all we have is deaths by region and sex
	keep *male *female year region
	reshape long pct_deaths_, i(region year) j(sex) string
	rename pct_deaths_ deaths
	destring deaths, replace
	save `distribution', replace


	** TOP TEN CAUSES BY REGION
	tempfile causes
	clear
	gen foo=.
	set obs 1
	save `causes', replace
	foreach letter of local letters {
		use "`cod_in_dir'/tables/table5_1`letter'.dta", clear
		gen year=floor((2013+2010)/2)
		tempfile d10
		save `d10', replace

		** more invoved for 2004 report but it didnt need extract
		** taking from the rawest tables
		import excel using "`cod_in_dir'/tables2004/table7_1`letter'.xlsx", clear
		rename (A B C) (cause pct_deaths_male pct_deaths_female)
		replace cause = "Ill-defined/ All other symptoms,signs and abnormal clinical and laboratory findings" if regexm(lower(cause), "ill-defined")
		keep if real(pct_deaths_male) != .
		keep cause pct*
		gen region = ""
		replace region = "North" if "`letter'"=="A"
		replace region = "North East" if "`letter'"=="B"
		replace region = "East" if "`letter'"=="C"
		replace region = "Central" if "`letter'"=="D"
		replace region = "West" if "`letter'"=="E"
		replace region = "South" if "`letter'"=="F"
		gen table = "7.1`letter'"
		** choose the midpoint year
		gen year = floor((2004+2006)/2)
		tempfile d04
		save `d04', replace
		append using `d10'
		append using `causes'
		save `causes', replace
	}
	drop if cause=="Total"
	keep cause year pct_deaths_male pct_deaths_female region
	destring pct*, replace
	reshape long pct_deaths_, i(cause region year) j(sex) string
	drop if pct_deaths_==.
	rename pct_deaths_ pct_deaths
	bysort region sex year: egen total_pct = total(pct_deaths)
	**  requirement because data is susceptible to rounding issues
	count if abs(100-total_pct)>=.5
	assert `r(N)'==0
	replace pct_deaths = pct_deaths/total_pct
	drop total_pct
	save `causes', replace

	** RURAL URBAN CAUSE FRACTIONS
	use "`cod_in_dir'/tables/table2_3C.dta", clear
	gen year = floor((2010+2013)/2)
	tempfile d10
	save `d10', replace
	import excel using "`cod_in_dir'/tables2004/table2_3C.xlsx", clear
	rename (A B C) (cause pct_deaths_male pct_deaths_female)
	keep cause pct*
	** mimic structure of 2010 file so that same code can be used
	gen rank = cause
	gen year = floor((2004+2006)/2)
	drop in 1/5
	append using `d10'
	** clean: pull out urbanicity, keep only male/female, drop unnecessary rows
	gen urbanicity = rank if regexm(rank, "Area")
	replace urbanicity = regexs(1) if regexm(rank, "((Urban)|(Rural))")
	replace urbanicity = lower(urbanicity)
	replace urbanicity = urbanicity[_n-1] if urbanicity==""
	keep cause year *male *female urbanicity
	destring pct*, replace
	replace cause = "Ill-defined/ All other symptoms,signs and abnormal clinical and laboratory findings" if regexm(lower(cause), "ill-defined")
	replace cause = trim(cause)
	drop if cause=="" | cause=="Total" | regexm(cause, "(Urban)|(Rural)") | pct_deaths_male==.
	** reshape to desired long structure for merging
	reshape long pct_deaths_, i(cause year urbanicity) j(sex) string
	rename pct_deaths pct_deaths
	** rescale so that pct_deaths adds perfectly to 1
	bysort urbanicity sex year: egen total_pct = total(pct_deaths)
	count if abs(100-total_pct)>.1
	assert `r(N)'==0
	replace pct_deaths = pct_deaths/total_pct
	drop total_pct
	** save
	tempfile urbanicity
	save `urbanicity', replace

	** RURAL URBAN DEATHS
	use "`cod_in_dir'/tables/table2_2C.dta", clear
	gen year = floor((2010+2013)/2)
	tempfile d10
	save `d10', replace

	import excel using "`cod_in_dir'/tables2004/table2_2C.xlsx", clear
	rename (A B D H J) (age_group pct_deaths_rural_male pct_deaths_rural_female pct_deaths_urban_male pct_deaths_urban_female)
	keep if real(pct_deaths_rural_male) != .
	gen year = floor((2004+2006)/2)
	append using `d10'
	keep  year age_group *male *female
	** will get age from age splitting
	keep if age_group=="Total"
	destring pct*, replace
	** do reshapes so that there is just one deaths column
	reshape long pct_deaths_urban_ pct_deaths_rural_, i(age_group year) j(sex) string
	** take out the underscore so that urbanicity is just "rural"&"urban"
	rename *_ *
	reshape long pct_deaths_, i(age_group sex year) j(urbanicity) string
	** this is just called "pct_deaths" because of an artifact of the extract program
	rename pct_deaths_ deaths
	keep year sex urbanicity deaths
	tempfile urb_dist
	save `urb_dist', replace

	** RURAL URBAN RELATIVE DEATH RATES, MAKING PERCENT URBAN/RURAL BY CAUSE
	use `urbanicity', clear
	merge m:1 urbanicity sex year using `urb_dist', assert(3) nogen
	replace deaths = deaths*pct_deaths
	drop pct_deaths
	merge m:1 urbanicity sex year using `pop_urb', assert(3) nogen
	gen rate = deaths/pop
	keep cause urbanicity sex rate year
	reshape wide rate, i(cause sex year) j(urbanicity) string
	keep if raterural!=. & rateurban !=.
	gen pcturban = rateurban/(rateurban+raterural)
	gen pctrural = raterural/(rateurban+raterural)
	keep cause sex pct* year
	tempfile pct_urb
	save `pct_urb', replace

	** AGE PATTERN
	** Number of total deaths by age
	use "`cod_in_dir'/tables/table2_2A.dta", clear
	drop if age_group =="Total"
	keep *male *female age_group
	replace age_group = subinstr(age_group, "-", "_", .)
	replace age_group = "70plus" if age_group =="70+"
	rename pct_deaths_* deaths*
	destring deaths*, replace
	reshape long deaths, i(age_group) j(sex) string
	drop if inlist(age_group, "0_1", "1_4")
	replace age_group="30_44" if inlist(age_group, "30_34", "35_44")
	collapse (sum) deaths, by(age_group sex)
	tempfile age_dist
	save `age_dist', replace

	** causes that will be in final dataset
	use `pct_urb', clear
	keep cause
	replace cause = lower(cause)
	drop if cause=="all other remaining causes"
	duplicates drop
	tempfile final_causes
	save `final_causes', replace

	** Percentage of deaths in an age by cause
	use "`cod_in_dir'/tables/table1_3A.dta", clear
	gen sex = final_icd_codes_person if regexm(final_icd_codes_person, "(MALE)")
	replace sex = "PERSON" in 1
	replace sex = sex[_n-1] if sex==""
	drop if sex=="PERSON"
	replace sex = lower(sex)
	rename final_icd_codes_person deaths
	drop if real(deaths)==.
	drop if cause=="Total"
	keep cause pct* sex
	destring pct*, replace
	reshape long pct_deaths_, i(cause sex) j(age_group) string
	drop if age_group =="total"
	tempfile age_cause
	save `age_cause', replace

	merge m:1 age_group sex using `age_dist', assert(3) keep(3) nogen
	rename pct_deaths_ pct_deaths
	bysort age_group sex: egen total = total(pct_deaths)
	replace pct_deaths = pct_deaths/total
	replace deaths = deaths*pct_deaths
	replace cause = lower(cause)
	drop pct_deaths total
	** now replace non-data causes with cc_code cause so that cc_code can be age split as well
	merge m:1 cause using `final_causes', assert(1 3)
	replace cause = "all other remaining causes" if _merge==1
	collapse (sum) deaths, by(age_group sex cause) fast

	bysort cause sex: egen total = total(deaths)
	gen double pct_deaths = deaths/total
	replace cause = lower(cause)
	tempfile age
	save `age', replace

	** DEATHS BY CAUSE/REGION/SEX, SPLIT INTO DEATHS BY CAUSE/STATE/SEX WITH ENV
	** THEN SPLIT INTO CAUSE/STATE/URBANICITY/SEX WITH RURAL/URBAN FRACTIONS
	** get mapping of state to region
	use "$j/WORK/03_cod/01_database/00_documentation/IND/SCD_SRS_region_mapping.dta", clear
	keep if srs_region != ""
	keep location_name srs_region
	replace location_name = subinstr(location_name, ", Urban", "", .)
	replace location_name = subinstr(location_name, ", Rural", "", .)
	duplicates drop
	merge 1:m location_name using `pop_state', assert(2 3) keep(3) nogen
	rename srs_region region
	bysort region sex year: egen region_env = total(env)
	gen pct_deaths = env/region_env
	keep region sex location_name pct_deaths year
	tempfile state_splits
	save `state_splits', replace
	** split region/sex deaths by cause
	use `causes', clear
	merge m:1 region sex year using `distribution', assert(3) keep(3) nogen
	replace deaths = deaths*pct_deaths
	drop pct_deaths
	** split region/cause/sex deaths by state
	joinby region sex year using `state_splits'
	replace deaths = deaths*pct_deaths
	drop pct_deaths
	tempfile statesexcause
	save `statesexcause', replace
	save "$j/WORK/03_cod/01_database/03_datasets/`data_name'/data/raw/deaths_before_urban_rural_splitting.dta", replace

	merge m:1 cause sex year using `pct_urb', assert(1 3)
	replace cause = "All Other Remaining Causes" if _merge==1
	drop _merge
	** use the all other causes split otherwise (heavily weighted towards rural because more deaths there...)
	merge m:1 cause sex year using `pct_urb', assert(3 4) nogen update
	gen deathsUrban = deaths*pcturban
	gen deathsRural = deaths*pctrural
	collapse (sum) deathsUrban deathsRural, by(location_name cause sex year) fast
	reshape long deaths, i(location_name cause sex year) j(u) string
	replace location_name = location_name + ", " + u
	merge m:1 location_name using `locations', assert(2 3) keep(3) nogen
	drop u

	** SPOT CHECK THAT THE DEATH TOTALS ARE UNCHANGED
	merge m:1 location_name using "$j/WORK/03_cod/01_database/00_documentation/IND/SCD_SRS_region_mapping.dta", keep(3) assert(2 3) keepusing(srs_region) nogen
	su deaths if srs_region=="South" & sex=="female" & year==2011
	assert abs(`r(sum)'-21636)<.01
	su deaths if srs_region=="North" & sex=="male" & year==2011
	assert abs(`r(sum)'-13713)<.01
	su deaths if srs_region=="North East" & sex=="female" & year==2005
	assert abs(`r(sum)'-3628)<.01
	su deaths if srs_region=="East" & sex=="male" & year==2005
	assert abs(`r(sum)'-14798)<.01
	su deaths if srs_region=="West" & sex=="male" & year==2005
	assert abs(`r(sum)'-9691)<.01
	drop srs_region
	replace cause = lower(cause)

	** NOW ADD AGE USING 2010-2013 AGE DISTRIBUTION
	tempfile f
	save `f', replace
	joinby cause sex using `age', unmatched(master)
	gen new_deaths = deaths*pct_deaths
	replace new_deaths = deaths if _merge==1
	drop deaths _merge pct_deaths total
	rename new_deaths deaths

** INITIAL CLEANING **

	** Remove unnecessary rows and columns from data

** CAUSE AND CAUSE_NAME (cause, cause_name) **

	** Make cause long if needed

	** cause (string)
	** Format the cause list from the compiled datasets to be consistent accross them
	** NOTE: IF THIS DATASET DOES NOT COVER ALL DEATHS (FOCUSES ON JUST INJURIES OR SOMETHING) YOU MUST CREATE
	** A ROW FOR 'CC' (COMBINED CODE) FOR EACH COUNTRY-YEAR SO THAT CAUSE FRACTIONS CAN BE CALCULATED LATER. 
	** CC SHOULD REPRESENT ALL DEATHS ATTRIBUTED TO CAUSES OTHER THAN THOSE THAT ARE THE FOCUS OF THE STUDY.
	** THE NUMBERS FOR THIS SHOULD BE FOUND SOMEWHERE IN THE ORIGINAL DATA SOURCE. 

	** cause_name (string)
	** If the causes are just codes, we will need a cause_name to carry its labels, if available. Otherwise leave blank.
	gen cause_name = ""

** YEAR (year) **

	** Make year long if needed
	** Generate a year value that is appropriate for each observation
	** year (numeric) - whole years only
	

** SEX (sex) **

	** Make sex long if needed

	** Make sex=1 if Male, sex=2 if Female, sex=9 if Unknown
	gen sex_id = 1 if sex=="male"
	replace sex_id = 2 if sex=="female"
	count if sex_id==.
	assert `r(N)'==0
	drop sex
	rename sex_id sex

** AGE VARIABLES (deaths#, frmat, im_frmat) **
	** Make age wide, using the prefix "deaths"
	** Format into WHO age codes
	
	** deaths (numeric): If deaths are in CFs or rates, find the proper denominator to format them into deaths
	gen age = 91 if age_group=="0_4"
	replace age = 7 if age_group=="5_14"
	replace age = 9 if age_group=="15_29"
	replace age = 12 if age_group=="30_44"
	replace age = 15 if age_group=="45_54"
	replace age = 17 if age_group=="55_69"
	replace age = 20 if age_group=="70plus"
	count if age==.
	assert `r(N)'==0
	drop age_group
	reshape wide deaths, i(cause sex year location_id) j(age)

	** frmat (numeric): find the WHO format here "J:\WORK\03_cod\02_datasets\programs\agesex_splitting\documentation\Age formats documentation.xlsx"
	gen frmat = 139

	** im_frmat (numeric): from the same file as above
	gen im_frmat = 9
	
	
** *********************************************************************************************
**
** SOURCE IDENTIFIERS (source, source_label, NID, source_type, national, list) **
	** First create a source variable, so we know the origin of each country year
	capture drop source
	gen source = "`data_name'"

	** if the source is different across appended datasets, give each dataset a unique source_label and NID
	gen source_label = ""
	gen NID = 23279

	** APPEND - add new source_labels if needed

	** source_types: Burial; Cancer registry; Census; Hospital; Mortuary; Police; Sibling history, survey; Surveillance; Survey; VA National; VA Subnational; VR; VR Subnational
	gen source_type = "VA National"
	
	** national (numeric) - Is this source nationally representative? should be denoted in the source itself. Otherwise consult a researcher. 1= yes 0=no
	gen national = 1
		
	** list (string): what is the tabulation of the cause list? What is the name of the sheet in master_cause_map.xlsx which will merge with the causes in this dataset?
	** For ICD-based datasets, this variable with be the name of the ICD-type. For all other datasets, this variable will be the source name. There must only be one value for list in the whole dataset; for example, if
	** the data contains ICD10 and ICD9-detail, the source should be split into two folders in 03_datasets.
	gen list = "India_SRS_states_report"
	
** *********************************************************************************************
**
** COUNTRY IDENTIFIERS (location_id, iso3, subdiv) **
	** location_id (numeric) if it is a supported subnational site located in this directory: J:\DATA\IHME_COUNTRY_CODES
	** gen location_id = .
		
	** iso3 (string)
	gen iso3 = "IND"

	** subdiv (string): if you can't find a location id for the specific locatation (e.g. rural, urban, Jakarta, Southern States...) enter it here, otherwise leave ""
	** THIS FIELD CANNOT HAVE COMMAS (OTHER CHARACTERS ARE FINE)

	gen subdiv = "urban" if regexm(location_name, "Urban")
	replace subdiv = "rural" if subdiv==""
	
** *********************************************************************************************
** STANDARDIZE AGE GROUPINGS ***
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/format_gbd_age_groups.do" 
** ************************************************************************************************* **
** SPECIAL TREATMENTS ACCORDING TO RESEARCHER REQUESTS
	** HERE YOU MAY POOL AGES, SEXES, YEARS, noting in this document: "J:\WORK\03_cod\00_documentation\Data Queue and Requests.xlsx"
	
	** HERE YOU MAY HAVE SOURCE-SPECIFIC ADJUSTMENTS THAT ARE NOT IN THE PREP OR COMPILE

** *********************************************************************************************
**
** EXPORT **
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/export.do" `data_name'
	
	