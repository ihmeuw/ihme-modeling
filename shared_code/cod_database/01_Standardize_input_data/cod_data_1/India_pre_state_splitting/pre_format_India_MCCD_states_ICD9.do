** ************************************************************************************************* **				
** Purpose: Format all ICD9 MCCD data pre-state splitting						
** ************************************************************************************************* **

** PREP STATA**
	clear all
	set mem 1000m

	set more off
	ssc install bygap
	
	** set the prefix for whichever os we're in
	if c(os) == "Windows" {
		global j "J:"
	}
	if c(os) == "Unix" {
		global j "/home/j"
		set odbcmgr unixodbc
	}
	
** *********************************************************************************************
**

** What we call the data: same as folder name
	local data_name India_MCCD_states_ICD9

** Establish data import directories
	local jdata_in_dir "$j/DATA/"
	local cod_in_dir "$j/WORK/03_cod/01_database/03_datasets/`data_name'/data/raw/1990_1998"
	local out_dir
	
	
** *********************************************************************************************
**

** IMPORT **							
 	quietly {
	pause on
	foreach year of numlist 1990/1996 1998 {
		di in red "working on year `year'"
		** get all the filenames for the year we are working on
		local files`year' : dir "`cod_in_dir'" files "IND_MEDICAL_CERTIFICATION_COD_`year'_DEATHS_BY_SEX_STATE_ICD9*", respectcase
		** append them all together
		tempfile `year'
		clear
		gen foo = .
		save ``year'', replace
		foreach file of local files`year' {
			** import the file
			import excel "`cod_in_dir'/`file'", clear
			keep if regexm(D, "[0-9]") | !regexm(D, "States")
			gen foo = .
			** sometimes the first line will contain the first part of the state, then the second line will contain the second part. So add these together
			foreach var of varlist C-foo {
				if "`var'" != "foo" {
					replace `var' = `var'[_n] + `var'[_n+1] if regexm(`var', "^[A-Za-z]+-$")
				}
			}
			** this condition gets rid of the line that contains the second part of the state name
			keep if (D!="" & E != "" & F != "") | regexm(D, "[0-9]")
			** clean columns of interest
			foreach var of varlist C-foo {
				** clean state-level data
				if "`var'" != "foo" {
					** want state names to be the stata variable names, so take out the dashes, spaces, and ampersands
					replace `var'=subinstr(`var', "-", "", .) if !regexm(`var', "[0-9]")
					replace `var'=subinstr(`var', " ", "_", .) if !regexm(`var', "[0-9]")
					replace `var'=subinstr(`var', "_&", "", .) if !regexm(`var', "[0-9]")
					** store the state name in a local
					local subdiv = `var'[1]
					** fix a spelling mistake
					if "`subdiv'"=="Mainpur" {
						local subdiv = "Manipur"
					}
					** rename variable to one that will be convenient to a later reshape to make state long
					local new_name = "deaths`subdiv'"
					capture rename `var' `new_name'
				}
			}
			
			** A and B always contain cause and sex
			rename (A B) (cause sex)
			** was also able to confirm that this condition keeps only the observations and removes header and footer observations
			keep if regexm(sex, "(Male)|(Female)")
			** keep the filename and ordering so that we can sort on it for later cause name work
			gen file_name = "`file'"
			gen src_sort = _n
			
			** now, ready to append
			append using ``year''
			save ``year'', replace
		}
		** don't need this foo anymore
		drop foo
		** add the year and save
		gen year = `year'
		save ``year'', replace
		di in red "finished year `year'"
	}
	** append all the years together
	clear
	tempfile master
	gen foo = .
	save `master', replace
	foreach year of numlist 1990/1996 1998 {
		append using ``year''
		save `master', replace
	}
	** deaths and deathsIndia are both row total columns that are unnecessary
	drop foo deaths deathsIndia
	save `master', replace
	}
	
	** destring deaths and fill in the cause where it is blank
	destring deaths*, replace
	replace cause = cause[_n-1] if cause==""
	** this collapse gets all the info for one file in wide format, then the reshape makes state long
	collapse (sum) deaths*, by(cause sex year file_name src_sort)
	reshape long deaths, i(cause sex year file_name src_sort) j(subdiv) string
	
** Clean the cause list
	tempfile preCause
	save `preCause', replace
	
	** CODE / NAME SPLITTING
		** All the causes have a format something like "Disease name (icd code)". So split that into just the code and the name. This makes cleaning and standardizing easier.
		** fix unnecessary characters
		replace cause = subinstr(cause, ".", "", .)
		replace cause = subinstr(cause, "'", "", .)
		**  split to cause codes and name using a regex string match 
		gen cause_icd = regexs(2) if regexm(cause, "^([A-Za-z0-9 \(\)&,'-]+)(\([A-Za-z0-9 &,-\.]+\))$")
		gen cause_name_old = regexs(1) if regexm(cause, "^([A-Za-z0-9 \(\)&,'-]+)(\([A-Za-z0-9 &,-\.]+\))$")
		replace cause_icd = trim(cause_icd)
		** this gets rid of the parens around the icd code
		replace cause_icd = substr(cause_icd, 2, length(cause_icd)-2)
		** 600-508 is mislabeled
		replace cause_icd = "600-608" if cause_icd=="600-508"
		** easy 
		drop if cause=="All Causes"
		
		** these two were not captured with the regex. Fix here
		replace cause_icd = "179, 182" if regexm(cause, "179, 182")
		replace cause_icd = "630-676" if regexm(cause, "630-676")
		
		** some causes actually don't have a code in parents after the name, so keep the cause name the same
		replace cause_name_old = cause if cause_name_old==""
		** to matching with the cause fix map later
		replace cause_name_old = trim(cause_name_old)
		tempfile preMerge
		save `preMerge', replace
	
	** MAPPING INCORRECT CAUSE / CAUSE NAMES TO STANDARD ONES
		import excel "`cod_in_dir'/cause_fix.xlsx", firstrow clear
		** set the right variables for merge and do it
		keep cause_icd-cause_name_new
		tempfile cause_fix
		save `cause_fix', replace
		merge 1:m cause_icd cause_name_old using `preMerge'
		** fix two cause codes manually
		replace cause_icd = "099" if cause_name_new=="Other Veneral Diseases" | cause_name_old=="Other Veneral Diseases"
		replace cause_icd = "402-404" if cause_name_new=="Hypertensive Heart Disease" | cause_name_old=="Hypertensive Heart Disease"
		** all the _merge==2 cause names are already standard, so just replace cause_name_new with the original
		replace cause_name_new = cause_name_old if _merge==2
		** now cause_name_new and cause_icd contain fully standard non-duplicate causes, so rename and drop the old stuff
		drop cause_name_old cause
		rename (cause_name_new cause_icd) (cause_name cause)
		drop _merge
	
	** DROPPING SUB-TOTAL PARENT CAUSES
		** Again, most of work was generating the map file, standardize causes using the above process, then saved to the below stata file.
		merge m:1 cause cause_name using "`cod_in_dir'/parent_causes_clean.dta"
		
		** Matched causes are parent causes, which are either chapters or sub-totals. set the cause level based on below factors.
		gen parent_cause=1 if _merge==3
		gen cause_level = 2 if parent_cause ==1
		replace cause_level = 1 if regexm(cause_name, "^[XIV]+\ ")
		replace cause_level = 3 if parent_cause==.
		replace cause_level = 2 if regexm(cause_name, "^E[0-9][0-9]")
		** one thing didn't work automatically; fix it manually. NOTE: irony
		replace cause_level = 2 if cause_name == "Congenital Anomalies"
		
		** determine which is the cause level 2 and which is the level 3.
		bysort file_name year subdiv cause_level cause cause_name sex: gen cause_count = _N
		tempfile wrongCauseLevels
		save `wrongCauseLevels', replace
		** now fix these up
		keep if cause_count==2
		sort file_name year subdiv cause_level cause cause_name sex src_sort
		** now that they are sorted and we know everything is a pair-wise duplicate, we can say that the odd indexed observations are the first-appearing in the file and therefore the cause level 2
		replace cause_level=3 if mod(_n, 2)==0
		count if cause_level==2
		local level2 = `r(N)'
		count if cause_level==3
		local level3 = `r(N)'
		** there should be an even split between cause level 2 and cause level 3
		assert(`level2'==`level3')
		tempfile fixedCauseLevel
		save `fixedCauseLevel', replace
		
		use `wrongCauseLevels', clear
		drop if cause_count==2
		append using `fixedCauseLevel'
		
		
** GET RID OF THE CAUSE CLEANING PROCESS VARIABLES
	keep cause cause_name sex year file_name src_sort cause_level subdiv deaths
	
	tempfile preCode
	save `preCode', replace
	
	sort file_name src_sort
	** actually important to use 1993 because it is one of the years that includes all causes
	keep if year==1993
	keep cause cause_name cause_level
	duplicates drop
	
	do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD9/code/levels_to_cause_code.do"
	
	merge 1:m cause cause_name cause_level using `preCode'
	tempfile postCode
	save `postCode', replace
	
	keep cause_code sex year subdiv deaths cause_level
	collapse (sum) deaths, by(cause* sex year subdiv)
	
** SEX **
	replace sex = "1" if sex=="Male"
	replace sex = "2" if sex=="Female"
	destring sex, replace
	
	tempfile 90s
	save `90s', replace 
	
** Load the 1980's data **
	
	do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD9/code/clean_append_pre1990.do"
	append using `90s'
	
	tempfile all_clean
	save `all_clean', replace
	
** CAUSE

	drop cause_level
	merge m:1 cause_code using "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD9/maps/cause_list/cause_codes_to_causes_by_year.dta", assert(3) keep(3) nogen
	replace cause199x =cause1983 if cause199x==""
	rename cause199x cause_name
	count if cause_name==""
	assert(`r(N)'==0)
	drop cause1*
	rename cause_code cause
	count if cause==""
	assert(`r(N)'==0)
	
** AGE **
	rename deaths deaths26
	gen frmat = 9
	gen im_frmat=9
	
** *********************************************************************************************
**
** SOURCE IDENTIFIERS (source, source_label, NID, source_type, national, list) **
	** First create a source variable, so we know the origin of each country year
	capture drop source
	gen source = "`data_name'"

	** if the source is different across appended datasets, give each dataset a unique source_label and NID
	replace subdiv = subinstr(subdiv, "_", " ", .)
	gen source_label = ""
	gen NID = .
	replace NID = 157418 if year==1990
	replace NID = 157420 if year==1991
	replace NID = 157421 if year==1992
	replace NID = 157422 if year==1993
	replace NID = 157423 if year==1994
	replace NID = 157424 if year==1995
	replace NID = 157425 if year==1996
	replace NID = 157426 if year==1998

	** APPEND - add new source_labels if needed

	** source_types: Burial; Cancer registry; Census; Hospital; Mortuary; Police; Sibling history, survey; Surveillance; Survey; VA National; VA Subnational; VR; VR Subnational
	gen source_type = "VR"
	
	** national (numeric) - Is this source nationally representative? should be denoted in the source itself. Otherwise consult a researcher. 1= yes 0=no
	gen national = 1
		
	** list (string): what is the tabulation of the cause list? What is the name of the sheet in master_cause_map.xlsx which will merge with the causes in this dataset?
	** For ICD-based datasets, this variable with be the name of the ICD-type. For all other datasets, this variable will be the source name. There must only be one value for list in the whole dataset; for example, if
	** the data contains ICD10 and ICD9-detail, the source should be split into two folders in 03_datasets.
	gen list = "`data_name'"
	
** *********************************************************************************************
**
** COUNTRY IDENTIFIERS (location_id, iso3, subdiv) **
	** location_id (numeric) if it is a supported subnational site located in this directory
	gen location_id = .
	replace location_id=44540 if subdiv=="Andaman Nicobar Islands"
	replace location_id=43872 if subdiv=="Andhra PrUSER"
	replace location_id=43873 if subdiv=="Arunachal PrUSER"
	replace location_id=43874 if subdiv=="Assam"
	replace location_id=43880 if subdiv=="Delhi"
	replace location_id=43881 if subdiv=="Goa" | subdiv=="Goa Daman  Diu"
	replace location_id=43882 if subdiv=="Gujarat"
	replace location_id=43883 if subdiv=="Haryana"
	replace location_id=43884 if subdiv=="Himachal PrUSER"
	replace location_id=43887 if subdiv=="Karnataka"
	replace location_id=43888 if subdiv=="Kerala"
	replace location_id=44540 if subdiv=="Lakshdweep"
	replace location_id=44540 if subdiv=="Lakshadweep"
	replace location_id=43890 if subdiv=="Madhya PrUSER"
	replace location_id=43891 if subdiv=="Maharashtra"
	replace location_id=43892 if subdiv=="Manipur"
	replace location_id=43893 if subdiv=="Meghalaya"
	replace location_id=43894 if subdiv=="Mizoram"
	replace location_id=43895 if subdiv=="Nagaland"
	replace location_id=43896 if subdiv=="Orissa"
	replace location_id=44540 if subdiv=="Pondicherry"
	replace location_id=43898 if subdiv=="Punjab"
	replace location_id=43899 if subdiv=="Rajasthan"
	replace location_id=43900 if subdiv=="Sikkim"
	replace location_id=43901 if subdiv=="Tamil Nadu"
	replace location_id=43903 if subdiv=="Tripura"
	replace location_id=43904 if subdiv=="Uttar PrUSER"
	replace subdiv = "The Six Minor Territories" if location_id == 44540
	
	count if location_id ==.
	assert r(N) ==0
	
	collapse (sum) deaths*, by(cause sex year subdiv cause_name frmat im_frmat source source_label NID source_type national list location_id)
	
	tempfile messy_states
	save `messy_states', replace
	** now conform the subdiv field to the database
	levelsof(location_id), local(lids)
	local lids : list clean lids
	local lid_string = ""
	local count = 1
	foreach lid in `lids' {
		if `count' != 1 {
			local lid_string = "`lid_string', `lid'"
		}
		else {
			local lid_string = "`lid'"
		}
		local count = `count' + 1
	}

	preserve
		odbc load, exec("SELECT location_id, location_ascii_name as subdiv FROM shared.location WHERE location_id IN (`lid_string')") dsn(prodcod) clear
		tempfile lnames
		save `lnames', replace
	restore

	drop subdiv
	merge m:1 location_id using `lnames', assert(3) keep(3) update nogen
	replace subdiv = subinstr(subdiv, ",", ":", .)

	bysort location_id year: egen loc_year_deaths = total(deaths26)
	drop if loc_year_deaths==0
	drop loc_year_deaths
		
	** iso3 (string)
	gen iso3 = "IND"
	
** *********************************************************************************************
** STANDARDIZE AGE GROUPINGS ***
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/format_gbd_age_groups.do"
** ************************************************************************************************* **
** SPECIAL TREATMENTS ACCORDING TO RESEARCHER REQUESTS
	** HERE YOU MAY POOL AGES, SEXES, YEARS, noting in this document: "J:\WORK/03_cod/00_documentation/Data Queue and Requests.xlsx"
	
	** HERE YOU MAY HAVE SOURCE-SPECIFIC ADJUSTMENTS THAT ARE NOT IN THE PREP OR COMPILE

	do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/MCCD_states_N_code_scaling.do"

	** REMOVE N CODES
	tempfile withN
	save `withN', replace
	do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/split_N_codes_into_E_codes.do"
	tempfile split
	save `split', replace
	use `withN' if !regexm(cause, "^(17)|(18)")
	append using `split'

	** REMOVE LEVEL 1 AND LEVEL 2 CAUSES
	drop if !regexm(cause, "^[0-9]+\.[0-9]+\.[0-9]+")

	drop if cause=="18.11.01"

** *********************************************************************************************
**
** EXPORT
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/export.do" `data_name'

	
