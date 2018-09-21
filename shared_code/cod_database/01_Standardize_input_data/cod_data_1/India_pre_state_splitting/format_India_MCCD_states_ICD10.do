** ************************************************************************************************* **						
** Purpose: Format India_MCCD_states_ICD10 data pre-state splitting 			
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
	global data_name India_MCCD_states_ICD10

** Establish data import directories 
	global jdata_in_dir "$j/DATA/"
	global cod_in_dir "$j/WORK/03_cod/01_database/03_datasets/${data_name}/data/raw/"
	global cod_code "$j/WORK/03_cod/01_database/03_datasets/${data_name}/code"

	log using "$j/WORK/03_cod/01_database/03_datasets/${data_name}/logs/00_formatted", replace
	
** Map file
	import excel using "$cod_in_dir/MCCD_ICD10_cause_codes.xlsx", firstrow clear
	tempfile cause_map
	save `cause_map', replace
	
** *********************************************************************************************
**
** If appending multiple: best practice is to loop the import, cleaning, cause, year, sex, and age processes together.
** IMPORT **
	** Import from J/DATA
	quietly{
		do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/clean_Gujarat.do"
		tempfile Gujarat
		save `Gujarat', replace
		di in red "Gujarat complete"
		
		do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/clean_Karnataka.do"
		tempfile Karnataka
		save `Karnataka', replace
		di in red "Karnataka complete"

		do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/clean_Maharashtra.do"
		tempfile Maharashtra
		save `Maharashtra', replace
		di in red "Maharashtra complete"

		do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/clean_Mizoram.do"
		tempfile Mizoram
		save `Mizoram', replace
		di in red "Mizoram complete"
		
		do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/clean_Rajasthan.do"
		tempfile Rajasthan
		save `Rajasthan', replace
		di in red "Rajasthan complete"
		
		do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/clean_Chhattisgarh.do"
		tempfile Chhattisgarh
		save `Chhattisgarh', replace
		di in red "Chhattisgarh complete"
	}
		
		do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/clean_National.do"
		tempfile National
		save `National', replace
		di in red "National complete"


	clear
	local files Gujarat Karnataka Maharashtra Mizoram Rajasthan Chhattisgarh National
	
	foreach x of local files { 
		display in red "appending `x'"
		append using ``x''
	}

	
** *********************************************************************************************
** STANDARDIZE AGE GROUPINGS ***
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/format_gbd_age_groups.do" 
** ************************************************************************************************* **
** SPECIAL TREATMENTS ACCORDING TO RESEARCHER REQUESTS
	** HERE YOU MAY POOL AGES, SEXES, YEARS, noting in this document: "J:/WORK/03_cod/00_documentation/Data Queue and Requests.xlsx"
	** change source label to make one cause list
	replace source_label = "MCCD_ICD10"

	** ReSOLVE DuPLICATE DEATHS IN E AND N CODES
		do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/MCCD_states_N_code_scaling.do"

	** ENSURE THERE AREN'T NEGATIVE DEATHS
		** replace select negative deaths with 0s. These have been investigated to verify they aren't extraction errors. Then make sure we got em all
		local has_negatives if (subdiv=="Manipur" & year==2010 & cause=="10.03.03" & sex==1) | (subdiv=="Sikkim" & year==2008 & cause=="14.01.05" & sex==2)
		replace deaths1 = 0 `has_negatives'
		replace deaths26 = 0 `has_negatives'
		gen negative=0
		foreach var of varlist deaths* {
			replace negative = 1 if `var'<0
		}
		count if negative ==1
		assert r(N)==0
		drop negative
	
	** UNION TERRITORIES SHOULD BE COMBINED
	replace location_id = 44540 if inlist(location_id, 43871, 43876, 43878, 43879, 43889, 43897)

	** MAKE SUBDIV BASED ON THE NAME IN THE DATABASE
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
	merge m:1 location_id using `lnames', assert(3) keep(3) nogen
	replace subdiv = subinstr(subdiv, ",", ":", .)

	bysort location_id year: egen loc_year_deaths = total(deaths1)
	drop if loc_year_deaths==0
	drop loc_year_deaths

	collapse (sum) deaths*, by(NID cause cause_name frmat im_frmat iso3 list location_id national sex source source_label source_type year subdiv) fast
	** REMOVE N CODES
	tempfile withN
	save `withN', replace
	do "$j/WORK/03_cod/01_database/03_datasets/India_MCCD_states_ICD10/code/split_N_codes_into_E_codes.do"
	tempfile split
	save `split', replace
	use `withN' if !regexm(cause, "^(19)|(20)")
	append using `split'

	** REMOVE LEVEL 1 AND LEVEL 2 CAUSES
	drop if !regexm(cause, "^[0-9]+\.[0-9]+\.[0-9]+")
	
		
** *********************************************************************************************
**
** EXPORT **
	local data_name India_MCCD_states_ICD10
	do "$j/WORK/03_cod/01_database/02_programs/prep/code/export.do" `data_name'
	
	
	
