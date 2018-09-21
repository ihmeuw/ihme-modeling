** ****************************************************
** Purpose: recode causes based on age or sex, which can't be done before age-sex splitting or redistrubution.
** ******************************************************************************************************************
 ** Prep Stata
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

** Source
	global source "`1'"

** Date
	global timestamp "`2'"

** Username
	global username "`3'"

**  Local for the directory where all the COD data are stored
	local in_dir "$j/WORK/03_cod/01_database/03_datasets"
	global in_dir "$j/WORK/03_cod/01_database/03_datasets"

** Local for the output directory
	local out_dir "$j/WORK/03_cod/01_database/02_programs/compile"

** Log output
	capture log close _all
	log using "`in_dir'//$source//logs/08_recode_${timestamp}", replace

** Read in
	use "`in_dir'/$source/data/final/07_merged_long.dta", clear

	** MAKE COMPLETENESS ADJUSTMENTS (this drops country years of VR from our database)
	do "$j/WORK/03_cod/01_database/02_programs/compile/code/completeness_adjustments.do" "$source"
	
	** Recode a few gbd causes conditionally on age. This is ideally done in the redistribution packages for a particular dataset
		** recode CKD to congenital anomalies in NN
		replace acause = "cong_other" if (substr(acause, 1, 3) == "ckd" & acause!="ckd_other" ) & (age == 91 | age == 93)

		** recode COPD and Asthma to neonatal parent --> NOW TO LRI, AS WELL AS PNN ASTHMA
		replace acause = "lri" if ((acause == "resp_copd" | acause == "resp_asthma" | acause=="resp_other" | acause=="resp_interstitial") & (age == 91 | age == 93)) |  (acause=="resp_asthma" & age==94)

		** Drop any maternal cause below age 10 and above age 55
		drop if regexm(acause, "maternal") & (age < 10 | age >=55)

		** recode Hepatitis to other perinatal in NN (exception for ICD9_USSR_Tabulation and ICD10_tabulation since they don't have A17.4 in acause list.. do neonatal parent for those)
 		replace acause = "neonatal_hemolytic" if strmatch(acause,"hepatitis*") & (age == 91 | age == 93) & source != "ICD9_USSR_Tabulation" & source != "ICD10_tabulated"
		replace acause = "neonatal" if strmatch(acause,"hepatitis*") & (age == 91 | age == 93) & source == "ICD9_USSR_Tabulation" & source == "ICD10_tabulated"

		** recode a handful of cancers to other neoplasms
		replace acause = "neo_other" if acause == "neo_esophageal" & ((age < 15 | age>90))
		replace acause = "neo_other" if acause == "neo_stomach" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_larynx" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_lung" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_breast" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_cervical" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_uterine" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_prostate" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_colorectal" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_mouth" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_otherpharynx" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_gallbladder" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_pancreas" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_melanoma" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_nmsc" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_ovarian" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_testicular" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_bladder" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_myeloma" & (age < 15 | age>90)
		replace acause = "neo_other" if acause == "neo_meso" & (age < 15 | age>90)

		** inj_war is only in 2005
		replace acause = "inj_homicide" if acause == "inj_war" & iso3 == "JAM" & year == 2005 & source == "ICD10"

		** In ICD10 2005 there a large number of deaths due to homicides, but in 2006 many of these deaths have moved to unintentional firearms. 2006 is missing homicides deaths. Move deaths from unintentional firearms to homicides.
		replace acause = "inj_homicide" if acause == "inj_mech_gun" & iso3 == "JAM" & year == 2006 & source == "ICD10"

		**  Recode all of data Suriname from 1995-2012 (ICD10 ) for “digest_ibd” to “digest” 
		replace acause = "digest" if acause=="digest_ibd" & source=="ICD10" & iso3=="SUR" & year>=1995 & year<=2012

		** Endo-procedural deaths should go to inj_medical
		replace acause = "inj_medical" if acause == "endo_procedural"

		** Recode Schizophrenia to cc_code in Tibet "
		replace acause = "cc_code" if location_id==518 & acause=="mental_schizo"

		** Recode any HIV that shows up before 1980. This is not epidemiologically possible.
		replace acause = "cc_code" if inlist(acause, "hiv", "hiv_tb", "hiv_other") & year < 1980

		** Deaths assigned to diabetes in neonatal period (age 0-28 days) in all data formats (Except ICD9 and ICD10 detail) including all MCCD, DSP , Russia format, VA shoul dbe recoded to diabetes
		replace acause = "neonatal" if regexm(acause, "diabetes") & ((age==91)|(age==93))

		** Any death in VA and SCD that assigned to the Stroke in under age 20 years have to recode to all CVD
		replace acause = "cvd" if regexm(acause, "cvd_stroke") & ((age<20) | (age>=91 & age<=94)) & (substr(source_type, 1, 2)=="VA" | index(source_type, "Verbal Autopsy"))

		** More recodes  done by master_bridge_map 
		if inlist(source, "India_SCD_states_rural", "India_CRS", "India_MCCD_states_ICD9", "India_MCCD_states_ICD10", "India_Maharashtra_SCD") | substr(source_type, 1, 2) == "VA" | index(source_type,"Verbal Autopsy") | inlist(source, "India_MCCD_Orissa_ICD10", "India_MCCD_Delhi_ICD10") | inlist(source, "ICD9_BTL", "Russia_FMD_1989_1998", "China_1991_2002", "ICD9_USSR_Tabulation") | inlist(source, "ICD10_tabulated", "Thailand_Public_Health_Statistics") {
			tempfile prerecode
			save `prerecode', replace
			if inlist(source, "India_SCD_states_rural", "India_CRS", "India_MCCD_states_ICD9", "India_MCCD_states_ICD10", "India_Maharashtra_SCD") | inlist(source, "ICD9_BTL", "Russia_FMD_1989_1998", "China_1991_2002", "ICD9_USSR_Tabulation") local sheetlab "$source"
			else if (substr(source_type, 1, 2) == "VA" | index(source_type,"Verbal Autopsy")) local sheetlab "INDEPTH_ICD10_VA"
			else if inlist(source, "India_MCCD_Orissa_ICD10", "India_MCCD_Delhi_ICD10") local sheetlab "India_MCCD_states_ICD10"
			else if inlist(source, "ICD10_tabulated", "Thailand_Public_Health_Statistics") local sheetlab "ICD10_tabulated"
			import excel using "$j/WORK/03_cod/01_database/02_programs/compile/data/master_bridge_map.xlsx", sheet("`sheetlab'") clear firstrow
			merge 1:m acause using `prerecode', nogen keep(2 3)
			replace acause = bridge_code if acause != bridge_code & bridge_code != ""
			drop bridge_code
		}

		** In all VR move mental_drug deaths in under 15 and to unintentional poisoning
		if source_type == "VR" {
			replace acause = "inj_poisoning" if acause == "mental_alcohol" & (age <15| age >90)
			replace acause = "inj_poisoning" if acause == "mental_drug" & (age <15| age >90)
			replace acause = "inj_poisoning" if acause == "mental_drug_opioids" & (age <15 | age == 94)
			replace acause = "inj_poisoning" if acause == "mental_drug_cocaine" & (age <15| age >90)
			replace acause = "inj_poisoning" if acause == "mental_drug_amphet" & (age <15| age >90)
			replace acause = "inj_poisoning" if acause == "mental_drug_cannabis" & (age <15| age >90)
			replace acause = "inj_poisoning" if acause == "mental_drug_other" & (age <15| age >90)
		}
		
		** In India MCCD neonatal sepsis should only be in under 1 month
		if source == "India_MCCD_states_ICD9" | source == "India_MCCD_states_ICD10" {
			replace acause = "neonatal" if acause == "neonatal_sepsis" & !(age == 91 | age == 93)
		}

		** In India_SCD_states_rural we are trying to get rid of all the redistribution artifacts
		if source=="India_SCD_states_rural" {
			tempfile data
			save `data', replace
			** make sure deaths are exactly what we want them to be
			gen deaths = cf_rd*sample_size
			** collapse to location cause age
			collapse (sum) deaths, by(location_id acause age) fast
			** count the number over 1
			gen over1 = 1 if deaths>1
			replace over1 = 0 if deaths<=1
			collapse (sum) over1, by(location_id acause) fast
			** keep the location causes that have NO location-cause death totals over 1 in ANY age
			keep if over1==0
			gen recode = 1
			keep location_id acause recode
			tempfile to_recode
			save `to_recode', replace
			merge 1:m location_id acause using `data', assert(2 3) keep(2 3) nogen
			replace acause = "cc_code" if recode==1
			drop recode
			** no need to collapse now because there is one at the end of the script
		}

		** In the raw data for BTL some countries have a combination of tabulated causes, numerical causes, in addition to disaggregated causes. Some of the country years are missing the tabulated cause which should have a majority of the deaths. Recode these to the parent since the deaths are too low.
		if source == "ICD9_BTL" {
			tempfile btl_data
			save `btl_data', replace
			import excel using "$j/WORK/03_cod/01_database/02_programs/compile/data/btl_cancer_recodes_list.xlsx", firstrow clear
			merge 1:m iso3 year acause using `btl_data',keep(2 3)
			replace acause = recode_acause if _m == 3
			drop _m recode_acause
		}

		** source_label in QAT VR source is only helpful for mapping and redistribution, after that it will break code because its not supposed to be a unique identifier
		if "$source"=="QAT_VR_1986_1999" {
			** as long as this source is dividing its source_label by age like this, eliminate this division
			replace source_label = "QAT_VR_1986_1999" if regexm(source_label, "(all_ages)|(infants)")
			collapse (sum) cf* deaths* (mean) sample_size, by(iso3 age dev_status region NID source source_label source_type list location_id subdiv national sex year acause) fast
			gen beforeafter = .
		}

		if "$source"=="Sweden_ICD9" {
			** the pre 1990 years are only neonatal deaths, drop so we are not uploading 0's
			count if deaths_raw>0 & !inlist(age, 91, 93, 94) & year<1990
			drop if "$source"=="Sweden_ICD9" & year<1990 & !inlist(age, 91, 93, 94) & `r(N)'==0
		}

		** CREATE TELANGANA OUT OF ANDHRA PRADESH HERE
		if inlist("$source", "India_SCD_states_rural", "India_MCCD_states_ICD9", "India_MCCD_states_ICD10") {
			do "$j/WORK/03_cod/01_database/02_programs/compile/code/split_andhra_to_telangana.do"
		}


		** Let's collapse one last time to get rid of any duplicates
		collapse (sum) cf* deaths* (mean) sample_size, by(iso3 age dev_status region NID source source_label source_type list location_id subdiv national sex year acause beforeafter) fast


** ********************************************************************************************************************************************************************
** *********************************************************************************************************************************************************************
** STEP 3: MERGE THE COMPLETE CAUSE MAP ONTO THE DEATH DATA (and remove non-reporting causes)

	** Recode non-reporting causes to it's parent cause
	merge m:1 acause using "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", keep(1 3) keepusing(parent_id cause_id secret_cause) nogen
	replace cause_id = parent_id if secret_cause == 1 & acause !="cc_code"
	drop secret_cause acause parent_id

	** Merge on cause list without non-reporting causes for this round of GBD 
	merge m:1 cause_id using "$j/WORK/00_dimensions/03_causes/gbd2015_causes_all.dta", keep(1 3) nogen keepusing(acause)
	
	** Merging on non-reporting reverts some bridge mapping changes.
	if "$source"=="ICD9_BTL" {
		replace acause = "digest" if acause == "digest_other"
	}

	** collapse to remove duplicates
	collapse (sum) cf* deaths* (mean) sample_size, by(iso3 age dev_status region NID source source_label source_type list location_id subdiv national sex year acause beforeafter) fast

	** create cf_final
		gen double cf_final = cf_rd

	** save before adjustment
		compress
		save "`in_dir'/$source/data/final/08_recoded.dta", replace
		save "`in_dir'/$source/data/final/_archive/08_recoded_$timestamp.dta", replace
