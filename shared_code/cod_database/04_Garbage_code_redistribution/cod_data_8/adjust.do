** ****************************************************
** Purpose: Post redistribution adjustments 
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
	
//  Local for the directory where all the COD data are stored
	local in_dir "$j/WORK/03_cod/01_database/03_datasets"
	global in_dir "$j/WORK/03_cod/01_database/03_datasets"

// Local for the output directory
	local out_dir "$j/WORK/03_cod/01_database/02_programs/compile"
	
// Load functions used
	run "$j/WORK/04_epi/01_database/01_code/04_models/prod/fastcollapse.ado"
	run "$j/WORK/04_epi/01_database/01_code/04_models/prod/get_models.ado"

// Log output
	capture log close _all
	log using "`in_dir'//$source//logs/09_adjust_${timestamp}", replace

// Do we want to reprep anemia proportions?
	local prepanemia = 0

// Do we want to reprep injury proportions?
	local prepinj = 0
	
// Read in data
	use "`in_dir'//${source}/data/final/08_recoded.dta", clear

// ********************************************************************************************************************************************************************
// ********************************************************************************************************************************************************************
// SET UP NEEDED FILES (ONLY RERUN IF INPUTS CHANGE)

if `prepanemia'==1 {
// Load anemia proportions to get prevalence of severe anemia
	#delimit ;
	odbc load, exec("
		SELECT
			s.sequela_id,
			s.sequela_name,
			mv.model_version_id
		FROM 
			epi.model_versions mv
		JOIN
			epi.sequelae s ON mv.sequela_id = s.sequela_id
		JOIN 
			(SELECT 
				srg.sequela_id
			FROM 
				epi.sequela_reporting_group srg
			JOIN 
				epi.reporting_group rg ON srg.reporting_group_id = rg.reporting_group_id AND reporting_group_type_id=8
			WHERE
				rg.reporting_group_name LIKE 'Severe anemia%') sas ON mv.sequela_id = sas.sequela_id
		JOIN 
			epi.causes c ON s.cause_id = c.cause_id
		WHERE
			mv.is_best = 1
	") strConnection clear;
	#delimit cr
	levelsof model_version_id, local(models) sep(" ") c
	get_models, type("epi") model_version_ids(`models')
	keep if age <= 80 & location_type == "country" & parameter_type == "Prevalence"
	keep sequela_id sequela_name acause model_version_id iso3 year age sex mean
	replace age = 91 if age == 0
	replace age = 93 if age > 0.009 & age < 0.011
	replace age = 94 if age > 0.09 & age < 0.11
	expand 5, gen(newobs)
	bysort sequela_id iso3 sex age year (newobs): gen newyears = _n
	replace newyears = newyears-1
	drop if (year == 2010 & newyears >= 3)
	replace year = year+newyears
	duplicates tag sequela_id iso3 sex age year, gen(dups)
	drop if (dups == 1 & newobs == 1) | year > 2015
	replace mean = . if newobs == 1
	drop newobs
	isid sequela_id iso3 year age sex
	sort sequela_id iso3 sex age year
	egen group_id = group(sequela_id iso3 sex age year)
	ipolate mean group_id, gen(series_prev) epolate
	fastcollapse series_prev, by(iso3 sex age year acause) type(sum)
	egen double anemia_prop = pc(series_prev), by(iso3 sex age year) prop
	rename acause target
	gen acause = "nutrition_iron"
	save "`out_dir'/data/inputs/Anemia_Input.dta", replace
}

if `prepinj'==1 {
// the injury fractions 
	// 1. prep results
		// there are seperate estimates for males and females, and seperate estimates for under/over 5. put them together
		use "`c_estimates_m_o5'", clear
		gen sex = 1
		tempfile cmo5
		save `cmo5'
		use "`c_estimates_f_o5'", clear
		gen sex =2
		append using `cmo5'
		drop if age <5
		tempfile o5
		save `o5'
		use "`c_estimates_m_u5'", clear
		gen sex = 1
		tempfile cmu5
		save `cmu5'
		use "`c_estimates_f_u5'", clear
		gen sex = 2
		append using `cmu5'
		drop if age >=5
		append using `o5'
		
	// 2. compute estimated injury fractions
		gen double injury_fraction = ensemble_mean / envelope
		keep iso3 year sex age injury_fraction
		drop if sex==3 | age==99
		compress
		save "`out_dir'/data/inputs/Injury_Fractions_Input.dta", replace

// the rti fractions
	// 1. prep gbd cause results

		// there are seperate estimates for males and females, and seperate estimates for under/over 5. put them together
		use "`c011_estimates_m'", clear
		gen sex = 1
		tempfile cm
		save `cm'
		use "`c011_estimates_f'", clear
		gen sex =2
		append using `cm'
		
	// 2. compute estimated rti fractions
		gen rti_fraction = ensemble_mean / envelope
		keep iso3 year sex age rti_fraction
		drop if sex==3 | age==99
		compress
		save "`out_dir'/data/inputs/RTI_Fractions_Input.dta", replace
}

// ********************************************************************************************************************************************************************
// ********************************************************************************************************************************************************************
// ADJUST UNLIKELY CAUSES AND DEAL WITH OBSERVATIONS THAT WERE CREATED DURING REDISTRIBUTION

	// We have a zeroes problem where there is not actual data. Drop where sample size is zero, except for Vital Registration where we believe there is generally high coverage
	drop if (sample_size==0) & regexm(source_type, "VR")!=1 & source != "INDEPTH"
	drop if sample_size == 0 & source == "INDEPTH" & subdiv == "Kisumu"

// ********************************************************************************************************************************************************************
// ********************************************************************************************************************************************************************
// APPLY ANEMIA ADJUSTMENT
// We have determined that Anemia deaths are likely to be actually due to various other causes of them. Use estimates of proportion of anemia attributable
// to a number of targets codes. Apply them for VA only

// 1. Load proportions
// 2. Apply adjustment
// 3. Re-apply recodes
// 4. Double check
count if substr(source_type,1,2) == "VA" & acause == "nutrition_iron"
if `r(N)'>0 {
// Get death totals for later
	gen deaths = cf_final*sample_size
	summ deaths
	local preadj = round(`r(sum)',0.01)
	drop deaths
// 1.	
	joinby iso3 sex age year acause using "`out_dir'/data/inputs/Anemia_Input.dta", unmatched(master)
	replace acause = target  if _merge == 3
	
// 2. 
	gen cf_anemia_adj = cf_final * anemia_prop if _merge == 3
	drop if cf_anemia_adj == 0 & _merge == 3
	replace cf_final = cf_anemia_adj if _merge == 3
	replace cf_rd = 0 if _merge == 3 & acause != "nutrition_iron"
	replace cf_corr = 0 if _merge == 3 & acause != "nutrition_iron"
	replace cf_raw = 0 if _merge == 3 & acause != "nutrition_iron"
	
// 3. Reapply recodes
	** General recodes
		replace acause = "cong_other" if (substr(acause, 1, 3) == "ckd" & acause!="ckd_other" ) & (age == 91 | age == 93)
		drop if regexm(acause, "maternal") & (age < 10 | age >=55)
	
	** VA recode
		do "$j/WORK/03_cod/01_database/02_programs/compile/code/recode_VA.do"
	
	** India recode
		if inlist(source, "India_SCD_states_rural", "India_CRS", "India_MCCD_states_ICD9", "India_MCCD_states_ICD10", "India_Maharashtra_SCD") | list=="INDEPTH_ICD10_VA" | inlist(source, "India_MCCD_Orissa_ICD10", "India_MCCD_Delhi_ICD10") {
			tempfile prerecode
			save `prerecode', replace
			if inlist(source, "India_SCD_states_rural", "India_CRS", "India_MCCD_states_ICD9", "India_MCCD_states_ICD10", "India_Maharashtra_SCD") local sheetlab "$source"
			else if list=="INDEPTH_ICD10_VA" local sheetlab "INDEPTH_ICD10_VA"
			else if inlist(source, "India_MCCD_Orissa_ICD10", "India_MCCD_Delhi_ICD10") local sheetlab "India_MCCD_states_ICD10"
			import excel using "$j/WORK/03_cod/01_database/02_programs/compile/data/master_bridge_map.xlsx", sheet("`sheetlab'") clear firstrow
			merge 1:m acause using `prerecode', nogen keep(2 3)
			replace acause = bridge_code if acause != bridge_code & bridge_code != ""
			drop bridge_code
		}

// 4.
	gen deaths = cf_final*sample_size
	summ deaths
	local postadj = round(`r(sum)',0.01)
	drop deaths target cf_anemia_adj
	assert abs(`postadj'-`preadj') < 1
}	
// Save
	collapse (sum) cf* deaths*, by(iso3 region location_id subdiv year age sex source source_label source_type NID national list acause sample_size) fast
 
 // ********************************************************************************************************************************************************************
// ********************************************************************************************************************************************************************
// Apply RTI-Fraction Adjustment
// All of the police data from the Various_RTI folder (with the exception of Andreeva 2006 which gets the previous adjustment) is RTI only. It needs an adjustment similar to the injury-fraction adjustment

//  multiply injury fractions by cause fractions in mortuary/hospital data
count if (source == "Various_RTI" & source_label != "Andreeva 2006") | source == "GSRRS_Bloomberg_RTI"
if `r(N)' > 0 {
	gen needs_rti_adjustment = 1 if source == "Various_RTI" & source_label != "Andreeva 2006"
	
	// first, recompute the cause fractions without any non-injuries to be sure that we're working with injury fractions only
	drop if (substr(acause,1,9) != "inj_trans" | acause == "cc_code") & needs_rti_adjustment ==1
	bysort location_id subdiv iso3 year sex age source_type source_label source NID list national: egen double new_cf = pc(cf_final) if needs_rti_adjustment ==1, prop
	replace cf_final = new_cf if needs_rti_adjustment == 1 
	
// then merge on the estimated injury fractions and multiply
	preserve
		use "`out_dir'/data/inputs/RTI_Fractions_Input.dta", clear
		expand 2 if year==2011, gen(new)
		replace year = 2012 if new==1
		replace age = 91 if age == 0
		replace age = 93 if age == 0.01
		replace age = 94 if age == 0.1
		drop new
		tempfil rti
		save `rti'
	restore
	merge m:1 iso3 year sex age using `rti', keep(1 3) nogen
	replace cf_final = cf_final * rti_fraction if needs_rti_adjustment == 1 
	drop rti_fraction new_cf 

// recalculate cause fractions to make absolutely sure they sum to 1 
	bysort iso3 location_id subdiv year sex age source_type source_label source NID list national: egen double new_cf = pc(cf_final) if deaths_rd != 0 & inlist(source_type, "Hospital", "Mortuary", "Hospital Report")!=1 & needs_rti_adjustment != 1, prop
	replace new_cf = 0 if new_cf == .
	** This looks like it's changing a lot of values, but it's just changing zero to missing, and adding a very small rounding error
	replace cf_final = new_cf if deaths_rd != 0 & new_cf != . & new_cf != 1 & new_cf != 0 & needs_rti_adjustment != 1 & inlist(source_type, "Hospital", "Mortuary", "Hospital Report")!=1
	drop new_cf deaths* needs_rti_adjustment
}	
// drop CC_code here as we are no longer calcualting cause fractions
	drop if acause == "cc_code" | acause == "_u"
	
// save before aggregation
	gen double cf_before_aggregation = cf_final
	save "`in_dir'/$source/data/final/09_adjusted.dta", replace
	save "`in_dir'/$source/data/final/_archive/09_adjusted_$timestamp.dta", replace
