//-------------------------------------------------------------------------------------------------
// Purpose: compiling and standardizing previously extracted Clearinghouse (ICBDSR) data for GBD 2016
// {AUTHOR NAME}

// GBD 2016
//-------------------------------------------------------------------------------------------------


/* This code does the following:
	- load data and compile older Clearinghouse data
	- format variable names to match with the recently extracted 2014 Clearinghouse report
	- format the location names
	- include relevant study-level covariates as necessary
	- append all years together
	- collapse to modelable entity (ME) leve1
	- collapse to cause level

*/

// setup
clear all 
set more off
set maxvar 30000
cap restore, not
cap log close 

//---------------------
// filepaths
local 2013_in_dir "{FILEPATH}"
local 1991_in_dir "{FILEPATH}"
local 2016_dir "{FILEPATH}"

local date {DATE}

// load data & format as necessary for appending together
// standardize differences between individuals who extracted the data

import excel using "`2013_in_dir'\GBD2013_ICBDSR_2003_{USERNAME}.xlsx", firstrow clear
			replace add11totalnumberoflivebirth = "." if add11=="nr"
			destring add11, replace 
	tempfile 2003_raw
	save `2003_raw', replace

import excel using "`2013_in_dir'\GBD2013_ICBDSR_2004_{USERNAME}.xlsx", firstrow clear 
			tostring GBD_cause, replace
			tostring response_rate, replace
			tostring numerator, replace  
			tostring add2site, replace 
			tostring add7, replace 
			tostring add8, replace 
			tostring add9, replace 
			tostring add12, replace 
			tostring add13, replace 
	tempfile 2004_raw
	save `2004_raw', replace

import excel using "`2013_in_dir'\GBD2013_ICBDSR_2005_{USERNAME}.xlsx", firstrow clear 
			tostring add5ageatdianosis, replace 

	tempfile 2005_raw
	save `2005_raw', replace

import excel using "`2013_in_dir'\GBD2013_ICBDSR_2006_{USERNAME}.xlsx", firstrow clear 
			replace add11totalnumberoflivebirth = "." if add11=="nr"
			destring add11, replace 
	tempfile 2006_raw
	save `2006_raw', replace

import excel using "`2013_in_dir'\GBD2013_ICBDSR_2007_{USERNAME}.xlsx", firstrow clear 
			tostring GBD_cause, replace
			tostring response_rate, replace
			tostring numerator, replace  
			tostring add2site, replace 
			tostring add7, replace 
			tostring add8, replace 
			tostring add9, replace 
			tostring add12, replace 
			tostring add13, replace 
			tostring notes, replace 
	tempfile 2007_raw
	save `2007_raw', replace

import excel using "`2013_in_dir'\GBD2013_ICBDSR_2009_{USERNAME}.xlsx", firstrow clear 
	tempfile 2009_raw
	save `2009_raw', replace

import excel using "`2013_in_dir'\GBD2013_ICBDSR_2010_{USERNAME}.xlsx", firstrow clear 
				tostring add12, replace 
	tempfile 2010_raw
	save `2010_raw', replace

import excel using "`2013_in_dir'\GBD2013_ICBDSR_2011_{USERNAME}.xlsx", firstrow clear 
			tostring add12, replace 
	tempfile 2011_raw
	save `2011_raw', replace

import excel using "`2013_in_dir'\GBD2013_ICBDSR_2012_{USERNAME}.xlsx", firstrow clear 
			tostring numerator, replace 
			tostring add12, replace 
	tempfile 2012_raw
	save `2012_raw', replace


// append all {USERNAME} data
	use `2003_raw', clear 
		append using `2005_raw'
		append using `2006_raw'
		append using `2009_raw'
		append using `2010_raw'
		append using `2011_raw'
		append using `2012_raw'

// and append all {USERNAME} data
		append using `2004_raw'
		append using `2007_raw'

//-------------------------------------------
// variable names
	
	// drop vars that are not needed
	drop row_id nid_old table_num grouping sequelae healthstate case_def case_diag ///
	 specificity raw_adjusted period_unit period_value lower_CI upper_CI confidence_limit standard_error ///
	 cv_ ignore raw_adjusted group add_15 add1diagnostic Add16 add6 effective_sample_size parameter parameter_value parameter_units

	 // drop extra blank rows
	 drop if case_name==""

// format sex variable
	tostring sex, replace
	replace sex="Both" if sex=="3"

// site names
	rename add2site site2
	
//--------------------------------------------------
// clean information in the "add" variables

	// destring 
	replace add7 = "." if inlist(add7, "not perimitted", "not permitted", "nr")
	replace add7 = "0" if add7 =="O"
	destring add7, replace ignore("<" "=" " ")

	replace add8 = "." if add8=="nr"
	destring add8, replace ignore("<" "=" " ")

	replace add9 = "." if add9=="nr"
	destring add9, replace ignore("<" "=" " ")

	replace add12 = "." if add12=="nr"
	destring add12, replace

	replace add13 = "." if inlist(add13, "not permitted", "Not permitted", "nr", "nr ", "nor permitted")
	destring add13, replace 


	// change var names 
		rename add11 live_births
		rename add12 stillbirths
		rename add13 total_terminations

		replace numerator = "" if numerator=="nr"
		destring numerator, replace ignore("." <)

		rename add7 cases_TOP 
		rename add8 cases_SB
		rename add9 cases_LB

		
drop add14 add_14 // cv_estimate not needed
drop add3denominator // already have a "denominator" variable


// "age at diagnosis" is the definition used for stillbirths in the various registries
	replace add5ageatdian = "" if add5ageatdian =="??"
	replace add5ageatdian = "" if add5ageatdian=="."
	destring add5ageatdian, replace ignore(">" "days" "g")
replace add5ageatdiagnosis = add5ageatdianosis if add5ageatdianosis != .
	drop add5ageatdian

		tostring add5, replace 
	replace add5 = "12 weeks" if add5 =="12"
	replace add5 = "16 weeks" if add5 =="16"
	replace add5 = "20 weeks" if add5 =="20"
	replace add5 = "22 weeks" if add5 =="22"
	replace add5 = "23 weeks" if add5 =="23"
	replace add5 = "24 weeks" if add5 =="24"
	replace add5 = "26 weeks" if add5 =="26"
	replace add5 = "28 weeks" if add5 =="28"
	replace add5 = "180 days" if add5 =="180"
	replace add5 = "500g" if add5=="500"
	replace add5 = "1000g" if add5=="1000"
	replace add5 = "" if add5 =="."
		rename add5age stillbirth_definition

rename add15coverage coverage_type
rename notes note_SR

// re-name covariates... 
	rename add10cv_chromo_incl cv_chromo_included
	rename add4cv_postnatal_dx cv_postnatal_diagnosis

//----------------------------------------------			
// merge in the cause-mapping information

rename GBD_cause original_case_name


	preserve
		import excel using "`2016_dir'\clearinghouse_cause_mapping_gbd2013.xlsx", clear

drop A H 
rename B original_case_name
rename C clearinghouse_case_name
rename D GBD_cause_group
rename E GBD_cause
rename F case_definition_notes 
rename G drop
	drop in 1

tempfile cause_map 
save `cause_map', replace

restore

// prep for merge
drop GBD_cause
rename case_name original_case_name


merge m:1 original_case_name using `cause_map', keep(3) nogen

//----------------------------------------------------------------------
// merge in standardized registry names
preserve
import excel using "`2016_dir'\clearinghouse_location_mapping.xlsx", sheet("2003 - 2012 mapping") clear
	rename A site
	rename B site2
	rename C iso3
	rename E registry_name
	rename D gbd_region
	drop in 1
		duplicates drop site site2 iso3 gbd_region, force

		tempfile registry_name_map
		save `registry_name_map', replace
restore

merge m:1 site site2 iso3 gbd_region using `registry_name_map', nogen keep(3)
	drop iso3 gbd_region

	replace sampling_type = ""
		destring sampling_type, replace 

			replace age_end =0 if age_end !=0

tempfile 2001_to_2010
save `2001_to_2010', replace 

//---------------------------------------------------------------------------------------------------
//---------------------------------------------------------------------------------------------------
// Now prep the 1991 report data to append in:

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_anencephaly_ls.csv", clear
	gen cause_name = "Anencephaly"
	drop gbd_cause 
	tempfile anencephaly
	save `anencephaly', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_anorectralatresia_ls.csv", clear
	gen cause_name = "Anorectal atresia/stenosis"
	tempfile anorectal_atresia
	save `anorectal_atresia', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_bladderextrophy_ls.csv", clear
	gen cause_name = "Bladder exstrophy"
		replace add16 = "" if add16 == "#REF!"
		destring add16, replace 
	tempfile bladder_exstrophy
	save `bladder_exstrophy', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_cleftlip_{USERNAME}.csv", clear
	gen cause_name = "Cleft lip with or without cleft palate"
		replace page_num = "72"
		destring page_num, replace 
		drop upper_ci
	tempfile cleft_lip
	save `cleft_lip', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_cleftpalate_ls.csv", clear
	gen cause_name = "Cleft palate without cleft lip"
	tempfile cleft_palate 
	save `cleft_palate', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_conjoinedtwins_ls.csv", clear
	gen cause_name = "Conjoined twins"
		replace add16 = "" if add16 == "#REF!"
		destring add16, replace 
	tempfile conjoined_twins
	save `conjoined_twins', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_diaphragmatichernia_ls.csv", clear
	gen cause_name = "Diaphragmatic hernia"
	tempfile diaphragmatic_hernia
	save `diaphragmatic_hernia', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_downsyndrome_ls.csv", clear
	gen cause_name = "Down Syndrome"
	tempfile down 
	save `down', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_encephalocele_ls.csv", clear
	gen cause_name = "Encephalocele"
	tempfile encephalocele
	save `encephalocele', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_Epispadias_{USERNAME}.csv", clear
	gen cause_name = "Epispadias"
		tostring denominator, replace 
		tostring add3, replace 
		tostring add5, replace 
	tempfile epispadias
	save `epispadias', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_Esophageal_abdomwall_ls.csv", clear
	gen cause_name = "Total abdominal wall defects (include unspecified)"
	tempfile abdom_wall
	save `abdom_wall', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_Esophageal_atresia_{USERNAME}.csv", clear
	gen cause_name = "Oesophageal atresia/stenosis with or without fistula"
	tempfile oesophageal_atresia
	save `oesophageal_atresia', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_Gastroschisis_{USERNAME}.csv", clear
	gen cause_name = "Gastroschisis"
	tempfile gastroschisis
	save `gastroschisis', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_Hypospadias_{USERNAME}.csv", clear
	gen cause_name = "Hypospadias"
	tempfile hypospadias 
	save `hypospadias', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_limbreduction_{USERNAME}.csv", clear 
	gen cause_name = "Total Limb reduction defects (include unspecified)"
	tempfile limb_reduction
	save `limb_reduction', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_microtia_{USERNAME}.csv", clear
	gen cause_name = "Microtia"
		tostring denominator, replace 
		tostring add3, replace 
	tempfile microtia 
	save `microtia', replace

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_omphalocele_{USERNAME}.csv", clear 
	gen cause_name = "Omphalocele"
		tostring add3, replace 
	tempfile omphalocele
	save `omphalocele', replace 

import delimited using "`1991_in_dir'\GBD2013_Congential_malformation_spinabifida_{USERNAME}.csv", clear
	gen cause_name = "Spina bifida"
	tempfile spina_bifida
	save `spina_bifida', replace

// append causes together 
use `anencephaly', clear 
	append using `anorectal_atresia'
	append using `bladder_exstrophy'
	append using `cleft_lip'
	append using `cleft_palate'
	append using `conjoined_twins'	
	append using `diaphragmatic_hernia' 
	append using `down'
	append using `encephalocele' 
	append using `epispadias'
	append using `abdom_wall' 
	append using `oesophageal_atresia'
	append using `gastroschisis'
	append using `hypospadias'	
	append using `limb_reduction'
	append using `microtia'
	append using `omphalocele'
	append using `spina_bifida'


//------------------
// drop vars with no useful information 
		// note that many of the "add*" variables don't contain any information
keep nid file_name file_location response_rate gbd_region iso3 subnational_id site national urbanicity year_start year_end ///
	sex age_start age_end numerator denominator notes add2 add3 add4 add5 cause_name  


// formatting vars
	destring denominator, ignore (",") replace 
		destring add3, ignore(",") replace 
	
	tostring sex, replace
	replace sex="Both" if sex=="3"
	
// standardize variable names
	rename add2site site2
	rename add3 denominator2
	rename add4 cv_postnatal_dx

// "age at diagnosis" is the definition used for stillbirths in the various registries
	replace add5 = "16 weeks" if add5 =="16"
	replace add5 = "20 weeks" if add5 =="20"
	replace add5 = "28 weeks" if add5 =="28"
	replace add5 = "180 days" if add5 =="180"
	replace add5 = "500g" if add5=="500"
	replace add5 = "1000g" if add5=="1000"
	replace add5 = "" if add5 =="."
		rename add5 stillbirth_definition

	rename notes note_SR
	replace file_name = "CONGENITAL_MALFORMATIONS_WORLDWIDE_1991_Y2014M01D09"

// "Although  stillbirths  are included in the data of a majority of programs in the Clearinghouse, they are defined variably 
//  by a minimum of 16, 20, or 28 weeks of gestation or by birth weight limits of at least 500 or 1,000 grams"
//  NID 129094
//  eg. in the 1991 report cases were not available by live births / stillbirths
	gen cv_livestill =1
	gen cv_topnotrecorded =0

	rename numerator cases_LB

//-----------------------------------------
// merge in standardized registry names 
	preserve
	import excel using "`2016_dir'\clearinghouse_location_mapping.xlsx", sheet("1991 report mapping") clear

	rename A site
	rename B site2
	rename C iso3
	rename E registry_name
	rename D gbd_region
	drop in 1
		duplicates drop site site2 iso3 gbd_region, force
		drop F G H I

		tempfile registry_name_map
		save `registry_name_map', replace
	restore 

merge m:1 site site2 iso3 gbd_region using `registry_name_map', keep(3) nogen
	drop site2 denominator2


//-------------------------------------------------------------------------
tempfile 1974_to_1988
save `1974_to_1988', replace
//---------------------------------------------------------------------------------------------
// Prep the 2014 report 
	import excel using "`2016_dir'\ICBDSR_2014_mapping_and_extraction_09_15_16.xlsx", sheet("Extraction") firstrow clear

	replace specificity=""
	replace location_name=""
	replace response_rate =""
	replace age_start =0
	drop BH cv_

		
		replace note_SR = "Stillbirths defined at 20 weeks; diagnosis until age 1" if reg=="France: REMERA" & year_start==2012

		egen new_note_SR = concat(note_SR age_end), punct("; cases collected until age ")
		replace note_SR = new_note_SR if age_end !=""
			drop new_note_SR
		replace age_end="0"
		destring age_end, replace 

	gen file_name = "ICBDSR_2014"
	rename Clearinghousecausename original_case_name

	egen note_SR2 = concat(note_SR note_coverage), punct("; ")
		replace note_SR = note_SR2 if note_coverage !=""
		drop note_SR2 note_coverage 

	rename totalterminations total_terminations
	rename totalbirths denominator

//-----------------------------------
	append using `1974_to_1988'
	append using `2001_to_2010'

// clean up & standardize varnames to match other registries
		// cut to the minimum number of vars 
	drop site2 field_citation study_type CVs is_outlier location_id location_name ihme_loc_id standard_error ///
			effective_sample_size urbanicity_type uncertainty_type uncertainty_type_value recall_type recall_type_value sampling_type site_memo ///
			unit_value_as_published sex_issue age_issue group_review group specificity sex mean upper lower ///
			measure_adj representative_name source_type year_issue measure_issue unit_type age_demographer

	rename site site_memo 

	// coverage_type variable goes in the note_SR 
		egen new_note_SR = concat(note_SR coverage_type), punct("; registry is ")
		replace note_SR = new_note_SR if coverage_type !=""
			drop new_note_SR coverage_type
		
	// response rate 
		replace response_rate = "100" if inlist(response_rate, "101", "102", "103", "104", "105")
		replace response_rate = "100" if inlist(response_rate, "106", "107", "108", "109", "110")
		replace response_rate = "100" if inlist(response_rate, "111", "112", "113", "114")
			replace response_rate = subinstr(response_rate, ["*"], "", .)

	// case names
		replace original_case_name = case_name if case_name !=""
		replace original_case_name = cause_name if cause_name !=""
			drop case_name cause_name

	// drop maternal age-specific Down syndrome data
		drop if regexm(original_case_name, "Down") & age_start !=0
		drop if age_start !=0

	// covariates
		replace cv_topnotrecorded = 0 if cases_TOP !=.
		replace cv_livestill = 0 if cases_SB !=.
		rename cv_chromo_included cv_includes_chromos
		replace cv_postnatal_dx = cv_postnatal_diag if cv_postnatal_diag !=.
			gen cv_prenatal = 1 if cv_postnatal_dx ==.
				replace cv_prenatal =0 if cv_prenatal ==.
			drop cv_postnatal_dx cv_postnatal_diag // check on this... 

	// var names
		rename cases_LB cases
		rename cases_SB prenatal_fd
		rename cases_TOP prenatal_top
		rename denominator sample_size 
			replace sample_size = live_births if sample_size ==. & live_births !=.
			drop live_births
			drop if sample_size==.

	// fix citations
	replace field_cit = "International Clearinghouse for Birth Defects Surveillance and Research. International Clearinghouse for Birth Defects Surveillance and Research Annual Report 2014. Rome, Italy: International Clearinghouse for Birth Defects Surveillance and Research, 2016." if year_start==2012

//-------------------------------------------------------
tempfile all_years
save `all_years', replace 

//--------------------------
// merge in bundle_id's 
//-------------------------------------------------------
use `all_years', replace 
// merge in location information 

	// export list of registries to create location map
	replace registry_name = trim(registry_name)
		preserve
			keep registry_name
			duplicates drop
			sort registry_name
			export excel using "{FILEPATH}\ICBDSR_location_map_`date'.xlsx", firstrow(variables)
		restore

	// merge in location names from map
	preserve
		import excel using "{FILEPATH}\ICBDSR_location_map_12_19_16.xlsx", firstrow clear 
		tempfile loc_names
		save `loc_names', replace
	restore

	merge m:1 registry_name using `loc_names', keep(3) nogen
		
	// merge in GBD location information
	replace location_name = trim(location_name)
	
	preserve
		run "{FILEPATH}\get_location_metadata.ado"
			get_location_metadata, location_set_id(9) clear
				duplicates drop location_name, force 
							tempfile locations 
							save `locations', replace 
			restore

	merge m:1 location_name using `locations', keepusing(location_id ihme_loc_id) keep(1 3) nogen
		replace location_id = 35436 if location_name=="Tokyo"
		replace ihme_loc_id = "JPN_35436" if location_name=="Tokyo"

// clean up
drop iso3 gbd_region subnational_id national urbanicity

drop site_memo
	rename registry_name site_memo

//------------------------------------------------------------------------------
// map to modelable entities and causes
		
	// export list of case names
	preserve
		keep original_case_name 
		duplicates drop
		sort orig
		export excel using "{FILEPATH}\ICBDSR_case_names_`date'2.xlsx", firstrow(variables) sheet(new) sheetreplace 
	restore

	preserve
		import excel using "{FILEPATH}\ICBDSR_case_names_12_19_16.xlsx", firstrow clear 
		replace orig = trim(orig)
		duplicates drop orig case_def, force
			tempfile causes
			save `causes', replace
	restore

	replace orig = trim(orig)
	merge m:1 original_case_name case_definition_notes using `causes', keep(3) nogen

		drop if cause=="drop" | cause=="other"

		preserve
			import excel using "{FILEPATH}\Bundle_to_ME_map_{USERNAME}.xlsx", firstrow clear 
				keep if regexm(modelable_entity_name, "irth prevalence")
					keep modelable_entity_name bundle_id
					replace modelable_entity_name =trim(modelable_entity_name)
			
			tempfile bundle_map
			save `bundle_map', replace
			restore

	merge m:1 modelable_entity_name using `bundle_map', nogen keep(1 3)
		drop if inlist(modelable, "microcephaly")

	rename original_case_name case_name

//  other metadata that needs to be fixed before collapsing

	// fill in file_path values
		replace file_path = file_location if file_path =="" & file_location !=""
		replace file_path = "{FILEPATH}\CONGENITAL_MALFORMATIONS_WORLDWIDE_1991_Y2014M01D09.PDF" ///
				if file_name=="CONGENITAL_MALFORMATIONS_WORLDWIDE_1991_Y2014M01D09" & file_path==""
					drop file_location 

	// fill in missing field_citation values 
		replace field_cit = "International Clearinghouse for Birth Defects Monitoring Systems. International Clearinghouse for Birth Defects Monitoring Systems Annual Report 2003. Rome, Italy: International Centre on Birth Defects." if nid==127952
		replace field_cit = "International Clearinghouse for Birth Defects Monitoring Systems. International Clearinghouse for Birth Defects Monitoring Systems Annual Report 2004. Rome, Italy: International Centre on Birth Defects, 2006." if nid==128693
		replace field_cit = "International Clearinghouse for Birth Defects Surveillance and Research. International Clearinghouse for Birth Defects Surveillance and Research Annual Report 2005. Rome, Italy: International Clearinghouse for Birth Defects Surveillance and Research, 2007" if nid==128702
		replace field_cit = "International Clearinghouse for Birth Defects Surveillance and Research. International Clearinghouse for Birth Defects Surveillance and Research Annual Report 2006. Rome, Italy: International Clearinghouse for Birth Defects Surveillance and Research, 2007." if nid==128711
		replace field_cit = "International Clearinghouse for Birth Defects Surveillance and Research. International Clearinghouse for Birth Defects Surveillance and Research Annual Report 2007. Rome, Italy: International Clearinghouse for Birth Defects Surveillance and Research, 2008." if nid==128726
		replace field_cit = "International Clearinghouse for Birth Defects Surveillance and Research. International Clearinghouse for Birth Defects Surveillance and Research Annual Report 2009. Rome, Italy: International Clearinghouse for Birth Defects Surveillance and Research." if nid==128727 
		replace field_cit = "International Clearinghouse for Birth Defects Surveillance and Research. International Clearinghouse for Birth Defects Surveillance and Research Annual Report 2010. Rome, Italy: International Clearinghouse for Birth Defects Surveillance and Research, 2011." if nid==128728
		replace field_cit = "International Clearinghouse for Birth Defects Surveillance and Research. International Clearinghouse for Birth Defects Surveillance and Research Annual Report 2011. Rome, Italy: International Clearinghouse for Birth Defects Surveillance and Research, 2012." if nid==128729
		replace field_cit = "International Clearinghouse for Birth Defects Surveillance and Research. International Clearinghouse for Birth Defects Surveillance and Research Annual Report 2012. Rome, Italy: International Clearinghouse for Birth Defects Surveillance and Research, 2013." if nid==128730
		replace field_cit = "International Clearinghouse for Birth Defects Monitoring Systems. Congenital Malformations Worldwide: A Report from the International Clearinghouse for Birth Defects Monitoring Systems. Amsterdam, Netherlands: Elsevier, 1991." if nid==129094
		replace field_cit = "International Clearinghouse for Birth Defects Surveillance and Research. International Clearinghouse for Birth Defects Surveillance and Research Annual Report 2014. Rome, Italy: International Clearinghouse for Birth Defects Surveillance and Research, 2016." if nid==264790
				

// drop duplicated registry data from 2009: 
	drop if site_memo=="Hungary" & regexm(file_name, "2009") & year_start==2007

// fix underlying_nid value
	replace underlying_nid= 265364 if site=="Mexico: RYVEMCE" & year_start==2012

// fix file_name
	replace file_name = "ICBDSR_2006" if site=="Australia: VBDR" & year_start==2004


//-------------------------------
// split "hypospadias and epispadias" according to the proportions 
	tab site_memo year_start if inlist(case_name, "Hypospadias & Epispadias", "Hypospadias & epispadias", "Hypospadias + epispadias")

	// get proportion
	preserve
		sort case_name
		keep if regexm(case_name, "spadias") & !(inlist(case_name, "Hypospadias & Epispadias", "Hypospadias & epispadias", "Hypospadias + epispadias"))
			replace case_name = "Hypospadias" if regexm(case_name, "ypospad")

		collapse(sum) cases sample_size, by(case_name cause)
		gen mean = cases/sample_size
		bysort cause: egen tot = sum(mean)
		gen prop = mean/tot
			local e_prop = prop in 1
			local h_prop = prop in 2
	restore


// split "Hypospadias and Epispadias" according to proportions
	replace case_name = "Hypospadias and Epispadias" if inlist(case_name, "Hypospadias & Epispadias", "Hypospadias & epispadias", "Hypospadias + epispadias")
		expand 2 if case_name=="Hypospadias and Epispadias", gen(new)

				replace cases = cases*`h_prop' if new==0 & case_name=="Hypospadias and Epispadias"
				replace sample_size = sample_size*`h_prop' if new==0 & case_name=="Hypospadias and Epispadias"
					replace case_name = "Hypospadias" if new==0 & case_name=="Hypospadias and Epispadias"
					replace modelable_entity_name = "Birth prevalence of congenital genital anomalies" if case_name=="Hypospadias"
					replace bundle_id = 619 if case_name== "Hypospadias"


				replace cases = cases*`e_prop' if new==0 & case_name=="Hypospadias and Epispadias" 
				replace sample_size = sample_size*`e_prop' if new==0 & case_name=="Hypospadias and Epispadias"
					replace case_name = "Epispadias" if new==1 & case_name=="Hypospadias and Epispadias"
					replace modelable_entity_name = "Birth prevalence of congenital urinary anomalies" if case_name=="Epispadias"
					replace bundle_id = 617 if case_name=="Epispadias"

					drop new


//------------------------------
// add sex splits for cleft lip
		// Sex split = 64% of those with cleft lip (with or without cleft palate)  ///
				// and 43.5% of those with isolated cleft palate are male (Reference NIDs 310647, 310645, and 310649). 
			gen sex=""
			gen group =.
			gen specificity=""
			gen group_review=.

		preserve
			keep if cause=="cleft"
			expand 2, gen(new)
				replace sample_size = sample_size/2 

				replace sex = "Male" if new==0
					replace cases = cases * 0.64 if new==0 & case_name == "Cleft lip with or without palate"
					replace cases = cases * 0.435 if new==0 & case_name == "Cleft palate without cleft lip"

				replace sex = "Female" if new ==1
					replace cases = (cases * (1 - 0.64)) if new==1 & case_name == "Cleft lip with or without palate"
					replace cases = (cases * (1 - 0.435)) if new==1 & case_name == "Cleft palate without cleft lip"

			replace group = 1
			replace specificity="sex split"
			replace group_review =1

			tempfile cleft_sex_split
			save `cleft_sex_split', replace
		restore

		append using `cleft_sex_split'

			replace specificity = "both sexes" if cause=="cleft" & sex==""
			replace group_review =0 if cause=="cleft" & sex==""
			replace group = 2 if cause=="cleft" & sex==""
				drop new 

tempfile with_splits
save `with_splits', replace 

//-------------------------------------------------
// collapse to the ME level
use `with_splits', clear 

	// create concatenated case_name & case_definition variables
			replace case_name = subinstr(case_name, `"""', "", . )
			replace note_SR = subinstr(note_SR, `"""', "", . )
			replace case_definition = subinstr(note_SR, `"""', "", . )
			tostring page_num, replace
			tostring table_num, replace 

	egen collapse_group = group(site_memo file_path file_name field_citation_value underlying_field_citation_value extractor nid ///
			underlying_nid smaller_site_unit sample_size age_start age_end year_start year_end measure ///
			location_name location_id ihme_loc_id modelable_entity_name bundle_id cause sex group specificity group_review), missing


	// keep only totals within the limb reduction ME when the breakdowns are also available, so that groups are not double-counted when collapsed 
		bysort collapse_group: gen count = _N
		drop if count >1 & bundle_id==605 & !regexm(case_name, "Total")

	// also drop the duplicated Down Syndrome data to prevent double-counting across maternal ages
		gen down_fix =1 if count >1 & bundle_id==227
			foreach var in cases prenatal_fd prenatal_top {
					bysort collapse_group: egen `var'_temp = max(`var') if down_fix==1
					replace `var' = `var'_temp if down_fix==1
					drop `var'_temp
						}
						duplicates drop count year_start year_end sex bundle_id case_name site_memo cases prenatal*  sample_size, force
						drop down_fix

tempfile pre_collapse
save `pre_collapse', replace

	sort collapse_group modelable_entity_name case_name
		// case_name
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// case_definition
		by collapse_group : gen new_case_definition = case_def[1]
		by collapse_group : replace new_case_def = new_case_def[_n-1] + ", " + case_def if _n > 1
		by collapse_group : replace new_case_def = new_case_def[_N]

		// note_SR
		by collapse_group : gen new_note_SR = note_SR[1]
		by collapse_group : replace new_note_SR = new_note_SR[_n-1] + ", " + note_SR if _n > 1
		by collapse_group : replace new_note_SR = new_note_SR[_N]

		// page_num
		by collapse_group : gen new_page_num = page_num[1]
		by collapse_group : replace new_page_num = new_page_num[_n-1] + ", " + page_num if _n > 1
		by collapse_group : replace new_page_num = new_page_num[_N]

		// table_num
		by collapse_group : gen new_table_num = table_num[1]
		by collapse_group : replace new_table_num = new_table_num[_n-1] + ", " + table_num if _n > 1
		by collapse_group : replace new_table_num = new_table_num[_N]

		// response_rate
		by collapse_group : gen new_response_rate = response_rate[1]
		by collapse_group : replace new_response_rate = new_response_rate[_n-1] + ", " + response_rate if _n > 1
		by collapse_group : replace new_response_rate = new_response_rate[_N]

// drop "other"'s
	drop if modelable=="other"

	// collapse
		collapse (sum) cases prenatal_top prenatal_fd (max) cv_livestill cv_topnot cv_prenatal (median) sample_size, ///
			by(site_memo file_path file_name field_citation_value underlying_field_citation_value extractor nid ///
			underlying_nid smaller_site_unit case_definition age_start age_end ///
			year_start year_end measure location_name location_id ihme_loc_id modelable_entity_name bundle_id cause ///
			sex group specificity group_review new_case_name new_note_SR new_case_definition count)

		rename new_* *

	tempfile collapsed1
	save `collapsed1', replace


//--------------------------------------------
// collapse to causes, for causes that have the "total" envelopes
	
use `pre_collapse', clear
	drop collapse_group count
	keep if inlist(cause, "neural", "msk", "digest", "heart") 

	// create concatenated case_name & case_definition variables
			replace case_name = subinstr(case_name, `"""', "", . )
			replace note_SR = subinstr(note_SR, `"""', "", . )
			replace case_definition = subinstr(note_SR, `"""', "", . )
			tostring page_num, replace
			tostring table_num, replace 

	egen collapse_group = group(site_memo file_path file_name field_citation_value underlying_field_citation_value extractor nid ///
			underlying_nid smaller_site_unit age_start age_end year_start year_end measure ///
			location_name location_id ihme_loc_id cause sex group specificity group_review sample_size), missing
		bysort collapse_group: gen count = _N


	// within a collapse group, drop omphalocele, gastro, and unspecified abdom wall when "total abdominal wall" is also reported 
		 gen t1 =1 if regexm(case_name, "Total") & regexm(case_name, "bdominal wall")
		 	bysort collapse_group: egen t2 = max(t1)
		 drop if regexm(case_name, "mphalocele") & t2==1
		 drop if regexm(case_name, "astroschisis") & t2==1
		 	drop t1 t2

	sort collapse_group modelable_entity_name case_name 
		// case_name
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// case_definition
		by collapse_group : gen new_case_definition = case_def[1]
		by collapse_group : replace new_case_def = new_case_def[_n-1] + ", " + case_def if _n > 1
		by collapse_group : replace new_case_def = new_case_def[_N]

		// note_SR
		by collapse_group : gen new_note_SR = note_SR[1]
		by collapse_group : replace new_note_SR = new_note_SR[_n-1] + ", " + note_SR if _n > 1
		by collapse_group : replace new_note_SR = new_note_SR[_N]

		// page_num
		by collapse_group : gen new_page_num = page_num[1]
		by collapse_group : replace new_page_num = new_page_num[_n-1] + ", " + page_num if _n > 1
		by collapse_group : replace new_page_num = new_page_num[_N]

		// table_num
		by collapse_group : gen new_table_num = table_num[1]
		by collapse_group : replace new_table_num = new_table_num[_n-1] + ", " + table_num if _n > 1
		by collapse_group : replace new_table_num = new_table_num[_N]

		// response_rate
		by collapse_group : gen new_response_rate = response_rate[1]
		by collapse_group : replace new_response_rate = new_response_rate[_n-1] + ", " + response_rate if _n > 1
		by collapse_group : replace new_response_rate = new_response_rate[_N]

// collapse
		collapse (sum) cases prenatal_top prenatal_fd (max) cv_livestill cv_topnot cv_prenatal (median) sample_size, ///
			by(site_memo file_path file_name field_citation_value underlying_field_citation_value extractor nid ///
			underlying_nid smaller_site_unit age_start age_end ///
			year_start year_end measure location_name location_id ihme_loc_id cause ///
			sex group specificity group_review new_case_name new_note_SR new_case_definition count)

	rename new_* *

	// fill in ME and bundle information
	gen modelable_entity_name=""
		replace modelable= "Birth prevalence of total congenital heart defects" if cause=="heart"
		replace modelable= "Birth prevalence of total neural tube defects" if cause=="neural"
		replace modelable= "Total musculoskeletal congenital anomalies" if cause=="msk"
		replace modelable= "Birth prevalence of total congenital digestive anomalies" if cause=="digest"

	gen bundle_id =.
		replace bundle_id = 621 if cause=="digest"
		replace bundle_id = 603 if cause=="msk"
		replace bundle_id = 629 if cause=="heart"
		replace bundle_id = 609 if cause=="neural"

tempfile collapsed2
save `collapsed2', replace

//------------------------------------
     append using `collapsed1'
//------------------------------------

// code in cv_subset and drop case_names that are insufficient			
	preserve
		duplicates drop case_name, force 
		keep case_name cause modelable count
		order cause modelable case_name count
		sort cause modelable count
		gen cv_subset =""

		export excel using "{FILEPATH}\cv_subset_mapping_{VERSION}.xls", sheetreplace firstrow(variables)
	restore */

	preserve
		import excel using "{FILEPATH}\cv_subset_mapping_CURRENT.xls", firstrow clear 			
		keep if cv_subset !=.
		tempfile cv_subset
		save `cv_subset', replace
	restore

	merge m:1 case_name using `cv_subset' , nogen
		rename cv_subset cv_under_report

	
//-----------------------------------------------------------------
// final variable names changes and other formatting for upload to database

	// fill in underlying NIDs
	preserve
		import excel using "{FILEPATH}\ICBDSR_underlying_nids.xlsx", clear firstrow 
			tempfile underlying_nids
			save `underlying_nids', replace 
		restore

	merge m:1 year_start site_memo nid using `underlying_nids', keepusing(underlying_nid underlying_field )update replace 

	// generate necessary variables 
		gen mean = cases/sample_size
		gen lower =.
		gen upper =.
		gen standard_error =.
		gen effective_sample_size =.
		gen unit_type = "Person"
		gen uncertainty_type =.
		gen uncertainty_type_value =.
		gen unit_value_as_published =1
		gen outlier_type_id =0

		tostring measure, replace 
			replace measure = "prevalence"
		replace sex = "Both" if sex==""
			
		gen seq =.
		gen seq_parent =.

		gen page_num =.
		gen table_num =.
		gen source_type = "Registry - congenital"
		gen representative_name = "Representative for subnational location only"
		gen urbanicity_type = "Mixed/both"
		gen recall_type = "Point"
		gen recall_type_value =.
		gen sampling_type =.
		gen response_rate =.
		gen case_diagnostics = "Cases defined according to ICBDSR case definitions; see note_SR section"
		gen note_modeler =.
		gen input_type = "extracted"
		gen design_effect =.
		gen prenatal_fd_definition = "see case_definition field"
		drop case_definition_notes 
		replace case_definition = subinstr(case_definition, " ; ", "", .)

		gen sex_issue =0
		gen year_issue =0
		gen age_issue = 0
		gen age_demographer =0
		gen measure_issue =0
		gen measure_adjustment =0
		replace extractor = "{USERNAME}"

		replace smaller_site_unit =1 if regexm(case_definition, "regional") & !regexm(site_memo, "USA")
			replace smaller_site_unit =0 if smaller_site_unit==1

	// get the cv's to match up 
		gen cv_aftersurgery =0
		gen cv_1facility_only =0
		gen cv_low_income_hosp =0
	
	// drop unnecessary vars 
		drop count
		drop file_name

	// cv_excludes_chromos
		gen cv_excludes_chromos = 1 if cv_includes_chromos==0
		replace cv_excludes_chromos =0 if cv_includes_chromos==1


// order variables to match the 2016 extraction template
	order bundle_id modelable_entity_name seq seq_parent underlying_nid nid field_citation_value file_path page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo ///
		sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size unit_type unit_value_as_published ///
		measure_issue measure_adjustment uncertainty_type uncertainty_type_value representative_name urbanicity_type recall_type recall_type_value sampling_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor ///
		 input_type underlying_field_citation_value design_effect cv* ///
		 prenatal_fd prenatal_top prenatal_fd_definition

cap drop _m

//---------------------------------------------------------
// split into bundles and save files for upload

	// format "cause" var
		replace cause = "cong_" + cause
		replace cause = cause + "s" if cause=="cong_down"
		replace cause = cause + "ive" if cause=="cong_digest"

local date {DATE}

	 levelsof bundle_id, local(bundles)
		foreach b in `bundles' {
			preserve
			keep if bundle_id==`b'
			local c = cause

				local file  "{FILEPATH}\ICBDSR_`c'_`b'_prepped_`date'"
				export excel using "`file'.xls", firstrow(variables) sheet(extraction) replace 

				local me = modelable_entity_name
				di in red "Exported `me' data for upload"
			restore
		}




