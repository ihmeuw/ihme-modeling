
// {AUTHOR NAME}
// Purpose: cross-walk NBDPN *congenital heart anomalies* data to match the case names included in other registries

/* Backstory: The NBDPN (USA registry data) reports more conditions within each category of congenital heart defects
	than any other birth defects registry the GBD Study currently uses. Thus, we treat this registry as the "gold standard" among 
	registries for diagnosing congenital heart defects at birth. Without any registry crosswalks, the birth prev congenital heart 
	models show MUCH higher estimates in the US than anywhere in the world. In GBD 2015, we cross-walked the USA down to match all 
	other registies; however, the better practice is to cross-walk all other registries up to the NBDPN case definition list. 

	To do this in DisMod, we need to upload multiple copies of the NBDPN data, one to drive each registry-specific crosswalk; 
	i.e., we need to generate a dataset of hypothetical NBDPN birth prevalence levels if the NBDPN used only case defs reported 
	in EUROCAT, World Atlas, etc. */

//--------------------
/* Outline: 
	1. prep NBDPN data to pre-collapse stage (use previously written code, see "NBDPN_data_prep_2016_GATHER")

	2. get list of case_names and code whether they were included in each registry; e.g.
		case_name | is_EUROCAT | is_ICDBSR | is_World_Atlas | is_MCHS |
		 TGA			0			1				1			0

	3. for each registry: drop if is_reg==0, then collapse to modelable entity (ME) level, 
											then collapse to cause level,
											then format and export using registry-specific covariates

	/---------
	After code is complete: 
	4. upload duplicated data to NEW bundles 
	5. upload all old data to NEW bundles 
	-----------/ */

 // setup
	clear all
	set more off
	set maxvar 10000
	cap restore, not
	cap log close

	// set prefix for the OS 
	if c(os) == "Windows" {
		global dl "{FILEPATH}"
	}
	if c(os) == "Unix" {
		global dl "{FILEPATH}"
		set odbcmgr unixodbc
	}

	local dir "$dl\{FILEPATH}"

//----------------------------------------------------------------
// 1. prep NBDPN data to pre-collapse stage 

	// file paths
	local in_dir "{FILEPATH}"
	local cause_map  "{FILEPATH}\usa_cause_map.xlsx"
	local bundle_map "{FILEPATH}\Bundle_to_ME_map_{USERNAME}.xlsx"

	local date {DATE} 

	//------------------------------------------------------------------------------
	// load data
	import excel using "`in_dir'\{FILEPATH}", ///
	sheet("extraction") firstrow clear
	drop in 1
	drop if nid == ""
		drop row_num parent_id input_type modelable_entity_id modelable_entity_name underlying_nid underlying_field_citation_value table_num ///
		 standard_error recall_type_value sampling_type response_rate group specificity group_review note_modeler data_sheet_filepath file_path ///
		 effective_sample_size design_effect uncertainty_type uncertainty_type_value BO case_definition case_diagnostics
			destring nid location_id smaller_site_unit sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer ///
			measure mean lower upper cases sample_size unit_type unit_value_as_published measure_issue measure_adjustment is_outlier, replace 
		tempfile 2000_data
		save `2000_data', replace 

	import excel using "`in_dir'\{FILEPATH}", ///
	sheet("extraction") firstrow clear
	drop in 1
	drop if nid == . 
	replace case_name = trim(case_name)	
	drop if case_name == ""
		drop row_num parent_id input_type modelable_entity_id modelable_entity_name underlying_nid underlying_field_citation_value table_num ///
		 standard_error recall_type_value sampling_type response_rate group specificity group_review note_modeler cv_ data_sheet_filepath file_path cv_ ///
		 effective_sample_size design_effect uncertainty_type uncertainty_type_value BO case_definition case_diagnostics		
		tempfile 2001_data
		save `2001_data', replace 

	import excel using "`in_dir'\{FILEPATH}", ///
	sheet("extraction") firstrow clear
		drop in 1
	drop if nid == ""
		drop row_num parent_id input_type modelable_entity_id modelable_entity_name underlying_nid underlying_field_citation_value table_num ///
		 standard_error recall_type_value sampling_type response_rate group specificity group_review note_modeler data_sheet_filepath file_path ///
		 effective_sample_size design_effect uncertainty_type uncertainty_type_value BN case_definition case_diagnostics		
			destring nid location_id smaller_site_unit sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer ///
			measure mean lower upper cases sample_size unit_type unit_value_as_published measure_issue measure_adjustment is_outlier cv*, replace 
		tempfile 2005_data
		save `2005_data', replace
	
	import excel using "`in_dir'\{FILEPATH}", ///
	sheet("extraction") firstrow clear	
	drop in 1 
	drop if nid == "" 
		drop row_num parent_id input_type modelable_entity_id modelable_entity_name underlying_nid underlying_field_citation_value table_num ///
		 standard_error recall_type_value sampling_type response_rate group specificity group_review note_modeler data_sheet_filepath file_path  ///
		 effective_sample_size design_effect uncertainty_type uncertainty_type_value BO cv_ case_definition case_diagnostics		
			destring nid location_id smaller_site_unit sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer ///
			measure mean lower upper cases sample_size unit_type unit_value_as_published measure_issue measure_adjustment is_outlier cv*, replace
	tempfile 2010_data
	save `2010_data', replace

	import excel using "`in_dir'\{FILEPATH}", ///
	sheet("extraction") firstrow clear
		drop in 1
		drop if nid == ""
			drop row_num parent_id input_type modelable_entity_id modelable_entity_name underlying_nid underlying_field_citation_value table_num ///
		 	standard_error recall_type_value sampling_type response_rate group specificity group_review note_modeler data_sheet_filepath file_path ///
		 	effective_sample_size design_effect uncertainty_type uncertainty_type_value BO cv_ case_definition case_diagnostics	 B* C* D* E* F*
				destring nid location_id smaller_site_unit sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer ///
				measure mean lower upper cases sample_size unit_type unit_value_as_published measure_issue measure_adjustment is_outlier cv*, replace
		replace case_name = trim(case_name)
		tempfile 2015_data1
		save `2015_data1', replace

	import excel using "`in_dir'\{FILEPATH}", ///
	sheet("extraction") firstrow clear
		drop in 1
		drop if nid == ""
			drop row_num parent_id input_type modelable_entity_id modelable_entity_name underlying_nid underlying_field_citation_value table_num ///
		 	standard_error recall_type_value sampling_type response_rate group specificity group_review note_modeler data_sheet_filepath file_path ///
		 	effective_sample_size design_effect uncertainty_type uncertainty_type_value BO cv_ case_definition case_diagnostics		
				destring nid location_id smaller_site_unit sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer ///
				measure mean lower upper cases sample_size unit_type unit_value_as_published measure_issue measure_adjustment is_outlier cv*, replace
		tempfile 2015_data2
		save `2015_data2', replace


	//-----------------------------------------
	use `2000_data', clear
	append using `2001_data' 
	append using `2005_data'
	append using `2010_data'
	append using `2015_data1'
	append using `2015_data2'
		rename case_name usa_case_name

	// identify and fill in missing variables now so they don't cause problems later 
	foreach var of varlist * {
	cap confirm string variable `var'
	if !_rc {
		di "`var'"
		count if `var' ==""
		}
	cap confirm numeric variable `var'
	if !_rc {
		di "`var'"
		count if `var' ==.
		}	
	}

	// extracting missing info as necessary
	replace year_issue =0 if year_issue==.
	replace recall_type ="Point" if recall_type==""
	replace field_cit = "2001_Report_Birth_Defects_Prevention_Network" if field_cit==""
	replace extractor = "{USERNAME}" if extractor==""
	replace urban = "Mixed/both" if urban==""
	replace smaller_site_unit=0 if smaller_site_unit==.
	replace is_outlier =0 if is_outlier==.

	gen split_loc = strpos(location_name, "|")
		replace location_name = substr(location_name, 1, split_loc -1 )
		drop split_loc

	// fill in covariate coding for cv_livestill and cv_topnotrecorded, using information from the note_SR column and state reports
		// this is the same as what was done for 2015
		replace cv_livestill = 1 if year_start == 1989 & inlist(location_name, "Arizona", "Missouri", "South Carolina")
		replace cv_livestill = 1 if year_start == 1994 & inlist(location_name, "Massachusetts")
		replace cv_livestill = 1 if year_start == 1995 & inlist(location_name, "Arizona")
		replace cv_livestill = 1 if year_start == 1996 & inlist(location_name, "Massacusetts")
		replace cv_livestill = 1 if year_start == 1998 & inlist(location_name, "Alabama")
		replace cv_topnotrecorded = 1 if year_start == 1998 & inlist(location_name, "Alabama")

		replace cv_livestill = 1 if location_name =="Illinois" & inlist(year_start, 1998)
		replace cv_livestill = 1 if location_name=="Kentucky" &  inlist(year_start, 1998)
		replace cv_topnotrecorded = 1 if location_name == "Texas" & inlist(year_start, 1998)  
		replace cv_livestill = 1 if location_name == "Texas" & inlist(year_start, 1998)

		replace cv_livestill = 1 if year_start ==2003 & inlist(location_name, "Arizona", "Arkansas", "Illinois", "Nebraska", "Oklahoma")
		replace cv_livestill = 1 if year_start ==2003 & inlist(location_name, "Maine", "Massachusetts", "New Hampshire", "Tennessee", "Texas", "Puerto Rico")
		replace cv_topnotrecorded = 1 if year_start ==2003 & inlist(location_name, "Texas", "Puerto Rico")

		replace cv_livestill = 1 if location_name=="New Mexico" & inlist(year_start, 2004)
		replace cv_topnotrecorded = 1 if location_name=="New Mexico" & inlist(year_start, 2004)
		replace cv_livestill = 1 if location_name=="South Carolina" &inlist(year_start, 2006, 2007) 
		replace cv_livestill = 1 if year_start == 2008 & inlist(location_name, "Arizona", "California", "Colorado", "Delaware", "Georgia", "Illinois", "Iowa", "Kansas")
		replace cv_livestill = 1 if year_start ==2008 & inlist(location_name, "Kentucky", "Maine", "Massachusetts", "Michigan", "Mew Hampshire", "New Mexico", "North Carolina")
		replace cv_livestill = 1 if year_start ==2008 & inlist(location_name, "Oklahoma", "Puerto Rico", "Rhode Island", "South Carolina", "Tennessee", "Texas", "Utah", "Wisconsin")
		replace cv_livestill = 1 if year_start ==2010 & inlist(location_name, "Arizona", "Maine", "Maryland")	
		replace cv_livestill = 1 if year_start ==2011 & inlist(location_name, "Arizona")
		replace cv_livestill = 1 if year_start ==2012 & inlist(location_name, "Arizona")

			replace cv_livestill=0 if cv_livestill==.
			replace cv_top=0 if cv_top==.

	// want cases & sample size only 
	replace lower=.
	replace upper=.

	//-----------------------------------------------------------------------------------
	// map to current cases 
	preserve
		import excel using "`cause_map'", firstrow clear 
			drop D E
		tempfile cause_map_prepped
		save `cause_map_prepped', replace
	restore

	merge m:1 usa_case_name using `cause_map_prepped', keep(3) nogen
		rename modelable_entity_name modelable_entity_name_old
		replace modelable_entity_name_old=trim(modelable_entity_name_old)
		rename usa_case_name case_name

	// & map to bundle_id's 
	preserve
		import excel using "`bundle_map'", firstrow clear 
			keep if regexm(modelable_entity_name, "irth prevalence") & is_2016=="Y"
			replace modelable_entity_name_old=trim(modelable_entity_name_old)
				replace modelable_entity_name=trim(modelable_entity_name)
				tempfile bundle_map_prepped
				save `bundle_map_prepped', replace
	restore

	merge m:1 modelable_entity_name_old using `bundle_map_prepped', keep(1 3) nogen
		drop if inlist(modelable_entity_name_old, "exclude" )
		drop is_2015 is_2016 cv_

	//-------------------------------------------------------------------
	// Add sex splits for cleft lip
		// Sex split = 64% of those with cleft lip (with or without cleft palate)  ///
				// and 43.5% of those with isolated cleft palate are male (Reference NIDs 310647, 310645, and 310649). 

		// creating male and female data points for birth prevalence, and using group review to hide the both-sex cleft data 
			gen is_lip =1 if cause=="cong_cleft" & inlist(case_name, "Cleft lip alone", "Cleft lip with cleft palate", "Cleft lip with and without cleft palate")
			gen is_palate =1 if cause=="cong_cleft" & inlist(case_name, "Cleft paate without cleft lip", "Cleft palate alone", "Cleft palate without cleft lip")

			expand 2 if cause=="cong_cleft", gen(new)
			replace sample_size = sample_size/2 if cause=="cong_cleft"

				replace sex = "Male" if new==0 & cause== "cong_cleft"
					replace cases = cases * 0.64 if new==0 & is_lip==1
					replace cases = cases * 0.435 if new==0 & is_palate==1

				replace sex = "Female" if new == 1 & cause== "cong_cleft"
					replace cases = (cases * (1 - 0.64)) if new==1 & is_lip==1
					replace cases = (cases * (1 - 0.435)) if new==1 & is_palate==1
	
			replace measure_adjustment=1 if cause=="cong_cleft"
			drop new

	//-----------------------------------------------------------------------------
	// Split the "Hypospadias and Epispadias", using proportions of Hypo/Epi where they are available separately
	
	// get proportion
	preserve
		sort case_name
		keep if inlist(case_name, "Epispadias", "Hypospadias")
		collapse(sum) cases sample_size, by(case_name cause)
		gen mean = cases/sample_size 
		bysort cause: egen tot = sum(mean)
		gen prop = mean/tot
			local e_prop = prop in 1
			local h_prop = prop in 2
	restore

	// split "Hypospadias and Epispadias" according to proportions
	replace case_name = "Hypospadias and Epispadias" if case_name =="Hypospadias  and Epispadias"
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

//---------------------------------
tempfile ready_for_crosswalks
save `ready_for_crosswalks', replace

//--------------------------------------------------------------------------------------------

// 2. Merge in identifiers for whether case_names were included in each registry 
use `ready_for_crosswalks', clear

	replace cause = trim(cause)
	keep if cause=="cong_heart"

		// standardize case names in extracted data 
				rename case_name case_name_old
					preserve
					import excel using "`dir'\{FILEPATH}", firstrow clear 
						tempfile case_names
						save `case_names', replace 
					restore
				merge m:1 case_name_old using `case_names', nogen

		// merge in spreadsheet with indicators for whether the case_names are included in registries 
			preserve
				import excel using "`dir'\registry_crosswalk_indicators_heartonly.xls", firstrow clear 
					tempfile reg_indicators
					save `reg_indicators', replace
				restore 
			merge m:1 case_name modelable_entity_name using `reg_indicators', nogen

// 3. Duplicate to one copy for each registry crosswalk

 foreach reg in EUROCAT ICBDSR World_Atlas {

		preserve
			keep if is_`reg'==1
			gen reg_crosswalk="`reg'"
			tempfile `reg'
			save ``reg'', replace 
		restore 
	}

	use `EUROCAT', clear
		append using `ICBDSR'
		append using `World_Atlas'

tempfile ready_to_collapse
save `ready_to_collapse', replace

//-------------------------------------------
// collapse to modelable entities
use `ready_to_collapse', clear
	drop if modelable_entity_name_old=="other"
		drop modelable_entity_name_old

	// create concatenated case_name, note_SR and page_num variables 
	egen collapse_group = group(modelable_entity_name bundle_id cause nid field_citation_value source_type ///
		 location_name location_id ihme_loc_id smaller_site_unit site_memo sex ///
		sex_issue year_start year_end age_start age_end age_demographer measure unit_type unit_value_as_published measure_issue measure_adjustment ///
		representative_name urbanicity_type recall_type extractor is_outlier cv* reg_crosswalk), missing

				replace case_name = subinstr(case_name, `"""', "", . )
				replace note_SR = subinstr(note_SR, `"""', "", . )
			sort collapse_group modelable_entity_name case_name 
		
		// case_name
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// note_SR
		by collapse_group : gen new_note_SR = note_SR[1]
		by collapse_group : replace new_note_SR = new_note_SR[_n-1] + ", " + note_SR if _n > 1
		by collapse_group : replace new_note_SR = new_note_SR[_N]

		// page_num
		by collapse_group : gen new_page_num = page_num[1]
		by collapse_group : replace new_page_num = new_page_num[_n-1] + ", " + page_num if _n > 1
		by collapse_group : replace new_page_num = new_page_num[_N]
			 drop collapse_group

	tempfile temp
	save `temp', replace

	use `temp', clear

	// collapse
	count
	gen case_name_count = 1
	collapse (sum) cases case_name_count, by(modelable_entity_name bundle_id cause nid field_citation_value sample_size source_type ///
		 location_name location_id ihme_loc_id smaller_site_unit site_memo sex sex_issue year_start year_end year_issue age_start age_end age_demographer ///
		 measure unit_type unit_value_as_published measure_issue measure_adjustment representative_name urbanicity_type recall_type extractor is_outlier cv* ///
		 new_* reg_crosswalk)
			count
			rename new_* *

		tempfile collapsed_to_MEs
		save `collapsed_to_MEs', replace 

//-------------------------------------------------------
// collapse to causes
use `ready_to_collapse', clear
	keep if inlist(cause, "cong_neural", "cong_msk", "cong_digest", "cong_heart")

	// create concatenated case_name, note_SR and page_num variables 
	egen collapse_group = group(cause nid field_citation_value source_type ///
		 location_name location_id ihme_loc_id smaller_site_unit site_memo sex ///
		sex_issue year_start year_end age_start age_end age_demographer measure unit_type unit_value_as_published measure_issue measure_adjustment ///
		representative_name urbanicity_type recall_type extractor is_outlier cv* reg_crosswalk), missing
			replace case_name = subinstr(case_name, `"""', "", . )
			replace note_SR = subinstr(note_SR, `"""', "", . )
		sort collapse_group modelable_entity_name case_name 
		
		// case_name
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// note_SR
		by collapse_group : gen new_note_SR = note_SR[1]
		by collapse_group : replace new_note_SR = new_note_SR[_n-1] + ", " + note_SR if _n > 1
		by collapse_group : replace new_note_SR = new_note_SR[_N]

		// page_num
		by collapse_group : gen new_page_num = page_num[1]
		by collapse_group : replace new_page_num = new_page_num[_n-1] + ", " + page_num if _n > 1
		by collapse_group : replace new_page_num = new_page_num[_N]
 	drop collapse_group modelable_entity_name_old

	tempfile temp2
	save `temp2', replace 

	use `temp2', clear 

	// collapse
	count
	gen case_name_count = 1
	collapse (sum) cases case_name_count, by(cause nid field_citation_value source_type ///
		 sample_size location_name location_id ihme_loc_id smaller_site_unit site_memo sex sex_issue year_start year_end year_issue age_start age_end age_demographer ///
		 measure unit_type unit_value_as_published measure_issue measure_adjustment representative_name urbanicity_type recall_type extractor is_outlier cv* ///
		 new_* reg_crosswalk)
			count
				 rename new_* *

		// fill in ME and bundle information
		gen modelable_entity_name=""
 				replace modelable= "Birth prevalence of total congenital digestive anomalies" if cause=="cong_digest"
 				replace modelable= "Birth prevalence of total congenital heart defects" if cause=="cong_heart"
 				replace modelable= "Birth prevalence of total musculoskeletal congenital anomalies" if cause=="cong_msk"
 				replace modelable= "Birth prevalence of total neural tube defects" if cause=="cong_neural"
 					
 				preserve
				import excel using "`bundle_map'", firstrow clear 
					keep if regexm(modelable_entity_name, "otal") & regexm(modelable_entity_name, "irth prevalence")

					tempfile bundle_map2
					save `bundle_map2', replace 
				restore
 			merge m:1 modelable_entity_name using `bundle_map2', keep(3) nogen
 			drop is_2015 is_2016 modelable_entity_name_old

 	append using `collapsed_to_MEs'
 	replace cause = trim(cause)

tempfile almost_ready
save `almost_ready', replace 

// necessary vars and formatting changes for upload to database
use `almost_ready', clear 

	gen mean =.
	gen lower =.
	gen upper =.

	gen seq =.
 	gen parent_id =. 
 	gen input_type =. 
 	gen underlying_nid =.
 	gen underlying_field_citation_value =.
 	gen table_num =.
 	gen standard_error =.
 	gen recall_type_value =.
 	gen sampling_type =.
 	gen response_rate =.
 	gen group =.
 	gen specificity =.
 	gen group_review =.
 	gen note_modeler =.
 	gen age_issue =0

	gen effective_sample_size = sample_size
	gen design_effect =.
	gen uncertainty_type =.
	gen uncertainty_type_value =.

	gen case_definition = case_name
	gen case_diagnostics =.

	rename is_outlier outlier_type_id
	gen file_path = "Original extraction sheet: {FILEPATH}"
	replace extractor = extractor + " + {USERNAME}"

			gen cv_inpatient=0
			gen cv_1facility_only=0
			gen cv_low_income_hosp=0
			gen prenatal_fd_definition = "See information in note_SR column"
			gen cv_excludes_chromos=0 // all of these data include chromosomal conditions

	//---------------------------------------------------
	replace mean = cases/sample_size

				// drop extra vars 
					drop cv_diag_postnatal cv_diag_chromosomal cv_echo_us cv_autopsy cv_not24* case_name_count

				// trim note_SR so it fits max character limit for database record
				replace note_SR = substr(note_SR, 1, 1998) //

//--------------------------------------------------------------
// pull in all current data from the database
	local bundle_dir "$dl/{FILEPATH}"

	foreach b in {BUNDLE IDS} {
	local c cong_heart 
	preserve
	local f "`bundle_dir'/{FILEPATH}" 
			local files : dir "`f'" files *
			local max 0
				foreach ff of local files{
   				 local chooseme = substr("`ff'", 9,4)
    				di "`chooseme'"
    				if(`max' < `chooseme'){
       				 local max `chooseme'
   					 }
					}

			local final_file = "`f'/request_`max'.xlsx"
			di "`final_file'"

		import excel using "`final_file'", firstrow clear
			foreach var in sampling_type underlying_field_citation {
				tostring `var', replace 
				}

		foreach var in table_num uncertainty_type specificity input_type page_num case_diagnostics underlying_field_citation file_path sampling_type note_modeler prenatal_fd_def extractor {
			cap tostring `var', replace
			cap replace `var'="" if `var'=="."
				}


		tempfile `b'_data
		save ``b'_data', replace 
		restore
	}

	foreach var in table_num uncertainty_type specificity input_type page_num case_diagnostics underlying_field_citation file_path sampling_type note_modeler prenatal_fd_def extractor {
			tostring `var', replace
			replace `var'="" if `var'=="."
				}

	append using `629_data'
	append using `631_data'
	append using `633_data'
	append using `635_data'
	append using `637_data'

	//-------------
	// code in registry-specific covariates 

		gen cv_eurocat_to_nbdpn = 1 if (reg=="Eurocat" | regexm(field_cit, "EUROCAT Prevalence Tables"))
		gen cv_icbdsr_to_nbdpn =1 if (reg=="ICBDSR" | inlist(nid, 127952, 128693, 128702, 128711, 128726, 128727, 128728, 128729, 128730, 129094, 264790 ))
		gen cv_worldatlas_to_nbdpn =1 if (reg=="World_Atlas" | regexm(field_cit, "World Atlas of Birth Defects"))
			foreach r in eurocat icbdsr worldatlas {
				replace cv_`r'_to_nbdpn =0 if cv_`r'_to_nbdpn ==.
				}

			replace cv_under_report = 1 if regexm(field_cit, "China Maternal and Child Health")

	// other formatting / cleanup
		replace prenatal_fd_definition = prenatal_fd_def if prenatal_fd_definition !=""
		replace outlier_type_id= is_outlier if outlier_type_id==.
		replace cv_livestill= 1 if cv_no_still ==1 
		replace note_SR = note_sr if note_sr !=""
			drop note_sr
			replace page_num = subinstr(page_num, ", ,", "", .)
			drop count count2 keep cv_aftersurgery is_dental reg_crosswalk prenatal_fd_def is_outlier cv_echo_us cv_inpatient cv_no_still_births file_name

	order bundle_id modelable_entity_name seq underlying_nid nid field_citation_value file_path page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo ///
		sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size unit_type unit_value_as_published ///
		measure_issue measure_adjustment uncertainty_type_value uncertainty_type representative_name urbanicity_type recall_type recall_type_value sampling_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor ///
			parent_id input_type underlying_field_citation_value design_effect prenatal_fd_definition cv_*


	// substitute new bundles 
		replace bundle_id=702 if bundle_id==629
			replace bundle_id=745 if bundle_id==631
			replace bundle_id=743 if bundle_id==633
			replace bundle_id=746 if bundle_id==635
			replace bundle_id=744 if bundle_id==637

		replace seq=. // make this replacement for the first upload only

// upload to new bundles 
//-------------
	local date {DATE}
	
	foreach b in {BUNDLE IDS} {
		preserve
		keep if bundle_id==`b'
		local destination_file "`bundle_dir'/{FILEPATH}/all_input_data_with_NBDPN_duplicated_`date'.xls"

			export excel using "`destination_file'", replace firstrow(variables) sheet("extraction")
			di in red "`c' `b' input data saved"
		restore
		}






