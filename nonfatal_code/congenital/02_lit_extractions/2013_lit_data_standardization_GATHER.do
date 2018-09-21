
** Purpose: Standardize 2013 extracted scientific literature data for congenital birth defects, prep for upload to GBD 2016 database
	** Save individual files in each congenital bundle folder 

** {AUTHOR NAME}, GBD 2016  
**----------------------------------------------------------------------------
** setup
	clear all
	set more off
	set maxvar 10000
	cap restore, not

		// set prefix for the OS 
	if c(os) == "Windows" {
		global dl "{FILEPATH}"
	}
	if c(os) == "Unix" {
		global dl "{FILEPATH}"
		set odbcmgr unixodbc
	}


local function_dir "$dl/{FILEPATH}"

		local date {DATE}

		local in_dir "{FILEPATH}"
			local input1 "`in_dir'\{FILEPATH}"
			local input2 "`in_dir'\{FILEPATH}"
			local input3 "`in_dir'\{FILEPATH}"
			local input4 "`in_dir'\{FILEPATH}"
		
	local dir "{FILEPATH}"
	local out_dir "{FILEPATH}"
	
** mapping files, created for this data prep:
	local case_name_mapping_file "{FILEPATH}"
	local subnational_mapping_file "{FILEPATH}"

** location-mapping information 
	preserve
		run "`function_dir'/get_ids.ado"
		run "`function_dir'/get_location_metadata.ado" 

		get_location_metadata, location_set_id(9) clear
			gen iso3 = substr(ihme_loc_id, 1, 3)
			drop if ihme_loc_id=="" 

			tempfile loc_data 
			save `loc_data', replace
	restore 

**-------------------------------------------
** 1. load data

	import excel using "`input1'", clear firstrow
		gen source = "{USERNAME}"

		destring page_num,ignore("e" "E" "S") replace 
		destring table_num, ignore("a") replace 
		replace age_start = ".5833333" if age_start=="7m"
			destring age_start, replace
		
		rename add1 cv_prenatal
		rename add2 cv_postnatal // note that some studies are marked as both cv_prenatal and cv_postnatal
		rename add3 prenatal_total 
		rename add7 prenatal_fd_definition 

		rename add4 prenatal_top
		rename add5 prenatal_fd
		rename add6 prenatal_lb 

		rename add8 cv_chromo_excl
		rename add9 cv_aftersurgery
	
		tempfile input1_temp
		save `input1_temp', replace 

	import excel using "`input2'", clear firstrow 
		gen source = "{USERNAME}"

		tostring field_geography, replace 
		tostring sampling_type, replace 
		tostring notes, replace 
		tostring add7, replace 
		drop L V 
		rename is_raw raw_adjusted 

		rename add1 cv_prenatal
		rename add2 cv_postnatal // note that some studies are marked as both cv_prenatal and cv_postnatal
		rename add3 prenatal_total 
		rename add4 prenatal_top
		rename add5 prenatal_fd
		rename add6 prenatal_lb 
		rename add7 prenatal_fd_definition 
		rename add8 cv_chromo_excl
		rename add9 cv_aftersurgery

		tempfile input2_temp
		save `input2_temp', replace 

	import excel using "`input3'", clear firstrow
		gen source = "{USERNAME}"

		drop if file_location==""
		drop BC BD BE BF BG BH BI BJ BK
		tostring raw_adjusted, replace 
		tostring prenatal_fd_definition, replace

		tempfile input3_temp
		save `input3_temp', replace 

	import excel using "`input4'", clear firstrow
		gen source = "{USERNAME}" 

		tostring field_citation, replace
	

		tempfile input4_temp
		save `input4_temp', replace

	** append together...
	use `input1_temp', clear 
		append using `input2_temp' 
		append using `input3_temp' 
			tostring prenatal_fd_definition, replace
		append using `input4_temp'

**------------------------------------------------------------------
** drop variables that are no longer useful
	drop row_id sequela_name sequela_id description grouping uploaded field_issue field_secondary_type field_publisher field_keywords field_volume field_authors field_journal field_file_url field_data_url field_location field_publication_status ///
		field_contributor field_pages field_file_name field_accessed_date field_dismod_model field_private field_pub_year field_geography /// 
		recall_type recall_type_value sample_size  design_effect socio_characteristics data_status current_file_name current_file_location project_title project_nid ///
		healthstate sequelae period_unit period_value field_type field_iso3 field_excluded confidence_limit ignore population_characteristics orig_uncertainty_type group

	replace gbd_cause = "cong_heart" if acause=="cong_heart"
		drop acause

** re-name or generate vars that need to be included in 2016 Epi uploader
	replace  extractor = "{USERNAME}" if source=="{USERNAME}"
		replace extractor="{USERNAME}" if source=="{USERNAME}" | source=="{USERNAME}"
		replace extractor ="{USERNAME}" if source=="{USERNAME}" 
			drop source 

	replace parameter = parameter_type if parameter_type != ""
		drop parameter_type 
	replace parameter ="mtwith" if parameter=="with condition mortality" | parameter=="with-condition mortality"
		replace parameter = "mtspecific" if parameter=="cause specific mortality"
			replace parameter="prevalence" if parameter==""
			rename parameter measure

	tostring sex, replace
		replace sex = "Both" if sex=="3"
		replace sex = "Female" if sex=="2"
		replace sex = "Male" if sex=="1"


	tostring urbanicity, replace 
		replace urbanicity = "Unknown" if urbanicity=="1"
		replace urbanicity = "Rural" if urbanicity=="2"
		replace urbanicity = "Urban" if urbanicity=="3"
		replace urbanicity = "Mixed/both" if urbanicity=="4"
			replace urbanicity_type = urbanicity if urbanicity_type==""
				drop urbanicity

	gen representative_name = "Representative for subnational location only" if national_type=="Subnationally representative"
		drop national_type

**---------------------------------------------------------------
** standardizing mean, ci's and sample sizes: 


** vars of interest: lower lower_ci upper upper_ci mean parameter_value num denom prenatal_top prenatal_lb prenatal_fd

	replace mean = parameter_value if mean==.
		drop parameter_value
	rename parameter_units unit_value_as_published

	replace lower = lower_ci if lower==. & lower_ci !=.
		drop lower_ci

	replace upper = upper_ci if upper==. & upper_ci !=.
		drop upper_ci

	// Upload interface will re-calculate prevalence vals & ci's 
		replace mean =. if num !=. & denom !=. & measure=="prevalence"
		replace unit_value=1 if num !=. & denom !=. & measure=="prevalence"

	// need to fill in missing denominator values
		replace denom = 2220000 if denom==. & notes=="2.22 million births"
			// a few that don't have sufficient info available
				count if denom==. & mean==.
				drop if denom==. & mean==.

	**----------------------------------------------
	// mortality data:
		
		** NID 138825, Gilboa - mtspecific: convert;  Gilboa mtwith: should be mtspecific, convert for unit_val
			replace measure="mtspecific" if measure=="mtwith" & regexm(file_name, "Gilboa")
			replace mean = mean/unit_val if measure=="mtspecific" & regexm(file_name, "Gilboa")

		** NID 138890, Nembhard 2009 - mtwith should be mtspecific; convert for unit_val
			replace measure="mtspecific" if measure=="mtspecific" & regexm(file_name, "Nembhard_2009")
			replace mean = mean/unit_val if measure=="mtspecific" & regexm(file_name, "Nembhard_2009")

		** NID 138785, Boneva 2001 - mtwith should be mtspecific; convert for unit_val
			replace measure="mtspecific" if measure=="mtspecific" & regexm(file_name, "Boneva")
			replace mean = mean/unit_val if measure=="mtspecific" & regexm(file_name, "Boneva")
		
		gen new_unit_val = 1 if unit_val >1 & regexm(file_name, "Gilboa") | regexm(file_name, "Nembhard_2008") | regexm(file_name, "Nembhard")

	** convert values using unit_value_as_published:
	foreach v in mean upper lower {
	replace `v' = `v'/1000 if unit_val==1000 & measure==""
		replace `v' = `v'/10000 if unit_val==10000
		replace `v' = `v'/100000 if unit_val==100000

		** address extraction issues
			replace `v' = `v'/1000 if (nid==138830 | nid==138781)
			}
			replace lower = lower/100000 if nid==138960
			replace upper = upper/100000 if nid==138960


	replace new_unit_val = 1 if unit_val >1 
	replace unit_val = new_unit_val if new_unit_val !=.
		drop new_unit_val 

			** when the "numerator" value isn't filled in explicitly -- studies with prenatal diagnosis:
				replace numerator=prenatal_lb if numerator==. & prenatal_lb !=.
				replace numerator=(prenatal_total - prenatal_top) if numerator==. & prenatal_total !=. & prenatal_top !=.

	rename numerator cases 
	rename denominator sample_size

**----------------------------------------------------------------------
	gen uncertainty_type_value = 95

	rename field_citation field_citation_value
	rename file_location file_path 
	rename orig_unit_type unit_type 

		gen underlying_nid=""

	rename notes note_SR 
	gen note_modeler = ""

	** preserve information about studies with known issues 
	gen sex_issue = 0
	gen year_issue = 1 if regexm(issues, "year")
		replace year_issue = 0 if year_issue ==.
	gen age_issue = 0
	gen measure_issue = 1 if regexm(issues, "case_name")
		replace measure_issue = 0 if measure_issue ==.
			drop issues 

	** and preserve information about adjustments made during extraction 
	gen measure_adjustment = 0 if inlist(raw_adjusted, "raw", "raw ")
		replace measure_adjustment = 0 if raw_adjusted== "."
			drop raw_adjusted  

	** encode the "study_type" variable to match current Epi lit template
		replace study_type = data_type if data_type !=""
			drop data_type
	replace source_type = ""
		replace source_type =  "Survey - cohort" if study_type=="case-control"
		replace source_type = "Survey - cohort" if inlist(study_type,"cohort", "retrospective cohort", "retrospective", "Study: cohort")
		replace source_type = "Survey - cross-sectional" if inlist(study_type, "survey", "cross-sectional", "cross-sectional study", "retrospective cross sectional", "Study: cross-sectional")
		replace source_type = "Facility - inpatient" if study_type =="hospital"
		replace source_type = "Registry - congenital" if inlist(study_type, "registry", "surveillance")
		replace source_type = "Survey - cohort" if source_type==""
			drop study_type 

	** year variables
		drop field_time_start field_time_end

	** convert cv_chromo_excl to cv_chromo_incl, so that the reference definition is 0
		replace cv_chromo_incl=1 if cv_chromo_excl==0
		replace cv_chromo_incl=0 if cv_chromo_excl==1
			drop cv_chromo_excl

		** fill in cv_topnotrecordd & cv_livestill
			gen cv_livestill=1 if prenatal_fd !=.
				replace cv_livestill=0 if prenatal_fd ==.
			gen cv_topnotrecorded=1 if prenatal_top !=.
				replace cv_topnotrecorded=0 if prenatal_fd==.
				** assumes studies that did not report terminations and/or stillbirths did not include them in the counts

		gen age_demographer = 0 
		gen recall_type = "Point" 

**------------------------------------------
** location information
	rename site site_memo

	** convert iso3 codes to ihme_loc_id's

		** create an indicator for whether data needs to be re-extracted at the subnational level 
		gen is_subnat = 1 if inlist(iso3, "BRA", "CHN", "IDN", "IND", "JPN")
			replace is_subnat = 1 if inlist(iso3,"KEN", "MEX", "SAU", "SWE", "USA", "ZAF")
				replace is_subnat = 0 if is_subnat==.
		gen reextract_subnat = 1 if is_subnat==1 & subnational_id==.
			replace reextract_subnat = 0 if reextract_subnat ==.

			** data is already extracted at the subnational level for CHN, IND, MEX but not for BRA, IND, JPN, SAU, SWE, or USA
				egen ihme_loc_id = concat(iso3 subnational_id), punct("_")
				replace ihme_loc_id = iso3 if subnational_id==.
				
				** get list of studies to re-extract for subnational locations 
					preserve
						keep if reextract_subnat ==1 | iso3=="IND"
						bysort file_name: gen count = _n

						** create a location-mapping file to get the subnational information mapped 
						keep file_name file_path iso3 site_memo
							duplicates drop 
							gen ihme_loc_id=.
							sort iso3 
							export excel using "`subnational_mapping_file'_`date'.xlsx", replace firstrow(variables) 
					restore			

			** merge in ihme_loc_id's and smaller_site_unit from subnational location mapping file 
				preserve
					import excel using "`subnational_mapping_file'.xlsx", firstrow clear 
						tempfile subnational_map
						save `subnational_map', replace
				restore
 
		merge m:1 file_name file_path iso3 site_memo using `subnational_map', update replace nogen
			replace ihme_loc_id= trim(ihme_loc_id)

				merge m:1 ihme_loc_id using `loc_data', keepusing(location_name location_id) keep(1 3) nogen 
					drop if site_memo == "Guadeloupe" 
					** we don't model Guadeloupe

					drop location_type iso3 subnational_id national gbd_region
						drop reextract_subnat is_subnat 

		** merge in the location_name and location_id variables from ihme_loc_id
			preserve
				get_location_metadata, location_set_id(9) clear
					duplicates drop ihme_loc_id, force // do not map England subnational sites at this time
				tempfile 2015_locs
				save `2015_locs', replace
			restore

			drop location_id location_name
			merge m:1 ihme_loc_id using `2015_locs', keep(3) keepusing(location_name location_id) nogen

**------------------------------------------------------------------------------------------
** merge in the exlusions and additional covariate coding
	drop cv_1facility_only smaller_site_unit // will merge in updated versions of those  
	preserve
		import excel using "{FILEPATH}", firstrow clear 
			drop note 
			tempfile add_cvs 
			save `add_cvs', replace
	restore

		** make some adjustments to the file names so that these will merge
			replace file_name = "Chung_2010_Impact_of_fetal_echocardiography" if file_name == "Chung_2010_Impact_of_fetalechocardiography"
			replace file_name = "Hannoush_2004_Patterns_of_congenital_heart" if file_name =="Hannoush_2004"
			replace file_name = "Koppel_2003_Effectiveness_of_pulse_oximetry" if file_name=="Koppel_2003"
			replace file_name = "Marelli_2010_Sex_Differences" if file_name =="Marelli_2010_Sex_differences"
			replace file_name = "McBride_2005_epidemiology_of_noncomplex" if file_name =="McBride_2005_epidemiology_of_non_complex"
			replace file_name = "OMalley_1996_epidemiologic_characteristics" if file_name =="Omalley_1996_epidemiologic_characteristics"
			replace file_name = "Roos-Hesselink_1995_Atrial" if file_name =="Roos-Hesselink_1995_Atrial_files"
			replace file_name = "Shin_2012_Improved_Survival_Among_Children_With_Spinal_Bifidia" if file_name =="Shin_2012_Improved_Survival_Among_Chidlren_With_Spinal_Bifidia"
			replace file_name = "Welke_2006_Current" if file_name == "Welke_2006_Currect"
			replace file_name = "Wilson_1998_long_term_outcome_after_the_mustard_repair" if file_name =="Wilson_1998_long-term_outcome_after_the_mustard_repair"

		merge m:1 file_name using `add_cvs' , keep(3) nogen 
			replace smaller_site_unit=1 if smaller_site_unit==. // missing for two studies only 

		** replace file locations to reflect new folders with PDFs 
		gen file_path_new = "{FILEPATH}"
			egen file_path_new2 = concat(file_path_new file_name), punct(\)
			replace file_path_new2 = file_path_new2 + ".pdf"
				drop file_path_new
				rename file_path_new2 file_path_new 

		** drop papers with duplicate birth registry data
			drop if is_duplicate_registry==1 & measure=="prevalence" & age_end==0
				drop is_duplicate_registry 

tempfile ready_for_case_mapping
save `ready_for_case_mapping', replace


**-----------------------------------------------------------
** map case_names to modelable entities 
	local current_cause_map "`dir'\{FILEPATH}\all_cong_case_definition_map.xlsx"

foreach cause in cong_chromo cong_downs cong_heart cong_neural {
	preserve 
		import excel using "`current_cause_map'", sheet(`cause') firstrow clear 
			keep gbd_cause case_name case_definition ME_name 
				replace case_name = trim(case_name)
			duplicates drop 

			tempfile `cause'_file
			save ``cause'_file', replace 
		restore
		}

	local cause cong_cleft
	preserve 
		import excel using "`current_cause_map'", sheet(`cause') firstrow clear 
			keep gbd_cause case_name case_definition ME_name lip_only palate_only lip_and_cleft_only is_total
				replace case_name = trim(case_name)
			duplicates drop 

			tempfile `cause'_file
			save ``cause'_file', replace 
	restore


	local cause cong_other
		preserve 
			import excel using "`current_cause_map'", sheet(`cause') firstrow clear 
				keep gbd_cause case_name case_definition ME_name new_gbd_cause
				replace case_name = trim(case_name)
			duplicates drop 

			tempfile `cause'_file
			save ``cause'_file', replace 
		restore


foreach cause in cong_chromo cong_cleft cong_downs cong_heart cong_neural cong_other {
	preserve
		keep if gbd_cause=="`cause'"
		replace case_name = trim(case_name)
		merge m:1 gbd_cause case_name case_definition using ``cause'_file' , keep(3) nogen 

		tempfile `cause'_with_case_names
		save ``cause'_with_case_names', replace 
	restore
	}

** and causes that don't need case mappping 
	foreach cause in cong_klinefelter cong_turner {
		preserve
		keep if gbd_cause=="`cause'"
			gen ME_name = ""

			replace ME_name = "Turner Syndrome" if gbd_cause=="cong_turner"
			replace ME_name = "Klinefelter Syndrome" if gbd_cause=="cong_klinefelter"

		tempfile `cause'_case_names
		save ``cause'_case_names', replace 
		restore 
		}

** adjustment for cong_cleft cases, so we don't collapse the totals in with the subcomponents 
	use `cong_cleft_with_case_names'
			bysort file_name: egen has_total =max(is_total)
			bysort file_name: egen has_lip =max(lip_only)
					**when a paper reports total cleft counts and subcomponents, keep the totals and drop subcomponents:
					gen drop_me=1 if has_total==1 & has_lip==1 & is_total==0

					tempfile cong_cleft_with_case_names2
					save `cong_cleft_with_case_names2', replace 

**-----------------------------------------------------------------------------
** append 
	use `cong_chromo_with_case_names', clear 
		append using `cong_cleft_with_case_names2'
		append using `cong_downs_with_case_names'
		append using `cong_heart_with_case_names'
		append using `cong_neural_with_case_names'
		append using `cong_klinefelter_case_names'
		append using `cong_turner_case_names'
		append using `cong_other_with_case_names'

** adjust and drop
	rename ME_name modelable_entity_name
		replace modelable = trim(modelable)

	replace gbd_cause = new_gbd_cause if gbd_cause=="cong_other"
		replace gbd_cause = trim(gbd_cause)
		rename gbd_cause cause
			drop new_gbd_cause

	** drop observations we don't model as part of congenital
		drop if cause =="exclude"
	** drop observations that fall in the "other" category
		drop if cause =="cong_other"

	** keep the most case_name information possible
		replace case_name = case_definition if case_name=="congenital heart disease (total)" & case_definition !=""

		replace modelable = "Total congenital heart birth defects" if cause=="cong_heart" & modelable=="Total"
		replace modelable = "Total congenital digestive birth defects" if cause=="cong_digest" & modelable=="Total"
		replace modelable = "Total congenital musculoskeletal birth defects" if cause=="cong_msk" & modelable=="Total"
		replace modelable = "Total urogenital birth defects" if cause=="cong_urogenital" & modelable=="Total"
		replace modelable = "Total neural tube defects" if cause=="cong_neural" & modelable=="Total"

**------------------------------------------------------------------------------------------
** Drop duplicate data (same file_name & same data, but different extractors)

		** tag the duplicates 
		preserve
			keep nid file_name file_path title extractor
			duplicates drop
			duplicates tag file_name, gen(is_dup)
			keep if is_dup==1
			tempfile dups 
			save `dups', replace
		restore

		merge m:1 nid file_name file_path title extractor using `dups', keepusing(is_dup) nogen
			drop if is_dup==1 & extractor != "{USERNAME}"
	
	** change NIDS as needed
		replace nid=138921 if nid==138922 & file_name=="Scarlett_2004_Thirty-Five"
		replace nid=138924 if nid==138925 & file_name=="Sever_1988_The_prevalence_at_birth_of_congenital"
		replace nid=138894 if nid==138895 & file_name=="Olsen_2010_Late_mortality_among"

**---------------------------------
tempfile needs_bundles
save `needs_bundles', replace

	use `needs_bundles',  clear 

	** Merge bundle_id's:
		preserve
			import excel using "{FILEPATH}", firstrow clear 
				gen is_birth_prev = 1 if regexm(modelable_entity_name, "irth prevalence")
					replace is_birth_prev =0 if is_birth_prev==.
					keep if modelable_entity_name_old !=""
					replace modelable_entity_name_old =trim(modelable_entity_name_old)

				tempfile bundle_map
				save `bundle_map', replace
			restore

		rename modelable_entity_name modelable_entity_name_old
			replace modelable_entity_name_old =trim(modelable_entity_name_old)
		gen is_birth_prev =1 if age_end==0
			replace is_birth_prev =0 if is_birth_prev==.

	merge m:1 modelable_entity_name_old is_birth_prev using `bundle_map', keep(3) nogen
		** note that this drops data that doesn't fit into any of the ME's, eg. total urogenital birth defects data

	drop is_2016 is_2015 is_birth_prev modelable_entity_name_old

**--------------------------------------------
	tostring table_num page_num, replace 

	tempfile ready_to_collapse
	save `ready_to_collapse', replace


**----------------------------------------------------------------------------------------------------------------
** 1. collapse to modelable entities (prevalence only): 
use `ready_to_collapse', clear

			 preserve
				keep if regexm(modelable, "otal")
				tempfile totals_to_add
				save `totals_to_add', replace 
			restore
			drop if regexm(modelable, "otal")
	
	**not going to collapse the with-condition mortality data
		preserve
			keep if measure !="prevalence"
			tempfile mtwith
			save `mtwith', replace
		restore
	drop if measure !="prevalence"

**----------------------------------
**create concatenated case_name & case_diagnostics variables so that this information is not lost during/after the collapse

 		local collapse_vars nid field_citation_value title file_name file_path cause sampling_type response_rate site_memo year_start year_end sex age_start age_end measure ///
 			unit_value_as_published standard_error sample_size effective_sample_size note_SR specificity cv_prenatal cv_postnatal source_type location_id location_name ///
 				unit_type urbanicity_type extractor cv_chromo_incl representative_name uncertainty_type_value underlying_nid note_modeler sex_issue year_issue age_issue measure_issue measure_adjustment age_demographer ///
 				recall_type ihme_loc_id smaller_site_unit site_memo_new cv_1facility_only cv_low_income_hosp cv_inpatient cv_topnotrecorded cv_livestill file_path_new  modelable_entity_name bundle_id prenatal_fd_def	
 				count


		egen collapse_group = group(`collapse_vars'), missing
		
		sort collapse_group modelable_entity_name case_name case_diag
		// case_name
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// case_diagnostics
		by collapse_group : gen new_case_diagnostics = case_diag[1]
		by collapse_group : replace new_case_diagnostics = new_case_diag[_n-1] + ", " + case_diag if _n > 1
		by collapse_group : replace new_case_diag = new_case_diag[_N]
			replace new_case_diag = subinstr(new_case_diag, ", ,", "", .)
			replace new_case_diag = "" if new_case_diag==", "

		// page_num
		tostring page_num, replace
		by collapse_group : gen new_page_num = page_num[1]
		by collapse_group : replace new_page_num = new_page_num[_n-1] + ", " + page_num if _n > 1
		by collapse_group : replace new_page_num = new_page_num[_N]

		// table_num
		tostring table_num, replace
		by collapse_group : gen new_table_num = table_num[1]
		by collapse_group : replace new_table_num = new_table_num[_n-1] + ", " + table_num if _n > 1
		by collapse_group : replace new_table_num = new_table_num[_N]

 			drop collapse_group

	** drop redundant data for cleft palate
			drop if drop_me==1
			drop lip_only palate_only lip_and_cleft_only is_total has_total has_lip drop_me

	**----------------------------------
	** collapse to modelable entities: 
	count
	gen case_name_count=1
 	
 		collapse (sum) cases mean lower upper prenatal_total prenatal_top prenatal_fd prenatal_lb case_name_count,  ///
 			by(`collapse_vars' new_case_name new_case_diagnostics new_page_num new_table_num)		
 			count

 				rename new_* *

 			tempfile collapsed_to_MEs
 			save `collapsed_to_MEs', replace 

**--------------------------------------------------------------
** 2. collapse to causes, for upload to envelope models

use `ready_to_collapse', clear
	drop if regexm(modelable, "otal")
		drop if measure !="prevalence"

	** Create concatenated case_name variables
		replace case_diagnostics = subinstr(case_diagnostics, `"""', "", . )


 		local collapse_vars nid field_citation_value title file_name file_path  cause sampling_type response_rate site_memo year_start year_end sex age_start age_end measure ///
 			unit_value_as_published standard_error sample_size effective_sample_size note_SR specificity cv_prenatal cv_postnatal prenatal_fd_definition source_type location_id location_name ///
 				unit_type urbanicity_type extractor cv_chromo_incl representative_name uncertainty_type_value underlying_nid note_modeler sex_issue year_issue age_issue measure_issue measure_adjustment age_demographer ///
 				recall_type ihme_loc_id smaller_site_unit site_memo_new cv_1facility_only cv_low_income_hosp cv_inpatient cv_topnotrecorded cv_livestill file_path_new

 	
		egen collapse_group = group(`collapse_vars'), missing
		
		sort collapse_group modelable_entity_name case_name case_diag
		// case_name
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// case_diagnostics
		by collapse_group : gen new_case_diagnostics = case_diag[1]
		by collapse_group : replace new_case_diagnostics = new_case_diag[_n-1] + ", " + case_diag if _n > 1
		by collapse_group : replace new_case_diag = new_case_diag[_N]
			replace new_case_diag = subinstr(new_case_diag, ", ,", "", .)
			replace new_case_diag = "" if new_case_diag==", "

		// page_num
		tostring page_num, replace
		by collapse_group : gen new_page_num = page_num[1]
		by collapse_group : replace new_page_num = new_page_num[_n-1] + ", " + page_num if _n > 1
		by collapse_group : replace new_page_num = new_page_num[_N]

		// table_num
		tostring table_num, replace
		by collapse_group : gen new_table_num = table_num[1]
		by collapse_group : replace new_table_num = new_table_num[_n-1] + ", " + table_num if _n > 1
		by collapse_group : replace new_table_num = new_table_num[_N]

 			drop collapse_group
	
	** collapse to causes
	keep if inlist(cause, "cong_neural", "cong_msk", "cong_digest", "cong_heart")
	count
	gen case_name_count=1
 	
 		collapse (sum) cases mean lower upper prenatal_total prenatal_top prenatal_fd prenatal_lb case_name_count ,  ///
 			by(`collapse_vars' new_case_name new_case_diagnostics new_page_num new_table_num)				
 			count
 
			rename new_* *

 	** fill in modelable entity names
 				gen modelable_entity_name=""
 					replace modelable= "Total congenital digestive anomalies" if cause=="cong_digest"
 					replace modelable= "Total congenital heart defects" if cause=="cong_heart"
 					replace modelable= "Total musculoskeletal congenital anomalies" if cause=="cong_msk"
 					replace modelable= "Total neural tube defects" if cause=="cong_neural"
 						sort modelable
 				gen is_collapsed =1 

	** append on the "total" counts
		append using `totals_to_add'
			replace is_collapsed=0 if is_collapsed==.

	tempfile collapsed_to_envelope_MEs
	save `collapsed_to_envelope_MEs', replace

** append all data together
	append using `collapsed_to_MEs'
		replace is_collapsed=1 if is_collapsed==.
	append using `mtwith'
		replace is_collapsed=0 if is_collapsed==.

		sort modelable bundle
		carryforward bundle_id, replace 

**---------------------
tempfile all_collapsed
save `all_collapsed', replace

**------------------------------------------------------------------------
** code cv_underreport to address data that has insufficient case names included in a given congenital category
use `all_collapsed', clear 

		preserve 
			duplicates drop cause modelable case_name bundle_id, force 
			keep cause modelable case_name bundle measure
			sort cause modelable measure case_name
			order cause model bundle measure case_name
			export excel using "`dir'/{FILEPATH}/2013_lit_cv_subset_coding.xls", firstrow(variables) sheet("{DATE}") sheetreplace
		restore

 	** code in cv_under_report:
		preserve
			import excel using "`dir'/{FILEPATH}/2013_lit_cv_subset_coding.xls", clear sheet("{DATE}") firstrow
			keep if cv_under_report !=""
			duplicates drop
			tempfile cv_under_report
			save `cv_under_report', replace
		restore
			
	merge m:1 case_name bundle cause modelable measure using `cv_under_report', nogen keep(3)

 	**Notes: cv_underreport is coded as 0 if the case names are reasonably complete for a given ME; 
				** coded as 1 if the case names are not complete for a given ME but still complete enough to use and cross-walk to the more complete data points
				** "drop" if the case names are so limited for a given ME that the data should not be used; mostly used for the total-cause MEs where case names are specific to a sub-cause 

		/* // this code block produces a dataframe that allows us to determine if more case names should be added to the cv_underreport mapping file: 
			preserve
			keep if _m==1
			keep cause modelable case_name bundle measure 
				duplicates drop 
				sort cause modelable measure case_name
				order cause model bundle measure case_name
					br 
			restore */	

**-------------------------------
** calculate prevalence 

	replace mean = cases/sample_size if measure=="prevalence" & cases !=. & sample_size !=.
		replace lower= . if (lower==0 & upper==0 & mean !=0)	
		replace upper =. if (lower==. & upper==0 & mean !=0)

		** rounding issue in input data from Gilboa mortality paper (NID 138825): mean -> 0, so cannot back-calculate sample size
		drop if (mean==0 & cases !=0 & sample_size==.)

//---------------------------------------------------------------------------------------------
// Final prep before upload:

		** covariates
			rename cv_chromo_incl cv_includes_chromos
				gen cv_excludes_chromos = 1 if cv_includes_chromos==0
				replace cv_excludes_chromos =0 if cv_includes_chromos==1
			drop cv_postnatal


	** group, group_review, specificity			
		** need to group_review out the mortality-at-birth data that is reporting stillbirths; age_start=0 and age_end=0
		gen group_review =.
		gen group=.
			replace group_review =0 if measure !="prevalence" & age_start==0 & age_end==0
			replace group = 1 if measure !="prevalence" & age_start==0 & age_end==0
			replace specificity = specificity + "; reporting stillbirths only" if group_review==0
				replace group =1 if group==. & specificity !=""
				replace group_review = 1 if group_review==. & specificity !=""


		// additional variables
			replace uncertainty_type_value =. if upper==. & lower==.
			gen uncertainty_type =. 

			replace sampling_type = "" if sampling_type== "."
			replace urbanicity_type = "Mixed/both" if urbanicity_type=="Representative"
	
			replace representative_name = "Unknown" if representative_name ==""
			replace unit_type = "Person" if measure=="prevalence"
			replace unit_type = "Person*year" if measure !="prevalence"

			replace case_diagnostics = substr(case_diagnostics, 1, 1998) // the max character limit for Epi uploader is 2000 characters 

			gen outlier_type_id=0
			gen design_effect=.
			gen recall_type_value=.
			gen input_type=.
			gen seq =.
			drop file_path
				rename file_path_new file_path
			replace extractor = extractor + " + {USERNAME}"
			egen site = concat(site_memo site_memo_new), punct("; ") 
				replace site = site_memo if site_memo== site_memo_new
				drop site_memo site_memo_new
				rename site site_memo 


//------------------------------
// order variables to match the 2016 extraction template

tempfile done 
save `done', replace 

order bundle_id modelable_entity_name seq underlying_nid nid field_citation_value file_path page_num table_num source_type location_name location_id ihme_loc_id smaller_site_unit site_memo ///
		sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size unit_type unit_value_as_published ///
		measure_issue measure_adjustment uncertainty_type_value uncertainty_type representative_name urbanicity_type recall_type recall_type_value sampling_type design_effect input_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor ///
			cv* prenatal_top prenatal_fd prenatal_fd_definition outlier_type_id 

	// drop variables that are not part of the standard Epi lit template
			drop title file_name is_dup prenatal_total prenatal_lb case_name_count lip_only palate_only lip_and_cleft_only is_total has_total has_lip drop_me is_collapsed is_dental

			
//--------------------------------------------------
// format "cause" var for folder structure
		replace cause = cause + "s" if cause=="cong_down"
		replace cause = cause + "ive" if cause=="cong_digest"


// export all, including insufficient case names
	export excel using "`dir'\{FILEPATH}\2013_lit_data_all_bundles_combined_`date'.xls", firstrow(variables) replace
		drop if cv_under=="drop" // where the collection of case_names is not sufficient for the entire category; eg. "Dandywark snydrome" for other chromosomal anomalies


// split into bundles and save files for upload 
	 levelsof bundle_id, local(bundles)
		foreach b in `bundles' {
			preserve
			keep if bundle_id==`b'
			local c = cause

				local file  "{FILEPATH}"
				export excel using "`file'.xls", firstrow(variables) sheet(extraction) replace 

				levelsof modelable_entity_name, local(me) clean
				di in red "Exported `me' data for upload"
			restore
		}
