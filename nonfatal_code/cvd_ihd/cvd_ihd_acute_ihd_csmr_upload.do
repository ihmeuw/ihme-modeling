// Preps custom IHD CSMR for upload to epi database

// Prep stata
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}

	adopath + FILEPATH
  
// LOAD LOCATION_IDS
	get_demographics, gbd_team(epi)
	local location_ids = r(location_ids)

// Directories	
local tmp_dir "FILEPATH"

// Logs 
cap log close
cap log using "FILEPATH/prep_upload.smcl", replace
	
// Append data
tempfile mi_prep
save `mi_prep', replace emptyok

foreach location of local location_ids {
	capture use "`tmp_dir'/csmr_mi_`location'.dta", clear
			if _rc {
			}
			else {
				append using `mi_prep', force
				save `mi_prep', replace
				}
			}
	
use `mi_prep', clear

// Format for upload
	gen seq = ""
	gen bundle_id = 114
	gen bundle_name = "Myocardial infarction due to ischemic heart disease"
	//gen nid = 239850
	gen field_citation_value = "Institute for Health Metrics and Evaluation (IHME). IHME DisMod Output as Input Data 2015 (AMI CSMR)"
	gen source_type = "Mixed or estimation"
	gen smaller_site_unit = 0

	gen age_demographer = 1
	gen age_issue = 0
	gen age_start = 30 if age_group_id==11
	replace age_start=35 if age_group_id==12
	replace age_start=40 if age_group_id==13
	replace age_start=45 if age_group_id==14
	replace age_start=50 if age_group_id==15
	replace age_start=55 if age_group_id==16
	replace age_start=60 if age_group_id==17
	replace age_start=65 if age_group_id==18
	replace age_start=70 if age_group_id==19
	replace age_start=75 if age_group_id==20
	replace age_start=80 if age_group_id==30
	replace age_start=85 if age_group_id==31
	replace age_start=90 if age_group_id==32
	replace age_start=95 if age_group_id==235
	gen age_end = age_start + 4
	
	gen sex = "Male" if sex_id==1
	replace sex="Female" if sex_id==2
	gen sex_issue = 0

	gen year_start = year_id
	gen year_end = year_id
	gen year_issue = 0

	gen unit_type = "Person"
	gen unit_value_as_published = 1
	gen measure_adjustment = 0
	gen measure_issue = 0
	gen measure = "mtspecific"
	gen case_definition = "MI/IHD*IHD deaths"
	gen note_modeler = "custom AMI csmr"
	gen extractor = "USERNAME"
	gen is_outlier = 0

	gen underlying_nid = ""
	gen sampling_type = ""
	gen representative_name = "Unknown"
	gen urbanicity_type = "Unknown"
	gen recall_type = "Not Set"
	gen uncertainty_type = "Sample size"
	gen input_type = ""
	gen upper = ""
	gen lower = ""
	gen standard_error = ""
	gen effective_sample_size = ""
	gen design_effect = ""
	gen site_memo = ""
	gen case_name = ""
	gen case_diagnostics = ""
	gen response_rate = ""
	gen note_SR = ""
	gen uncertainty_type_value = ""
	gen seq_parent = ""
	gen recall_type_value = ""
	gen cases = ""

	drop cause_id measure_id model_version_id age_group_id year_id sex_id population // drop unneeded variables

save "`tmp_dir'/ami_csmr_forupload.dta", replace

drop if sample_size==0
export excel using "FILEPATH/ami_csmr_for_upload_19Jun2017.xlsx", replace sheet("extraction") firstrow(variables)

upload_epi_data, bundle_id(114) filepath("FILEPATH/ami_csmr_for_upload_19Jun2017.xlsx") clear

log close
