// WORLD ATLAS OF BIRTH DEFECTS: DATA PREPARATION / STANDARDIZATION
// Purpose: compiling and standardizing previously extracted World Atlas of Birth Defects data, for GBD 2016

// {AUTHOR NAME}
// January 2016
//--------------------------------------------------

/* Outline:
	1. load and compile together csv's
	2. format vars and add variables required for upload to central database
	3. map case definitions to GBD causes
	4. map locations to GBD locations
	4.2. add NIDs and citation info
	4.3. adjust case counts and sample size for missing years of registry information 
	5. add cleft sex splits

	6. collapse to the modelable entity level
	7. collapse to the cause level for causes with envelope models

	8. code cv_subset for insufficient case names and drop
	9. standardize variable names to match database upload template

	- export to bundles
	*/
//-------------------------------------------------

// setup
clear all 
set more off
set maxvar 30000
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

global in_dir "$dl/{FILEPATH}"
local out_dir "$dl/{FILEPATH}"

//---------------------------------------------------
// 1. load and compile csv's -- adapted from GBD 2013 code

gen foo = .
	tempfile master
	save `master', replace
	clear
	
	local cong: dir"$in_dir/" files "GBD2013*", respect
	foreach file of local cong {
		di in red "importing `file'"
		import excel using "$in_dir/`file'", firstrow clear
		foreach var of varlist * {
			tostring `var', force replace
		}	
		gen orig_file_name = "`file'"
		append using `master'
		tempfile master
		save `master'
	}
	
// Clean up
	drop foo
	// Drop unecessary/ blank rows
		drop if GBD == ""
	// Fix cause names so they will merge with the map
		rename GBD_cause cause_name
		replace cause_name = lower(cause_name)
		replace cause_name = subinstr(cause_name, "_", " ", .)
		replace cause_name = "anophthalmos" if cause_name == "."
	// destring numerical vars
		destring year* nid age* sex, replace
		
// NID info
	replace nid = 127534 // World Atlas of Birth Defects NID

// drop unnecessary vars
	drop grouping sequelae healthstate case_def case_diag study_type sampling_type cv_ raw_adjusted ignore urbanicity AO BK BJ period_unit period_value confidence_limit field_citation 

// save 
	tempfile world_atlas
	save `world_atlas', replace

//-----------------------------------------------------------
// 2. format vars to the current database template and clean "add" variables
use `world_atlas', clear 

	rename notes note_SR 

	replace year_start = 1900 + year_start
	replace note_SR = note_SR + "; registry did not report all years of data" if regexm(year_end, "no")
		replace year_end = substr(year_end, 1, 2)
		destring year_end, replace 
		replace year_end = 1900 + year_end

	rename parameter measure
		drop parameter_value

	drop orig_file_name file_location
	replace file_name = "WORLD_ATLAS_OF_BIRTH_DEFECTS_2003_Y2014M01D03"


	replace note_SR = note_SR + ", " if note_SR != ""
		replace note_SR = note_SR + "structural anomalies diagnosed until first birthday, chromosomal at any time" 
		replace age_end = "0" // note that diagnoses are made until age 1 for some structural anomalies 
		destring age_end, replace 

		destring denom, replace
		destring num, replace
		drop upper_CI lower_CI parameter_units standard_error effective_sample_size
		replace measure="prevalence"

		replace group =""
		replace specificity=""

		// standardize var names
			rename numerator cases 
			rename denominator sample_size


	// covariates and information in "add" variables 
			drop add1diag add2site add3denom add4 add6 add7 add8 add9 add10 add11 add12 add13 add14 add15 add16 add17 // do not need the information in these variables
			
			// information about age of prenatal diagnosis
			replace add5ageatdiagnosis = add5ageatdianosis if add5ageatdiag =="" & add5ageatdian !=""
				drop add5ageatdian
				replace add5 = add5 + " weeks gestation" if inlist(add5, "16", "20", "22", "24", "28")
				replace add5 = add5 + " grams" if inlist(add5, "500", "1000")
				replace add5 = "" if case_name=="Down Syndrome"
					

			// the "add15" variable information entered for Down syndrome is registry coverage information, not age at diagnosis
			gen coverage = add5 if case_name=="Down Syndrome"
				replace coverage = coverage + " % population coverage" if cov !=""
				replace note_SR = coverage + ", " + note_SR if coverage != ""
					drop coverage 
					// the response_rate variable was the "coverage" information
					drop response_rate 


			// covariate information
			rename add5 prenatal_fd_definition
			gen cv_prenatal =1
			gen cv_low_income_hosp =1
			gen cv_1facility_only=0
			gen cv_aftersurgery =0

//------------------------------------------------------------
// 3. map to GBD causes / modelable entities / bundles 

	preserve
	import excel using "{FILEPATH}\world_atlas_case_name_mapping.xlsx", firstrow clear 
		tempfile cause_info
		save `cause_info', replace
	restore

	merge m:1 cause_name using `cause_info', nogen
		drop if modelable_entity_name=="drop"
		drop cause_name

//----------------------------------------------------------
// 4. map to GBD locations

	// export information to create a mapping file:
		preserve
			keep site iso3 national subnational_id 
			duplicates drop
			sort site
			br 
		export excel using "`out_dir'/world_atlas_location_mapping", firstrow(variables) sheet("raw") sheetreplace 
		restore

	// merge in location_name and smaller_site_unit from mapping file
		preserve
			import excel using "`out_dir'\world_atlas_location_mapping.xls", firstrow clear
			tempfile loc_map 
			save `loc_map', replace 
		restore 
		merge m:1 iso3 subnational_id site national using `loc_map', nogen

		drop site gbd_region iso3 national subnational_id 
		rename site_new site_memo
		replace site_memo = location_name if site_memo==""

	// merge in location_id's 
		preserve
	 		run "{FILEPATH}\get_location_metadata.ado"
			get_location_metadata, location_set_id(9) clear
				drop if location_name=="Georgia" & location_type=="admin0"
				duplicates drop location_name, force // necessary in order to merge by location_name
			tempfile loc_metadata 
			save `loc_metadata', replace 
		restore 
		merge m:1 location_name using `loc_metadata', keepusing(location_id) nogen keep(1 3)

		gen representative_type = "Nationally representative only" if smaller_site_unit==0
		replace representative_type = "Representative for subnational location only" if smaller_site_unit==1

//------------------------------------
// & merge in underlying NIDs
		preserve 
			import excel using "`out_dir'/world_atlas_NIDs_from_GHDx.xlsx", firstrow clear 
			tempfile nid_info
			save `nid_info', replace 
		restore

		merge m:1 site_memo location_name using `nid_info', nogen // all merge 
			drop geography

tempfile with_loc_info
save `with_loc_info', replace 

//-------------------------------------------------------------------------------------------------------------------------------------------------------------------
// 5.2: Adjust sample sizes and case counts for case names that are missing years of reporting, so that the collapses will work properly
			// eg. Tetralogy of Fallot has only cases for 1998, but want to collapse this with transposition of great vessels, which has years 1996-1998.
			// making these adjustments at the cause level.
	use `with_loc_info', clear 

			egen g = group(site_memo location_name file_name smaller_site_unit age_start age_end cause sex group), missing

			bysort g: egen sample_max = max(sample_size)
				gen correction_factor = sample_max/sample_size
				gen cases_fix = cases*correction_factor 
					replace cases = cases_fix if sample_max != sample_size
					replace sample_size = sample_max if sample_max != sample_size
				
			bysort g: egen year_start_new = min(year_start)
			bysort g: egen year_end_new = max(year_end)
				replace year_start = year_start_new if year_start != year_start_new
				replace year_end = year_end_new if year_end != year_end_new

					replace note_SR = note_SR + "; cases and sample sizes adjusted for missing registry-years" if correct !=1
						drop *_fix *_new correction_factor g

//--------------------------------------
// duplicate rows for UK/England subnational registries:
	// assuming that the county memberships of these registries are the same as what is reported on the EUROCAT website, 
	// use the same process of mapping to the UTLA (upper tier local authority) level as was used in EUROCAT prevalence table prep code 
		
			// first rename registry names to match the registry names used in the EUROCAT prev tables:
				gen registry_name =""
					replace registry_name="N W Thames - UK" if site_memo=="England North Thames West"
					replace registry_name="Merseyside & Cheshire - UK" if site_memo=="England Mersey"

		// UK population weights and indicators for which UTLAs are included in each registry
			preserve
       		import excel using "{FILEPATH}\uk_population_weights_for_EUROCAT_registries_2016_12_23.xlsx", firstrow clear 
       			tostring population, replace 
       			tempfile uk_pops
       			save `uk_pops', replace
       		restore

       	//------------------------------
       	// expand rows for "Merseyside & Cheshire - UK":
       			preserve
       			use `uk_pops', clear 
       				keep if Merseyside_Cheshire== 1
       					keep location_id location_name population Merseyside_Cheshire 
       					egen total = total(population)
       					gen weight = population/total
       					gen registry_name = "Merseyside & Cheshire - UK"
       				gen count = _n
       					local n = _N
       				tempfile uk1
       				save `uk1', replace 
       			restore

       	expand `n' if registry_name== "Merseyside & Cheshire - UK"
			bysort registry_name year_start sample_size site_memo location_name location_id case_name ///
				: gen count = _n
       	merge m:1 registry_name count using `uk1' , keepusing(weight location_name location_id) nogen update replace

				// want to have duplicated rows for each region 
				foreach var in cases sample_size {
       				replace `var' = `var' * weight if registry_name=="Merseyside & Cheshire - UK"
       						}
							replace note_SR = "Split data from the Merseyside & Cheshire registry to underlying administrative areas using population weights; " + note_SR if site_memo=="England Mersey"
       							drop weight count
						

       	//-------------
       	// expand rows for "N W Thames - UK"
       		// North West London, Hertfordshire (East of England region) and Bedfordshire
       		preserve
       			use `uk_pops', clear 
       				keep if NW_Thames==1
       				keep location_id location_name population NW_Thames
       					egen total = total(population)
       					gen weight = population/total
       					gen registry_name = "N W Thames - UK"
       				gen count = _n
       					local n = _N
       				tempfile uk3
       				save `uk3', replace 
       			restore

		expand `n' if registry_name== "N W Thames - UK"
			bysort registry_name year_start sample_size site_memo location_name location_id case_name ///
				: gen count = _n
       	merge m:1 registry_name count using `uk3' , keepusing(weight location_name location_id) nogen update replace 

       					// want to have duplicated rows for each region
       					foreach var in cases sample_size {
       					replace `var' = `var' * weight if registry_name=="N W Thames - UK"
       						}
       					replace note_SR = "Split data from the N W Thames registry to underlying administrative areas using population weights; " + note_SR if site_memo=="England North Thames West"
       						drop weight count 
       						

       	//-----------------
		// two copies of the "England & Wales" data, one assigned to England and the other to Wales 
			expand 2 if site_memo == "England & Wales", gen(new)
			   replace location_name = "England" if site_memo == "England & Wales" & new==0
        			replace location_id = 4749 if site_memo == "England & Wales" & new==0
     			replace location_name = "Wales" if site_memo == "England & Wales" & new==1
        			replace location_id = 4636 if site_memo =="England & Wales" & new==1
        			drop new 
 
  		replace note_SR = "Data from the 'England & Wales' registry duplicated; one copy assigned to each of England and Wales;" + note_SR if site_memo=="England & Wales"
  			replace note_SR = subinstr(note_SR, "Whales", "Wales", .)
				drop registry_name

tempfile with_uk_splits
save `with_uk_splits', replace

//--------------------------------------------------
// 5. add sex splits for cleft lip +/- palate
	// Sex split = 64% of those with cleft lip (with or without cleft palate)  ///
	// and 43.5% of those with isolated cleft palate are male (Reference NIDs 310647, 310645, and 310649). 

	use `with_uk_splits', clear
	gen group_review=.
	tostring sex, replace
		replace sex = "Both" if sex=="3"
		drop if sex=="."

	preserve
		keep if cause=="cleft"
			expand 2, gen(new)
				replace sample_size = sample_size/2 // assumes 50/50 sex split among births in the population
				
				
				replace sex = "Male" if new==0
					replace cases = cases * 0.64 if new==0 & case_name == "Cleft lip with or without cleft palate"
					replace cases = cases * 0.435 if new==0 & case_name == "Cleft palate without cleft lip"

				replace sex = "Female" if new ==1
					replace cases = (cases * (1 - 0.64)) if new==1 & case_name == "Cleft lip with or without cleft palate"
					replace cases = (cases * (1 - 0.435)) if new==1 & case_name == "Cleft palate without cleft lip"
	
			replace group = "1"
			replace specificity="sex split"
			replace group_review =1

			tempfile cleft_sex_split
			save `cleft_sex_split', replace
		restore

		append using `cleft_sex_split'

			replace specificity = "both sexes" if cause=="cleft" & sex=="Both"
			replace group_review =0 if cause=="cleft" & sex=="Both"
			replace group = "2" if cause=="cleft" & sex=="Both"
				replace group ="" if cause != "cleft"
				replace specificity ="" if cause !="cleft"
				drop new 

tempfile with_splits
save `with_splits', replace 

//---------------------------------------------------------------
// 6. Collapse to the ME level
use `with_splits', clear
	gen count =1

		// create concatenated case_name & note_SR variables
			replace case_name = subinstr(case_name, `"""', "", . )
			replace note_SR = subinstr(note_SR, `"""', "", . )
			
		egen collapse_group = group(site_memo file_name field_citation_value nid ///
			underlying_nid smaller_site_unit sample_size age_start age_end year_start year_end measure ///
			location_name location_id modelable_entity_name bundle_id cause sex group specificity group_review representative_type), missing

	sort collapse_group modelable_entity_name case_name
		// case_name
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]

		// note_SR
		by collapse_group : gen new_note_SR = note_SR[1]
		by collapse_group : replace new_note_SR = new_note_SR[_n-1] + ", " + note_SR if _n > 1
		by collapse_group : replace new_note_SR = new_note_SR[_N]

		// table_num
		by collapse_group : gen new_table_num = table_num[1]
		by collapse_group : replace new_table_num = new_table_num[_n-1] + ", " + table_num if _n > 1
		by collapse_group : replace new_table_num = new_table_num[_N]

	// collapse
	collapse (sum) cases count (mean) cv_*, by(site_memo file_name field_citation_value nid underlying_nid smaller_site_unit sample_size age_start age_end year_start year_end measure ///
			location_name location_id modelable_entity_name bundle_id cause sex group specificity group_review representative_type new_case_name new_note_SR new_table_num prenatal_fd_definition) 

			rename new_* *


tempfile collapsed1
save `collapsed1', replace


//-----------------------------------------------------------------
// 7. Collapse to the cause level, for causes with envelope models 

use `with_splits', clear 
	keep if inlist(cause, "neural", "msk", "digest", "heart") 
	gen count =1

 	egen collapse_group = group(site_memo file_name field_citation_value nid ///
			underlying_nid smaller_site_unit sample_size age_start age_end year_start year_end measure ///
			location_name location_id cause sex group specificity group_review representative_type), missing

		sort collapse_group modelable_entity_name case_name 
		// case_name
		by collapse_group : gen new_case_name = case_name[1]
		by collapse_group : replace new_case_name = new_case_name[_n-1] + ", " + case_name if _n > 1
		by collapse_group : replace new_case_name = new_case_name[_N]
	
		// note_SR
		by collapse_group : gen new_note_SR = note_SR[1]
		by collapse_group : replace new_note_SR = new_note_SR[_n-1] + ", " + note_SR if _n > 1
		by collapse_group : replace new_note_SR = new_note_SR[_N]

		// table_num
		by collapse_group : gen new_table_num = table_num[1]
		by collapse_group : replace new_table_num = new_table_num[_n-1] + ", " + table_num if _n > 1
		by collapse_group : replace new_table_num = new_table_num[_N]

		// collapse 
		collapse (sum) cases count (mean) cv_*, by(site_memo file_name field_citation_value nid underlying_nid smaller_site_unit sample_size age_start age_end year_start year_end measure ///
			location_name location_id cause sex group specificity group_review representative_type new_case_name new_note_SR new_table_num prenatal_fd_definition) 

			rename new_* *

	// fill in ME and bundle information
	gen modelable_entity_name=""
		replace modelable= "Birth prevalence of total congenital heart defects" if cause=="heart"
		replace modelable= "Birth prevalence of total neural tube defects" if cause=="neural"
		replace modelable= "Total musculoskeletal congenital anomalies" if cause=="msk"
		replace modelable= "Birth prevalence of total congenital digestive anomalies" if cause=="digest"

	gen bundle_id=.
		replace bundle_id = 621 if cause=="digest"
		replace bundle_id = 603 if cause=="msk"
		replace bundle_id = 629 if cause=="heart"
		replace bundle_id = 609 if cause=="neural"

tempfile collapsed2
save `collapsed2', replace

//------------------------------------
     append using `collapsed1'
//-----------------------------------------------------------------------------------------
// 8. Code cv_subset / cv_under_report for case_name combinations that do not meet criteria
	
	// Export information for map creation
	preserve
		gen count2 =1
		collapse (sum) count2, by(case_name cause modelable count)
		order cause modelable case_name count
		sort cause modelable count
		gen cv_subset =""

		export excel using "`out_dir'\cv_subset_mapping.xls", sheetreplace firstrow(variables)
	restore

	// use map to mark or drop data points with insufficient case_name combinations
	preserve
		import excel using "`out_dir'\cv_subset_mapping_CURRENT.xls", firstrow clear 			
		keep if cv_subset !=.
		tempfile cv_subset
		save `cv_subset', replace
	restore 
		merge m:1 case_name modelable_entity_name cause count using `cv_subset', nogen

		// drop cv_subset=1 data points
		drop if cv_subset==1
		rename cv_subset cv_under_report 

	tempfile almost_ready
	save `almost_ready', replace 

//----------------------------------------------------------------------------------------------------------------
// 9. Fill in other variables to standardize to the database upload template
	use `almost_ready', replace

	gen mean = cases/sample_size

	gen seq =.
	gen seq_parent =.
	gen file_path = "{FILEPATH}"
	gen page_num =.

	gen lower =.
	gen upper =.
	gen standard_error=.
	gen effective_sample_size=.

	gen unit_type = "Person"
		gen uncertainty_type =.
		gen uncertainty_type_value =.
		gen unit_value_as_published =1
		gen outlier_type_id =0

	rename representative_type representative_name
	gen urbanicity_type = "Mixed/both"
		gen recall_type = "Point"
		gen recall_type_value =.
		gen sampling_type =.
		gen response_rate =.
		gen case_diagnostics = "Cases defined according to ICBDSR case definitions; see note_SR section"
		gen case_definition = case_name
		gen note_modeler =.
		gen input_type = "extracted"
		gen design_effect =.

	gen source_type = "Registry - congenital"
	gen sex_issue =0
		gen year_issue =0
		gen age_issue = 0
		gen age_demographer =0
		gen measure_issue =0
		gen measure_adjustment =0

		rename field_citation underlying_field_citation_value
		gen field_citation_value = "European Surveillance of Congenital Anomalies (EUROCAT), International Centre on Birth Defects, World Health Organization (WHO). World Atlas of Birth Defects. 2nd ed. Geneva, Switzerland: World Health Organization (WHO), 2003."

		// data available are counts of live and still births; does not include terminations 
		gen prenatal_fd =.
		gen prenatal_top =.
		gen cv_topnotrecorded =0
		gen cv_livestill =1

		// assuming that this includes chromosomal cases, because data source does not indicate otherwise
		gen cv_includes_chromos =1 if !inlist(cause, "chromo", "cleft")

	gen extractor = "{USERNAME}"

	order bundle_id modelable_entity_name seq seq_parent underlying_nid nid field_citation_value file_path page_num table_num source_type location_name location_id smaller_site_unit site_memo ///
		sex sex_issue year_start year_end year_issue age_start age_end age_issue age_demographer measure mean lower upper standard_error effective_sample_size cases sample_size unit_type unit_value_as_published ///
		measure_issue measure_adjustment uncertainty_type uncertainty_type_value representative_name urbanicity_type recall_type recall_type_value sampling_type response_rate case_name case_definition case_diagnostics group specificity group_review note_modeler note_SR extractor ///
		 input_type underlying_field_citation_value design_effect cv* ///
		 prenatal_fd prenatal_top prenatal_fd_definition


//------------------------------------------------------
// 10. Export to bundle-specific folders for upload 

	// format "cause" var for folder structure
		replace cause = "cong_" + cause
		replace cause = cause + "s" if cause=="cong_down"
		replace cause = cause + "ive" if cause=="cong_digest"

	local date {DATE} 

	levelsof bundle_id, local(bundles)
		foreach b in `bundles' {
			preserve
			keep if bundle_id==`b'
			local c = cause

	local file  "{FILEPATH}/WorldAtlas_`c'_`b'_prepped_`date'"
				export excel using "`file'.xls", firstrow(variables) sheet(extraction) replace 

				local me = modelable_entity_name
				di in red "Exported `me' data for upload"
			restore
			}
