
** **************************************************************************
** CONFIGURATION
** **************************************************************************
	** ****************************************************************
	** Prepare STATA for use
	**
	** This section sets the application preferences.  The local applications
	**	preferences include memory allocation, variables limits, color scheme,
	**	defining the J drive (data), and setting a local for the date.
	**
	** ****************************************************************
		// Set application preferences
			// Clear memory and set memory and variable limits
				clear all
				set mem 10G
				set maxvar 32000

			// Set to run all selected code without pausing
				set more off

			// Set graph output color scheme
				set scheme s1color

			// Define J drive (data) for cluster (UNIX) and Windows (Windows)
				if c(os) == "Unix" {
					global prefix "DRIVE"
					set odbcmgr unixodbc
				}
				else if c(os) == "Windows" {
					global prefix "DRIVE"
				}

			// Get date
				local today = date(c(current_date), "DMY")
				local year = year(`today')
				local month = string(month(`today'),"%02.0f")
				local day = string(day(`today'),"%02.0f")
				local today = "`year'_`month'_`day'"


		** ****************************************************************
		** SET LOCALS
		**
		** Set data_name local and create associated folder structure for
		**	formatting prep.
		**
		** ****************************************************************
			// Data Source Name
				local data_name "MEX_SINAIS"
			// Original data folder
				local input_folder "$prefix{FILEPATH}/"
			// Log folder
				local log_folder "$prefix{FILEPATH}/"
				capture mkdir "`log_folder'"
			// Output folder
				local output_folder "$prefix{FILEPATH}/"
				local archive_folder "`output_folder'/"
				local wide_format_folder "{FILEPATH}/"
				capture mkdir "`output_folder'"
				capture mkdir "`archive_folder'"


		** ****************************************************************
		** CREATE LOG
		** ****************************************************************
			capture log close
			log using "`log_folder'/00_format_`today'.log", replace

		** ****************************************************************
		** GET GBD RESOURCES
		** ****************************************************************
			// Get Mexico location ids
				insheet using "$prefix{FILEPATH}/mex_hospital_data_location_key.csv", comma clear
				rename idedo cedocve
				drop if location_id == .
				/*
				odbc load, exec("QUERY") clear
				*/
				tempfile mexico_locations
				save `mexico_locations', replace
			
			// Get Mexico subnational pop data
				import excel using "$prefix{FILEPATH}/MEX_MOH_POP_BY_STATE_AGE_SEX_1990_2012_Y2014M05D12_no_accents.xlsx", clear firstrow
				rename Ao year
				destring(year), replace
				rename Entidad subdiv
				drop Maleall Femaleall
				reshape long M F, i(year subdiv) j(ageX)
				expand 2
				bysort year subdiv ageX : gen sex = _n
				gen pop = .
				replace pop = M if sex == 1
				replace pop = F if sex == 2
				tostring ageX, replace
				gen age = substr(ageX,1,2)
				destring age, replace
				replace age = 1 if age == 14
				replace age = 5 if age == 59
				replace age = 80 if age > 80 & age < 100
				collapse (sum) pop, by(subdiv year sex age) fast
				replace subdiv = "Coahuila" if subdiv == "Coahuila de Zaragoza"
				replace subdiv = "Queretaro" if subdiv == "Queretaro Arteaga"
				rename subdiv location_name
				drop if location_name == "Total"
				merge m:1 location_name using `mexico_locations', keep(1 3) keepusing(location_id) nogen
				rename location_name subdiv
				// Save
				tempfile MEX_subnational_pop
				save `MEX_subnational_pop', replace
			
** **************************************************************************
** RUN PROGRAGM
** **************************************************************************
	// GET DATA
		// Save individual data sets into temporary files
			foreach n of numlist 2000 2001 2002 2004 {
				display in red "Working on `n'"
				use cedocve cveedad edad sexo motegre afecprin ingre causaext nid egreso ingre using "`input_folder'{FILEPATH}/EGRESOS`n'.DTA", clear
				gen year=`n'
				capture tostring ingre, replace
				capture tostring egreso, replace
				foreach var of varlist * {
					local new_name = lower("`var'")
					capture rename `var' `new_name'
				}
				tempfile mex_`n'
				save "`mex_`n''", replace
			}
			foreach n of numlist 2003 2005 2006 2007 2008 2009 {
				display in red "Working on `n'"
				use cedocve cveedad edad sexo motegre afecprin ingre AFEC* causaext nid egreso using "`input_folder'{FILEPATH}/EGRESOS`n'.DTA", clear
				gen year=`n'
				capture tostring ingre, replace
				capture tostring egreso, replace
				foreach var of varlist * {
					local new_name = lower("`var'")
					capture rename `var' `new_name'
				}
				tempfile mex_`n'
				save "`mex_`n''", replace
			}
			foreach n of numlist 2010 2011 2012 {
				display in red "Working on `n'"
				// Get list of diagnosis codes
					// Get data
						use "`input_folder'{FILEPATH}/AFECCIONES.DTA", clear
						foreach var of varlist * {
							local new_name = lower("`var'")
							capture rename `var' `new_name'
						}
					// Reformat
						rename afec AFEC0
						if `n' == 2012 {
							drop numafec
							egen numafec = seq(), by(id clues folio)
						}
						reshape wide AFEC0, i(id clues folio egreso) j(numafec)
					// Save
						compress
						tempfile diag_codes_`n'
						save `diag_codes_`n'', replace
						n di "`n'"
				use cedocve id clues folio egreso cveedad edad sexo motegre afecprin ingre causaext using "`input_folder'{FILEPATH}/EGRESOS`n'.DTA", clear
				merge 1:m id clues folio egreso using `diag_codes_`n''
				gen year=`n'
				capture tostring ingre, replace
				capture tostring egreso, replace
				capture tostring cedocve, replace
				capture tostring cveedad, replace
				capture tostring sexo, replace
				capture tostring motegre, replace

				foreach var of varlist * {
					local nn = lower("`var'")
					rename `var' `nn'
				}
				tempfile mex_`n'
				save "`mex_`n''", replace
			}
		// Append datasets together
			clear
			forvalues year = 2000(1)2012 {
				display in red "Appending on data from `year'"
				append using "`mex_`year''"
			}
			
			tempfile mex_00_11
			save `mex_00_11', replace

	// process data year by year instead of in gigantic DF
	forvalues year = 2000(1)2012 {
		display in red "Processing data from `year'"
		use `mex_00_11', clear
		keep if year == `year'

		// ENSURE ALL VARIABLES ARE PRESENT
			// source (string): source name
				gen source = "`data_name'"
			// NID (numeric)
				gen NID = .
				replace NID = 86949 if year == 2000
				replace NID = 86950 if year == 2001
				replace NID = 86951 if year == 2002
				replace NID = 86952 if year == 2003
				replace NID = 86953 if year == 2004
				replace NID = 86954 if year == 2005
				replace NID = 86955 if year == 2006
				replace NID = 86956 if year == 2007
				replace NID = 86957 if year == 2008
				replace NID = 86958 if year == 2009
				replace NID = 94170 if year == 2010
				replace NID = 94171 if year == 2011
				replace NID = 121282 if year == 2012
			// iso3 (string)
				gen iso3 = "MEX"
			// subdiv (string)
				destring(cedocve), replace
				merge m:1 cedocve using `mexico_locations', keep(1 3) keepusing(location_id location_name) nogen
				rename location_name subdiv
			// location_id (numeric)
				** defined above
			// national (numeric): 0 = no, 1 = yes
				gen national = 0
			// year (numeric)
				** already exists
			// age (numeric)
				rename cveedad age_units
				destring(age_units), replace
				rename edad age
				gen age_start=.
				replace age_start = 1 if age >=1 & age <=4
				forvalues a = 5(5)90 {
					replace age_start = `a' if age >= `a' & age <= (`a' + 4)
				}
				replace age_start = 95 if age >= 95
				
				replace age_start = 0 if (age_units == 1 & age <= 6) | age_units == 0
				replace age_start = .01 if age_units == 1 & age >= 7 & age <= 27
				replace age_start = .1 if (age_units == 2 & age < 12) | (age_units == 1 & age >= 28)
				replace age_start = 1 if age_units == 2 & age >= 12
				replace age_start = 0 if age_units == 3 & age == 0
				
				replace age_start = 999 if age == 999
				
				drop age age_units
				rename age_start age
			// frmat (numeric): find the WHO format here "{FILEPATH}/Age formats documentation.xlsx"
				gen frmat = 2
			// im_frmat (numeric): from the same file as above
				gen im_frmat = 2
			// sex (numeric): 1=male 2=female 9=missing
				rename sexo sex
				destring(sex), replace
			// platform (string): "Inpatient", "Outpatient"
				gen platform = "Inpatient 1"
				replace platform = "Inpatient 2" if ingre == egreso
			// patient_id (string)
				gen patient_id = ""
			// icd_vers (string): ICD version - "ICD10", "ICD9_detail"
				gen icd_vers = "ICD10"
			// dx_* (string): diagnoses
				// Rename main diagnosis
					rename afecprin afec00
				// Rename diagnoses to use the dx_ convention
					foreach var of varlist afec0* {	
						replace `var'=subinstr(`var', "-", "", .)
						local newname = subinstr("`var'", "afec0", "dx_", 1)
						rename `var' `newname'
					}
				// Change diagnosis code numbering (main diagnosis  = dx_1)
					forvalues i = 8(-1)0 {
						local nn = `i' + 1
						display "Renaming dx_`i' to dx_`nn'"
						rename dx_`i' dx_`nn'
					}
			// ecode_* (string): variable if E codes are specifically mentioned
				rename causaext ecode_1
				replace ecode_1 = "" if ecode_1 == "8888"
			// Remove extra characters and make upper case
				foreach var of varlist dx_* ecode_* {
					display in red "Standardizing `var'"
					replace `var' = subinstr(`var', "-","",.)
					replace `var' = upper(`var')
					count if substr(`var',-1,.) == "X"
					while `r(N)' > 0 {
						replace `var' = substr(`var',1,length(`var')-1) if substr(`var',-1,.) == "X"
						count if substr(`var',-1,.) == "X"
					}
				}
			// Inpatient variables
				// discharges (numeric)
					gen metric_discharges = 1
				// day cases (numeric)
					gen metric_day_cases = 0
					replace metric_day_cases = 1 if ingre == egreso
				// deaths (numeric)
					destring(motegre), replace
					gen metric_deaths = 0
					replace metric_deaths = 1 if motegre == 5

		
		// collapse data before redistributing by location or else you just zero out a lot of values
		// the weights are all small, less than 0.5. so 1 * n < 0.5 will always result in a value less than 0.5 which gets dropped				
		collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

		// REDISTRIBUTE DATA THAT IS NOT ASSIGNED TO A SPECIFIC STATE
			// Get list of subnationals
				levelsof(location_id), local(lids) clean
			// Save data
				tempfile master_data
				save `master_data', replace
			// Get subnational proportions
				preserve
					use `MEX_subnational_pop', clear
					drop if subdiv == "Total"
					egen wgt = pc(pop), by(year sex age) prop
					rename age age_grp
					tempfile subnational_prop
					save `subnational_prop', replace
				restore
			// Redistribute subnational data
				// Get unallocated data
					keep if location_id == .
					drop if sex != 1 & sex != 2
					tempfile unallocated_locs
					save `unallocated_locs', replace
				// Create duplicate unallocated for all locations
					clear
					foreach lid of local lids {
						append using `unallocated_locs'
						replace location_id = `lid' if location_id == .
					}
				// Merge on proportions
					gen age_grp = age
					replace age_grp = 0 if age < 1
					replace age_grp = 80 if age >= 80
					drop subdiv
					merge m:1 location_id year sex age_grp using `subnational_prop', keep(1 3) keepusing(subdiv wgt) nogen
				// Multiply metrics by proportions
					foreach metric of varlist metric_* {
						replace `metric' = `metric' * wgt
						replace `metric' = 0 if `metric' < 0.5
						replace `metric' = 1 if `metric' >= 0.5 & `metric' < 1
					}
				// Append on master data
					append using `master_data'
					drop if location_id == .

		// VARIABLE CHECK
			// If any of the variables in our template are missing, create them now (even if they are empty)
			// All of the following variables should be present
				#delimit;
				order
				iso3 subdiv location_id national
				source NID
				year
				age frmat im_frmat
				sex platform patient_id
				icd_vers dx_* ecode_*
				metric_*;
			// Drop any variables not in our template of variables to keep
				keep
				iso3 subdiv location_id national
				source NID
				year
				age frmat im_frmat
				sex platform patient_id
				icd_vers dx_* ecode_*
				metric_*;
				#delimit cr

		// COLLAPSE DATA
		// this was the final collapse in the original formatting scripts
		collapse (sum) metric_*, by(iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_* ecode_*) fast

		preserve
			keep if metric_discharges > 0
			compress
			save "`wide_format_folder'/MEX_SINAIS_`year'.dta", replace
		restore
		gen cause_primary = dx_1
		// Prioritize External Cause of Injury & Service Codes over Nature of Injury
			// Diagnosis codes
				local dx_count = 0
				foreach var of varlist dx_* {
					local dx_count = `dx_count' + 1
				}
				local dx_count = `dx_count'
				// Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes
					forvalues dx = `dx_count'(-1)1 {
						display "Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes in dx_`dx'"
						replace cause_primary = dx_`dx' if (inlist(substr(dx_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(dx_`dx',1,1),"E") & icd_vers == "ICD9_detail")
					}
			// External cause set
				local ecode_count = 0
				foreach var of varlist ecode_* {
					local ecode_count = `ecode_count' + 1
				}
				local ecode_count = `ecode_count'
				// Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes
					forvalues dx = `ecode_count'(-1)1 {
						display "Prioritizing ICD-10 V, W, X, Y codes and ICD-9 E codes in ecode_`dx'"
						replace cause_primary = ecode_`dx' if (inlist(substr(ecode_`dx',1,1),"V","W","X","Y") & icd_vers == "ICD10") | (inlist(substr(ecode_`dx',1,1),"E") & icd_vers == "ICD9_detail")
					}	
		// Reshape long
			drop dx_1
			rename cause_primary dx_1
			. gen long id = _n
			reshape long dx_ ecode_, i(id) j(dx_ecode_id)
			replace dx_ecode_id = 2 if dx_ecode_id > 2
			drop if missing(dx_)
			//drop id

		// Collapse
			keep iso3 subdiv location_id national source NID year age frmat im_frmat sex platform patient_id icd_vers dx_ecode_id metric_* dx_ ecode_

		// STANDARDIZE PLATFORMS
			replace platform = "Outpatient" if platform == "Emergency"
			replace platform = "1" if platform == "Inpatient" | platform == "Inpatient 1"
			// drop day cases
			drop if platform == "Inpatient 2"
			replace platform = "2" if platform == "Outpatient"
			destring platform, replace
			assert platform != .
			
		// STANDARDIZE METRICS
			gen cases = .
				capture confirm var metric_discharges
				if !_rc {
					replace cases = metric_discharges if platform == 1
				}
				capture confirm var metric_discharges_weighted
				if !_rc {
					replace cases = metric_discharges_weighted if platform == 1
				}
				capture confirm var metric_visits
				if !_rc {
					replace cases = metric_visits if platform == 2
				}
				capture confirm var metric_visits_weighted
				if !_rc {
					replace cases = metric_visits_weighted if platform == 2
				}
			gen deaths = .
				capture confirm var metric_deaths
				if !_rc {
					replace deaths = metric_deaths
				}
				capture confirm var metric_deaths_weighted
				if !_rc {
					replace deaths = metric_deaths_weighted
				}
			
		// STANDARDIZE AGES
			drop if age == .
			replace age = 95 if age > 95
			replace age = 0 if age < 1

			rename age age_start
			gen age_end = age_start + 4
			replace age_end = 1 if age_start == 0
			replace age_end = 99 if age_start == 95
			replace age_end = 4 if age_start == 1
			
		// LOCATION_ID
			rename location_id location_id_orig
			merge m:1 iso3 using "$prefix{FILEPATH}/location_ids.dta"
				assert _m != 1
				drop if _m == 2
				drop _m
			replace location_id = location_id_orig if location_id_orig != .
			drop location_id_orig
			
		// COLLAPSE CASES
			collapse (sum) cases deaths, by(iso3 subdiv location_id source national year age_* sex platform icd_vers dx_ dx_ecode_id NID) fast

			// modify columns to fit our structure
				rename dx_ cause_code
				rename dx_ecode_id diagnosis_id

			// save year processed data
			tempfile processed_`year'
			save "`processed_`year''", replace
	}

	clear
	forvalues year = 2000(1)2012 {
		display in red "Appending on data from `year'"
		append using "`processed_`year''"
	}

	// drop unneeded cols
	drop iso3 subdiv
	
	// SAVE
		compress
		save "`output_folder'/formatted_MEX_SINAIS.dta", replace
		save "`archive_folder'/formatted_MEX_SINAIS_`today'.dta", replace

	capture log close


// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
