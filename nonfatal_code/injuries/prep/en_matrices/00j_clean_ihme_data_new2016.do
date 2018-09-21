** Author: USERNAME
** Date: DATE
** Purpose: Clean New GBD Hospital Data for GBD 2016

clear all
set more off
capture log close
capture restore, not

local check = 1
if `check' == 1 {
	local 1 "FILEPATH"
	local 2 "FILEPATH"
	local 3 "FILEPATH"
	local 4 "`1'/FILEPATH"
	local 5 "`1'/FILEPATH"
}

** import macros
global prefix `1'
local code_dir `2'
local output_data_dir `3'
local log_dir `4'
// Directory of general GBD ado functions
local gbd_ado `5'
// slots used
local slots `6'
// Step diagnostics directory
local diag_dir `7'
// Name for this job
local name `8'

// Import GBD functions
adopath + `gbd_ado'
adopath + "`code_dir'/ado"

// start_timer, dir("`diag_dir'") name("`name'") slots(`slots')

// Log
log using "`log_dir'/FILEPATH.smcl", replace name(worker)

** set other macros
local hospital_data_dir "FILEPATH"
if "$prefix" == "FILEPATH" set odbcmgr unixodbc

** first get the list of sources to grab
cd "`hospital_data_dir'"
local files :  dir . files "*.csv"

	local filepath "`hospital_data_dir'/FILEPATH.csv"
	di "Working on `file'"
	import delimited "`filepath'", clear

	keep iso3 year_start age_start facility_id code_system_id dx* sex
	rename year_start year
	rename age_start age
	rename facility_id platform

	replace platform = "Inpatient" if regexm(platform, "inpatient")
	replace platform = "Inpatient" if platform == "hospital"
	replace platform = "Outpatient" if platform == "emergency"
	replace platform = "Outpatient" if platform == "day clinic"
	replace platform = "Inpatient" if regexm(platform, "ospital")
	replace platform = "Outpatient" if regexm(platform, "Cl") & regexm(platform, "nica")

	ds dx*
	local diagnoses `r(varlist)'

	foreach diag of local diagnoses {
		tostring `diag', replace
		replace `diag' = subinstr(`diag', ".", "", .)
	}

	local count: word count `diagnoses'
	if `count' == 1 {
			di "This source, `source', only has 1 diagnosis code so don't look at it."
		}
		else {

			tempfile alldata
			save `alldata', replace

			** get the ICD code version for this source
			
			if code_system_id == 1 {
				local version 9
			}
			else {
				local version 10
			}

			use `alldata', clear

			gen ecode = .
			gen ncode = .

			foreach dx of local diagnoses {
				di "Working on diagnosis `dx'"

				map_icd_2016 `dx', icd_vers(`version') code_dir(`code_dir')
				qui replace ecode = 1 if regexm(cause_`dx', "inj")
				qui replace ncode = 1 if regexm(cause_`dx', "N")
			}

			keep if ecode ==  1 & ncode == 1

			keep cause_dx* year age sex platform

			count

			if `r(N)' == 0 {
				di "There are no dual-coded E and N observations for this source"
			}
			else {

				* get the number of diagnosis fields
				ds cause_dx*
				local diagnoses `r(varlist)'
				local diag_count : word count `diagnoses'

				* set the counter for e and ncodes so that we can loop through variables but only create if someone has an
				* E and N code that we care about for that diagnosis code
				local ecounter 1
				local ncounter 1

				forvalues i = 1/`diag_count' {

					di "Working on `i' of ecodes"
					generate tmp_ecode_`ecounter' = cause_dx_`i' if regexm(cause_dx_`i', "^inj")
					cap confirm variable tmp_ecode_`ecounter'
					if !_rc {
						local ++ecounter
					}

					di "Working on `i' of ncodes"
					generate tmp_ncode_`ncounter' = cause_dx_`i' if regexm(cause_dx_`i', "^N")
					cap confirm variable tmp_ncode_`ncounter'
					if !_rc {
						local ++ncounter
					}

					drop cause_dx_`i'
				}

				ds tmp_ecode*
				local ecode_vars `r(varlist)'

				ds tmp_ncode*
				local ncode_vars `r(varlist)'

				local ecode_varnum : word count `ecode_vars'
				local ncode_varnum : word count `ncode_vars'

				forvalues i = 1/`ecode_varnum' {

					qui generate final_ecode_`i' = ""

					forvalues j = 1/`ecode_varnum' {

						di "Working on `i' + `j' for ecodes"

						qui replace final_ecode_`i' = tmp_ecode_`j' if final_ecode_`i' == ""
						qui replace tmp_ecode_`j' = "" if tmp_ecode_`j' == final_ecode_`i'
					}
				}

				forvalues i = 1/`ncode_varnum' {
					qui generate final_ncode_`i' = ""

					forvalues j = 1/`ncode_varnum' {

						di "Working on `i' + `j' for ncodes"

						qui replace final_ncode_`i' = tmp_ncode_`j' if final_ncode_`i' == ""
						qui replace tmp_ncode_`j' = "" if tmp_ncode_`j' == final_ncode_`i'
					}
				}

			drop tmp_*

			local source = upper("`source'")
				
			** generate a binary indicator for inpatient/not-inpatient
			generate inpatient = 0
			replace inpatient = 1 if platform == "Inpatient"
			drop platform

		}
	
	}
			** end block for sources with E-N data
	}

	** save appended/cleaned sources
	compress
	export delimited using "`output_data_dir'/FILEPATH.csv", delim(",") replace
	
	// end_timer, dir("`diag_dir'") name("`name'")
	
	log close worker
	





















}
