** Author: USERNAME
** Date: DATE
** Purpose: Clean GBD Hospital Data for EN matrices

clear all
set more off
capture log close
capture restore, not

local check = 99
if `check' == 1 {
	local 1 "FILEPATH"
	local 2 "FILEPATH"
	local 3 "`1'/FILEPATH"
	local 4 "`1'/FILEPATH"
	local 5 "`1'/FILEPATH"
}

** import macros
global prefix `1'
local code_dir `2'
local output_data_dir `3'
di "Output: `output_data_dir'"
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
local hospital_dir_list : dir "`hospital_data_dir'" dirs "*"

** loop through the sources
local x=1
foreach source of local hospital_dir_list {

	local source=upper("`source'")
	di in red "starting source `source'"
	
	local source_file "`hospital_data_dir'/FILEPATH.dta"
	di "`source_file'"
	
	** want to use only sources where the "01_mapped.dta" file exists
	capture confirm file "`source_file'"
	
	if !_rc {
		
		display "`source' is ready"
		
		use iso3 year age sex platform icd_vers dx* using "`source_file'", clear

		* count the number of diagnosis fields
		ds dx*
		local diagnoses `r(varlist)'

		local count: word count `diagnoses'

		if `count' == 1 {
			di "This source, `source', only has 1 diagnosis code so don't look at it."
		}
		else {

			tempfile alldata
			save `alldata', replace

			** get the ICD code version for this source
			
			if regexm(icd_vers, "ICD10") {
				local version 10
			}
			else {
				local version 9
			}

			use `alldata', clear

			** map the sourcen to E- and N-code ICD codes for 2016

			gen ecode = .
			gen ncode = .

			foreach dx of local diagnoses {
				di "Working on diagnosis `dx'"

				map_icd_2016 `dx', icd_vers(`version') code_dir(`code_dir')
				qui replace ecode = 1 if regexm(cause_`dx', "inj")
				qui replace ncode = 1 if regexm(cause_`dx', "N")
			}

			keep if ecode ==  1 & ncode == 1

			keep cause_dx* year age sex platform iso3

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
			
			** replace sex with missing
			replace sex=. if sex==9		

			** need to append all of the sources onto one another
			if `x'==1 {
				gen source = "`source'"
				tempfile sources
				save `sources', replace
				local ++x
			}
			
			else {
				gen source = "`source'"
				append using `sources'
				save `sources', replace
				local ++x
			}

		}
	
	}
			** end block for sources with E-N data
	}
	}
	** end block for sources where the "FILEPATH.dta" file exists
	
	** save appended/cleaned sources
	compress
	export delimited using "`output_data_dir'/FILEPATH.csv", delim(",") replace
	
	log close worker
	