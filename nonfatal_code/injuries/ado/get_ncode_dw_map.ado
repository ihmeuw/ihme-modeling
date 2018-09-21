/*
AUTHOR: USERNAME

DATE: DATE

PURPOSE: Get a map from injuries N-codes to their corresponding DW draws
*/

cap program drop get_ncode_dw_map
program define get_ncode_dw_map
	version 12
	syntax , Out_dir(string) Category(string) prefix(string)
	
	qui {
	
// Define filepaths
	local in_dir "FILEPATH"
	local n_hs_input "`in_dir'/FILEPATH.csv"
	local hs_dw_input "`prefix'/FILEPATH.csv"
	local tbi_pct_data "`prefix'/FILEPATH.CSV"
	
// Import N-code to health state map
	import delimited using "`n_hs_input'", clear case(preserve) delim(",") varnames(1)
	keep ncode hs_`category' hhseqid_`category'
	drop if hs_`category' == "" | hs_`category' == "custom"
	rename hs_`category' healthstate
	tempfile n_hs
	save `n_hs'
	
// Import Health-state to DW map
	import delimited using "`hs_dw_input'", clear case(preserve) delim(",") varnames(1) asdouble
	drop healthstate_id
	// drop if healthstate == ""
	rename hhseqid hhseqid_`category'
		// Edit for using just hhseqid:
		duplicates drop hhseqid_`category', force
	tempfile hs_dw
	save `hs_dw'
	drop if healthstate == ""
	tempfile hs_dw_tbi
	save `hs_dw_tbi'
	use `hs_dw', clear
	
// Merge 2 maps together
	merge 1:m hhseqid_`category' using `n_hs', keep(match) nogen
	drop healthstate
	tempfile n_dw
	save `n_dw'
	
// Calculate long-term TBI values and insert
	if "`category'" != "st" {
		
	// Generate file of TBI draws
		capture mkdir "`out_dir'/FILEPATH"
		gen_tbi_pct_draws, in_path("`tbi_pct_data'") out_dir("`out_dir'/FILEPATH")
		
		foreach n in N27 N28 {
		// Import % draws
			import delimited using "`out_dir'/FILEPATH/`n'.csv", clear varnames(1)
			rename draw* pct*
			
		// Merge file onto health state to DW map
			merge 1:1 healthstate using `hs_dw_tbi', keep(match) nogen
		
		// Collapse to one set of DW draws
			forvalues x = 0/999 {
				replace draw`x' = draw`x' * pct_`x'
				drop pct_`x'
			}
			collapse (sum) draw*
			gen ncode = "`n'"
			tempfile `n'
			save ``n''
			
		// Erase the temporary % file
			erase "`out_dir'/FILEPATH/`n'.csv"
		}
		
	// Append onto rest of N-code to DW map
		use `n_dw', clear
		foreach n in N27 N28 {
			append using ``n''
		}
		save `n_dw', replace
	}
	
// Remove now empty temporary directory
	cap rmdir "`out_dir'/FILEPATH"

// Format
	order ncode, first
	sort_by_ncode ncode
	format draw* %16.0g

// Save final file
	rename ncode n_code
	keep n_code draw*
	export delimited using "`out_dir'/FILEPATH.csv", replace delim(",")
	
	}
	
end
