// File Purpose: combine output from gpr models to create exposure inputs to PAF
// Author: [AUTHOR]
// Date: [DATE]

//Set directories
	if c(os) == "Unix" {
		global j "/home/j"
		set more off
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "J:"
	}


//Housekeeping
clear all 
set more off
set maxvar 30000

** toggle
*local run "run1" // first run
*local run "run2" // custom code fix
*local run "run3"
*local run "run4" // running for team meeting 8/25 (these are not final results)
*local run "run5" // GBD 2020 best version, hopefully
local run "run6" // offset all data slightly to avoid values of exactly 0 or 1
local c_date= c(current_date)
local date = subinstr("`c_date'", " " , "_", .)
local gbd_round "GBD2020" // change as needed
local decomp_step "iterative" // change as needed

//Set relevant locals
local input_folder		"FILEPATH/`gbd_round'/`decomp_step'/wash_sanitation/`run'"
local output_folder		"`input_folder'"
local graphloc			"FILEPATH"
adopath + "FILEPATH"

// Prep dataset
**Improved proportion sanitation**
import delimited "`input_folder'/wash_sanitation_imp_prop", clear
keep location_id year_id age_group_id sex_id draw_*
forvalues n = 0/999 {
	rename draw_`n' prop_improved_`n'
}
tempfile sanitation
save `sanitation', replace


**Sewer**
import delimited "`input_folder'/wash_sanitation_piped", clear
keep location_id year_id age_group_id draw_*
forvalues n = 0/999 {
	rename draw_`n' prev_sewer_`n'
}

tempfile sewer
save `sewer', replace

// Merge on with improved sanitation
merge 1:1 location_id year_id using `sanitation', keep(1 3) nogen

//Calculate improved prevalence by multiplying times 1- sewer prevalence
forvalues n = 0/999 {
	gen prev_improved_`n' = prop_improved_`n' * (1 - prev_sewer_`n')
}
// estimate remaining unimproved category
forvalues n = 0/999 {
	gen prev_unimp_`n' = 1 - (prev_improved_`n' + prev_sewer_`n')
}

****replace negative prevalence numbers
local cats "improved sewer unimp" 
foreach cat of local cats {
	forvalues n = 0/999 {
	replace prev_`cat'_`n' = 0.0001 if prev_`cat'_`n' < 0
	replace prev_`cat'_`n' = 0.999 if prev_`cat'_`n' > 1	
		}
}

**rescale draws from all three categories to make sure they add up to 1
forvalues n = 0/999 {
	gen total_`n' = (prev_improved_`n' + prev_sewer_`n' + prev_unimp_`n')
	replace prev_improved_`n' = (prev_improved_`n'/(total_`n'))
	replace prev_sewer_`n' = (prev_sewer_`n'/(total_`n'))
	replace prev_unimp_`n' = (prev_unimp_`n'/(total_`n'))
}
drop total*

tempfile san_cats
save `san_cats', replace

//Save data on share and J for graphing
foreach exp in unimp improved sewer {
	preserve
	keep age_group_id location_id year_id sex_id prev_`exp'_*
	save "`output_folder'/`exp'", replace
	restore
}
**save data**
save "`graphloc'/allcat_prev_san_`date'", replace

