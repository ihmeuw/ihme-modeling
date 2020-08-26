// File Purpose: combine output from gpr models to create exposure inputs to PAF

//Housekeeping
clear all 
set more off
set maxvar 30000

** toggle
local run "run1" // first run
local decomp_step "decomp_step4" // change as needed
local c_date= c(current_date)
local date = subinstr("`c_date'", " " , "_", .)

//Set relevant locals
local input_folder		"FILEPATH"
local output_folder		"FILEPATH"
local graphloc			"FILEPATH"

// Prep dataset
**Improved proportion sanitation**
import delimited "FILEPATH", clear
keep location_id year_id age_group_id sex_id draw_*
forvalues n = 0/999 {
	rename draw_`n' prop_improved_`n'
}
tempfile sanitation
save `sanitation', replace


**Sewer**
import delimited "FILEPATH", clear
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

//Save data for graphing
foreach exp in unimp improved sewer {
	preserve
	keep age_group_id location_id year_id sex_id prev_`exp'_*
	save "FILEPATH", replace
	restore
}
**save data**
save "FILEPATH", replace
