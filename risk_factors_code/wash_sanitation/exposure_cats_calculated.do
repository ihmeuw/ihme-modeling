// Additional Comments: 

//Set directories
	if c(os) == "Unix" {
		global j "FILEPATH"
		set more off
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "FILEPATH"
	}


//Housekeeping
clear all 
set more off
set maxvar 30000

** toggle
local run "run5"

//Set relevant locals
local input_folder		""
local output_folder		"`input_folder'"
local graphloc			""
adopath + ""

local date ""

// Prep dataset
**Sanitation**
import delimited "`input_folder'/wash_sanitation_imp", clear
keep location_id year_id age_group_id draw_*
forvalues n = 0/999 {
	rename draw_`n' prev_improved_`n'
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

foreach exp in unimp improved sewer {
	preserve
	keep age_group_id location_id year_id prev_`exp'_*
	save "`output_folder'/`exp'", replace
	restore
}
**save data**
save "`graphloc'/allcat_prev_san_`date'", replace
