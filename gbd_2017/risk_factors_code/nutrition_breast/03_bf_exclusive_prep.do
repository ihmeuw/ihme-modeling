** Project: RF: Suboptimal Breastfeeding
** Purpose: Calculate and Prep Non-Exclusive Breastfeeding 0 to 5 months

clear
set more off
set maxvar 30000
capture restore, not


//set relevant locals
adopath + "FILEPATH"
local bf_prev_draws  	"FILEPATH"
local out_dir_draws		"FILEPATH"


use "FILEPATH", clear
replace location_id = 44533 if location_id == 6

**change age_group id to reflect the correct age groups
replace age_group_id = 3 if age == .01
replace age_group_id = 4 if age == 0.1

local cats "cat1 cat2 cat3 cat4"
foreach cat of local cats {
	preserve
		keep ihme_loc_id year_id sex_id location_id age_group_id exp_`cat'_*
		forvalues n = 0/999 {
			rename exp_`cat'_`n' draw_`n'
		}
		export delimited "FILEPATH", replace
	restore
}
