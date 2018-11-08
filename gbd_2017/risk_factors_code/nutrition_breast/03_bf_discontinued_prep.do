** Project: RF: Suboptimal Breastfeeding
** Purpose: Calculate and Prep Discontinued Breastfeeding 6 to 23 months

clear
capture restore, not
set more off
set maxvar 30000

	
//set relevant locals
	local bf_prev_draws  	"FILEPATH"
	adopath + "FILEPATH"
	local out_dir_draws "FILEPATH"

**change age_group id to reflect the correct age groups
use "FILEPATH", clear
gen age_group_id = .
replace age_group_id = 4 if age == 0.1
replace age_group_id = 5 if age == 1

replace location_id = 44533 if location_id == 6
rename iso3 ihme_loc_id
rename sex sex_id

local cats "cat1 cat2"
foreach cat of local cats {
	preserve
		keep ihme_loc_id year_id sex_id location_id age_group_id exp_`cat'_*
		forvalues n = 0/999 {
			rename exp_`cat'_`n' draw_`n'
		}
		save "FILEPATH", replace
		export delimited "FILEPATH", replace
	restore
}
