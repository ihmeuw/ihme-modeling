// Subtract fracture injuries from other musculoskeletal and re-upload the results

clear all
set more off
set mem 2g
set maxvar 32000
if c(os) == "Unix" {
global prefix "FILEPATH"
set odbcmgr unixodbc
}
else if c(os) == "Windows" {
global prefix "FILEPATH"
}

local tester 1

local subtract = 0
local epi_upload = 1
local target_me = 3136

local repo "FILEPATH"
adopath + "FILEPATH"
adopath + "FILEPATH"
load_params


get_demographics, gbd_team(epi) clear
global year_ids `r(year_ids)'
global location_ids `r(location_ids)'
global sex_ids `r(sex_ids)'


cap mkdir "FILEPATH"

if `subtract' == 1 {
	foreach location_id of global location_ids {
		foreach year of global year_ids {
			foreach sex of global sex_ids {
				! qsub -P proj_injuries_2 -N inj_msk_`location_id'_`year'_`sex' -pe multi_slot 4 -l mem_free=8 "FILEPATH" "FILEPATH" "`repo' `location_id' `year' `sex'"
			}
		}
	}
}

// Upload results
if `epi_upload' == 1 {
	run "FILEPATH"
	save_results, modelable_entity_id(`target_me') description("Prevalence and Incidence: Final Submission") in_dir("FILEPATH") mark_best("yes")
}

di "DONE"

