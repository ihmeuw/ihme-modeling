**Splits based on the regressions (parent script)
**GBD 2017

//prep stata
clear all
set more off
set maxvar 32000 //maximum number of columns stata allows

//Set OS flexibility
if c(os) == "Unix" {
	local j "FILEPATH"
	local h "FILEPATH"
}
else if c(os) == "Windows" {
	local j "FILEPATH"
	local h "FILEPATH"
}

qui do "`j'/FILEPATH/save_results_epi.ado"
qui do "`j'/FILEPATH/get_demographics.ado"

***********************************************************
***RUN IDIOPATHIC SPLIT IN PARALLEL
***********************************************************

//set locals
local save_folder_whz2_noedema FILEPATH
local save_folder_whz2_edema FILEPATH
local save_folder_whz3_noedema FILEPATH
local save_folder_whz3_edema FILEPATH
local ratio_folder FILEPATH
local code_folder FILEPATH

//get locations we want
get_demographics, gbd_team(epi) clear
local location_ids `r(location_id)'

//submit jobs
foreach loc of local location_ids {
	local loc `loc'
	!qsub -N "pem_`loc'" -o "FILEPATH" -e "FILEPATH" -pe multi_slot 4 -P "PROJECT" "`j'/FILEPATH" "`code_folder'/04_incidence_child_script.do" "`code_folder' `save_folder_whz2_noedema' `save_folder_whz2_edema' `save_folder_whz3_noedema' `save_folder_whz3_edema' `loc' `ratio_folder'"
}

//save results 
local file_pattern "{measure_id}_{location_id}.csv"
local measure_ids 5 6
local description_whz2_noedema "prev + incidence: moderate wasting (-2 to -3 SD) without edema, MODEL VERSION IDS (ST GPR)"
local description_whz2_edema "prev + incidence: moderate wasting (-2 to -3 SD) with edema, MODEL VERSION IDS (ST GPR)"
local description_whz3_noedema "prev + incidence: severe wasting (>-3 SD) without edema, MODEL VERSION IDS (ST GPR)"
local description_whz3_edema "prev + incidence: severe wasting (>-3 SD) with edema, MODEL VERSION IDS (ST GPR)"

save_results_epi, modelable_entity_id(1607) input_file_pattern(`file_pattern') description("`description_whz3_noedema'") input_dir(`save_folder_whz3_noedema') measure_id(`measure_ids') mark_best(True) clear
save_results_epi, modelable_entity_id(1608) input_file_pattern(`file_pattern') description("`description_whz3_edema'") input_dir(`save_folder_whz3_edema') measure_id(`measure_ids') mark_best(True) clear
save_results_epi, modelable_entity_id(10981) input_file_pattern(`file_pattern') description("`description_whz2_noedema'") input_dir(`save_folder_whz2_noedema') measure_id(`measure_ids') mark_best(True) clear
save_results_epi, modelable_entity_id(1606) input_file_pattern(`file_pattern') description("`description_whz2_edema'") input_dir(`save_folder_whz2_edema') measure_id(`measure_ids') mark_best(True) clear

