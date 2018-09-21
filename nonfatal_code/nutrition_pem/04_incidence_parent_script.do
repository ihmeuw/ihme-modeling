
//prep stata
clear all
set more off
set maxvar 32000 //maximum number of columns stata allows

//Set OS flexibility
if c(os) == "Unix" {
	local j "/home/j"
	local h "~"
}
else if c(os) == "Windows" {
	local j "J:"
	local h "H:"
}

do "/*FILEPATH*/save_results.do"
do "/*FILEPATH*/get_demographics.ado"

***********************************************************
***RUN IDIOPATHIC SPLIT IN PARALLEL
***********************************************************

//set locals
local save_folder_whz2_noedema /*FILEPATH*/
local save_folder_whz2_edema /*FILEPATH*/
local save_folder_whz3_noedema /*FILEPATH*/
local save_folder_whz3_edema /*FILEPATH*/
local ratio_folder /*FILEPATH*/
local code_folder /*FILEPATH*/

//get locations we want
get_demographics, gbd_team(epi) clear

local location_ids `r(location_ids)'

//submit jobs
foreach loc of local location_ids {
	local loc `loc'
	!qsub -N "pem_`loc'" -o "/*FILEPATH*/" -e "/*FILEPATH*/" -pe multi_slot 4 -P "proj_ensemble_models" "/*FILEPATH*/" "`code_folder'/04_incidence_child_script.do" "`code_folder' `save_folder_whz2_noedema' `save_folder_whz2_edema' `save_folder_whz3_noedema' `save_folder_whz3_edema' `loc' `ratio_folder'"
}

//save results 
local file_pattern "{measure_id}_{location_id}.csv"
*local file_pattern "6_{location_id}.csv"
local description_whz2_noedema "prev + incidence: moderate wasting (-2 to -3 SD) without edema, 190556 190550 190547 190553 (ST GPR)"
local description_whz2_edema "prev + incidence: moderate wasting (-2 to -3 SD) with edema, 190556 190550 190547 190553 (ST GPR)"
local description_whz3_noedema "prev + incidence: severe wasting (>-3 SD) without edema, 190556 190550 190547 190553 (ST GPR)"
local description_whz3_edema "prev + incidence: severe wasting (>-3 SD) with edema, 190556 190550 190547 190553 (ST GPR)"

save_results, modelable_entity_id(1607) mark_best(yes) env(prod) file_pattern(`file_pattern') description("`description_whz3_noedema'") in_dir(`save_folder_whz3_noedema') 
save_results, modelable_entity_id(1608) mark_best(yes) env(prod) file_pattern(`file_pattern') description("`description_whz3_edema'") in_dir(`save_folder_whz3_edema')
save_results, modelable_entity_id(10981) mark_best(yes) env(prod) file_pattern(`file_pattern') description("`description_whz2_noedema'") in_dir(`save_folder_whz2_noedema')
save_results, modelable_entity_id(1606) mark_best(yes) env(prod) file_pattern(`file_pattern') description("`description_whz2_edema'") in_dir(`save_folder_whz2_edema')

