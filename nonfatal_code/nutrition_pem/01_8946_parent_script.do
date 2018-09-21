** Calculate Childhood wasting exposure, -3 to -2 sd

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

do "FILEPATH/save_results.do"
do "FILEPATH/get_demographics.ado"

//set locals
local save_folder_8946 /FILEPATH/8946/
local code_folder /FILEPATH/pem/

//get locations we want
get_demographics, gbd_team(epi) clear

local location_ids `r(location_ids)'

//submit jobs
foreach loc of local location_ids {
	local loc `loc'
	!qsub -N "pem_`loc'" -o "FILEPATH/output" -e "/FILEPATH/errors" -P "proj_ensemble_models" "FILEPATH/stata_shell.sh" "`code_folder'/01_8946_child_script.do" "`code_folder' `save_folder_8946'  `loc'"
}

//save results 
local file_pattern "{measure_id}_{location_id}.csv"
local description_8946 "prevalence of WHZ < -3 SD to < -2 SD, 2:190460 , 3:190463  (ST GPR)"
save_results, modelable_entity_id(8946) mark_best(yes) env(prod) file_pattern(`file_pattern') description("`description_8946'") in_dir(`save_folder_8946') 



