** Calculate Childhood wasting exposure, -3 to -2 sd
** GBD 2017

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

//get locations we want
get_demographics, gbd_team(epi) clear

local location_ids `r(location_id)'

//set locals
local save_folder_8946 FILEPATH
local code_folder FILEPATH

//submit jobs
foreach loc of local location_ids {
	local loc `loc'
	!qsub -N "pem_`loc'" -o "FILEPATH" -e "FILEPATH" -P "PROJECT" "`j'/FILEPATH" "`code_folder'/01_8946_child_script.do" "`code_folder' `save_folder_8946'  `loc'"
}

//save results 
local file_pattern "{measure_id}_{location_id}.csv"
local description_8946 "PEM code DESCRIPTION, all ages"
local measure_ids 5
save_results_epi, modelable_entity_id(8946) input_file_pattern(`file_pattern') description("`description_8946'") input_dir(`save_folder_8946') measure_id(`measure_ids')



