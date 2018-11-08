** calculate incidence:prevalence ratio for non-fatal PEM
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

qui do "`j'/FILEPATH/get_demographics.ado"


//set locals
local save_folder_ratio FILEPATH
local code_folder FILEPATH

//get locations we want
get_demographics, gbd_team(epi) clear
local location_ids `r(location_id)'

//submit jobs
foreach loc of local location_ids {
	local loc `loc'
	!qsub -N "pem_`loc'" -o "FILEPATH" -e "FILEPATH" -pe multi_slot 4 -P "PROJECTS" "`j'/FILEPATH" "`code_folder'/03_ratio_child_script.do" "`code_folder' `save_folder_ratio' `loc'"
}
