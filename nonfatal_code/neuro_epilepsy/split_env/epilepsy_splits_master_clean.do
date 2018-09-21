**Splits based on the regressions (parent script)
**2/17/2017

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

do "FILEPATH/get_demographics.ado"
do "FILEPATH/save_results.do"

***********************************************************
***RUN IDIOPATHIC SPLIT IN PARALLEL
***********************************************************

//set locals
local save_folder_secondary FILEPATH
local save_folder_primary FILEPATH
local code_folder FILEPATH
local idiopathic_prop FILEPATH/idio_draws_wide2.dta

//get locations we want
get_demographics, gbd_team(epi) clear

local location_ids `r(location_ids)'

//submit jobs
foreach loc of local location_ids {
	local loc `loc'
	!qsub -N "idiopathic_`loc'" -o "FILEPATH" -e "FILEPATH" -pe multi_slot 4 -P "proj_custom_models" "FILEPATH/stata_shell.sh" "FILEPATH.do" "`code_folder' `save_folder_secondary' `save_folder_primary' `loc' `idiopathic_prop'"
}

//save results secondary
local file_pattern "{location_id}.csv"
local description_secondary "Secondary Epilepsy, split from envelope using regression results"
local description_primary "Primary Epilepsy, split from envelope using regression results"

save_results, modelable_entity_id(3026) mark_best(yes) env(prod) file_pattern(`file_pattern') description("`description_secondary'") in_dir(`save_folder_secondary') 

save_results, modelable_entity_id(3025) mark_best(yes) env(prod) file_pattern(`file_pattern') description("`description_primary'") in_dir(`save_folder_primary') 

***********************************************************
***RUN SEVERE AND TNF SPLITS IN PARALLEL AND SAVE RESULTS
***********************************************************

//set locals
local save_folder_severe FILEPATH
local save_folder_notsevere FILEPATH
local save_folder_tnf FILEPATH
local code_folder FILEPATH
local severe_prop FILEPATH.dta
local tg_prop FILEPATH.dta
local tnf_prop FILEPATH.dta

//get locations we want
get_demographics, gbd_team(epi) clear

local location_ids `r(location_ids)'

//submit jobs
foreach loc of local location_ids {
	local loc `loc'
	!qsub -N "severe_`loc'" -o "FILEPATH" -e "FILEPATH" -pe multi_slot 4 -P "proj_custom_models" "FILEPATH/stata_shell.sh" "FILEPATH.do" "`code_folder' `save_folder_severe' `save_folder_notsevere' `save_folder_tnf' `loc' `severe_prop' `tg_prop' `tnf_prop'"
}

//save results
local file_pattern "{location_id}.csv"
local description_severe "Severe Epilepsy, split from envelope using regression results"
local description_notsevere "Less Severe Epilepsy, split from envelope using regression results"
local description_tnf "Epilepsy that is Treated Without Fits, split from envelope using regression results"

save_results, modelable_entity_id(1953) mark_best(yes) description(`description_severe') env(prod) in_dir(`save_folder_severe') file_pattern(`file_pattern')

save_results, modelable_entity_id(1952) mark_best(yes) description(`description_notsevere') env(prod) in_dir(`save_folder_notsevere') file_pattern(`file_pattern')

save_results, modelable_entity_id(1951) mark_best(yes) description(`description_tnf') env(prod) in_dir(`save_folder_tnf') file_pattern(`file_pattern')

