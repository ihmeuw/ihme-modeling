**Interpolation of death rates based on second Dismod model for Parkinson's disease (parent script)
**12/30/2016

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

***********************************************************
***RUN INTERPOLATION IN PARALELL
***********************************************************

//set locals
local save_folder FILEPATH
local code_folder FILEPATH
local version_id 152276

//get year_start we want
numlist "1980 1995 2000 2005 2010"
local year_start `r(numlist)'

//get sexes
numlist "1 2"
local sexes `r(numlist)'

//submit jobs
foreach year of local year_start {
	foreach sex of local sexes {
		local year `year'
		local sex `sex'
		!qsub -N "parkinson_`year'_`sex'" -o "FILEPATH" -e "FILEPATH" -pe multi_slot 15 -P "proj_custom_models" "FILEPATH/stata_shell.sh" "FILEPATH.do" "`code_folder' `save_folder' `year' `version_id' `sex'"
	}
}


save_results, cause_id(544) description("Parkinson Epi Model 152276") in_dir(`save_folder') mark_best(yes)
