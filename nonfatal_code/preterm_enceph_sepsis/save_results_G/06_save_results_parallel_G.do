/*******************************************

Description: This is the first lower-level script submitted by 
/FILEPATH/. /FILEPATH/ does two things: 
1. submits another lower-level script (not parallelized)
that formats mild_imp prevalence data and runs save_results 
(we do not run a second DisMod model for mild impairment) and 
2. appends and formats modsev prevalence data and saves it for 
upload into the final DisMod model. 

*******************************************/

clear all 
set more off
set maxvar 30000
version 13.0

// priming the working environment
if c(os) == "Windows" {
			local j /*FILEPATH*/
			local working_dir = /*FILEPATH*/
		}
		if c(os) == "Unix" {
			local j /*FILEPATH*/
			ssc install estout, replace 
			ssc install metan, replace
			local working_dir = /*FILEPATH*/
		} 

// arguments
local acause `1'
local me_id `2'

// directories
local out_dir /*FILEPATH*/
local upload_dir /*FILEPATH*/

// functions
adopath + /*FILEPATH*/

******************************************************************

// submit mild/asymp (prevalence only) jobs - will get into format for save_results function (because we don't run DisMod for mild/asymp)

/* QSUB TO NEXT SCRIPT TO SAVE ASYMPT RESULTS*/

/* QSUB TO NEXT SCRIPT TO SAVE MILD RESULTS*/

local ages 0 2 3 4
foreach age_id in `ages' {
	
	/* QSUB TO NEXT SCRIPT TO SAVE MOD/SEV RESULTS FOR EACH AGE GROUP*/
}

