
*** BOILERPLATE ***
  clear all
  set maxvar 10000
  set more off
  
  adopath + "FILEPATH"
  run "FILEPATH".do

  
*** PULL IN CAUSE_ID & DESCRIPTION FROM BASH COMMAND ***  
	gettoken cause_id description: 0
	gettoken mark_best description: description
  
 
*** SET UP OUTPUT DIRECTORIES ***  
	local rootDir  "FILEPATH"
	local causeDir "FILEPATH"
	local drawDir "FILEPATH"
	local logDir "FILEPATH"
	


*** START LOG ***  
	log using "FILEPATH", replace
  

*** SAVE RESULTS ***
	save_results_cod, cause_id(`cause_id') mark_best(`mark_best') file_pattern({location_id}.csv) description("`description'") in_dir(`drawDir')

  
	log close
  
  
  



