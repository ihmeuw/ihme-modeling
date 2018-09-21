
*** BOILERPLATE ***
	clear all
	set more off, perm
	set maxvar 10000

	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		local j "J:"
		}


*** SETUP TEMPFILES & LOCALS ***
	tempfile ab 

	local inputDir FILEPATH 
	local dataDir  FILEPATH

	local inFiles zika_fatalities.xlsx zika_gbs.xlsx _asymp inf_mod preg 
	

*** CREATE SHELL FILE TO STORE VALUES OF ALPHA & BETA ***
	clear

	set obs `=wordcount("`inFiles'")'
	generate outcome   = ""
	generate alpha     = .
	generate beta      = .
	generate multiplier = .
	save `ab'

*** LOOP THROUGH OUTCOMES, LOAD FILES & USE METAPROP TO ESTIMATE ALPHA & BETA *	
	local i 1

	foreach inFile of local inFiles {
		local outcome = subinstr(subinstr("`inFile'", "zika_", "", .), ".xlsx", "", .)
	
		display "`inFile' = `outcome'"
		
		if "`inFile'" == "zika_fatalities.xlsx"  | "`inFile'" == "zika_gbs.xlsx" {
			import excel "`dataDir'/`inFile'", sheet("Sheet1") firstrow clear
	
			if "`outcome'"=="gbs" keep if case_diagnostics == "RT-PCR or seroneutralisation"
			drop if missing(sample_size)
	
			capture destring sample_size, replace force
			capture destring value_case, replace force
	
			metaprop value_case sample_size, random nograph
			local mean `r(ES)'
			local se   `r(seES)'

			local alpha = `mean' * (`mean' - `mean'^2 - `se'^2) / `se'^2 
			local beta  = `alpha' * (1 - `mean') / `mean' 	
		
			use `ab', clear
			replace outcome = "`outcome'" in `i'
			replace alpha   = `alpha' in `i'
			replace beta    = `beta' in `i'
			save `ab', replace
			}
		
		else if "`inFile'" == "_asymp" {
			use `ab', clear
			replace outcome = "`outcome'" in `i'
			replace multiplier = . in `i'
			save `ab', replace
			}
			
		else if "`inFile'" == "inf_mod" {
			use `ab', clear
			replace outcome = "`outcome'" in `i'
			replace multiplier = 1 in `i'
			save `ab', replace
			}
		else {
			use `ab', clear
			replace outcome = "`outcome'" in `i'
			replace multiplier = 5 in `i'
			save `ab', replace
			}

	
		local ++i
		}	

generate modelable_entity_id = 10401 if outcome=="inf_mod" 	
replace  modelable_entity_id = 10402 if outcome=="gbs"	
replace  modelable_entity_id = 11028 if outcome=="_asymp" 

preserve

use "FILEPATH/prAsymptomatic.dta", clear
drop if n_total == 312 // drop children-only point
collapse (sum) n*, by(location)
metaprop n_symptomatic n_total, random nograph
local mean `r(ES)'
local se   `r(seES)'

local alpha = `mean' * (`mean' - `mean'^2 - `se'^2) / `se'^2 
local beta  = `alpha' * (1 - `mean') / `mean' 

restore

replace alpha = `alpha' if inlist(outcome, "_asymp", "preg")
replace beta  = `beta' if inlist(outcome, "_asymp", "preg")

		
*** SAVE THE FILE ***
	save `inputDir'/outcomes.dta, replace 


	
	
	
	
	
	
	
	
	
	
	
	