//housekeeping
clear all
cap restore, not
cap log close
set maxvar 20000

// set j
if c(os)=="Unix" global j "FILEPATH"
else global j "FILEPATH"

// set relevant locals
local data_folder		"filepath"
local output_folder	 	"filepath"


***NON-EXCLUSIVE BREASTFEEDING***
foreach loc in "CHN" "MEX" "GBR" 				{
	
	use "`data_folder'/`loc'/data/estimated_nonexclBF", clear
	keep iso3 year prov_*
	drop *mean *upper *lower

	forvalues n = 0/999 			{
		
		rename prov_EBF_`n' exp_cat4_`n'
		rename prov_predBF_`n' exp_cat3_`n'
		rename prov_partBF_`n' exp_cat2_`n'
		
		gen exp_cat1_`n' = 1 - prov_ABF_`n'
		gen total_`n' = (exp_cat1_`n'+exp_cat2_`n'+exp_cat3_`n'+exp_cat4_`n')
	
		forvalues m = 1/4 	{
		quietly replace exp_cat`m'_`n' = exp_cat`m'_`n' / total_`n'
		
		}
				}
		
	keep iso3 year exp_cat*	
	gen sex = "B"
	tempfile `loc'
	save ``loc'', replace
	
	}
	
**append subnational draws to the national draws & save file 
insheet using "`output_folder'/all_cats_lnnto5.csv", comma clear
append using `CHN' `MEX' `GBR'
outsheet using "`output_folder'/all_cats_lnnto5_w_subnational.csv", comma replace

********

**DISCONTINUED BREASTFEEDING**
foreach loc in "CHN" "MEX" "GBR"	{
use "`data_folder'/`loc'/FILEPATH/estimated_discontinuedBF.dta", clear

	local ages "6to11 12to23" 
	foreach age of local ages {
		
		preserve
		keep iso3 year prov_`age'_*

			forvalues n = 0/999 {
		
			rename prov_`age'_`n' exp_cat2_`n'
			gen exp_cat1_`n' = 1 - exp_cat2_`n'
			order exp_cat1_`n' exp_cat2_`n', after(year)
	}
	
		keep iso3 year exp_cat*	
		gen sex = "B"
	
		tempfile `loc'_`age'
		save ``loc'_`age'', replace 
		restore
	
		}
			}
			
**append subnational draws to the national draws & save file 
use `CHN_6to11', clear
append using `MEX_6to11'
append using `GBR_6to11'
outsheet using "`output_folder'/all_cats_6to11_subnational.csv", comma replace

use `CHN_12to23', clear
append using `MEX_12to23'
append using `GBR_12to23'
outsheet using "`output_folder'/all_cats_12to23_subnational.csv", comma replace

***********************************
**********end of code*****************
************************************
