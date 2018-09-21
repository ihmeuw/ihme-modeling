// File Name: gen_prev_newcat.do
// File Purpose: combine output from proportion models to split each source type group by HWT use 


// Additional Comments: 
//Set directories
	if c(os) == "Unix" {
		global j "FILEPATH"
		set more off
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global j "FILEPATH"
	}

//Housekeeping
clear all 
set more off
set maxvar 32767

*******toggle version*******
local version 1 // initial pre-review week run
local version 2 //
local version 3 // first submission run through
local version 4 // added NSSO/BRA data-second submission run

//Set relevant locals
local input_folder		"filepath"
local output_folder		"filepath"
local graphloc			"filepath"
adopath + ""

local date "06242017"

// Prep GPR draws of exposure by access to piped or improved water sources

	// improved water
	import delimited "`input_folder'/wash_water_imp", clear
	keep location_id year_id age_group_id draw_*
	forvalues n = 0/999 {
		rename draw_`n' iimp_mean`n'
	}
	tempfile imp_water
	save `imp_water', replace
	
	// piped water
	import delimited "`input_folder'/wash_water_piped", clear
	keep location_id year_id draw_*
	merge 1:1 location_id year_id using `imp_water', keep(1 3) nogen
	
	// Calculate unimproved population
	forvalues n = 0/999 {
		rename draw_`n' ipiped_mean`n'
		gen iunimp_mean`n' = 1 - (iimp_mean`n' + ipiped_mean`n')
	}

	tempfile water_cats
	save `water_cats', replace

// Household water treatment exposures prep
local models "itreat_imp itreat_piped itreat_unimp tr_imp tr_piped tr_unimp"
foreach model of local models {
	import delimited "`input_folder'/wash_water_`model'", clear
	sort location_id year_id
	keep location_id year_id draw_*
	
		forvalues n = 0/999 {
			rename draw_`n' prop_`model'`n'
			}
	
	tempfile `model'
	save ``model'', replace
} 

// merge all draws
use `water_cats', clear
local sources imp unimp piped
	foreach source of local sources {
	merge m:1 location_id year using `itreat_`source'' , keepusing(prop_itreat_`source'*) nogen keep(1 3)
	merge m:1 location_id year using `tr_`source'', keepusing(prop_tr_`source'*) nogen keep(1 3)

		forvalues d = 0/999 {
			gen prop_untr_`source'`d' = 1 - (prop_tr_`source'`d')
			rename prop_tr_`source'`d' prop_any_treat_`source'`d'
			gen prop_treat2_`source'`d' = prop_any_treat_`source'`d' - prop_itreat_`source'`d'
		}
	}

tempfile compiled_draws
save `compiled_draws', replace
	
**********************************************
*******Generate estimates for final categories**********
**********************************************
use `compiled_draws', clear
local sources imp unimp piped
foreach source of local sources {
	
	forvalues n = 0/999 {
	
	gen prev_`source'_t_`n' = prop_itreat_`source'`n' * i`source'_mean`n'
	gen prev_`source'_t2_`n' = prop_treat2_`source'`n'* i`source'_mean`n'
	gen prev_`source'_untr_`n' = prop_untr_`source'`n'* i`source'_mean`n'
	
	}
}

keep location_id year_id prev_*
tempfile all_prev
save `all_prev', replace

// Prep the country codes file
	get_location_metadata, location_set_id(22) clear
	keep if level >= 3
	keep location_id location_name super_region_id super_region_name region_name
	tempfile country_info
	save `country_info', replace

// Merge on location info in order to bin all high income countries
	use `all_prev', clear
	merge m:1 location_id using `country_info', nogen keep(1 3)
	
// for now - replace negative draws
local sources imp unimp piped
foreach source of local sources {

	local trx untr t t2
	foreach t of local trx {
	
	forvalues n = 0/999 {

		replace prev_`source'_`t'_`n' = 0.0001 if prev_`source'_`t'_`n' < 0
		replace prev_`source'_`t'_`n' = .9998 if prev_`source'_`t'_`n' >= 1
		
				}
			}
		}


//Squeeze in categories to make sure they add up to 1
forvalues n = 0/999 {
	egen prev_total_`n' = rowtotal(*prev*_`n')
	replace prev_piped_untr_`n' = prev_piped_untr_`n' / prev_total_`n'
	replace prev_piped_t2_`n' = prev_piped_t2_`n' / prev_total_`n'
	replace prev_imp_t_`n' = prev_imp_t_`n' / prev_total_`n'
	replace prev_imp_t2_`n' = prev_imp_t2_`n' / prev_total_`n'
	replace prev_imp_untr_`n' = prev_imp_untr_`n' / prev_total_`n'
	replace prev_unimp_t_`n' = prev_unimp_t_`n' / prev_total_`n'
	replace prev_unimp_t2_`n' = prev_unimp_t2_`n' / prev_total_`n'
	replace prev_unimp_untr_`n' = prev_unimp_untr_`n' / prev_total_`n'
	replace prev_piped_t_`n' = prev_piped_t_`n' / prev_total_`n'
	}
	
	drop *total*
	tempfile check
	save `check', replace

// Implement the fecal proportion split
import delimited "`input_folder'/prop_fecal.csv", clear
forvalues n = 0/999{
	rename draw_`n' fecal_`n'
	replace fecal_`n' = .001 if fecal_`n' < 0
	replace fecal_`n' = .999 if fecal_`n' > 1
}

merge 1:1 location_id year_id using `check', nogen

// Use fecal proportion to split into basic piped
forvalues n = 0/999{
	gen prev_bas_piped_untr_`n' = prev_piped_untr_`n' * fecal_`n'
	gen prev_bas_piped_t2_`n' = prev_piped_t2_`n' * fecal_`n'
	gen prev_bas_piped_t_`n' = prev_piped_t_`n' * fecal_`n'
}
drop fecal_*

// Subtract total piped from basic piped to calculate high quality piped
//gen prev_piped_untr_hq_`n' = prev_piped_untr_`n' - prev_bas_piped_untr_`n'
	//gen prev_piped_t2_hq_`n' = prev_piped_t2_`n' - prev_bas_piped_t2_`n'
forvalues n = 0/999 {
	gen prev_piped_t_hq_`n' = (prev_piped_t_`n' - prev_bas_piped_t_`n') + (prev_piped_t2_`n' - prev_bas_piped_t2_`n') + (prev_piped_untr_`n' - prev_bas_piped_untr_`n')
	gen prev_piped_untr_hq_`n' = 0
	gen prev_piped_t2_hq_`n' = 0
}

**save data**
save "`graphloc'/allcat_prev_water_`date'.dta", replace
// Save each category separately in prep for save_results
local exposures imp_t imp_t2 imp_untr unimp_t unimp_t2 unimp_untr bas_piped_t bas_piped_t2 bas_piped_untr piped_untr_hq piped_t2_hq
	foreach exposure of local exposures {
		preserve
			keep location_id year_id prev_`exposure'_*
			if "`exposure'" == "piped_t2" {
				drop prev_piped_t2_hq_*
			}
			if "`exposure'" == "piped_untr" {
				drop prev_piped_untr_hq_*
			}
			gen age_group_id = 22
			save "`output_folder'/`exposure'", replace

		restore
	}

//End of Code//
