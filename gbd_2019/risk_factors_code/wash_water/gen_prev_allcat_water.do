// File Purpose: combine output from proportion models to split each source type group by HWT use 

//Housekeeping
clear all 
set more off
set maxvar 32767

*******toggle version*******
local run "run1" // first run
local decomp_step "decomp_step4" // change as needed
local c_date= c(current_date)
local date = subinstr("`c_date'", " " , "_", .)

//Set relevant locals
local input_folder		"FILEPATH"
local output_folder		"FILEPATH"
local graphloc			"FILEPATH"

// Prep GPR draws of exposure by access to piped or improved water sources

	// improved water proportion
	import delimited "FILEPATH", clear
	keep location_id year_id age_group_id draw_*
	forvalues n = 0/999 {
		rename draw_`n' iimp_prop`n'
	}
	tempfile imp_water
	save `imp_water', replace
	
	// piped water
	import delimited "FILEPATH", clear
	keep location_id year_id draw_*
	merge 1:1 location_id year_id using `imp_water', keep(1 3) nogen
	
	// calculate improved prevalence by multiplying times 1-piped
	forvalues n = 0/999 {
		rename draw_`n' ipiped_mean`n'
		gen iimp_mean`n' = iimp_prop`n' * (1 - ipiped_mean`n')
	}

	// Calculate unimproved population
	forvalues n = 0/999 {
		gen iunimp_mean`n' = 1 - (iimp_mean`n' + ipiped_mean`n')
	}

	tempfile water_source
	save `water_source', replace

	// Household water treatment exposures prep
	import delimited "FILEPATH", clear
	keep location_id year_id draw_*
	forvalues n = 0/999 {
		rename draw_`n' prev_no_treat`n'
	}
	tempfile no_treat
	save `no_treat', replace

	// boil/filter proportion
	import delimited "FILEPATH", clear
	keep location_id year_id draw_*
	merge 1:1 location_id year_id using `no_treat', keep(1 3) nogen

	// calculate boil/filter prevalence by multiplying times 1-no treatment
	forvalues n = 0/999 {
		rename draw_`n' prop_boil`n'
		gen prev_boil`n' = prop_boil`n' * (1 - prev_no_treat`n')
	}

	//Calculate remaining category of solar/chlorine treatment
	forvalues n = 0/999 {
		gen prev_solar`n' = 1 - (prev_boil`n' + prev_no_treat`n')
	}

	
	// merge source and treatment draws together
	merge 1:1 location_id year_id using `water_source', keep(1 3) nogen
	tempfile compiled_draws
	save `compiled_draws', replace
	
********************************************************
*******Generate estimates for final categories**********
********************************************************
use `compiled_draws', clear
local sources imp unimp piped
foreach source of local sources {
	
	forvalues n = 0/999 {
	
	gen prev_`source'_t_`n' = prev_boil`n' * i`source'_mean`n'
	gen prev_`source'_t2_`n' = prev_solar`n'* i`source'_mean`n'
	gen prev_`source'_untr_`n' = prev_no_treat`n'* i`source'_mean`n'
	
	}
}

keep location_id year_id prev_*
tempfile all_prev
save `all_prev', replace

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
import delimited "FILEPATH", clear
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

// Subtract basic piped from total piped to calculate high quality piped
forvalues n = 0/999 {
	gen prev_piped_t_hq_`n' = (prev_piped_t_`n' - prev_bas_piped_t_`n') + (prev_piped_t2_`n' - prev_bas_piped_t2_`n') + (prev_piped_untr_`n' - prev_bas_piped_untr_`n')
	gen prev_piped_untr_hq_`n' = 0
	gen prev_piped_t2_hq_`n' = 0
}

**save data**
save "FILEPATH", replace

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
			save "FILEPATH", replace

		restore
	}

//End of Code//
