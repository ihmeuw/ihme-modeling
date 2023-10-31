// *********************************************************************************************************************************************************************
// Purpose:		Calculate variance in FAO data. Launched by 05_launch_variance.do, parallized by location_id
//
** **************************************************************************
** RUNTIME CONFIGURATION
** **************************************************************************
// Set preferences for STATA
	// Clear memory and set memory and variable limits
		clear
	// Set to run all selected code without pausing
		set more off
	// Set graph output color scheme
		set scheme s1color
	// Remove previous restores
		cap restore, not
		timer clear
		if c(os) == "Unix" {
			global prefix "FILEPATH"
			set odbcmgr unixodbc
			cap log close
			}
		else if c(os) == "Windows" {
			global prefix "FILEPATH"
		}

		local version "GBD2021"
			
	// Define input and output filepaths
	local input_f "/FILEPATH/`version'"
	local output_f "FILEPATH/'version'/variance_adjustment"

	
di "THE LOCATION ID IS `location_id'"
	di "THE INPUT FOLDER IS `input_f'"
	di "THE OUTPUT FOLDER IS `output_f'"

	
*parallelize on location_id;
	
	
	use "/FILEPATH/FAO_all_'version'.dta"
	keep if inlist(location_id, `=subinstr("`location_id'", " ", ",", .)')
	drop if grams_daily_unadj == 0 | grams_daily_unadj == .
    levelsof location_id, local(location_id)

	
	//drop grams_daily
		
foreach l of local location_id {
	preserve
	display "`l'"
	keep if location_id==`l'	

	// Estimation of dispersion using variance of first differences
		levelsof gbd_cause, local(foods)
		gen variance = .
		gen cv = .
		gen sd_resid = .
		
		
		foreach food of local foods {
			// Generate SD of residuals using a lowess regression
				lowess grams_daily_unadj year if gbd_cause == "`food'", bwidth(.5) gen(pred) nograph
				gen diff = grams_daily_unadj - pred
				summ diff

				foreach year of numlist 1960/2016 {
				bysort location_id: egen temp = sd(diff) if inrange(year, `year'-5, `year'+5)
				replace sd_resid = temp if year == `year'
				drop temp
				}

				foreach year of numlist 1960/1965 {
				bysort location_id: egen temp = sd(diff) if inrange(year, 1960, `year'+10-(`year'-1960))
				replace sd_resid = temp if year == `year'
				drop temp
				}

				foreach year of numlist 2010/2016 {
				bysort location_id: egen temp = sd(diff) if inrange(year, `year'-10+(2016-`year'), 2016)
				replace sd_resid = temp if year == `year'
				drop temp
				}

				replace variance = (sd_resid)^2 if gbd_cause == "`food'"
				replace cv = (sqrt(variance)/grams_daily_unadj) if gbd_cause == "`food'"

				drop pred diff
		}
			
	
		
		save "`output_f'/`l'_variance_unadj.dta", replace
		
		restore
}

		
		
	cap log close

