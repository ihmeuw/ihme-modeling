// ***************************************************************************
// ***************************************************************************
// Purpose:		Compile the calculated variances                             //
** **************************************************************************
** RUNTIME CONFIGURATION
** **************************************************************************
// Set preferences for STATA
// Clear memory and set memory and variable limits
	clear
// Set to run all selected code without pausing
	set more off
// Remove previous restores
	cap restore, not
	if c(os) == "Unix" {
		local prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		local prefix "FILEPATH"
	}
	

	
local version "GBD2021"

adopath + "FILEPATH/stata"

local input_f "FILEPATH/'version'/variance_adjustment"
capture confirm file `input_f'
if _rc mkdir `input_f'

local output_f "FILEPATH/'version'/variance_adjustment/compilation"
capture confirm file `output_f'
if _rc mkdir `output_f'


//get locations 
run "FILEPATH/get_location_metadata.ado"
get_location_metadata, location_set_id(22) gbd_round_id(7) clear
keep if is_estimate==1
keep location_id region_name ihme_loc_id
	tempfile full_iso
	save `full_iso', replace

// Append files from CBLI estimation analysis
	local var_files : dir "`input_f'/" files "*variance_unadj.dta", respectcase
	local counter = 1
	
	foreach f in `var_files' {
		di in red "`counter': `f'"
		if `counter' == 1 use "`input_f'/`f'", clear
		else append using "`input_f'/`f'"
		local counter = `counter' + 1
	}

// add on region variable
	merge m:1 location_id using `full_iso'
		keep if _merge == 3
		drop _merge
// Final cleanup
	cap rename countryname location_name
	keep ihme_loc_id location_id location_name year gbd_cause grams_daily* total_calories ihme_risk variance cv sd_resid nid region_name
	sort ihme_loc_id year
	order ihme_loc_id year grams_daily variance cv sd_resid nid
	
// create a variable populated by the regional max for each country
	egen region_var_max = max(variance), by(region_name gbd_cause)

// create a flag variable if there is no variation in the data within a country
	sort location_id gbd_cause grams_daily_unadj
	by location_id gbd_cause grams_daily_unadj: gen duplicate = cond(_N==1,0,_N)
		quietly by location_id gbd_cause: gen counter = _N
		gen flag = 1 if duplicate == counter
			drop counter

// replace those flagged (no data variation) with the regional max
	replace variance = region_var_max if flag == 1
		drop region_var_max flag region_name
	
	compress
	
		save "`output_f'/FAO_sales_raw_data_variance_unadj_GBD2021.dta", replace

	