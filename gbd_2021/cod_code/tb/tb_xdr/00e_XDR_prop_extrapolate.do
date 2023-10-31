// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
// Purpose:		Computes XDR-TB proportions at the super-region level and then extrapolates to the final estimation year
// Author:		USERNAME
// Edited:      USERNAME
// Description: Aggregate cases to the super-region level to compute proportion of XDR-TB cases out of all MDR-TB cases
//				Extrpolate and interpolate from 1980 to final estimaation year
// ****************************************************************************************************************************************************
// ****************************************************************************************************************************************************
         
**********************************************************************************************************************
** ESTABLISH SETTINGS FOR SCRIPT
**********************************************************************************************************************		 
		 
// Load settings

	// Clear memory and establish settings
	clear all
	set more off
	set scheme s1color

	// Define focal drives
	if c(os) == "Unix" {
		global prefix "ADDRESS"
		local function_prefix "ADDRESS"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "ADDRESS"
		local function_prefix "ADDRESS"
	}

	// Close any open log file
	cap log close

**********************************************************************************************************************
** STEP 1: COMPUTE PROPORTION OF XDR-TB IN MDR-TB CASES
**********************************************************************************************************************

	// Load in location information
	use "FILEPATH/locations_22.dta", clear
	keep ihme_loc_id region_name super_region_name
	rename ihme_loc_id iso3

	// Save tempfile
	tempfile iso3
	save `iso3', replace

	// Read in MDR data
	insheet using "FILEPATH/mdr_IHME.csv", comma names clear
	tempfile mdr
	save `mdr', replace 

	// Load in testing data
	insheet using "FILEPATH/IHME.csv", comma names clear
	replace country="Czech Republic" if country=="Czechia"
	replace country="Curaao" if country=="Curaçao"

	// Merge in data
	merge 1:1 country using `mdr', keep(3)nogen
	merge 1:1 iso3 using `iso3', keep(3)nogen

	// Compute prop of XDR-TB at super-region level
	drop if mdr_dst==. & xdr==.
	collapse (sum) mdr_dst xdr, by (super_region_name)
	gen XDR_prop=xdr/mdr_dst

	// Save proportions
	save "FILEPATH/xdr_prop_GBD2019.dta", replace

**********************************************************************************************************************
** STEP 2: EXTRAPOLATE FROM 1980 TO 2019
**********************************************************************************************************************

	// Load in proportion
	use "FILEPATH/xdr_prop_GBD2019.dta", clear

	// Generate year to extrapolate to
	gen year_id=2019
		expand 2, gen(new)
		replace year_id=1992 if new==1
		drop new
	replace XDR_prop=0 if year==1992

	// Prepare to project back to 1980
	expand 2 if year==1992, gen(exp)
	replace year=1980 if exp==1
	drop exp

	// interpolate/extrapolate
	egen panel = group(super_region_name)
	tsset panel year
	tsfill, full

	// Clean
	bysort panel: ipolate XDR_prop year, gen(xdr_prop_new) epolate
	bysort panel: replace super_region_name = super_region_name[_n-1] if super_region_name==""
	keep super_region_name year_id xdr_prop_new
	rename xdr_prop_new XDR_prop

	// Output proportions
	save "FILEPATH/xdr_prop_GBD2019.dta", replace



