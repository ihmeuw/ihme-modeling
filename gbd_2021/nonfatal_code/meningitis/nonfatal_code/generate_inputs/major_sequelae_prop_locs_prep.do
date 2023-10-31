// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************
// Purpose:		Merge location_ids as iso3 codes to major_sequalae studies from <NAME>
// Author:		USERNAME
// Last updated:	12/8/2015
// Description:	This code only needs to be run once ever (possibly once per year such to change file name, and for general understanding of code)
//  			unless more studies with major sequalae proportions are being added to the regression 

// *********************************************************************************************************************************************************************
// *********************************************************************************************************************************************************************


// prep stata
	clear all
	set more off
	set mem 2g
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
	}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
	}
	adopath + "FILEPATH"

// run code; change file paths as appropriate to modelers scratch 

	import excel "FILEPATH", firstrow clear
	keep if location_type == "country"
	drop location_name location_type
	tempfile iso3_ids
	save `iso3_ids'
	
	import delimited "FILEPATH", clear
	rename countryiso3code iso3

	merge m:1 iso3 using `iso3_ids', keep(3) nogen

	//update gbd(year) for year of modeling 
	export delimited "FILEPATH", replace

