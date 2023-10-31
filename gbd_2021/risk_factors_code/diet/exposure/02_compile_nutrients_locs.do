*****************************************************************************************************************************
**  Description: This script compiles all of the location level nutrient composition data into one csv
**
****************************************************************************************************************************
**** Set preferences for STATA
	** Clear memory and set memory and variable limits
		clear all
		set maxvar 32000
	** Set to run all selected code without pausing
		set more off
	** Remove previous restores
		cap restore, not
		if c(os) == "Unix" {
			global prefix "FILEPATH"
			set odbcmgr unixodbc
		}
		else if c(os) == "Windows" {
			global prefix "FILEPATH"
		}

**call in the "version" that is named in the parent script 

args version version_description gbd_round
local output_f "FILEPATH/`version'/"
capture confirm file `output_f'
if _rc mkdir `output_f'

file open myfile using `output_f'version_description.txt, write replace
file write myfile "`version_description'"
file close myfile

use "FILEPATH", clear

	**create a local of each country found in the above dataset
	levelsof location_id, local(countrys)


local count = 0

foreach country of local countrys {

	use "FILEPATH", clear

		replace countries = subinstr(countries, "_", " ", .)
		replace countries = subinstr(countries, "0", ";", .)
		replace countries = "Bolivia (Plurinational State of)" if countries == "Bolivia"
		replace countries = "Côte d'Ivoire" if countries == "Ivory Coast"
		replace countries = "Venezuela (Bolivarian Republic of)" if countries == "Venezuela"
		replace countries = "Democratic People's Republic of Korea" if countries == "DPRK"
		replace countries = "Sudan (former)" if countries == "Sudan"
		replace countries = "Iran (Islamic Republic of)" if countries == "Iran"
		replace countries = "Lao People's Democratic Republic of" if countries == "Laos"
 
		cap drop _*

		**Create a list of candidates for whole grains
		preserve
		gen whole_grain_ratio = est_fiber_g / est_total_carbohydrates_diff
		gen whole_grain_flag = 1 if whole_grain_ratio >= .1
		keep if whole_grain_flag == 1
			keep product_codes products ele_codes ele_name ndb_no comn shrt_desc whole_grain*
		    duplicates drop
		export delimited "FILEPATH", replace
		restore

		drop product_codes products ele_codes country_codes data old_data ele_name population ndb_no comn refuse shrt_desc
		**keep just a single, summed estimate for each country year
		keep countries location_id year *sum
		duplicates drop countries year, force
		
		compress

	if `count' == 0 {
		*instead, just save a collapsed version (just a country-year estimate) instead of by item
		save "FILEPATH/compiled_nutrients_`version'.dta", replace
		local count = `count' + 1
	}

	if `count' > 0 {
		tempfile country_year_data
		save `country_year_data', replace

		use "FILEPATH/compiled_nutrients_`version'.dta", clear
			append using `country_year_data'

		local count = `count' + 1

		*instead, just save a collapsed version (just a country-year estimate) instead of by item
		save "FILEPATH/compiled_nutrients_`version'.dta", replace
	}

}
