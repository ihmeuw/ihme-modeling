import delimited FILEPATH, clear 
drop if inlist(countr, "Brazil", "China", "Indonesia", "Japan", "Mexico", "United States of America")
gen location_name = countr

replace location_name = "The Bahamas" if location_name=="Bahamas"
replace location_name = "Brunei" if location_name=="Brunei Darussalam"
replace location_name = "Federated States of Micronesia" if location_name=="Micronesia, Federated States of"
replace location_name = "Laos" if location_name=="Lao PDR"
replace location_name = "Saint Vincent and the Grenadines" if location_name=="St. Vincent & the Grenadines"
replace location_name = "Saint Kitts and Nevis" if location_name=="St. Kitts & Nevis"


tempfile data1 data2
save `data1'


import delimited FILEPATH, clear 
keep if inlist(countr, "Brazil", "China", "Indonesia", "Japan", "Mexico", "United States of America")
merge 1:1 id using "FILEPATH", assert(3) nogenerate
keep year den? n_types loc_id
rename loc_id location_id
save `data2'

get_location_metadata, location_set_id(35) clear

preserve
keep location_id location_name location_ascii_name location_name_short *region* ihme_loc_id 
tempfile locMeta1 locMeta2
save `locMeta2'

restore
keep if level==3
keep location_id location_name location_ascii_name location_name_short *region* ihme_loc_id 
save `locMeta1'




merge 1:m location_name using `data1', keep(2 3) 


replace region = "Americas" if location_name=="French Guiana"

generate fillRegion = region_name
replace  fillRegion = "Caribbean" if region=="Americas"
replace  fillRegion = "Oceania" if region=="Asia"
replace  fillRegion = "Eastern Sub-Saharan Africa" if region=="Africa"

foreach region in "Caribbean" "Oceania" "Eastern Sub-Saharan Africa" {
	foreach var of varlist *region* {
		quietly {
			levelsof `var' if region_name=="`region'", local(temp) clean
			capture replace `var' = "`temp'" if fillRegion=="`region'" & missing(`var')
			if _rc!=0 replace `var' = `temp' if fillRegion=="`region'" & missing(`var')
			}
		}
	}

egen group = group(location_name)
replace location_id = 999000 + group if missing(location_id)
save `data1', replace

use `locMeta2', clear
merge 1:m location_id using `data2', assert(1 3) keep(3) nogenerate

append using `data1'



keep location_id location_name *region_* ihme_loc_id year den? n_types
rename year year_id

save FILEPATH, replace
	
