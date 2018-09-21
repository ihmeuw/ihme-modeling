//NAME 
// GBD 2015 Fatal Discontinuities
// Format natural and technological diaster events that have been researched online  


clear
set more off
pause on

if c(os) == "Windows" {
	global prefix ""
}
else {
	global prefix ""
	set odbcmgr unixodbc
}

global datadir "FILEPATH"

do "FILEPATH"

// GBD2015 DB for locations
* local dsn "prodcod"
local location_ver 153

do "FILEPATH"
create_connection_string, server("modeling-cod-db") database("shared")
local conn_string `r(conn_string)'

	
	
	** Get location metadata
clear
gen iso3 = ""
tempfile codes_sub
save `codes_sub', replace
foreach sub in CHN GBR MEX IND IDN BRA JPN SAU SWE USA KEN ZAF {
if inlist("`sub'", "JPN") local admin = 4
else if inlist("`sub'", "IND") local admin "4, 12"
else local admin = 3
odbc load, `conn_string' exec("SELECT location_id, location_name, region_name, ihme_loc_id FROM shared.location_hierarchy_history WHERE ihme_loc_id LIKE '`sub'%' AND location_type_id IN (`admin') AND location_set_version_id = `location_ver'") clear
replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
rename (location_name region_name ihme_loc_id) (location gbd_region iso3)
if "`sub'" == "IND" {
replace location = subinstr(location, "?", "a", .) if regexm(location, "Arun\?chal|Bih\?r|Gujar\?t|Hary\?na|Karn\?taka|Mah\?r\?shtra|Megh\?laya|N\?g\?land|R\?jasth\?n|N\?du")
replace location = subinstr(location, "?", "i", .) if regexm(location, "Chhatt\?sgarh|Kashm\?r")
}
else if "`sub'" == "JPN" {
replace location = subinstr(location, "?", "o", .)
replace location = subinstr(location, "Ô", "O", .)
replace location = proper(location)
}
append using `codes_sub'
save `codes_sub', replace
}
		// Drop Distrito Federal to keep location unique. There are Distrito Federal in both Mexico and Brazil, thankfully no Brazil DF in data.
drop if location == "Distrito Federal" & iso3 == "BRA"	
save `codes_sub', replace
		
	** Nationals
odbc load, exec("SELECT location_id, location_name, region_name, ihme_loc_id, location_type_id FROM shared.location_hierarchy_history WHERE location_type_id IN (2,3,4,8,12) AND location_set_version_id = `location_ver'") `conn_string' clear
replace ihme_loc_id = substr(ihme_loc_id, 1, 3)
drop if regexm(location_name, "County") & regexm(region_name, "High-income North America")
* drop if location_id == 891	// District of Columbia admin2
rename (location_name region_name ihme_loc_id) (countryname gbd_region iso3)
// Drop unnecessary codes
**drop if inlist(iso3, "BRA", "CHN", "GBR", "IND", "JPN", "KEN") | inlist(iso3, "MEX", "SWE", "SAU", "USA", "ZAF")
drop if iso3 == "USA" & (location_type_id == 3 | location_type_id == 4)
drop if location_type_id == 3 & inlist(iso3, "JPN", "BRA", "MEX", "IND")
tempfile codes
save `codes', replace
		
** subnational population weights
use "FILEPATH", clear
quietly {
rename year_id year
keep if sex == "both" & age_group_id == 22
gen keep = 0
foreach iso in BRA CHN GBR IND IDN JPN KEN MEX SWE SAU USA ZAF {
replace keep = 1 if regexm(ihme_loc_id, "`iso'")
}
replace keep = 0 if ihme_loc_id == "CHN_44533"
drop if keep == 0
drop sex* age* parent_id keep source location_id
drop if length(ihme_loc_id)==3
rename pop pop_
** Drop India state, only use urban rural
gen iso_length = length(ihme_loc_id)
drop if regexm(ihme_loc_id, "IND_") & iso_length == 8
drop iso_length
** Drop England
drop if ihme_loc_id == "GBR_4749"
levelsof ihme_loc_id, local(ids) clean
bysort ihme_loc_id : assert location_name == location_name[1]
foreach id of local ids {
levelsof location_name if ihme_loc_id == "`id'", local(`id'_name) clean
}
drop location_name level *egion_id
reshape wide pop_, i(year) j(ihme_loc_id) string
foreach iso in BRA CHN GBR IND IDN JPN KEN MEX SWE SAU USA ZAF {
looUSER pop_`iso'
local varlist `r(varlist)'
egen pop_`iso'_tot = rowtotal(pop_`iso'_*)
foreach var of local varlist {
local stub = subinstr("`var'", "pop_", "", 1)
gen weight_`stub' = `var' / pop_`iso'_tot
}
}
drop *tot pop_*
reshape long weight_, i(year) j(ihme_loc_id) string
drop if weight_ == .
split ihme_loc_id, parse("_")
rename ihme_loc_id1 iso3
rename ihme_loc_id2 location_id
rename weight_ weight
drop if location_id == "tot"
destring location_id, replace
gen gbd_country_iso3 = iso3
gen location_name = ""
foreach id of local ids {
replace location_name = "``id'_name'" if ihme_loc_id == "`id'"
}
			
			
expand 25
gen type = ""
bysort year location_id : gen indic = _n
replace type = "Air" if indic == 1
replace type = "Chemical spill" if indic == 2
replace type = "Cold wave" if indic == 3
replace type = "Collapse" if indic == 4
replace type = "Drought" if indic == 5
replace type = "Earthquake" if indic == 6
replace type = "Explosion" if indic == 7
replace type = "Famine" if indic == 8
replace type = "Fire" if indic == 9
replace type = "Flood" if indic == 10
replace type = "Gas leak" if indic == 11
replace type = "Heat wave" if indic == 12
replace type = "Other" if indic == 13
replace type = "Other Geophysical" if indic == 14
replace type = "Other hydrological" if indic == 15
replace type = "Poisoning" if indic == 16
replace type = "Rail" if indic == 17
replace type = "Road" if indic == 18
replace type = "Volcanic activity" if indic == 19
replace type = "Water" if indic == 20
replace type = "Wildfire" if indic == 21
replace type = "legal intervention" if indic == 22
replace type = "storm" if indic == 23
replace type = "terrorism" if indic == 24
replace type = "war" if indic == 25
drop indic
isid year location_id type

* gen _merge = 1

tempfile subnatl_popweight
save `subnatl_popweight', replace
}
			
		** Populations
			* odbc load, `conn_string' clear exec("SELECT year_id, location_id, sex_id, age_group_id, mean_pop, age_group_name_short FROM mortality.output JOIN mortality.output_version USING (output_version_id) JOIN shared.age_group USING (age_group_id) WHERE is_best = 1")
get_population, location_id("-1") age_group_id("22") sex_id("3") year_id("-1") clear
rename population mean_pop
keep year_id location_id mean_pop
rename year_id year
tempfile population
save `population', replace

	
	** Online supplemental data, 2016 

import delimited using "FILEPATH", clear
keep nid national subnational finest* year low best high specific
rename specific cause
gen source = "Online supplement 2016"
rename national countryname
rename subnational location 
* replace countryname = subnational if subnational != ""
replace countryname = "United States" if countryname == "USA"
replace location = "Uttar PrUSER, Rural" if location == "Uttar PrUSER"
rename best numkilled
drop if regexm(finestgbdcause, "war")
replace numkilled = low if numkilled == . & high == .
gen acause = finestgbdcause
replace acause = "inj_disaster" if regexm(finestgbdcause, "inj_disaster")

merge m:1 countryname using `codes', nogen keep(3) assert(2 3) keepusing(iso3 location_id)

drop finestgbdcause finest

tempfile 2016_supp
save `2016_supp', replace


	** Online supplemental data
	// Technological disasters
import delimited "FILEPATH", clear varnames(1)
keep if nid != .
gen year = 2015
cap drop source
gen source = "Online supplement 2015"
rename country countryname
rename dis_subtype type
drop if inlist(type, "", "--", "Radiation")
rename iso iso3
rename death_best numkilled

keep nid iso3 countryname location type numkilled year source

merge m:1 countryname using `codes', nogen keep(3) assert(2 3) keepusing(iso3 location_id)

tempfile tech_supp
save `tech_supp', replace
		
	// Natural disasters
import delimited "FILEPATH", clear varnames(1)
keep if nid != .
cap drop source
gen source = "Online supplement 2015"
rename country countryname
rename event type
rename death_best numkilled
replace type = "storm" if regexm(type, "Typhoon")
replace type = "Landslide" if regexm(type, "[L|l]andslide")
replace type = "Flood" if regexm(type, "Severe weather") | type == "Floods"

drop if type == "cholera outbreak"

expand 2 if countryname == "Afghanistan, Pakistan", gen(new)
replace numkilled = 115 if new == 1 & countryname == "Afghanistan, Pakistan"
replace countryname = "Afghanistan" if new == 1 & countryname == "Afghanistan, Pakistan"
replace numkilled = numkilled - 115 if new == 0 & countryname == "Afghanistan, Pakistan"
replace countryname = "Pakistan" if new == 0 & countryname == "Afghanistan, Pakistan"
drop new

keep nid countryname location type numkilled deaths_* year source

merge m:1 countryname using `codes', nogen keep(3) assert(2 3) keepusing(iso3 location_id)


		
append using `tech_supp'
** add in 2016 data
append using `2016_supp'
replace type = cause if type == ""
replace deaths_low = low if low != .
replace deaths_high = high if high != .
drop cause low high

// Format subnational location_ids
tempfile pre_sub_split
save `pre_sub_split', replace
keep if inlist(iso3, "CHN", "MEX", "GBR", "IND", "IDN", "BRA", "JPN") | inlist(iso3,"SAU","SWE","USA","KEN","ZAF")
replace location = "Guangdong" if location == "Shenzhen"
replace location = "Assam" if location == "Assam state"
replace location = "Tamil Nadu" if location == "" & iso3 == "IND" & nid == 237646
expand 2 if location == "Tamil Nadu, Andhra PrUSER", gen(new)
replace numkilled = numkilled / 2 if location == "Tamil Nadu, Andhra PrUSER" & new == 0
replace deaths_low = deaths_low / 2 if location == "Tamil Nadu, Andhra PrUSER" & new == 0
replace deaths_high = deaths_high / 2 if location == "Tamil Nadu, Andhra PrUSER" & new == 0
replace location = "Tamil Nadu" if location == "Tamil Nadu, Andhra PrUSER" & new == 0
replace numkilled = numkilled / 2 if location == "Tamil Nadu, Andhra PrUSER" & new == 1
replace deaths_low = deaths_low / 2 if location == "Tamil Nadu, Andhra PrUSER" & new == 1
replace deaths_high = deaths_high / 2 if location == "Tamil Nadu, Andhra PrUSER" & new == 1
replace location = "Andhra PrUSER" if location == "Tamil Nadu, Andhra PrUSER" & new == 1
replace location = "Makkah" if location == "Mina"
replace location = location + ", Rural" if year >= 2015 & type == "Flood" & iso3 == "IND"
replace location = location + ", Urban" if year >= 2015 & type == "Explosion" & iso3 == "IND"
		
drop iso3 location_id

merge m:1 location using `codes_sub'
	drop if _m == 2

tempfile sub
save `sub', replace



* keep if _m == 1
		
replace iso3 = "IND" if countryname == "India" & location == ""
****** related to 2016 supplemental data...  12/27/16 *************
replace iso3 = "IND_4873" if location == "Uttar PrUSER, Rural"
replace iso3 = "CHN" if countryname == "China" & location == ""
replace iso3 = "CHN_507" if location == "Jiangxi"
replace iso3 = "IDN" if countryname == "Indonesia" & location == ""
replace iso3 = "IDN_4709" if location == "Aceh"
replace iso3 = "JPN" if countryname == "Japan" & location == ""
replace iso3 = "USA" if countryname == "United States" & location == ""
replace iso3 = "MEX" if countryname == "Mexico" & location == ""
replace iso3 = "BRA" if countryname == "Brazil" & location == ""
replace iso3 = "KEN_35647" if location == "Nakuru"
replace iso3 = "IND_4841" if location == "Andhra PrUSER, Rural"
replace iso3 = "IND_4859" if location == "Madhya PrUSER, Urban"
replace iso3 = "IND_4843" if location == "Assam, Rural"
replace iso3 = "IND_43937" if location == "Tamil Nadu, Rural"
replace iso3 = "SAU_44546" if location == "Makkah"
replace iso3 = "CHN_496" if location == "Guangdong"

			******************************************************************8*
preserve
* split subnational-bearing countries that aren't already specified
keep if _m == 1
assert location == ""
			
assert iso3 != ""
collapse (sum) numkilled deaths_low deaths_high, by(_merge iso3 year source nid) fast
gen type = "Disaster"
drop _merge
merge 1:m iso3 year using `subnatl_popweight'
keep if _m == 3
duplicates drop
rename location_name location
drop _merge

replace numkilled = numkilled * weight
replace deaths_low = deaths_low * weight
replace deaths_high = deaths_high * weight

tempfile split_subs
save `split_subs', replace

			**************************
* levelsof nid, local(nids)
* gen keep = 0
* tempfile subnatl_estimate
* save `subnatl_estimate', replace
* rename iso3 ihme_loc_id
* rename _merge merge_0
* foreach nid of local nids {
* keep if nid == `nid'
* replace keep = 1
* cap drop weight location_id location_name
* merge 1:m ihme_loc_id year using `subnatl_popweight'
* rename _merge merge_nid_`nid'
* **, keepusing(weight location_id location_name) keep(3) assert(2 3) nogen
* assert weight != .

* append using `subnatl_estimate'
* drop if keep != 1 & nid == `nid'
* save `subnatl_estimate', replace
* }

* replace numkilled = numkilled * weight
* replace deaths_low = deaths_low * weight
* replace deaths_high = deaths_high * weight
* rename location_name location
* * gen countryname = "India"

** now do countries with subnat-specific locations, just need their loc_id...
* clear
* gen extra_var = 1
* tempfile direct_subs
* save `direct_subs', replace

restore
keep if _m == 3
collapse (sum) numkilled deaths_low deaths_high, by(_merge iso3 year source nid) fast
rename iso3 ihme_loc_id
gen type = "Disaster"
drop _merge
gen len = strlen(ihme_loc_id)
gen location_id = substr(ihme_loc_id,5, len)
destring location_id, replace

append using `split_subs'

* tempfile pre_merge
* save `pre_merge', replace

* levelsof nid, local(nids)
* foreach nid of local nids {
* use `pre_merge', clear
* keep if nid == `nid'
* merge 1:m ihme_loc_id year using `subnatl_popweight', keepusing(location_id)
* duplicates drop
* keep if _m == 3
* drop _merge
* * rename location_name location
* append using `direct_subs'
* save `direct_subs', replace
* }

tempfile subnatl_estimate
save `subnatl_estimate', replace

use `pre_sub_split', clear
drop if inlist(iso3, "CHN", "MEX", "GBR", "IDN", "IND", "BRA", "JPN") | inlist(iso3,"SAU","SWE","USA","KEN","ZAF")

append using `subnatl_estimate'

* append using `sub'
* drop if _m == 1

* drop _m

merge m:1 location_id year using `population', keepusing(mean_pop) // , nogen keep(3) assert(2 3) 
keep if _m == 3 
assert inlist(_m, 2, 3)
drop _m

rename type cause
collapse (sum) numkilled deaths_low deaths_high, by(countryname iso3 location_id year cause source nid mean_pop) fast

gen l_disaster_rate = deaths_low / mean_pop
gen u_disaster_rate = deaths_high / mean_pop
replace l_disaster_rate = . if l_disaster_rate == 0
replace u_disaster_rate = . if u_disaster_rate == 0

keep countryname iso3 location_id year cause source nid numkilled l_disaster_rate u_disaster_rate
replace iso3 = "CHN" if location_id == 496 | location_id == 507
replace iso3 = "IDN" if location_id == 4709
replace iso3 = "KEN" if location_id == 35647
replace iso3 = "IND" if location_id == 43937
assert iso3 != ""
replace nid = 9993 if source == "Online supplement 2016"
assert nid != .
	// save in our folder
saveold "FILEPATH", replace
saveold "FILEPATH", replace		
		
		
		
	
		
		
		
		
		
