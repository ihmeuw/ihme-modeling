*** BOILERPLATE ***
clear all
set maxvar 10000
set more off

if c(os)=="Unix" {
    local "ADDRESS"
    set odbcmgr ADDRESS
}
else {
    local ADDRESS = "FILEPATH"
}


*** LOAD SHARED FUNCTIONS ***
adopath + FILEPATH
run FILEPATH
run FILEPATH


*** PULL LOCATION METADATA ***
get_location_metadata, location_set_id(35) clear
keep if location_type=="admin0" | is_estimate==1
keep location_id location_name *region* ihme_loc_id

generate countryIso = substr(ihme_loc_id, 1, 3)
generate global = 1


*** MERGE IN EXPANSION FACTOR DRAWS ***    
merge 1:m location_id using FILEPATH, assert(3) nogenerate
keep if countryIso==ihme_loc_id


bysort countryIso: egen nData = total(!missing(cases))
collapse (mean) efImplied_*, by(*region* countryIso nData) fast

local yfCountries AGO ARG BEN BOL BRA BFA BDI CMR CAF TCD COL COG CIV COD ECU GNQ ETH GAB GHA GIN GMB GNB GUY KEN LBR MLI MRT NER NGA PAN PRY PER RWA SEN SLE SDN SSD SUR TGO TTO UGA VEN ERI SOM STP TZA ZMB

gen yfCountry = 0
foreach countryIso of local yfCountries {
    quietly replace yfCountry = 1 if countryIso=="`countryIso'"
}

keep if yfCountry==1
preserve
keep if nData==0
keep countryIso
tempfile noData
save `noData', replace

restore
keep if nData>0
tempfile haveData
save `haveData', replace


collapse (mean) efImplied_*

cross using `noData'
append using `haveData'

egen efMean = rowmean(efImplied_*)
egen efLower = rowpctile(efImplied_*), p(2.5)
egen efUpper = rowpctile(efImplied_*), p(97.5)

br countryIso  efMean efLower efUpper

sort efMean

keep countryIso ef_*


save FILEPATH, replace
