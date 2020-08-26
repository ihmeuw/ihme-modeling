
*** BOILERPLATE ***
clear all
set more off, perm
set maxvar 10000

if c(os) == "Unix" {
    local ADDRESS "FILEPATH"
    set odbcmgr ADDRESS
    }
else if c(os) == "Windows" {
    local ADDRESS "FILEPATH"
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
merge 1:m location_id using FILEPATH



*** FLAG ZIKA TRANSMISSION COUNTRIES ***
local zCountries 10 11 21 21 27 97 102 111 118 125 129 130 133 135 136 190 216 385 422 532 4751 4754 4755 4756 4758 4759 4761 4764 4766 4767 4768 4769 4774 4776 105 107 108 109 110 112 113 114 115 116 117 119 121 122 123 126 127 128 131 132

generate zika = 0

foreach loc of local zCountries {
	quietly replace zika = 1 if location_id==`loc'
	}


collapse (max) zika (mean) efImplied_*, by(countryIso *region* global) fast	



*** REPLACE MISSING VALUES WITH NEAREST NON-MISSING LEVEL OF LOCATIONS HIERARCHY ***		
forvalues i = 0 / 999 {
	quietly {
		replace efImplied_`i' = 1 / efImplied_`i'

		foreach level in countryIso region_name super_region_name global {
			bysort `level': egen tempMean = mean(efImplied_`i')
			replace efImplied_`i' = tempMean if missing(efImplied_`i')
			drop tempMean 
			}		
		}
	di "." _continue
	}


egen efMean = rowmean(efImplied_*)
egen efSe = rowsd(efImplied_*)


generate efAlpha = efMean * (efMean - efMean^2 - efSe^2) / efSe^2 
generate efBeta  = efAlpha * (1 - efMean) / efMean 


keep countryIso efAlpha efBeta

save FILEPATH, replace    // reporting rates
