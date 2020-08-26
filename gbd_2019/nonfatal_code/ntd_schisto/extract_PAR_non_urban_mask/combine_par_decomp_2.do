** this pulls in most recent BRT results and combines them together

** prep stata
clear all
set more off
set maxvar 32000
if c(os) == "Unix" {
	global prefix "ADDRESS"
	set odbcmgr ADDRESS
}
else if c(os) == "Windows" {
	global prefix "ADDRESS"
}


** set adopath
adopath + $prefix/FILEPATH
run $prefix/FILEPATH


	tempfile merge
	forvalues i = 1 / 1000 {
		import delimited using "FILEPATH", clear
		drop v1
		if `i'>1 quietly merge 1:1 zone using `merge', assert(3) nogenerate
		save `merge', replace
	}



rename prop_1000 prop_0
rename zone location_id

order prop_*, sequential  
*after(period)

tempfile combined
save `combined'

keep if !inlist(location_id, 491,494,496,497,503,504,506,507,514,516,520,521)



drop  if location_id==16

keep location_id prop_*

tempfile drop_chn_phl
save `drop_chn_phl'


get_location_metadata, location_set_id(35) clear
keep location_id location_name ihme_loc_id

merge 1:m location_id using `drop_chn_phl', assert(1 3) keep(3) nogenerate

drop if strmatch(ihme_loc_id, "BRA_*")

tempfile drop_chn_bra_phl
save `drop_chn_bra_phl'

*brining in gbd 2017 urban mask results and subsetting for brazil to later append to the decomp 2 extract par
use "$prefix/FILEPATH", clear
keep if strmatch(ihme_loc_id, "BRA_*")
tempfile bra_urban_mark
save `bra_urban_mark'

use `drop_chn_bra_phl'
append using `bra_urban_mark'

drop mean 

export delimited using "FILEPATH", replace
save "FILEPATH", replace


use "$prefix/FILEPATH", clear

expand 2 if year_id==2017, gen(new)
 replace year_id=2015 if new==1
 drop new
 
expand 2 if year_id==2017, gen(new)
replace year_id=2019 if new==1
drop new

sort location_id year_id

export delimited using "FILEPATH", replace
save "FILEPATH", replace

****DECOMP 2 CODE ENDS HERE *****

