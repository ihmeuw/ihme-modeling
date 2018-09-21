clear all

local source_out_dir "FILEPATH"	
use "`source_out_dir'/d01_formatted_population.dta", clear
count 
local cn `r(N)'
** location-age-specific corrections
foreach i in pop50 pop51 pop52 pop53 pop54 {
	replace `i' = (pop49*pop55)^0.5 if ihme_loc_id == "IND_4851" & sex == "female" & year == 1971
}

foreach i in pop25 pop26 pop27 pop28 pop29 {
	replace `i' = (pop24*pop30)^0.5 if ihme_loc_id == "USA_546" & sex == "male" & year == 1970
}

foreach i in pop55 pop56 pop57 pop58 pop59 {
	replace `i' = (pop54*pop60)^0.5 if ihme_loc_id == "USA_570" & sex == "female" & year == 1950
}

foreach i in pop10 pop11 pop12 pop13 pop14 {
	replace `i' = (pop9*pop15)^0.5 if ihme_loc_id == "USA_529" & sex == "female" & year == 1960
}

tempfile all
save `all', replace

drop if  (ihme_loc_id == "IND_4851" & year == 1971) | (ihme_loc_id == "USA_546" & year == 1970) | (ihme_loc_id == "USA_570" & year == 1950) | (ihme_loc_id == "USA_529" & year == 1960)
tempfile pres
save `pres', replace

use `all', clear
keep if  (ihme_loc_id == "IND_4851" & year == 1971) | (ihme_loc_id == "USA_546" & year == 1970) | (ihme_loc_id == "USA_570" & year == 1950) | (ihme_loc_id == "USA_529" & year == 1960)

reshape long agegroup pop, i( ihme_loc_id  country  sex  year  month  day  source_type  pop_source  pop_footnote  pop_nid) j(age)
rename pop num

reshape wide agegroup num, i( ihme_loc_id  country  age  year  month  day  source_type  pop_source  pop_footnote  pop_nid) j(sex) string
replace numboth = numfemale + nummale

reshape long agegroup num, i( ihme_loc_id  country  age  year  month  day  source_type  pop_source  pop_footnote  pop_nid) j(sex) string
rename num pop

reshape wide agegroup pop, i( ihme_loc_id  country  sex  year  month  day  source_type  pop_source  pop_footnote  pop_nid) j(age)
tempfile corrected
save `corrected', replace

use `pres', clear
append using `corrected'

count
assert `cn' == `r(N)'

saveold  "`source_out_dir'/d01_formatted_population.dta", replace

