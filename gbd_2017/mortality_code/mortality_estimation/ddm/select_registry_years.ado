cap program drop select_registry_years
program define select_registry_years


clear
set mem 500m
set maxvar 32000
set more off

********************************************************
** Set parameters
syntax, data(string)

set obs 1
g filename = "`data'"
levelsof filename, local(filename)

use `filename', clear

sort year
egen min_registry_year = min(year)
egen max_registry_year = max(year)

levelsof min_registry_year, local (min_registry_year)
levelsof max_registry_year, local (max_registry_year)

gen usable = 0
local obser = _N
gen tmp = _n
local usable_year = `min_registry_year'
forvalues i = 1/`obser' {
	local iplus = `i' + 1
	if (`iplus' > `obser'){
		break
	}

	levelsof year if tmp == `i', local(current_year)

	if (`current_year' != `usable_year'){
		continue
	}

	forvalues j = `iplus'/`obser'{
		levelsof year if tmp == `j', local(next_year)
		local diff = `next_year' - `current_year'
		if (`diff' == 5){
			replace usable = 1 if tmp == `j'
			levelsof year if year == `next_year', local(usable_year)
			continue, break
		}
		if (`diff' != 5 & `diff' < 5) {
			continue
		}
		if (`diff' != 5 & `diff' > 5) {

			replace usable = 1 if tmp == `j'
			levelsof year if year == `next_year', local (usable_year)
			continue,break
		}
	}
}
replace usable = 1 if year == `min_registry_year'
replace usable = 1 if year == `max_registry_year'

keep if usable == 1
drop usable tmp

end

* DONE