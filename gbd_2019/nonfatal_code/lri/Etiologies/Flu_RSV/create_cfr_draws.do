// Prep LRI case fatality scalars //
// This pulls results for the ratio 
// of CFR for bacterial/viral pneumonia
// and saves for use in PAF calculations //
set more off

import delimited "filepath", clear
keep if id!=.
tempfile ages
save `ages'

use "filepath", clear
merge 1:1 id using `ages', nogen
forvalues draw = 1/1000 {
	local i = `draw' - 1
	rename draw`draw' scalarCFR_`i'
}
saveold "filepath", replace
