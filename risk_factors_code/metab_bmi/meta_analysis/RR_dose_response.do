clear all
set more off
set varabbrev off
cap log close

log using "FILEPATH", smcl replace

import delimited using "FILEPATH", clear
tempfile alz
save `alz', replace

import excel using "FILEPATH", clear firstrow
keep if bmi_upper != .
keep if keep == 1

levelsof outcome, local(outcomes)

foreach o of local outcomes {
	preserve

	di as error "Outcome: `o'"

	keep if outcome == "`o'"
	keep if bmi_upper != .
	keep if keep == 1

	replace person_years = samplesize*lengthoffollowup if person_years==.

	bysort outcome source sex: egen tpy = total(person_years)
	bysort outcome source sex: egen tcase = total(cases)
	replace tpy = totalpersonyears if tpy == . | tpy == 0
	replace tcase = totalcases if tcase == . | tcase == 0

	gen logrr = log(mean)
	gen logrrl = log(lower)
	gen logrru = log(upper)
	gen selogrr = (logrru - logrrl) / 3.92

	egen gid = group(source sex studyname)
	gen source_type = 2

	sort gid edose

	glst logrr edose, se(selogrr) cov(tpy tcase) ir pfirst(gid source_type) eform

	restore
}

foreach o of local outcomes {
	preserve

	di as error "Outcome: `o'"

	keep if outcome == "`o'"
	keep if bmi_upper != .
	keep if keep == 1

	replace person_years = samplesize*lengthoffollowup if person_years==.

	bysort outcome source sex: egen tpy = total(person_years)
	bysort outcome source sex: egen tcase = total(cases)
	replace tpy = totalpersonyears if tpy == . | tpy == 0
	replace tcase = totalcases if tcase == . | tcase == 0

	gen logrr = log(mean)
	gen logrrl = log(lower)
	gen logrru = log(upper)
	gen selogrr = (logrru - logrrl) / 3.92

	egen gid = group(source sex studyname)
	gen source_type = 2

	sort gid edose

	levelsof gid, local(gids)

	gen pooled_logrr = .
	gen pooled_selogrr = .

	foreach gid of local gids {
		di as error "`gid'"
		glst logrr edose if gid == `gid', se(selogrr) cov(tpy tcase) ir eform
		mat coeff = e(b)
		mat var = e(V)
		replace pooled_logrr = coeff[1,1] if gid == `gid'
		replace pooled_selogrr = sqrt(var[1,1]) if gid == `gid'

	}

	keep source sex studyname pooled_logrr pooled_selogrr
	duplicates drop *, force

	if "`o'" == "dementia" {
		append using `alz'
	}

	egen id = concat(source sex), p("_")
	metaan pooled_logrr pooled_selogrr, dl exp forest label(id) effect("RR")
	graph export "FILEPATH", as(pdf) replace

	metaan pooled_logrr pooled_selogrr, fe exp forest label(id) effect("RR")
	graph export "FILEPATH", as(pdf) replace

	restore
}

cap log close
