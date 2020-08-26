import excel using "FILEPATH", firstrow clear

sort location_id year_start

*this workd - but we can just run the collapse below
*by location_id year_start: egen sumcases = total(cases)

collapse (sum) cases, by(location_id year_start)
gen last_year_case=.

by location_id : replace last_year_case = cases[_n-1]

gen percent_change= (((cases-last_year_case)/last_year_case)*100)

*to get mean case by locations
bysort location_id: egen mean = mean(cases)

export delimited using "FILEPATH", replace
