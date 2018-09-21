// Estimates the ratio of severe/very-severe to moderate lower respiratory infections //
// Input data are from systematic review of scientific literature //

set seed 619003744
set more off

// LRI dataset //
import excel "FILEPATH/request_65036.xlsx", firstrow clear

bysort nid: egen keep_sev = total(cv_diag_severe)
drop if keep_sev==0
keep if is_outlier==0
sort field_citation_value

gen log_mean = log(mean)
replace sample_size = mean*(1-mean)/standard_error^2 if nid == 108872
replace sample_size = mean*(1-mean)/standard_error^2 if sample_size== . 

save `base'
 
// Collapse for analysis //

drop if cv_diag_severe == 0
collapse (sum)mean (sum)cases (sum)sample_size (sum)standard_error, by(nid field_citation_value location_id)
rename mean severe
rename standard_error error_severe
rename sample_size sample_severe
tempfile sev
save `sev'

use `base', clear
drop if cv_diag_severe== 1
collapse (sum)mean (sum)cases (sum)sample_size (sum)standard_error, by(nid field_citation_value location_id)

merge m:m nid location_id using `sev', keep(3) nogen

gen sev_logit = logit(severe)
gen non_logit = logit(mean)

gen se_severe = sqrt(severe*(1-severe)/sample_severe)
gen se_non = sqrt(mean*(1-mean)/sample_size)

forval i = 1/1000{
	gen draw_sv_`i' = logit(rnormal(severe, se_severe))
	gen draw_non_`i' = logit(rnormal(mean, se_non))
}
egen se_sv_logit = rowsd(draw_sv*)
egen se_no_logit = rowsd(draw_non_*)
drop draw_sv* draw_non*

forval i = 1/1000 {
	gen sev_`i' = invlogit(rnormal(sev_logit, se_sv_logit))
	gen non_`i' = invlogit(rnormal(non_logit, se_no_logit))
	gen ratio_`i' = sev_`i'/(non_`i' + sev_`i')
}

drop sev_* non_*
egen mean_ratio = rowmean(ratio*)
egen se_ratio = rowsd(ratio*)
drop ratio_*
gen ratio = severe/mean

merge m:1 location_id using "FILEPATH/ihme_loc_metadata_2016.dta", keep(3) nogen force

sort field_citation_value
gen label = substr(field_citation_value, 1, strpos(field_citation_value, ",")-1) + " " + location_name 

metan mean_ratio se_ratio, label(namevar = label) random

metan mean_ratio se_ratio, by(super_region_name) label(namevar = label) random
