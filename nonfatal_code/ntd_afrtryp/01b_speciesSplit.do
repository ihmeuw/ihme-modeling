
*** BOILERPLATE ***
	clear all
	set more off
	set maxvar 32000
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		set odbcmgr unixodbc
		}
	else if c(os) == "Windows" {
		global prefix "FILEPATH"
		}
	
	
tempfile master

*** PULL IN GEOGRAPHIC RESTRICTIONS ***
	foreach species in r g {			
		import delimited using "FILEPATH/hat_`species'_gr_map.csv", clear
		forvalues i = 4 / 49 {
			rename v`i' status_`species'`=`i'+1966'
			}
		reshape long status_`species', i( ihme_loc_id location_name location_id) j(year_id)
		if "`species'"=="g" merge 1:1 location_id year using `master', assert(3) nogenerate
		save `master', replace
		}

use FILEPATH/data2Model.dta, clear

merge 1:1 location_id year_id using `master', assert(2 3) keep(3)

expand 2 if year_id==2015, gen(new)
replace year_id = year_id + new
drop new
	
gen pr_g = reported_tgb / total_reported
gen pr_r = reported_tgr/ total_reported
replace pr_g = 1 if status_g==1 & status_r==0
replace pr_g = 0 if status_g==0 & status_r==1
replace pr_r = 1 if status_g==0 & status_r==1
replace pr_r = 0 if status_g==1 & status_r==0

sort location_id year_id
by location_id: replace pr_g = pr_g[_n-1] if missing(pr_g)
by location_id: replace pr_r = pr_r[_n-1] if missing(pr_r)
gsort location_id -year_id
by location_id: replace pr_g = pr_g[_n-1] if missing(pr_g)
by location_id: replace pr_r = pr_r[_n-1] if missing(pr_r)

keep location_id year_id pr_* 

save FILEPATH/speciesSplit.dta, replace
