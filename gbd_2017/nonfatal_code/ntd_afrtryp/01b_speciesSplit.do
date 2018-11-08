
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


run FILEPATH/get_demographics.ado"
get_demographics, gbd_team(ADDRESS) clear
local currentYear =  word("`r(year_id)'", -1)

*** PULL IN GEOGRAPHIC RESTRICTIONS ***
	foreach species in r g {			
		import delimited using "FILEPATH/hat_`species'_gr_map.csv", clear
		forvalues i = 4 / 51 {
			rename v`i' status_`species'`=`i'+1966'
			}
		reshape long status_`species', i( ihme_loc_id location_name location_id) j(year_id)
		if "`species'"=="g" merge 1:1 location_id year using `master', assert(3) nogenerate
		save `master', replace
		}

save "FILEPATH/species_split_step.dta", replace

		
use FILEPATH/data2Model.dta, clear
drop _merge

merge 1:1 location_id year_id using "FILEPATH/species_split_step.dta", assert(2 3) keep(3)
*need to drop all that are _merge!=3

drop new
***********not working**********
quietly sum year_id
local lastYear = `r(max)'


forvalues y = `=`lastYear'+1'/`currentYear' {
	expand 2 if year_id==`lastYear', gen(new)
	replace year_id = `y' if new==1
	drop new
	}
	
	
	*******************************************
	
*Fix sierra leone needs to have a 2016 and 2017 row in the data

expand 2 if year_id==2015 & location_id==217, gen(new)
replace year_id=2016 if new==1
drop new

expand 2 if year_id==2016 & location_id==217, gen(new)
replace year_id=2017 if new==1
drop new


***do the same for Mozambique


expand 2 if year_id==2015 & location_id==184, gen(new)
replace year_id=2016 if new==1
drop new

expand 2 if year_id==2016 & location_id==184, gen(new)
replace year_id=2017 if new==1

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


** add rows for 1980-89

expand 11 if year_id==1990, gen(new)
replace year_id=1980 if new==1


*create a counter
bysort location_id (year_id) : gen Nyear = sum(new[_n - 1]) 
replace year_id=year_id+Nyear if year_id<1990

drop Nyear
*check # of rows of data per country
table location_id

gsort location_id -year_id


save FILEPATH/speciesSplit.dta, replace
