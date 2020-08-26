*** ======================= BOILERPLATE ======================= ***
clear all
set more off
set maxvar 32000
if c(os) == "Unix" {
  global prefix FILEPATH
  set odbcmgr ADDRESS
}
else if c(os) == "Windows" {
  // Compatibility paths for Windows STATA GUI development
  global prefix FILEPATH
  local 2 FILEPATH
  local 3 "--draws"
  local 4 FILEPATH
  local 5 "--interms"
  local 6 FILEPATH
  local 7 "--logs"
  local 8 FILEPATH
}

  // define locals from jobmon task
  local params_dir        `2'
  local draws_dir         `4'
  local interms_dir       `6'
  local logs_dir          `8'

  cap log using "`logs_dir'/FILEPATH", replace
  if !_rc local close_log 1
  else local close_log 0

  di "`params_dir'"
  di "`draws_dir'"
  di "`interms_dir'"
  di "`logs_dir'"

  * Load shared functions
  adopath + FILEPATH


*** ======================= MAIN EXECUTION ======================= ***
tempfile master
get_demographics, gbd_team(epi) clear
local currentYear =  word("`r(year_id)'", -1)

** PULL IN GEOGRAPHIC RESTRICTIONS ***
*species rhodesiense = "r"
import delimited using "`params_dir'/FILEPATH", clear
drop if type=="citation"
drop if type=="quality"
drop parent_id level loc_nm_sh spr_reg_id region_id type gaul_code

forvalues i = 12 / 51 {
    rename v`i' status_r`=`i'+1968'
}

rename pre1980 status_r1979
export delimited using "`interms_dir'/FILEPATH", replace


*species gambiense = "g"
import delimited using "`params_dir'/FILEPATH", clear
drop if type=="citation"
drop if type=="quality"
drop parent_id level loc_nm_sh spr_reg_id region_id type gaul_code

forvalues i = 12 / 51 {
    rename v`i' status_g`=`i'+1968'
}

rename pre1980 status_g1979
export delimited using "`interms_dir'/FILEPATH", replace


*reshaping to long format; merge species R and G
foreach species in r g {
    import delimited using "`interms_dir'/FILEPATH_`species'.csv", clear

    reshape long status_`species', i(ihme_lc_id loc_name loc_id) j(year_id)
    if "`species'"=="g" merge 1:1 loc_id year using `master', assert(3) nogenerate
    save `master', replace
}

tab status_g status_r

*renaming pa/a and pp/p to numerals 
gen status_g_new=1 if status_g=="p"| status_g=="pp"
replace status_g_new=0 if status_g=="a"| status_g=="pa"

gen status_r_new=1 if status_r=="p"| status_r=="pp"
replace status_r_new=0 if status_r=="a"| status_r=="pa"

rename ihme_lc_id ihme_loc_id
rename loc_name location_name
rename loc_id location_id

drop status_g status_r

rename status_g_new status_g
rename status_r_new status_r

*adding 1977 and 1978 to geo_rest
expand 2 if year_id==1979, gen(new)
replace year_id=1978 if new==1
drop new

expand 2 if year_id==1978, gen(new)
replace year_id=1977 if new==1
drop new

save "`interms_dir'/FILEPATH", replace
use "`interms_dir'/FILEPATH", clear

merge 1:1 location_id year_id using "`interms_dir'/FILEPATH", assert(2 3) keep(3)

*ensure proportions of  gambiense = pr_g  &  rhodesiense = pr_g  reflect input data
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


*pr_g and pr_r are missing for locations 179, 210, 213 as no cases were reported for these locations for either species.
*Set these missing values to 0s:
*Ethiopia, Liberia, Niger
replace pr_g=0 if pr_g==. & inlist(location_id,179,210,213)
replace pr_r=0 if pr_g==. & inlist(location_id,179,210,213)

*replacing all status_g and status_r with 0s if country has no reported data >=1990.
*Burundi, Ethiopia, Rwanda, Botswana, Gambia, Guinea-Bissau, Liberia, Niger, Sengal, Sudan, Busia County (Kenya)
replace pr_g = 0 if inlist(location_id, 175, 179, 185, 193, 206, 209, 210, 213, 216, 522, 35620)
replace pr_r = 0 if inlist(location_id, 175, 179, 185, 193, 206, 209, 210, 213, 216, 522, 35620)
*35620 reflects Kenya geo_restrictions; equate Kenyan data to Busia County data
replace pr_r = 1 if location_id==35620

keep location_id year_id pr_* 

export delimited using "`interms_dir'/FILEPATH", replace

import delimited using "`params_dir'/FILEPATH", clear


sort location_id year_id 

drop if year_id==1977
drop if year_id==1978
drop if year_id==1979



save "`interms_dir'/FILEPATH", replace



*** ======================= CLOSE LOG ======================= ***
if `close_log' log close

exit, STATA clear
