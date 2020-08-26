* Purpose: Submit Rabies Non-Fatal Estimation code by location

* Before running:
* 1) set run_id - make sure not to overwrite old run_id
* 2) confirm decomp steps on child script
* 3) confirm and record latest codcorrect used to make estimates

clear all
set more off

* get demographic information to pass on
run FILEPATH
get_demographics, gbd_team(epi) clear
local locations `r(location_id)'
local years `r(year_id)'
local ages `r(age_group_id)'
local currentYear = max(`=subinstr("`years'", " ", ",", .)')


* 1) set run_id 
local run_id ADDRESS
local rootDir "FILEPATH"


* create directory for draws
local outDir `rootDir'ADDRESS
local locations 189 190 191 193 53573 204 205 206 207 208 209 210 211 212 213 484 485 25318 25319 25320 25321 25322 25323 25324 25325 25326 25327 25328 25329 25330 25331 25332 25333 4721


* SUBMIT BASH FILES *
foreach location_id of local locations {
    ! qsub  -P ADDRESS -l m_mem_free=1G -l fthread=1 -l archive=True -l h_rt=0:10:00 -q all.q -N d2c_`location_id' "FILEPATH" "`location_id'" "`=subinstr("`years'", " ", "_", .)'" "`=subinstr("`ages'", " ", "_", .)'" "`outDir'"
    sleep 1000
}
