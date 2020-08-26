clear all 
set more off, perm
set maxvar 10000

* ESTABLISH TEMPFILES AND APPROPRIATE DRIVE DESIGNATION FOR THE OS * 
if c(os) == "Unix" {
	local ADDRESS"
	local user : env USER
	local ADDRESS
	set odbcmgr ADDRESS
}
else if c(os) == "Windows" {
	local ADDRESS "FILEPATH"
	local ADDRESS "FILEPATH"
}

local inputDirFILEPATH 
local newinput FILEPATH
local skeleton `newinput'FILEPATH
tempfile pop bra

adopath + FILEPATH



* LOAD THE SKELETON, FOR BRAZILIAN STATES AND ALL AGES *
use `skeleton', clear
keep if strmatch(ihme_loc_id, "BRA_*") & age_group_id==22 & year>=1980 & sex==3
keep location_id parent_id location_name ihme_loc_id year_id sex_id population
bysort year_id: egen mean_pop_bra = total(population)
save `pop'

levelsof location_id, local(locations) clean



* PULL THE ALL-AGE YF COD DATA FOR BRAZILIAN STATES *
get_cod_data, cause_id(358) location_id(`locations') year_id(2017) decomp_step('step4') clear

generate endemic = !inlist(location_id, 4751, 4755, 4764, 4766, 4769, 4774)

drop if endemic==0


export delimited "FILEPATH", replace
