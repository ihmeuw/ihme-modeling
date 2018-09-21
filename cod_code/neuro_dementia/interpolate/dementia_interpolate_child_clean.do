**Interpolation of death rates based on second Dismod model for Alzheimer's disease (child script)
**12/30/2016

//prep stata 
clear all
set more off
set maxvar 32000

// Set OS flexibility 
	if c(os) == "Unix" {
		local j "/home/j"
		set odbcmgr unixodbc
		local h "~"
	}
	else if c(os) == "Windows" {
		local j "J:"
		local h "H:"
	}

do "FILEPATH/interpolate.ado"
do "FILEPATH/get_population.ado"
do "FILEPATH/get_demographics.ado"
//transfer locals
args code_folder save_folder year version_id sex

//display locals to see which jobs work
di "`code_folder' `save_folder' `year' `sex'"

if `year' != 2010 & `year' != 1980 {
	local year_end = `year' + 5
} 
else if `year' == 2010 {
	local year_end = `year' + 6
}
else if `year' == 1980 {
	local year_end = `year' + 15
}


//interpolate dismod dementia csmr for all years
interpolate, gbd_id_field(modelable_entity_id) gbd_id(11641) measure_ids(15) sex_ids(`sex') version_id(`version_id') reporting_year_start(`year') reporting_year_end(`year_end') source("epi") clear

//fix dementia ages- don't die under 13
numlist "2/20 30/32 235"
local ages = "`r(numlist)'"
local ages = subinstr("`ages'", " ", ",",.)
keep if inlist(age_group_id, `ages')
//because under forty don't die of dementia
forvalues j=0/999 {
	quietly replace draw_`j' = 0 if age_group_id<13 
}
tempfile data
save `data'

//create deaths from csmr 
//get populations
//get locations 
get_demographics, gbd_team(cod) clear
local location_ids `r(location_ids)'

get_population, location_id(`location_ids') age_group_id("2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 30 31 32 235") sex_id("1 2") year_id("-1") clear
keep if year_id>=1980
tempfile pop 
save `pop'

//merge onto data
use `data'
merge 1:1 age_group_id sex_id year_id location_id using `pop', nogenerate keep(3)

//turn into number of deaths
forvalues j=0/999 {
	quietly replace draw_`j' = draw_`j' * population
}
drop population process_version_map_id
quietly replace measure_id = 1
cap gen cause_id = 543

//make sure there are no overalapping years
if `year_end' != 2016 {
	drop if year_id == `year_end'
}

export delim using "FILEPATH.csv", replace



