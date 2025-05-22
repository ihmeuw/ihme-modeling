/ Purpose:	Run linear model (Step 1)

// /////////////////////////////////////////////////
// CONFIGURE ENVIRONMENT
// /////////////////////////////////////////////////
** Set code directory for Git
//set trace on 
//set more off
//macro dir


//local user "`c(username)'"
local user "`user"

quietly {
if c(os) == "Unix" {
global J "/home/j"
local code_dir = "FILEPATH"
local outdir = "FILEPATH"
local H = "FILEPATH"
set more off
set odbcmgr unixodbc
cd "`outdir'"
}
else {
global J "J:"
local H = "FILEPATH"
local code_dir = "FILEPATH"
local outdir = "H:/"
}	 
}
adopath + "FILEPATH/functions1/"
adopath + "FILEPATH/functions2/"
adopath + "FILEPATH/functions3/"
adopath + "FILEPATH/functions4/"
clear all

// Toggle whether to generate a new dataset based on DB run
local gen_new = 1



///loc map 
insheet using "`outdir'/get_locations_2020.csv", comma clear 
keep location_id location_name level
tempfile locmap
save `locmap', replace

//loc map 2 
insheet using "`outdir'/get_locations_2020.csv", comma clear
keep location_id ihme_loc_id location_name
rename location_id parent_id 
rename ihme_loc_id parent_loc_id
tempfile locmap2 
save `locmap2', replace	



// /////////////////////////////////////////////////
// GET COUNTRY LIST
// /////////////////////////////////////////////////
// Include only non-GEN countries (mostly CON) 
// Of CON, include only VR-only or VR plus
// run get_locations in R and save the output in a csv since the get_locations function has been discontinued in stata
//keep location_id ihme_loc_id
insheet using "FILEPATH/get_locations_2020.csv", comma clear
keep location_id ihme_loc_id location_name
tempfile locs
save `locs' 


// Get all subnational locations, along with the total number of subnationals expected in each
insheet using "FILEPATH/get_locations_2020_lowest.csv", comma clear
keep if level >= 4
//Drop hk/macaou since they are not really subnationals
drop if level == 4 & regexm(ihme_loc_id,"CHN")
split ihme_loc_id, parse("_")
rename ihme_loc_id1 parent_loc_id
bysort parent_loc_id: gen num_locs = _N


// Mainland has all the data
replace parent_loc_id = "CHN_44533" if parent_loc_id == "CHN" 
drop parent_id 
merge m:1 parent_loc_id using `locmap2', keep(1 3) nogen
keep ihme_loc_id parent_loc_id parent_id num_locs 
tempfile subnat_locs
save `subnat_locs'


duplicates drop parent_loc_id, force
keep parent_loc_id
rename parent_loc_id ihme_loc_id
tempfile parent_locs
save `parent_locs'

//import delimited using "FILEPATH", clear varnames(nonames)
import delimited using "FILEPATH", clear varnames(nonames)
rename v1 ihme_loc_id
duplicates drop ihme_loc_id, force
// still want to run india
drop if regexm(ihme, "IND")
// We only want to keep the countries that are NOT in the gen_countries list
merge 1:1 ihme_loc_id using `locs', keep(2) nogen 
keep ihme_loc_id
tempfile st_locs
save `st_locs'



// Scalar computed using limited use data in prepping china script
//insheet using "FILEPATH/prepped_hiv_data.csv", comma names clear
/*
insheet using "/FILEPATH/prepped_hiv_data.csv", comma names clear
gen infect_deaths = hiv_deaths + aids_deaths
drop aids_deaths hiv_deaths
tempfile infect
save `infect', replace

insheet using "`outdir'/cod_data_2020.csv", comma names clear
keep if regexm(cod_source_label, "China_2004_2012") 
drop if age_group_id == 22 | age_group_id == 27
collapse (sum) deaths, by(year_id location_id location_name)
keep if year_id > 2012
rename deaths vr_deaths
merge 1:1 location_name year_id using `infect', keep(3) nogen
gen scalar_gbd2019 = infect_deaths/vr_deaths 
replace scalar_gbd2019 = 1 if scalar < 1
collapse(max) scalar_gbd2019, by(location_name)
*/
// save "FILEPATH", replace 




// /////////////////////////////////////////////////
// GET DATA
// /////////////////////////////////////////////////

// get_cod_data, cause_id("298") gbd_round_id("6") decomp_step("step4") clear
insheet using "`outdir'/cod_data_2020.csv", clear
// TODO convert datatypes
destring deaths, replace force
destring pop, replace force
destring env, replace force
// drop if rate == "NA"
// destring rate, replace

merge m:1 location_id using `locs', keep(3) nogen
// Drop GEN countries from raw data, and higher-level locs from st_locs
merge m:1 ihme_loc_id using `st_locs', keep(3) nogen 
tempfile master
save `master'

use `master', clear

// Drop non-VR sources that we can't use
gen exception = 0 
replace exception = 1 if regexm(ihme_loc_id,"IDN") 
replace exception = 1 if regexm(ihme_loc_id,"IND") 
replace exception = 1 if regexm(ihme_loc_id,"VNM")
//drop if inlist(data_type,"Surveillance","Verbal Autopsy") & exception == 0. data_type is now data_type_name
drop exception

// No HIV deaths before 1981
drop if year_id < 1981 

// Get list of countries with actual data here
preserve
duplicates drop ihme_loc_id, force
keep ihme_loc_id
tempfile cod_data_locs
save `cod_data_locs'
restore

tempfile master
save `master'


// /////////////////////////////////////////////////
// EDIT PARAMETER FILE
// /////////////////////////////////////////////////

insheet using "FILEPATH/locations_four_plus_stars.csv", clear
keep ihme stars 
tempfile star 
save `star', replace 
use `locs', clear 
keep ihme 
merge 1:1 ihme using `star' 
gen complete = 0 
replace complete = 1 if _m == 3  
keep ihme complete 
replace complete = 1 if regexm(ihme,"GBR")
replace complete = 1 if regexm(ihme,"RUS")
replace complete = 1 if regexm(ihme,"NZL")
replace complete = 1 if regexm(ihme,"NOR")
replace complete = 1 if regexm(ihme,"UKR")
tempfile comp_indic 
save `comp_indic', replace


//import delimited using "`code_dir'/params.csv", clear // uncomment this and remove the hardcoded path below 
import delimited using "/homes/meverma/hiv_onboarding/spectrum/hiv_gbd/02_internal_models/hiv_stgpr/params.csv", clear
// Drop GEN countries and countries without CoD VR data from params, and higher-level locs from st_locs
merge m:1 ihme_loc_id using `st_locs', keep(3) nogen 
merge 1:1 ihme_loc_id using `comp_indic', keep(3) nogen
merge 1:1 ihme_loc_id using `cod_data_locs', keep(3) nogen

// Space-Time Parameters
// Lambda: Time-weight effect in ST. t_weights_lookup[i][j] = (1-(a_dist/(max_a_dist+1))**self.lambdaa)**3
//			Essentially, lower lambda expands the width of data used by ST to calculate a single-year estimate
replace lambda = .1 if complete == 1
replace lambda = .3 if complete == 0

// Omega: Age weight in ST:	for i in range(len(self.age_map)+1):
//	            				a_weights_lookup[i] = 1 / np.exp(self.omega*i)
//		  A higher omega should narrow the bandwith of ages used to calculate a single-age estimate (since the distance from one point to another would increase much more steeply)
//			However, omega = 0 should make it so no data is used except for the data in the age group itself<<<<<<< HEAD
replace omega = 1 
*non smooth changes in age data, but we still trust the data 
replace omega = 1 if inlist(ihme, "BMU", "URY", "MEX_4648", "PAN","BGR","CZE","HUN")
replace omega = 1 if inlist(ihme,"ROU","SRB","CHL","KGZ","TJK","FJI","GUM")
replace omega = 1 if inlist(ihme,"KIR","MNR","BLZ","BMU","BRB","DMA","GRD","LCA","PRI")
replace omega = 1 if inlist(ihme,"SUR","VCT","NZL_44850","NZL_44851","CYP","FIN","GRC","ISL","ITA")
replace omega = 1 if inlist(ihme,"LUX","MLT","NLD","BHR","LVA","MDA","KOR","SGP","GRL")
replace omega = 1 if inlist(ihme,"CHN_354","CRI","GTM","MUS","PHL","SYC")


replace zeta = .95

// GPR Parameters: See page 9 and 10 in http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.385.4366&rep=rep1&type=pdf
// Scale: Stretches the x axis of the data in use. Higher scale -> bigger window of data used
replace scale = 5 if complete == 1
replace scale = 7 if complete == 0
// replace scale = 15 // Try to see if this fixes huge upticks in ST-GPR in 2014/15 for many countries due to the 1984 anchoring points and log-space modeling

// Amp: Stretches the y axis -- lower amp allows for less "wiggle" vertically
// Ex: replace amp = "regional" if ihme_loc_id == "PRK"
// replace amp = "national" if complete == 1 // National amp crashes CRI males among others -- why? 
// STP is only location without a regional mad and only a global one so have to set to global
replace amp = "global" if ihme == "STP"

export delimited using "`outdir'/params.csv", delimit(",") replace


// /////////////////////////////////////////////////
// RUN REGRESSIONS
// /////////////////////////////////////////////////
use `master', clear
gen national_type_id = 0
merge m:1 location_id using `locmap', keep(1 3) nogen
replace national_type_id = 1 if level == 3 // national
replace national_type_id = 2 if level >= 4 // subnational
fastcollapse deaths env pop sample_size, type(sum) by(location_id year_id age_group_id sex_id national_type_id data_type_name site_id nid)
drop if inlist(age_group_id, 2, 3) 


// Convert to CF space
// gen cf_HIV=dths_final/mean_env

// If you want Death Rate space, uncomment the below... sorry about the confusing variable naming
gen death_rate = (deaths/pop)*100
summ death_rate if death_rate > 0
replace death_rate = r(min) if death_rate == 0
// Calculate sample size
rename sample_size deaths_ss
gen sample_size = (deaths_ss/env)*pop
summ sample_size if sample_size > 0
replace sample_size = r(min) if sample_size == 0
gen scalar_ss = deaths_ss / env
sum scalar_ss 
local mean_scale = `r(mean)'
tempfile data
save `data'
outsheet using "`outdir'/death_rate_2019.csv", comma replace

// Make template

insheet using "/FILEPATH/get_locations_2020_lowest.csv", comma clear
keep location_id ihme_loc_id location_name region_id region_name super_region_id super_region_name level 

// this needs to be updated for the old age groups
expand 2
bysort location_id: gen sex_id = _n

expand 55
bysort location_id sex_id: gen year_id = _n + 1969


expand 23
bysort location_id sex_id year_id: gen age_group_id = _n + 5 if _n>=1 & _n<=15 
bysort location_id sex_id year_id: replace age_group_id = _n + 14 if _n>=16 & _n<=18
bysort location_id sex_id year_id: replace age_group_id = _n + 15 if _n==19
bysort location_id sex_id year_id: replace age_group_id = _n + 215 if _n==20
bysort location_id sex_id year_id: replace age_group_id = _n + 217 if _n==21 
bysort location_id sex_id year_id: replace age_group_id = _n + 366 if _n>=22 & _n<=23

  
//old age groups  
//expand 23
//bysort location_id sex_id year_id: gen age_group_id = _n + 1 if _n<=19 
//bysort location_id sex_id year_id: replace age_group_id = _n + 10 if _n>=20 & _n<=22 
//bysort location_id sex_id year_id: replace age_group_id = 235 if _n ==23 



order location_id ihme_loc_id year_id sex_id age_group_id



drop if year_id < 1981

tempfile master
save `master'

merge 1:m location_id year_id age_group_id sex_id using `data', keep(1 3) nogen



// Delete the anchoring point if that is the only data point available for the location -- don't want to pull all estimates to 0 across the time series 
// Instead remove all data so it'll move to the regional average
bysort location_id age_group_id sex_id: egen data_count = count(death_rate) 
gen exception = 0 
//going to turn this off
replace exception = 1
tab ihme
replace exception = 1 if regexm(ihme, "IDN") 
replace exception = 1 if inlist(age_group_id,2,3)
foreach var in deaths death_rate sample_size {
replace `var' = . if data_count == 1 & exception == 0 
}
foreach var in data_type_name {
replace `var' = "" if data_count == 1 & exception ==0
}
drop exception
drop data_count

// Prep for linear regression
// gen lt_cf = logit(cf_HIV) 

/// This is the input file for Central ST-GPR
outsheet using "`outdir'/gpr_input_new.csv", comma replace

// (Changing from gen ln_dr = ln(death_rate) to death_rate for transition to central ST-GPR. No need to transform zeroes because all death rates are non-zero (many are close, but none have values for DR but not for ln_dr)
gen ln_dr = ln(death_rate) 
bysort location_id age_group_id sex_id: egen peak_dr = max(ln_dr)
gen possible_peakyear = year_id if (ln_dr == peak_dr) & (ln_dr != .)
bysort location_id age_group_id sex_id: egen peakyear = max(possible_peakyear) 
bysort location_id sex_id: egen location_peakyear = mean(peakyear)
bysort region_id sex_id: egen region_peakyear = mean(peakyear)
bysort super_region_id sex_id: egen sr_peakyear = mean(peakyear)
egen global_peakyear = mean(peakyear)

local peaktype "informed"
if "`peaktype'" == "informed"{ 
replace peakyear = location_peakyear if peakyear == .
replace peakyear = region_peakyear if peakyear == .
replace peakyear = sr_peakyear if peakyear == .
replace peakyear = global_peakyear if peakyear == .
}
else if "`peaktype'" == "uninformed"{
}

replace peakyear = round(peakyear)

// Manually set location/age-specific knots based on prior knowledge
replace peakyear = 2004 if location_id == 22 
replace peakyear = 2005 if ihme == "VNM"



gen year_relto_peak = year-peakyear

mkspline spl1 0 spl2 = year_relto_peak
// mkspline spl1 1995 spl2 = year

// Run linear regression 

rreg ln_dr spl1 spl2 i.region_id i.age_group_id i.sex_id 



predict ln_dr_predicted

// Flag subnationals
gen sn_flag = 0
replace sn_flag = 1 if (national_type_id == 2) | (national_type_id == 0)

sort location_id age_group_id year_id
keep location_id year_id age_group_id sex_id national_type_id data_type_name site sample_size sn_flag death_rate deaths env pop ln_dr ln_dr_predicted 
*to calculate age_weights they have to be consecutive integers 
//replace age_group_id = 21 if age_group_id ==30
//replace age_group_id = 22 if age_group_id == 31 
//replace age_group_id = 23 if age_group_id == 32 
//replace age_group_id = 24 if age_group_id == 235
replace age_group_id = 21 if age_group_id ==30
replace age_group_id = 22 if age_group_id == 31 
replace age_group_id = 23 if age_group_id == 32 
replace age_group_id = 24 if age_group_id == 235
replace age_group_id = 5 if age_group_id == 34
replace age_group_id = 4 if age_group_id == 238 
replace age_group_id = 3 if age_group_id == 389 
replace age_group_id = 2 if age_group_id == 388 


//very important to have it sorted
sort location_id age_group_id year_id
outsheet using "linear_predictions.csv", comma replace

// END OF DO FILE